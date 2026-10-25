//! Metadata service contracts for Velorix control-plane state.

#![forbid(unsafe_code)]

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::RwLock;
use tonic::{
    metadata::MetadataValue,
    transport::{Channel, ClientTlsConfig, Endpoint, Identity},
    Request, Response, Status,
};
use velorix_core::relation::{RelationSchemaError, VelorixRelationCatalogV1};
use velorix_core::standing_program::RuntimeCheckpointInputCoverageV1;
use velorix_storage::{
    log::{
        DurableIngestAdmissionRecordV1, IngestAdmissionCoordinator,
        ReserveIngestRangeAdmissionOutcome,
    },
    materialized_view_registry::ActiveMaterializedViewRecord,
    object_key::ObjectKey,
    relation_catalog_registry::{
        CreateRelationCatalogOutcome, RelationCatalogRegistry, RelationCatalogRegistryError,
    },
};

pub mod proto {
    tonic::include_proto!("velorix.meta.v1");
}

mod published_input_bootstrap;
#[cfg(feature = "rhiza-backend")]
pub mod rhiza;
#[cfg(feature = "rhiza-backend")]
pub mod rhiza_kv;
#[cfg(feature = "rhiza-backend")]
pub mod rhiza_kv_snapshot;
#[cfg(feature = "rhiza-backend")]
pub mod rhiza_meta;
mod rhiza_snapshot;
mod source_cut;
mod view_bootstrap;
pub use published_input_bootstrap::{
    PublishedInputBootstrapCursorV1, PublishedInputBootstrapProgressV1,
    PUBLISHED_INPUT_BOOTSTRAP_SCHEMA_VERSION_V1,
};

pub use source_cut::{
    CaptureIngestSourceCutRequest, CaptureRelationIngestSourceCutRequest,
    CaptureRelationIngestSourceCutsRequest, IngestSourceCutV1, IngestSourcePartitionCutV1,
    IngestSourceRelationCutV1, IngestSourceRelationIdentityV1, RelationIngestPartitionCutV1,
    RelationIngestPublicationRefV1, RelationIngestSourceCutV1, RelationIngestSourceIdentityCutV1,
    INGEST_SOURCE_CUT_SCHEMA_VERSION_V1, INGEST_SOURCE_IDENTITY_GENERATION_V1,
    RELATION_INGEST_SOURCE_CUT_SCHEMA_VERSION_V1,
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

/// Reserves an ingest range under the independent authority domain that will
/// later authorize its object publication.  This prevents a reservation made
/// for one namespace/view from being published with another scope's token.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReserveAuthoritativeIngestRangeRequest {
    pub reservation: IngestRangeReservation,
    pub authority: PartitionAuthorityToken,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommitIngestRangeOutcome {
    Committed,
    Duplicate,
    Conflict,
}

/// Authoritatively binds a reserved ingest range to the immutable object that
/// contains its payload.  `request_id` is caller supplied; reusing it with a
/// different digest is a conflict, never a second publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishIngestReservationRequest {
    pub reservation: IngestRangeReservation,
    pub authority: PartitionAuthorityToken,
    pub request_id: String,
    pub request_digest: String,
    pub object_key: String,
    pub object_digest: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PublishIngestReservationOutcome {
    Committed,
    Duplicate,
    Conflict,
    InvalidAuthority,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AuthoritativeIngestPublication {
    pub reservation: IngestRangeReservation,
    pub authority_key: PartitionAuthorityKey,
    pub request_id: String,
    pub request_digest: String,
    pub object_key: String,
    pub object_digest: String,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct MetaStoreCapabilities {
    pub standing_runtime_fencing: StandingRuntimeFencingCapability,
    #[serde(default)]
    pub partition_authority: PartitionAuthorityCapability,
    #[serde(default)]
    pub relation_ingest: RelationIngestCapability,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct RelationIngestCapability {
    pub backend_name: String,
    pub relation_scoped_authority: bool,
    pub committed_publication_source_cut: bool,
    pub durable_across_restart: bool,
}

impl Default for RelationIngestCapability {
    fn default() -> Self {
        Self {
            backend_name: "unsupported".to_string(),
            relation_scoped_authority: false,
            committed_publication_source_cut: false,
            durable_across_restart: false,
        }
    }
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

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub published_input_bootstrap: Option<PublishedInputBootstrapProgressV1>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_endpoint: Option<String>,
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

/// Identifies the authority domain for relation ingest. This is intentionally
/// a distinct type from `PartitionAuthorityKey`: view/worker leases must not
/// be usable to reserve or publish a relation batch.
#[derive(Clone, Debug, Eq, Hash, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct RelationPartitionAuthorityKey {
    pub namespace: String,
    pub relation_id: String,
    pub stream_id: String,
    pub partition_id: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct RelationPartitionAuthorityToken {
    pub key: RelationPartitionAuthorityKey,
    pub owner_id: String,
    pub owner_epoch: u64,
    pub expires_at_unix_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AcquireRelationPartitionAuthorityRequest {
    pub key: RelationPartitionAuthorityKey,
    pub owner_id: String,
    pub current_token: Option<RelationPartitionAuthorityToken>,
    pub ttl_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AcquireRelationPartitionAuthorityOutcome {
    Acquired(RelationPartitionAuthorityToken),
    Renewed(RelationPartitionAuthorityToken),
    Conflict(RelationPartitionAuthorityToken),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReserveRelationAuthoritativeIngestRangeRequest {
    pub reservation: IngestRangeReservation,
    pub authority: RelationPartitionAuthorityToken,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishRelationIngestReservationRequest {
    pub reservation: IngestRangeReservation,
    pub authority: RelationPartitionAuthorityToken,
    pub request_id: String,
    pub request_digest: String,
    pub object_key: String,
    pub object_digest: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RelationAuthoritativeIngestPublication {
    pub reservation: IngestRangeReservation,
    pub authority_key: RelationPartitionAuthorityKey,
    pub request_id: String,
    pub request_digest: String,
    pub object_key: String,
    pub object_digest: String,
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PublishStandingRuntimeCheckpointRequest {
    pub expected_previous: Option<StandingRuntimeCheckpointPointer>,
    pub candidate: StandingRuntimeCheckpointPointer,
    pub owner: StandingRuntimeOwnerToken,
    /// The complete authoritative relation-ingest source cut observed while
    /// building `candidate`.  This is an optional guard so old callers keep
    /// the exact pre-existing wire shape when it is absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_relation_source_cuts: Option<Vec<RelationIngestSourceIdentityCutV1>>,
}

pub const MAX_DEMAND_METADATA_BATCH_LOOKUPS: usize = 64;
/// Active registry RPC envelopes may carry the existing bounded snapshot, so
/// tonic must be configured above its default 4 MiB message limit.
pub const MAX_META_GRPC_MESSAGE_BYTES: usize = 8 * 1024 * 1024;

/// Authority boundary for the active-view registry.  This is deliberately the
/// existing object-store authority plus namespace; it does not create a new
/// tenant or authorization model.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActiveViewRegistryScopeV1 {
    pub authority_store_id: String,
    pub namespace: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActiveViewRecordVersionV1 {
    pub generation: u64,
    pub record: ActiveMaterializedViewRecord,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActiveViewRegistryReadRequestV1 {
    pub scope: ActiveViewRegistryScopeV1,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActiveViewRegistrySnapshotV1 {
    pub snapshot_revision: u64,
    /// False means this scope has never been imported/initialized. Callers
    /// must not fall back to S3 once it is true, even when `records` is empty.
    pub imported: bool,
    pub records: Vec<ActiveViewRecordVersionV1>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateActiveViewRecordRequestV1 {
    pub scope: ActiveViewRegistryScopeV1,
    pub record: ActiveMaterializedViewRecord,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CreateActiveViewRecordOutcomeV1 {
    Created(ActiveViewRecordVersionV1),
    Duplicate(ActiveViewRecordVersionV1),
    Conflict,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompareExchangeActiveViewRecordRequestV1 {
    pub scope: ActiveViewRegistryScopeV1,
    pub view_id: String,
    pub expected_generation: u64,
    pub record: ActiveMaterializedViewRecord,
    /// Required when changing lifecycle on a standing-runtime record. Other
    /// API-authorized metadata updates remain generation-CAS operations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<StandingRuntimeOwnerToken>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompareExchangeActiveViewRecordOutcomeV1 {
    Updated(ActiveViewRecordVersionV1),
    Duplicate(ActiveViewRecordVersionV1),
    Conflict,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImportActiveViewRegistryRequestV1 {
    pub scope: ActiveViewRegistryScopeV1,
    pub records: Vec<ActiveMaterializedViewRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ImportActiveViewRegistryOutcomeV1 {
    Imported,
    Duplicate,
    Conflict,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PromoteViewBootstrapWithActiveViewRecordRequestV1 {
    pub bootstrap: PromoteViewBootstrapRequest,
    pub scope: ActiveViewRegistryScopeV1,
    pub expected_generation: u64,
    pub record: ActiveMaterializedViewRecord,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ActiveViewRegistryStateV1 {
    initialized: bool,
    snapshot_revision: u64,
    #[serde(with = "crate::rhiza_snapshot::map_pairs")]
    records: HashMap<String, ActiveViewRecordVersionV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DemandRelationCatalogLookup {
    pub relation_id: String,
    pub relation_version: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DemandStandingRuntimeCheckpointLookup {
    pub tenant_id: String,
    pub program_id: String,
    pub view_id: String,
}

/// A bounded, typed set of demand-start reads pinned to one metadata snapshot.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadDemandMetadataBatchRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_cuts: Option<CaptureRelationIngestSourceCutsRequest>,
    #[serde(default)]
    pub relation_catalogs: Vec<DemandRelationCatalogLookup>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub standing_runtime_checkpoint: Option<DemandStandingRuntimeCheckpointLookup>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadDemandMetadataBatchResponse {
    /// Returned in the order of `source_cuts.relations`.
    pub source_cuts: Option<Vec<RelationIngestSourceIdentityCutV1>>,
    /// Returned in the order of `relation_catalogs`.
    pub relation_catalogs: Vec<VelorixRelationCatalogV1>,
    /// `None` means the requested pointer was absent, or no pointer was requested.
    pub standing_runtime_checkpoint: Option<StandingRuntimeCheckpointPointer>,
}

impl ReadDemandMetadataBatchRequest {
    pub fn validate(&self) -> Result<(), MetaStoreError> {
        let source_cut_relations = self
            .source_cuts
            .as_ref()
            .map_or(0, |source_cuts| source_cuts.relations.len());
        let lookups = source_cut_relations
            .saturating_add(self.relation_catalogs.len())
            .saturating_add(self.standing_runtime_checkpoint.is_some() as usize);
        if lookups == 0 {
            return Err(MetaStoreError::EmptyField { field: "lookups" });
        }
        if lookups > MAX_DEMAND_METADATA_BATCH_LOOKUPS {
            return Err(MetaStoreError::DemandMetadataBatchTooLarge {
                actual: lookups,
                max: MAX_DEMAND_METADATA_BATCH_LOOKUPS,
            });
        }
        if let Some(source_cuts) = &self.source_cuts {
            source_cuts.validate()?;
        }
        let mut catalogs = BTreeSet::new();
        for lookup in &self.relation_catalogs {
            require_non_empty("relation_id", &lookup.relation_id)?;
            require_non_empty("relation_version", &lookup.relation_version)?;
            if !catalogs.insert((&lookup.relation_id, &lookup.relation_version)) {
                return Err(MetaStoreError::DuplicateDemandMetadataLookup {
                    kind: "relation_catalog",
                });
            }
        }
        if let Some(checkpoint) = &self.standing_runtime_checkpoint {
            require_non_empty("tenant_id", &checkpoint.tenant_id)?;
            require_non_empty("program_id", &checkpoint.program_id)?;
            require_non_empty("view_id", &checkpoint.view_id)?;
        }
        Ok(())
    }
}

impl ReadDemandMetadataBatchResponse {
    fn validate_for(&self, request: &ReadDemandMetadataBatchRequest) -> Result<(), MetaStoreError> {
        if self.relation_catalogs.len() != request.relation_catalogs.len()
            || self.source_cuts.is_some() != request.source_cuts.is_some()
            || self
                .source_cuts
                .as_ref()
                .zip(request.source_cuts.as_ref())
                .is_some_and(|(actual, requested)| actual.len() != requested.relations.len())
            || self
                .source_cuts
                .as_ref()
                .zip(request.source_cuts.as_ref())
                .is_some_and(|(actual, requested)| {
                    actual
                        .iter()
                        .zip(&requested.relations)
                        .any(|(cut, relation)| {
                            &cut.relation != relation
                                || cut.cut.namespace != requested.namespace
                                || cut.cut.relation_id != relation.relation_id
                        })
                })
            || (request.standing_runtime_checkpoint.is_none()
                && self.standing_runtime_checkpoint.is_some())
        {
            return Err(MetaStoreError::Serialization(
                "demand metadata batch response shape does not match request".into(),
            ));
        }
        for (catalog, lookup) in self
            .relation_catalogs
            .iter()
            .zip(&request.relation_catalogs)
        {
            if catalog.relation_schema.relation_id != lookup.relation_id
                || catalog.relation_schema.relation_version != lookup.relation_version
            {
                return Err(MetaStoreError::Serialization(
                    "demand metadata batch returned a mismatched relation catalog".into(),
                ));
            }
        }
        if let (Some(checkpoint), Some(lookup)) = (
            &self.standing_runtime_checkpoint,
            &request.standing_runtime_checkpoint,
        ) {
            if checkpoint.tenant_id != lookup.tenant_id
                || checkpoint.program_id != lookup.program_id
                || checkpoint.view_id != lookup.view_id
            {
                return Err(MetaStoreError::Serialization(
                    "demand metadata batch returned a mismatched checkpoint pointer".into(),
                ));
            }
        }
        Ok(())
    }
}

pub(crate) async fn read_demand_metadata_batch_from_state(
    store: InMemoryMetaStore,
    request: ReadDemandMetadataBatchRequest,
) -> Result<ReadDemandMetadataBatchResponse, MetaStoreError> {
    request.validate()?;
    let guard = store.inner.read().await;
    let source_cuts = match request.source_cuts {
        Some(source_request) => {
            for relation in &source_request.relations {
                let catalog = guard
                    .relation_catalogs
                    .get(&(
                        relation.relation_id.clone(),
                        relation.relation_version.clone(),
                    ))
                    .ok_or_else(|| MetaStoreError::RelationCatalogNotFound {
                        relation_id: relation.relation_id.clone(),
                        relation_version: relation.relation_version.clone(),
                    })?;
                if catalog.schema_fingerprint.as_str() != relation.schema_fingerprint.as_str() {
                    return Err(MetaStoreError::Serialization(
                        "demand source-cut fingerprint does not match registered relation catalog"
                            .into(),
                    ));
                }
            }
            // Retain same-scope fingerprint mismatches for the builder to reject.
            let is_requested = |reservation: &IngestRangeReservation| {
                source_request.relations.iter().any(|relation| {
                    relation.relation_id == reservation.relation_id
                        && relation.relation_version == reservation.relation_version
                })
            };
            let reservations = guard
                .relation_ingest_reservations
                .values()
                .flatten()
                .filter_map(|reservation| {
                    let authority = guard.relation_authority_reservation_keys.get(reservation)?;
                    (authority.namespace == source_request.namespace && is_requested(reservation))
                        .then(|| (authority.clone(), reservation.clone()))
                })
                .collect::<Vec<_>>();
            let publications = guard
                .relation_authoritative_ingest_publications
                .values()
                .filter(|publication| {
                    publication.authority_key.namespace == source_request.namespace
                        && is_requested(&publication.reservation)
                })
                .cloned()
                .collect::<Vec<_>>();
            Some(source_cut::build_relation_ingest_source_cuts(
                &source_request,
                reservations,
                publications,
            )?)
        }
        None => None,
    };
    let mut relation_catalogs = Vec::with_capacity(request.relation_catalogs.len());
    for lookup in request.relation_catalogs {
        relation_catalogs.push(
            guard
                .relation_catalogs
                .get(&(lookup.relation_id.clone(), lookup.relation_version.clone()))
                .cloned()
                .ok_or(MetaStoreError::RelationCatalogNotFound {
                    relation_id: lookup.relation_id,
                    relation_version: lookup.relation_version,
                })?,
        );
    }
    let standing_runtime_checkpoint = match request.standing_runtime_checkpoint {
        Some(lookup) => {
            validate_standing_runtime_scope(
                &lookup.tenant_id,
                &lookup.program_id,
                &lookup.view_id,
            )?;
            guard
                .standing_runtime_checkpoints
                .get(&(lookup.tenant_id, lookup.program_id, lookup.view_id))
                .cloned()
        }
        None => None,
    };
    Ok(ReadDemandMetadataBatchResponse {
        source_cuts,
        relation_catalogs,
        standing_runtime_checkpoint,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PublishStandingRuntimeCheckpointOutcome {
    Published,
    Duplicate,
    Conflict,
    /// The requested source cut no longer describes the authoritative
    /// relation-ingest state.  This is intentionally distinct from pointer
    /// predecessor CAS conflict so callers can recapture and retry safely.
    SourceCutChanged,
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
    #[error("relation source identity generation `{generation}` is unsupported")]
    UnsupportedRelationGeneration { generation: u64 },
    #[error("metadata timestamp overflow")]
    TimestampOverflow,
    #[error("metadata snapshot exceeds the existing 8 MiB Rhiza KV limit")]
    SnapshotTooLarge,
    #[error("active-view registry exceeds the existing 8 MiB gRPC snapshot limit")]
    ActiveViewRegistryTooLarge,
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
    #[error("duplicate demand metadata lookup of kind `{kind}`")]
    DuplicateDemandMetadataLookup { kind: &'static str },
    #[error("demand metadata batch has {actual} lookups; maximum is {max}")]
    DemandMetadataBatchTooLarge { actual: usize, max: usize },
    #[error("overlapping source-cut ranges for {stream_id}/p={partition_id}")]
    OverlappingSourceCutRange {
        stream_id: String,
        partition_id: u32,
    },
    #[error(
        "relation source cut incomplete for {relation_id}/{stream_id}/p={partition_id}: {reason}"
    )]
    IncompleteRelationSourceCut {
        relation_id: String,
        stream_id: String,
        partition_id: u32,
        reason: &'static str,
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
    #[error("rhiza metadata store error: {0}")]
    Rhiza(String),
    #[error("rhiza metadata mutation {request_id} has indeterminate commit state: {detail}")]
    RhizaIndeterminate { request_id: String, detail: String },
    #[error("rhiza metadata CAS contention after {attempts} attempts")]
    RhizaContention { attempts: usize },
    #[error("active view record identity does not match registry scope or requested view id")]
    ActiveViewRecordIdentityMismatch,
    #[error("active view registry import contains duplicate view id `{view_id}`")]
    DuplicateActiveViewId { view_id: String },
    #[error("active view registry contains duplicate normalized API path `{normalized_url_path}`")]
    DuplicateActiveViewApiPath { normalized_url_path: String },
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

    async fn read_active_view_registry_snapshot(
        &self,
        request: ActiveViewRegistryReadRequestV1,
    ) -> Result<ActiveViewRegistrySnapshotV1, MetaStoreError> {
        validate_active_view_scope(&request.scope)?;
        Err(MetaStoreError::UnsupportedCapability(
            "active_view_registry",
        ))
    }

    async fn create_active_view_record(
        &self,
        request: CreateActiveViewRecordRequestV1,
    ) -> Result<CreateActiveViewRecordOutcomeV1, MetaStoreError> {
        validate_active_view_scope(&request.scope)?;
        validate_active_view_record(&request.scope, &request.record, None)?;
        Err(MetaStoreError::UnsupportedCapability(
            "active_view_registry",
        ))
    }

    async fn compare_exchange_active_view_record(
        &self,
        request: CompareExchangeActiveViewRecordRequestV1,
    ) -> Result<CompareExchangeActiveViewRecordOutcomeV1, MetaStoreError> {
        validate_active_view_scope(&request.scope)?;
        validate_active_view_record(&request.scope, &request.record, Some(&request.view_id))?;
        Err(MetaStoreError::UnsupportedCapability(
            "active_view_registry",
        ))
    }

    async fn import_active_view_registry(
        &self,
        request: ImportActiveViewRegistryRequestV1,
    ) -> Result<ImportActiveViewRegistryOutcomeV1, MetaStoreError> {
        validate_active_view_scope(&request.scope)?;
        validate_active_view_records(&request.scope, &request.records)?;
        Err(MetaStoreError::UnsupportedCapability(
            "active_view_registry",
        ))
    }

    async fn promote_view_bootstrap_with_active_view_record(
        &self,
        request: PromoteViewBootstrapWithActiveViewRecordRequestV1,
    ) -> Result<PromoteViewBootstrapOutcome, MetaStoreError> {
        request.bootstrap.validate()?;
        validate_active_view_scope(&request.scope)?;
        validate_active_view_record(
            &request.scope,
            &request.record,
            Some(&request.bootstrap.view_id),
        )?;
        Err(MetaStoreError::UnsupportedCapability(
            "active_view_registry",
        ))
    }

    /// Reads the fixed demand-start metadata set from one pinned state snapshot.
    async fn read_demand_metadata_batch(
        &self,
        request: ReadDemandMetadataBatchRequest,
    ) -> Result<ReadDemandMetadataBatchResponse, MetaStoreError> {
        request.validate()?;
        Err(MetaStoreError::UnsupportedCapability(
            "demand_metadata_read_batch",
        ))
    }

    async fn reserve_ingest_range(
        &self,
        reservation: IngestRangeReservation,
    ) -> Result<ReserveIngestRangeOutcome, MetaStoreError>;

    async fn reserve_authoritative_ingest_range(
        &self,
        request: ReserveAuthoritativeIngestRangeRequest,
    ) -> Result<ReserveIngestRangeOutcome, MetaStoreError> {
        request.validate()?;
        Err(MetaStoreError::UnsupportedCapability(
            "authoritative_ingest_publication",
        ))
    }

    async fn commit_ingest_range(
        &self,
        reservation: IngestRangeReservation,
    ) -> Result<CommitIngestRangeOutcome, MetaStoreError> {
        reservation.validate()?;
        Err(MetaStoreError::UnsupportedCapability(
            "committed_ingest_source_cut",
        ))
    }

    /// Atomically validates a reservation and its partition authority, then
    /// records the authoritative object reference.  There is deliberately no
    /// fallback to `commit_ingest_range`: that would leave a stale writer able
    /// to publish an object after losing its partition authority.
    async fn publish_ingest_reservation(
        &self,
        request: PublishIngestReservationRequest,
    ) -> Result<PublishIngestReservationOutcome, MetaStoreError> {
        request.validate()?;
        Err(MetaStoreError::UnsupportedCapability(
            "authoritative_ingest_publication",
        ))
    }

    async fn read_authoritative_ingest_publication(
        &self,
        _request_id: &str,
    ) -> Result<Option<AuthoritativeIngestPublication>, MetaStoreError> {
        Err(MetaStoreError::UnsupportedCapability(
            "authoritative_ingest_publication",
        ))
    }

    async fn list_authoritative_ingest_publications(
        &self,
        _key: &PartitionAuthorityKey,
    ) -> Result<Vec<AuthoritativeIngestPublication>, MetaStoreError> {
        Err(MetaStoreError::UnsupportedCapability(
            "authoritative_ingest_publication",
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

    async fn read_relation_ingest_capability(
        &self,
    ) -> Result<RelationIngestCapability, MetaStoreError> {
        Ok(RelationIngestCapability::default())
    }

    async fn capture_relation_ingest_source_cut(
        &self,
        request: CaptureRelationIngestSourceCutRequest,
    ) -> Result<RelationIngestSourceCutV1, MetaStoreError> {
        request.validate()?;
        Err(MetaStoreError::UnsupportedCapability(
            "relation_committed_ingest_source_cut",
        ))
    }

    async fn capture_relation_ingest_source_cuts(
        &self,
        request: CaptureRelationIngestSourceCutsRequest,
    ) -> Result<Vec<RelationIngestSourceIdentityCutV1>, MetaStoreError> {
        request.validate()?;
        Err(MetaStoreError::UnsupportedCapability(
            "relation_ingest_source_cuts",
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

    async fn acquire_standing_runtime_owner_with_endpoint(
        &self,
        _request: AcquireStandingRuntimeOwnerRequest,
        _owner_endpoint: String,
    ) -> Result<AcquireStandingRuntimeOwnerOutcome, MetaStoreError> {
        Err(MetaStoreError::UnsupportedCapability(
            "standing_runtime_owner_endpoint",
        ))
    }

    /// Creates transport bound to the calling executor for independent renewal.
    /// In-process stores need no replacement. Transport-backed wrappers must
    /// forward this hook so renewal does not depend on the request executor.
    async fn isolated_owner_renewal_store(
        &self,
    ) -> Result<Option<Arc<dyn MetaStore>>, MetaStoreError> {
        Ok(None)
    }

    /// Extends only this exact unexpired epoch; never reacquires lost ownership.
    async fn renew_standing_runtime_owner(
        &self,
        _owner: StandingRuntimeOwnerToken,
        _ttl_ms: u64,
    ) -> Result<StandingRuntimeOwnerClaim, MetaStoreError> {
        Err(MetaStoreError::UnsupportedCapability(
            "conditional_standing_runtime_owner_renewal",
        ))
    }

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

    async fn acquire_relation_partition_authority(
        &self,
        request: AcquireRelationPartitionAuthorityRequest,
    ) -> Result<AcquireRelationPartitionAuthorityOutcome, MetaStoreError> {
        request.validate()?;
        Err(MetaStoreError::UnsupportedCapability(
            "relation_partition_authority",
        ))
    }

    async fn read_relation_partition_authority(
        &self,
        key: &RelationPartitionAuthorityKey,
    ) -> Result<Option<RelationPartitionAuthorityToken>, MetaStoreError> {
        key.validate()?;
        Err(MetaStoreError::UnsupportedCapability(
            "relation_partition_authority",
        ))
    }

    async fn reserve_relation_authoritative_ingest_range(
        &self,
        request: ReserveRelationAuthoritativeIngestRangeRequest,
    ) -> Result<ReserveIngestRangeOutcome, MetaStoreError> {
        request.validate()?;
        Err(MetaStoreError::UnsupportedCapability(
            "relation_authoritative_ingest_publication",
        ))
    }

    async fn publish_relation_ingest_reservation(
        &self,
        request: PublishRelationIngestReservationRequest,
    ) -> Result<PublishIngestReservationOutcome, MetaStoreError> {
        request.validate()?;
        Err(MetaStoreError::UnsupportedCapability(
            "relation_authoritative_ingest_publication",
        ))
    }

    async fn read_relation_authoritative_ingest_publication(
        &self,
        request_id: &str,
    ) -> Result<Option<RelationAuthoritativeIngestPublication>, MetaStoreError> {
        require_non_empty("request_id", request_id)?;
        Err(MetaStoreError::UnsupportedCapability(
            "relation_authoritative_ingest_publication",
        ))
    }

    async fn list_relation_authoritative_ingest_publications(
        &self,
        key: &RelationPartitionAuthorityKey,
    ) -> Result<Vec<RelationAuthoritativeIngestPublication>, MetaStoreError> {
        key.validate()?;
        Err(MetaStoreError::UnsupportedCapability(
            "relation_authoritative_ingest_publication",
        ))
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

    async fn read_active_view_registry_snapshot(
        &self,
        request: ActiveViewRegistryReadRequestV1,
    ) -> Result<ActiveViewRegistrySnapshotV1, MetaStoreError> {
        (**self).read_active_view_registry_snapshot(request).await
    }

    async fn create_active_view_record(
        &self,
        request: CreateActiveViewRecordRequestV1,
    ) -> Result<CreateActiveViewRecordOutcomeV1, MetaStoreError> {
        (**self).create_active_view_record(request).await
    }

    async fn compare_exchange_active_view_record(
        &self,
        request: CompareExchangeActiveViewRecordRequestV1,
    ) -> Result<CompareExchangeActiveViewRecordOutcomeV1, MetaStoreError> {
        (**self).compare_exchange_active_view_record(request).await
    }

    async fn import_active_view_registry(
        &self,
        request: ImportActiveViewRegistryRequestV1,
    ) -> Result<ImportActiveViewRegistryOutcomeV1, MetaStoreError> {
        (**self).import_active_view_registry(request).await
    }

    async fn promote_view_bootstrap_with_active_view_record(
        &self,
        request: PromoteViewBootstrapWithActiveViewRecordRequestV1,
    ) -> Result<PromoteViewBootstrapOutcome, MetaStoreError> {
        (**self)
            .promote_view_bootstrap_with_active_view_record(request)
            .await
    }

    async fn read_demand_metadata_batch(
        &self,
        request: ReadDemandMetadataBatchRequest,
    ) -> Result<ReadDemandMetadataBatchResponse, MetaStoreError> {
        (**self).read_demand_metadata_batch(request).await
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

    async fn reserve_authoritative_ingest_range(
        &self,
        request: ReserveAuthoritativeIngestRangeRequest,
    ) -> Result<ReserveIngestRangeOutcome, MetaStoreError> {
        (**self).reserve_authoritative_ingest_range(request).await
    }

    async fn commit_ingest_range(
        &self,
        reservation: IngestRangeReservation,
    ) -> Result<CommitIngestRangeOutcome, MetaStoreError> {
        (**self).commit_ingest_range(reservation).await
    }

    async fn publish_ingest_reservation(
        &self,
        request: PublishIngestReservationRequest,
    ) -> Result<PublishIngestReservationOutcome, MetaStoreError> {
        (**self).publish_ingest_reservation(request).await
    }

    async fn read_authoritative_ingest_publication(
        &self,
        request_id: &str,
    ) -> Result<Option<AuthoritativeIngestPublication>, MetaStoreError> {
        (**self)
            .read_authoritative_ingest_publication(request_id)
            .await
    }

    async fn list_authoritative_ingest_publications(
        &self,
        key: &PartitionAuthorityKey,
    ) -> Result<Vec<AuthoritativeIngestPublication>, MetaStoreError> {
        (**self).list_authoritative_ingest_publications(key).await
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

    async fn read_relation_ingest_capability(
        &self,
    ) -> Result<RelationIngestCapability, MetaStoreError> {
        (**self).read_relation_ingest_capability().await
    }

    async fn capture_relation_ingest_source_cuts(
        &self,
        request: CaptureRelationIngestSourceCutsRequest,
    ) -> Result<Vec<RelationIngestSourceIdentityCutV1>, MetaStoreError> {
        (**self).capture_relation_ingest_source_cuts(request).await
    }

    async fn capture_relation_ingest_source_cut(
        &self,
        request: CaptureRelationIngestSourceCutRequest,
    ) -> Result<RelationIngestSourceCutV1, MetaStoreError> {
        (**self).capture_relation_ingest_source_cut(request).await
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

    async fn acquire_standing_runtime_owner_with_endpoint(
        &self,
        request: AcquireStandingRuntimeOwnerRequest,
        owner_endpoint: String,
    ) -> Result<AcquireStandingRuntimeOwnerOutcome, MetaStoreError> {
        (**self)
            .acquire_standing_runtime_owner_with_endpoint(request, owner_endpoint)
            .await
    }

    async fn renew_standing_runtime_owner(
        &self,
        owner: StandingRuntimeOwnerToken,
        ttl_ms: u64,
    ) -> Result<StandingRuntimeOwnerClaim, MetaStoreError> {
        (**self).renew_standing_runtime_owner(owner, ttl_ms).await
    }

    async fn isolated_owner_renewal_store(
        &self,
    ) -> Result<Option<Arc<dyn MetaStore>>, MetaStoreError> {
        (**self).isolated_owner_renewal_store().await
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

    async fn acquire_relation_partition_authority(
        &self,
        request: AcquireRelationPartitionAuthorityRequest,
    ) -> Result<AcquireRelationPartitionAuthorityOutcome, MetaStoreError> {
        (**self).acquire_relation_partition_authority(request).await
    }

    async fn read_relation_partition_authority(
        &self,
        key: &RelationPartitionAuthorityKey,
    ) -> Result<Option<RelationPartitionAuthorityToken>, MetaStoreError> {
        (**self).read_relation_partition_authority(key).await
    }

    async fn reserve_relation_authoritative_ingest_range(
        &self,
        request: ReserveRelationAuthoritativeIngestRangeRequest,
    ) -> Result<ReserveIngestRangeOutcome, MetaStoreError> {
        (**self)
            .reserve_relation_authoritative_ingest_range(request)
            .await
    }

    async fn publish_relation_ingest_reservation(
        &self,
        request: PublishRelationIngestReservationRequest,
    ) -> Result<PublishIngestReservationOutcome, MetaStoreError> {
        (**self).publish_relation_ingest_reservation(request).await
    }

    async fn read_relation_authoritative_ingest_publication(
        &self,
        request_id: &str,
    ) -> Result<Option<RelationAuthoritativeIngestPublication>, MetaStoreError> {
        (**self)
            .read_relation_authoritative_ingest_publication(request_id)
            .await
    }

    async fn list_relation_authoritative_ingest_publications(
        &self,
        key: &RelationPartitionAuthorityKey,
    ) -> Result<Vec<RelationAuthoritativeIngestPublication>, MetaStoreError> {
        (**self)
            .list_relation_authoritative_ingest_publications(key)
            .await
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
    evaluation_now_unix_ms: Option<u64>,
}

#[derive(Clone, Default, Serialize, Deserialize)]
struct InMemoryMetaState {
    #[serde(with = "crate::rhiza_snapshot::map_pairs")]
    relation_catalogs: HashMap<(String, String), VelorixRelationCatalogV1>,
    #[serde(with = "crate::rhiza_snapshot::map_pairs")]
    ingest_reservations: HashMap<(String, u32), Vec<IngestRangeReservation>>,
    #[serde(with = "crate::rhiza_snapshot::map_pairs")]
    legacy_batch_keys: HashMap<String, IngestRangeReservation>,
    #[serde(with = "crate::rhiza_snapshot::map_pairs")]
    authoritative_ingest_reservation_keys: HashMap<IngestRangeReservation, PartitionAuthorityKey>,
    committed_ingest_batch_keys: BTreeSet<String>,
    #[serde(with = "crate::rhiza_snapshot::map_pairs")]
    authoritative_ingest_publications: HashMap<String, InMemoryIngestPublication>,
    ingest_catalog_epoch: u64,
    #[serde(with = "crate::rhiza_snapshot::map_pairs")]
    view_bootstraps: HashMap<(String, String, String), ViewBootstrapControlV1>,
    #[serde(default, with = "crate::rhiza_snapshot::map_pairs")]
    active_view_registries: HashMap<ActiveViewRegistryScopeV1, ActiveViewRegistryStateV1>,
    #[serde(with = "crate::rhiza_snapshot::map_pairs")]
    view_dependency_graph_revisions: HashMap<String, u64>,
    #[serde(with = "crate::rhiza_snapshot::map_pairs")]
    standing_runtime_owners: HashMap<(String, String, String), StandingRuntimeOwnerClaim>,
    #[serde(with = "crate::rhiza_snapshot::map_pairs")]
    standing_runtime_checkpoints:
        HashMap<(String, String, String), StandingRuntimeCheckpointPointer>,
    partition_authority_now_unix_ms: u64,
    #[serde(with = "crate::rhiza_snapshot::map_pairs")]
    partition_authorities: HashMap<PartitionAuthorityKey, PartitionAuthorityToken>,
    #[serde(with = "crate::rhiza_snapshot::map_pairs")]
    partition_checkpoint_pointers: HashMap<PartitionAuthorityKey, PartitionCheckpointPointer>,
    #[serde(with = "crate::rhiza_snapshot::map_pairs")]
    relation_partition_authorities:
        HashMap<RelationPartitionAuthorityKey, RelationPartitionAuthorityToken>,
    #[serde(with = "crate::rhiza_snapshot::map_pairs")]
    relation_ingest_reservations: HashMap<(String, u32), Vec<IngestRangeReservation>>,
    #[serde(with = "crate::rhiza_snapshot::map_pairs")]
    relation_authority_reservation_keys:
        HashMap<IngestRangeReservation, RelationPartitionAuthorityKey>,
    #[serde(with = "crate::rhiza_snapshot::map_pairs")]
    relation_batch_keys: HashMap<String, IngestRangeReservation>,
    #[serde(with = "crate::rhiza_snapshot::map_pairs")]
    relation_authoritative_ingest_publications:
        HashMap<String, RelationAuthoritativeIngestPublication>,
    #[serde(with = "crate::rhiza_snapshot::map_pairs")]
    relation_reservation_publications: HashMap<IngestRangeReservation, String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct InMemoryIngestPublication {
    publication: AuthoritativeIngestPublication,
}

impl InMemoryMetaStore {
    async fn acquire_owner_claim(
        &self,
        request: AcquireStandingRuntimeOwnerRequest,
        owner_endpoint: Option<String>,
    ) -> Result<AcquireStandingRuntimeOwnerOutcome, MetaStoreError> {
        request.validate()?;
        let now = self.evaluation_time_ms()?;
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
                    owner_endpoint: owner_endpoint.or(current.owner_endpoint),
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
                    owner_endpoint,
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
                    owner_endpoint,
                    owner_epoch: 1,
                    expires_at_unix_ms,
                };
                guard.standing_runtime_owners.insert(key, claim.clone());
                Ok(AcquireStandingRuntimeOwnerOutcome::Acquired(claim))
            }
        }
    }

    #[cfg(any(test, feature = "rhiza-backend"))]
    pub(crate) fn from_state_for_evaluation(state: InMemoryMetaState, now_unix_ms: u64) -> Self {
        let mut state = state;
        state.partition_authority_now_unix_ms = now_unix_ms;
        Self {
            inner: Arc::new(RwLock::new(state)),
            evaluation_now_unix_ms: Some(now_unix_ms),
        }
    }

    #[cfg(feature = "rhiza-backend")]
    pub(crate) fn from_shared_state_for_evaluation(
        inner: Arc<RwLock<InMemoryMetaState>>,
        now_unix_ms: u64,
    ) -> Self {
        Self {
            inner,
            evaluation_now_unix_ms: Some(now_unix_ms),
        }
    }

    #[cfg(any(test, feature = "rhiza-backend"))]
    pub(crate) async fn snapshot_state(&self) -> InMemoryMetaState {
        self.inner.read().await.clone()
    }

    fn evaluation_time_ms(&self) -> Result<u64, MetaStoreError> {
        self.evaluation_now_unix_ms.map_or_else(unix_time_ms, Ok)
    }

    async fn authority_read_time_ms(&self) -> u64 {
        match self.evaluation_now_unix_ms {
            Some(now_unix_ms) => now_unix_ms,
            None => self.inner.read().await.partition_authority_now_unix_ms,
        }
    }

    /// Controls the in-memory backend clock for deterministic authority tests.
    /// Production callers never provide time through the authority API.
    pub async fn set_partition_authority_clock_for_test(&self, now_unix_ms: u64) {
        self.inner.write().await.partition_authority_now_unix_ms = now_unix_ms;
    }

    #[doc(hidden)]
    pub async fn expire_standing_runtime_owner_for_test(
        &self,
        tenant_id: &str,
        program_id: &str,
        view_id: &str,
    ) -> Result<(), MetaStoreError> {
        validate_standing_runtime_scope(tenant_id, program_id, view_id)?;
        let key = standing_runtime_owner_scope_key(tenant_id, program_id, view_id);
        let mut state = self.inner.write().await;
        let owner = state
            .standing_runtime_owners
            .get_mut(&key)
            .ok_or(MetaStoreError::StandingRuntimeOwnerMismatch)?;
        owner.expires_at_unix_ms = 0;
        Ok(())
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
            relation_ingest: RelationIngestCapability {
                backend_name: "in-memory".into(),
                relation_scoped_authority: true,
                committed_publication_source_cut: true,
                durable_across_restart: false,
            },
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

    async fn read_active_view_registry_snapshot(
        &self,
        request: ActiveViewRegistryReadRequestV1,
    ) -> Result<ActiveViewRegistrySnapshotV1, MetaStoreError> {
        validate_active_view_scope(&request.scope)?;
        let guard = self.inner.read().await;
        let registry = guard.active_view_registries.get(&request.scope);
        if let Some(registry) = registry {
            for (view_id, version) in &registry.records {
                validate_active_view_record(&request.scope, &version.record, Some(view_id))?;
            }
            validate_active_view_registry(registry)?;
        }
        let mut records: Vec<_> = registry
            .into_iter()
            .flat_map(|registry| registry.records.values().cloned())
            .collect();
        records.sort_by(|left, right| left.record.view_id.cmp(&right.record.view_id));
        Ok(ActiveViewRegistrySnapshotV1 {
            snapshot_revision: registry.map_or(0, |registry| registry.snapshot_revision),
            imported: registry.is_some_and(|registry| registry.initialized),
            records,
        })
    }

    async fn create_active_view_record(
        &self,
        request: CreateActiveViewRecordRequestV1,
    ) -> Result<CreateActiveViewRecordOutcomeV1, MetaStoreError> {
        validate_active_view_scope(&request.scope)?;
        validate_active_view_record(&request.scope, &request.record, None)?;
        let mut guard = self.inner.write().await;
        let mut candidate = guard
            .active_view_registries
            .get(&request.scope)
            .cloned()
            .unwrap_or_default();
        if let Some(existing) = candidate.records.get(&request.record.view_id) {
            return Ok(if existing.record == request.record {
                CreateActiveViewRecordOutcomeV1::Duplicate(existing.clone())
            } else {
                CreateActiveViewRecordOutcomeV1::Conflict
            });
        }
        let generation = 1;
        let version = ActiveViewRecordVersionV1 {
            generation,
            record: request.record,
        };
        candidate.snapshot_revision = candidate
            .snapshot_revision
            .checked_add(1)
            .ok_or(MetaStoreError::TimestampOverflow)?;
        candidate.initialized = true;
        candidate
            .records
            .insert(version.record.view_id.clone(), version.clone());
        if let Err(error) = validate_active_view_registry(&candidate) {
            if matches!(error, MetaStoreError::DuplicateActiveViewApiPath { .. }) {
                return Ok(CreateActiveViewRecordOutcomeV1::Conflict);
            }
            return Err(error);
        }
        guard
            .active_view_registries
            .insert(request.scope, candidate);
        Ok(CreateActiveViewRecordOutcomeV1::Created(version))
    }

    async fn compare_exchange_active_view_record(
        &self,
        request: CompareExchangeActiveViewRecordRequestV1,
    ) -> Result<CompareExchangeActiveViewRecordOutcomeV1, MetaStoreError> {
        validate_active_view_scope(&request.scope)?;
        validate_active_view_record(&request.scope, &request.record, Some(&request.view_id))?;
        let mut guard = self.inner.write().await;
        let Some(current) = guard.active_view_registries.get(&request.scope) else {
            return Ok(CompareExchangeActiveViewRecordOutcomeV1::Conflict);
        };
        let Some(existing) = current.records.get(&request.view_id) else {
            return Ok(CompareExchangeActiveViewRecordOutcomeV1::Conflict);
        };
        if existing.record == request.record {
            return Ok(CompareExchangeActiveViewRecordOutcomeV1::Duplicate(
                existing.clone(),
            ));
        }
        if existing.generation != request.expected_generation {
            return Ok(CompareExchangeActiveViewRecordOutcomeV1::Conflict);
        }
        if (existing.record.runtime.is_some()
            || existing.record.execution_mode
                == Some(
                    velorix_storage::materialized_view_registry::MaterializedViewExecutionMode::StandingRuntime,
                ))
            && existing.record.lifecycle != request.record.lifecycle
        {
            let Some(owner) = request.owner.as_ref() else {
                return Err(MetaStoreError::StandingRuntimeOwnerMismatch);
            };
            let Some(runtime) = existing.record.runtime.as_ref() else {
                return Err(MetaStoreError::StandingRuntimeOwnerMismatch);
            };
            let identity = &runtime.standing_program_identity;
            if owner.tenant_id != identity.tenant_id
                || owner.program_id != identity.program_id
                || owner.view_id != request.view_id
                || !identity
                    .view_ids
                    .iter()
                    .any(|view_id| view_id == &request.view_id)
            {
                return Err(MetaStoreError::StandingRuntimeOwnerMismatch);
            }
            let owner_key = (
                identity.tenant_id.clone(),
                identity.program_id.clone(),
                request.view_id.clone(),
            );
            validate_current_standing_runtime_owner(
                guard.standing_runtime_owners.get(&owner_key),
                owner,
                self.evaluation_time_ms()?,
            )?;
        }
        let version = ActiveViewRecordVersionV1 {
            generation: existing
                .generation
                .checked_add(1)
                .ok_or(MetaStoreError::TimestampOverflow)?,
            record: request.record,
        };
        let mut candidate = current.clone();
        candidate.snapshot_revision = candidate
            .snapshot_revision
            .checked_add(1)
            .ok_or(MetaStoreError::TimestampOverflow)?;
        candidate
            .records
            .insert(request.view_id.clone(), version.clone());
        if let Err(error) = validate_active_view_registry(&candidate) {
            if matches!(error, MetaStoreError::DuplicateActiveViewApiPath { .. }) {
                return Ok(CompareExchangeActiveViewRecordOutcomeV1::Conflict);
            }
            return Err(error);
        }
        guard
            .active_view_registries
            .insert(request.scope, candidate);
        Ok(CompareExchangeActiveViewRecordOutcomeV1::Updated(version))
    }

    async fn import_active_view_registry(
        &self,
        request: ImportActiveViewRegistryRequestV1,
    ) -> Result<ImportActiveViewRegistryOutcomeV1, MetaStoreError> {
        validate_active_view_scope(&request.scope)?;
        validate_active_view_records(&request.scope, &request.records)?;
        let mut guard = self.inner.write().await;
        if let Some(existing) = guard.active_view_registries.get(&request.scope) {
            let same = existing.initialized
                && existing.records.len() == request.records.len()
                && request.records.iter().all(|record| {
                    existing
                        .records
                        .get(&record.view_id)
                        .is_some_and(|entry| entry.generation == 1 && entry.record == *record)
                });
            return Ok(if same {
                ImportActiveViewRegistryOutcomeV1::Duplicate
            } else {
                ImportActiveViewRegistryOutcomeV1::Conflict
            });
        }
        let records: HashMap<_, _> = request
            .records
            .into_iter()
            .map(|record| {
                let id = record.view_id.clone();
                (
                    id,
                    ActiveViewRecordVersionV1 {
                        generation: 1,
                        record,
                    },
                )
            })
            .collect();
        let candidate = ActiveViewRegistryStateV1 {
            initialized: true,
            snapshot_revision: 1,
            records,
        };
        validate_active_view_registry(&candidate)?;
        guard
            .active_view_registries
            .insert(request.scope, candidate);
        Ok(ImportActiveViewRegistryOutcomeV1::Imported)
    }

    async fn read_demand_metadata_batch(
        &self,
        request: ReadDemandMetadataBatchRequest,
    ) -> Result<ReadDemandMetadataBatchResponse, MetaStoreError> {
        read_demand_metadata_batch_from_state(self.clone(), request).await
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
        let exact = guard
            .ingest_reservations
            .get(&key)
            .is_some_and(|reservations| {
                reservations.iter().any(|existing| existing == &reservation)
            });
        if exact {
            return Ok(ReserveIngestRangeOutcome::Duplicate);
        }
        if guard
            .ingest_reservations
            .get(&key)
            .is_some_and(|reservations| {
                reservations
                    .iter()
                    .any(|existing| existing.overlaps(&reservation))
            })
        {
            return Ok(ReserveIngestRangeOutcome::Conflict);
        }

        guard
            .legacy_batch_keys
            .insert(reservation.batch_key.clone(), reservation.clone());
        let reservations = guard.ingest_reservations.entry(key).or_default();
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

    async fn reserve_authoritative_ingest_range(
        &self,
        request: ReserveAuthoritativeIngestRangeRequest,
    ) -> Result<ReserveIngestRangeOutcome, MetaStoreError> {
        request.validate()?;
        let mut guard = self.inner.write().await;
        let now = guard.partition_authority_now_unix_ms;
        if validate_current_partition_authority(
            guard.partition_authorities.get(&request.authority.key),
            &request.authority,
            now,
        )
        .is_err()
        {
            return Ok(ReserveIngestRangeOutcome::Conflict);
        }
        let key = (
            request.reservation.stream_id.clone(),
            request.reservation.partition_id,
        );
        let exact = guard
            .ingest_reservations
            .get(&key)
            .is_some_and(|reservations| {
                reservations
                    .iter()
                    .any(|entry| entry == &request.reservation)
            });
        if exact {
            return match guard
                .authoritative_ingest_reservation_keys
                .get(&request.reservation)
            {
                Some(existing) if existing == &request.authority.key => {
                    Ok(ReserveIngestRangeOutcome::Duplicate)
                }
                _ => Ok(ReserveIngestRangeOutcome::Conflict),
            };
        }
        let overlapping = guard
            .ingest_reservations
            .get(&key)
            .is_some_and(|reservations| {
                reservations
                    .iter()
                    .any(|entry| entry.overlaps(&request.reservation))
            });
        if overlapping {
            return Ok(ReserveIngestRangeOutcome::Conflict);
        }
        guard.legacy_batch_keys.insert(
            request.reservation.batch_key.clone(),
            request.reservation.clone(),
        );
        let reservations = guard.ingest_reservations.entry(key).or_default();
        reservations.push(request.reservation.clone());
        reservations.sort_by_key(|entry| entry.start_offset_inclusive);
        guard
            .authoritative_ingest_reservation_keys
            .insert(request.reservation, request.authority.key);
        Ok(ReserveIngestRangeOutcome::Reserved)
    }

    async fn publish_ingest_reservation(
        &self,
        request: PublishIngestReservationRequest,
    ) -> Result<PublishIngestReservationOutcome, MetaStoreError> {
        request.validate()?;
        let key = (
            request.reservation.stream_id.clone(),
            request.reservation.partition_id,
        );
        let mut guard = self.inner.write().await;
        let replay = InMemoryIngestPublication {
            publication: AuthoritativeIngestPublication {
                reservation: request.reservation.clone(),
                authority_key: request.authority.key.clone(),
                request_id: request.request_id.clone(),
                request_digest: request.request_digest.clone(),
                object_key: request.object_key.clone(),
                object_digest: request.object_digest.clone(),
            },
        };
        if let Some(existing) = guard
            .authoritative_ingest_publications
            .get(&request.request_id)
        {
            return Ok(if existing == &replay {
                PublishIngestReservationOutcome::Duplicate
            } else {
                PublishIngestReservationOutcome::Conflict
            });
        }
        let now = guard.partition_authority_now_unix_ms;
        if validate_current_partition_authority(
            guard.partition_authorities.get(&request.authority.key),
            &request.authority,
            now,
        )
        .is_err()
        {
            return Ok(PublishIngestReservationOutcome::InvalidAuthority);
        }
        if guard
            .authoritative_ingest_reservation_keys
            .get(&request.reservation)
            != Some(&request.authority.key)
        {
            return Ok(PublishIngestReservationOutcome::Conflict);
        }
        if !guard
            .ingest_reservations
            .get(&key)
            .is_some_and(|reservations| reservations.contains(&request.reservation))
        {
            return Ok(PublishIngestReservationOutcome::Conflict);
        }
        if guard
            .committed_ingest_batch_keys
            .contains(&request.reservation.batch_key)
        {
            return Ok(PublishIngestReservationOutcome::Conflict);
        }
        guard
            .committed_ingest_batch_keys
            .insert(request.reservation.batch_key.clone());
        guard
            .authoritative_ingest_publications
            .insert(request.request_id, replay);
        Ok(PublishIngestReservationOutcome::Committed)
    }

    async fn read_authoritative_ingest_publication(
        &self,
        request_id: &str,
    ) -> Result<Option<AuthoritativeIngestPublication>, MetaStoreError> {
        require_non_empty("request_id", request_id)?;
        Ok(self
            .inner
            .read()
            .await
            .authoritative_ingest_publications
            .get(request_id)
            .map(|publication| publication.publication.clone()))
    }

    async fn list_authoritative_ingest_publications(
        &self,
        key: &PartitionAuthorityKey,
    ) -> Result<Vec<AuthoritativeIngestPublication>, MetaStoreError> {
        key.validate()?;
        Ok(self
            .inner
            .read()
            .await
            .authoritative_ingest_publications
            .values()
            .filter(|publication| publication.publication.authority_key == *key)
            .map(|publication| publication.publication.clone())
            .collect())
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
            self.evaluation_time_ms()?,
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
            self.evaluation_time_ms()?,
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
        // Legacy Active records returned above remain valid without progress.
        // A pending derived record must be explicitly upgraded by the API from
        // verified full-baseline evidence; an empty physical cut is not proof.
        if guard.standing_runtime_checkpoints.get(&key) != Some(&request.checkpoint)
            || request.checkpoint.bootstrap_generation != control.bootstrap_generation
            || request.checkpoint.plan_hash != control.plan_hash
            || !view_bootstrap::checkpoint_covers_source_cut(&request.checkpoint, activation_cut)
            || (!control.view_inputs.is_empty()
                && !request
                    .checkpoint
                    .published_input_bootstrap
                    .as_ref()
                    .is_some_and(|progress| {
                        progress.matches_control(&control) && progress.is_complete()
                    }))
        {
            return Ok(PromoteViewBootstrapOutcome::Conflict);
        }
        control.lifecycle = ViewBootstrapLifecycleV1::Active;
        control.active_checkpoint = Some(request.checkpoint);
        guard.view_bootstraps.insert(key, control.clone());
        Ok(PromoteViewBootstrapOutcome::Promoted(control))
    }

    async fn promote_view_bootstrap_with_active_view_record(
        &self,
        request: PromoteViewBootstrapWithActiveViewRecordRequestV1,
    ) -> Result<PromoteViewBootstrapOutcome, MetaStoreError> {
        request.bootstrap.validate()?;
        validate_active_view_scope(&request.scope)?;
        validate_active_view_record(
            &request.scope,
            &request.record,
            Some(&request.bootstrap.view_id),
        )?;
        let bootstrap = request.bootstrap;
        let key = (
            bootstrap.tenant_id.clone(),
            bootstrap.program_id.clone(),
            bootstrap.view_id.clone(),
        );
        let mut guard = self.inner.write().await;
        validate_current_standing_runtime_owner(
            guard.standing_runtime_owners.get(&key),
            &bootstrap.owner,
            self.evaluation_time_ms()?,
        )?;
        let Some(mut control) = guard.view_bootstraps.get(&key).cloned() else {
            return Ok(PromoteViewBootstrapOutcome::Conflict);
        };
        if control.bootstrap_generation != bootstrap.bootstrap_generation
            || control.plan_hash != bootstrap.plan_hash
            || bootstrap.owner.tenant_id != bootstrap.tenant_id
            || bootstrap.owner.program_id != bootstrap.program_id
            || bootstrap.owner.view_id != bootstrap.view_id
        {
            return Ok(PromoteViewBootstrapOutcome::Conflict);
        }
        let registry = guard.active_view_registries.get(&request.scope);
        let Some(active) = registry.and_then(|r| r.records.get(&bootstrap.view_id)) else {
            return Ok(PromoteViewBootstrapOutcome::Conflict);
        };
        if active.generation != request.expected_generation {
            return Ok(PromoteViewBootstrapOutcome::Conflict);
        }
        if control.lifecycle == ViewBootstrapLifecycleV1::Active {
            return if control.active_checkpoint.as_ref() == Some(&bootstrap.checkpoint)
                && active.record == request.record
            {
                Ok(PromoteViewBootstrapOutcome::Duplicate(control))
            } else {
                Ok(PromoteViewBootstrapOutcome::Conflict)
            };
        }
        let Some(activation_cut) = control.activation_cut.as_ref() else {
            return Ok(PromoteViewBootstrapOutcome::Conflict);
        };
        if guard.standing_runtime_checkpoints.get(&key) != Some(&bootstrap.checkpoint)
            || bootstrap.checkpoint.bootstrap_generation != control.bootstrap_generation
            || bootstrap.checkpoint.plan_hash != control.plan_hash
            || !view_bootstrap::checkpoint_covers_source_cut(&bootstrap.checkpoint, activation_cut)
            || (!control.view_inputs.is_empty()
                && !bootstrap
                    .checkpoint
                    .published_input_bootstrap
                    .as_ref()
                    .is_some_and(|progress| {
                        progress.matches_control(&control) && progress.is_complete()
                    }))
        {
            return Ok(PromoteViewBootstrapOutcome::Conflict);
        }
        let mut candidate = guard
            .active_view_registries
            .get(&request.scope)
            .expect("registry was validated while holding the write lock")
            .clone();
        let active = candidate
            .records
            .get_mut(&bootstrap.view_id)
            .expect("active record was validated while holding the write lock");
        let generation = active
            .generation
            .checked_add(1)
            .ok_or(MetaStoreError::TimestampOverflow)?;
        candidate.snapshot_revision = candidate
            .snapshot_revision
            .checked_add(1)
            .ok_or(MetaStoreError::TimestampOverflow)?;
        *active = ActiveViewRecordVersionV1 {
            generation,
            record: request.record,
        };
        if let Err(error) = validate_active_view_registry(&candidate) {
            if matches!(error, MetaStoreError::DuplicateActiveViewApiPath { .. }) {
                return Ok(PromoteViewBootstrapOutcome::Conflict);
            }
            return Err(error);
        }
        guard
            .active_view_registries
            .insert(request.scope, candidate);
        control.lifecycle = ViewBootstrapLifecycleV1::Active;
        control.active_checkpoint = Some(bootstrap.checkpoint);
        guard.view_bootstraps.insert(key, control.clone());
        Ok(PromoteViewBootstrapOutcome::Promoted(control))
    }

    async fn renew_standing_runtime_owner(
        &self,
        owner: StandingRuntimeOwnerToken,
        ttl_ms: u64,
    ) -> Result<StandingRuntimeOwnerClaim, MetaStoreError> {
        AcquireStandingRuntimeOwnerRequest {
            tenant_id: owner.tenant_id.clone(),
            program_id: owner.program_id.clone(),
            view_id: owner.view_id.clone(),
            owner_id: owner.owner_id.clone(),
            ttl_ms,
        }
        .validate()?;
        let mut guard = self.inner.write().await;
        let now = self.evaluation_time_ms()?;
        let key =
            standing_runtime_owner_scope_key(&owner.tenant_id, &owner.program_id, &owner.view_id);
        validate_current_standing_runtime_owner(
            guard.standing_runtime_owners.get(&key),
            &owner,
            now,
        )?;
        let mut claim = guard
            .standing_runtime_owners
            .get(&key)
            .cloned()
            .ok_or(MetaStoreError::StandingRuntimeOwnerMismatch)?;
        claim.expires_at_unix_ms = now
            .checked_add(ttl_ms)
            .ok_or(MetaStoreError::TimestampOverflow)?;
        guard.standing_runtime_owners.insert(key, claim.clone());
        Ok(claim)
    }

    async fn acquire_standing_runtime_owner(
        &self,
        request: AcquireStandingRuntimeOwnerRequest,
    ) -> Result<AcquireStandingRuntimeOwnerOutcome, MetaStoreError> {
        self.acquire_owner_claim(request, None).await
    }

    async fn acquire_standing_runtime_owner_with_endpoint(
        &self,
        request: AcquireStandingRuntimeOwnerRequest,
        owner_endpoint: String,
    ) -> Result<AcquireStandingRuntimeOwnerOutcome, MetaStoreError> {
        validate_owner_endpoint(&owner_endpoint)?;
        self.acquire_owner_claim(request, Some(owner_endpoint))
            .await
    }

    async fn read_standing_runtime_owner(
        &self,
        tenant_id: &str,
        program_id: &str,
        view_id: &str,
    ) -> Result<Option<StandingRuntimeOwnerClaim>, MetaStoreError> {
        validate_standing_runtime_scope(tenant_id, program_id, view_id)?;
        let now = self.evaluation_time_ms()?;
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
            self.evaluation_time_ms()?,
        )?;
        let current = guard.standing_runtime_checkpoints.get(&key);

        if current == Some(&request.candidate) {
            return Ok(PublishStandingRuntimeCheckpointOutcome::Duplicate);
        }

        // Keep the source-cut check inside the same write-lock critical
        // section as owner validation and predecessor CAS.  Rebuild the full
        // authoritative relation-wide cuts (including publication identity
        // and object refs), not merely partition frontiers.
        if let Some(expected_cuts) = request.expected_relation_source_cuts.as_deref() {
            let expected_cuts =
                source_cut::canonicalize_relation_ingest_source_cuts(expected_cuts)?;
            let capture_request = CaptureRelationIngestSourceCutsRequest {
                namespace: expected_cuts[0].cut.namespace.clone(),
                relations: expected_cuts
                    .iter()
                    .map(|entry| entry.relation.clone())
                    .collect(),
            };
            let reservations = guard
                .relation_ingest_reservations
                .values()
                .flatten()
                .filter_map(|reservation| {
                    guard
                        .relation_authority_reservation_keys
                        .get(reservation)
                        .cloned()
                        .map(|authority| (authority, reservation.clone()))
                })
                .collect::<Vec<_>>();
            let publications = guard
                .relation_authoritative_ingest_publications
                .values()
                .cloned()
                .collect::<Vec<_>>();
            let actual_cuts = source_cut::build_relation_ingest_source_cuts(
                &capture_request,
                reservations,
                publications,
            )?;
            if source_cut::canonicalize_relation_ingest_source_cuts(&actual_cuts)? != expected_cuts
            {
                return Ok(PublishStandingRuntimeCheckpointOutcome::SourceCutChanged);
            }
        }
        if current != request.expected_previous.as_ref() {
            return Ok(PublishStandingRuntimeCheckpointOutcome::Conflict);
        }

        if let Some(progress) = &request.candidate.published_input_bootstrap {
            let Some(control) = guard.view_bootstraps.get(&key) else {
                return Ok(PublishStandingRuntimeCheckpointOutcome::Conflict);
            };
            if !progress.matches_control(control)
                || (control.lifecycle == ViewBootstrapLifecycleV1::Active
                    && !progress.is_complete())
            {
                return Ok(PublishStandingRuntimeCheckpointOutcome::Conflict);
            }
        }
        if let Some(previous) =
            current.and_then(|pointer| pointer.published_input_bootstrap.as_ref())
        {
            if !request
                .candidate
                .published_input_bootstrap
                .as_ref()
                .is_some_and(|next| next.follows(previous))
            {
                return Ok(PublishStandingRuntimeCheckpointOutcome::Conflict);
            }
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
        let now = self.authority_read_time_ms().await;
        let guard = self.inner.read().await;
        Ok(guard
            .partition_authorities
            .get(key)
            .filter(|token| token.expires_at_unix_ms > now)
            .cloned())
    }

    async fn acquire_relation_partition_authority(
        &self,
        request: AcquireRelationPartitionAuthorityRequest,
    ) -> Result<AcquireRelationPartitionAuthorityOutcome, MetaStoreError> {
        request.validate()?;
        let mut guard = self.inner.write().await;
        let now = guard.partition_authority_now_unix_ms;
        let expires_at_unix_ms = now
            .checked_add(request.ttl_ms)
            .ok_or(MetaStoreError::TimestampOverflow)?;
        match guard
            .relation_partition_authorities
            .get(&request.key)
            .cloned()
        {
            Some(current) if current.expires_at_unix_ms > now => {
                if current.owner_id == request.owner_id
                    && request.current_token.as_ref() == Some(&current)
                {
                    let renewed = RelationPartitionAuthorityToken {
                        expires_at_unix_ms,
                        ..current
                    };
                    guard
                        .relation_partition_authorities
                        .insert(request.key, renewed.clone());
                    Ok(AcquireRelationPartitionAuthorityOutcome::Renewed(renewed))
                } else {
                    Ok(AcquireRelationPartitionAuthorityOutcome::Conflict(current))
                }
            }
            Some(current) => {
                let owner_epoch = current
                    .owner_epoch
                    .checked_add(1)
                    .ok_or(MetaStoreError::AuthorityEpochOverflow)?;
                let token = RelationPartitionAuthorityToken {
                    key: request.key.clone(),
                    owner_id: request.owner_id,
                    owner_epoch,
                    expires_at_unix_ms,
                };
                guard
                    .relation_partition_authorities
                    .insert(request.key, token.clone());
                Ok(AcquireRelationPartitionAuthorityOutcome::Acquired(token))
            }
            None => {
                let token = RelationPartitionAuthorityToken {
                    key: request.key.clone(),
                    owner_id: request.owner_id,
                    owner_epoch: 1,
                    expires_at_unix_ms,
                };
                guard
                    .relation_partition_authorities
                    .insert(request.key, token.clone());
                Ok(AcquireRelationPartitionAuthorityOutcome::Acquired(token))
            }
        }
    }

    async fn read_relation_partition_authority(
        &self,
        key: &RelationPartitionAuthorityKey,
    ) -> Result<Option<RelationPartitionAuthorityToken>, MetaStoreError> {
        key.validate()?;
        let now = self.authority_read_time_ms().await;
        let guard = self.inner.read().await;
        Ok(guard
            .relation_partition_authorities
            .get(key)
            .filter(|token| token.expires_at_unix_ms > now)
            .cloned())
    }

    async fn read_relation_ingest_capability(
        &self,
    ) -> Result<RelationIngestCapability, MetaStoreError> {
        Ok(RelationIngestCapability {
            backend_name: "in-memory".into(),
            relation_scoped_authority: true,
            committed_publication_source_cut: true,
            durable_across_restart: false,
        })
    }

    async fn capture_relation_ingest_source_cut(
        &self,
        request: CaptureRelationIngestSourceCutRequest,
    ) -> Result<RelationIngestSourceCutV1, MetaStoreError> {
        request.validate()?;
        let guard = self.inner.read().await;
        source_cut::build_relation_ingest_source_cut(
            &request,
            guard
                .relation_authoritative_ingest_publications
                .values()
                .cloned(),
        )
    }

    async fn capture_relation_ingest_source_cuts(
        &self,
        request: CaptureRelationIngestSourceCutsRequest,
    ) -> Result<Vec<RelationIngestSourceIdentityCutV1>, MetaStoreError> {
        request.validate()?;
        let guard = self.inner.read().await;
        let reservations = guard
            .relation_ingest_reservations
            .values()
            .flatten()
            .filter_map(|reservation| {
                guard
                    .relation_authority_reservation_keys
                    .get(reservation)
                    .cloned()
                    .map(|authority| (authority, reservation.clone()))
            })
            .collect::<Vec<_>>();
        let publications = guard
            .relation_authoritative_ingest_publications
            .values()
            .cloned()
            .collect::<Vec<_>>();
        source_cut::build_relation_ingest_source_cuts(&request, reservations, publications)
    }

    async fn reserve_relation_authoritative_ingest_range(
        &self,
        request: ReserveRelationAuthoritativeIngestRangeRequest,
    ) -> Result<ReserveIngestRangeOutcome, MetaStoreError> {
        request.validate()?;
        let mut guard = self.inner.write().await;
        if validate_current_relation_partition_authority(
            guard
                .relation_partition_authorities
                .get(&request.authority.key),
            &request.authority,
            guard.partition_authority_now_unix_ms,
        )
        .is_err()
        {
            return Ok(ReserveIngestRangeOutcome::Conflict);
        }
        let key = (
            request.reservation.stream_id.clone(),
            request.reservation.partition_id,
        );
        let legacy_conflict = guard
            .ingest_reservations
            .get(&key)
            .is_some_and(|reservations| {
                reservations
                    .iter()
                    .any(|existing| existing.overlaps(&request.reservation))
            });
        let relation_key = (
            request.reservation.stream_id.clone(),
            request.reservation.partition_id,
        );
        if let Some(existing_authority) = guard
            .relation_authority_reservation_keys
            .get(&request.reservation)
        {
            return Ok(if existing_authority == &request.authority.key {
                ReserveIngestRangeOutcome::Duplicate
            } else {
                ReserveIngestRangeOutcome::Conflict
            });
        }
        let relation_overlap = guard
            .relation_ingest_reservations
            .get(&relation_key)
            .is_some_and(|reservations| {
                reservations
                    .iter()
                    .any(|existing| existing.overlaps(&request.reservation))
            });
        if legacy_conflict
            || relation_overlap
            || guard
                .legacy_batch_keys
                .contains_key(&request.reservation.batch_key)
            || guard
                .relation_batch_keys
                .contains_key(&request.reservation.batch_key)
        {
            return Ok(ReserveIngestRangeOutcome::Conflict);
        }
        guard.relation_batch_keys.insert(
            request.reservation.batch_key.clone(),
            request.reservation.clone(),
        );
        guard
            .relation_ingest_reservations
            .entry(relation_key)
            .or_default()
            .push(request.reservation.clone());
        guard
            .relation_authority_reservation_keys
            .insert(request.reservation.clone(), request.authority.key);
        Ok(ReserveIngestRangeOutcome::Reserved)
    }

    async fn publish_relation_ingest_reservation(
        &self,
        request: PublishRelationIngestReservationRequest,
    ) -> Result<PublishIngestReservationOutcome, MetaStoreError> {
        request.validate()?;
        let mut guard = self.inner.write().await;
        let publication = RelationAuthoritativeIngestPublication {
            reservation: request.reservation.clone(),
            authority_key: request.authority.key.clone(),
            request_id: request.request_id.clone(),
            request_digest: request.request_digest.clone(),
            object_key: request.object_key.clone(),
            object_digest: request.object_digest.clone(),
        };
        if validate_current_relation_partition_authority(
            guard
                .relation_partition_authorities
                .get(&request.authority.key),
            &request.authority,
            guard.partition_authority_now_unix_ms,
        )
        .is_err()
        {
            return Ok(PublishIngestReservationOutcome::InvalidAuthority);
        }
        if let Some(existing) = guard
            .relation_authoritative_ingest_publications
            .get(&request.request_id)
        {
            return Ok(if existing == &publication {
                PublishIngestReservationOutcome::Duplicate
            } else {
                PublishIngestReservationOutcome::Conflict
            });
        }
        if guard
            .relation_reservation_publications
            .get(&request.reservation)
            .is_some_and(|request_id| request_id != &request.request_id)
        {
            return Ok(PublishIngestReservationOutcome::Conflict);
        }
        if guard
            .relation_authority_reservation_keys
            .get(&request.reservation)
            != Some(&request.authority.key)
        {
            return Ok(PublishIngestReservationOutcome::Conflict);
        }
        if guard
            .relation_batch_keys
            .get(&request.reservation.batch_key)
            .is_some_and(|existing| existing != &request.reservation)
        {
            return Ok(PublishIngestReservationOutcome::Conflict);
        }
        guard
            .relation_authoritative_ingest_publications
            .insert(request.request_id.clone(), publication);
        guard
            .relation_reservation_publications
            .insert(request.reservation, request.request_id);
        Ok(PublishIngestReservationOutcome::Committed)
    }

    async fn read_relation_authoritative_ingest_publication(
        &self,
        request_id: &str,
    ) -> Result<Option<RelationAuthoritativeIngestPublication>, MetaStoreError> {
        require_non_empty("request_id", request_id)?;
        Ok(self
            .inner
            .read()
            .await
            .relation_authoritative_ingest_publications
            .get(request_id)
            .cloned())
    }

    async fn list_relation_authoritative_ingest_publications(
        &self,
        key: &RelationPartitionAuthorityKey,
    ) -> Result<Vec<RelationAuthoritativeIngestPublication>, MetaStoreError> {
        key.validate()?;
        let mut publications = self
            .inner
            .read()
            .await
            .relation_authoritative_ingest_publications
            .values()
            .filter(|publication| publication.authority_key == *key)
            .cloned()
            .collect::<Vec<_>>();
        publications.sort_by(|a, b| {
            a.reservation
                .start_offset_inclusive
                .cmp(&b.reservation.start_offset_inclusive)
                .then_with(|| {
                    a.reservation
                        .end_offset_exclusive
                        .cmp(&b.reservation.end_offset_exclusive)
                })
                .then_with(|| a.request_id.cmp(&b.request_id))
        });
        Ok(publications)
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
            relation_ingest: RelationIngestCapability::default(),
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

fn validate_owner_endpoint(endpoint: &str) -> Result<(), MetaStoreError> {
    let Some((scheme, authority)) = endpoint.split_once("://") else {
        return Err(MetaStoreError::Serialization(
            "invalid owner endpoint origin".into(),
        ));
    };
    if !matches!(scheme, "http" | "https")
        || authority.is_empty()
        || authority.contains(['/', '\\', '?', '#', '@'])
    {
        return Err(MetaStoreError::Serialization(
            "owner endpoint must be an HTTP(S) origin without credentials, path, query, or fragment".into(),
        ));
    }
    tonic::transport::Endpoint::from_shared(endpoint.to_owned()).map_err(|error| {
        MetaStoreError::Serialization(format!("invalid owner endpoint: {error}"))
    })?;
    Ok(())
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

impl RelationPartitionAuthorityKey {
    fn validate(&self) -> Result<(), MetaStoreError> {
        require_non_empty("namespace", &self.namespace)?;
        require_non_empty("relation_id", &self.relation_id)?;
        require_non_empty("stream_id", &self.stream_id)?;
        Ok(())
    }
}

impl RelationPartitionAuthorityToken {
    fn validate(&self) -> Result<(), MetaStoreError> {
        self.key.validate()?;
        if self.owner_id.is_empty() || self.owner_epoch == 0 {
            return Err(MetaStoreError::PartitionAuthorityInvalidToken);
        }
        Ok(())
    }
}

impl AcquireRelationPartitionAuthorityRequest {
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

impl ReserveRelationAuthoritativeIngestRangeRequest {
    fn validate(&self) -> Result<(), MetaStoreError> {
        self.reservation.validate()?;
        self.authority.validate()?;
        if self.authority.key.stream_id != self.reservation.stream_id
            || self.authority.key.partition_id != self.reservation.partition_id
            || self.authority.key.relation_id != self.reservation.relation_id
        {
            return Err(MetaStoreError::PartitionAuthorityTokenScopeMismatch);
        }
        Ok(())
    }
}

impl PublishRelationIngestReservationRequest {
    fn validate(&self) -> Result<(), MetaStoreError> {
        self.reservation.validate()?;
        self.authority.validate()?;
        require_non_empty("request_id", &self.request_id)?;
        require_non_empty("request_digest", &self.request_digest)?;
        require_non_empty("object_key", &self.object_key)?;
        require_non_empty("object_digest", &self.object_digest)?;
        if self.authority.key.stream_id != self.reservation.stream_id
            || self.authority.key.partition_id != self.reservation.partition_id
            || self.authority.key.relation_id != self.reservation.relation_id
        {
            return Err(MetaStoreError::PartitionAuthorityTokenScopeMismatch);
        }
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

impl PublishIngestReservationRequest {
    fn validate(&self) -> Result<(), MetaStoreError> {
        self.reservation.validate()?;
        self.authority.validate()?;
        require_non_empty("request_id", &self.request_id)?;
        require_non_empty("request_digest", &self.request_digest)?;
        require_non_empty("object_key", &self.object_key)?;
        require_non_empty("object_digest", &self.object_digest)?;
        if self.authority.key.stream_id != self.reservation.stream_id
            || self.authority.key.partition_id != self.reservation.partition_id
        {
            return Err(MetaStoreError::PartitionAuthorityTokenScopeMismatch);
        }
        Ok(())
    }
}

impl ReserveAuthoritativeIngestRangeRequest {
    fn validate(&self) -> Result<(), MetaStoreError> {
        self.reservation.validate()?;
        self.authority.validate()?;
        if self.authority.key.stream_id != self.reservation.stream_id
            || self.authority.key.partition_id != self.reservation.partition_id
        {
            return Err(MetaStoreError::PartitionAuthorityTokenScopeMismatch);
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
        if let Some(progress) = &self.published_input_bootstrap {
            progress.validate()?;
            if !progress.is_complete() && self.input_coverage.is_some() {
                return Err(MetaStoreError::Serialization(
                    "partial published bootstrap cannot claim input coverage".into(),
                ));
            }
            if progress.bootstrap_generation != self.bootstrap_generation
                || progress.plan_hash != self.plan_hash
            {
                return Err(MetaStoreError::Serialization(
                    "published bootstrap checkpoint identity mismatch".into(),
                ));
            }
        }
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
                if !self.coverage_hash.is_empty()
                    || (self.published_input_bootstrap.is_none()
                        && ((self.bootstrap_generation == 0) != self.plan_hash.is_empty()
                            || (self.bootstrap_generation != 0
                                && self.previous_checkpoint_key.is_empty())))
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

fn validate_current_relation_partition_authority(
    current: Option<&RelationPartitionAuthorityToken>,
    authority: &RelationPartitionAuthorityToken,
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
    // Rhiza deliberately reports its logical CAS/lease contract as
    // unsupported for multi-writer runtime admission: its process-clock TTL
    // has no bounded-skew guarantee. Authentication must not turn that
    // explicit backend refusal into a generic formula-derived `true`.
    if capability.backend_name == "rhiza-kv" {
        capability.multi_writer_fencing_safe &= control_plane_auth_enforced;
    } else {
        capability.multi_writer_fencing_safe = capability.linearizable_owner_lease
            && capability.durable_monotonic_owner_epoch
            && capability.owner_validated_checkpoint_publish
            && capability.publish_checks_owner_and_latest_atomically
            && capability.publish_rejects_expired_owner
            && capability.latest_read_linearizable
            && capability.publish_rejects_scope_mismatch
            && capability.control_plane_auth_enforced;
    }
    capability.production_bounded_failover_safe = capability.multi_writer_fencing_safe
        && capability.authoritative_backend_time
        && capability.bounded_wall_clock_failover;
    capability.production_multi_writer_safe = capability.production_bounded_failover_safe;
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

fn validate_active_view_scope(scope: &ActiveViewRegistryScopeV1) -> Result<(), MetaStoreError> {
    require_non_empty("authority_store_id", &scope.authority_store_id)?;
    require_non_empty("namespace", &scope.namespace)
}

fn validate_active_view_record(
    _scope: &ActiveViewRegistryScopeV1,
    record: &ActiveMaterializedViewRecord,
    expected_view_id: Option<&str>,
) -> Result<(), MetaStoreError> {
    require_non_empty("view_id", &record.view_id)?;
    require_non_empty("spec_hash", &record.spec_hash)?;
    if expected_view_id.is_some_and(|expected| expected != record.view_id) {
        return Err(MetaStoreError::ActiveViewRecordIdentityMismatch);
    }
    Ok(())
}

fn validate_active_view_records(
    scope: &ActiveViewRegistryScopeV1,
    records: &[ActiveMaterializedViewRecord],
) -> Result<(), MetaStoreError> {
    let mut ids = BTreeSet::new();
    for record in records {
        validate_active_view_record(scope, record, None)?;
        if !ids.insert(record.view_id.as_str()) {
            return Err(MetaStoreError::DuplicateActiveViewId {
                view_id: record.view_id.clone(),
            });
        }
    }
    Ok(())
}

fn validate_active_view_registry(
    registry: &ActiveViewRegistryStateV1,
) -> Result<(), MetaStoreError> {
    let mut api_paths = BTreeSet::new();
    for versioned in registry.records.values() {
        let Some(path) = versioned
            .record
            .api
            .as_ref()
            .and_then(|api| api.url_path.as_deref())
        else {
            continue;
        };
        let normalized = path.trim_matches('/');
        if !api_paths.insert(normalized) {
            return Err(MetaStoreError::DuplicateActiveViewApiPath {
                normalized_url_path: normalized.to_string(),
            });
        }
    }
    let snapshot = ActiveViewRegistrySnapshotV1 {
        snapshot_revision: registry.snapshot_revision,
        imported: registry.initialized,
        records: registry.records.values().cloned().collect(),
    };
    let size = serde_json::to_vec(&snapshot)
        .map_err(|error| MetaStoreError::Serialization(error.to_string()))?
        .len();
    if size > MAX_META_GRPC_MESSAGE_BYTES - 256 {
        return Err(MetaStoreError::ActiveViewRegistryTooLarge);
    }
    Ok(())
}

fn validate_active_view_rpc_payload_size(size: usize) -> Result<(), Status> {
    if size > MAX_META_GRPC_MESSAGE_BYTES - 256 {
        Err(Status::resource_exhausted(
            "active view registry payload exceeds 8 MiB",
        ))
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

    async fn publish_standing_runtime_checkpoint_rpc(
        &self,
        request: Request<proto::PublishStandingRuntimeCheckpointRequest>,
        guarded: bool,
    ) -> Result<Response<proto::PublishStandingRuntimeCheckpointResponse>, Status>
    where
        S: MetaStore,
    {
        self.authorize(&request)?;
        let request = request.into_inner();
        let expected_relation_source_cuts = if request.expected_relation_source_cuts_json.is_empty()
        {
            None
        } else {
            Some(
                serde_json::from_slice(&request.expected_relation_source_cuts_json)
                    .map_err(|error| Status::invalid_argument(error.to_string()))?,
            )
        };
        if guarded && expected_relation_source_cuts.is_none() {
            return Err(Status::invalid_argument(
                "guarded checkpoint publication requires expected relation source cuts",
            ));
        }
        if !guarded && expected_relation_source_cuts.is_some() {
            return Err(Status::failed_precondition(
                "guarded checkpoint publication requires the guarded RPC",
            ));
        }
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
                expected_relation_source_cuts,
            })
            .await
            .map_err(meta_status)?;

        Ok(Response::new(
            proto::PublishStandingRuntimeCheckpointResponse {
                outcome: publish_standing_runtime_checkpoint_outcome(&outcome).to_string(),
            },
        ))
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
            relation_ingest: Some(relation_ingest_capability_to_proto(
                capabilities.relation_ingest,
            )),
        }))
    }

    async fn acquire_relation_partition_authority(
        &self,
        request: Request<proto::AcquireRelationPartitionAuthorityRequest>,
    ) -> Result<Response<proto::AcquireRelationPartitionAuthorityResponse>, Status> {
        self.authorize(&request)?;
        let request = acquire_relation_partition_authority_request_from_proto(request.into_inner())
            .map_err(partition_authority_status)?;
        let outcome = self
            .store
            .acquire_relation_partition_authority(request)
            .await
            .map_err(partition_authority_status)?;
        let token = match &outcome {
            AcquireRelationPartitionAuthorityOutcome::Acquired(token)
            | AcquireRelationPartitionAuthorityOutcome::Renewed(token)
            | AcquireRelationPartitionAuthorityOutcome::Conflict(token) => token.clone(),
        };
        let outcome_name = match outcome {
            AcquireRelationPartitionAuthorityOutcome::Acquired(_) => "acquired",
            AcquireRelationPartitionAuthorityOutcome::Renewed(_) => "renewed",
            AcquireRelationPartitionAuthorityOutcome::Conflict(_) => "conflict",
        };
        Ok(Response::new(
            proto::AcquireRelationPartitionAuthorityResponse {
                outcome: outcome_name.to_string(),
                token: Some(relation_partition_authority_token_to_proto(token)),
            },
        ))
    }

    async fn read_relation_partition_authority(
        &self,
        request: Request<proto::ReadRelationPartitionAuthorityRequest>,
    ) -> Result<Response<proto::ReadRelationPartitionAuthorityResponse>, Status> {
        self.authorize(&request)?;
        let key = request
            .into_inner()
            .key
            .ok_or_else(|| Status::invalid_argument("relation authority key is required"))
            .and_then(|key| {
                relation_partition_authority_key_from_proto(key).map_err(partition_authority_status)
            })?;
        let token = self
            .store
            .read_relation_partition_authority(&key)
            .await
            .map_err(partition_authority_status)?;
        Ok(Response::new(
            proto::ReadRelationPartitionAuthorityResponse {
                found: token.is_some(),
                token: token.map(relation_partition_authority_token_to_proto),
            },
        ))
    }

    async fn reserve_relation_authoritative_ingest_range(
        &self,
        request: Request<proto::ReserveRelationAuthoritativeIngestRangeRequest>,
    ) -> Result<Response<proto::ReserveIngestRangeResponse>, Status> {
        self.authorize(&request)?;
        let request =
            reserve_relation_authoritative_ingest_range_request_from_proto(request.into_inner())
                .map_err(partition_authority_status)?;
        let outcome = self
            .store
            .reserve_relation_authoritative_ingest_range(request)
            .await
            .map_err(partition_authority_status)?;
        Ok(Response::new(proto::ReserveIngestRangeResponse {
            outcome: reserve_ingest_range_outcome(&outcome).to_string(),
        }))
    }

    async fn publish_relation_ingest_reservation(
        &self,
        request: Request<proto::PublishRelationIngestReservationRequest>,
    ) -> Result<Response<proto::PublishIngestReservationResponse>, Status> {
        self.authorize(&request)?;
        let request = publish_relation_ingest_reservation_request_from_proto(request.into_inner())
            .map_err(partition_authority_status)?;
        let outcome = self
            .store
            .publish_relation_ingest_reservation(request)
            .await
            .map_err(partition_authority_status)?;
        Ok(Response::new(proto::PublishIngestReservationResponse {
            outcome: publish_ingest_reservation_outcome(&outcome).to_string(),
        }))
    }

    async fn read_relation_authoritative_ingest_publication(
        &self,
        request: Request<proto::ReadRelationAuthoritativeIngestPublicationRequest>,
    ) -> Result<Response<proto::ReadRelationAuthoritativeIngestPublicationResponse>, Status> {
        self.authorize(&request)?;
        let publication = self
            .store
            .read_relation_authoritative_ingest_publication(&request.into_inner().request_id)
            .await
            .map_err(partition_authority_status)?;
        Ok(Response::new(
            proto::ReadRelationAuthoritativeIngestPublicationResponse {
                found: publication.is_some(),
                publication: publication.map(relation_authoritative_ingest_publication_to_proto),
            },
        ))
    }

    async fn list_relation_authoritative_ingest_publications(
        &self,
        request: Request<proto::ListRelationAuthoritativeIngestPublicationsRequest>,
    ) -> Result<Response<proto::ListRelationAuthoritativeIngestPublicationsResponse>, Status> {
        self.authorize(&request)?;
        let key = request
            .into_inner()
            .key
            .ok_or_else(|| Status::invalid_argument("relation authority key is required"))
            .and_then(|key| {
                relation_partition_authority_key_from_proto(key).map_err(partition_authority_status)
            })?;
        let publications = self
            .store
            .list_relation_authoritative_ingest_publications(&key)
            .await
            .map_err(partition_authority_status)?;
        Ok(Response::new(
            proto::ListRelationAuthoritativeIngestPublicationsResponse {
                publications: publications
                    .into_iter()
                    .map(relation_authoritative_ingest_publication_to_proto)
                    .collect(),
            },
        ))
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

    async fn publish_ingest_reservation(
        &self,
        request: Request<proto::PublishIngestReservationRequest>,
    ) -> Result<Response<proto::PublishIngestReservationResponse>, Status> {
        self.authorize(&request)?;
        let request = publish_ingest_reservation_request_from_proto(request.into_inner())
            .map_err(partition_authority_status)?;
        let outcome = self
            .store
            .publish_ingest_reservation(request)
            .await
            .map_err(partition_authority_status)?;
        Ok(Response::new(proto::PublishIngestReservationResponse {
            outcome: publish_ingest_reservation_outcome(&outcome).to_string(),
        }))
    }

    async fn read_authoritative_ingest_publication(
        &self,
        request: Request<proto::ReadAuthoritativeIngestPublicationRequest>,
    ) -> Result<Response<proto::ReadAuthoritativeIngestPublicationResponse>, Status> {
        self.authorize(&request)?;
        let publication = self
            .store
            .read_authoritative_ingest_publication(&request.into_inner().request_id)
            .await
            .map_err(partition_authority_status)?;
        Ok(Response::new(
            proto::ReadAuthoritativeIngestPublicationResponse {
                found: publication.is_some(),
                publication: publication.map(authoritative_ingest_publication_to_proto),
            },
        ))
    }

    async fn list_authoritative_ingest_publications(
        &self,
        request: Request<proto::ListAuthoritativeIngestPublicationsRequest>,
    ) -> Result<Response<proto::ListAuthoritativeIngestPublicationsResponse>, Status> {
        self.authorize(&request)?;
        let key = request
            .into_inner()
            .key
            .ok_or_else(|| Status::invalid_argument("partition authority key is required"))
            .and_then(|key| {
                partition_authority_key_from_proto(key).map_err(partition_authority_status)
            })?;
        let publications = self
            .store
            .list_authoritative_ingest_publications(&key)
            .await
            .map_err(partition_authority_status)?;
        Ok(Response::new(
            proto::ListAuthoritativeIngestPublicationsResponse {
                publications: publications
                    .into_iter()
                    .map(authoritative_ingest_publication_to_proto)
                    .collect(),
            },
        ))
    }

    async fn capture_ingest_source_cut(
        &self,
        request: Request<proto::CaptureIngestSourceCutRequest>,
    ) -> Result<Response<proto::CaptureIngestSourceCutResponse>, Status> {
        self.authorize(&request)?;
        let request: CaptureIngestSourceCutRequest =
            serde_json::from_slice(&request.into_inner().request_json)
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

    async fn capture_relation_ingest_source_cut(
        &self,
        request: Request<proto::CaptureRelationIngestSourceCutRequest>,
    ) -> Result<Response<proto::CaptureRelationIngestSourceCutResponse>, Status> {
        self.authorize(&request)?;
        let request = request.into_inner();
        let relation_version = request.relation_version;
        let schema_fingerprint = request.schema_fingerprint;
        let authority = request
            .authority
            .ok_or_else(|| Status::invalid_argument("relation authority key is required"))
            .and_then(|key| {
                relation_partition_authority_key_from_proto(key).map_err(partition_authority_status)
            })?;
        let source_cut = self
            .store
            .capture_relation_ingest_source_cut(CaptureRelationIngestSourceCutRequest {
                authority,
                relation_version,
                schema_fingerprint,
            })
            .await
            .map_err(meta_status)?;
        Ok(Response::new(
            proto::CaptureRelationIngestSourceCutResponse {
                source_cut_json: serde_json::to_vec(&source_cut)
                    .map_err(|error| Status::internal(error.to_string()))?,
            },
        ))
    }

    async fn capture_relation_ingest_source_cuts(
        &self,
        request: Request<proto::CaptureRelationIngestSourceCutsRequest>,
    ) -> Result<Response<proto::CaptureRelationIngestSourceCutsResponse>, Status> {
        self.authorize(&request)?;
        let request = serde_json::from_slice(&request.into_inner().request_json)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let source_cuts = self
            .store
            .capture_relation_ingest_source_cuts(request)
            .await
            .map_err(meta_status)?;
        Ok(Response::new(
            proto::CaptureRelationIngestSourceCutsResponse {
                source_cuts_json: serde_json::to_vec(&source_cuts)
                    .map_err(|error| Status::internal(error.to_string()))?,
            },
        ))
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

    async fn read_demand_metadata_batch(
        &self,
        request: Request<proto::ReadDemandMetadataBatchRequest>,
    ) -> Result<Response<proto::ReadDemandMetadataBatchResponse>, Status> {
        let total_started = std::time::Instant::now();
        self.authorize(&request)?;
        let request_json = request.into_inner().request_json;
        let request_bytes = request_json.len();
        let decode_started = std::time::Instant::now();
        let request: ReadDemandMetadataBatchRequest = serde_json::from_slice(&request_json)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let json_decode = decode_started.elapsed();
        let validate_started = std::time::Instant::now();
        request
            .validate()
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let request_validate = validate_started.elapsed();
        let store_started = std::time::Instant::now();
        let response = self
            .store
            .read_demand_metadata_batch(request)
            .await
            .map_err(meta_status)?;
        let store_elapsed = store_started.elapsed();
        let encode_started = std::time::Instant::now();
        let response_json =
            serde_json::to_vec(&response).map_err(|error| Status::internal(error.to_string()))?;
        let json_encode = encode_started.elapsed();
        let total = total_started.elapsed();
        if std::env::var_os("VELORIX_META_DIAGNOSTICS").is_some() {
            eprintln!(
                "meta_diag op=read_demand_metadata_batch request_bytes={request_bytes} response_bytes={} json_decode_us={} request_validate_us={} store_us={} json_encode_us={} total_us={}",
                response_json.len(),
                json_decode.as_micros(),
                request_validate.as_micros(),
                store_elapsed.as_micros(),
                json_encode.as_micros(),
                total.as_micros(),
            );
        }
        Ok(Response::new(proto::ReadDemandMetadataBatchResponse {
            response_json,
        }))
    }

    async fn read_active_view_registry_snapshot(
        &self,
        request: Request<proto::ActiveViewRegistrySnapshotRequest>,
    ) -> Result<Response<proto::ActiveViewRegistrySnapshotResponse>, Status> {
        let total_started = std::time::Instant::now();
        self.authorize(&request)?;
        let request_json = request.into_inner().request_json;
        let request_bytes = request_json.len();
        let request_validate_started = std::time::Instant::now();
        validate_active_view_rpc_payload_size(request_json.len())?;
        let request_validate = request_validate_started.elapsed();
        let decode_started = std::time::Instant::now();
        let request: ActiveViewRegistryReadRequestV1 = serde_json::from_slice(&request_json)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let json_decode = decode_started.elapsed();
        let store_started = std::time::Instant::now();
        let response = self
            .store
            .read_active_view_registry_snapshot(request)
            .await
            .map_err(meta_status)?;
        let store_elapsed = store_started.elapsed();
        let encode_started = std::time::Instant::now();
        let response_json =
            serde_json::to_vec(&response).map_err(|error| Status::internal(error.to_string()))?;
        let json_encode = encode_started.elapsed();
        let response_validate_started = std::time::Instant::now();
        if response_json.len() > MAX_META_GRPC_MESSAGE_BYTES - 256 {
            return Err(Status::resource_exhausted(
                "active view registry snapshot exceeds 8 MiB",
            ));
        }
        let response_validate = response_validate_started.elapsed();
        let total = total_started.elapsed();
        if std::env::var_os("VELORIX_META_DIAGNOSTICS").is_some() {
            eprintln!(
                "meta_diag op=read_active_view_registry_snapshot request_bytes={request_bytes} response_bytes={} json_decode_us={} request_validate_us={} store_us={} response_validate_us={} json_encode_us={} total_us={}",
                response_json.len(),
                json_decode.as_micros(),
                request_validate.as_micros(),
                store_elapsed.as_micros(),
                response_validate.as_micros(),
                json_encode.as_micros(),
                total.as_micros(),
            );
        }
        Ok(Response::new(proto::ActiveViewRegistrySnapshotResponse {
            response_json,
        }))
    }

    async fn create_active_view_record(
        &self,
        request: Request<proto::ActiveViewRecordMutationRequest>,
    ) -> Result<Response<proto::ActiveViewRecordMutationResponse>, Status> {
        self.authorize(&request)?;
        let request_json = request.into_inner().request_json;
        validate_active_view_rpc_payload_size(request_json.len())?;
        let request: CreateActiveViewRecordRequestV1 = serde_json::from_slice(&request_json)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let response = self
            .store
            .create_active_view_record(request)
            .await
            .map_err(meta_status)?;
        Ok(Response::new(proto::ActiveViewRecordMutationResponse {
            response_json: serde_json::to_vec(&response)
                .map_err(|error| Status::internal(error.to_string()))?,
        }))
    }

    async fn compare_exchange_active_view_record(
        &self,
        request: Request<proto::ActiveViewRecordMutationRequest>,
    ) -> Result<Response<proto::ActiveViewRecordMutationResponse>, Status> {
        self.authorize(&request)?;
        let request_json = request.into_inner().request_json;
        validate_active_view_rpc_payload_size(request_json.len())?;
        let request: CompareExchangeActiveViewRecordRequestV1 =
            serde_json::from_slice(&request_json)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let response = self
            .store
            .compare_exchange_active_view_record(request)
            .await
            .map_err(meta_status)?;
        Ok(Response::new(proto::ActiveViewRecordMutationResponse {
            response_json: serde_json::to_vec(&response)
                .map_err(|error| Status::internal(error.to_string()))?,
        }))
    }

    async fn import_active_view_registry(
        &self,
        request: Request<proto::ActiveViewRecordMutationRequest>,
    ) -> Result<Response<proto::ActiveViewRecordMutationResponse>, Status> {
        self.authorize(&request)?;
        let request_json = request.into_inner().request_json;
        validate_active_view_rpc_payload_size(request_json.len())?;
        let request: ImportActiveViewRegistryRequestV1 = serde_json::from_slice(&request_json)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let response = self
            .store
            .import_active_view_registry(request)
            .await
            .map_err(meta_status)?;
        Ok(Response::new(proto::ActiveViewRecordMutationResponse {
            response_json: serde_json::to_vec(&response)
                .map_err(|error| Status::internal(error.to_string()))?,
        }))
    }

    async fn promote_view_bootstrap_with_active_view_record(
        &self,
        request: Request<proto::ActiveViewRecordMutationRequest>,
    ) -> Result<Response<proto::ActiveViewRecordMutationResponse>, Status> {
        self.authorize(&request)?;
        let request_json = request.into_inner().request_json;
        validate_active_view_rpc_payload_size(request_json.len())?;
        let request: PromoteViewBootstrapWithActiveViewRecordRequestV1 =
            serde_json::from_slice(&request_json)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let response = self
            .store
            .promote_view_bootstrap_with_active_view_record(request)
            .await
            .map_err(meta_status)?;
        Ok(Response::new(proto::ActiveViewRecordMutationResponse {
            response_json: serde_json::to_vec(&response)
                .map_err(|error| Status::internal(error.to_string()))?,
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

    async fn reserve_authoritative_ingest_range(
        &self,
        request: Request<proto::ReserveAuthoritativeIngestRangeRequest>,
    ) -> Result<Response<proto::ReserveIngestRangeResponse>, Status> {
        self.authorize(&request)?;
        let request = reserve_authoritative_ingest_range_request_from_proto(request.into_inner())
            .map_err(partition_authority_status)?;
        let outcome = self
            .store
            .reserve_authoritative_ingest_range(request)
            .await
            .map_err(partition_authority_status)?;
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
        let acquire = AcquireStandingRuntimeOwnerRequest {
            tenant_id: request.tenant_id,
            program_id: request.program_id,
            view_id: request.view_id,
            owner_id: request.owner_id,
            ttl_ms: request.ttl_ms,
        };
        let outcome = if request.owner_endpoint.is_empty() {
            self.store.acquire_standing_runtime_owner(acquire).await
        } else {
            self.store
                .acquire_standing_runtime_owner_with_endpoint(acquire, request.owner_endpoint)
                .await
        }
        .map_err(meta_status)?;

        Ok(Response::new(proto::AcquireStandingRuntimeOwnerResponse {
            outcome: acquire_standing_runtime_owner_outcome(&outcome).to_string(),
            claim: Some(acquire_standing_runtime_owner_claim(outcome)),
        }))
    }

    async fn renew_standing_runtime_owner(
        &self,
        request: Request<proto::RenewStandingRuntimeOwnerRequest>,
    ) -> Result<Response<proto::AcquireStandingRuntimeOwnerResponse>, Status> {
        self.authorize(&request)?;
        let request = request.into_inner();
        let owner = request
            .owner
            .ok_or_else(|| Status::invalid_argument("missing owner token"))?;
        let claim = self
            .store
            .renew_standing_runtime_owner(
                standing_runtime_owner_token_from_proto(owner),
                request.ttl_ms,
            )
            .await
            .map_err(meta_status)?;
        Ok(Response::new(proto::AcquireStandingRuntimeOwnerResponse {
            outcome: "renewed".into(),
            claim: Some(standing_runtime_owner_claim_to_proto(claim)),
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
        self.publish_standing_runtime_checkpoint_rpc(request, false)
            .await
    }

    async fn publish_standing_runtime_checkpoint_guarded(
        &self,
        request: Request<proto::PublishStandingRuntimeCheckpointRequest>,
    ) -> Result<Response<proto::PublishStandingRuntimeCheckpointResponse>, Status> {
        self.publish_standing_runtime_checkpoint_rpc(request, true)
            .await
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

fn publish_ingest_reservation_request_from_proto(
    request: proto::PublishIngestReservationRequest,
) -> Result<PublishIngestReservationRequest, MetaStoreError> {
    let reservation = request
        .reservation
        .ok_or_else(|| MetaStoreError::Serialization("ingest reservation is required".into()))
        .map(ingest_range_reservation_from_proto)?;
    let authority = request
        .authority
        .ok_or_else(|| {
            MetaStoreError::Serialization("partition authority token is required".into())
        })
        .and_then(partition_authority_token_from_proto)?;
    let request = PublishIngestReservationRequest {
        reservation,
        authority,
        request_id: request.request_id,
        request_digest: request.request_digest,
        object_key: request.object_key,
        object_digest: request.object_digest,
    };
    request.validate()?;
    Ok(request)
}

fn reserve_authoritative_ingest_range_request_from_proto(
    request: proto::ReserveAuthoritativeIngestRangeRequest,
) -> Result<ReserveAuthoritativeIngestRangeRequest, MetaStoreError> {
    let reservation = request
        .reservation
        .ok_or_else(|| MetaStoreError::Serialization("ingest reservation is required".into()))
        .map(ingest_range_reservation_from_proto)?;
    let authority = request
        .authority
        .ok_or_else(|| {
            MetaStoreError::Serialization("partition authority token is required".into())
        })
        .and_then(partition_authority_token_from_proto)?;
    let request = ReserveAuthoritativeIngestRangeRequest {
        reservation,
        authority,
    };
    request.validate()?;
    Ok(request)
}

fn reserve_authoritative_ingest_range_request_to_proto(
    request: ReserveAuthoritativeIngestRangeRequest,
) -> proto::ReserveAuthoritativeIngestRangeRequest {
    proto::ReserveAuthoritativeIngestRangeRequest {
        reservation: Some(ingest_range_reservation_to_proto(request.reservation)),
        authority: Some(partition_authority_token_to_proto(request.authority)),
    }
}

fn authoritative_ingest_publication_to_proto(
    publication: AuthoritativeIngestPublication,
) -> proto::AuthoritativeIngestPublication {
    proto::AuthoritativeIngestPublication {
        reservation: Some(ingest_range_reservation_to_proto(publication.reservation)),
        authority_key: Some(partition_authority_key_to_proto(publication.authority_key)),
        request_id: publication.request_id,
        request_digest: publication.request_digest,
        object_key: publication.object_key,
        object_digest: publication.object_digest,
    }
}

fn authoritative_ingest_publication_from_proto(
    publication: proto::AuthoritativeIngestPublication,
) -> Result<AuthoritativeIngestPublication, MetaStoreError> {
    Ok(AuthoritativeIngestPublication {
        reservation: publication
            .reservation
            .ok_or_else(|| MetaStoreError::Serialization("ingest reservation is required".into()))
            .map(ingest_range_reservation_from_proto)?,
        authority_key: publication
            .authority_key
            .ok_or_else(|| {
                MetaStoreError::Serialization("partition authority key is required".into())
            })
            .and_then(partition_authority_key_from_proto)?,
        request_id: publication.request_id,
        request_digest: publication.request_digest,
        object_key: publication.object_key,
        object_digest: publication.object_digest,
    })
}

fn publish_ingest_reservation_request_to_proto(
    request: PublishIngestReservationRequest,
) -> proto::PublishIngestReservationRequest {
    proto::PublishIngestReservationRequest {
        reservation: Some(ingest_range_reservation_to_proto(request.reservation)),
        authority: Some(partition_authority_token_to_proto(request.authority)),
        request_id: request.request_id,
        request_digest: request.request_digest,
        object_key: request.object_key,
        object_digest: request.object_digest,
    }
}

fn publish_ingest_reservation_outcome(outcome: &PublishIngestReservationOutcome) -> &'static str {
    match outcome {
        PublishIngestReservationOutcome::Committed => "committed",
        PublishIngestReservationOutcome::Duplicate => "duplicate",
        PublishIngestReservationOutcome::Conflict => "conflict",
        PublishIngestReservationOutcome::InvalidAuthority => "invalid_authority",
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

fn relation_ingest_capability_to_proto(
    capability: RelationIngestCapability,
) -> proto::RelationIngestCapability {
    proto::RelationIngestCapability {
        backend_name: capability.backend_name,
        relation_scoped_authority: capability.relation_scoped_authority,
        committed_publication_source_cut: capability.committed_publication_source_cut,
        durable_across_restart: capability.durable_across_restart,
    }
}

fn relation_ingest_capability_from_proto(
    capability: proto::RelationIngestCapability,
) -> RelationIngestCapability {
    RelationIngestCapability {
        backend_name: capability.backend_name,
        relation_scoped_authority: capability.relation_scoped_authority,
        committed_publication_source_cut: capability.committed_publication_source_cut,
        durable_across_restart: capability.durable_across_restart,
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

fn relation_partition_authority_key_to_proto(
    key: RelationPartitionAuthorityKey,
) -> proto::RelationPartitionAuthorityKey {
    proto::RelationPartitionAuthorityKey {
        namespace: key.namespace,
        relation_id: key.relation_id,
        stream_id: key.stream_id,
        partition_id: key.partition_id,
    }
}

fn relation_partition_authority_key_from_proto(
    key: proto::RelationPartitionAuthorityKey,
) -> Result<RelationPartitionAuthorityKey, MetaStoreError> {
    let key = RelationPartitionAuthorityKey {
        namespace: key.namespace,
        relation_id: key.relation_id,
        stream_id: key.stream_id,
        partition_id: key.partition_id,
    };
    key.validate()?;
    Ok(key)
}

fn relation_partition_authority_token_to_proto(
    token: RelationPartitionAuthorityToken,
) -> proto::RelationPartitionAuthorityToken {
    proto::RelationPartitionAuthorityToken {
        key: Some(relation_partition_authority_key_to_proto(token.key)),
        owner_id: token.owner_id,
        owner_epoch: token.owner_epoch,
        expires_at_unix_ms: token.expires_at_unix_ms,
    }
}

fn relation_partition_authority_token_from_proto(
    token: proto::RelationPartitionAuthorityToken,
) -> Result<RelationPartitionAuthorityToken, MetaStoreError> {
    let key = token
        .key
        .ok_or_else(|| MetaStoreError::Serialization("relation authority key is required".into()))
        .and_then(relation_partition_authority_key_from_proto)?;
    let token = RelationPartitionAuthorityToken {
        key,
        owner_id: token.owner_id,
        owner_epoch: token.owner_epoch,
        expires_at_unix_ms: token.expires_at_unix_ms,
    };
    token.validate()?;
    Ok(token)
}

fn acquire_relation_partition_authority_request_from_proto(
    request: proto::AcquireRelationPartitionAuthorityRequest,
) -> Result<AcquireRelationPartitionAuthorityRequest, MetaStoreError> {
    let key = request
        .key
        .ok_or_else(|| MetaStoreError::Serialization("relation authority key is required".into()))
        .and_then(relation_partition_authority_key_from_proto)?;
    let request = AcquireRelationPartitionAuthorityRequest {
        key,
        owner_id: request.owner_id,
        current_token: request
            .current_token
            .map(relation_partition_authority_token_from_proto)
            .transpose()?,
        ttl_ms: request.ttl_ms,
    };
    request.validate()?;
    Ok(request)
}

fn relation_authoritative_ingest_publication_to_proto(
    publication: RelationAuthoritativeIngestPublication,
) -> proto::RelationAuthoritativeIngestPublication {
    let relation_version = publication.reservation.relation_version.clone();
    let schema_fingerprint = publication.reservation.schema_fingerprint.clone();
    proto::RelationAuthoritativeIngestPublication {
        reservation: Some(ingest_range_reservation_to_proto(publication.reservation)),
        authority_key: Some(relation_partition_authority_key_to_proto(
            publication.authority_key,
        )),
        request_id: publication.request_id,
        request_digest: publication.request_digest,
        object_key: publication.object_key,
        object_digest: publication.object_digest,
        relation_version,
        schema_fingerprint,
    }
}

fn relation_authoritative_ingest_publication_from_proto(
    publication: proto::RelationAuthoritativeIngestPublication,
) -> Result<RelationAuthoritativeIngestPublication, MetaStoreError> {
    let reservation = publication
        .reservation
        .ok_or_else(|| MetaStoreError::Serialization("ingest reservation is required".into()))
        .map(ingest_range_reservation_from_proto)?;
    if (!publication.relation_version.is_empty()
        && publication.relation_version != reservation.relation_version)
        || (!publication.schema_fingerprint.is_empty()
            && publication.schema_fingerprint != reservation.schema_fingerprint)
    {
        return Err(MetaStoreError::Serialization(
            "relation publication identity does not match reservation".into(),
        ));
    }
    let publication = RelationAuthoritativeIngestPublication {
        reservation,
        authority_key: publication
            .authority_key
            .ok_or_else(|| {
                MetaStoreError::Serialization("relation authority key is required".into())
            })
            .and_then(relation_partition_authority_key_from_proto)?,
        request_id: publication.request_id,
        request_digest: publication.request_digest,
        object_key: publication.object_key,
        object_digest: publication.object_digest,
    };
    publication.reservation.validate()?;
    if publication.authority_key.relation_id != publication.reservation.relation_id
        || publication.authority_key.stream_id != publication.reservation.stream_id
        || publication.authority_key.partition_id != publication.reservation.partition_id
    {
        return Err(MetaStoreError::PartitionAuthorityTokenScopeMismatch);
    }
    Ok(publication)
}

fn reserve_relation_authoritative_ingest_range_request_from_proto(
    request: proto::ReserveRelationAuthoritativeIngestRangeRequest,
) -> Result<ReserveRelationAuthoritativeIngestRangeRequest, MetaStoreError> {
    let reservation = request
        .reservation
        .ok_or_else(|| MetaStoreError::Serialization("ingest reservation is required".into()))
        .map(ingest_range_reservation_from_proto)?;
    let authority = request
        .authority
        .ok_or_else(|| MetaStoreError::Serialization("relation authority token is required".into()))
        .and_then(relation_partition_authority_token_from_proto)?;
    let request = ReserveRelationAuthoritativeIngestRangeRequest {
        reservation,
        authority,
    };
    request.validate()?;
    Ok(request)
}

fn reserve_relation_authoritative_ingest_range_request_to_proto(
    request: ReserveRelationAuthoritativeIngestRangeRequest,
) -> proto::ReserveRelationAuthoritativeIngestRangeRequest {
    proto::ReserveRelationAuthoritativeIngestRangeRequest {
        reservation: Some(ingest_range_reservation_to_proto(request.reservation)),
        authority: Some(relation_partition_authority_token_to_proto(
            request.authority,
        )),
    }
}

fn publish_relation_ingest_reservation_request_from_proto(
    request: proto::PublishRelationIngestReservationRequest,
) -> Result<PublishRelationIngestReservationRequest, MetaStoreError> {
    let reservation = request
        .reservation
        .ok_or_else(|| MetaStoreError::Serialization("ingest reservation is required".into()))
        .map(ingest_range_reservation_from_proto)?;
    let authority = request
        .authority
        .ok_or_else(|| MetaStoreError::Serialization("relation authority token is required".into()))
        .and_then(relation_partition_authority_token_from_proto)?;
    let request = PublishRelationIngestReservationRequest {
        reservation,
        authority,
        request_id: request.request_id,
        request_digest: request.request_digest,
        object_key: request.object_key,
        object_digest: request.object_digest,
    };
    request.validate()?;
    Ok(request)
}

fn publish_relation_ingest_reservation_request_to_proto(
    request: PublishRelationIngestReservationRequest,
) -> proto::PublishRelationIngestReservationRequest {
    proto::PublishRelationIngestReservationRequest {
        reservation: Some(ingest_range_reservation_to_proto(request.reservation)),
        authority: Some(relation_partition_authority_token_to_proto(
            request.authority,
        )),
        request_id: request.request_id,
        request_digest: request.request_digest,
        object_key: request.object_key,
        object_digest: request.object_digest,
    }
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
        PublishStandingRuntimeCheckpointOutcome::SourceCutChanged => "source_cut_changed",
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
        owner_endpoint: claim.owner_endpoint.unwrap_or_default(),
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
        owner_endpoint: (!claim.owner_endpoint.is_empty()).then_some(claim.owner_endpoint),
        owner_epoch: claim.owner_epoch,
        expires_at_unix_ms: claim.expires_at_unix_ms,
    }
}

fn acquire_standing_runtime_owner_outcome_from_proto(
    response: proto::AcquireStandingRuntimeOwnerResponse,
    expected_owner_endpoint: Option<&str>,
) -> Result<AcquireStandingRuntimeOwnerOutcome, MetaStoreError> {
    let claim = response
        .claim
        .ok_or_else(|| MetaStoreError::UnexpectedOutcome("missing owner claim".to_string()))
        .map(standing_runtime_owner_claim_from_proto)?;
    let endpoint_matches = expected_owner_endpoint
        .is_none_or(|expected| claim.owner_endpoint.as_deref() == Some(expected));
    match response.outcome.as_str() {
        "acquired" if endpoint_matches => Ok(AcquireStandingRuntimeOwnerOutcome::Acquired(claim)),
        "renewed" if endpoint_matches => Ok(AcquireStandingRuntimeOwnerOutcome::Renewed(claim)),
        "acquired" | "renewed" => Err(MetaStoreError::UnexpectedOutcome(
            "owner endpoint protocol acknowledgement missing or mismatched".into(),
        )),
        "conflict" => Ok(AcquireStandingRuntimeOwnerOutcome::Conflict(claim)),
        other => Err(MetaStoreError::UnexpectedOutcome(other.to_string())),
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
        published_input_bootstrap: if pointer.published_input_bootstrap_json.is_empty() {
            None
        } else {
            Some(
                serde_json::from_slice(&pointer.published_input_bootstrap_json)
                    .map_err(|error| MetaStoreError::Serialization(error.to_string()))?,
            )
        },
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
        published_input_bootstrap_json: pointer
            .published_input_bootstrap
            .map(|progress| {
                serde_json::to_vec(&progress)
                    .expect("bootstrap progress contains only JSON-safe fields")
            })
            .unwrap_or_default(),
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
        MetaStoreError::SnapshotTooLarge => Status::resource_exhausted(error.to_string()),
        MetaStoreError::ActiveViewRegistryTooLarge => Status::resource_exhausted(error.to_string()),
        MetaStoreError::RelationCatalogNotFound { .. } => Status::not_found(error.to_string()),
        MetaStoreError::RelationCatalogConflict { .. } => Status::already_exists(error.to_string()),
        MetaStoreError::RelationSchema(_)
        | MetaStoreError::EmptyIngestRange { .. }
        | MetaStoreError::EmptyField { .. }
        | MetaStoreError::InvalidBearerToken { .. }
        | MetaStoreError::InvalidDuration { .. }
        | MetaStoreError::IntegerOutOfRange { .. }
        | MetaStoreError::UnsupportedRelationGeneration { .. }
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
        | MetaStoreError::DuplicateDemandMetadataLookup { .. }
        | MetaStoreError::ActiveViewRecordIdentityMismatch
        | MetaStoreError::DuplicateActiveViewId { .. }
        | MetaStoreError::DuplicateActiveViewApiPath { .. }
        | MetaStoreError::DemandMetadataBatchTooLarge { .. }
        | MetaStoreError::OverlappingSourceCutRange { .. }
        | MetaStoreError::IncompleteRelationSourceCut { .. }
        | MetaStoreError::UnexpectedOutcome(_) => Status::invalid_argument(error.to_string()),
        MetaStoreError::UnsupportedCapability(_) => Status::failed_precondition(error.to_string()),
        MetaStoreError::RhizaIndeterminate { .. } => Status::unknown(error.to_string()),
        MetaStoreError::Remote(_)
        | MetaStoreError::Oss(_)
        | MetaStoreError::Rhiza(_)
        | MetaStoreError::RhizaContention { .. } => Status::unavailable(error.to_string()),
    }
}

fn partition_authority_status(error: MetaStoreError) -> Status {
    match error {
        MetaStoreError::SnapshotTooLarge => Status::resource_exhausted(error.to_string()),
        MetaStoreError::ActiveViewRegistryTooLarge => Status::resource_exhausted(error.to_string()),
        MetaStoreError::EmptyField { .. }
        | MetaStoreError::InvalidDuration { .. }
        | MetaStoreError::IntegerOutOfRange { .. }
        | MetaStoreError::UnsupportedRelationGeneration { .. }
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
        | MetaStoreError::DuplicateDemandMetadataLookup { .. }
        | MetaStoreError::ActiveViewRecordIdentityMismatch
        | MetaStoreError::DuplicateActiveViewId { .. }
        | MetaStoreError::DuplicateActiveViewApiPath { .. }
        | MetaStoreError::DemandMetadataBatchTooLarge { .. }
        | MetaStoreError::OverlappingSourceCutRange { .. }
        | MetaStoreError::IncompleteRelationSourceCut { .. }
        | MetaStoreError::NonMonotonicCheckpointEpoch { .. }
        | MetaStoreError::Remote(_)
        | MetaStoreError::Oss(_)
        | MetaStoreError::Rhiza(_)
        | MetaStoreError::RhizaIndeterminate { .. }
        | MetaStoreError::RhizaContention { .. }
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
    endpoint: Endpoint,
    bearer_token: Option<MetadataValue<tonic::metadata::Ascii>>,
}

/// Explicit TLS material for a metadata client. The CA is always required;
/// client identity is optional for one-way TLS and required by native-mTLS
/// servers. No insecure or certificate-verification bypass is exposed.
#[derive(Clone, Debug)]
pub struct GrpcClientTlsConfig {
    pub domain_name: String,
    pub ca_cert_pem: Vec<u8>,
    pub client_cert_pem: Option<Vec<u8>>,
    pub client_key_pem: Option<Vec<u8>>,
}

impl GrpcMetaStore {
    pub async fn connect(endpoint: impl AsRef<str>) -> Result<Self, MetaStoreError> {
        Self::connect_inner(endpoint.as_ref(), None).await
    }

    pub async fn connect_with_tls(
        endpoint: impl AsRef<str>,
        tls: GrpcClientTlsConfig,
    ) -> Result<Self, MetaStoreError> {
        Self::connect_inner(endpoint.as_ref(), Some(tls)).await
    }

    async fn connect_inner(
        endpoint: &str,
        tls: Option<GrpcClientTlsConfig>,
    ) -> Result<Self, MetaStoreError> {
        // Bound both the initial dial and every RPC made through this channel.
        // In particular, a client used for readiness must not hang forever when
        // the Rhiza quorum disappears.
        let mut channel = Endpoint::from_shared(endpoint.to_string())
            .map(|endpoint| {
                endpoint
                    .connect_timeout(std::time::Duration::from_secs(30))
                    .timeout(std::time::Duration::from_secs(30))
            })
            .map_err(|error| MetaStoreError::Remote(error.to_string()))?;
        if let Some(tls) = tls {
            let _ = rustls::crypto::ring::default_provider().install_default();
            if tls.domain_name.trim().is_empty() || tls.ca_cert_pem.is_empty() {
                return Err(MetaStoreError::Serialization(
                    "metadata TLS requires a domain name and CA certificate".into(),
                ));
            }
            let client_identity = match (tls.client_cert_pem, tls.client_key_pem) {
                (Some(cert), Some(key)) if !cert.is_empty() && !key.is_empty() => {
                    Some(Identity::from_pem(cert, key))
                }
                (None, None) => None,
                _ => {
                    return Err(MetaStoreError::Serialization(
                        "metadata TLS client certificate and key must be supplied together".into(),
                    ));
                }
            };
            let mut config = ClientTlsConfig::new()
                .domain_name(tls.domain_name)
                .ca_certificate(tonic::transport::Certificate::from_pem(tls.ca_cert_pem));
            if let Some(identity) = client_identity {
                config = config.identity(identity);
            }
            channel = channel
                .tls_config(config)
                .map_err(|error| MetaStoreError::Remote(error.to_string()))?;
        }
        let endpoint = channel;
        let channel = endpoint
            .connect()
            .await
            .map_err(|error| MetaStoreError::Remote(error.to_string()))?;
        let client = proto::velorix_meta_client::VelorixMetaClient::new(channel)
            .max_decoding_message_size(MAX_META_GRPC_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_META_GRPC_MESSAGE_BYTES);

        Ok(Self {
            client,
            endpoint,
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

    pub async fn connect_with_bearer_token_and_tls(
        endpoint: impl AsRef<str>,
        bearer_token: impl Into<String>,
        tls: GrpcClientTlsConfig,
    ) -> Result<Self, MetaStoreError> {
        let mut store = Self::connect_with_tls(endpoint, tls).await?;
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
    async fn isolated_owner_renewal_store(
        &self,
    ) -> Result<Option<Arc<dyn MetaStore>>, MetaStoreError> {
        // Reconnect on the renewal executor: Channel::clone shares the original
        // buffer worker and connection driver instead of migrating either one.
        let channel = self
            .endpoint
            .connect()
            .await
            .map_err(|error| MetaStoreError::Remote(error.to_string()))?;
        Ok(Some(Arc::new(Self {
            client: proto::velorix_meta_client::VelorixMetaClient::new(channel)
                .max_decoding_message_size(MAX_META_GRPC_MESSAGE_BYTES)
                .max_encoding_message_size(MAX_META_GRPC_MESSAGE_BYTES),
            endpoint: self.endpoint.clone(),
            bearer_token: self.bearer_token.clone(),
        })))
    }

    async fn read_active_view_registry_snapshot(
        &self,
        request: ActiveViewRegistryReadRequestV1,
    ) -> Result<ActiveViewRegistrySnapshotV1, MetaStoreError> {
        let request_json = serde_json::to_vec(&request)
            .map_err(|error| MetaStoreError::Serialization(error.to_string()))?;
        let response = self
            .client()
            .read_active_view_registry_snapshot(
                self.request(proto::ActiveViewRegistrySnapshotRequest { request_json }),
            )
            .await
            .map_err(|error| MetaStoreError::Remote(error.to_string()))?
            .into_inner();
        serde_json::from_slice(&response.response_json)
            .map_err(|error| MetaStoreError::Serialization(error.to_string()))
    }

    async fn create_active_view_record(
        &self,
        request: CreateActiveViewRecordRequestV1,
    ) -> Result<CreateActiveViewRecordOutcomeV1, MetaStoreError> {
        let request_json = serde_json::to_vec(&request)
            .map_err(|error| MetaStoreError::Serialization(error.to_string()))?;
        let response = self
            .client()
            .create_active_view_record(
                self.request(proto::ActiveViewRecordMutationRequest { request_json }),
            )
            .await
            .map_err(|error| MetaStoreError::Remote(error.to_string()))?
            .into_inner();
        serde_json::from_slice(&response.response_json)
            .map_err(|error| MetaStoreError::Serialization(error.to_string()))
    }

    async fn compare_exchange_active_view_record(
        &self,
        request: CompareExchangeActiveViewRecordRequestV1,
    ) -> Result<CompareExchangeActiveViewRecordOutcomeV1, MetaStoreError> {
        let request_json = serde_json::to_vec(&request)
            .map_err(|error| MetaStoreError::Serialization(error.to_string()))?;
        let response = self
            .client()
            .compare_exchange_active_view_record(
                self.request(proto::ActiveViewRecordMutationRequest { request_json }),
            )
            .await
            .map_err(|error| MetaStoreError::Remote(error.to_string()))?
            .into_inner();
        serde_json::from_slice(&response.response_json)
            .map_err(|error| MetaStoreError::Serialization(error.to_string()))
    }

    async fn import_active_view_registry(
        &self,
        request: ImportActiveViewRegistryRequestV1,
    ) -> Result<ImportActiveViewRegistryOutcomeV1, MetaStoreError> {
        let request_json = serde_json::to_vec(&request)
            .map_err(|error| MetaStoreError::Serialization(error.to_string()))?;
        let response = self
            .client()
            .import_active_view_registry(
                self.request(proto::ActiveViewRecordMutationRequest { request_json }),
            )
            .await
            .map_err(|error| MetaStoreError::Remote(error.to_string()))?
            .into_inner();
        serde_json::from_slice(&response.response_json)
            .map_err(|error| MetaStoreError::Serialization(error.to_string()))
    }

    async fn promote_view_bootstrap_with_active_view_record(
        &self,
        request: PromoteViewBootstrapWithActiveViewRecordRequestV1,
    ) -> Result<PromoteViewBootstrapOutcome, MetaStoreError> {
        let request_json = serde_json::to_vec(&request)
            .map_err(|error| MetaStoreError::Serialization(error.to_string()))?;
        let response = self
            .client()
            .promote_view_bootstrap_with_active_view_record(
                self.request(proto::ActiveViewRecordMutationRequest { request_json }),
            )
            .await
            .map_err(|error| MetaStoreError::Remote(error.to_string()))?
            .into_inner();
        serde_json::from_slice(&response.response_json)
            .map_err(|error| MetaStoreError::Serialization(error.to_string()))
    }

    // Relation-scoped authority forwarding is kept separate from the legacy
    // view-scoped methods below.

    async fn acquire_relation_partition_authority(
        &self,
        request: AcquireRelationPartitionAuthorityRequest,
    ) -> Result<AcquireRelationPartitionAuthorityOutcome, MetaStoreError> {
        request.validate()?;
        let response = self
            .client()
            .acquire_relation_partition_authority(
                self.request(proto::AcquireRelationPartitionAuthorityRequest {
                    key: Some(relation_partition_authority_key_to_proto(request.key)),
                    owner_id: request.owner_id,
                    current_token: request
                        .current_token
                        .map(relation_partition_authority_token_to_proto),
                    ttl_ms: request.ttl_ms,
                }),
            )
            .await
            .map_err(partition_authority_remote_error)?
            .into_inner();
        let token = response
            .token
            .ok_or_else(|| {
                MetaStoreError::UnexpectedOutcome("missing relation authority token".into())
            })
            .and_then(relation_partition_authority_token_from_proto)?;
        match response.outcome.as_str() {
            "acquired" => Ok(AcquireRelationPartitionAuthorityOutcome::Acquired(token)),
            "renewed" => Ok(AcquireRelationPartitionAuthorityOutcome::Renewed(token)),
            "conflict" => Ok(AcquireRelationPartitionAuthorityOutcome::Conflict(token)),
            other => Err(MetaStoreError::UnexpectedOutcome(other.to_string())),
        }
    }

    async fn read_relation_partition_authority(
        &self,
        key: &RelationPartitionAuthorityKey,
    ) -> Result<Option<RelationPartitionAuthorityToken>, MetaStoreError> {
        key.validate()?;
        let response = self
            .client()
            .read_relation_partition_authority(self.request(
                proto::ReadRelationPartitionAuthorityRequest {
                    key: Some(relation_partition_authority_key_to_proto(key.clone())),
                },
            ))
            .await
            .map_err(partition_authority_remote_error)?
            .into_inner();
        match (response.found, response.token) {
            (true, Some(token)) => relation_partition_authority_token_from_proto(token).map(Some),
            (false, None) => Ok(None),
            (true, None) => Err(MetaStoreError::UnexpectedOutcome(
                "missing relation authority token".into(),
            )),
            (false, Some(_)) => Err(MetaStoreError::UnexpectedOutcome(
                "relation authority response has a token without found".into(),
            )),
        }
    }

    async fn reserve_relation_authoritative_ingest_range(
        &self,
        request: ReserveRelationAuthoritativeIngestRangeRequest,
    ) -> Result<ReserveIngestRangeOutcome, MetaStoreError> {
        request.validate()?;
        let response = self
            .client()
            .reserve_relation_authoritative_ingest_range(
                self.request(reserve_relation_authoritative_ingest_range_request_to_proto(request)),
            )
            .await
            .map_err(partition_authority_remote_error)?
            .into_inner();
        match response.outcome.as_str() {
            "reserved" => Ok(ReserveIngestRangeOutcome::Reserved),
            "duplicate" => Ok(ReserveIngestRangeOutcome::Duplicate),
            "conflict" => Ok(ReserveIngestRangeOutcome::Conflict),
            other => Err(MetaStoreError::UnexpectedOutcome(other.to_string())),
        }
    }

    async fn publish_relation_ingest_reservation(
        &self,
        request: PublishRelationIngestReservationRequest,
    ) -> Result<PublishIngestReservationOutcome, MetaStoreError> {
        request.validate()?;
        let response = self
            .client()
            .publish_relation_ingest_reservation(self.request(
                publish_relation_ingest_reservation_request_to_proto(request),
            ))
            .await
            .map_err(partition_authority_remote_error)?
            .into_inner();
        match response.outcome.as_str() {
            "committed" => Ok(PublishIngestReservationOutcome::Committed),
            "duplicate" => Ok(PublishIngestReservationOutcome::Duplicate),
            "conflict" => Ok(PublishIngestReservationOutcome::Conflict),
            "invalid_authority" => Ok(PublishIngestReservationOutcome::InvalidAuthority),
            other => Err(MetaStoreError::UnexpectedOutcome(other.to_string())),
        }
    }

    async fn read_relation_authoritative_ingest_publication(
        &self,
        request_id: &str,
    ) -> Result<Option<RelationAuthoritativeIngestPublication>, MetaStoreError> {
        require_non_empty("request_id", request_id)?;
        let response = self
            .client()
            .read_relation_authoritative_ingest_publication(self.request(
                proto::ReadRelationAuthoritativeIngestPublicationRequest {
                    request_id: request_id.to_string(),
                },
            ))
            .await
            .map_err(partition_authority_remote_error)?
            .into_inner();
        response
            .publication
            .map(relation_authoritative_ingest_publication_from_proto)
            .transpose()
    }

    async fn list_relation_authoritative_ingest_publications(
        &self,
        key: &RelationPartitionAuthorityKey,
    ) -> Result<Vec<RelationAuthoritativeIngestPublication>, MetaStoreError> {
        key.validate()?;
        let response = self
            .client()
            .list_relation_authoritative_ingest_publications(self.request(
                proto::ListRelationAuthoritativeIngestPublicationsRequest {
                    key: Some(relation_partition_authority_key_to_proto(key.clone())),
                },
            ))
            .await
            .map_err(partition_authority_remote_error)?
            .into_inner();
        response
            .publications
            .into_iter()
            .map(relation_authoritative_ingest_publication_from_proto)
            .collect()
    }
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
            relation_ingest: response
                .relation_ingest
                .map(relation_ingest_capability_from_proto)
                .unwrap_or_default(),
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

    async fn publish_ingest_reservation(
        &self,
        request: PublishIngestReservationRequest,
    ) -> Result<PublishIngestReservationOutcome, MetaStoreError> {
        request.validate()?;
        let response = self
            .client()
            .publish_ingest_reservation(
                self.request(publish_ingest_reservation_request_to_proto(request)),
            )
            .await
            .map_err(partition_authority_remote_error)?
            .into_inner();
        match response.outcome.as_str() {
            "committed" => Ok(PublishIngestReservationOutcome::Committed),
            "duplicate" => Ok(PublishIngestReservationOutcome::Duplicate),
            "conflict" => Ok(PublishIngestReservationOutcome::Conflict),
            "invalid_authority" => Ok(PublishIngestReservationOutcome::InvalidAuthority),
            other => Err(MetaStoreError::UnexpectedOutcome(other.to_string())),
        }
    }

    async fn read_authoritative_ingest_publication(
        &self,
        request_id: &str,
    ) -> Result<Option<AuthoritativeIngestPublication>, MetaStoreError> {
        require_non_empty("request_id", request_id)?;
        let response = self
            .client()
            .read_authoritative_ingest_publication(self.request(
                proto::ReadAuthoritativeIngestPublicationRequest {
                    request_id: request_id.to_string(),
                },
            ))
            .await
            .map_err(partition_authority_remote_error)?
            .into_inner();
        response
            .publication
            .map(authoritative_ingest_publication_from_proto)
            .transpose()
    }

    async fn list_authoritative_ingest_publications(
        &self,
        key: &PartitionAuthorityKey,
    ) -> Result<Vec<AuthoritativeIngestPublication>, MetaStoreError> {
        key.validate()?;
        let response = self
            .client()
            .list_authoritative_ingest_publications(self.request(
                proto::ListAuthoritativeIngestPublicationsRequest {
                    key: Some(partition_authority_key_to_proto(key.clone())),
                },
            ))
            .await
            .map_err(partition_authority_remote_error)?
            .into_inner();
        response
            .publications
            .into_iter()
            .map(authoritative_ingest_publication_from_proto)
            .collect()
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

    async fn read_demand_metadata_batch(
        &self,
        request: ReadDemandMetadataBatchRequest,
    ) -> Result<ReadDemandMetadataBatchResponse, MetaStoreError> {
        request.validate()?;
        let request_json = serde_json::to_vec(&request)
            .map_err(|error| MetaStoreError::Serialization(error.to_string()))?;
        let response = self
            .client()
            .read_demand_metadata_batch(
                self.request(proto::ReadDemandMetadataBatchRequest { request_json }),
            )
            .await
            .map_err(|error| MetaStoreError::Remote(error.to_string()))?
            .into_inner();
        let response: ReadDemandMetadataBatchResponse =
            serde_json::from_slice(&response.response_json)
                .map_err(|error| MetaStoreError::Serialization(error.to_string()))?;
        response.validate_for(&request)?;
        Ok(response)
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

    async fn reserve_authoritative_ingest_range(
        &self,
        request: ReserveAuthoritativeIngestRangeRequest,
    ) -> Result<ReserveIngestRangeOutcome, MetaStoreError> {
        request.validate()?;
        let response = self
            .client()
            .reserve_authoritative_ingest_range(
                self.request(reserve_authoritative_ingest_range_request_to_proto(request)),
            )
            .await
            .map_err(partition_authority_remote_error)?
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

    async fn capture_relation_ingest_source_cut(
        &self,
        request: CaptureRelationIngestSourceCutRequest,
    ) -> Result<RelationIngestSourceCutV1, MetaStoreError> {
        request.validate()?;
        let response = self
            .client()
            .capture_relation_ingest_source_cut(self.request(
                proto::CaptureRelationIngestSourceCutRequest {
                    authority: Some(relation_partition_authority_key_to_proto(request.authority)),
                    relation_version: request.relation_version,
                    schema_fingerprint: request.schema_fingerprint,
                },
            ))
            .await
            .map_err(|error| MetaStoreError::Remote(error.to_string()))?
            .into_inner();
        serde_json::from_slice(&response.source_cut_json)
            .map_err(|error| MetaStoreError::Serialization(error.to_string()))
    }

    async fn capture_relation_ingest_source_cuts(
        &self,
        request: CaptureRelationIngestSourceCutsRequest,
    ) -> Result<Vec<RelationIngestSourceIdentityCutV1>, MetaStoreError> {
        let request_json = serde_json::to_vec(&request)
            .map_err(|error| MetaStoreError::Serialization(error.to_string()))?;
        let response = self
            .client()
            .capture_relation_ingest_source_cuts(
                self.request(proto::CaptureRelationIngestSourceCutsRequest { request_json }),
            )
            .await
            .map_err(|error| MetaStoreError::Remote(error.to_string()))?
            .into_inner();
        serde_json::from_slice(&response.source_cuts_json)
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
                    owner_endpoint: String::new(),
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
        acquire_standing_runtime_owner_outcome_from_proto(response, None)
    }

    async fn acquire_standing_runtime_owner_with_endpoint(
        &self,
        request: AcquireStandingRuntimeOwnerRequest,
        owner_endpoint: String,
    ) -> Result<AcquireStandingRuntimeOwnerOutcome, MetaStoreError> {
        validate_owner_endpoint(&owner_endpoint)?;
        let expected_owner_endpoint = owner_endpoint.clone();
        let response = self
            .client()
            .acquire_standing_runtime_owner(self.request(
                proto::AcquireStandingRuntimeOwnerRequest {
                    tenant_id: request.tenant_id,
                    program_id: request.program_id,
                    view_id: request.view_id,
                    owner_id: request.owner_id,
                    ttl_ms: request.ttl_ms,
                    owner_endpoint,
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
        acquire_standing_runtime_owner_outcome_from_proto(response, Some(&expected_owner_endpoint))
    }

    async fn renew_standing_runtime_owner(
        &self,
        owner: StandingRuntimeOwnerToken,
        ttl_ms: u64,
    ) -> Result<StandingRuntimeOwnerClaim, MetaStoreError> {
        let response = self
            .client()
            .renew_standing_runtime_owner(self.request(proto::RenewStandingRuntimeOwnerRequest {
                owner: Some(standing_runtime_owner_token_to_proto(owner)),
                ttl_ms,
            }))
            .await
            .map_err(|error| MetaStoreError::Remote(error.to_string()))?
            .into_inner();
        if response.outcome != "renewed" {
            return Err(MetaStoreError::UnexpectedOutcome(response.outcome));
        }
        response
            .claim
            .map(standing_runtime_owner_claim_from_proto)
            .ok_or_else(|| MetaStoreError::UnexpectedOutcome("missing renewed owner claim".into()))
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
        let guarded = request.expected_relation_source_cuts.is_some();
        let expected_relation_source_cuts_json = request
            .expected_relation_source_cuts
            .as_ref()
            .map(serde_json::to_vec)
            .transpose()
            .map_err(|error| MetaStoreError::Serialization(error.to_string()))?
            .unwrap_or_default();
        let request = self.request(proto::PublishStandingRuntimeCheckpointRequest {
            expected_previous: request
                .expected_previous
                .map(standing_runtime_checkpoint_pointer_to_proto),
            candidate: Some(standing_runtime_checkpoint_pointer_to_proto(
                request.candidate,
            )),
            owner: Some(standing_runtime_owner_token_to_proto(request.owner)),
            expected_relation_source_cuts_json,
        });
        let response = if guarded {
            self.client()
                .publish_standing_runtime_checkpoint_guarded(request)
                .await
        } else {
            self.client()
                .publish_standing_runtime_checkpoint(request)
                .await
        }
        .map_err(|error| match error.code() {
            tonic::Code::Unimplemented if guarded => MetaStoreError::UnsupportedCapability(
                "guarded_standing_runtime_checkpoint_source_cut_publish",
            ),
            tonic::Code::FailedPrecondition if guarded => MetaStoreError::UnsupportedCapability(
                "guarded_standing_runtime_checkpoint_source_cut_publish",
            ),
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
            "source_cut_changed" => Ok(PublishStandingRuntimeCheckpointOutcome::SourceCutChanged),
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

#[cfg(test)]
mod relation_partition_authority_tests {
    use super::*;

    fn key(relation_id: &str) -> RelationPartitionAuthorityKey {
        RelationPartitionAuthorityKey {
            namespace: "tenant".into(),
            relation_id: relation_id.into(),
            stream_id: "stream".into(),
            partition_id: 0,
        }
    }

    fn reservation(relation_id: &str) -> IngestRangeReservation {
        IngestRangeReservation {
            stream_id: "stream".into(),
            partition_id: 0,
            start_offset_inclusive: 0,
            end_offset_exclusive: 1,
            batch_key: format!("{relation_id}-batch"),
            payload_digest: format!("{relation_id}-digest"),
            relation_id: relation_id.into(),
            relation_version: "v1".into(),
            schema_fingerprint: "schema".into(),
            writer_epoch: 1,
        }
    }

    #[tokio::test]
    async fn relation_authority_fences_scope_takeover_and_duplicate_publication() {
        let store = InMemoryMetaStore::default();
        store.set_partition_authority_clock_for_test(100).await;
        let a = match store
            .acquire_relation_partition_authority(AcquireRelationPartitionAuthorityRequest {
                key: key("relation-a"),
                owner_id: "owner-a".into(),
                current_token: None,
                ttl_ms: 10,
            })
            .await
            .unwrap()
        {
            AcquireRelationPartitionAuthorityOutcome::Acquired(token) => token,
            other => panic!("unexpected acquire outcome: {other:?}"),
        };
        let mut wrong = reservation("relation-b");
        wrong.batch_key = "wrong-batch".into();
        assert!(matches!(
            store
                .reserve_relation_authoritative_ingest_range(
                    ReserveRelationAuthoritativeIngestRangeRequest {
                        reservation: wrong,
                        authority: a.clone(),
                    },
                )
                .await,
            Err(MetaStoreError::PartitionAuthorityTokenScopeMismatch)
        ));
        let r = reservation("relation-a");
        assert_eq!(
            store
                .reserve_relation_authoritative_ingest_range(
                    ReserveRelationAuthoritativeIngestRangeRequest {
                        reservation: r.clone(),
                        authority: a.clone(),
                    },
                )
                .await
                .unwrap(),
            ReserveIngestRangeOutcome::Reserved
        );
        let publish = || PublishRelationIngestReservationRequest {
            reservation: r.clone(),
            authority: a.clone(),
            request_id: "request-a".into(),
            request_digest: "request-digest".into(),
            object_key: "staging/a".into(),
            object_digest: "object-a".into(),
        };
        assert_eq!(
            store
                .publish_relation_ingest_reservation(publish())
                .await
                .unwrap(),
            PublishIngestReservationOutcome::Committed
        );
        assert_eq!(
            store
                .publish_relation_ingest_reservation(publish())
                .await
                .unwrap(),
            PublishIngestReservationOutcome::Duplicate
        );
        store.set_partition_authority_clock_for_test(200).await;
        let b = match store
            .acquire_relation_partition_authority(AcquireRelationPartitionAuthorityRequest {
                key: key("relation-a"),
                owner_id: "owner-b".into(),
                current_token: None,
                ttl_ms: 10,
            })
            .await
            .unwrap()
        {
            AcquireRelationPartitionAuthorityOutcome::Acquired(token) => token,
            other => panic!("unexpected takeover outcome: {other:?}"),
        };
        assert_ne!(a.owner_epoch, b.owner_epoch);
        let mut stale_publish = publish();
        stale_publish.request_id = "request-a-stale".into();
        assert_eq!(
            store
                .publish_relation_ingest_reservation(stale_publish)
                .await
                .unwrap(),
            PublishIngestReservationOutcome::InvalidAuthority
        );
    }

    #[test]
    fn relation_contract_is_present_in_meta_paths() {
        let source = include_str!("lib.rs");
        assert!(source.contains("velorix_relation_partition_authorities"));
        assert!(source.contains("velorix_relation_ingest_reservations"));
        assert!(source.contains("async fn acquire_relation_partition_authority"));
        assert!(source.contains("async fn list_relation_authoritative_ingest_publications"));
    }

    #[tokio::test]
    async fn conditional_owner_renewal_preserves_epoch_and_fences_expiry_and_takeover() {
        let store = InMemoryMetaStore::from_state_for_evaluation(InMemoryMetaState::default(), 100);
        let acquire = |owner_id: &str| AcquireStandingRuntimeOwnerRequest {
            tenant_id: "tenant".into(),
            program_id: "program".into(),
            view_id: "view".into(),
            owner_id: owner_id.into(),
            ttl_ms: 10,
        };
        let original = match store
            .acquire_standing_runtime_owner(acquire("a"))
            .await
            .unwrap()
        {
            AcquireStandingRuntimeOwnerOutcome::Acquired(claim) => claim,
            other => panic!("unexpected {other:?}"),
        };
        let before =
            InMemoryMetaStore::from_state_for_evaluation(store.snapshot_state().await, 109);
        let renewed = before
            .renew_standing_runtime_owner(original.token(), 10)
            .await
            .unwrap();
        assert_eq!(renewed.token(), original.token());
        assert_eq!(renewed.expires_at_unix_ms, 119);
        let mut foreign = original.token();
        foreign.owner_id = "b".into();
        assert!(matches!(
            before.renew_standing_runtime_owner(foreign, 10).await,
            Err(MetaStoreError::StandingRuntimeOwnerMismatch)
        ));
        let expired =
            InMemoryMetaStore::from_state_for_evaluation(before.snapshot_state().await, 119);
        assert!(matches!(
            expired
                .renew_standing_runtime_owner(original.token(), 10)
                .await,
            Err(MetaStoreError::StandingRuntimeOwnerMismatch)
        ));
        let takeover = match expired
            .acquire_standing_runtime_owner(acquire("b"))
            .await
            .unwrap()
        {
            AcquireStandingRuntimeOwnerOutcome::Acquired(claim) => claim,
            other => panic!("unexpected {other:?}"),
        };
        assert_eq!(takeover.owner_epoch, original.owner_epoch + 1);
        assert!(matches!(
            expired
                .renew_standing_runtime_owner(original.token(), 10)
                .await,
            Err(MetaStoreError::StandingRuntimeOwnerMismatch)
        ));
    }

    #[tokio::test]
    async fn in_memory_fixed_evaluation_time_reproduces_owner_expiry() {
        let store = InMemoryMetaStore::from_state_for_evaluation(InMemoryMetaState::default(), 100);
        let claim = match store
            .acquire_standing_runtime_owner(AcquireStandingRuntimeOwnerRequest {
                tenant_id: "tenant".into(),
                program_id: "program".into(),
                view_id: "view".into(),
                owner_id: "owner".into(),
                ttl_ms: 10,
            })
            .await
            .unwrap()
        {
            AcquireStandingRuntimeOwnerOutcome::Acquired(claim) => claim,
            other => panic!("unexpected outcome: {other:?}"),
        };
        assert_eq!(claim.expires_at_unix_ms, 110);
        let before_expiry =
            InMemoryMetaStore::from_state_for_evaluation(store.snapshot_state().await, 109);
        assert!(before_expiry
            .read_standing_runtime_owner("tenant", "program", "view")
            .await
            .unwrap()
            .is_some());
        let after_expiry =
            InMemoryMetaStore::from_state_for_evaluation(store.snapshot_state().await, 110);
        assert!(after_expiry
            .read_standing_runtime_owner("tenant", "program", "view")
            .await
            .unwrap()
            .is_none());
    }
}

#[cfg(test)]
mod demand_metadata_batch_validation_tests {
    use super::*;

    fn catalog(relation_id: &str, version: &str) -> VelorixRelationCatalogV1 {
        use velorix_core::relation::{
            ArrowPhysicalTypeV1, RelationColumnV1, RelationOperationV1, RelationSemanticRoleV1,
            VelorixLogicalTypeV1, VelorixRelationSchemaV1, CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID,
        };

        let relation_schema = VelorixRelationSchemaV1 {
            relation_id: relation_id.into(),
            relation_name: relation_id.into(),
            relation_version: version.into(),
            columns: vec![RelationColumnV1 {
                column_id: "weight".into(),
                name: "weight".into(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 0,
                semantic_role: RelationSemanticRoleV1::Weight,
            }],
            primary_key_column_ids: vec![],
            weight_column_id: "weight".into(),
            allowed_operations: vec![RelationOperationV1::Insert],
            event_time_column_id: None,
        };
        VelorixRelationCatalogV1::from_relation_schema(
            relation_schema,
            CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.into(),
        )
        .unwrap()
    }

    fn request(source_cut_relation_count: usize) -> ReadDemandMetadataBatchRequest {
        ReadDemandMetadataBatchRequest {
            source_cuts: Some(CaptureRelationIngestSourceCutsRequest {
                namespace: "tenant".into(),
                relations: (0..source_cut_relation_count)
                    .map(|index| IngestSourceRelationIdentityV1 {
                        relation_id: format!("source-{index}"),
                        relation_version: "v1".into(),
                        relation_generation: INGEST_SOURCE_IDENTITY_GENERATION_V1,
                        schema_fingerprint: "schema".into(),
                    })
                    .collect(),
            }),
            relation_catalogs: (0..62)
                .map(|index| DemandRelationCatalogLookup {
                    relation_id: format!("catalog-{index}"),
                    relation_version: "v1".into(),
                })
                .collect(),
            standing_runtime_checkpoint: Some(DemandStandingRuntimeCheckpointLookup {
                tenant_id: "tenant".into(),
                program_id: "program".into(),
                view_id: "view".into(),
            }),
        }
    }

    #[test]
    fn demand_metadata_batch_limit_counts_each_source_relation() {
        assert!(request(1).validate().is_ok());
        assert!(matches!(
            request(2).validate(),
            Err(MetaStoreError::DemandMetadataBatchTooLarge {
                actual: 65,
                max: MAX_DEMAND_METADATA_BATCH_LOOKUPS,
            })
        ));
    }

    #[tokio::test]
    async fn batch_source_cut_ignores_unrequested_relation_records() {
        let store = InMemoryMetaStore::default();
        let orders_catalog = catalog("orders", "v1");
        let orders_fingerprint = orders_catalog.schema_fingerprint.to_string();
        let inventory_catalog = catalog("inventory", "v1");
        let inventory_fingerprint = inventory_catalog.schema_fingerprint.to_string();
        let requested = IngestRangeReservation {
            stream_id: "orders-stream".into(),
            partition_id: 0,
            start_offset_inclusive: 0,
            end_offset_exclusive: 10,
            batch_key: "orders-batch".into(),
            payload_digest: "sha256:orders".into(),
            relation_id: "orders".into(),
            relation_version: "v1".into(),
            schema_fingerprint: orders_fingerprint.clone(),
            writer_epoch: 1,
        };
        let unrelated = IngestRangeReservation {
            stream_id: "customers-stream".into(),
            partition_id: 0,
            start_offset_inclusive: 0,
            end_offset_exclusive: 20,
            batch_key: "customers-batch".into(),
            payload_digest: "sha256:customers".into(),
            relation_id: "customers".into(),
            relation_version: "v2".into(),
            schema_fingerprint: "sha256:customers-schema".into(),
            writer_epoch: 2,
        };
        let requested_authority = RelationPartitionAuthorityKey {
            namespace: "tenant".into(),
            relation_id: "orders".into(),
            stream_id: requested.stream_id.clone(),
            partition_id: requested.partition_id,
        };
        let unrelated_authority = RelationPartitionAuthorityKey {
            namespace: "tenant".into(),
            relation_id: "customers".into(),
            stream_id: unrelated.stream_id.clone(),
            partition_id: unrelated.partition_id,
        };
        {
            let mut state = store.inner.write().await;
            state
                .relation_catalogs
                .insert(("orders".into(), "v1".into()), orders_catalog);
            state
                .relation_catalogs
                .insert(("inventory".into(), "v1".into()), inventory_catalog);
            state.relation_ingest_reservations.insert(
                (requested.stream_id.clone(), requested.partition_id),
                vec![requested.clone()],
            );
            state.relation_ingest_reservations.insert(
                (unrelated.stream_id.clone(), unrelated.partition_id),
                vec![unrelated.clone()],
            );
            state
                .relation_authority_reservation_keys
                .insert(requested.clone(), requested_authority.clone());
            state
                .relation_authority_reservation_keys
                .insert(unrelated.clone(), unrelated_authority.clone());
            state.relation_authoritative_ingest_publications.insert(
                "orders-request".into(),
                RelationAuthoritativeIngestPublication {
                    reservation: requested,
                    authority_key: requested_authority,
                    request_id: "orders-request".into(),
                    request_digest: "sha256:orders-request".into(),
                    object_key: "objects/orders".into(),
                    object_digest: "sha256:orders-object".into(),
                },
            );
            state.relation_authoritative_ingest_publications.insert(
                "customers-request".into(),
                RelationAuthoritativeIngestPublication {
                    reservation: unrelated,
                    authority_key: unrelated_authority,
                    request_id: "customers-request".into(),
                    request_digest: "sha256:customers-request".into(),
                    object_key: "objects/customers".into(),
                    object_digest: "sha256:customers-object".into(),
                },
            );
        }

        let result = store
            .read_demand_metadata_batch(ReadDemandMetadataBatchRequest {
                source_cuts: Some(CaptureRelationIngestSourceCutsRequest {
                    namespace: "tenant".into(),
                    relations: vec![
                        IngestSourceRelationIdentityV1 {
                            relation_id: "orders".into(),
                            relation_version: "v1".into(),
                            relation_generation: INGEST_SOURCE_IDENTITY_GENERATION_V1,
                            schema_fingerprint: orders_fingerprint,
                        },
                        IngestSourceRelationIdentityV1 {
                            relation_id: "inventory".into(),
                            relation_version: "v1".into(),
                            relation_generation: INGEST_SOURCE_IDENTITY_GENERATION_V1,
                            schema_fingerprint: inventory_fingerprint,
                        },
                    ],
                }),
                relation_catalogs: vec![],
                standing_runtime_checkpoint: None,
            })
            .await
            .unwrap();

        let cuts = result.source_cuts.unwrap();
        assert_eq!(cuts.len(), 2);
        assert_eq!(cuts[0].relation.relation_id, "orders");
        assert_eq!(cuts[0].cut.partitions[0].publications.len(), 1);
        assert_eq!(
            cuts[0].cut.partitions[0].publications[0].request_id,
            "orders-request"
        );
        assert_eq!(cuts[1].relation.relation_id, "inventory");
        assert!(cuts[1].cut.partitions.is_empty());
    }

    #[tokio::test]
    async fn batch_rejects_source_cut_fingerprint_that_differs_from_catalog() {
        let store = InMemoryMetaStore::default();
        let relation_catalog = catalog("orders", "v1");
        store
            .inner
            .write()
            .await
            .relation_catalogs
            .insert(("orders".into(), "v1".into()), relation_catalog);

        let result = store
            .read_demand_metadata_batch(ReadDemandMetadataBatchRequest {
                source_cuts: Some(CaptureRelationIngestSourceCutsRequest {
                    namespace: "tenant".into(),
                    relations: vec![IngestSourceRelationIdentityV1 {
                        relation_id: "orders".into(),
                        relation_version: "v1".into(),
                        relation_generation: INGEST_SOURCE_IDENTITY_GENERATION_V1,
                        schema_fingerprint: "sha256:request".into(),
                    }],
                }),
                relation_catalogs: vec![],
                standing_runtime_checkpoint: None,
            })
            .await;

        assert!(matches!(
            result,
            Err(MetaStoreError::Serialization(message)) if message.contains("fingerprint")
        ));
    }

    #[tokio::test]
    async fn batch_rejects_stored_source_cut_fingerprint_mismatches() {
        let store = InMemoryMetaStore::default();
        let relation_catalog = catalog("orders", "v1");
        let fingerprint = relation_catalog.schema_fingerprint.to_string();
        let reservation = IngestRangeReservation {
            stream_id: "orders-stream".into(),
            partition_id: 0,
            start_offset_inclusive: 0,
            end_offset_exclusive: 10,
            batch_key: "orders-batch".into(),
            payload_digest: "sha256:orders".into(),
            relation_id: "orders".into(),
            relation_version: "v1".into(),
            schema_fingerprint: "sha256:stored".into(),
            writer_epoch: 1,
        };
        let authority = RelationPartitionAuthorityKey {
            namespace: "tenant".into(),
            relation_id: "orders".into(),
            stream_id: reservation.stream_id.clone(),
            partition_id: reservation.partition_id,
        };
        {
            let mut state = store.inner.write().await;
            state
                .relation_catalogs
                .insert(("orders".into(), "v1".into()), relation_catalog);
            state.relation_ingest_reservations.insert(
                (reservation.stream_id.clone(), reservation.partition_id),
                vec![reservation.clone()],
            );
            state
                .relation_authority_reservation_keys
                .insert(reservation.clone(), authority.clone());
            state.relation_authoritative_ingest_publications.insert(
                "orders-request".into(),
                RelationAuthoritativeIngestPublication {
                    reservation,
                    authority_key: authority,
                    request_id: "orders-request".into(),
                    request_digest: "sha256:orders-request".into(),
                    object_key: "objects/orders".into(),
                    object_digest: "sha256:orders-object".into(),
                },
            );
        }

        let result = store
            .read_demand_metadata_batch(ReadDemandMetadataBatchRequest {
                source_cuts: Some(CaptureRelationIngestSourceCutsRequest {
                    namespace: "tenant".into(),
                    relations: vec![IngestSourceRelationIdentityV1 {
                        relation_id: "orders".into(),
                        relation_version: "v1".into(),
                        relation_generation: INGEST_SOURCE_IDENTITY_GENERATION_V1,
                        schema_fingerprint: fingerprint,
                    }],
                }),
                relation_catalogs: vec![],
                standing_runtime_checkpoint: None,
            })
            .await;

        assert!(matches!(
            result,
            Err(MetaStoreError::Serialization(message)) if message.contains("fingerprint")
        ));
    }
}

#[cfg(test)]
mod standing_runtime_owner_endpoint_tests {
    use super::*;

    fn owner_response(
        outcome: &str,
        owner_endpoint: &str,
    ) -> proto::AcquireStandingRuntimeOwnerResponse {
        proto::AcquireStandingRuntimeOwnerResponse {
            outcome: outcome.into(),
            claim: Some(proto::StandingRuntimeOwnerClaim {
                tenant_id: "tenant".into(),
                program_id: "program".into(),
                view_id: "view".into(),
                owner_id: "owner".into(),
                owner_epoch: 1,
                expires_at_unix_ms: 100,
                owner_endpoint: owner_endpoint.into(),
            }),
        }
    }

    #[test]
    fn grpc_acquire_rejects_success_without_matching_endpoint_acknowledgement() {
        // A proto3 server that predates the field drops it and returns the default empty value.
        for returned_endpoint in ["", "http://other-owner.example:8080"] {
            let result = acquire_standing_runtime_owner_outcome_from_proto(
                owner_response("acquired", returned_endpoint),
                Some("http://owner.example:8080"),
            );

            assert!(matches!(
                result,
                Err(MetaStoreError::UnexpectedOutcome(message))
                    if message.contains("endpoint protocol acknowledgement")
            ));
        }
    }

    #[test]
    fn grpc_acquire_accepts_matching_endpoint_acknowledgement() {
        for outcome in ["acquired", "renewed"] {
            let result = acquire_standing_runtime_owner_outcome_from_proto(
                owner_response(outcome, "http://owner.example:8080"),
                Some("http://owner.example:8080"),
            );
            assert!(matches!(
                (outcome, result),
                (
                    "acquired",
                    Ok(AcquireStandingRuntimeOwnerOutcome::Acquired(_))
                ) | (
                    "renewed",
                    Ok(AcquireStandingRuntimeOwnerOutcome::Renewed(_))
                )
            ));
        }
    }

    #[test]
    fn grpc_acquire_allows_conflict_with_another_owner_endpoint() {
        let result = acquire_standing_runtime_owner_outcome_from_proto(
            owner_response("conflict", "http://other-owner.example:8080"),
            Some("http://owner.example:8080"),
        );

        assert!(matches!(
            result,
            Ok(AcquireStandingRuntimeOwnerOutcome::Conflict(claim))
                if claim.owner_endpoint.as_deref() == Some("http://other-owner.example:8080")
        ));
    }

    #[test]
    fn owner_endpoint_survives_snapshot_and_proto_roundtrips() {
        let claim = StandingRuntimeOwnerClaim {
            tenant_id: "tenant".into(),
            program_id: "program".into(),
            view_id: "view".into(),
            owner_id: "owner".into(),
            owner_endpoint: Some("https://api.example:8443".into()),
            owner_epoch: 2,
            expires_at_unix_ms: 100,
        };
        let mut state = InMemoryMetaState::default();
        state.standing_runtime_owners.insert(
            standing_runtime_owner_scope_key("tenant", "program", "view"),
            claim,
        );
        let snapshot = rhiza_snapshot::encode(&state).unwrap();
        let snapshot_claim = rhiza_snapshot::decode(&snapshot)
            .unwrap()
            .standing_runtime_owners
            .remove(&standing_runtime_owner_scope_key(
                "tenant", "program", "view",
            ))
            .unwrap();
        let proto_claim = standing_runtime_owner_claim_from_proto(
            standing_runtime_owner_claim_to_proto(snapshot_claim.clone()),
        );
        assert_eq!(proto_claim, snapshot_claim);
    }

    #[test]
    fn owner_endpoint_validation_rejects_paths_and_credentials() {
        assert!(validate_owner_endpoint("https://api.example:8443").is_ok());
        assert!(validate_owner_endpoint("http://api.example").is_ok());
        for endpoint in [
            "http:///missing-host",
            "http://api.example/path",
            "http://api.example/",
            "http:\\\\api.example\\path",
            "http://user@api.example",
            "http://api.example?x=1",
            "http://api.example#fragment",
        ] {
            assert!(validate_owner_endpoint(endpoint).is_err(), "{endpoint}");
        }
    }

    #[tokio::test]
    async fn acquire_and_renew_preserve_the_claimed_owner_endpoint() {
        let store = InMemoryMetaStore::from_state_for_evaluation(InMemoryMetaState::default(), 100);
        let request = AcquireStandingRuntimeOwnerRequest {
            tenant_id: "tenant".into(),
            program_id: "program".into(),
            view_id: "view".into(),
            owner_id: "owner-a".into(),
            ttl_ms: 10,
        };
        let claim = match store
            .acquire_standing_runtime_owner_with_endpoint(
                request.clone(),
                "https://api-a.example:8443".into(),
            )
            .await
            .unwrap()
        {
            AcquireStandingRuntimeOwnerOutcome::Acquired(claim) => claim,
            other => panic!("unexpected acquire outcome: {other:?}"),
        };
        let renewed = store
            .renew_standing_runtime_owner(claim.token(), 10)
            .await
            .unwrap();
        assert_eq!(
            renewed.owner_endpoint.as_deref(),
            Some("https://api-a.example:8443")
        );
        let conflict = match store
            .acquire_standing_runtime_owner_with_endpoint(
                AcquireStandingRuntimeOwnerRequest {
                    owner_id: "owner-b".into(),
                    ..request
                },
                "https://api-b.example:8443".into(),
            )
            .await
            .unwrap()
        {
            AcquireStandingRuntimeOwnerOutcome::Conflict(claim) => claim,
            other => panic!("unexpected acquire outcome: {other:?}"),
        };
        assert_eq!(conflict.owner_endpoint, renewed.owner_endpoint);
    }

    #[tokio::test]
    async fn nested_arc_trait_objects_forward_owner_endpoint_capability() {
        let inner: Arc<dyn MetaStore> = Arc::new(InMemoryMetaStore::default());
        let store: Arc<dyn MetaStore> = Arc::new(inner);
        let request = AcquireStandingRuntimeOwnerRequest {
            tenant_id: "tenant".into(),
            program_id: "program".into(),
            view_id: "view".into(),
            owner_id: "owner".into(),
            ttl_ms: 10_000,
        };
        let claim = match store
            .acquire_standing_runtime_owner_with_endpoint(request, "http://api.example".into())
            .await
            .unwrap()
        {
            AcquireStandingRuntimeOwnerOutcome::Acquired(claim) => claim,
            other => panic!("unexpected acquire outcome: {other:?}"),
        };
        assert_eq!(claim.owner_endpoint.as_deref(), Some("http://api.example"));
    }
}
