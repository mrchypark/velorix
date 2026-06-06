//! Metadata service contracts for Velorix control-plane state.

#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use object_store::ObjectStore;
use thiserror::Error;
use tokio::sync::RwLock;
use tonic::{metadata::MetadataValue, transport::Channel, Request, Response, Status};
use velorix_core::relation::{RelationSchemaError, VelorixRelationCatalogV1};
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
pub struct MetaStoreCapabilities {
    pub standing_runtime_fencing: StandingRuntimeFencingCapability,
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
    #[error("metadata serialization error: {0}")]
    Serialization(String),
    #[error("standing runtime checkpoint pointer scope mismatch")]
    StandingRuntimeCheckpointScopeMismatch,
    #[error("standing runtime owner token does not match the current unexpired owner")]
    StandingRuntimeOwnerMismatch,
    #[error("metadata capability `{0}` is not supported by this backend")]
    UnsupportedCapability(&'static str),
    #[error("remote metadata service error: {0}")]
    Remote(String),
    #[error("remote metadata service returned unexpected outcome `{0}`")]
    UnexpectedOutcome(String),
    #[error("object-store metadata store error: {0}")]
    Oss(String),
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

    async fn reserve_ingest_range(
        &self,
        reservation: IngestRangeReservation,
    ) -> Result<ReserveIngestRangeOutcome, MetaStoreError> {
        (**self).reserve_ingest_range(reservation).await
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
}

#[derive(Clone, Default)]
pub struct InMemoryMetaStore {
    inner: Arc<RwLock<InMemoryMetaState>>,
}

#[derive(Default)]
struct InMemoryMetaState {
    relation_catalogs: HashMap<(String, String), VelorixRelationCatalogV1>,
    ingest_reservations: HashMap<(String, u32), Vec<IngestRangeReservation>>,
    standing_runtime_owners: HashMap<(String, String, String), StandingRuntimeOwnerClaim>,
    standing_runtime_checkpoints:
        HashMap<(String, String, String), StandingRuntimeCheckpointPointer>,
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
        Ok(ReserveIngestRangeOutcome::Reserved)
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
                    .ok_or(MetaStoreError::TimestampOverflow)?;
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

impl StandingRuntimeCheckpointPointer {
    fn validate(&self) -> Result<(), MetaStoreError> {
        validate_standing_runtime_scope(&self.tenant_id, &self.program_id, &self.view_id)?;
        require_non_empty("checkpoint_key", &self.checkpoint_key)?;
        require_non_empty("content_hash", &self.content_hash)?;
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
        let request = request.into_inner();
        let outcome = self
            .store
            .reserve_ingest_range(IngestRangeReservation {
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
            })
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
        let outcome = self
            .store
            .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
                expected_previous: request
                    .expected_previous
                    .map(standing_runtime_checkpoint_pointer_from_proto),
                candidate: standing_runtime_checkpoint_pointer_from_proto(candidate),
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
) -> StandingRuntimeCheckpointPointer {
    StandingRuntimeCheckpointPointer {
        tenant_id: pointer.tenant_id,
        program_id: pointer.program_id,
        view_id: pointer.view_id,
        checkpoint_key: pointer.checkpoint_key,
        logical_epoch: pointer.logical_epoch,
        content_hash: pointer.content_hash,
    }
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
        | MetaStoreError::Serialization(_)
        | MetaStoreError::StandingRuntimeCheckpointScopeMismatch
        | MetaStoreError::StandingRuntimeOwnerMismatch
        | MetaStoreError::UnexpectedOutcome(_) => Status::invalid_argument(error.to_string()),
        MetaStoreError::UnsupportedCapability(_) => Status::failed_precondition(error.to_string()),
        MetaStoreError::Remote(_) | MetaStoreError::Oss(_) | MetaStoreError::Hiqlite(_) => {
            Status::unavailable(error.to_string())
        }
    }
}

#[derive(Clone)]
pub struct GrpcMetaStore {
    client: Arc<tokio::sync::Mutex<proto::velorix_meta_client::VelorixMetaClient<Channel>>>,
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
                "CREATE TABLE IF NOT EXISTS velorix_standing_runtime_checkpoints (
                    tenant_id TEXT NOT NULL,
                    program_id TEXT NOT NULL,
                    view_id TEXT NOT NULL,
                    checkpoint_key TEXT NOT NULL,
                    logical_epoch INTEGER NOT NULL,
                    content_hash TEXT NOT NULL,
                    PRIMARY KEY (tenant_id, program_id, view_id)
                )",
                vec![],
            )
            .await
            .map_err(hiqlite_error)?;
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
        Ok(())
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
            .client
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
            .map_err(hiqlite_error)?;
        rows.into_iter()
            .next()
            .map(StandingRuntimeOwnerClaimRow::into_claim)
            .transpose()
    }
}

#[cfg(feature = "hiqlite-backend")]
#[async_trait]
impl MetaStore for HiqliteMetaStore {
    async fn read_meta_store_capabilities(&self) -> Result<MetaStoreCapabilities, MetaStoreError> {
        Ok(MetaStoreCapabilities {
            standing_runtime_fencing: hiqlite_standing_runtime_fencing_capability(false),
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
            .client
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
            .map_err(hiqlite_error)?;
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
            .client
            .query_map::<CatalogJsonRow, _>(
                "SELECT catalog_json FROM velorix_relation_catalogs
                    WHERE relation_id = $1 AND relation_version = $2",
                vec![
                    hiqlite::Param::from(relation_id.to_string()),
                    hiqlite::Param::from(relation_version.to_string()),
                ],
            )
            .await
            .map_err(hiqlite_error)?;
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
            .client
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
            .map_err(hiqlite_error)?;
        if inserted == 1 {
            return Ok(ReserveIngestRangeOutcome::Reserved);
        }

        let exact_rows = self
            .client
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
            .map_err(hiqlite_error)?;
        if exact_rows
            .into_iter()
            .any(|existing| existing.into_reservation() == reservation)
        {
            Ok(ReserveIngestRangeOutcome::Duplicate)
        } else {
            Ok(ReserveIngestRangeOutcome::Conflict)
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
            .client
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
            .map_err(hiqlite_error)?;
        let results = txn.result.map_err(hiqlite_error)?;
        let raft_timestamp = txn.timestamp;
        let changed = hiqlite_txn_changed_rows(results, 0)?;
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
            .client
            .txn_with_raft_serialized_timestamp([(
                "UPDATE velorix_standing_runtime_owners
                    SET expires_at_unix_ms = expires_at_unix_ms
                    WHERE 0",
                vec![],
            )])
            .await
            .map_err(hiqlite_error)?;
        txn.result.map_err(hiqlite_error)?;
        let now =
            u64::try_from(txn.timestamp.unix_ms).map_err(|_| MetaStoreError::TimestampOverflow)?;
        Ok(self
            .read_standing_runtime_owner_record(tenant_id, program_id, view_id)
            .await?
            .filter(|claim| claim.expires_at_unix_ms > now))
    }

    async fn publish_standing_runtime_checkpoint(
        &self,
        request: PublishStandingRuntimeCheckpointRequest,
    ) -> Result<PublishStandingRuntimeCheckpointOutcome, MetaStoreError> {
        request.validate()?;
        let candidate_epoch = i64_from_u64("logical_epoch", request.candidate.logical_epoch)?;
        let owner_epoch = i64_from_u64("owner_epoch", request.owner.owner_epoch)?;
        let (changed, raft_timestamp) = if let Some(expected) = &request.expected_previous {
            let expected_epoch =
                i64_from_u64("expected_previous.logical_epoch", expected.logical_epoch)?;
            let txn = self
                .client
                .txn_with_raft_serialized_timestamp([(
                    "UPDATE velorix_standing_runtime_checkpoints
                            SET checkpoint_key = $1,
                                logical_epoch = $2,
                                content_hash = $3
                            WHERE tenant_id = $4
                              AND program_id = $5
                              AND view_id = $6
                              AND checkpoint_key = $7
                              AND logical_epoch = $8
                              AND content_hash = $9
                              AND EXISTS (
                                  SELECT 1
                                  FROM velorix_standing_runtime_owners owner
                                  WHERE owner.tenant_id = $4
                                    AND owner.program_id = $5
                                    AND owner.view_id = $6
                                    AND owner.owner_id = $10
                                    AND owner.owner_epoch = $11
                                    AND owner.expires_at_unix_ms > $12
                                    AND $13 = $4
                                    AND $14 = $5
                                    AND $15 = $6
                              )",
                    vec![
                        hiqlite::Param::from(request.candidate.checkpoint_key.clone()),
                        hiqlite::Param::from(candidate_epoch),
                        hiqlite::Param::from(request.candidate.content_hash.clone()),
                        hiqlite::Param::from(request.candidate.tenant_id.clone()),
                        hiqlite::Param::from(request.candidate.program_id.clone()),
                        hiqlite::Param::from(request.candidate.view_id.clone()),
                        hiqlite::Param::from(expected.checkpoint_key.clone()),
                        hiqlite::Param::from(expected_epoch),
                        hiqlite::Param::from(expected.content_hash.clone()),
                        hiqlite::Param::from(request.owner.owner_id.clone()),
                        hiqlite::Param::from(owner_epoch),
                        hiqlite::Param::raft_serialized_unix_ms(),
                        hiqlite::Param::from(request.owner.tenant_id.clone()),
                        hiqlite::Param::from(request.owner.program_id.clone()),
                        hiqlite::Param::from(request.owner.view_id.clone()),
                    ],
                )])
                .await
                .map_err(hiqlite_error)?;
            let results = txn.result.map_err(hiqlite_error)?;
            (hiqlite_txn_changed_rows(results, 0)?, txn.timestamp)
        } else {
            let txn = self
                .client
                .txn_with_raft_serialized_timestamp([(
                    "INSERT INTO velorix_standing_runtime_checkpoints (
                            tenant_id,
                            program_id,
                            view_id,
                            checkpoint_key,
                            logical_epoch,
                            content_hash
                        )
                        SELECT $1, $2, $3, $4, $5, $6
                        WHERE NOT EXISTS (
                            SELECT 1 FROM velorix_standing_runtime_checkpoints
                            WHERE tenant_id = $1
                              AND program_id = $2
                              AND view_id = $3
                        )
                        AND EXISTS (
                            SELECT 1
                            FROM velorix_standing_runtime_owners owner
                            WHERE owner.tenant_id = $1
                              AND owner.program_id = $2
                              AND owner.view_id = $3
                              AND owner.owner_id = $7
                              AND owner.owner_epoch = $8
                              AND owner.expires_at_unix_ms > $9
                              AND $10 = $1
                              AND $11 = $2
                              AND $12 = $3
                        )",
                    vec![
                        hiqlite::Param::from(request.candidate.tenant_id.clone()),
                        hiqlite::Param::from(request.candidate.program_id.clone()),
                        hiqlite::Param::from(request.candidate.view_id.clone()),
                        hiqlite::Param::from(request.candidate.checkpoint_key.clone()),
                        hiqlite::Param::from(candidate_epoch),
                        hiqlite::Param::from(request.candidate.content_hash.clone()),
                        hiqlite::Param::from(request.owner.owner_id.clone()),
                        hiqlite::Param::from(owner_epoch),
                        hiqlite::Param::raft_serialized_unix_ms(),
                        hiqlite::Param::from(request.owner.tenant_id.clone()),
                        hiqlite::Param::from(request.owner.program_id.clone()),
                        hiqlite::Param::from(request.owner.view_id.clone()),
                    ],
                )])
                .await
                .map_err(hiqlite_error)?;
            let results = txn.result.map_err(hiqlite_error)?;
            (hiqlite_txn_changed_rows(results, 0)?, txn.timestamp)
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
            .client
            .query_consistent_map::<StandingRuntimeCheckpointPointerRow, _>(
                "SELECT
                    tenant_id,
                    program_id,
                    view_id,
                    checkpoint_key,
                    logical_epoch,
                    content_hash
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
            .map_err(hiqlite_error)?;
        rows.into_iter()
            .next()
            .map(StandingRuntimeCheckpointPointerRow::into_pointer)
            .transpose()
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
struct StandingRuntimeCheckpointPointerRow {
    tenant_id: String,
    program_id: String,
    view_id: String,
    checkpoint_key: String,
    logical_epoch: i64,
    content_hash: String,
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
        }
    }
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
fn hiqlite_error(error: hiqlite::Error) -> MetaStoreError {
    MetaStoreError::Hiqlite(error.to_string())
}

#[cfg(feature = "hiqlite-backend")]
fn i64_from_u64(field: &'static str, value: u64) -> Result<i64, MetaStoreError> {
    i64::try_from(value).map_err(|_| MetaStoreError::IntegerOutOfRange { field, value })
}

#[cfg(feature = "hiqlite-backend")]
fn hiqlite_txn_changed_rows(
    results: Vec<Result<usize, hiqlite::Error>>,
    changed_result_index: usize,
) -> Result<usize, MetaStoreError> {
    let mut changed = None;
    for (index, result) in results.into_iter().enumerate() {
        let rows = result.map_err(hiqlite_error)?;
        if index == changed_result_index {
            changed = Some(rows);
        }
    }
    changed.ok_or_else(|| {
        MetaStoreError::UnexpectedOutcome(format!(
            "hiqlite transaction did not return result index {changed_result_index}"
        ))
    })
}

impl GrpcMetaStore {
    pub async fn connect(endpoint: impl AsRef<str>) -> Result<Self, MetaStoreError> {
        let client =
            proto::velorix_meta_client::VelorixMetaClient::connect(endpoint.as_ref().to_string())
                .await
                .map_err(|error| MetaStoreError::Remote(error.to_string()))?;

        Ok(Self {
            client: Arc::new(tokio::sync::Mutex::new(client)),
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
}

#[async_trait]
impl MetaStore for GrpcMetaStore {
    async fn read_meta_store_capabilities(&self) -> Result<MetaStoreCapabilities, MetaStoreError> {
        let response = self
            .client
            .lock()
            .await
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
        })
    }

    async fn store_relation_catalog(
        &self,
        catalog: VelorixRelationCatalogV1,
    ) -> Result<StoreRelationCatalogOutcome, MetaStoreError> {
        let catalog_json = serde_json::to_vec(&catalog)
            .map_err(|error| MetaStoreError::Serialization(error.to_string()))?;
        let response = self
            .client
            .lock()
            .await
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
            .client
            .lock()
            .await
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
            .client
            .lock()
            .await
            .reserve_ingest_range(self.request(proto::ReserveIngestRangeRequest {
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
            }))
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

    async fn acquire_standing_runtime_owner(
        &self,
        request: AcquireStandingRuntimeOwnerRequest,
    ) -> Result<AcquireStandingRuntimeOwnerOutcome, MetaStoreError> {
        let response = self
            .client
            .lock()
            .await
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
            .client
            .lock()
            .await
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
            .client
            .lock()
            .await
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
            .client
            .lock()
            .await
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
        )))
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
                && publish_impl.contains("owner.expires_at_unix_ms > $12")
                && publish_impl.contains("owner.expires_at_unix_ms > $9"),
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
        let checkpoint_mutation = publish_impl
            .find("velorix_standing_runtime_checkpoints")
            .expect("publish should mutate checkpoint rows");
        let owner_validation = publish_impl
            .find("AND owner.owner_id =")
            .expect("publish mutation must validate current owner token in SQL");

        assert!(
            checkpoint_mutation < owner_validation,
            "Hiqlite publish must validate owner token and authority-time expiry inside the checkpoint mutation"
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

        assert!(
            publish_impl.contains(
                "SET checkpoint_key = $1,\n                                logical_epoch = $2,\n                                content_hash = $3"
            ),
            "Hiqlite binds SQL parameters by first appearance; publish update SET params must be $1..$3"
        );
        let candidate_key = publish_impl
            .find("hiqlite::Param::from(request.candidate.checkpoint_key.clone())")
            .expect("publish update should bind candidate checkpoint_key");
        let candidate_epoch_param = publish_impl
            .find("hiqlite::Param::from(candidate_epoch)")
            .expect("publish update should bind candidate logical_epoch");
        let candidate_hash = publish_impl
            .find("hiqlite::Param::from(request.candidate.content_hash.clone())")
            .expect("publish update should bind candidate content_hash");
        let candidate_tenant = publish_impl
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
            publish_impl.contains("AND $13 = $4\n                                    AND $14 = $5\n                                    AND $15 = $6"),
            "Hiqlite checkpoint update path must enforce owner scope equals candidate scope inside the SQL mutation"
        );
        assert!(
            publish_impl.contains("AND $10 = $1\n                              AND $11 = $2\n                              AND $12 = $3"),
            "Hiqlite checkpoint insert path must enforce owner scope equals candidate scope inside the SQL mutation"
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
