//! Narrow metadata administration facade for API control-plane wiring.

pub use velorix_meta::{
    validate_bearer_token, AcquireStandingRuntimeOwnerOutcome, AcquireStandingRuntimeOwnerRequest,
    GrpcMetaStore, InMemoryMetaStore, IngestRangeReservation, MetaStore, MetaStoreError,
    PublishStandingRuntimeCheckpointOutcome, PublishStandingRuntimeCheckpointRequest,
    ReserveIngestRangeOutcome, StandingRuntimeCheckpointPointer, StandingRuntimeFencingCapability,
    StandingRuntimeOwnerClaim, StandingRuntimeOwnerToken, StoreRelationCatalogOutcome,
    STANDING_RUNTIME_BACKEND_TIME_SOURCE_RAFT_REPLICATED,
    STANDING_RUNTIME_FENCING_CAPABILITY_SCHEMA_VERSION,
    STANDING_RUNTIME_LEASE_AUTHORITY_KIND_HIQLITE_RAFT_SERIALIZED,
    STANDING_RUNTIME_LEASE_AUTHORITY_KIND_RAFT_REPLICATED_TIME,
    STANDING_RUNTIME_LEASE_EXPIRY_SEMANTICS_BACKEND_WALL_CLOCK_TTL,
    STANDING_RUNTIME_LEASE_EXPIRY_SEMANTICS_OPERATION_DRIVEN_LOGICAL,
    STANDING_RUNTIME_OUTPUT_DELTA_REF_PREFIX, STANDING_RUNTIME_OUTPUT_MANIFEST_REF_PREFIX,
    STANDING_RUNTIME_OWNER_SCOPE_KIND_TENANT_PROGRAM_VIEW,
};
