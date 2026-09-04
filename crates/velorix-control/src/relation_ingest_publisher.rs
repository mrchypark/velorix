//! Fenced, idempotent publication of relation ingest batches.
//!
//! This module is the reusable control-plane boundary for relation ingest. It
//! deliberately does not mint an owner identity: a caller must inject a
//! stable `owner_id` in [`RelationIngestPublisherConfig`]. Object-store
//! staging is create-only and lives outside the committed ingest namespace;
//! metadata publication is the authority that makes the staged object
//! discoverable to relation source cuts.

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use crate::operator_authority::{
    validate_operator_authority, ObjectStoreAuthorityRef, OperatorAuthorityStartupComponents,
    OperatorStartupError, ValidatedOperatorAuthority,
};
use bytes::Bytes;
use serde::Serialize;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::sync::Mutex;
use velorix_meta::{
    AcquireRelationPartitionAuthorityOutcome, AcquireRelationPartitionAuthorityRequest,
    IngestRangeReservation, MetaStore, MetaStoreError, PublishIngestReservationOutcome,
    PublishRelationIngestReservationRequest, RelationAuthoritativeIngestPublication,
    RelationIngestCapability, RelationPartitionAuthorityKey, RelationPartitionAuthorityToken,
    ReserveIngestRangeOutcome, ReserveRelationAuthoritativeIngestRangeRequest,
};
use velorix_storage::{
    log::{IngestLog, IngestLogError, IngestStagingWriteOutcome},
    object_key::{ObjectKey, ObjectKeyError},
};

const REQUEST_SCHEMA_VERSION: u8 = 1;
const REQUEST_ID_PREFIX: &str = "relation-ingest-v1-";

/// Configuration for one relation/stream/partition publisher.
///
/// `owner_id` has no default by design. Deployments must inject an identity
/// that remains stable across retries and is unique among concurrent writers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelationIngestPublisherConfig {
    pub namespace: String,
    pub relation_id: String,
    pub relation_version: String,
    pub schema_fingerprint: String,
    pub stream_id: String,
    pub partition_id: u32,
    pub owner_id: String,
    pub authority_ttl_ms: u64,
}

/// The relation authority scope used for fencing and deterministic request
/// identity. Relation version and schema fingerprint remain part of a batch's
/// reservation, while the authority key is intentionally relation-scoped.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelationIngestScope {
    pub namespace: String,
    pub relation_id: String,
    pub relation_version: String,
    pub schema_fingerprint: String,
    pub stream_id: String,
    pub partition_id: u32,
}

impl RelationIngestScope {
    fn from_config(config: &RelationIngestPublisherConfig) -> Self {
        Self {
            namespace: config.namespace.clone(),
            relation_id: config.relation_id.clone(),
            relation_version: config.relation_version.clone(),
            schema_fingerprint: config.schema_fingerprint.clone(),
            stream_id: config.stream_id.clone(),
            partition_id: config.partition_id,
        }
    }

    fn authority_key(&self) -> RelationPartitionAuthorityKey {
        RelationPartitionAuthorityKey {
            namespace: self.namespace.clone(),
            relation_id: self.relation_id.clone(),
            stream_id: self.stream_id.clone(),
            partition_id: self.partition_id,
        }
    }
}

/// The authority session owned by a publisher. The mutex makes acquire,
/// renewal, and publication operations bounded and serialized per scope.
#[derive(Clone, Debug)]
pub struct RelationIngestPublisherSession {
    scope: RelationIngestScope,
    owner_id: String,
    authority_ttl_ms: u64,
    token: Arc<Mutex<Option<RelationPartitionAuthorityToken>>>,
}

impl RelationIngestPublisherSession {
    fn new(config: &RelationIngestPublisherConfig) -> Self {
        Self {
            scope: RelationIngestScope::from_config(config),
            owner_id: config.owner_id.clone(),
            authority_ttl_ms: config.authority_ttl_ms,
            token: Arc::new(Mutex::new(None)),
        }
    }

    pub fn scope(&self) -> &RelationIngestScope {
        &self.scope
    }

    pub fn owner_id(&self) -> &str {
        &self.owner_id
    }

    pub async fn token(&self) -> Option<RelationPartitionAuthorityToken> {
        self.token.lock().await.clone()
    }

    /// Acquire the relation authority, or renew it with the exact current
    /// token. A conflict or backend failure clears the local token.
    pub async fn acquire_or_renew(
        &self,
        meta: &dyn MetaStore,
    ) -> Result<RelationPartitionAuthorityToken, RelationIngestPublisherError> {
        let mut token = self.token.lock().await;
        let current_token = token.clone();
        let result = meta
            .acquire_relation_partition_authority(AcquireRelationPartitionAuthorityRequest {
                key: self.scope.authority_key(),
                owner_id: self.owner_id.clone(),
                current_token,
                ttl_ms: self.authority_ttl_ms,
            })
            .await;
        match result {
            Ok(AcquireRelationPartitionAuthorityOutcome::Acquired(next))
            | Ok(AcquireRelationPartitionAuthorityOutcome::Renewed(next)) => {
                *token = Some(next.clone());
                Ok(next)
            }
            Ok(AcquireRelationPartitionAuthorityOutcome::Conflict(current)) => {
                *token = None;
                Err(RelationIngestPublisherError::AuthorityConflict {
                    current: Box::new(current),
                })
            }
            Err(source) => {
                *token = None;
                Err(RelationIngestPublisherError::AuthorityLost {
                    source: Box::new(source),
                })
            }
        }
    }

    fn clear(&self, token: &mut Option<RelationPartitionAuthorityToken>) {
        *token = None;
    }
}

/// Result of a successful relation publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishedRelationIngest {
    pub reservation: IngestRangeReservation,
    pub request_id: String,
    pub request_digest: String,
    pub object_key: ObjectKey,
    pub object_digest: String,
}

/// Reusable relation ingest publisher.
#[derive(Clone)]
pub struct RelationIngestPublisher {
    meta: Arc<dyn MetaStore>,
    ingest_log: IngestLog,
    config: RelationIngestPublisherConfig,
    session: RelationIngestPublisherSession,
    capability_validated: Arc<AtomicBool>,
}

impl RelationIngestPublisher {
    /// Constructs a publisher after validating local configuration. Call
    /// [`Self::start`] before publishing; startup checks the metadata
    /// capability and acquires the first fenced token.
    pub fn new(
        meta: Arc<dyn MetaStore>,
        startup: &OperatorAuthorityStartupComponents,
        config: RelationIngestPublisherConfig,
    ) -> Result<Self, RelationIngestPublisherError> {
        validate_config(&config)?;
        let ingest_log = startup.ingest_log()?;
        let session = RelationIngestPublisherSession::new(&config);
        Ok(Self {
            meta,
            ingest_log,
            config,
            session,
            capability_validated: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Constructs a publisher from opaque authority evidence produced by the
    /// startup capability probe.
    pub fn from_validated_authority(
        meta: Arc<dyn MetaStore>,
        authority: ValidatedOperatorAuthority,
        config: RelationIngestPublisherConfig,
    ) -> Result<Self, RelationIngestPublisherError> {
        let startup = OperatorAuthorityStartupComponents::from_validated_authority(authority);
        Self::new(meta, &startup, config)
    }

    /// Runs the real same-store object-store capability probe and constructs a
    /// publisher from its opaque startup evidence.
    pub async fn from_store(
        meta: Arc<dyn MetaStore>,
        authority: ObjectStoreAuthorityRef,
        store: Arc<dyn object_store::ObjectStore>,
        backend_name: impl AsRef<str>,
        probe_prefix: impl AsRef<str>,
        config: RelationIngestPublisherConfig,
    ) -> Result<Self, RelationIngestPublisherError> {
        let validated =
            validate_operator_authority(authority, store, backend_name, probe_prefix).await?;
        Self::from_validated_authority(meta, validated, config)
    }

    pub fn config(&self) -> &RelationIngestPublisherConfig {
        &self.config
    }

    pub fn session(&self) -> RelationIngestPublisherSession {
        self.session.clone()
    }

    pub async fn list_committed(
        &self,
    ) -> Result<Vec<velorix_storage::log::IngestBatchDescriptor>, RelationIngestPublisherError>
    {
        self.ingest_log.list_committed().await.map_err(Into::into)
    }

    pub fn capability_validated(&self) -> bool {
        self.capability_validated.load(Ordering::Acquire)
    }

    /// Validates capability evidence (if needed) and acquires the relation
    /// authority. This is the required startup gate for a publisher.
    pub async fn start(
        &self,
    ) -> Result<RelationPartitionAuthorityToken, RelationIngestPublisherError> {
        if !self.capability_validated.load(Ordering::Acquire) {
            let capabilities = self.meta.read_meta_store_capabilities().await?;
            validate_relation_ingest_capability(&capabilities.relation_ingest)?;
            self.capability_validated.store(true, Ordering::Release);
        }
        self.session.acquire_or_renew(self.meta.as_ref()).await
    }

    pub async fn renew(
        &self,
    ) -> Result<RelationPartitionAuthorityToken, RelationIngestPublisherError> {
        if !self.capability_validated.load(Ordering::Acquire) {
            return Err(RelationIngestPublisherError::CapabilityNotValidated);
        }
        self.session.acquire_or_renew(self.meta.as_ref()).await
    }

    /// Publishes one immutable payload. The operation is idempotent for the
    /// same scope, range, and payload digest and is serialized with renewal.
    pub async fn publish(
        &self,
        start_offset_inclusive: u64,
        end_offset_exclusive: u64,
        payload: Bytes,
    ) -> Result<PublishedRelationIngest, RelationIngestPublisherError> {
        if !self.capability_validated.load(Ordering::Acquire) {
            return Err(RelationIngestPublisherError::CapabilityNotValidated);
        }
        let mut token = self.session.token.lock().await;
        let authority = token
            .clone()
            .ok_or(RelationIngestPublisherError::NotStarted)?;
        let payload_digest = digest_bytes(&payload);
        let request_id = deterministic_request_id(
            &self.session.scope,
            start_offset_inclusive,
            end_offset_exclusive,
            &payload_digest,
        )?;
        let request_digest = deterministic_request_digest(
            &self.session.scope,
            start_offset_inclusive,
            end_offset_exclusive,
            &payload_digest,
        )?;
        let batch_key = ObjectKey::ingest_batch(
            &self.config.stream_id,
            self.config.partition_id,
            start_offset_inclusive,
            end_offset_exclusive,
        )?;
        let staging_key = ObjectKey::ingest_staging(
            &self.config.stream_id,
            self.config.partition_id,
            start_offset_inclusive,
            end_offset_exclusive,
            &request_id,
        )?;
        let reservation = IngestRangeReservation {
            stream_id: self.config.stream_id.clone(),
            partition_id: self.config.partition_id,
            start_offset_inclusive,
            end_offset_exclusive,
            batch_key: batch_key.to_string(),
            payload_digest: payload_digest.clone(),
            relation_id: self.config.relation_id.clone(),
            relation_version: self.config.relation_version.clone(),
            schema_fingerprint: self.config.schema_fingerprint.clone(),
            writer_epoch: authority.owner_epoch,
        };

        match self
            .meta
            .reserve_relation_authoritative_ingest_range(
                ReserveRelationAuthoritativeIngestRangeRequest {
                    reservation: reservation.clone(),
                    authority: authority.clone(),
                },
            )
            .await
        {
            Ok(ReserveIngestRangeOutcome::Reserved | ReserveIngestRangeOutcome::Duplicate) => {}
            Ok(ReserveIngestRangeOutcome::Conflict) => {
                if self
                    .meta
                    .read_relation_partition_authority(&authority.key)
                    .await?
                    .as_ref()
                    != Some(&authority)
                {
                    self.session.clear(&mut token);
                    return Err(RelationIngestPublisherError::AuthorityLost {
                        source: Box::new(MetaStoreError::UnsupportedCapability(
                            "stale_relation_authority",
                        )),
                    });
                }
                return Err(RelationIngestPublisherError::RangeConflict {
                    reservation: Box::new(reservation),
                });
            }
            Err(source) => return Err(RelationIngestPublisherError::Meta(Box::new(source))),
        }

        match self
            .ingest_log
            .stage_write(staging_key.as_str(), payload, payload_digest.clone())
            .await?
        {
            IngestStagingWriteOutcome::Created(_) | IngestStagingWriteOutcome::Duplicate(_) => {}
            IngestStagingWriteOutcome::Conflict {
                existing_digest,
                requested_digest,
                ..
            } => {
                return Err(RelationIngestPublisherError::StagingConflict {
                    object_key: staging_key,
                    existing_digest,
                    requested_digest,
                });
            }
        }

        let request = PublishRelationIngestReservationRequest {
            reservation: reservation.clone(),
            authority: authority.clone(),
            request_id: request_id.clone(),
            request_digest: request_digest.clone(),
            object_key: staging_key.to_string(),
            object_digest: payload_digest.clone(),
        };
        let outcome = self
            .meta
            .publish_relation_ingest_reservation(request.clone())
            .await;
        let _ = match outcome {
            Ok(PublishIngestReservationOutcome::Committed)
            | Ok(PublishIngestReservationOutcome::Duplicate) => {
                self.verify_publication(&request, &mut token).await?
            }
            Ok(PublishIngestReservationOutcome::Conflict) => {
                return Err(RelationIngestPublisherError::PublicationConflict { request_id });
            }
            Ok(PublishIngestReservationOutcome::InvalidAuthority) => {
                self.session.clear(&mut token);
                return Err(RelationIngestPublisherError::AuthorityLost {
                    source: Box::new(MetaStoreError::UnsupportedCapability(
                        "invalid_relation_authority",
                    )),
                });
            }
            Err(source) => {
                self.verify_after_uncertain_publish(&request, source, &mut token)
                    .await?
            }
        };

        Ok(PublishedRelationIngest {
            reservation,
            request_id,
            request_digest,
            object_key: staging_key,
            object_digest: payload_digest,
        })
    }

    async fn verify_publication(
        &self,
        request: &PublishRelationIngestReservationRequest,
        token: &mut Option<RelationPartitionAuthorityToken>,
    ) -> Result<RelationAuthoritativeIngestPublication, RelationIngestPublisherError> {
        let publication = self
            .meta
            .read_relation_authoritative_ingest_publication(&request.request_id)
            .await?
            .ok_or_else(|| RelationIngestPublisherError::PublicationMissing {
                request_id: request.request_id.clone(),
            })?;
        validate_publication_match(&publication, request)?;
        if publication.authority_key != request.authority.key {
            self.session.clear(token);
            return Err(RelationIngestPublisherError::AuthorityLost {
                source: Box::new(MetaStoreError::UnsupportedCapability(
                    "publication_authority_mismatch",
                )),
            });
        }
        Ok(publication)
    }

    async fn verify_after_uncertain_publish(
        &self,
        request: &PublishRelationIngestReservationRequest,
        first_error: MetaStoreError,
        token: &mut Option<RelationPartitionAuthorityToken>,
    ) -> Result<RelationAuthoritativeIngestPublication, RelationIngestPublisherError> {
        match self
            .meta
            .read_relation_authoritative_ingest_publication(&request.request_id)
            .await
        {
            Ok(Some(publication)) => {
                validate_publication_match(&publication, request)?;
                if publication.authority_key != request.authority.key {
                    self.session.clear(token);
                    return Err(RelationIngestPublisherError::AuthorityLost {
                        source: Box::new(MetaStoreError::UnsupportedCapability(
                            "publication_authority_mismatch",
                        )),
                    });
                }
                Ok(publication)
            }
            Ok(None) => match self
                .meta
                .publish_relation_ingest_reservation(request.clone())
                .await
            {
                Ok(PublishIngestReservationOutcome::Committed)
                | Ok(PublishIngestReservationOutcome::Duplicate) => {
                    self.verify_publication(request, token).await
                }
                Ok(PublishIngestReservationOutcome::InvalidAuthority) => {
                    self.session.clear(token);
                    Err(RelationIngestPublisherError::AuthorityLost {
                        source: Box::new(MetaStoreError::UnsupportedCapability(
                            "invalid_relation_authority",
                        )),
                    })
                }
                Ok(PublishIngestReservationOutcome::Conflict) => {
                    Err(RelationIngestPublisherError::PublicationConflict {
                        request_id: request.request_id.clone(),
                    })
                }
                Err(retry_error) => match self.verify_publication(request, token).await {
                    Ok(publication) => Ok(publication),
                    Err(RelationIngestPublisherError::PublicationMissing { .. }) => {
                        Err(RelationIngestPublisherError::UncertainPublication {
                            request_id: request.request_id.clone(),
                            first_error: Box::new(first_error),
                            retry_error: Box::new(retry_error),
                        })
                    }
                    Err(error) => Err(error),
                },
            },
            Err(read_error) => Err(RelationIngestPublisherError::UncertainPublication {
                request_id: request.request_id.clone(),
                first_error: Box::new(first_error),
                retry_error: Box::new(read_error),
            }),
        }
    }
}

#[derive(Debug, Error)]
pub enum RelationIngestPublisherError {
    #[error("relation ingest publisher config field `{field}` must not be empty")]
    InvalidConfig { field: &'static str },
    #[error("relation ingest authority ttl must be greater than zero")]
    InvalidTtl,
    #[error("relation ingest metadata capability is not supported by `{backend_name}`; missing {missing:?}")]
    UnsupportedRelationIngestCapability {
        backend_name: String,
        missing: Vec<&'static str>,
    },
    #[error("relation ingest publisher startup capability was not validated")]
    CapabilityNotValidated,
    #[error("relation ingest publisher has not acquired relation authority")]
    NotStarted,
    #[error("relation ingest authority is held by `{}` at epoch {}", current.owner_id, current.owner_epoch)]
    AuthorityConflict {
        current: Box<RelationPartitionAuthorityToken>,
    },
    #[error("relation ingest authority was lost: {source}")]
    AuthorityLost { source: Box<MetaStoreError> },
    #[error("relation ingest range reservation conflicts with an existing range: {reservation:?}")]
    RangeConflict {
        reservation: Box<IngestRangeReservation>,
    },
    #[error("staging object `{object_key}` conflicts: existing={existing_digest}, requested={requested_digest}")]
    StagingConflict {
        object_key: ObjectKey,
        existing_digest: String,
        requested_digest: String,
    },
    #[error("metadata publication `{request_id}` conflicts with existing publication")]
    PublicationConflict { request_id: String },
    #[error("metadata publication `{request_id}` was absent after publish response")]
    PublicationMissing { request_id: String },
    #[error("metadata publication `{request_id}` does not exactly match the request")]
    PublicationMismatch { request_id: String },
    #[error("metadata publication `{request_id}` remained uncertain after one bounded retry: first={first_error}; retry={retry_error}")]
    UncertainPublication {
        request_id: String,
        first_error: Box<MetaStoreError>,
        retry_error: Box<MetaStoreError>,
    },
    #[error(transparent)]
    OperatorStartup(#[from] OperatorStartupError),
    #[error(transparent)]
    Meta(Box<MetaStoreError>),
    #[error(transparent)]
    Storage(Box<IngestLogError>),
    #[error(transparent)]
    ObjectKey(Box<ObjectKeyError>),
}

impl From<MetaStoreError> for RelationIngestPublisherError {
    fn from(source: MetaStoreError) -> Self {
        Self::Meta(Box::new(source))
    }
}

impl From<IngestLogError> for RelationIngestPublisherError {
    fn from(source: IngestLogError) -> Self {
        Self::Storage(Box::new(source))
    }
}

impl From<ObjectKeyError> for RelationIngestPublisherError {
    fn from(source: ObjectKeyError) -> Self {
        Self::ObjectKey(Box::new(source))
    }
}

fn validate_config(
    config: &RelationIngestPublisherConfig,
) -> Result<(), RelationIngestPublisherError> {
    for (field, value) in [
        ("namespace", config.namespace.as_str()),
        ("relation_id", config.relation_id.as_str()),
        ("relation_version", config.relation_version.as_str()),
        ("schema_fingerprint", config.schema_fingerprint.as_str()),
        ("stream_id", config.stream_id.as_str()),
        ("owner_id", config.owner_id.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(RelationIngestPublisherError::InvalidConfig { field });
        }
    }
    if config.authority_ttl_ms == 0 {
        return Err(RelationIngestPublisherError::InvalidTtl);
    }
    Ok(())
}

fn validate_relation_ingest_capability(
    capability: &RelationIngestCapability,
) -> Result<(), RelationIngestPublisherError> {
    let mut missing = Vec::new();
    if !capability.relation_scoped_authority {
        missing.push("relation_scoped_authority");
    }
    if !capability.committed_publication_source_cut {
        missing.push("committed_publication_source_cut");
    }
    if !capability.durable_across_restart {
        missing.push("durable_across_restart");
    }
    if missing.is_empty() {
        Ok(())
    } else {
        Err(
            RelationIngestPublisherError::UnsupportedRelationIngestCapability {
                backend_name: capability.backend_name.clone(),
                missing,
            },
        )
    }
}

#[derive(Serialize)]
struct RequestIdentity<'a> {
    schema_version: u8,
    namespace: &'a str,
    relation_id: &'a str,
    relation_version: &'a str,
    schema_fingerprint: &'a str,
    stream_id: &'a str,
    partition_id: u32,
    start_offset_inclusive: u64,
    end_offset_exclusive: u64,
    payload_digest: &'a str,
}

fn request_identity<'a>(
    scope: &'a RelationIngestScope,
    start_offset_inclusive: u64,
    end_offset_exclusive: u64,
    payload_digest: &'a str,
) -> Result<Vec<u8>, RelationIngestPublisherError> {
    if start_offset_inclusive >= end_offset_exclusive {
        return Err(RelationIngestPublisherError::InvalidConfig {
            field: "offset_range",
        });
    }
    serde_json::to_vec(&RequestIdentity {
        schema_version: REQUEST_SCHEMA_VERSION,
        namespace: &scope.namespace,
        relation_id: &scope.relation_id,
        relation_version: &scope.relation_version,
        schema_fingerprint: &scope.schema_fingerprint,
        stream_id: &scope.stream_id,
        partition_id: scope.partition_id,
        start_offset_inclusive,
        end_offset_exclusive,
        payload_digest,
    })
    .map_err(|error| {
        RelationIngestPublisherError::Meta(Box::new(MetaStoreError::Serialization(
            error.to_string(),
        )))
    })
}

pub fn deterministic_request_digest(
    scope: &RelationIngestScope,
    start_offset_inclusive: u64,
    end_offset_exclusive: u64,
    payload_digest: &str,
) -> Result<String, RelationIngestPublisherError> {
    let identity = request_identity(
        scope,
        start_offset_inclusive,
        end_offset_exclusive,
        payload_digest,
    )?;
    Ok(format!("sha256:{:x}", Sha256::digest(identity)))
}

pub fn deterministic_request_id(
    scope: &RelationIngestScope,
    start_offset_inclusive: u64,
    end_offset_exclusive: u64,
    payload_digest: &str,
) -> Result<String, RelationIngestPublisherError> {
    let digest = deterministic_request_digest(
        scope,
        start_offset_inclusive,
        end_offset_exclusive,
        payload_digest,
    )?;
    Ok(format!(
        "{REQUEST_ID_PREFIX}{}",
        digest.trim_start_matches("sha256:")
    ))
}

fn digest_bytes(payload: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(payload))
}

fn validate_publication_match(
    publication: &RelationAuthoritativeIngestPublication,
    request: &PublishRelationIngestReservationRequest,
) -> Result<(), RelationIngestPublisherError> {
    if publication.reservation != request.reservation
        || publication.authority_key != request.authority.key
        || publication.request_id != request.request_id
        || publication.request_digest != request.request_digest
        || publication.object_key != request.object_key
        || publication.object_digest != request.object_digest
    {
        return Err(RelationIngestPublisherError::PublicationMismatch {
            request_id: request.request_id.clone(),
        });
    }
    Ok(())
}
