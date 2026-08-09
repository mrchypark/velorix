use std::{
    collections::HashMap,
    sync::{Mutex, MutexGuard},
};

use async_trait::async_trait;
use thiserror::Error;
use velorix_storage::manifest::PartitionOwnerClaim;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PartitionLeaseKey {
    pub namespace: String,
    pub view_id: String,
    pub stream_id: String,
    pub partition_id: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionLeaseGrant {
    pub key: PartitionLeaseKey,
    pub owner_id: String,
    pub owner_epoch: u64,
    pub expires_at_unix_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaseAcquireRequest {
    pub key: PartitionLeaseKey,
    pub owner_id: String,
    /// Caller-supplied time is for domain tests and dev adapters.
    ///
    /// A production lease adapter must not treat a local caller clock alone as
    /// ownership authority.
    pub now_unix_ms: u64,
    pub ttl_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LeaseBackendKind {
    InMemoryDev,
    KubernetesLease,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProductionOwnershipBackendConfig {
    pub lease_backend: LeaseBackendKind,
    pub supports_durable_epoch_records: bool,
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum LeaseError {
    #[error("lease key field `{field}` must not be empty")]
    InvalidLeaseKey { field: &'static str },
    #[error("lease owner id must not be empty")]
    InvalidOwnerId,
    #[error("lease ttl must be greater than zero: ttl_ms={ttl_ms}")]
    InvalidTtl { ttl_ms: u64 },
    #[error("lease expiry time overflow: now_unix_ms={now_unix_ms}, ttl_ms={ttl_ms}")]
    LeaseTimeOverflow { now_unix_ms: u64, ttl_ms: u64 },
    #[error("lease owner epoch overflow for current grant `{current:?}`")]
    OwnerEpochOverflow { current: PartitionLeaseGrant },
    #[error("lease is currently held by `{}` at epoch {}", current.owner_id, current.owner_epoch)]
    Conflict { current: PartitionLeaseGrant },
    #[error("lease is currently held by `{}` at epoch {}", current.owner_id, current.owner_epoch)]
    NotLeaseHolder { current: PartitionLeaseGrant },
    #[error("lease is not currently held")]
    LeaseNotHeld,
    #[error("production ownership requires durable lease backend, got {lease_backend:?}")]
    ProductionLeaseBackendNotDurable { lease_backend: LeaseBackendKind },
    #[error("production ownership requires durable epoch record support")]
    ProductionOwnershipMissingDurableEpochRecords,
}

pub fn validate_production_ownership_backend(
    config: &ProductionOwnershipBackendConfig,
) -> Result<(), LeaseError> {
    if config.lease_backend == LeaseBackendKind::InMemoryDev {
        return Err(LeaseError::ProductionLeaseBackendNotDurable {
            lease_backend: config.lease_backend,
        });
    }

    if !config.supports_durable_epoch_records {
        return Err(LeaseError::ProductionOwnershipMissingDurableEpochRecords);
    }

    Ok(())
}

#[async_trait]
pub trait PartitionLeaseClient: Send + Sync {
    /// Acquires a partition lease or renews the caller's existing lease.
    ///
    /// Implementations issue an owner claim only. They must not persist
    /// checkpoint progress or treat the lease backend as database authority.
    async fn acquire_or_renew(
        &self,
        request: LeaseAcquireRequest,
    ) -> Result<PartitionLeaseGrant, LeaseError>;

    /// Returns the current unexpired grant at the caller-supplied time.
    ///
    /// Caller-supplied time is only a domain boundary input. A production
    /// Kubernetes adapter must not use the local caller clock alone as
    /// ownership authority.
    async fn current(
        &self,
        key: &PartitionLeaseKey,
        now_unix_ms: u64,
    ) -> Result<Option<PartitionLeaseGrant>, LeaseError>;

    /// Releases the current grant when called by its holder.
    ///
    /// Both owner id and owner epoch must match the current unexpired grant.
    /// Non-holder or stale-epoch release fails closed with
    /// [`LeaseError::NotLeaseHolder`].
    async fn release(
        &self,
        key: &PartitionLeaseKey,
        owner_id: &str,
        owner_epoch: u64,
        now_unix_ms: u64,
    ) -> Result<(), LeaseError>;
}

/// In-memory lease client for tests, bootstrap flows, and local development.
///
/// This client is not a production ownership backend. Its caller-supplied clock
/// and process-local state do not provide Kubernetes adapter semantics or
/// durable fencing authority.
#[derive(Debug, Default)]
pub struct InMemoryPartitionLeaseClient {
    leases: Mutex<HashMap<PartitionLeaseKey, PartitionLeaseGrant>>,
}

impl From<PartitionLeaseGrant> for PartitionOwnerClaim {
    fn from(grant: PartitionLeaseGrant) -> Self {
        Self {
            owner_id: grant.owner_id,
            owner_epoch: grant.owner_epoch,
        }
    }
}

#[async_trait]
impl PartitionLeaseClient for InMemoryPartitionLeaseClient {
    async fn acquire_or_renew(
        &self,
        request: LeaseAcquireRequest,
    ) -> Result<PartitionLeaseGrant, LeaseError> {
        validate_key(&request.key)?;
        validate_owner_id(&request.owner_id)?;
        if request.ttl_ms == 0 {
            return Err(LeaseError::InvalidTtl {
                ttl_ms: request.ttl_ms,
            });
        }
        let expires_at_unix_ms = request.now_unix_ms.checked_add(request.ttl_ms).ok_or(
            LeaseError::LeaseTimeOverflow {
                now_unix_ms: request.now_unix_ms,
                ttl_ms: request.ttl_ms,
            },
        )?;

        let mut leases = self.lock_leases();
        let grant =
            match leases.get(&request.key) {
                Some(current) if current.expires_at_unix_ms > request.now_unix_ms => {
                    if current.owner_id != request.owner_id {
                        return Err(LeaseError::Conflict {
                            current: current.clone(),
                        });
                    }

                    PartitionLeaseGrant {
                        key: request.key.clone(),
                        owner_id: request.owner_id.clone(),
                        owner_epoch: current.owner_epoch,
                        expires_at_unix_ms,
                    }
                }
                Some(current) => {
                    let owner_epoch = current.owner_epoch.checked_add(1).ok_or(
                        LeaseError::OwnerEpochOverflow {
                            current: current.clone(),
                        },
                    )?;

                    PartitionLeaseGrant {
                        key: request.key.clone(),
                        owner_id: request.owner_id.clone(),
                        owner_epoch,
                        expires_at_unix_ms,
                    }
                }
                None => PartitionLeaseGrant {
                    key: request.key.clone(),
                    owner_id: request.owner_id.clone(),
                    owner_epoch: 1,
                    expires_at_unix_ms,
                },
            };

        leases.insert(request.key, grant.clone());

        Ok(grant)
    }

    async fn current(
        &self,
        key: &PartitionLeaseKey,
        now_unix_ms: u64,
    ) -> Result<Option<PartitionLeaseGrant>, LeaseError> {
        validate_key(key)?;

        Ok(self
            .lock_leases()
            .get(key)
            .filter(|grant| grant.expires_at_unix_ms > now_unix_ms)
            .cloned())
    }

    async fn release(
        &self,
        key: &PartitionLeaseKey,
        owner_id: &str,
        owner_epoch: u64,
        now_unix_ms: u64,
    ) -> Result<(), LeaseError> {
        validate_key(key)?;
        validate_owner_id(owner_id)?;

        let mut leases = self.lock_leases();
        match leases.get(key) {
            Some(current) if current.expires_at_unix_ms <= now_unix_ms => {
                Err(LeaseError::LeaseNotHeld)
            }
            Some(current) if current.owner_id == owner_id && current.owner_epoch == owner_epoch => {
                let mut released = current.clone();
                released.expires_at_unix_ms = now_unix_ms;
                leases.insert(key.clone(), released);
                Ok(())
            }
            Some(current) => Err(LeaseError::NotLeaseHolder {
                current: current.clone(),
            }),
            None => Err(LeaseError::LeaseNotHeld),
        }
    }
}

impl InMemoryPartitionLeaseClient {
    fn lock_leases(&self) -> MutexGuard<'_, HashMap<PartitionLeaseKey, PartitionLeaseGrant>> {
        self.leases
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

fn validate_key(key: &PartitionLeaseKey) -> Result<(), LeaseError> {
    validate_key_field("namespace", &key.namespace)?;
    validate_key_field("view_id", &key.view_id)?;
    validate_key_field("stream_id", &key.stream_id)?;

    Ok(())
}

fn validate_key_field(field: &'static str, value: &str) -> Result<(), LeaseError> {
    if value.trim().is_empty() {
        Err(LeaseError::InvalidLeaseKey { field })
    } else {
        Ok(())
    }
}

fn validate_owner_id(owner_id: &str) -> Result<(), LeaseError> {
    if owner_id.trim().is_empty() {
        Err(LeaseError::InvalidOwnerId)
    } else {
        Ok(())
    }
}
