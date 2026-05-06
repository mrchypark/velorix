use std::collections::BTreeMap;

use async_trait::async_trait;
use k8s_openapi::{
    api::coordination::v1::{Lease, LeaseSpec},
    apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta},
    jiff::Timestamp,
};
use kube::{
    api::{Api, PostParams},
    Client,
};
use thiserror::Error;
use velorix_control::lease::{
    LeaseAcquireRequest, LeaseError, PartitionLeaseClient, PartitionLeaseGrant, PartitionLeaseKey,
};
use velorix_storage::{
    object_key::ObjectKey, ownership::OwnershipEpochRecord, state::CheckpointPublisher,
};

const VIEW_ID_ANNOTATION: &str = "control.velorix.io/view-id";
const STREAM_ID_ANNOTATION: &str = "control.velorix.io/stream-id";
const PARTITION_ID_ANNOTATION: &str = "control.velorix.io/partition-id";

#[derive(Clone, Debug)]
pub struct KubernetesPartitionLeaseClient<A> {
    api: A,
}

impl<A> KubernetesPartitionLeaseClient<A> {
    pub fn new(api: A) -> Self {
        Self { api }
    }
}

impl<A> KubernetesPartitionLeaseClient<A>
where
    A: LeaseApi,
{
    pub async fn acquire_or_renew_kubernetes(
        &self,
        request: LeaseAcquireRequest,
    ) -> Result<PartitionLeaseGrant, KubernetesLeaseError> {
        validate_request(&request)?;
        let name = lease_name(&request.key);
        let existing = self.api.get(&request.key.namespace, &name).await?;
        let lease = match existing {
            Some(lease) => renew_or_reacquire_lease(&request, lease)?,
            None => new_lease(&request, &name)?,
        };

        let written = if lease.metadata.resource_version.is_some() {
            self.api.replace(&name, lease).await?
        } else {
            self.api.create(lease).await?
        };

        current_grant_from_lease(&request.key, &written, request.now_unix_ms)?.ok_or(
            KubernetesLeaseError::InvalidLeaseBody {
                field: "spec.holderIdentity",
            },
        )
    }

    pub async fn current_kubernetes(
        &self,
        key: &PartitionLeaseKey,
        now_unix_ms: u64,
    ) -> Result<Option<PartitionLeaseGrant>, KubernetesLeaseError> {
        validate_key(key)?;
        match self.api.get(&key.namespace, &lease_name(key)).await? {
            Some(lease) => current_grant_from_lease(key, &lease, now_unix_ms),
            None => Ok(None),
        }
    }

    pub async fn release_kubernetes(
        &self,
        key: &PartitionLeaseKey,
        owner_id: &str,
        owner_epoch: u64,
        now_unix_ms: u64,
    ) -> Result<(), KubernetesLeaseError> {
        validate_key(key)?;
        validate_owner_id(owner_id)?;

        let name = lease_name(key);
        let Some(mut lease) = self.api.get(&key.namespace, &name).await? else {
            return Err(LeaseError::LeaseNotHeld.into());
        };
        let Some(current) = current_grant_from_lease(key, &lease, now_unix_ms)? else {
            return Err(LeaseError::LeaseNotHeld.into());
        };
        if current.owner_id != owner_id || current.owner_epoch != owner_epoch {
            return Err(LeaseError::NotLeaseHolder { current }.into());
        }

        let spec = lease_spec_mut(&mut lease)?;
        spec.holder_identity = None;
        spec.renew_time = Some(micro_time_from_unix_ms(now_unix_ms)?);
        self.api.replace(&name, lease).await?;

        Ok(())
    }
}

#[async_trait]
impl<A> PartitionLeaseClient for KubernetesPartitionLeaseClient<A>
where
    A: LeaseApi,
{
    async fn acquire_or_renew(
        &self,
        request: LeaseAcquireRequest,
    ) -> Result<PartitionLeaseGrant, LeaseError> {
        self.acquire_or_renew_kubernetes(request)
            .await
            .map_err(KubernetesLeaseError::into_partition_lease_error)
    }

    async fn current(
        &self,
        key: &PartitionLeaseKey,
        now_unix_ms: u64,
    ) -> Result<Option<PartitionLeaseGrant>, LeaseError> {
        self.current_kubernetes(key, now_unix_ms)
            .await
            .map_err(KubernetesLeaseError::into_partition_lease_error)
    }

    async fn release(
        &self,
        key: &PartitionLeaseKey,
        owner_id: &str,
        owner_epoch: u64,
        now_unix_ms: u64,
    ) -> Result<(), LeaseError> {
        self.release_kubernetes(key, owner_id, owner_epoch, now_unix_ms)
            .await
            .map_err(KubernetesLeaseError::into_partition_lease_error)
    }
}

#[async_trait]
pub trait LeaseApi: Clone + Send + Sync + 'static {
    async fn get(&self, namespace: &str, name: &str)
        -> Result<Option<Lease>, KubernetesLeaseError>;
    async fn create(&self, lease: Lease) -> Result<Lease, KubernetesLeaseError>;
    async fn replace(&self, name: &str, lease: Lease) -> Result<Lease, KubernetesLeaseError>;
}

#[derive(Clone)]
pub struct KubeLeaseApi {
    client: Client,
}

impl KubeLeaseApi {
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}

#[async_trait]
impl LeaseApi for KubeLeaseApi {
    async fn get(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<Lease>, KubernetesLeaseError> {
        let api: Api<Lease> = Api::namespaced(self.client.clone(), namespace);
        match api.get(name).await {
            Ok(lease) => Ok(Some(lease)),
            Err(kube::Error::Api(response)) if response.code == 404 => Ok(None),
            Err(error) => Err(KubernetesLeaseError::api("get", error)),
        }
    }

    async fn create(&self, lease: Lease) -> Result<Lease, KubernetesLeaseError> {
        let namespace =
            lease
                .metadata
                .namespace
                .as_deref()
                .ok_or(KubernetesLeaseError::InvalidLeaseBody {
                    field: "metadata.namespace",
                })?;
        let api: Api<Lease> = Api::namespaced(self.client.clone(), namespace);
        match api.create(&PostParams::default(), &lease).await {
            Ok(lease) => Ok(lease),
            Err(kube::Error::Api(response)) if response.code == 409 => {
                Err(KubernetesLeaseError::WriteConflict)
            }
            Err(error) => Err(KubernetesLeaseError::api("create", error)),
        }
    }

    async fn replace(&self, name: &str, lease: Lease) -> Result<Lease, KubernetesLeaseError> {
        let namespace =
            lease
                .metadata
                .namespace
                .as_deref()
                .ok_or(KubernetesLeaseError::InvalidLeaseBody {
                    field: "metadata.namespace",
                })?;
        let api: Api<Lease> = Api::namespaced(self.client.clone(), namespace);
        match api.replace(name, &PostParams::default(), &lease).await {
            Ok(lease) => Ok(lease),
            Err(kube::Error::Api(response)) if response.code == 409 => {
                Err(KubernetesLeaseError::WriteConflict)
            }
            Err(error) => Err(KubernetesLeaseError::api("replace", error)),
        }
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum KubernetesLeaseError {
    #[error("{0}")]
    Lease(Box<LeaseError>),
    #[error("kubernetes lease API {operation} failed: {message}")]
    Api {
        operation: &'static str,
        message: String,
    },
    #[error("kubernetes lease write conflict")]
    WriteConflict,
    #[error("invalid kubernetes lease body field `{field}`")]
    InvalidLeaseBody { field: &'static str },
    #[error("invalid kubernetes lease timestamp `{field}`")]
    InvalidTimestamp { field: &'static str },
}

impl KubernetesLeaseError {
    fn api(operation: &'static str, error: kube::Error) -> Self {
        Self::Api {
            operation,
            message: error.to_string(),
        }
    }

    fn into_partition_lease_error(self) -> LeaseError {
        match self {
            Self::Lease(error) => *error,
            Self::WriteConflict
            | Self::Api { .. }
            | Self::InvalidLeaseBody { .. }
            | Self::InvalidTimestamp { .. } => LeaseError::LeaseNotHeld,
        }
    }
}

impl From<LeaseError> for KubernetesLeaseError {
    fn from(error: LeaseError) -> Self {
        Self::Lease(Box::new(error))
    }
}

pub fn ownership_epoch_record_from_grant(
    grant: &PartitionLeaseGrant,
    lease_identity: impl Into<String>,
    created_at: impl Into<String>,
    previous_checkpoint_version: Option<u64>,
) -> OwnershipEpochRecord {
    OwnershipEpochRecord {
        stream_id: grant.key.stream_id.clone(),
        partition_id: grant.key.partition_id,
        owner_id: grant.owner_id.clone(),
        owner_epoch: grant.owner_epoch,
        lease_identity: lease_identity.into(),
        created_at: created_at.into(),
        previous_epoch: grant.owner_epoch.checked_sub(1),
        previous_checkpoint_version,
    }
}

pub async fn persist_ownership_epoch_record_from_grant(
    publisher: &CheckpointPublisher,
    grant: &PartitionLeaseGrant,
    lease_identity: impl Into<String>,
    created_at: impl Into<String>,
    previous_checkpoint_version: Option<u64>,
) -> Result<ObjectKey, velorix_storage::state::CheckpointPublishError> {
    let record = ownership_epoch_record_from_grant(
        grant,
        lease_identity,
        created_at,
        previous_checkpoint_version,
    );
    publisher.create_ownership_epoch_record(&record).await
}

pub fn set_partition_annotations(lease: &mut Lease, key: &PartitionLeaseKey) {
    let annotations = lease.metadata.annotations.get_or_insert_with(BTreeMap::new);
    annotations.insert(VIEW_ID_ANNOTATION.to_string(), key.view_id.clone());
    annotations.insert(STREAM_ID_ANNOTATION.to_string(), key.stream_id.clone());
    annotations.insert(
        PARTITION_ID_ANNOTATION.to_string(),
        key.partition_id.to_string(),
    );
}

pub fn partition_lease_identity(key: &PartitionLeaseKey) -> String {
    format!(
        "coordination.k8s.io/v1/namespaces/{}/leases/{}",
        key.namespace,
        lease_name(key)
    )
}

fn new_lease(request: &LeaseAcquireRequest, name: &str) -> Result<Lease, KubernetesLeaseError> {
    let ttl_seconds = ttl_seconds(request.ttl_ms)?;
    let mut lease = Lease {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(request.key.namespace.clone()),
            ..ObjectMeta::default()
        },
        spec: Some(LeaseSpec {
            acquire_time: Some(micro_time_from_unix_ms(request.now_unix_ms)?),
            holder_identity: Some(request.owner_id.clone()),
            lease_duration_seconds: Some(ttl_seconds),
            lease_transitions: Some(1),
            renew_time: Some(micro_time_from_unix_ms(request.now_unix_ms)?),
            ..LeaseSpec::default()
        }),
    };
    set_partition_annotations(&mut lease, &request.key);
    Ok(lease)
}

fn renew_or_reacquire_lease(
    request: &LeaseAcquireRequest,
    mut lease: Lease,
) -> Result<Lease, KubernetesLeaseError> {
    validate_lease_key(&lease, &request.key)?;
    let previous = current_grant_from_lease(&request.key, &lease, request.now_unix_ms)?;
    let previous_is_none = previous.is_none();
    let previous_epoch = lease_epoch(&lease)?;

    let owner_epoch = match previous {
        Some(current) if current.owner_id != request.owner_id => {
            return Err(LeaseError::Conflict { current }.into());
        }
        Some(ref current) => current.owner_epoch,
        None => previous_epoch
            .checked_add(1)
            .ok_or(KubernetesLeaseError::InvalidLeaseBody {
                field: "spec.leaseTransitions",
            })?,
    };

    let spec = lease_spec_mut(&mut lease)?;
    spec.holder_identity = Some(request.owner_id.clone());
    spec.lease_duration_seconds = Some(ttl_seconds(request.ttl_ms)?);
    spec.lease_transitions =
        Some(
            i32::try_from(owner_epoch).map_err(|_| KubernetesLeaseError::InvalidLeaseBody {
                field: "spec.leaseTransitions",
            })?,
        );
    spec.renew_time = Some(micro_time_from_unix_ms(request.now_unix_ms)?);
    if spec.acquire_time.is_none() || previous_is_none {
        spec.acquire_time = Some(micro_time_from_unix_ms(request.now_unix_ms)?);
    }

    Ok(lease)
}

fn current_grant_from_lease(
    key: &PartitionLeaseKey,
    lease: &Lease,
    now_unix_ms: u64,
) -> Result<Option<PartitionLeaseGrant>, KubernetesLeaseError> {
    validate_lease_key(lease, key)?;
    let spec = lease_spec(lease)?;
    let duration_ms = lease_duration_ms(spec)?;
    let owner_epoch = lease_epoch(lease)?;
    let Some(owner_id) = spec
        .holder_identity
        .as_ref()
        .filter(|owner| !owner.is_empty())
    else {
        return Ok(None);
    };
    if owner_epoch == 0 {
        return Err(KubernetesLeaseError::InvalidLeaseBody {
            field: "spec.leaseTransitions",
        });
    }
    let renew_time = spec
        .renew_time
        .as_ref()
        .ok_or(KubernetesLeaseError::InvalidLeaseBody {
            field: "spec.renewTime",
        })?;
    let renewed_at = unix_ms_from_micro_time(renew_time, "spec.renewTime")?;
    let expires_at_unix_ms =
        renewed_at
            .checked_add(duration_ms)
            .ok_or(KubernetesLeaseError::InvalidLeaseBody {
                field: "spec.leaseDurationSeconds",
            })?;

    if expires_at_unix_ms <= now_unix_ms {
        return Ok(None);
    }

    Ok(Some(PartitionLeaseGrant {
        key: key.clone(),
        owner_id: owner_id.clone(),
        owner_epoch,
        expires_at_unix_ms,
    }))
}

fn request_key_from_lease(lease: &Lease) -> Result<PartitionLeaseKey, KubernetesLeaseError> {
    let namespace =
        lease
            .metadata
            .namespace
            .clone()
            .ok_or(KubernetesLeaseError::InvalidLeaseBody {
                field: "metadata.namespace",
            })?;
    let annotations =
        lease
            .metadata
            .annotations
            .as_ref()
            .ok_or(KubernetesLeaseError::InvalidLeaseBody {
                field: "metadata.annotations",
            })?;
    let partition_id = annotation(annotations, PARTITION_ID_ANNOTATION)?
        .parse::<u32>()
        .map_err(|_| KubernetesLeaseError::InvalidLeaseBody {
            field: PARTITION_ID_ANNOTATION,
        })?;

    Ok(PartitionLeaseKey {
        namespace,
        view_id: annotation(annotations, VIEW_ID_ANNOTATION)?.to_string(),
        stream_id: annotation(annotations, STREAM_ID_ANNOTATION)?.to_string(),
        partition_id,
    })
}

fn validate_lease_key(lease: &Lease, key: &PartitionLeaseKey) -> Result<(), KubernetesLeaseError> {
    let body_key = request_key_from_lease(lease)?;
    if &body_key != key {
        return Err(KubernetesLeaseError::InvalidLeaseBody {
            field: "metadata.annotations",
        });
    }

    Ok(())
}

fn annotation<'a>(
    annotations: &'a BTreeMap<String, String>,
    key: &'static str,
) -> Result<&'a str, KubernetesLeaseError> {
    annotations
        .get(key)
        .filter(|value| !value.is_empty())
        .map(String::as_str)
        .ok_or(KubernetesLeaseError::InvalidLeaseBody { field: key })
}

fn lease_spec(lease: &Lease) -> Result<&LeaseSpec, KubernetesLeaseError> {
    lease
        .spec
        .as_ref()
        .ok_or(KubernetesLeaseError::InvalidLeaseBody { field: "spec" })
}

fn lease_spec_mut(lease: &mut Lease) -> Result<&mut LeaseSpec, KubernetesLeaseError> {
    lease
        .spec
        .as_mut()
        .ok_or(KubernetesLeaseError::InvalidLeaseBody { field: "spec" })
}

fn lease_epoch(lease: &Lease) -> Result<u64, KubernetesLeaseError> {
    let transitions =
        lease_spec(lease)?
            .lease_transitions
            .ok_or(KubernetesLeaseError::InvalidLeaseBody {
                field: "spec.leaseTransitions",
            })?;
    u64::try_from(transitions).map_err(|_| KubernetesLeaseError::InvalidLeaseBody {
        field: "spec.leaseTransitions",
    })
}

fn lease_duration_ms(spec: &LeaseSpec) -> Result<u64, KubernetesLeaseError> {
    let seconds = spec
        .lease_duration_seconds
        .ok_or(KubernetesLeaseError::InvalidLeaseBody {
            field: "spec.leaseDurationSeconds",
        })?;
    if seconds <= 0 {
        return Err(KubernetesLeaseError::InvalidLeaseBody {
            field: "spec.leaseDurationSeconds",
        });
    }

    u64::try_from(seconds)
        .ok()
        .and_then(|seconds| seconds.checked_mul(1_000))
        .ok_or(KubernetesLeaseError::InvalidLeaseBody {
            field: "spec.leaseDurationSeconds",
        })
}

fn ttl_seconds(ttl_ms: u64) -> Result<i32, KubernetesLeaseError> {
    let rounded = ttl_ms
        .checked_add(999)
        .ok_or(LeaseError::LeaseTimeOverflow {
            now_unix_ms: 0,
            ttl_ms,
        })?
        / 1_000;
    i32::try_from(rounded).map_err(|_| LeaseError::InvalidTtl { ttl_ms }.into())
}

fn micro_time_from_unix_ms(unix_ms: u64) -> Result<MicroTime, KubernetesLeaseError> {
    let unix_ms = i64::try_from(unix_ms).map_err(|_| KubernetesLeaseError::InvalidTimestamp {
        field: "request.now_unix_ms",
    })?;
    Timestamp::from_millisecond(unix_ms)
        .map(MicroTime)
        .map_err(|_| KubernetesLeaseError::InvalidTimestamp {
            field: "request.now_unix_ms",
        })
}

fn unix_ms_from_micro_time(
    time: &MicroTime,
    field: &'static str,
) -> Result<u64, KubernetesLeaseError> {
    u64::try_from(time.0.as_millisecond())
        .map_err(|_| KubernetesLeaseError::InvalidTimestamp { field })
}

fn validate_request(request: &LeaseAcquireRequest) -> Result<(), KubernetesLeaseError> {
    validate_key(&request.key)?;
    validate_owner_id(&request.owner_id)?;
    if request.ttl_ms == 0 {
        return Err(LeaseError::InvalidTtl {
            ttl_ms: request.ttl_ms,
        }
        .into());
    }
    request
        .now_unix_ms
        .checked_add(request.ttl_ms)
        .ok_or(LeaseError::LeaseTimeOverflow {
            now_unix_ms: request.now_unix_ms,
            ttl_ms: request.ttl_ms,
        })?;

    Ok(())
}

fn validate_key(key: &PartitionLeaseKey) -> Result<(), KubernetesLeaseError> {
    validate_key_field("namespace", &key.namespace)?;
    validate_key_field("view_id", &key.view_id)?;
    validate_key_field("stream_id", &key.stream_id)?;

    Ok(())
}

fn validate_key_field(field: &'static str, value: &str) -> Result<(), KubernetesLeaseError> {
    if value.trim().is_empty() {
        Err(LeaseError::InvalidLeaseKey { field }.into())
    } else {
        Ok(())
    }
}

fn validate_owner_id(owner_id: &str) -> Result<(), KubernetesLeaseError> {
    if owner_id.trim().is_empty() {
        Err(LeaseError::InvalidOwnerId.into())
    } else {
        Ok(())
    }
}

fn lease_name(key: &PartitionLeaseKey) -> String {
    let view = dns_label_piece(&key.view_id, 40);
    let stream = dns_label_piece(&key.stream_id, 40);
    format!(
        "velorix-{view}-{stream}-p{}-{:016x}",
        key.partition_id,
        stable_hash(&format!(
            "{}/{}/{}",
            key.view_id, key.stream_id, key.partition_id
        ))
    )
}

fn dns_label_piece(value: &str, max_len: usize) -> String {
    let mut piece = String::new();
    for byte in value.bytes() {
        let next = match byte {
            b'a'..=b'z' | b'0'..=b'9' => byte as char,
            b'A'..=b'Z' => byte.to_ascii_lowercase() as char,
            _ => '-',
        };
        if piece.len() < max_len && (next != '-' || !piece.ends_with('-')) {
            piece.push(next);
        }
    }

    let piece = piece.trim_matches('-');
    if piece.is_empty() {
        "partition".to_string()
    } else {
        piece.to_string()
    }
}

fn stable_hash(value: &str) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in value.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}
