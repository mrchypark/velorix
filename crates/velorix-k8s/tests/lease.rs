use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use k8s_openapi::{
    api::coordination::v1::{Lease, LeaseSpec},
    apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta},
    jiff::Timestamp,
};
use object_store::{local::LocalFileSystem, ObjectStore};
use tempfile::TempDir;
use velorix_control::lease::{
    LeaseAcquireRequest, LeaseError, PartitionLeaseClient, PartitionLeaseGrant, PartitionLeaseKey,
};
use velorix_k8s::lease::{
    ownership_epoch_record_from_grant, persist_ownership_epoch_record_from_grant,
    KubernetesLeaseError, KubernetesPartitionLeaseClient, LeaseApi,
};
use velorix_storage::state::CheckpointPublisher;

#[tokio::test]
async fn kubernetes_lease_acquires_renews_and_reads_current_grant() {
    let api = FakeLeaseApi::default();
    let client = KubernetesPartitionLeaseClient::new(api);

    let first = client
        .acquire_or_renew(acquire_request("worker-a", 1_000, 2_000))
        .await
        .unwrap();
    let renewed = client
        .acquire_or_renew(acquire_request("worker-a", 2_000, 2_000))
        .await
        .unwrap();
    let current = client.current(&lease_key(), 2_500).await.unwrap();

    assert_eq!(first.owner_epoch, 1);
    assert_eq!(renewed.owner_epoch, first.owner_epoch);
    assert_eq!(renewed.expires_at_unix_ms, 4_000);
    assert_eq!(current, Some(renewed));
}

#[tokio::test]
async fn kubernetes_lease_fails_closed_when_another_holder_is_current() {
    let api = FakeLeaseApi::default();
    let client = KubernetesPartitionLeaseClient::new(api);
    let first = client
        .acquire_or_renew(acquire_request("worker-a", 1_000, 2_000))
        .await
        .unwrap();

    let err = client
        .acquire_or_renew(acquire_request("worker-b", 2_000, 2_000))
        .await
        .unwrap_err();

    assert!(matches!(err, LeaseError::Conflict { current } if current == first));
}

#[tokio::test]
async fn kubernetes_lease_release_requires_current_holder_and_epoch() {
    let api = FakeLeaseApi::default();
    let client = KubernetesPartitionLeaseClient::new(api);
    let grant = client
        .acquire_or_renew(acquire_request("worker-a", 1_000, 2_000))
        .await
        .unwrap();

    let stale_epoch = client
        .release(&grant.key, "worker-a", grant.owner_epoch + 1, 2_000)
        .await
        .unwrap_err();
    assert!(matches!(
        stale_epoch,
        LeaseError::NotLeaseHolder { current } if current == grant
    ));

    client
        .release(&grant.key, "worker-a", grant.owner_epoch, 2_100)
        .await
        .unwrap();
    assert_eq!(client.current(&lease_key(), 2_200).await.unwrap(), None);
}

#[tokio::test]
async fn kubernetes_lease_reacquires_with_higher_epoch_after_expiry_or_release() {
    let api = FakeLeaseApi::default();
    let client = KubernetesPartitionLeaseClient::new(api);
    let first = client
        .acquire_or_renew(acquire_request("worker-a", 1_000, 1_000))
        .await
        .unwrap();

    let second = client
        .acquire_or_renew(acquire_request("worker-b", 2_000, 1_000))
        .await
        .unwrap();

    assert_eq!(first.owner_epoch, 1);
    assert_eq!(second.owner_id, "worker-b");
    assert_eq!(second.owner_epoch, 2);
}

#[tokio::test]
async fn kubernetes_lease_fails_closed_on_update_conflict() {
    let api = FakeLeaseApi::default();
    let client = KubernetesPartitionLeaseClient::new(api.clone());
    client
        .acquire_or_renew(acquire_request("worker-a", 1_000, 2_000))
        .await
        .unwrap();
    api.conflict_next_replace();

    let err = client
        .acquire_or_renew_kubernetes(acquire_request("worker-a", 2_000, 2_000))
        .await
        .unwrap_err();

    assert_eq!(err, KubernetesLeaseError::WriteConflict);
}

#[tokio::test]
async fn kubernetes_lease_rejects_mismatched_returned_lease_key() {
    let api = FakeLeaseApi::default();
    api.mutate_next_write_key(wrong_lease_key());
    let client = KubernetesPartitionLeaseClient::new(api);

    let err = client
        .acquire_or_renew_kubernetes(acquire_request("worker-a", 1_000, 2_000))
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        KubernetesLeaseError::InvalidLeaseBody {
            field: "metadata.annotations"
        }
    ));
}

#[tokio::test]
async fn kubernetes_lease_rejects_invalid_body_fields() {
    let api = FakeLeaseApi::with_lease(lease_with_spec(LeaseSpec {
        holder_identity: Some("worker-a".to_string()),
        lease_duration_seconds: Some(0),
        lease_transitions: Some(1),
        renew_time: Some(micro_time(1_000)),
        ..LeaseSpec::default()
    }));
    let client = KubernetesPartitionLeaseClient::new(api);

    let err = client
        .current_kubernetes(&lease_key(), 1_100)
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        KubernetesLeaseError::InvalidLeaseBody {
            field: "spec.leaseDurationSeconds"
        }
    ));
}

#[test]
fn ownership_epoch_record_helper_is_pure_and_does_not_publish() {
    let grant = PartitionLeaseGrant {
        key: lease_key(),
        owner_id: "worker-a".to_string(),
        owner_epoch: 3,
        expires_at_unix_ms: 4_000,
    };

    let record = ownership_epoch_record_from_grant(
        &grant,
        "coordination.k8s.io/v1/namespaces/default/leases/test",
        "2026-05-06T00:00:00Z",
        Some(8),
    );

    assert_eq!(record.stream_id, "orders");
    assert_eq!(record.partition_id, 0);
    assert_eq!(record.owner_id, "worker-a");
    assert_eq!(record.owner_epoch, 3);
    assert_eq!(record.previous_epoch, Some(2));
    assert_eq!(record.previous_checkpoint_version, Some(8));
    record.validate().unwrap();
}

#[tokio::test]
async fn kubernetes_lease_grant_persists_durable_epoch_record_create_only() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(store);
    let grant = PartitionLeaseGrant {
        key: lease_key(),
        owner_id: "worker-a".to_string(),
        owner_epoch: 3,
        expires_at_unix_ms: 4_000,
    };

    let created_key = persist_ownership_epoch_record_from_grant(
        &publisher,
        &grant,
        "coordination.k8s.io/v1/namespaces/default/leases/lease-a",
        "2026-05-06T00:00:00Z",
        Some(8),
    )
    .await
    .unwrap();
    let duplicate_key = persist_ownership_epoch_record_from_grant(
        &publisher,
        &grant,
        "coordination.k8s.io/v1/namespaces/default/leases/lease-a",
        "2026-05-06T00:00:00Z",
        Some(8),
    )
    .await
    .unwrap();
    let stored = publisher
        .read_ownership_epoch_record("orders", 0, 3)
        .await
        .unwrap();

    assert_eq!(duplicate_key, created_key);
    assert_eq!(stored.owner_id, "worker-a");
    assert_eq!(stored.owner_epoch, 3);
    assert_eq!(
        stored.lease_identity,
        "coordination.k8s.io/v1/namespaces/default/leases/lease-a"
    );
    assert_eq!(stored.previous_checkpoint_version, Some(8));
}

#[derive(Clone, Default)]
struct FakeLeaseApi {
    lease: Arc<Mutex<Option<Lease>>>,
    next_resource_version: Arc<Mutex<u64>>,
    conflict_next_replace: Arc<Mutex<bool>>,
    mutate_next_write_key: Arc<Mutex<Option<PartitionLeaseKey>>>,
}

impl FakeLeaseApi {
    fn with_lease(lease: Lease) -> Self {
        Self {
            lease: Arc::new(Mutex::new(Some(lease))),
            next_resource_version: Arc::new(Mutex::new(2)),
            conflict_next_replace: Arc::new(Mutex::new(false)),
            mutate_next_write_key: Arc::new(Mutex::new(None)),
        }
    }

    fn conflict_next_replace(&self) {
        *self.conflict_next_replace.lock().unwrap() = true;
    }

    fn mutate_next_write_key(&self, key: PartitionLeaseKey) {
        *self.mutate_next_write_key.lock().unwrap() = Some(key);
    }
}

#[async_trait]
impl LeaseApi for FakeLeaseApi {
    async fn get(
        &self,
        _namespace: &str,
        _name: &str,
    ) -> Result<Option<Lease>, KubernetesLeaseError> {
        Ok(self.lease.lock().unwrap().clone())
    }

    async fn create(&self, mut lease: Lease) -> Result<Lease, KubernetesLeaseError> {
        let mut stored = self.lease.lock().unwrap();
        if stored.is_some() {
            return Err(KubernetesLeaseError::WriteConflict);
        }

        self.mutate_written_key(&mut lease);
        lease.metadata.resource_version = Some(self.next_resource_version());
        *stored = Some(lease.clone());
        Ok(lease)
    }

    async fn replace(&self, _name: &str, mut lease: Lease) -> Result<Lease, KubernetesLeaseError> {
        let mut conflict_next = self.conflict_next_replace.lock().unwrap();
        if *conflict_next {
            *conflict_next = false;
            return Err(KubernetesLeaseError::WriteConflict);
        }
        drop(conflict_next);

        let mut stored = self.lease.lock().unwrap();
        let current_version = stored
            .as_ref()
            .and_then(|current| current.metadata.resource_version.clone());
        if current_version != lease.metadata.resource_version {
            return Err(KubernetesLeaseError::WriteConflict);
        }

        self.mutate_written_key(&mut lease);
        lease.metadata.resource_version = Some(self.next_resource_version());
        *stored = Some(lease.clone());
        Ok(lease)
    }
}

impl FakeLeaseApi {
    fn mutate_written_key(&self, lease: &mut Lease) {
        if let Some(key) = self.mutate_next_write_key.lock().unwrap().take() {
            velorix_k8s::lease::set_partition_annotations(lease, &key);
        }
    }

    fn next_resource_version(&self) -> String {
        let mut next = self.next_resource_version.lock().unwrap();
        let value = *next;
        *next += 1;
        value.to_string()
    }
}

fn wrong_lease_key() -> PartitionLeaseKey {
    PartitionLeaseKey {
        namespace: "default".to_string(),
        view_id: "wrong_view".to_string(),
        stream_id: "wrong_stream".to_string(),
        partition_id: 1,
    }
}

fn lease_key() -> PartitionLeaseKey {
    PartitionLeaseKey {
        namespace: "default".to_string(),
        view_id: "balances_by_account".to_string(),
        stream_id: "orders".to_string(),
        partition_id: 0,
    }
}

fn acquire_request(owner_id: &str, now_unix_ms: u64, ttl_ms: u64) -> LeaseAcquireRequest {
    LeaseAcquireRequest {
        key: lease_key(),
        owner_id: owner_id.to_string(),
        now_unix_ms,
        ttl_ms,
    }
}

fn lease_with_spec(spec: LeaseSpec) -> Lease {
    let mut lease = Lease {
        metadata: ObjectMeta {
            name: Some("velorix-test".to_string()),
            namespace: Some("default".to_string()),
            resource_version: Some("1".to_string()),
            ..ObjectMeta::default()
        },
        spec: Some(spec),
    };
    velorix_k8s::lease::set_partition_annotations(&mut lease, &lease_key());
    lease
}

fn micro_time(unix_ms: i64) -> MicroTime {
    MicroTime(Timestamp::from_millisecond(unix_ms).unwrap())
}

fn temp_store() -> (TempDir, Arc<dyn ObjectStore>) {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();
    (temp_dir, Arc::new(store))
}
