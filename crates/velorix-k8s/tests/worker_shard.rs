use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use kube::runtime::watcher::Event;
use velorix_control::{
    lease::{
        LeaseAcquireRequest, LeaseError, PartitionLeaseClient, PartitionLeaseGrant,
        PartitionLeaseKey,
    },
    reconcile_plan::{ReconcileBlockReason, WorkerFact},
};
use velorix_k8s::{
    crd::{
        ObjectStoreAuthorityRef, OwnerEpochStatus, VelorixWorkerShard, VelorixWorkerShardSpec,
        WorkerShardStatus,
    },
    worker_shard::{
        handle_worker_shard_event, handle_worker_shard_event_with_output_sink,
        reconcile_worker_shard, worker_shard_watch_event, WorkerShardCommand,
        WorkerShardEpochStore, WorkerShardError, WorkerShardEvent, WorkerShardReconcileConfig,
        WorkerShardReconcileInput, WorkerShardReconcileOutput,
    },
};
use velorix_storage::ownership::OwnershipEpochRecord;

#[tokio::test]
async fn worker_shard_status_only_never_starts_worker() {
    let mut shard = shard();
    shard.status = Some(WorkerShardStatus {
        observed_generation: Some(2),
        current_owner_epoch: Some(OwnerEpochStatus {
            stream_id: "orders".to_string(),
            partition_id: 0,
            owner_id: "worker-a".to_string(),
            owner_epoch: 7,
        }),
        readiness: None,
    });
    let lease = FakeLeaseClient::default().with_current(None);
    lease.fail_acquire();
    let epoch_store = FakeEpochStore::default();

    let output = reconcile_worker_shard(&shard, &lease, &epoch_store, input(None))
        .await
        .unwrap();

    assert!(!has_start(&output));
    assert_eq!(
        output.commands,
        vec![WorkerShardCommand::AcquireLease {
            owner_id: "worker-a".to_string()
        }]
    );
}

#[tokio::test]
async fn worker_shard_lease_only_blocks_until_durable_epoch_record_is_read_back() {
    let lease = FakeLeaseClient::default().with_current(Some(grant("worker-a", 5)));
    let epoch_store = FakeEpochStore::default();

    let output = reconcile_worker_shard(&shard(), &lease, &epoch_store, input(None))
        .await
        .unwrap();

    assert!(output.commands.is_empty());
    assert!(!has_start(&output));
    assert_eq!(
        output.plan.block_reason,
        Some(ReconcileBlockReason::MissingDurableEpochRecordSupport)
    );
}

#[tokio::test]
async fn worker_shard_missing_lease_acquires_persists_epoch_record_and_emits_start() {
    let lease = FakeLeaseClient::default()
        .with_current(None)
        .with_acquired(grant("worker-a", 1));
    let epoch_store = FakeEpochStore::default();

    let output = reconcile_worker_shard(&shard(), &lease, &epoch_store, input(None))
        .await
        .unwrap();

    assert_eq!(
        output.commands,
        vec![
            WorkerShardCommand::AcquireLease {
                owner_id: "worker-a".to_string()
            },
            WorkerShardCommand::PersistEpochRecord {
                owner_id: "worker-a".to_string(),
                owner_epoch: 1,
            },
            WorkerShardCommand::StartWorker {
                owner_id: "worker-a".to_string(),
                owner_epoch: 1,
            },
        ]
    );
    assert_eq!(
        epoch_store.read("orders", 0, 1).await.unwrap(),
        Some(epoch_record("worker-a", 1))
    );
}

#[tokio::test]
async fn applied_worker_shard_event_reconciles_through_lease_and_epoch_authority() {
    let lease = FakeLeaseClient::default()
        .with_current(None)
        .with_acquired(grant("worker-a", 1));
    let epoch_store = FakeEpochStore::default();

    let output = handle_worker_shard_event(
        WorkerShardEvent::Applied(shard()),
        &lease,
        &epoch_store,
        input(None),
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(
        output.commands,
        vec![
            WorkerShardCommand::AcquireLease {
                owner_id: "worker-a".to_string()
            },
            WorkerShardCommand::PersistEpochRecord {
                owner_id: "worker-a".to_string(),
                owner_epoch: 1,
            },
            WorkerShardCommand::StartWorker {
                owner_id: "worker-a".to_string(),
                owner_epoch: 1,
            },
        ]
    );
    assert_eq!(
        epoch_store.read("orders", 0, 1).await.unwrap(),
        Some(epoch_record("worker-a", 1))
    );
}

#[tokio::test]
async fn applied_worker_shard_event_sends_acquire_persist_and_start_output_to_sink() {
    let lease = FakeLeaseClient::default()
        .with_current(None)
        .with_acquired(grant("worker-a", 1));
    let epoch_store = FakeEpochStore::default();
    let emitted = Arc::new(Mutex::new(Vec::new()));

    let output = handle_worker_shard_event_with_output_sink(
        WorkerShardEvent::Applied(shard()),
        &lease,
        &epoch_store,
        input(None),
        {
            let emitted = Arc::clone(&emitted);
            move |output| {
                emitted.lock().unwrap().push(output.commands.clone());
                Ok::<(), &'static str>(())
            }
        },
    )
    .await
    .unwrap()
    .unwrap();

    let expected = vec![
        WorkerShardCommand::AcquireLease {
            owner_id: "worker-a".to_string(),
        },
        WorkerShardCommand::PersistEpochRecord {
            owner_id: "worker-a".to_string(),
            owner_epoch: 1,
        },
        WorkerShardCommand::StartWorker {
            owner_id: "worker-a".to_string(),
            owner_epoch: 1,
        },
    ];
    assert_eq!(output.commands, expected);
    assert_eq!(*emitted.lock().unwrap(), vec![expected]);
}

#[tokio::test]
async fn applied_worker_shard_event_returns_sink_error() {
    let lease = FakeLeaseClient::default()
        .with_current(None)
        .with_acquired(grant("worker-a", 1));
    let epoch_store = FakeEpochStore::default();

    let err = handle_worker_shard_event_with_output_sink(
        WorkerShardEvent::Applied(shard()),
        &lease,
        &epoch_store,
        input(None),
        |_output| Err::<(), _>("enqueue failed"),
    )
    .await
    .unwrap_err();

    match err {
        WorkerShardError::CommandSink { message } => {
            assert_eq!(message, "enqueue failed");
        }
        other => panic!("expected command sink error, got {other:?}"),
    }
}

#[tokio::test]
async fn applied_worker_shard_event_sends_stop_output_to_sink() {
    let lease = FakeLeaseClient::default().with_current(Some(grant("worker-a", 6)));
    let epoch_store = FakeEpochStore::default().with_record(epoch_record("worker-a", 6));
    let emitted = Arc::new(Mutex::new(Vec::new()));

    let output = handle_worker_shard_event_with_output_sink(
        WorkerShardEvent::Applied(shard()),
        &lease,
        &epoch_store,
        input(Some(WorkerFact {
            owner_id: "worker-a".to_string(),
            owner_epoch: 5,
        })),
        {
            let emitted = Arc::clone(&emitted);
            move |output| {
                emitted.lock().unwrap().push(output.commands.clone());
                Ok::<(), &'static str>(())
            }
        },
    )
    .await
    .unwrap()
    .unwrap();

    let expected = vec![
        WorkerShardCommand::StopWorker {
            owner_id: "worker-a".to_string(),
            owner_epoch: 5,
        },
        WorkerShardCommand::StartWorker {
            owner_id: "worker-a".to_string(),
            owner_epoch: 6,
        },
    ];
    assert_eq!(output.commands, expected);
    assert_eq!(*emitted.lock().unwrap(), vec![expected]);
}

#[tokio::test]
async fn deleted_worker_shard_event_does_not_start_or_stop_workers() {
    let lease = FakeLeaseClient::default().with_current(Some(grant("worker-a", 1)));
    let epoch_store = FakeEpochStore::default().with_record(epoch_record("worker-a", 1));

    let output = handle_worker_shard_event(
        WorkerShardEvent::Deleted(shard()),
        &lease,
        &epoch_store,
        input(Some(WorkerFact {
            owner_id: "worker-a".to_string(),
            owner_epoch: 1,
        })),
    )
    .await
    .unwrap();

    assert_eq!(output, None);
}

#[tokio::test]
async fn deleted_worker_shard_event_does_not_send_output_to_sink() {
    let lease = FakeLeaseClient::default().with_current(Some(grant("worker-a", 1)));
    let epoch_store = FakeEpochStore::default().with_record(epoch_record("worker-a", 1));
    let emitted = Arc::new(Mutex::new(Vec::<Vec<WorkerShardCommand>>::new()));

    let output = handle_worker_shard_event_with_output_sink(
        WorkerShardEvent::Deleted(shard()),
        &lease,
        &epoch_store,
        input(Some(WorkerFact {
            owner_id: "worker-a".to_string(),
            owner_epoch: 1,
        })),
        {
            let emitted = Arc::clone(&emitted);
            move |output| {
                emitted.lock().unwrap().push(output.commands.clone());
                Ok::<(), &'static str>(())
            }
        },
    )
    .await
    .unwrap();

    assert_eq!(output, None);
    assert!(emitted.lock().unwrap().is_empty());
}

#[test]
fn worker_shard_watch_event_maps_kubernetes_events_to_reconcile_events() {
    assert!(worker_shard_watch_event(Event::Init).is_none());
    assert!(worker_shard_watch_event(Event::InitDone).is_none());

    match worker_shard_watch_event(Event::Apply(shard())) {
        Some(WorkerShardEvent::Applied(shard)) => {
            assert_eq!(shard.metadata.name.as_deref(), Some("orders-p0"));
        }
        other => panic!("expected applied worker shard event, got {other:?}"),
    }

    match worker_shard_watch_event(Event::InitApply(shard())) {
        Some(WorkerShardEvent::Applied(shard)) => {
            assert_eq!(shard.metadata.name.as_deref(), Some("orders-p0"));
        }
        other => panic!("expected init-applied worker shard event, got {other:?}"),
    }

    match worker_shard_watch_event(Event::Delete(shard())) {
        Some(WorkerShardEvent::Deleted(shard)) => {
            assert_eq!(shard.metadata.name.as_deref(), Some("orders-p0"));
        }
        other => panic!("expected deleted worker shard event, got {other:?}"),
    }
}

#[tokio::test]
async fn worker_shard_stale_worker_stops_when_higher_durable_epoch_is_observed() {
    let lease = FakeLeaseClient::default().with_current(Some(grant("worker-a", 6)));
    let epoch_store = FakeEpochStore::default().with_record(epoch_record("worker-a", 6));

    let output = reconcile_worker_shard(
        &shard(),
        &lease,
        &epoch_store,
        input(Some(WorkerFact {
            owner_id: "worker-a".to_string(),
            owner_epoch: 5,
        })),
    )
    .await
    .unwrap();

    assert_eq!(
        output.commands,
        vec![
            WorkerShardCommand::StopWorker {
                owner_id: "worker-a".to_string(),
                owner_epoch: 5,
            },
            WorkerShardCommand::StartWorker {
                owner_id: "worker-a".to_string(),
                owner_epoch: 6,
            },
        ]
    );
}

#[tokio::test]
async fn worker_shard_lease_epoch_conflict_blocks_with_no_start() {
    let lease = FakeLeaseClient::default().with_current(Some(grant("worker-a", 6)));
    let epoch_store = FakeEpochStore::default().with_record(epoch_record("worker-b", 6));

    let output = reconcile_worker_shard(&shard(), &lease, &epoch_store, input(None))
        .await
        .unwrap();

    assert!(output.commands.is_empty());
    assert!(!has_start(&output));
    assert_eq!(
        output.plan.block_reason,
        Some(ReconcileBlockReason::EpochRecordConflict)
    );
}

#[tokio::test]
async fn worker_shard_lease_epoch_regression_blocks_when_newer_durable_epoch_exists() {
    let lease = FakeLeaseClient::default().with_current(Some(grant("worker-a", 4)));
    let epoch_store = FakeEpochStore::default()
        .with_record(epoch_record("worker-a", 4))
        .with_record(epoch_record("worker-a", 6));

    let output = reconcile_worker_shard(&shard(), &lease, &epoch_store, input(None))
        .await
        .unwrap();

    assert!(output.commands.is_empty());
    assert!(!has_start(&output));
    assert_eq!(
        output.plan.block_reason,
        Some(ReconcileBlockReason::MissingDurableEpochRecordSupport)
    );
}

fn has_start(output: &WorkerShardReconcileOutput) -> bool {
    output
        .commands
        .iter()
        .any(|command| matches!(command, WorkerShardCommand::StartWorker { .. }))
}

fn input(worker: Option<WorkerFact>) -> WorkerShardReconcileInput {
    WorkerShardReconcileInput {
        now_unix_ms: 1_000,
        ttl_ms: 30_000,
        running_worker: worker,
        config: WorkerShardReconcileConfig {
            created_at: "2026-05-06T00:00:00Z".to_string(),
            previous_checkpoint_version: Some(8),
        },
    }
}

fn shard() -> VelorixWorkerShard {
    let mut shard = VelorixWorkerShard::new(
        "orders-p0",
        VelorixWorkerShardSpec {
            worker_id: "worker-a".to_string(),
            view_id: "balances_by_account".to_string(),
            stream_id: "orders".to_string(),
            partition_id: 0,
            desired_owner_id: "worker-a".to_string(),
            authority: ObjectStoreAuthorityRef {
                store_id: "primary".to_string(),
                namespace: "analytics".to_string(),
            },
        },
    );
    shard.metadata.namespace = Some("default".to_string());
    shard.metadata.generation = Some(2);
    shard
}

fn key() -> PartitionLeaseKey {
    PartitionLeaseKey {
        namespace: "default".to_string(),
        view_id: "balances_by_account".to_string(),
        stream_id: "orders".to_string(),
        partition_id: 0,
    }
}

fn grant(owner_id: &str, owner_epoch: u64) -> PartitionLeaseGrant {
    PartitionLeaseGrant {
        key: key(),
        owner_id: owner_id.to_string(),
        owner_epoch,
        expires_at_unix_ms: 31_000,
    }
}

fn epoch_record(owner_id: &str, owner_epoch: u64) -> OwnershipEpochRecord {
    OwnershipEpochRecord {
        stream_id: "orders".to_string(),
        partition_id: 0,
        owner_id: owner_id.to_string(),
        owner_epoch,
        lease_identity: velorix_k8s::lease::partition_lease_identity(&key()),
        created_at: "2026-05-06T00:00:00Z".to_string(),
        previous_epoch: owner_epoch.checked_sub(1),
        previous_checkpoint_version: Some(8),
    }
}

#[derive(Clone, Default)]
struct FakeLeaseClient {
    current: Arc<Mutex<Option<PartitionLeaseGrant>>>,
    acquired: Arc<Mutex<Option<PartitionLeaseGrant>>>,
    fail_acquire: Arc<Mutex<bool>>,
}

impl FakeLeaseClient {
    fn with_current(self, current: Option<PartitionLeaseGrant>) -> Self {
        *self.current.lock().unwrap() = current;
        self
    }

    fn with_acquired(self, acquired: PartitionLeaseGrant) -> Self {
        *self.acquired.lock().unwrap() = Some(acquired);
        self
    }

    fn fail_acquire(&self) {
        *self.fail_acquire.lock().unwrap() = true;
    }
}

#[async_trait]
impl PartitionLeaseClient for FakeLeaseClient {
    async fn acquire_or_renew(
        &self,
        _request: LeaseAcquireRequest,
    ) -> Result<PartitionLeaseGrant, LeaseError> {
        if *self.fail_acquire.lock().unwrap() {
            return Err(LeaseError::LeaseNotHeld);
        }

        let grant = self
            .acquired
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_else(|| grant("worker-a", 1));
        *self.current.lock().unwrap() = Some(grant.clone());
        Ok(grant)
    }

    async fn current(
        &self,
        _key: &PartitionLeaseKey,
        _now_unix_ms: u64,
    ) -> Result<Option<PartitionLeaseGrant>, LeaseError> {
        Ok(self.current.lock().unwrap().clone())
    }

    async fn release(
        &self,
        _key: &PartitionLeaseKey,
        _owner_id: &str,
        _owner_epoch: u64,
        _now_unix_ms: u64,
    ) -> Result<(), LeaseError> {
        Ok(())
    }
}

#[derive(Clone, Default)]
struct FakeEpochStore {
    records: Arc<Mutex<EpochRecords>>,
}

type EpochRecords = BTreeMap<(String, u32, u64), OwnershipEpochRecord>;

impl FakeEpochStore {
    fn with_record(self, record: OwnershipEpochRecord) -> Self {
        self.insert(record);
        self
    }

    fn insert(&self, record: OwnershipEpochRecord) {
        self.records.lock().unwrap().insert(
            (
                record.stream_id.clone(),
                record.partition_id,
                record.owner_epoch,
            ),
            record,
        );
    }
}

#[async_trait]
impl WorkerShardEpochStore for FakeEpochStore {
    async fn read(
        &self,
        stream_id: &str,
        partition_id: u32,
        owner_epoch: u64,
    ) -> Result<Option<OwnershipEpochRecord>, velorix_k8s::worker_shard::WorkerShardError> {
        Ok(self
            .records
            .lock()
            .unwrap()
            .get(&(stream_id.to_string(), partition_id, owner_epoch))
            .cloned())
    }

    async fn create(
        &self,
        record: OwnershipEpochRecord,
    ) -> Result<(), velorix_k8s::worker_shard::WorkerShardError> {
        self.insert(record);
        Ok(())
    }

    async fn has_newer(
        &self,
        stream_id: &str,
        partition_id: u32,
        owner_epoch: u64,
    ) -> Result<bool, velorix_k8s::worker_shard::WorkerShardError> {
        Ok(self.records.lock().unwrap().values().any(|record| {
            record.stream_id == stream_id
                && record.partition_id == partition_id
                && record.owner_epoch > owner_epoch
        }))
    }
}
