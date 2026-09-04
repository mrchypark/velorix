use std::{
    collections::BTreeMap,
    convert::Infallible,
    fs,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use futures::{channel::oneshot, stream, FutureExt};
use http::{Method, Request, Response, StatusCode};
use kube::{
    client::{Body, ClientBuilder},
    runtime::watcher::Event,
    ResourceExt,
};
use object_store::memory::InMemory;
use serde_json::{json, Value};
use velorix_control::{
    lease::{
        LeaseAcquireRequest, LeaseError, PartitionLeaseClient, PartitionLeaseGrant,
        PartitionLeaseKey,
    },
    reconcile_plan::{
        ObservedControlPlaneFacts, ReconcileAction, ReconcileBlockReason, ReconcilePlan, WorkerFact,
    },
};
use velorix_k8s::{
    crd::{
        ObjectStoreAuthorityRef, OwnerEpochStatus, VelorixWorkerShard, VelorixWorkerShardSpec,
        WorkerShardStatus,
    },
    startup::{validate_operator_authority, OperatorAuthorityStartupComponents},
    worker_shard::{
        build_kubernetes_worker_shard_operator_runtime, execute_worker_shard_commands,
        handle_worker_shard_event, handle_worker_shard_event_with_command_executor,
        handle_worker_shard_event_with_output_sink,
        handle_worker_shard_event_with_scoped_command_executor_and_authority,
        reconcile_worker_shard, resync_worker_shards_before_watch_with_kubernetes_runtime,
        resync_worker_shards_once_with_operator_runtime,
        resync_worker_shards_periodically_with_kubernetes_runtime,
        run_worker_shard_lifecycle_with_operator_event_stream, runtime_identity_from_worker_shard,
        worker_shard_pod_name, worker_shard_pod_name_for_identity, worker_shard_watch_event,
        KubernetesPodWorkerShardCommandExecutor, KubernetesPodWorkerShardScopedCommandExecutor,
        ProcessWorkerShardCommandExecutor, WorkerShardCommand, WorkerShardCommandExecutor,
        WorkerShardCommandExecutorError, WorkerShardEpochStore, WorkerShardError, WorkerShardEvent,
        WorkerShardLifecycleExit, WorkerShardLifecycleOptions, WorkerShardOperatorRuntime,
        WorkerShardPeriodicResyncOptions, WorkerShardPeriodicResyncSchedule,
        WorkerShardPodTemplate, WorkerShardProcessCommand, WorkerShardReconcileConfig,
        WorkerShardReconcileInput, WorkerShardReconcileOutput, WorkerShardResyncOptions,
        WorkerShardResyncSummary, WorkerShardRuntimeIdentity, WorkerShardScopedCommandExecutor,
    },
};
use velorix_storage::ownership::OwnershipEpochRecord;

#[tokio::test]
async fn checkpoint_publisher_epoch_store_for_production_uses_validated_authority() {
    let validated_authority = validate_operator_authority(
        ObjectStoreAuthorityRef::default(),
        Arc::new(InMemory::new()),
        "worker-shard-authority",
        "v1/probes/worker-shard",
    )
    .await
    .unwrap();
    let epoch_store =
        OperatorAuthorityStartupComponents::from_validated_authority(validated_authority)
            .worker_shard_epoch_store()
            .unwrap();
    let record = epoch_record("worker-a", 1);

    epoch_store.create(record.clone()).await.unwrap();

    assert_eq!(
        epoch_store
            .read(&record.stream_id, record.partition_id, record.owner_epoch)
            .await
            .unwrap(),
        Some(record)
    );
}

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
async fn applied_worker_shard_event_executes_reconciled_worker_commands() {
    let lease = FakeLeaseClient::default()
        .with_current(None)
        .with_acquired(grant("worker-a", 1));
    let epoch_store = FakeEpochStore::default();
    let executor = FakeCommandExecutor::default();

    let output = handle_worker_shard_event_with_command_executor(
        WorkerShardEvent::Applied(shard()),
        &lease,
        &epoch_store,
        input(None),
        &executor,
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(
        output.commands,
        vec![
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
        ]
    );
    assert_eq!(
        executor.actions(),
        vec![ExecutedWorkerCommand::Start {
            owner_id: "worker-a".to_string(),
            owner_epoch: 1,
        }]
    );
    assert_eq!(
        epoch_store.read("orders", 0, 1).await.unwrap(),
        Some(epoch_record("worker-a", 1))
    );
}

#[tokio::test]
async fn applied_worker_shard_event_returns_executor_error_from_operator_wiring() {
    let lease = FakeLeaseClient::default()
        .with_current(None)
        .with_acquired(grant("worker-a", 1));
    let epoch_store = FakeEpochStore::default();
    let executor = FakeCommandExecutor::default().fail_start("pod create failed");

    let err = handle_worker_shard_event_with_command_executor(
        WorkerShardEvent::Applied(shard()),
        &lease,
        &epoch_store,
        input(None),
        &executor,
    )
    .await
    .unwrap_err();

    match err {
        WorkerShardError::CommandExecutor { message } => {
            assert_eq!(message, "pod create failed");
        }
        other => panic!("expected command executor error, got {other:?}"),
    }
    assert_eq!(
        executor.actions(),
        vec![ExecutedWorkerCommand::Start {
            owner_id: "worker-a".to_string(),
            owner_epoch: 1,
        }]
    );
}

#[tokio::test]
async fn applied_worker_shard_event_wires_reconciled_start_to_kubernetes_pod_executor() {
    let lease = FakeLeaseClient::default()
        .with_current(None)
        .with_acquired(grant("worker-a", 1));
    let epoch_store = FakeEpochStore::default();
    let (client, requests) = fake_pod_client(StatusCode::CREATED);
    let executor = KubernetesPodWorkerShardCommandExecutor::new(
        client,
        "workers",
        WorkerShardPodTemplate::new("ghcr.io/velorix/velorix-worker:1.0.0").unwrap(),
    );

    let output = handle_worker_shard_event_with_command_executor(
        WorkerShardEvent::Applied(shard()),
        &lease,
        &epoch_store,
        input(None),
        &executor,
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(
        output.commands,
        vec![
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
        ]
    );
    assert_eq!(
        epoch_store.read("orders", 0, 1).await.unwrap(),
        Some(epoch_record("worker-a", 1))
    );

    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, Method::POST);
    assert_eq!(requests[0].path, "/api/v1/namespaces/workers/pods");
    assert_eq!(
        requests[0].body["metadata"]["name"],
        worker_shard_pod_name("worker-a", 1)
    );
    assert_eq!(
        requests[0].body["spec"]["containers"][0]["env"],
        json!([
            {"name": "VELORIX_WORKER_OWNER_ID", "value": "worker-a"},
            {"name": "VELORIX_WORKER_OWNER_EPOCH", "value": "1"}
        ])
    );
}

#[tokio::test]
async fn worker_shard_operator_runtime_executes_only_after_durable_lease_epoch_authority() {
    let lease = FakeLeaseClient::default()
        .with_current(None)
        .with_acquired(grant("worker-a", 1));
    let epoch_store = FakeEpochStore::default();
    let executor = FakeCommandExecutor::default();
    let runtime = WorkerShardOperatorRuntime::new(lease, epoch_store.clone(), executor.clone());

    let output = runtime
        .handle_event(WorkerShardEvent::Applied(shard()), input(None))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        output.commands,
        vec![
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
        ]
    );
    assert_eq!(
        epoch_store.read("orders", 0, 1).await.unwrap(),
        Some(epoch_record("worker-a", 1))
    );
    assert_eq!(
        executor.actions(),
        vec![ExecutedWorkerCommand::Start {
            owner_id: "worker-a".to_string(),
            owner_epoch: 1,
        }]
    );

    let blocked_runtime = WorkerShardOperatorRuntime::new(
        FakeLeaseClient::default().with_current(Some(grant("worker-a", 5))),
        FakeEpochStore::default(),
        FakeCommandExecutor::default(),
    );

    let blocked = blocked_runtime
        .handle_event(WorkerShardEvent::Applied(shard()), input(None))
        .await
        .unwrap()
        .unwrap();

    assert!(blocked.commands.is_empty());
    assert!(!has_start(&blocked));
}

#[tokio::test]
async fn worker_shard_operator_runtime_surfaces_authority_failures_without_starting_worker() {
    let lease = FakeLeaseClient::default().with_current(None);
    lease.fail_acquire();
    let executor = FakeCommandExecutor::default();
    let runtime =
        WorkerShardOperatorRuntime::new(lease, FakeEpochStore::default(), executor.clone());

    let err = runtime
        .handle_event(
            WorkerShardEvent::Applied(shard()),
            input(Some(WorkerFact {
                owner_id: "worker-a".to_string(),
                owner_epoch: 9,
            })),
        )
        .await
        .unwrap_err();

    match err {
        WorkerShardError::Authority { message } => {
            assert!(message.contains("held"));
        }
        other => panic!("expected runtime authority error, got {other:?}"),
    }
    assert_eq!(
        executor.actions(),
        vec![ExecutedWorkerCommand::Stop {
            owner_id: "worker-a".to_string(),
            owner_epoch: 9,
        }]
    );
}

#[tokio::test]
async fn worker_shard_operator_runtime_stops_matching_worker_when_renewal_fails() {
    let lease = FakeLeaseClient::default().with_current(Some(grant("worker-a", 1)));
    lease.fail_acquire();
    let executor = FakeCommandExecutor::default();
    let runtime = WorkerShardOperatorRuntime::with_authority(
        lease,
        FakeEpochStore::default().with_record(epoch_record("worker-a", 1)),
        executor.clone(),
        authority(),
    );

    let error = runtime
        .handle_event(
            WorkerShardEvent::Applied(shard()),
            input(Some(WorkerFact {
                owner_id: "worker-a".to_string(),
                owner_epoch: 1,
            })),
        )
        .await
        .unwrap_err();

    assert!(matches!(error, WorkerShardError::Authority { .. }));
    assert_eq!(
        executor.actions(),
        vec![ExecutedWorkerCommand::Stop {
            owner_id: "worker-a".to_string(),
            owner_epoch: 1,
        }]
    );
}

#[tokio::test]
async fn kubernetes_worker_shard_operator_runtime_uses_checked_epoch_store_from_startup_components()
{
    let validated_authority = validate_operator_authority(
        authority(),
        Arc::new(InMemory::new()),
        "worker-shard-runtime-authority",
        "v1/probes/worker-shard-runtime",
    )
    .await
    .unwrap();
    let components =
        OperatorAuthorityStartupComponents::from_validated_authority(validated_authority);
    let (client, requests) = fake_worker_shard_runtime_client();
    let runtime = build_kubernetes_worker_shard_operator_runtime(
        client,
        "default",
        &components,
        WorkerShardPodTemplate::new("ghcr.io/velorix/velorix-worker:1.0.0").unwrap(),
    )
    .unwrap();

    let output = runtime
        .handle_event(WorkerShardEvent::Applied(shard()), input(None))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        output.commands,
        vec![
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
        ]
    );

    let epoch_store = components.worker_shard_epoch_store().unwrap();
    assert_eq!(
        epoch_store.read("orders", 0, 1).await.unwrap(),
        Some(epoch_record("worker-a", 1))
    );

    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 4);
    assert_eq!(requests[0].method, Method::GET);
    assert!(requests[0]
        .path
        .starts_with("/apis/coordination.k8s.io/v1/namespaces/default/leases/velorix-"));
    assert_eq!(requests[1].method, Method::GET);
    assert!(requests[1]
        .path
        .starts_with("/apis/coordination.k8s.io/v1/namespaces/default/leases/velorix-"));
    assert_eq!(requests[2].method, Method::POST);
    assert_eq!(
        requests[2].path,
        "/apis/coordination.k8s.io/v1/namespaces/default/leases"
    );
    assert_eq!(
        requests[2].body["metadata"]["annotations"]["control.velorix.io/stream-id"],
        "orders"
    );
    assert_eq!(requests[3].method, Method::POST);
    assert_eq!(requests[3].path, "/api/v1/namespaces/default/pods");
    let expected_identity = runtime_identity_from_worker_shard(&shard(), "worker-a", 1).unwrap();
    assert_eq!(
        requests[3].body["metadata"]["name"],
        worker_shard_pod_name_for_identity(&expected_identity)
    );
}

#[tokio::test]
async fn kubernetes_worker_shard_operator_runtime_rejects_shard_authority_mismatch() {
    let validated_authority = validate_operator_authority(
        ObjectStoreAuthorityRef::default(),
        Arc::new(InMemory::new()),
        "worker-shard-runtime-authority",
        "v1/probes/worker-shard-runtime-mismatch",
    )
    .await
    .unwrap();
    let components =
        OperatorAuthorityStartupComponents::from_validated_authority(validated_authority);
    let (client, requests) = fake_worker_shard_runtime_client();
    let runtime = build_kubernetes_worker_shard_operator_runtime(
        client,
        "default",
        &components,
        WorkerShardPodTemplate::new("ghcr.io/velorix/velorix-worker:1.0.0").unwrap(),
    )
    .unwrap();

    let err = runtime
        .handle_event(WorkerShardEvent::Applied(shard()), input(None))
        .await
        .unwrap_err();

    match err {
        WorkerShardError::AuthorityMismatch { actual, expected } => {
            assert_eq!(actual, authority());
            assert_eq!(expected, ObjectStoreAuthorityRef::default());
        }
        other => panic!("expected authority mismatch, got {other:?}"),
    }
    assert!(requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn worker_shard_resync_pass_reconciles_listed_shards_through_authority_bound_runtime() {
    let (client, requests) = fake_worker_shard_list_client(vec![list_page(vec![shard()], None)]);
    let runtime = WorkerShardOperatorRuntime::with_authority(
        FakeLeaseClient::default()
            .with_current(None)
            .with_acquired(grant("worker-a", 1)),
        FakeEpochStore::default(),
        FakeCommandExecutor::default(),
        authority(),
    );

    let summary = resync_worker_shards_once_with_operator_runtime(
        client,
        "default",
        &runtime,
        |_| input(None),
        WorkerShardResyncOptions::default(),
    )
    .await
    .unwrap();

    assert_eq!(
        summary,
        WorkerShardResyncSummary {
            listed: 1,
            applied: 1,
        }
    );
    assert_eq!(requests.lock().unwrap()[0].method, Method::GET);
    assert_eq!(
        requests.lock().unwrap()[0].path,
        "/apis/control.velorix.io/v1alpha1/namespaces/default/velorixworkershards"
    );
}

#[tokio::test]
async fn worker_shard_resync_pass_stops_stale_running_worker_without_watch_event() {
    let (client, _requests) = fake_worker_shard_list_client(vec![list_page(vec![shard()], None)]);
    let executor = FakeCommandExecutor::default();
    let runtime = WorkerShardOperatorRuntime::with_authority(
        FakeLeaseClient::default().with_current(Some(grant("worker-a", 6))),
        FakeEpochStore::default().with_record(epoch_record("worker-a", 6)),
        executor.clone(),
        authority(),
    );

    let summary = resync_worker_shards_once_with_operator_runtime(
        client,
        "default",
        &runtime,
        |_| {
            input(Some(WorkerFact {
                owner_id: "worker-a".to_string(),
                owner_epoch: 5,
            }))
        },
        WorkerShardResyncOptions::default(),
    )
    .await
    .unwrap();

    assert_eq!(
        summary,
        WorkerShardResyncSummary {
            listed: 1,
            applied: 1,
        }
    );
    assert_eq!(
        executor.actions(),
        vec![
            ExecutedWorkerCommand::Stop {
                owner_id: "worker-a".to_string(),
                owner_epoch: 5,
            },
            ExecutedWorkerCommand::Start {
                owner_id: "worker-a".to_string(),
                owner_epoch: 6,
            },
        ]
    );
}

#[tokio::test]
async fn worker_shard_resync_pass_sorts_listed_shards_before_reconcile() {
    let west = shard_with_stream_partition("orders-west", 1);
    let east = shard_with_stream_partition("orders-east", 0);
    let (client, _requests) =
        fake_worker_shard_list_client(vec![list_page(vec![west.clone(), east.clone()], None)]);
    let runtime = WorkerShardOperatorRuntime::with_authority(
        FakeLeaseClient::default(),
        FakeEpochStore::default(),
        FakeCommandExecutor::default(),
        authority(),
    );
    let order = Arc::new(Mutex::new(Vec::new()));

    resync_worker_shards_once_with_operator_runtime(
        client,
        "default",
        &runtime,
        {
            let order = Arc::clone(&order);
            move |shard| {
                order.lock().unwrap().push(shard.name_any());
                input(None)
            }
        },
        WorkerShardResyncOptions::default(),
    )
    .await
    .unwrap();

    assert_eq!(
        *order.lock().unwrap(),
        vec![east.name_any(), west.name_any()]
    );
}

#[tokio::test]
async fn worker_shard_resync_pass_treats_empty_continue_token_as_terminal_page() {
    let (client, requests) =
        fake_worker_shard_list_client(vec![list_page(vec![shard()], Some(""))]);
    let runtime = WorkerShardOperatorRuntime::with_authority(
        FakeLeaseClient::default()
            .with_current(None)
            .with_acquired(grant("worker-a", 1)),
        FakeEpochStore::default(),
        FakeCommandExecutor::default(),
        authority(),
    );

    let summary = resync_worker_shards_once_with_operator_runtime(
        client,
        "default",
        &runtime,
        |_| input(None),
        WorkerShardResyncOptions::default(),
    )
    .await
    .unwrap();

    assert_eq!(
        summary,
        WorkerShardResyncSummary {
            listed: 1,
            applied: 1,
        }
    );
    assert_eq!(requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn worker_shard_resync_pass_rejects_unvalidated_runtime_before_listing() {
    let (client, requests) = fake_worker_shard_list_client(vec![list_page(vec![shard()], None)]);
    let runtime = WorkerShardOperatorRuntime::new(
        FakeLeaseClient::default(),
        FakeEpochStore::default(),
        FakeCommandExecutor::default(),
    );

    let error = resync_worker_shards_once_with_operator_runtime(
        client,
        "default",
        &runtime,
        |_| input(None),
        WorkerShardResyncOptions::default(),
    )
    .await
    .unwrap_err();

    match error {
        WorkerShardError::Authority { message } => {
            assert!(message.contains("validated operator authority"));
        }
        other => panic!("expected authority error, got {other:?}"),
    }
    assert!(requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn worker_shard_resync_pass_fails_closed_when_object_bound_is_exceeded() {
    let (client, requests) = fake_worker_shard_list_client(vec![list_page(vec![shard()], None)]);
    let runtime = WorkerShardOperatorRuntime::with_authority(
        FakeLeaseClient::default()
            .with_current(None)
            .with_acquired(grant("worker-a", 1)),
        FakeEpochStore::default(),
        FakeCommandExecutor::default(),
        authority(),
    );

    let error = resync_worker_shards_once_with_operator_runtime(
        client,
        "default",
        &runtime,
        |_| input(None),
        WorkerShardResyncOptions {
            max_shards: 0,
            ..WorkerShardResyncOptions::default()
        },
    )
    .await
    .unwrap_err();

    match error {
        WorkerShardError::ResyncBoundExceeded { bound, limit } => {
            assert_eq!(bound, "worker shards");
            assert_eq!(limit, 0);
        }
        other => panic!("expected resync bound error, got {other:?}"),
    }
    assert!(requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn worker_shard_resync_pass_fails_closed_when_page_bound_is_exceeded() {
    let (client, requests) =
        fake_worker_shard_list_client(vec![list_page(Vec::new(), Some("next-page"))]);
    let runtime = WorkerShardOperatorRuntime::with_authority(
        FakeLeaseClient::default(),
        FakeEpochStore::default(),
        FakeCommandExecutor::default(),
        authority(),
    );

    let error = resync_worker_shards_once_with_operator_runtime(
        client,
        "default",
        &runtime,
        |_| input(None),
        WorkerShardResyncOptions {
            max_pages: 1,
            ..WorkerShardResyncOptions::default()
        },
    )
    .await
    .unwrap_err();

    match error {
        WorkerShardError::ResyncBoundExceeded { bound, limit } => {
            assert_eq!(bound, "worker shard list pages");
            assert_eq!(limit, 1);
        }
        other => panic!("expected resync bound error, got {other:?}"),
    }
    assert_eq!(requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn kubernetes_worker_shard_startup_resync_reconciles_before_watch_entry() {
    let validated_authority = validate_operator_authority(
        authority(),
        Arc::new(InMemory::new()),
        "worker-shard-startup-resync-authority",
        "v1/probes/worker-shard-startup-resync",
    )
    .await
    .unwrap();
    let components =
        OperatorAuthorityStartupComponents::from_validated_authority(validated_authority);
    let (client, requests) =
        fake_worker_shard_startup_resync_client(vec![list_page(vec![shard()], None)]);

    let (_runtime, summary) = resync_worker_shards_before_watch_with_kubernetes_runtime(
        client,
        "default",
        &components,
        WorkerShardPodTemplate::new("ghcr.io/velorix/velorix-worker:1.0.0").unwrap(),
        |_| input(None),
        WorkerShardResyncOptions::default(),
    )
    .await
    .unwrap();

    assert_eq!(
        summary,
        WorkerShardResyncSummary {
            listed: 1,
            applied: 1,
        }
    );
    assert_eq!(
        components
            .worker_shard_epoch_store()
            .unwrap()
            .read("orders", 0, 1)
            .await
            .unwrap(),
        Some(epoch_record("worker-a", 1))
    );

    let requests = requests.lock().unwrap();
    assert_eq!(requests[0].method, Method::GET);
    assert_eq!(
        requests[0].path,
        "/apis/control.velorix.io/v1alpha1/namespaces/default/velorixworkershards"
    );
    assert_eq!(requests[1].method, Method::GET);
    assert!(requests[1]
        .path
        .starts_with("/apis/coordination.k8s.io/v1/namespaces/default/leases/velorix-"));
    assert_eq!(requests[2].method, Method::GET);
    assert!(requests[2]
        .path
        .starts_with("/apis/coordination.k8s.io/v1/namespaces/default/leases/velorix-"));
    assert_eq!(requests[3].method, Method::POST);
    assert_eq!(
        requests[3].path,
        "/apis/coordination.k8s.io/v1/namespaces/default/leases"
    );
    assert_eq!(requests[4].method, Method::POST);
    assert_eq!(requests[4].path, "/api/v1/namespaces/default/pods");
}

#[tokio::test]
async fn kubernetes_worker_shard_startup_resync_rejects_authority_mismatch_before_worker_start() {
    let validated_authority = validate_operator_authority(
        ObjectStoreAuthorityRef::default(),
        Arc::new(InMemory::new()),
        "worker-shard-startup-resync-authority-mismatch",
        "v1/probes/worker-shard-startup-resync-mismatch",
    )
    .await
    .unwrap();
    let components =
        OperatorAuthorityStartupComponents::from_validated_authority(validated_authority);
    let (client, requests) =
        fake_worker_shard_startup_resync_client(vec![list_page(vec![shard()], None)]);

    let err = match resync_worker_shards_before_watch_with_kubernetes_runtime(
        client,
        "default",
        &components,
        WorkerShardPodTemplate::new("ghcr.io/velorix/velorix-worker:1.0.0").unwrap(),
        |_| input(None),
        WorkerShardResyncOptions::default(),
    )
    .await
    {
        Ok(_) => panic!("expected startup resync authority mismatch"),
        Err(err) => err,
    };

    match err {
        WorkerShardError::AuthorityMismatch { actual, expected } => {
            assert_eq!(actual, authority());
            assert_eq!(expected, ObjectStoreAuthorityRef::default());
        }
        other => panic!("expected authority mismatch, got {other:?}"),
    }
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].path,
        "/apis/control.velorix.io/v1alpha1/namespaces/default/velorixworkershards"
    );
}

#[tokio::test]
async fn kubernetes_worker_shard_startup_resync_bound_failure_does_not_enter_worker_execution() {
    let validated_authority = validate_operator_authority(
        authority(),
        Arc::new(InMemory::new()),
        "worker-shard-startup-resync-bound-authority",
        "v1/probes/worker-shard-startup-resync-bound",
    )
    .await
    .unwrap();
    let components =
        OperatorAuthorityStartupComponents::from_validated_authority(validated_authority);
    let (client, requests) =
        fake_worker_shard_startup_resync_client(vec![list_page(vec![shard()], None)]);

    let err = match resync_worker_shards_before_watch_with_kubernetes_runtime(
        client,
        "default",
        &components,
        WorkerShardPodTemplate::new("ghcr.io/velorix/velorix-worker:1.0.0").unwrap(),
        |_| input(None),
        WorkerShardResyncOptions {
            max_pages: 0,
            ..WorkerShardResyncOptions::default()
        },
    )
    .await
    {
        Ok(_) => panic!("expected startup resync bound failure"),
        Err(err) => err,
    };

    match err {
        WorkerShardError::ResyncBoundExceeded { bound, limit } => {
            assert_eq!(bound, "worker shard list pages");
            assert_eq!(limit, 0);
        }
        other => panic!("expected resync bound error, got {other:?}"),
    }
    assert!(requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn kubernetes_worker_shard_periodic_resync_requeues_bounded_authority_checks() {
    let validated_authority = validate_operator_authority(
        authority(),
        Arc::new(InMemory::new()),
        "worker-shard-periodic-resync-authority",
        "v1/probes/worker-shard-periodic-resync",
    )
    .await
    .unwrap();
    let components =
        OperatorAuthorityStartupComponents::from_validated_authority(validated_authority);
    let (client, requests) = fake_worker_shard_startup_resync_client(vec![
        list_page(vec![shard()], None),
        list_page(Vec::new(), None),
    ]);

    let summaries = resync_worker_shards_periodically_with_kubernetes_runtime(
        client,
        "default",
        &components,
        WorkerShardPodTemplate::new("ghcr.io/velorix/velorix-worker:1.0.0").unwrap(),
        |_| input(None),
        WorkerShardPeriodicResyncOptions {
            interval: Duration::from_millis(1),
            resync: WorkerShardResyncOptions::default(),
            max_cycles: Some(2),
        },
    )
    .await
    .unwrap();

    assert_eq!(
        summaries,
        vec![
            WorkerShardResyncSummary {
                listed: 1,
                applied: 1,
            },
            WorkerShardResyncSummary {
                listed: 0,
                applied: 0,
            },
        ]
    );
    assert_eq!(
        components
            .worker_shard_epoch_store()
            .unwrap()
            .read("orders", 0, 1)
            .await
            .unwrap(),
        Some(epoch_record("worker-a", 1))
    );

    let requests = requests.lock().unwrap();
    let list_requests = requests
        .iter()
        .filter(|request| {
            request.method == Method::GET
                && request
                    .path
                    .ends_with("/namespaces/default/velorixworkershards")
        })
        .count();
    assert_eq!(list_requests, 2);
    assert_eq!(requests[0].method, Method::GET);
    assert_eq!(requests[4].method, Method::POST);
    assert_eq!(requests[4].path, "/api/v1/namespaces/default/pods");
}

#[tokio::test]
async fn kubernetes_worker_shard_periodic_resync_rejects_unbounded_summary_collection_before_listing(
) {
    let validated_authority = validate_operator_authority(
        authority(),
        Arc::new(InMemory::new()),
        "worker-shard-periodic-resync-unbounded-authority",
        "v1/probes/worker-shard-periodic-resync-unbounded",
    )
    .await
    .unwrap();
    let components =
        OperatorAuthorityStartupComponents::from_validated_authority(validated_authority);
    let (client, requests) =
        fake_worker_shard_startup_resync_client(vec![list_page(vec![shard()], None)]);

    let error = resync_worker_shards_periodically_with_kubernetes_runtime(
        client,
        "default",
        &components,
        WorkerShardPodTemplate::new("ghcr.io/velorix/velorix-worker:1.0.0").unwrap(),
        |_| input(None),
        WorkerShardPeriodicResyncOptions {
            interval: Duration::from_millis(1),
            resync: WorkerShardResyncOptions::default(),
            max_cycles: None,
        },
    )
    .await
    .unwrap_err();

    match error {
        WorkerShardError::ResyncBoundExceeded { bound, limit } => {
            assert_eq!(bound, "worker shard periodic resync cycles");
            assert_eq!(limit, 0);
        }
        other => panic!("expected resync bound error, got {other:?}"),
    }
    assert!(requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn worker_shard_lifecycle_runs_initial_resync_before_stream_events() {
    let validated_authority = validate_operator_authority(
        authority(),
        Arc::new(InMemory::new()),
        "worker-shard-lifecycle-supervisor-authority",
        "v1/probes/worker-shard-lifecycle-supervisor",
    )
    .await
    .unwrap();
    let components =
        OperatorAuthorityStartupComponents::from_validated_authority(validated_authority);
    let (client, requests) =
        fake_worker_shard_startup_resync_client(vec![list_page(vec![shard()], None)]);
    let runtime = build_kubernetes_worker_shard_operator_runtime(
        client.clone(),
        "default",
        &components,
        WorkerShardPodTemplate::new("ghcr.io/velorix/velorix-worker:1.0.0").unwrap(),
    )
    .unwrap();

    let exit = run_worker_shard_lifecycle_with_operator_event_stream(
        client,
        "default",
        &runtime,
        |_| input(None),
        WorkerShardLifecycleOptions {
            initial_resync: WorkerShardResyncOptions::default(),
            periodic_resync: Some(WorkerShardPeriodicResyncSchedule {
                interval: Duration::from_secs(60),
                resync: WorkerShardResyncOptions::default(),
            }),
        },
        stream::iter(vec![Ok(WorkerShardEvent::Applied(
            shard_with_stream_partition("payments", 1),
        ))]),
        std::future::pending::<()>(),
    )
    .await
    .unwrap();

    assert_eq!(exit, WorkerShardLifecycleExit::WatchEnded);
    assert_eq!(
        components
            .worker_shard_epoch_store()
            .unwrap()
            .read("orders", 0, 1)
            .await
            .unwrap(),
        Some(epoch_record("worker-a", 1))
    );
    assert_eq!(
        components
            .worker_shard_epoch_store()
            .unwrap()
            .read("payments", 1, 1)
            .await
            .unwrap(),
        Some(epoch_record_for("payments", 1, "worker-a", 1))
    );

    let requests = requests.lock().unwrap();
    assert_eq!(requests[0].method, Method::GET);
    assert_eq!(
        requests[0].path,
        "/apis/control.velorix.io/v1alpha1/namespaces/default/velorixworkershards"
    );
    let pod_create_names = requests
        .iter()
        .filter(|request| {
            request.method == Method::POST && request.path == "/api/v1/namespaces/default/pods"
        })
        .map(|request| {
            request
                .body
                .pointer("/metadata/name")
                .and_then(Value::as_str)
                .unwrap()
                .to_string()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        pod_create_names,
        vec![
            worker_shard_pod_name_for_identity(
                &runtime_identity_from_worker_shard(&shard(), "worker-a", 1).unwrap()
            ),
            worker_shard_pod_name_for_identity(
                &runtime_identity_from_worker_shard(
                    &shard_with_stream_partition("payments", 1),
                    "worker-a",
                    1,
                )
                .unwrap()
            ),
        ]
    );
    let pod_delete_paths = requests
        .iter()
        .filter(|request| request.method == Method::DELETE)
        .map(|request| request.path.clone())
        .collect::<Vec<_>>();
    assert_eq!(
        pod_delete_paths,
        vec![
            format!(
                "/api/v1/namespaces/default/pods/{}",
                worker_shard_pod_name_for_identity(
                    &runtime_identity_from_worker_shard(&shard(), "worker-a", 1).unwrap()
                )
            ),
            format!(
                "/api/v1/namespaces/default/pods/{}",
                worker_shard_pod_name_for_identity(
                    &runtime_identity_from_worker_shard(
                        &shard_with_stream_partition("payments", 1),
                        "worker-a",
                        1,
                    )
                    .unwrap()
                )
            ),
        ]
    );
}

#[tokio::test]
async fn worker_shard_lifecycle_fences_locally_started_workers_when_watch_errors() {
    let validated_authority = validate_operator_authority(
        authority(),
        Arc::new(InMemory::new()),
        "worker-shard-lifecycle-watch-error-authority",
        "v1/probes/worker-shard-lifecycle-watch-error",
    )
    .await
    .unwrap();
    let components =
        OperatorAuthorityStartupComponents::from_validated_authority(validated_authority);
    let (client, requests) =
        fake_worker_shard_startup_resync_client(vec![list_page(vec![shard()], None)]);
    let runtime = build_kubernetes_worker_shard_operator_runtime(
        client.clone(),
        "default",
        &components,
        WorkerShardPodTemplate::new("ghcr.io/velorix/velorix-worker:1.0.0").unwrap(),
    )
    .unwrap();

    let error = run_worker_shard_lifecycle_with_operator_event_stream(
        client,
        "default",
        &runtime,
        |_| input(None),
        WorkerShardLifecycleOptions {
            initial_resync: WorkerShardResyncOptions::default(),
            periodic_resync: Some(WorkerShardPeriodicResyncSchedule {
                interval: Duration::from_secs(60),
                resync: WorkerShardResyncOptions::default(),
            }),
        },
        stream::iter(vec![Err(WorkerShardError::Authority {
            message: "watch failed".to_string(),
        })]),
        std::future::pending::<()>(),
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        WorkerShardError::Authority { message } if message == "watch failed"
    ));
    assert_eq!(
        requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request.method == Method::DELETE)
            .count(),
        1
    );
}

#[tokio::test]
async fn worker_shard_lifecycle_rejects_zero_periodic_interval_before_initial_resync() {
    let validated_authority = validate_operator_authority(
        authority(),
        Arc::new(InMemory::new()),
        "worker-shard-lifecycle-zero-interval-authority",
        "v1/probes/worker-shard-lifecycle-zero-interval",
    )
    .await
    .unwrap();
    let components =
        OperatorAuthorityStartupComponents::from_validated_authority(validated_authority);
    let (client, requests) =
        fake_worker_shard_startup_resync_client(vec![list_page(vec![shard()], None)]);
    let runtime = build_kubernetes_worker_shard_operator_runtime(
        client.clone(),
        "default",
        &components,
        WorkerShardPodTemplate::new("ghcr.io/velorix/velorix-worker:1.0.0").unwrap(),
    )
    .unwrap();

    let error = match run_worker_shard_lifecycle_with_operator_event_stream(
        client,
        "default",
        &runtime,
        |_| input(None),
        WorkerShardLifecycleOptions {
            initial_resync: WorkerShardResyncOptions::default(),
            periodic_resync: Some(WorkerShardPeriodicResyncSchedule {
                interval: Duration::ZERO,
                resync: WorkerShardResyncOptions::default(),
            }),
        },
        Box::pin(stream::pending()),
        std::future::pending::<()>(),
    )
    .await
    {
        Ok(_) => panic!("expected lifecycle zero interval failure"),
        Err(error) => error,
    };

    match error {
        WorkerShardError::ResyncBoundExceeded { bound, limit } => {
            assert_eq!(bound, "worker shard periodic resync interval");
            assert_eq!(limit, 0);
        }
        other => panic!("expected periodic interval bound error, got {other:?}"),
    }
    assert!(requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn worker_shard_lifecycle_rejects_initial_object_bound_before_initial_resync() {
    let validated_authority = validate_operator_authority(
        authority(),
        Arc::new(InMemory::new()),
        "worker-shard-lifecycle-initial-bound-authority",
        "v1/probes/worker-shard-lifecycle-initial-bound",
    )
    .await
    .unwrap();
    let components =
        OperatorAuthorityStartupComponents::from_validated_authority(validated_authority);
    let (client, requests) =
        fake_worker_shard_startup_resync_client(vec![list_page(vec![shard()], None)]);
    let runtime = build_kubernetes_worker_shard_operator_runtime(
        client.clone(),
        "default",
        &components,
        WorkerShardPodTemplate::new("ghcr.io/velorix/velorix-worker:1.0.0").unwrap(),
    )
    .unwrap();

    let error = match run_worker_shard_lifecycle_with_operator_event_stream(
        client,
        "default",
        &runtime,
        |_| input(None),
        WorkerShardLifecycleOptions {
            initial_resync: WorkerShardResyncOptions {
                max_shards: 0,
                ..WorkerShardResyncOptions::default()
            },
            periodic_resync: None,
        },
        Box::pin(stream::pending()),
        std::future::pending::<()>(),
    )
    .await
    {
        Ok(_) => panic!("expected lifecycle initial resync bound failure"),
        Err(error) => error,
    };

    match error {
        WorkerShardError::ResyncBoundExceeded { bound, limit } => {
            assert_eq!(bound, "worker shards");
            assert_eq!(limit, 0);
        }
        other => panic!("expected resync bound error, got {other:?}"),
    }
    assert!(requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn worker_shard_lifecycle_shutdown_before_startup_skips_kubernetes_side_effects() {
    let validated_authority = validate_operator_authority(
        authority(),
        Arc::new(InMemory::new()),
        "worker-shard-lifecycle-startup-shutdown-authority",
        "v1/probes/worker-shard-lifecycle-startup-shutdown",
    )
    .await
    .unwrap();
    let components =
        OperatorAuthorityStartupComponents::from_validated_authority(validated_authority);
    let (client, requests) =
        fake_worker_shard_startup_resync_client(vec![list_page(vec![shard()], None)]);
    let runtime = build_kubernetes_worker_shard_operator_runtime(
        client.clone(),
        "default",
        &components,
        WorkerShardPodTemplate::new("ghcr.io/velorix/velorix-worker:1.0.0").unwrap(),
    )
    .unwrap();

    let exit = run_worker_shard_lifecycle_with_operator_event_stream(
        client,
        "default",
        &runtime,
        |_| input(None),
        WorkerShardLifecycleOptions {
            initial_resync: WorkerShardResyncOptions::default(),
            periodic_resync: None,
        },
        Box::pin(stream::pending()),
        std::future::ready(()),
    )
    .await
    .unwrap();

    assert_eq!(exit, WorkerShardLifecycleExit::Shutdown);
    assert!(requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn worker_shard_lifecycle_ready_shutdown_still_requires_validated_authority() {
    let (client, requests) =
        fake_worker_shard_startup_resync_client(vec![list_page(vec![shard()], None)]);
    let runtime = WorkerShardOperatorRuntime::new(
        FakeLeaseClient::default(),
        FakeEpochStore::default(),
        FakeCommandExecutor::default(),
    );

    let error = run_worker_shard_lifecycle_with_operator_event_stream(
        client,
        "default",
        &runtime,
        |_| input(None),
        WorkerShardLifecycleOptions {
            initial_resync: WorkerShardResyncOptions::default(),
            periodic_resync: None,
        },
        Box::pin(stream::pending()),
        std::future::ready(()),
    )
    .await
    .unwrap_err();

    match error {
        WorkerShardError::Authority { message } => {
            assert!(message.contains("validated operator authority"));
        }
        other => panic!("expected authority error, got {other:?}"),
    }
    assert!(requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn worker_shard_lifecycle_accepts_and_polls_non_unpin_stream() {
    let validated_authority = validate_operator_authority(
        authority(),
        Arc::new(InMemory::new()),
        "worker-shard-lifecycle-non-unpin-authority",
        "v1/probes/worker-shard-lifecycle-non-unpin",
    )
    .await
    .unwrap();
    let components =
        OperatorAuthorityStartupComponents::from_validated_authority(validated_authority);
    let (client, requests) =
        fake_worker_shard_startup_resync_client(vec![list_page(vec![shard()], None)]);
    let runtime = build_kubernetes_worker_shard_operator_runtime(
        client.clone(),
        "default",
        &components,
        WorkerShardPodTemplate::new("ghcr.io/velorix/velorix-worker:1.0.0").unwrap(),
    )
    .unwrap();
    let event_polled = Arc::new(Mutex::new(false));
    let events = stream::unfold(
        (false, Arc::clone(&event_polled)),
        |(emitted, event_polled)| async move {
            if emitted {
                None
            } else {
                *event_polled.lock().unwrap() = true;
                Some((Ok(WorkerShardEvent::Deleted(shard())), (true, event_polled)))
            }
        },
    );
    let shutdown = std::future::pending::<()>();

    let exit = run_worker_shard_lifecycle_with_operator_event_stream(
        client,
        "default",
        &runtime,
        |_| input(None),
        WorkerShardLifecycleOptions {
            initial_resync: WorkerShardResyncOptions::default(),
            periodic_resync: None,
        },
        events,
        shutdown,
    )
    .await
    .unwrap();

    assert_eq!(exit, WorkerShardLifecycleExit::WatchEnded);
    assert!(*event_polled.lock().unwrap());
    assert_eq!(worker_shard_list_request_count(&requests), 1);
}

#[tokio::test]
async fn worker_shard_lifecycle_periodic_resync_waits_interval_after_initial_resync() {
    let validated_authority = validate_operator_authority(
        authority(),
        Arc::new(InMemory::new()),
        "worker-shard-lifecycle-periodic-delay-authority",
        "v1/probes/worker-shard-lifecycle-periodic-delay",
    )
    .await
    .unwrap();
    let components =
        OperatorAuthorityStartupComponents::from_validated_authority(validated_authority);
    let (client, requests) =
        fake_worker_shard_startup_resync_client(vec![list_page(Vec::new(), None)]);
    let runtime = build_kubernetes_worker_shard_operator_runtime(
        client.clone(),
        "default",
        &components,
        WorkerShardPodTemplate::new("ghcr.io/velorix/velorix-worker:1.0.0").unwrap(),
    )
    .unwrap();

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let mut shutdown_tx = Some(shutdown_tx);
    let requests_for_stream = Arc::clone(&requests);
    let events = stream::poll_fn(move |_context| {
        assert_eq!(worker_shard_list_request_count(&requests_for_stream), 1);
        if let Some(shutdown_tx) = shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
        std::task::Poll::Pending::<Option<Result<WorkerShardEvent, WorkerShardError>>>
    });

    let exit = run_worker_shard_lifecycle_with_operator_event_stream(
        client,
        "default",
        &runtime,
        |_| input(None),
        WorkerShardLifecycleOptions {
            initial_resync: WorkerShardResyncOptions::default(),
            periodic_resync: Some(WorkerShardPeriodicResyncSchedule {
                interval: Duration::from_secs(60),
                resync: WorkerShardResyncOptions::default(),
            }),
        },
        events,
        async {
            let _ = shutdown_rx.await;
        },
    )
    .await
    .unwrap();

    assert_eq!(exit, WorkerShardLifecycleExit::Shutdown);
    assert_eq!(worker_shard_list_request_count(&requests), 1);
}

#[tokio::test]
async fn worker_shard_lifecycle_periodic_resync_runs_until_shutdown_without_summary_growth() {
    let validated_authority = validate_operator_authority(
        authority(),
        Arc::new(InMemory::new()),
        "worker-shard-lifecycle-periodic-authority",
        "v1/probes/worker-shard-lifecycle-periodic",
    )
    .await
    .unwrap();
    let components =
        OperatorAuthorityStartupComponents::from_validated_authority(validated_authority);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let shutdown_tx = Arc::new(Mutex::new(Some(shutdown_tx)));
    let (client, requests) = fake_worker_shard_startup_resync_client_with_list_observer(
        vec![list_page(Vec::new(), None)],
        {
            let shutdown_tx = Arc::clone(&shutdown_tx);
            move |list_requests| {
                if list_requests >= 2 {
                    if let Some(shutdown_tx) = shutdown_tx.lock().unwrap().take() {
                        let _ = shutdown_tx.send(());
                    }
                }
            }
        },
    );
    let runtime = build_kubernetes_worker_shard_operator_runtime(
        client.clone(),
        "default",
        &components,
        WorkerShardPodTemplate::new("ghcr.io/velorix/velorix-worker:1.0.0").unwrap(),
    )
    .unwrap();

    let exit = tokio::time::timeout(
        Duration::from_secs(1),
        run_worker_shard_lifecycle_with_operator_event_stream(
            client,
            "default",
            &runtime,
            |_| input(None),
            WorkerShardLifecycleOptions {
                initial_resync: WorkerShardResyncOptions::default(),
                periodic_resync: Some(WorkerShardPeriodicResyncSchedule {
                    interval: Duration::from_millis(1),
                    resync: WorkerShardResyncOptions::default(),
                }),
            },
            stream::pending(),
            async {
                let _ = shutdown_rx.await;
            },
        ),
    )
    .await
    .expect("periodic lifecycle should shut down after the second list request")
    .unwrap();

    assert_eq!(exit, WorkerShardLifecycleExit::Shutdown);
    assert!(
        worker_shard_list_request_count(&requests) > 1,
        "periodic lifecycle resync should keep running until shutdown"
    );
}

#[tokio::test]
async fn worker_shard_lifecycle_due_periodic_resync_is_not_starved_by_ready_events() {
    let lease = FakeLeaseClient::default();
    let runtime = WorkerShardOperatorRuntime::with_authority(
        lease,
        FakeEpochStore::default(),
        FakeCommandExecutor::default(),
        authority(),
    );
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let shutdown_tx = Arc::new(Mutex::new(Some(shutdown_tx)));
    let (client, requests) = fake_worker_shard_startup_resync_client_with_list_observer(
        vec![list_page(Vec::new(), None)],
        {
            let shutdown_tx = Arc::clone(&shutdown_tx);
            move |list_requests| {
                if list_requests >= 2 {
                    if let Some(shutdown_tx) = shutdown_tx.lock().unwrap().take() {
                        let _ = shutdown_tx.send(());
                    }
                }
            }
        },
    );

    let exit = tokio::time::timeout(
        Duration::from_secs(1),
        run_worker_shard_lifecycle_with_operator_event_stream(
            client,
            "default",
            &runtime,
            |_| input(None),
            WorkerShardLifecycleOptions {
                initial_resync: WorkerShardResyncOptions::default(),
                periodic_resync: Some(WorkerShardPeriodicResyncSchedule {
                    interval: Duration::from_millis(1),
                    resync: WorkerShardResyncOptions::default(),
                }),
            },
            stream::repeat_with(|| Ok(WorkerShardEvent::Deleted(shard()))),
            async {
                let _ = shutdown_rx.await;
            },
        ),
    )
    .await
    .expect("due periodic resync should not be starved by ready events")
    .unwrap();

    assert_eq!(exit, WorkerShardLifecycleExit::Shutdown);
    assert!(
        worker_shard_list_request_count(&requests) > 1,
        "periodic lifecycle resync should run even while watch events are always ready"
    );
}

#[tokio::test]
async fn worker_shard_lifecycle_drains_in_flight_event_reconcile_before_shutdown() {
    let lease = FakeLeaseClient::default()
        .with_current(None)
        .with_acquired(grant("worker-a", 1));
    let epoch_store = FakeEpochStore::default();
    let (started_tx, started_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let executor = DrainingScopedCommandExecutor::new(started_tx, release_rx);
    let runtime = WorkerShardOperatorRuntime::with_authority(
        lease,
        epoch_store,
        executor.clone(),
        authority(),
    );
    let (client, requests) =
        fake_worker_shard_startup_resync_client(vec![list_page(Vec::new(), None)]);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let lifecycle = run_worker_shard_lifecycle_with_operator_event_stream(
        client,
        "default",
        &runtime,
        |_| input(None),
        WorkerShardLifecycleOptions {
            initial_resync: WorkerShardResyncOptions::default(),
            periodic_resync: None,
        },
        stream::iter(vec![Ok(WorkerShardEvent::Applied(shard()))]),
        async {
            let _ = shutdown_rx.await;
        },
    );
    tokio::pin!(lifecycle);

    tokio::select! {
        result = &mut lifecycle => {
            panic!("lifecycle completed before the start command blocked: {result:?}");
        }
        started = started_rx => {
            started.expect("start command should begin");
        }
    }
    shutdown_tx.send(()).unwrap();
    assert!(
        lifecycle.as_mut().now_or_never().is_none(),
        "shutdown must not complete lifecycle while event reconcile is draining"
    );

    release_tx.send(()).unwrap();
    let exit = lifecycle.await.unwrap();

    assert_eq!(exit, WorkerShardLifecycleExit::Shutdown);
    assert!(executor.completed());
    assert_eq!(
        executor.actions(),
        vec![ExecutedWorkerCommand::Start {
            owner_id: "worker-a".to_string(),
            owner_epoch: 1,
        }]
    );
    assert_eq!(worker_shard_list_request_count(&requests), 1);
}

#[tokio::test]
async fn worker_shard_lifecycle_authority_mismatch_fails_before_pod_create() {
    let validated_authority = validate_operator_authority(
        ObjectStoreAuthorityRef::default(),
        Arc::new(InMemory::new()),
        "worker-shard-lifecycle-authority-mismatch",
        "v1/probes/worker-shard-lifecycle-authority-mismatch",
    )
    .await
    .unwrap();
    let components =
        OperatorAuthorityStartupComponents::from_validated_authority(validated_authority);
    let (client, requests) =
        fake_worker_shard_startup_resync_client(vec![list_page(vec![shard()], None)]);
    let runtime = build_kubernetes_worker_shard_operator_runtime(
        client.clone(),
        "default",
        &components,
        WorkerShardPodTemplate::new("ghcr.io/velorix/velorix-worker:1.0.0").unwrap(),
    )
    .unwrap();

    let error = match run_worker_shard_lifecycle_with_operator_event_stream(
        client,
        "default",
        &runtime,
        |_| input(None),
        WorkerShardLifecycleOptions {
            initial_resync: WorkerShardResyncOptions::default(),
            periodic_resync: None,
        },
        stream::iter(Vec::new()),
        std::future::pending::<()>(),
    )
    .await
    {
        Ok(_) => panic!("expected lifecycle authority mismatch"),
        Err(error) => error,
    };

    match error {
        WorkerShardError::AuthorityMismatch { actual, expected } => {
            assert_eq!(actual, authority());
            assert_eq!(expected, ObjectStoreAuthorityRef::default());
        }
        other => panic!("expected authority mismatch, got {other:?}"),
    }
    let requests = requests.lock().unwrap();
    assert!(requests
        .iter()
        .all(|request| !(request.method == Method::POST
            && request.path == "/api/v1/namespaces/default/pods")));
}

#[test]
fn worker_shard_scoped_pod_name_separates_same_owner_epoch_across_shards() {
    let first = runtime_identity_from_worker_shard(&shard(), "worker-a", 1).unwrap();
    let second = runtime_identity_from_worker_shard(
        &shard_with_stream_partition("orders-west", 1),
        "worker-a",
        1,
    )
    .unwrap();

    assert_ne!(
        worker_shard_pod_name_for_identity(&first),
        worker_shard_pod_name_for_identity(&second)
    );
}

#[tokio::test]
async fn scoped_kubernetes_pod_executor_rejects_conflicting_existing_pod_identity() {
    let template = WorkerShardPodTemplate::new("ghcr.io/velorix/velorix-worker:1.0.0").unwrap();
    let requested = runtime_identity_from_worker_shard(&shard(), "worker-a", 1).unwrap();
    let conflicting = runtime_identity_from_worker_shard(
        &shard_with_stream_partition("orders-west", 1),
        "worker-a",
        1,
    )
    .unwrap();
    let mut existing_pod = template.pod_for_identity(&conflicting);
    existing_pod.metadata.name = Some(worker_shard_pod_name_for_identity(&requested));
    let (client, _requests) = fake_pod_create_conflict_then_get_client(existing_pod);
    let executor = KubernetesPodWorkerShardScopedCommandExecutor::new(client, "default", template);

    let error = executor.start_worker(&requested).await.unwrap_err();

    assert!(error.message().contains("identity mismatch"));
}

#[tokio::test]
async fn kubernetes_pod_worker_shard_executor_deletes_stop_pod_by_owner_epoch_name() {
    let (client, requests) = fake_pod_client(StatusCode::OK);
    let executor = KubernetesPodWorkerShardCommandExecutor::new(
        client,
        "workers",
        WorkerShardPodTemplate::new("ghcr.io/velorix/velorix-worker:1.0.0").unwrap(),
    );

    executor.stop_worker("Worker_A/West", 42).await.unwrap();

    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, Method::DELETE);
    assert_eq!(
        requests[0].path,
        format!(
            "/api/v1/namespaces/workers/pods/{}",
            worker_shard_pod_name("Worker_A/West", 42)
        )
    );
    assert_eq!(requests[0].body["gracePeriodSeconds"], 0);
}

#[tokio::test]
async fn kubernetes_pod_worker_shard_executor_treats_duplicate_start_as_idempotent() {
    let (client, _requests) = fake_pod_client(StatusCode::CONFLICT);
    let executor = KubernetesPodWorkerShardCommandExecutor::new(
        client,
        "workers",
        WorkerShardPodTemplate::new("ghcr.io/velorix/velorix-worker:1.0.0").unwrap(),
    );

    executor.start_worker("worker-a", 1).await.unwrap();
}

#[tokio::test]
async fn kubernetes_pod_worker_shard_executor_treats_missing_stop_as_idempotent() {
    let (client, _requests) = fake_pod_client(StatusCode::NOT_FOUND);
    let executor = KubernetesPodWorkerShardCommandExecutor::new(
        client,
        "workers",
        WorkerShardPodTemplate::new("ghcr.io/velorix/velorix-worker:1.0.0").unwrap(),
    );

    executor.stop_worker("worker-a", 1).await.unwrap();
}

#[tokio::test]
async fn kubernetes_pod_worker_shard_executor_maps_api_failure_to_typed_error() {
    let (client, _requests) = fake_pod_client(StatusCode::INTERNAL_SERVER_ERROR);
    let executor = KubernetesPodWorkerShardCommandExecutor::new(
        client,
        "workers",
        WorkerShardPodTemplate::new("ghcr.io/velorix/velorix-worker:1.0.0").unwrap(),
    );

    let error = executor.start_worker("worker-a", 1).await.unwrap_err();

    assert!(error
        .message()
        .contains("kubernetes pod worker start failed"));
    assert!(error.message().contains("worker-a"));
    assert!(error.message().contains("epoch 1"));
}

#[tokio::test]
async fn scoped_kubernetes_pod_executor_stops_stale_pod_before_replacement_start() {
    let lease = FakeLeaseClient::default().with_current(Some(grant("worker-a", 6)));
    let epoch_store = FakeEpochStore::default().with_record(epoch_record("worker-a", 6));
    let (client, requests) = fake_pod_client(StatusCode::CREATED);
    let executor = KubernetesPodWorkerShardScopedCommandExecutor::new(
        client,
        "workers",
        WorkerShardPodTemplate::new("ghcr.io/velorix/velorix-worker:1.0.0").unwrap(),
    );

    let output = handle_worker_shard_event_with_scoped_command_executor_and_authority(
        WorkerShardEvent::Applied(shard()),
        &lease,
        &epoch_store,
        input(Some(WorkerFact {
            owner_id: "worker-a".to_string(),
            owner_epoch: 5,
        })),
        &executor,
        Some(&authority()),
    )
    .await
    .unwrap()
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
    let old_identity = runtime_identity_from_worker_shard(&shard(), "worker-a", 5).unwrap();
    let new_identity = runtime_identity_from_worker_shard(&shard(), "worker-a", 6).unwrap();
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].method, Method::DELETE);
    assert_eq!(
        requests[0].path,
        format!(
            "/api/v1/namespaces/workers/pods/{}",
            worker_shard_pod_name_for_identity(&old_identity)
        )
    );
    assert_eq!(
        requests[0].body,
        json!({
            "gracePeriodSeconds": 0
        })
    );
    assert_eq!(requests[1].method, Method::POST);
    assert_eq!(requests[1].path, "/api/v1/namespaces/workers/pods");
    assert_eq!(
        requests[1].body["metadata"]["name"],
        worker_shard_pod_name_for_identity(&new_identity)
    );
}

#[tokio::test]
async fn scoped_worker_shard_leader_handoff_stops_old_owner_and_starts_new_owner() {
    let lease = FakeLeaseClient::default().with_acquired(grant("worker-b", 2));
    let epoch_store = FakeEpochStore::default();
    let executor = FakeCommandExecutor::default();
    let shard = shard_with_desired_owner("worker-b");

    let output = handle_worker_shard_event_with_scoped_command_executor_and_authority(
        WorkerShardEvent::Applied(shard.clone()),
        &lease,
        &epoch_store,
        input(Some(WorkerFact {
            owner_id: "worker-a".to_string(),
            owner_epoch: 1,
        })),
        &executor,
        Some(&authority()),
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(
        output.commands,
        vec![
            WorkerShardCommand::StopWorker {
                owner_id: "worker-a".to_string(),
                owner_epoch: 1,
            },
            WorkerShardCommand::AcquireLease {
                owner_id: "worker-b".to_string(),
            },
            WorkerShardCommand::PersistEpochRecord {
                owner_id: "worker-b".to_string(),
                owner_epoch: 2,
            },
            WorkerShardCommand::StartWorker {
                owner_id: "worker-b".to_string(),
                owner_epoch: 2,
            },
        ]
    );
    assert_eq!(
        executor.actions(),
        vec![
            ExecutedWorkerCommand::Stop {
                owner_id: "worker-a".to_string(),
                owner_epoch: 1,
            },
            ExecutedWorkerCommand::Start {
                owner_id: "worker-b".to_string(),
                owner_epoch: 2,
            },
        ]
    );
    assert_eq!(
        epoch_store
            .read(&shard.spec.stream_id, shard.spec.partition_id, 2)
            .await
            .unwrap(),
        Some(epoch_record("worker-b", 2))
    );
}

#[tokio::test]
async fn scoped_kubernetes_pod_executor_stops_running_pod_on_lease_loss_without_replacement_start()
{
    let lease = FakeLeaseClient::default().with_current(None);
    lease.fail_acquire();
    let (client, requests) = fake_pod_client(StatusCode::OK);
    let executor = KubernetesPodWorkerShardScopedCommandExecutor::new(
        client,
        "workers",
        WorkerShardPodTemplate::new("ghcr.io/velorix/velorix-worker:1.0.0").unwrap(),
    );

    let error = handle_worker_shard_event_with_scoped_command_executor_and_authority(
        WorkerShardEvent::Applied(shard()),
        &lease,
        &FakeEpochStore::default(),
        input(Some(WorkerFact {
            owner_id: "worker-a".to_string(),
            owner_epoch: 9,
        })),
        &executor,
        Some(&authority()),
    )
    .await
    .unwrap_err();

    match error {
        WorkerShardError::Authority { message } => {
            assert!(message.contains("held"));
        }
        other => panic!("expected runtime authority error, got {other:?}"),
    }
    let identity = runtime_identity_from_worker_shard(&shard(), "worker-a", 9).unwrap();
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, Method::DELETE);
    assert_eq!(
        requests[0].path,
        format!(
            "/api/v1/namespaces/workers/pods/{}",
            worker_shard_pod_name_for_identity(&identity)
        )
    );
}

#[tokio::test]
async fn deleted_worker_shard_event_stops_running_worker() {
    let output = handle_worker_shard_event(
        WorkerShardEvent::Deleted(shard_with_owner_status("worker-a", 1)),
        &PanicLeaseClient,
        &PanicEpochStore,
        input(Some(WorkerFact {
            owner_id: "worker-a".to_string(),
            owner_epoch: 1,
        })),
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(
        output.commands,
        vec![WorkerShardCommand::StopWorker {
            owner_id: "worker-a".to_string(),
            owner_epoch: 1,
        }]
    );
    assert_eq!(
        output.plan.actions,
        vec![ReconcileAction::StopWorker {
            owner_id: "worker-a".to_string(),
            owner_epoch: 1,
        }]
    );
}

#[tokio::test]
async fn deleted_worker_shard_event_without_running_worker_emits_no_output() {
    let output = handle_worker_shard_event(
        WorkerShardEvent::Deleted(shard()),
        &PanicLeaseClient,
        &PanicEpochStore,
        input(None),
    )
    .await
    .unwrap();

    assert_eq!(output, None);
}

#[tokio::test]
async fn deleted_worker_shard_event_without_status_emits_no_output() {
    let output = handle_worker_shard_event(
        WorkerShardEvent::Deleted(shard()),
        &PanicLeaseClient,
        &PanicEpochStore,
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
async fn deleted_worker_shard_event_ignores_stale_status_for_newer_running_worker() {
    let output = handle_worker_shard_event(
        WorkerShardEvent::Deleted(shard_with_owner_status("worker-a", 1)),
        &PanicLeaseClient,
        &PanicEpochStore,
        input(Some(WorkerFact {
            owner_id: "worker-a".to_string(),
            owner_epoch: 2,
        })),
    )
    .await
    .unwrap();

    assert_eq!(output, None);
}

#[tokio::test]
async fn deleted_worker_shard_event_sends_stop_output_to_sink() {
    let lease = FakeLeaseClient::default().with_current(Some(grant("worker-a", 1)));
    let epoch_store = FakeEpochStore::default().with_record(epoch_record("worker-a", 1));
    let emitted = Arc::new(Mutex::new(Vec::<Vec<WorkerShardCommand>>::new()));

    let output = handle_worker_shard_event_with_output_sink(
        WorkerShardEvent::Deleted(shard_with_owner_status("worker-a", 1)),
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
    .unwrap()
    .unwrap();

    let expected = vec![WorkerShardCommand::StopWorker {
        owner_id: "worker-a".to_string(),
        owner_epoch: 1,
    }];
    assert_eq!(output.commands, expected);
    assert_eq!(*emitted.lock().unwrap(), vec![expected]);
}

#[tokio::test]
async fn deleted_worker_shard_event_with_generic_executor_does_not_execute_unscoped_stop() {
    let lease = FakeLeaseClient::default().with_current(Some(grant("worker-a", 1)));
    let epoch_store = FakeEpochStore::default().with_record(epoch_record("worker-a", 1));
    let executor = FakeCommandExecutor::default();

    let output = handle_worker_shard_event_with_command_executor(
        WorkerShardEvent::Deleted(shard_with_owner_status("worker-a", 1)),
        &lease,
        &epoch_store,
        input(Some(WorkerFact {
            owner_id: "worker-a".to_string(),
            owner_epoch: 1,
        })),
        &executor,
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(
        output.commands,
        vec![WorkerShardCommand::StopWorker {
            owner_id: "worker-a".to_string(),
            owner_epoch: 1,
        }]
    );
    assert!(executor.actions().is_empty());
}

#[tokio::test]
async fn scoped_deleted_worker_shard_event_executes_scoped_stop_when_status_matches() {
    let executor = FakeCommandExecutor::default();

    let output = handle_worker_shard_event_with_scoped_command_executor_and_authority(
        WorkerShardEvent::Deleted(shard_with_owner_status("worker-a", 1)),
        &PanicLeaseClient,
        &PanicEpochStore,
        input(Some(WorkerFact {
            owner_id: "worker-a".to_string(),
            owner_epoch: 1,
        })),
        &executor,
        Some(&authority()),
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(
        output.commands,
        vec![WorkerShardCommand::StopWorker {
            owner_id: "worker-a".to_string(),
            owner_epoch: 1,
        }]
    );
    assert_eq!(
        executor.actions(),
        vec![ExecutedWorkerCommand::Stop {
            owner_id: "worker-a".to_string(),
            owner_epoch: 1,
        }]
    );
}

#[tokio::test]
async fn scoped_deleted_worker_shard_event_validates_authority_before_stop_with_worker() {
    let executor = FakeCommandExecutor::default();

    let error = handle_worker_shard_event_with_scoped_command_executor_and_authority(
        WorkerShardEvent::Deleted(shard()),
        &PanicLeaseClient,
        &PanicEpochStore,
        input(Some(WorkerFact {
            owner_id: "worker-a".to_string(),
            owner_epoch: 1,
        })),
        &executor,
        Some(&ObjectStoreAuthorityRef::default()),
    )
    .await
    .unwrap_err();

    match error {
        WorkerShardError::AuthorityMismatch { actual, expected } => {
            assert_eq!(actual, authority());
            assert_eq!(expected, ObjectStoreAuthorityRef::default());
        }
        other => panic!("expected authority mismatch, got {other:?}"),
    }
    assert!(executor.actions().is_empty());
}

#[tokio::test]
async fn scoped_deleted_worker_shard_event_validates_authority_without_worker() {
    let executor = FakeCommandExecutor::default();

    let error = handle_worker_shard_event_with_scoped_command_executor_and_authority(
        WorkerShardEvent::Deleted(shard()),
        &PanicLeaseClient,
        &PanicEpochStore,
        input(None),
        &executor,
        Some(&ObjectStoreAuthorityRef::default()),
    )
    .await
    .unwrap_err();

    match error {
        WorkerShardError::AuthorityMismatch { actual, expected } => {
            assert_eq!(actual, authority());
            assert_eq!(expected, ObjectStoreAuthorityRef::default());
        }
        other => panic!("expected authority mismatch, got {other:?}"),
    }
    assert!(executor.actions().is_empty());
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

#[tokio::test]
async fn worker_shard_command_executor_stops_stale_worker_before_starting_replacement() {
    let executor = FakeCommandExecutor::default();

    execute_worker_shard_commands(
        &output_with_commands(vec![
            WorkerShardCommand::StopWorker {
                owner_id: "worker-a".to_string(),
                owner_epoch: 5,
            },
            WorkerShardCommand::StartWorker {
                owner_id: "worker-a".to_string(),
                owner_epoch: 6,
            },
        ]),
        &executor,
    )
    .await
    .unwrap();

    assert_eq!(
        executor.actions(),
        vec![
            ExecutedWorkerCommand::Stop {
                owner_id: "worker-a".to_string(),
                owner_epoch: 5,
            },
            ExecutedWorkerCommand::Start {
                owner_id: "worker-a".to_string(),
                owner_epoch: 6,
            },
        ]
    );
}

#[tokio::test]
async fn worker_shard_command_executor_ignores_acquire_and_persist_only_output() {
    let executor = FakeCommandExecutor::default();

    execute_worker_shard_commands(
        &output_with_commands(vec![
            WorkerShardCommand::AcquireLease {
                owner_id: "worker-a".to_string(),
            },
            WorkerShardCommand::PersistEpochRecord {
                owner_id: "worker-a".to_string(),
                owner_epoch: 7,
            },
        ]),
        &executor,
    )
    .await
    .unwrap();

    assert!(executor.actions().is_empty());
}

#[tokio::test]
async fn worker_shard_command_executor_returns_typed_failure_without_later_execution() {
    let executor = FakeCommandExecutor::default().fail_start("start failed");

    let err = execute_worker_shard_commands(
        &output_with_commands(vec![
            WorkerShardCommand::StartWorker {
                owner_id: "worker-a".to_string(),
                owner_epoch: 6,
            },
            WorkerShardCommand::StopWorker {
                owner_id: "worker-b".to_string(),
                owner_epoch: 4,
            },
        ]),
        &executor,
    )
    .await
    .unwrap_err();

    match err {
        WorkerShardError::CommandExecutor { message } => {
            assert_eq!(message, "start failed");
        }
        other => panic!("expected command executor error, got {other:?}"),
    }
    assert_eq!(
        executor.actions(),
        vec![ExecutedWorkerCommand::Start {
            owner_id: "worker-a".to_string(),
            owner_epoch: 6,
        }]
    );
}

#[tokio::test]
async fn worker_shard_command_executor_stop_failure_prevents_replacement_start() {
    let executor = FakeCommandExecutor::default().fail_stop("stop failed");

    let err = execute_worker_shard_commands(
        &output_with_commands(vec![
            WorkerShardCommand::StopWorker {
                owner_id: "worker-a".to_string(),
                owner_epoch: 5,
            },
            WorkerShardCommand::StartWorker {
                owner_id: "worker-a".to_string(),
                owner_epoch: 6,
            },
        ]),
        &executor,
    )
    .await
    .unwrap_err();

    match err {
        WorkerShardError::CommandExecutor { message } => {
            assert_eq!(message, "stop failed");
        }
        other => panic!("expected command executor error, got {other:?}"),
    }
    assert_eq!(
        executor.actions(),
        vec![ExecutedWorkerCommand::Stop {
            owner_id: "worker-a".to_string(),
            owner_epoch: 5,
        }]
    );
}

#[tokio::test]
async fn process_worker_shard_command_executor_runs_start_and_stop_with_owner_context() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("worker-actions.log");
    let command = shell_log_command(&log_path);
    let executor = ProcessWorkerShardCommandExecutor::new(command.clone(), command);

    execute_worker_shard_commands(
        &output_with_commands(vec![
            WorkerShardCommand::StopWorker {
                owner_id: "worker-a".to_string(),
                owner_epoch: 5,
            },
            WorkerShardCommand::StartWorker {
                owner_id: "worker-b".to_string(),
                owner_epoch: 6,
            },
        ]),
        &executor,
    )
    .await
    .unwrap();

    assert_eq!(
        fs::read_to_string(log_path).unwrap(),
        "stop worker-a 5\nstart worker-b 6\n"
    );
}

#[tokio::test]
async fn process_worker_shard_command_executor_rejects_empty_program() {
    let error = WorkerShardProcessCommand::new(" ", std::iter::empty::<String>()).unwrap_err();

    assert!(error.message().contains("program must not be empty"));
}

#[tokio::test]
async fn process_worker_shard_command_executor_rejects_empty_argv_config() {
    let error = WorkerShardProcessCommand::from_argv(std::iter::empty::<String>()).unwrap_err();

    assert!(error.message().contains("argv must not be empty"));
}

#[tokio::test]
async fn process_worker_shard_command_executor_returns_typed_error_on_nonzero_exit() {
    let start = WorkerShardProcessCommand::from_argv(["/bin/sh", "-c", "exit 7"]).unwrap();
    let stop = shell_log_command(&tempfile::tempdir().unwrap().path().join("unused.log"));
    let executor = ProcessWorkerShardCommandExecutor::new(start, stop);

    let error = execute_worker_shard_commands(
        &output_with_commands(vec![WorkerShardCommand::StartWorker {
            owner_id: "worker-a".to_string(),
            owner_epoch: 6,
        }]),
        &executor,
    )
    .await
    .unwrap_err();

    match error {
        WorkerShardError::CommandExecutor { message } => {
            assert!(message.contains("worker start command"));
            assert!(message.contains("exited with"));
        }
        other => panic!("expected command executor error, got {other:?}"),
    }
}

#[test]
fn worker_shard_pod_name_is_stable_dns_label_for_unsafe_owner_id() {
    let name = worker_shard_pod_name("Worker_A/West.Zone@VeryLongNameWithUnsafeCharacters", 42);

    assert_eq!(
        name,
        worker_shard_pod_name("Worker_A/West.Zone@VeryLongNameWithUnsafeCharacters", 42)
    );
    assert!(name.len() <= 63);
    assert!(name.starts_with("velorix-worker-worker-a-west"));
    assert!(name.ends_with("-42"));
    assert!(name
        .bytes()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-'));
    assert!(!name.ends_with('-'));
}

#[test]
fn worker_shard_pod_template_builds_start_pod_with_owner_context() {
    let template = WorkerShardPodTemplate::new("ghcr.io/velorix/velorix-worker:1.0.0")
        .unwrap()
        .with_command(["velorix-worker"])
        .with_args(["serve"])
        .with_label("control.velorix.io/view-id", "balances-by-account")
        .with_service_account_name("velorix-worker");

    let pod = template.pod_for_owner("Worker_A/West", 42);
    let labels = pod.metadata.labels.unwrap();
    let spec = pod.spec.unwrap();
    let container = spec.containers.into_iter().next().unwrap();

    assert_eq!(
        pod.metadata.name.as_deref(),
        Some(worker_shard_pod_name("Worker_A/West", 42).as_str())
    );
    assert_eq!(labels["app.kubernetes.io/name"], "velorix-worker");
    assert_eq!(labels["app.kubernetes.io/component"], "worker-shard");
    assert_eq!(labels["control.velorix.io/owner-id"], "worker-a-west");
    assert_eq!(labels["control.velorix.io/owner-epoch"], "42");
    assert_eq!(labels["control.velorix.io/view-id"], "balances-by-account");
    assert_eq!(spec.restart_policy.as_deref(), Some("Never"));
    assert_eq!(spec.service_account_name.as_deref(), Some("velorix-worker"));
    assert_eq!(container.name, "velorix-worker");
    assert_eq!(
        container.image.as_deref(),
        Some("ghcr.io/velorix/velorix-worker:1.0.0")
    );
    assert_eq!(
        container.command.as_deref(),
        Some(["velorix-worker".to_string()].as_slice())
    );
    assert_eq!(
        container.args.as_deref(),
        Some(["serve".to_string()].as_slice())
    );
    assert_eq!(
        container.env.unwrap(),
        vec![
            env_var("VELORIX_WORKER_OWNER_ID", "Worker_A/West"),
            env_var("VELORIX_WORKER_OWNER_EPOCH", "42"),
        ]
    );
}

#[test]
fn worker_shard_pod_template_rejects_empty_image() {
    let error = WorkerShardPodTemplate::new(" ").unwrap_err();

    assert!(error
        .message()
        .contains("worker pod image must not be empty"));
}

fn has_start(output: &WorkerShardReconcileOutput) -> bool {
    output
        .commands
        .iter()
        .any(|command| matches!(command, WorkerShardCommand::StartWorker { .. }))
}

fn output_with_commands(commands: Vec<WorkerShardCommand>) -> WorkerShardReconcileOutput {
    WorkerShardReconcileOutput {
        plan: ReconcilePlan::default(),
        facts: ObservedControlPlaneFacts::default(),
        commands,
        command_error: None,
    }
}

fn shell_log_command(log_path: &std::path::Path) -> WorkerShardProcessCommand {
    WorkerShardProcessCommand::new(
        "/bin/sh",
        [
            "-c",
            "printf '%s %s %s\n' \"$VELORIX_WORKER_ACTION\" \"$VELORIX_WORKER_OWNER_ID\" \"$VELORIX_WORKER_OWNER_EPOCH\" >> \"$1\"",
            "worker-shard-test",
            log_path.to_str().unwrap(),
        ],
    )
    .unwrap()
}

fn env_var(name: &str, value: &str) -> k8s_openapi::api::core::v1::EnvVar {
    k8s_openapi::api::core::v1::EnvVar {
        name: name.to_string(),
        value: Some(value.to_string()),
        value_from: None,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RecordedKubeRequest {
    method: Method,
    path: String,
    body: Value,
}

fn worker_shard_list_request_count(requests: &Arc<Mutex<Vec<RecordedKubeRequest>>>) -> usize {
    requests
        .lock()
        .unwrap()
        .iter()
        .filter(|request| {
            request.method == Method::GET
                && request
                    .path
                    .ends_with("/namespaces/default/velorixworkershards")
        })
        .count()
}

fn fake_pod_client(
    response_status: StatusCode,
) -> (kube::Client, Arc<Mutex<Vec<RecordedKubeRequest>>>) {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let service = tower::service_fn({
        let requests = Arc::clone(&requests);
        move |request: Request<Body>| {
            let requests = Arc::clone(&requests);
            async move {
                let method = request.method().clone();
                let path = request.uri().path().to_string();
                let body_bytes = request.into_body().collect_bytes().await.unwrap();
                let body: Value = if body_bytes.is_empty() {
                    Value::Null
                } else {
                    serde_json::from_slice(&body_bytes).unwrap()
                };
                requests.lock().unwrap().push(RecordedKubeRequest {
                    method: method.clone(),
                    path,
                    body: body.clone(),
                });

                let response_body = if response_status.is_success() {
                    if method == Method::DELETE {
                        json!({
                            "apiVersion": "v1",
                            "kind": "Status",
                            "metadata": {},
                            "status": "Success",
                            "code": response_status.as_u16()
                        })
                    } else {
                        body
                    }
                } else {
                    json!({
                        "apiVersion": "v1",
                        "kind": "Status",
                        "metadata": {},
                        "status": "Failure",
                        "message": "fake kubernetes error",
                        "reason": match response_status {
                            StatusCode::NOT_FOUND => "NotFound",
                            StatusCode::CONFLICT => "AlreadyExists",
                            _ => "InternalError",
                        },
                        "code": response_status.as_u16()
                    })
                };

                Ok::<_, Infallible>(
                    Response::builder()
                        .status(response_status)
                        .header("content-type", "application/json")
                        .body(Body::from(serde_json::to_vec(&response_body).unwrap()))
                        .unwrap(),
                )
            }
        }
    });

    (ClientBuilder::new(service, "default").build(), requests)
}

fn fake_pod_create_conflict_then_get_client(
    existing_pod: k8s_openapi::api::core::v1::Pod,
) -> (kube::Client, Arc<Mutex<Vec<RecordedKubeRequest>>>) {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let existing_pod = Arc::new(existing_pod);
    let service = tower::service_fn({
        let requests = Arc::clone(&requests);
        let existing_pod = Arc::clone(&existing_pod);
        move |request: Request<Body>| {
            let requests = Arc::clone(&requests);
            let existing_pod = Arc::clone(&existing_pod);
            async move {
                let method = request.method().clone();
                let path = request.uri().path().to_string();
                let body_bytes = request.into_body().collect_bytes().await.unwrap();
                let body: Value = if body_bytes.is_empty() {
                    Value::Null
                } else {
                    serde_json::from_slice(&body_bytes).unwrap()
                };
                requests.lock().unwrap().push(RecordedKubeRequest {
                    method: method.clone(),
                    path: path.clone(),
                    body,
                });

                let (status, response_body) = match method {
                    Method::POST => (
                        StatusCode::CONFLICT,
                        json!({
                            "apiVersion": "v1",
                            "kind": "Status",
                            "metadata": {},
                            "status": "Failure",
                            "message": "already exists",
                            "reason": "AlreadyExists",
                            "code": 409
                        }),
                    ),
                    Method::GET => (
                        StatusCode::OK,
                        serde_json::to_value(&*existing_pod).unwrap(),
                    ),
                    _ => (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        json!({
                            "apiVersion": "v1",
                            "kind": "Status",
                            "metadata": {},
                            "status": "Failure",
                            "message": "unexpected fake kubernetes request",
                            "reason": "InternalError",
                            "code": 500
                        }),
                    ),
                };

                Ok::<_, Infallible>(
                    Response::builder()
                        .status(status)
                        .header("content-type", "application/json")
                        .body(Body::from(serde_json::to_vec(&response_body).unwrap()))
                        .unwrap(),
                )
            }
        }
    });

    (ClientBuilder::new(service, "default").build(), requests)
}

fn fake_worker_shard_runtime_client() -> (kube::Client, Arc<Mutex<Vec<RecordedKubeRequest>>>) {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let service = tower::service_fn({
        let requests = Arc::clone(&requests);
        move |request: Request<Body>| {
            let requests = Arc::clone(&requests);
            async move {
                let method = request.method().clone();
                let path = request.uri().path().to_string();
                let body_bytes = request.into_body().collect_bytes().await.unwrap();
                let body: Value = if body_bytes.is_empty() {
                    Value::Null
                } else {
                    serde_json::from_slice(&body_bytes).unwrap()
                };
                requests.lock().unwrap().push(RecordedKubeRequest {
                    method: method.clone(),
                    path: path.clone(),
                    body: body.clone(),
                });

                let (status, response_body) = match (method, path.as_str()) {
                    (Method::GET, path)
                        if path.starts_with(
                            "/apis/coordination.k8s.io/v1/namespaces/default/leases/",
                        ) =>
                    {
                        (
                            StatusCode::NOT_FOUND,
                            json!({
                                "apiVersion": "v1",
                                "kind": "Status",
                                "metadata": {},
                                "status": "Failure",
                                "message": "fake lease not found",
                                "reason": "NotFound",
                                "code": 404
                            }),
                        )
                    }
                    (Method::POST, "/apis/coordination.k8s.io/v1/namespaces/default/leases") => {
                        (StatusCode::CREATED, body)
                    }
                    (Method::POST, "/api/v1/namespaces/default/pods") => {
                        (StatusCode::CREATED, body)
                    }
                    (Method::DELETE, path)
                        if path.starts_with("/api/v1/namespaces/default/pods/") =>
                    {
                        (
                            StatusCode::OK,
                            json!({
                                "apiVersion": "v1",
                                "kind": "Status",
                                "metadata": {},
                                "status": "Success",
                                "code": 200
                            }),
                        )
                    }
                    _ => (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        json!({
                            "apiVersion": "v1",
                            "kind": "Status",
                            "metadata": {},
                            "status": "Failure",
                            "message": "unexpected fake kubernetes request",
                            "reason": "InternalError",
                            "code": 500
                        }),
                    ),
                };

                Ok::<_, Infallible>(
                    Response::builder()
                        .status(status)
                        .header("content-type", "application/json")
                        .body(Body::from(serde_json::to_vec(&response_body).unwrap()))
                        .unwrap(),
                )
            }
        }
    });

    (ClientBuilder::new(service, "default").build(), requests)
}

fn fake_worker_shard_startup_resync_client(
    pages: Vec<WorkerShardListPage>,
) -> (kube::Client, Arc<Mutex<Vec<RecordedKubeRequest>>>) {
    fake_worker_shard_startup_resync_client_with_list_observer(pages, |_| {})
}

fn fake_worker_shard_startup_resync_client_with_list_observer(
    pages: Vec<WorkerShardListPage>,
    on_list_request: impl Fn(usize) + Send + Sync + 'static,
) -> (kube::Client, Arc<Mutex<Vec<RecordedKubeRequest>>>) {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let pages = Arc::new(Mutex::new(pages.into_iter()));
    let list_requests = Arc::new(Mutex::new(0usize));
    let on_list_request = Arc::new(on_list_request);
    let service = tower::service_fn({
        let requests = Arc::clone(&requests);
        let pages = Arc::clone(&pages);
        let list_requests = Arc::clone(&list_requests);
        let on_list_request = Arc::clone(&on_list_request);
        move |request: Request<Body>| {
            let requests = Arc::clone(&requests);
            let pages = Arc::clone(&pages);
            let list_requests = Arc::clone(&list_requests);
            let on_list_request = Arc::clone(&on_list_request);
            async move {
                let method = request.method().clone();
                let path = request.uri().path().to_string();
                let body_bytes = request.into_body().collect_bytes().await.unwrap();
                let body: Value = if body_bytes.is_empty() {
                    Value::Null
                } else {
                    serde_json::from_slice(&body_bytes).unwrap()
                };
                requests.lock().unwrap().push(RecordedKubeRequest {
                    method: method.clone(),
                    path: path.clone(),
                    body: body.clone(),
                });

                let (status, response_body) = match (method, path.as_str()) {
                    (
                        Method::GET,
                        "/apis/control.velorix.io/v1alpha1/namespaces/default/velorixworkershards",
                    ) => {
                        let list_request_count = {
                            let mut list_requests = list_requests.lock().unwrap();
                            *list_requests += 1;
                            *list_requests
                        };
                        on_list_request(list_request_count);
                        let page = pages
                            .lock()
                            .unwrap()
                            .next()
                            .unwrap_or_else(|| list_page(Vec::new(), None));
                        (
                            StatusCode::OK,
                            json!({
                                "apiVersion": "control.velorix.io/v1alpha1",
                                "kind": "VelorixWorkerShardList",
                                "metadata": {
                                    "continue": page.continue_token,
                                },
                                "items": page.items,
                            }),
                        )
                    }
                    (Method::GET, path)
                        if path.starts_with(
                            "/apis/coordination.k8s.io/v1/namespaces/default/leases/",
                        ) =>
                    {
                        (
                            StatusCode::NOT_FOUND,
                            json!({
                                "apiVersion": "v1",
                                "kind": "Status",
                                "metadata": {},
                                "status": "Failure",
                                "message": "fake lease not found",
                                "reason": "NotFound",
                                "code": 404
                            }),
                        )
                    }
                    (Method::POST, "/apis/coordination.k8s.io/v1/namespaces/default/leases") => {
                        (StatusCode::CREATED, body)
                    }
                    (Method::POST, "/api/v1/namespaces/default/pods") => {
                        (StatusCode::CREATED, body)
                    }
                    (Method::DELETE, path)
                        if path.starts_with("/api/v1/namespaces/default/pods/") =>
                    {
                        (
                            StatusCode::OK,
                            json!({
                                "apiVersion": "v1",
                                "kind": "Status",
                                "metadata": {},
                                "status": "Success",
                                "code": 200
                            }),
                        )
                    }
                    _ => (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        json!({
                            "apiVersion": "v1",
                            "kind": "Status",
                            "metadata": {},
                            "status": "Failure",
                            "message": "unexpected fake kubernetes request",
                            "reason": "InternalError",
                            "code": 500
                        }),
                    ),
                };

                Ok::<_, Infallible>(
                    Response::builder()
                        .status(status)
                        .header("content-type", "application/json")
                        .body(Body::from(serde_json::to_vec(&response_body).unwrap()))
                        .unwrap(),
                )
            }
        }
    });

    (ClientBuilder::new(service, "default").build(), requests)
}

#[derive(Clone)]
struct WorkerShardListPage {
    items: Vec<VelorixWorkerShard>,
    continue_token: Option<String>,
}

fn list_page(items: Vec<VelorixWorkerShard>, continue_token: Option<&str>) -> WorkerShardListPage {
    WorkerShardListPage {
        items,
        continue_token: continue_token.map(ToString::to_string),
    }
}

fn fake_worker_shard_list_client(
    pages: Vec<WorkerShardListPage>,
) -> (kube::Client, Arc<Mutex<Vec<RecordedKubeRequest>>>) {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let pages = Arc::new(Mutex::new(pages.into_iter()));
    let service = tower::service_fn({
        let requests = Arc::clone(&requests);
        let pages = Arc::clone(&pages);
        move |request: Request<Body>| {
            let requests = Arc::clone(&requests);
            let pages = Arc::clone(&pages);
            async move {
                let method = request.method().clone();
                let path = request.uri().path().to_string();
                let body_bytes = request.into_body().collect_bytes().await.unwrap();
                let body: Value = if body_bytes.is_empty() {
                    Value::Null
                } else {
                    serde_json::from_slice(&body_bytes).unwrap()
                };
                requests.lock().unwrap().push(RecordedKubeRequest {
                    method: method.clone(),
                    path: path.clone(),
                    body,
                });

                let (status, response_body) = match (method, path.as_str()) {
                    (
                        Method::GET,
                        "/apis/control.velorix.io/v1alpha1/namespaces/default/velorixworkershards",
                    ) => {
                        let page = pages
                            .lock()
                            .unwrap()
                            .next()
                            .unwrap_or_else(|| list_page(Vec::new(), None));
                        (
                            StatusCode::OK,
                            json!({
                                "apiVersion": "control.velorix.io/v1alpha1",
                                "kind": "VelorixWorkerShardList",
                                "metadata": {
                                    "continue": page.continue_token,
                                },
                                "items": page.items,
                            }),
                        )
                    }
                    _ => (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        json!({
                            "apiVersion": "v1",
                            "kind": "Status",
                            "metadata": {},
                            "status": "Failure",
                            "message": "unexpected fake kubernetes request",
                            "reason": "InternalError",
                            "code": 500
                        }),
                    ),
                };

                Ok::<_, Infallible>(
                    Response::builder()
                        .status(status)
                        .header("content-type", "application/json")
                        .body(Body::from(serde_json::to_vec(&response_body).unwrap()))
                        .unwrap(),
                )
            }
        }
    });

    (ClientBuilder::new(service, "default").build(), requests)
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

#[derive(Clone, Debug, Eq, PartialEq)]
enum ExecutedWorkerCommand {
    Stop { owner_id: String, owner_epoch: u64 },
    Start { owner_id: String, owner_epoch: u64 },
}

#[derive(Clone, Default)]
struct FakeCommandExecutor {
    actions: Arc<Mutex<Vec<ExecutedWorkerCommand>>>,
    fail_start: Arc<Mutex<Option<String>>>,
    fail_stop: Arc<Mutex<Option<String>>>,
}

impl FakeCommandExecutor {
    fn fail_start(self, message: &str) -> Self {
        *self.fail_start.lock().unwrap() = Some(message.to_string());
        self
    }

    fn fail_stop(self, message: &str) -> Self {
        *self.fail_stop.lock().unwrap() = Some(message.to_string());
        self
    }

    fn actions(&self) -> Vec<ExecutedWorkerCommand> {
        self.actions.lock().unwrap().clone()
    }
}

#[async_trait]
impl WorkerShardCommandExecutor for FakeCommandExecutor {
    async fn stop_worker(
        &self,
        owner_id: &str,
        owner_epoch: u64,
    ) -> Result<(), WorkerShardCommandExecutorError> {
        self.actions
            .lock()
            .unwrap()
            .push(ExecutedWorkerCommand::Stop {
                owner_id: owner_id.to_string(),
                owner_epoch,
            });
        if let Some(message) = self.fail_stop.lock().unwrap().clone() {
            return Err(WorkerShardCommandExecutorError::new(message));
        }
        Ok(())
    }

    async fn start_worker(
        &self,
        owner_id: &str,
        owner_epoch: u64,
    ) -> Result<(), WorkerShardCommandExecutorError> {
        self.actions
            .lock()
            .unwrap()
            .push(ExecutedWorkerCommand::Start {
                owner_id: owner_id.to_string(),
                owner_epoch,
            });
        if let Some(message) = self.fail_start.lock().unwrap().clone() {
            return Err(WorkerShardCommandExecutorError::new(message));
        }
        Ok(())
    }
}

#[async_trait]
impl WorkerShardScopedCommandExecutor for FakeCommandExecutor {
    async fn stop_worker(
        &self,
        identity: &WorkerShardRuntimeIdentity,
    ) -> Result<(), WorkerShardCommandExecutorError> {
        WorkerShardCommandExecutor::stop_worker(self, &identity.owner_id, identity.owner_epoch)
            .await
    }

    async fn start_worker(
        &self,
        identity: &WorkerShardRuntimeIdentity,
    ) -> Result<(), WorkerShardCommandExecutorError> {
        WorkerShardCommandExecutor::start_worker(self, &identity.owner_id, identity.owner_epoch)
            .await
    }
}

#[derive(Clone)]
struct DrainingScopedCommandExecutor {
    actions: Arc<Mutex<Vec<ExecutedWorkerCommand>>>,
    start_started: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    start_release: Arc<Mutex<Option<oneshot::Receiver<()>>>>,
    completed: Arc<Mutex<bool>>,
}

impl DrainingScopedCommandExecutor {
    fn new(start_started: oneshot::Sender<()>, start_release: oneshot::Receiver<()>) -> Self {
        Self {
            actions: Arc::new(Mutex::new(Vec::new())),
            start_started: Arc::new(Mutex::new(Some(start_started))),
            start_release: Arc::new(Mutex::new(Some(start_release))),
            completed: Arc::new(Mutex::new(false)),
        }
    }

    fn actions(&self) -> Vec<ExecutedWorkerCommand> {
        self.actions.lock().unwrap().clone()
    }

    fn completed(&self) -> bool {
        *self.completed.lock().unwrap()
    }
}

#[async_trait]
impl WorkerShardScopedCommandExecutor for DrainingScopedCommandExecutor {
    async fn stop_worker(
        &self,
        identity: &WorkerShardRuntimeIdentity,
    ) -> Result<(), WorkerShardCommandExecutorError> {
        self.actions
            .lock()
            .unwrap()
            .push(ExecutedWorkerCommand::Stop {
                owner_id: identity.owner_id.clone(),
                owner_epoch: identity.owner_epoch,
            });
        Ok(())
    }

    async fn start_worker(
        &self,
        identity: &WorkerShardRuntimeIdentity,
    ) -> Result<(), WorkerShardCommandExecutorError> {
        self.actions
            .lock()
            .unwrap()
            .push(ExecutedWorkerCommand::Start {
                owner_id: identity.owner_id.clone(),
                owner_epoch: identity.owner_epoch,
            });
        if let Some(start_started) = self.start_started.lock().unwrap().take() {
            let _ = start_started.send(());
        }
        let start_release = self.start_release.lock().unwrap().take();
        if let Some(start_release) = start_release {
            let _ = start_release.await;
        }
        *self.completed.lock().unwrap() = true;
        Ok(())
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
            authority: authority(),
        },
    );
    shard.metadata.namespace = Some("default".to_string());
    shard.metadata.generation = Some(2);
    shard
}

fn shard_with_desired_owner(owner_id: &str) -> VelorixWorkerShard {
    let mut shard = shard();
    shard.spec.worker_id = owner_id.to_string();
    shard.spec.desired_owner_id = owner_id.to_string();
    shard
}

fn shard_with_stream_partition(stream_id: &str, partition_id: u32) -> VelorixWorkerShard {
    let mut shard = VelorixWorkerShard::new(
        &format!("{stream_id}-p{partition_id}"),
        VelorixWorkerShardSpec {
            worker_id: "worker-a".to_string(),
            view_id: "balances_by_account".to_string(),
            stream_id: stream_id.to_string(),
            partition_id,
            desired_owner_id: "worker-a".to_string(),
            authority: authority(),
        },
    );
    shard.metadata.namespace = Some("default".to_string());
    shard.metadata.generation = Some(2);
    shard
}

fn shard_with_owner_status(owner_id: &str, owner_epoch: u64) -> VelorixWorkerShard {
    let mut shard = shard();
    shard.status = Some(WorkerShardStatus {
        observed_generation: Some(2),
        current_owner_epoch: Some(OwnerEpochStatus {
            stream_id: shard.spec.stream_id.clone(),
            partition_id: shard.spec.partition_id,
            owner_id: owner_id.to_string(),
            owner_epoch,
        }),
        readiness: None,
    });
    shard
}

fn authority() -> ObjectStoreAuthorityRef {
    ObjectStoreAuthorityRef {
        store_id: "primary".to_string(),
        namespace: "analytics".to_string(),
    }
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
    epoch_record_for("orders", 0, owner_id, owner_epoch)
}

fn epoch_record_for(
    stream_id: &str,
    partition_id: u32,
    owner_id: &str,
    owner_epoch: u64,
) -> OwnershipEpochRecord {
    let key = PartitionLeaseKey {
        namespace: "default".to_string(),
        view_id: "balances_by_account".to_string(),
        stream_id: stream_id.to_string(),
        partition_id,
    };
    OwnershipEpochRecord {
        stream_id: stream_id.to_string(),
        partition_id,
        owner_id: owner_id.to_string(),
        owner_epoch,
        lease_identity: velorix_k8s::lease::partition_lease_identity(&key),
        created_at: "2026-05-06T00:00:00Z".to_string(),
        previous_epoch: owner_epoch.checked_sub(1),
        previous_checkpoint_version: Some(8),
    }
}

struct PanicLeaseClient;

#[async_trait]
impl PartitionLeaseClient for PanicLeaseClient {
    async fn acquire_or_renew(
        &self,
        _request: LeaseAcquireRequest,
    ) -> Result<PartitionLeaseGrant, LeaseError> {
        panic!("delete handling must not acquire or renew leases")
    }

    async fn current(
        &self,
        _key: &PartitionLeaseKey,
        _now_unix_ms: u64,
    ) -> Result<Option<PartitionLeaseGrant>, LeaseError> {
        panic!("delete handling must not read leases")
    }

    async fn release(
        &self,
        _key: &PartitionLeaseKey,
        _owner_id: &str,
        _owner_epoch: u64,
        _now_unix_ms: u64,
    ) -> Result<(), LeaseError> {
        panic!("delete handling must not release leases")
    }
}

struct PanicEpochStore;

#[async_trait]
impl WorkerShardEpochStore for PanicEpochStore {
    async fn read(
        &self,
        _stream_id: &str,
        _partition_id: u32,
        _owner_epoch: u64,
    ) -> Result<Option<OwnershipEpochRecord>, velorix_k8s::worker_shard::WorkerShardError> {
        panic!("delete handling must not read epoch records")
    }

    async fn create(
        &self,
        _record: OwnershipEpochRecord,
    ) -> Result<(), velorix_k8s::worker_shard::WorkerShardError> {
        panic!("delete handling must not create epoch records")
    }

    async fn has_newer(
        &self,
        _stream_id: &str,
        _partition_id: u32,
        _owner_epoch: u64,
    ) -> Result<bool, velorix_k8s::worker_shard::WorkerShardError> {
        panic!("delete handling must not check newer epoch records")
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
