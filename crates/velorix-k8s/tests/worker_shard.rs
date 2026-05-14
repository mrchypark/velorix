use std::{
    collections::BTreeMap,
    convert::Infallible,
    fs,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use http::{Method, Request, Response, StatusCode};
use kube::{
    client::{Body, ClientBuilder},
    runtime::watcher::Event,
};
use object_store::memory::InMemory;
use serde_json::{json, Value};
use velorix_control::{
    lease::{
        LeaseAcquireRequest, LeaseError, PartitionLeaseClient, PartitionLeaseGrant,
        PartitionLeaseKey,
    },
    reconcile_plan::{ObservedControlPlaneFacts, ReconcileBlockReason, ReconcilePlan, WorkerFact},
};
use velorix_k8s::{
    crd::{
        ObjectStoreAuthorityRef, OwnerEpochStatus, VelorixWorkerShard, VelorixWorkerShardSpec,
        WorkerShardStatus,
    },
    startup::validate_operator_authority,
    worker_shard::{
        execute_worker_shard_commands, handle_worker_shard_event,
        handle_worker_shard_event_with_command_executor,
        handle_worker_shard_event_with_output_sink, reconcile_worker_shard, worker_shard_pod_name,
        worker_shard_watch_event, CheckpointPublisherEpochStore,
        KubernetesPodWorkerShardCommandExecutor, ProcessWorkerShardCommandExecutor,
        WorkerShardCommand, WorkerShardCommandExecutor, WorkerShardCommandExecutorError,
        WorkerShardEpochStore, WorkerShardError, WorkerShardEvent, WorkerShardPodTemplate,
        WorkerShardProcessCommand, WorkerShardReconcileConfig, WorkerShardReconcileInput,
        WorkerShardReconcileOutput,
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
    let epoch_store = CheckpointPublisherEpochStore::for_production(validated_authority).unwrap();
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
        WorkerShardPodTemplate::new("ghcr.io/floci-io/velorix-worker:1.0.0").unwrap(),
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
async fn kubernetes_pod_worker_shard_executor_deletes_stop_pod_by_owner_epoch_name() {
    let (client, requests) = fake_pod_client(StatusCode::OK);
    let executor = KubernetesPodWorkerShardCommandExecutor::new(
        client,
        "workers",
        WorkerShardPodTemplate::new("ghcr.io/floci-io/velorix-worker:1.0.0").unwrap(),
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
        WorkerShardPodTemplate::new("ghcr.io/floci-io/velorix-worker:1.0.0").unwrap(),
    );

    executor.start_worker("worker-a", 1).await.unwrap();
}

#[tokio::test]
async fn kubernetes_pod_worker_shard_executor_treats_missing_stop_as_idempotent() {
    let (client, _requests) = fake_pod_client(StatusCode::NOT_FOUND);
    let executor = KubernetesPodWorkerShardCommandExecutor::new(
        client,
        "workers",
        WorkerShardPodTemplate::new("ghcr.io/floci-io/velorix-worker:1.0.0").unwrap(),
    );

    executor.stop_worker("worker-a", 1).await.unwrap();
}

#[tokio::test]
async fn kubernetes_pod_worker_shard_executor_maps_api_failure_to_typed_error() {
    let (client, _requests) = fake_pod_client(StatusCode::INTERNAL_SERVER_ERROR);
    let executor = KubernetesPodWorkerShardCommandExecutor::new(
        client,
        "workers",
        WorkerShardPodTemplate::new("ghcr.io/floci-io/velorix-worker:1.0.0").unwrap(),
    );

    let error = executor.start_worker("worker-a", 1).await.unwrap_err();

    assert!(error
        .message()
        .contains("kubernetes pod worker start failed"));
    assert!(error.message().contains("worker-a"));
    assert!(error.message().contains("epoch 1"));
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
    let template = WorkerShardPodTemplate::new("ghcr.io/floci-io/velorix-worker:1.0.0")
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
        Some("ghcr.io/floci-io/velorix-worker:1.0.0")
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
