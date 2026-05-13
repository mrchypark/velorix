use async_trait::async_trait;
use futures::StreamExt;
use kube::{
    api::Api,
    runtime::watcher::{self, Event},
    Client, ResourceExt,
};
use thiserror::Error;
use velorix_control::{
    control_plane_contract::{
        ContractMetadata, VelorixView, VelorixViewSpec, VelorixViewStatus, WorkerIntent,
    },
    lease::{
        LeaseAcquireRequest, LeaseError, PartitionLeaseClient, PartitionLeaseGrant,
        PartitionLeaseKey,
    },
    reconcile_plan::{
        plan_reconcile, EpochRecordFact, LeaseFact, ObservedControlPlaneFacts, ReconcileAction,
        ReconcilePlan, WorkerFact,
    },
};
use velorix_storage::{ownership::OwnershipEpochRecord, state::CheckpointPublisher};

use crate::{
    crd::{VelorixWorkerShard, WorkerShardStatus},
    lease::{ownership_epoch_record_from_grant, partition_lease_identity},
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkerShardReconcileInput {
    pub now_unix_ms: u64,
    pub ttl_ms: u64,
    pub running_worker: Option<WorkerFact>,
    pub config: WorkerShardReconcileConfig,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkerShardReconcileConfig {
    pub created_at: String,
    pub previous_checkpoint_version: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkerShardReconcileOutput {
    pub plan: ReconcilePlan,
    pub facts: ObservedControlPlaneFacts,
    pub commands: Vec<WorkerShardCommand>,
    pub command_error: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WorkerShardCommand {
    AcquireLease { owner_id: String },
    PersistEpochRecord { owner_id: String, owner_epoch: u64 },
    StopWorker { owner_id: String, owner_epoch: u64 },
    StartWorker { owner_id: String, owner_epoch: u64 },
}

#[derive(Clone, Debug)]
pub enum WorkerShardEvent {
    Applied(VelorixWorkerShard),
    Deleted(VelorixWorkerShard),
}

#[derive(Debug, Error)]
pub enum WorkerShardError {
    #[error("missing kubernetes worker shard object field `{field}`")]
    MissingObjectField { field: &'static str },
    #[error(transparent)]
    Lease(Box<LeaseError>),
    #[error("ownership epoch record store failed: {0}")]
    EpochStore(String),
    #[error("kubernetes worker shard watcher failed: {message}")]
    Watcher { message: String },
    #[error("worker shard command sink failed: {message}")]
    CommandSink { message: String },
}

impl From<LeaseError> for WorkerShardError {
    fn from(error: LeaseError) -> Self {
        Self::Lease(Box::new(error))
    }
}

#[async_trait]
pub trait WorkerShardEpochStore: Send + Sync {
    async fn read(
        &self,
        stream_id: &str,
        partition_id: u32,
        owner_epoch: u64,
    ) -> Result<Option<OwnershipEpochRecord>, WorkerShardError>;

    async fn create(&self, record: OwnershipEpochRecord) -> Result<(), WorkerShardError>;

    async fn has_newer(
        &self,
        stream_id: &str,
        partition_id: u32,
        owner_epoch: u64,
    ) -> Result<bool, WorkerShardError>;
}

#[derive(Clone)]
pub struct CheckpointPublisherEpochStore {
    publisher: CheckpointPublisher,
}

impl CheckpointPublisherEpochStore {
    pub fn new(publisher: CheckpointPublisher) -> Self {
        Self { publisher }
    }
}

#[async_trait]
impl WorkerShardEpochStore for CheckpointPublisherEpochStore {
    async fn read(
        &self,
        stream_id: &str,
        partition_id: u32,
        owner_epoch: u64,
    ) -> Result<Option<OwnershipEpochRecord>, WorkerShardError> {
        match self
            .publisher
            .read_ownership_epoch_record(stream_id, partition_id, owner_epoch)
            .await
        {
            Ok(record) => Ok(Some(record)),
            Err(velorix_storage::state::CheckpointPublishError::ObjectStore(
                object_store::Error::NotFound { .. },
            )) => Ok(None),
            Err(err) => Err(WorkerShardError::EpochStore(err.to_string())),
        }
    }

    async fn create(&self, record: OwnershipEpochRecord) -> Result<(), WorkerShardError> {
        self.publisher
            .create_ownership_epoch_record(&record)
            .await
            .map(|_| ())
            .map_err(|err| WorkerShardError::EpochStore(err.to_string()))
    }

    async fn has_newer(
        &self,
        stream_id: &str,
        partition_id: u32,
        owner_epoch: u64,
    ) -> Result<bool, WorkerShardError> {
        self.publisher
            .has_newer_ownership_epoch_record(stream_id, partition_id, owner_epoch)
            .await
            .map_err(|err| WorkerShardError::EpochStore(err.to_string()))
    }
}

pub async fn reconcile_worker_shard<L, E>(
    shard: &VelorixWorkerShard,
    lease_client: &L,
    epoch_store: &E,
    input: WorkerShardReconcileInput,
) -> Result<WorkerShardReconcileOutput, WorkerShardError>
where
    L: PartitionLeaseClient,
    E: WorkerShardEpochStore,
{
    let desired = velorix_view_from_worker_shard(shard)?;
    let key = partition_lease_key_from_worker_shard(shard)?;
    let facts = observe_facts(&key, lease_client, epoch_store, &input).await?;
    let plan = plan_reconcile(&desired, &facts);
    let mut output = WorkerShardReconcileOutput {
        plan,
        facts,
        commands: Vec::new(),
        command_error: None,
    };

    apply_plan_actions(
        &desired,
        &key,
        lease_client,
        epoch_store,
        &input,
        &mut output,
    )
    .await?;

    Ok(output)
}

pub async fn handle_worker_shard_event<L, E>(
    event: WorkerShardEvent,
    lease_client: &L,
    epoch_store: &E,
    input: WorkerShardReconcileInput,
) -> Result<Option<WorkerShardReconcileOutput>, WorkerShardError>
where
    L: PartitionLeaseClient,
    E: WorkerShardEpochStore,
{
    match event {
        WorkerShardEvent::Applied(shard) => {
            reconcile_worker_shard(&shard, lease_client, epoch_store, input)
                .await
                .map(Some)
        }
        WorkerShardEvent::Deleted(_) => Ok(None),
    }
}

pub async fn handle_worker_shard_event_with_output_sink<L, E, SinkError>(
    event: WorkerShardEvent,
    lease_client: &L,
    epoch_store: &E,
    input: WorkerShardReconcileInput,
    mut output_sink: impl FnMut(&WorkerShardReconcileOutput) -> Result<(), SinkError> + Send,
) -> Result<Option<WorkerShardReconcileOutput>, WorkerShardError>
where
    L: PartitionLeaseClient,
    E: WorkerShardEpochStore,
    SinkError: std::fmt::Display,
{
    let output = handle_worker_shard_event(event, lease_client, epoch_store, input).await?;
    if let Some(output) = &output {
        output_sink(output).map_err(WorkerShardError::command_sink)?;
    }
    Ok(output)
}

pub async fn watch_worker_shards<L, E, SinkError>(
    client: Client,
    namespace: &str,
    lease_client: L,
    epoch_store: E,
    mut input_for_shard: impl FnMut(&VelorixWorkerShard) -> WorkerShardReconcileInput + Send,
    mut output_sink: impl FnMut(&WorkerShardReconcileOutput) -> Result<(), SinkError> + Send,
) -> Result<(), WorkerShardError>
where
    L: PartitionLeaseClient,
    E: WorkerShardEpochStore,
    SinkError: std::fmt::Display,
{
    let api: Api<VelorixWorkerShard> = Api::namespaced(client, namespace);
    let mut events = Box::pin(watcher::watcher(api, watcher::Config::default()));

    while let Some(event) = events.next().await {
        let event = event.map_err(WorkerShardError::watcher)?;
        if let Some(event) = worker_shard_watch_event(event) {
            let input = match &event {
                WorkerShardEvent::Applied(shard) | WorkerShardEvent::Deleted(shard) => {
                    input_for_shard(shard)
                }
            };
            handle_worker_shard_event_with_output_sink(
                event,
                &lease_client,
                &epoch_store,
                input,
                &mut output_sink,
            )
            .await?;
        }
    }
    Ok(())
}

pub fn worker_shard_watch_event(event: Event<VelorixWorkerShard>) -> Option<WorkerShardEvent> {
    match event {
        Event::Apply(shard) | Event::InitApply(shard) => Some(WorkerShardEvent::Applied(shard)),
        Event::Delete(shard) => Some(WorkerShardEvent::Deleted(shard)),
        Event::Init | Event::InitDone => None,
    }
}

pub fn partition_lease_key_from_worker_shard(
    shard: &VelorixWorkerShard,
) -> Result<PartitionLeaseKey, WorkerShardError> {
    let namespace = shard
        .namespace()
        .ok_or(WorkerShardError::MissingObjectField {
            field: "metadata.namespace",
        })?;

    Ok(PartitionLeaseKey {
        namespace,
        view_id: shard.spec.view_id.clone(),
        stream_id: shard.spec.stream_id.clone(),
        partition_id: shard.spec.partition_id,
    })
}

pub fn velorix_view_from_worker_shard(
    shard: &VelorixWorkerShard,
) -> Result<VelorixView, WorkerShardError> {
    let namespace = shard
        .namespace()
        .ok_or(WorkerShardError::MissingObjectField {
            field: "metadata.namespace",
        })?;

    Ok(VelorixView {
        api_version: "control.velorix.io/v1alpha1".to_string(),
        kind: "VelorixView".to_string(),
        metadata: ContractMetadata {
            name: shard.name_any(),
            namespace,
            generation: shard
                .metadata
                .generation
                .and_then(|generation| u64::try_from(generation).ok())
                .unwrap_or_default(),
        },
        spec_version: 1,
        spec: VelorixViewSpec {
            view_id: shard.spec.view_id.clone(),
            worker: WorkerIntent {
                stream_id: shard.spec.stream_id.clone(),
                partition_id: shard.spec.partition_id,
                owner_id: shard.spec.desired_owner_id.clone(),
            },
        },
        status: view_status_from_worker_status(shard.status.as_ref()),
    })
}

async fn observe_facts<L, E>(
    key: &PartitionLeaseKey,
    lease_client: &L,
    epoch_store: &E,
    input: &WorkerShardReconcileInput,
) -> Result<ObservedControlPlaneFacts, WorkerShardError>
where
    L: PartitionLeaseClient,
    E: WorkerShardEpochStore,
{
    let lease = lease_client.current(key, input.now_unix_ms).await?;
    let epoch_record = match lease.as_ref() {
        Some(grant) if !epoch_store_has_newer_record(key, grant, epoch_store).await? => {
            epoch_store
                .read(&key.stream_id, key.partition_id, grant.owner_epoch)
                .await?
        }
        Some(_) => None,
        None => None,
    };

    Ok(facts_from_observations(
        lease,
        epoch_record,
        input.running_worker.clone(),
    ))
}

async fn epoch_store_has_newer_record<E>(
    key: &PartitionLeaseKey,
    grant: &PartitionLeaseGrant,
    epoch_store: &E,
) -> Result<bool, WorkerShardError>
where
    E: WorkerShardEpochStore,
{
    epoch_store
        .has_newer(&key.stream_id, key.partition_id, grant.owner_epoch)
        .await
}

fn facts_from_observations(
    lease: Option<PartitionLeaseGrant>,
    epoch_record: Option<OwnershipEpochRecord>,
    worker: Option<WorkerFact>,
) -> ObservedControlPlaneFacts {
    ObservedControlPlaneFacts {
        lease: lease.map(|grant| LeaseFact {
            owner_id: grant.owner_id,
            owner_epoch: Some(grant.owner_epoch),
        }),
        epoch_record: epoch_record.map(|record| EpochRecordFact {
            owner_id: record.owner_id,
            owner_epoch: record.owner_epoch,
        }),
        worker,
    }
}

async fn apply_plan_actions<L, E>(
    desired: &VelorixView,
    key: &PartitionLeaseKey,
    lease_client: &L,
    epoch_store: &E,
    input: &WorkerShardReconcileInput,
    output: &mut WorkerShardReconcileOutput,
) -> Result<(), WorkerShardError>
where
    L: PartitionLeaseClient,
    E: WorkerShardEpochStore,
{
    let mut worker_after_stops = output.facts.worker.clone();
    let actions = output.plan.actions.clone();
    for action in actions {
        match action {
            ReconcileAction::StopWorker {
                owner_id,
                owner_epoch,
            } => {
                output.commands.push(WorkerShardCommand::StopWorker {
                    owner_id,
                    owner_epoch,
                });
                worker_after_stops = None;
            }
            ReconcileAction::StartWorker {
                owner_id,
                owner_epoch,
            } => output.commands.push(WorkerShardCommand::StartWorker {
                owner_id,
                owner_epoch,
            }),
            ReconcileAction::AcquireLease { owner_id } => {
                output.commands.push(WorkerShardCommand::AcquireLease {
                    owner_id: owner_id.clone(),
                });
                let request = LeaseAcquireRequest {
                    key: key.clone(),
                    owner_id,
                    now_unix_ms: input.now_unix_ms,
                    ttl_ms: input.ttl_ms,
                };
                let grant = match lease_client.acquire_or_renew(request).await {
                    Ok(grant) => grant,
                    Err(err) => {
                        output.command_error = Some(err.to_string());
                        return Ok(());
                    }
                };

                let record = ownership_epoch_record_from_grant(
                    &grant,
                    partition_lease_identity(key),
                    input.config.created_at.clone(),
                    input.config.previous_checkpoint_version,
                );
                output
                    .commands
                    .push(WorkerShardCommand::PersistEpochRecord {
                        owner_id: record.owner_id.clone(),
                        owner_epoch: record.owner_epoch,
                    });
                if let Err(err) = epoch_store.create(record).await {
                    output.command_error = Some(err.to_string());
                    return Ok(());
                }

                let read_back = epoch_store
                    .read(&key.stream_id, key.partition_id, grant.owner_epoch)
                    .await?;
                let next_facts = if !epoch_store_has_newer_record(key, &grant, epoch_store).await? {
                    facts_from_observations(Some(grant), read_back, worker_after_stops.clone())
                } else {
                    facts_from_observations(Some(grant), None, worker_after_stops.clone())
                };
                let next_plan = plan_reconcile(desired, &next_facts);
                append_non_acquire_commands(&next_plan, output);
                output.plan = next_plan;
                output.facts = next_facts;
            }
        }
    }

    Ok(())
}

fn append_non_acquire_commands(plan: &ReconcilePlan, output: &mut WorkerShardReconcileOutput) {
    for action in &plan.actions {
        match action {
            ReconcileAction::AcquireLease { .. } => {}
            ReconcileAction::StopWorker {
                owner_id,
                owner_epoch,
            } => output.commands.push(WorkerShardCommand::StopWorker {
                owner_id: owner_id.clone(),
                owner_epoch: *owner_epoch,
            }),
            ReconcileAction::StartWorker {
                owner_id,
                owner_epoch,
            } => output.commands.push(WorkerShardCommand::StartWorker {
                owner_id: owner_id.clone(),
                owner_epoch: *owner_epoch,
            }),
        }
    }
}

fn view_status_from_worker_status(status: Option<&WorkerShardStatus>) -> VelorixViewStatus {
    let Some(status) = status else {
        return VelorixViewStatus::default();
    };

    VelorixViewStatus {
        observed_generation: status
            .observed_generation
            .and_then(|generation| u64::try_from(generation).ok()),
        observed_checkpoint_version: None,
        observed_owner_epoch: status
            .current_owner_epoch
            .as_ref()
            .map(|owner| owner.owner_epoch),
        conditions: Vec::new(),
    }
}

impl WorkerShardError {
    fn watcher(error: watcher::Error) -> Self {
        Self::Watcher {
            message: error.to_string(),
        }
    }

    fn command_sink(error: impl std::fmt::Display) -> Self {
        Self::CommandSink {
            message: error.to_string(),
        }
    }
}
