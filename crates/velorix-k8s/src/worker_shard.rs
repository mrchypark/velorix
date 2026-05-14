use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use futures::StreamExt;
use k8s_openapi::{
    api::core::v1::{Container, EnvVar, Pod, PodSpec},
    apimachinery::pkg::apis::meta::v1::ObjectMeta,
};
use kube::{
    api::{Api, DeleteParams, PostParams},
    runtime::watcher::{self, Event},
    Client, ResourceExt,
};
use thiserror::Error;
use tokio::process::Command;
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
use velorix_storage::{
    capability::{AuthoritativeNamespace, AuthoritativeObjectStoreCapabilitiesV1},
    ownership::OwnershipEpochRecord,
    state::CheckpointPublisher,
};

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkerShardCommandExecutorError {
    message: String,
}

impl WorkerShardCommandExecutorError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl std::fmt::Display for WorkerShardCommandExecutorError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for WorkerShardCommandExecutorError {}

#[async_trait]
pub trait WorkerShardCommandExecutor: Send + Sync {
    async fn stop_worker(
        &self,
        owner_id: &str,
        owner_epoch: u64,
    ) -> Result<(), WorkerShardCommandExecutorError>;

    async fn start_worker(
        &self,
        owner_id: &str,
        owner_epoch: u64,
    ) -> Result<(), WorkerShardCommandExecutorError>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkerShardProcessCommand {
    program: String,
    args: Vec<String>,
}

impl WorkerShardProcessCommand {
    pub fn new(
        program: impl Into<String>,
        args: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self, WorkerShardCommandExecutorError> {
        let program = program.into();
        if program.trim().is_empty() {
            return Err(WorkerShardCommandExecutorError::new(
                "worker process command program must not be empty",
            ));
        }

        Ok(Self {
            program,
            args: args.into_iter().map(Into::into).collect(),
        })
    }

    pub fn from_argv(
        argv: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self, WorkerShardCommandExecutorError> {
        let mut argv = argv.into_iter().map(Into::into);
        let Some(program) = argv.next() else {
            return Err(WorkerShardCommandExecutorError::new(
                "worker process command argv must not be empty",
            ));
        };

        Self::new(program, argv)
    }
}

#[derive(Clone, Debug)]
pub struct ProcessWorkerShardCommandExecutor {
    start_command: WorkerShardProcessCommand,
    stop_command: WorkerShardProcessCommand,
}

impl ProcessWorkerShardCommandExecutor {
    pub fn new(
        start_command: WorkerShardProcessCommand,
        stop_command: WorkerShardProcessCommand,
    ) -> Self {
        Self {
            start_command,
            stop_command,
        }
    }

    async fn run(
        command: &WorkerShardProcessCommand,
        action: &str,
        owner_id: &str,
        owner_epoch: u64,
    ) -> Result<(), WorkerShardCommandExecutorError> {
        let status = Command::new(&command.program)
            .args(&command.args)
            .env("VELORIX_WORKER_ACTION", action)
            .env("VELORIX_WORKER_OWNER_ID", owner_id)
            .env("VELORIX_WORKER_OWNER_EPOCH", owner_epoch.to_string())
            .status()
            .await
            .map_err(|error| {
                WorkerShardCommandExecutorError::new(format!(
                    "failed to execute worker {action} command `{}`: {error}",
                    command.program
                ))
            })?;

        if status.success() {
            Ok(())
        } else {
            Err(WorkerShardCommandExecutorError::new(format!(
                "worker {action} command `{}` exited with {status}",
                command.program
            )))
        }
    }
}

#[async_trait]
impl WorkerShardCommandExecutor for ProcessWorkerShardCommandExecutor {
    async fn stop_worker(
        &self,
        owner_id: &str,
        owner_epoch: u64,
    ) -> Result<(), WorkerShardCommandExecutorError> {
        Self::run(&self.stop_command, "stop", owner_id, owner_epoch).await
    }

    async fn start_worker(
        &self,
        owner_id: &str,
        owner_epoch: u64,
    ) -> Result<(), WorkerShardCommandExecutorError> {
        Self::run(&self.start_command, "start", owner_id, owner_epoch).await
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkerShardPodTemplate {
    image: String,
    command: Option<Vec<String>>,
    args: Option<Vec<String>>,
    labels: BTreeMap<String, String>,
    service_account_name: Option<String>,
}

impl WorkerShardPodTemplate {
    pub fn new(image: impl Into<String>) -> Result<Self, WorkerShardCommandExecutorError> {
        let image = image.into();
        if image.trim().is_empty() {
            return Err(WorkerShardCommandExecutorError::new(
                "worker pod image must not be empty",
            ));
        }

        Ok(Self {
            image,
            command: None,
            args: None,
            labels: BTreeMap::new(),
            service_account_name: None,
        })
    }

    pub fn with_command(mut self, command: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.command = Some(command.into_iter().map(Into::into).collect());
        self
    }

    pub fn with_args(mut self, args: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.args = Some(args.into_iter().map(Into::into).collect());
        self
    }

    pub fn with_label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.insert(key.into(), value.into());
        self
    }

    pub fn with_service_account_name(mut self, service_account_name: impl Into<String>) -> Self {
        self.service_account_name = Some(service_account_name.into());
        self
    }

    pub fn pod_for_owner(&self, owner_id: &str, owner_epoch: u64) -> Pod {
        let pod_name = worker_shard_pod_name(owner_id, owner_epoch);
        let mut labels = self.labels.clone();
        labels.insert(
            "app.kubernetes.io/name".to_string(),
            "velorix-worker".to_string(),
        );
        labels.insert(
            "app.kubernetes.io/component".to_string(),
            "worker-shard".to_string(),
        );
        labels.insert(
            "control.velorix.io/owner-id".to_string(),
            dns_label_fragment(owner_id, 63),
        );
        labels.insert(
            "control.velorix.io/owner-epoch".to_string(),
            owner_epoch.to_string(),
        );

        Pod {
            metadata: ObjectMeta {
                name: Some(pod_name),
                labels: Some(labels),
                ..ObjectMeta::default()
            },
            spec: Some(PodSpec {
                containers: vec![Container {
                    name: "velorix-worker".to_string(),
                    image: Some(self.image.clone()),
                    command: self.command.clone(),
                    args: self.args.clone(),
                    env: Some(vec![
                        env_var("VELORIX_WORKER_OWNER_ID", owner_id),
                        env_var("VELORIX_WORKER_OWNER_EPOCH", &owner_epoch.to_string()),
                    ]),
                    ..Container::default()
                }],
                restart_policy: Some("Never".to_string()),
                service_account_name: self.service_account_name.clone(),
                ..PodSpec::default()
            }),
            status: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct KubernetesPodWorkerShardCommandExecutor {
    pods: Api<Pod>,
    template: WorkerShardPodTemplate,
}

impl KubernetesPodWorkerShardCommandExecutor {
    pub fn new(client: Client, namespace: &str, template: WorkerShardPodTemplate) -> Self {
        Self {
            pods: Api::namespaced(client, namespace),
            template,
        }
    }

    pub fn from_api(pods: Api<Pod>, template: WorkerShardPodTemplate) -> Self {
        Self { pods, template }
    }

    fn api_error(
        action: &str,
        owner_id: &str,
        owner_epoch: u64,
        error: kube::Error,
    ) -> WorkerShardCommandExecutorError {
        WorkerShardCommandExecutorError::new(format!(
            "kubernetes pod worker {action} failed for owner `{owner_id}` epoch {owner_epoch}: {error}"
        ))
    }
}

#[async_trait]
impl WorkerShardCommandExecutor for KubernetesPodWorkerShardCommandExecutor {
    async fn stop_worker(
        &self,
        owner_id: &str,
        owner_epoch: u64,
    ) -> Result<(), WorkerShardCommandExecutorError> {
        let pod_name = worker_shard_pod_name(owner_id, owner_epoch);
        match self
            .pods
            .delete(&pod_name, &DeleteParams::default().grace_period(0))
            .await
        {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(response)) if response.code == 404 => Ok(()),
            Err(error) => Err(Self::api_error("stop", owner_id, owner_epoch, error)),
        }
    }

    async fn start_worker(
        &self,
        owner_id: &str,
        owner_epoch: u64,
    ) -> Result<(), WorkerShardCommandExecutorError> {
        let pod = self.template.pod_for_owner(owner_id, owner_epoch);
        match self.pods.create(&PostParams::default(), &pod).await {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(response)) if response.code == 409 => Ok(()),
            Err(error) => Err(Self::api_error("start", owner_id, owner_epoch, error)),
        }
    }
}

pub fn worker_shard_pod_name(owner_id: &str, owner_epoch: u64) -> String {
    let prefix = "velorix-worker";
    let epoch = owner_epoch.to_string();
    let hash = stable_hash8(owner_id);
    let max_fragment_len = 63usize
        .saturating_sub(prefix.len())
        .saturating_sub(epoch.len())
        .saturating_sub(hash.len())
        .saturating_sub(3);
    let owner = dns_label_fragment(owner_id, max_fragment_len.max(1));
    format!("{prefix}-{owner}-{hash}-{epoch}")
}

fn env_var(name: &str, value: &str) -> EnvVar {
    EnvVar {
        name: name.to_string(),
        value: Some(value.to_string()),
        value_from: None,
    }
}

fn dns_label_fragment(value: &str, max_len: usize) -> String {
    let mut fragment = String::new();
    let mut last_was_dash = false;
    for byte in value.bytes() {
        let next = match byte {
            b'a'..=b'z' | b'0'..=b'9' => byte as char,
            b'A'..=b'Z' => (byte as char).to_ascii_lowercase(),
            _ => '-',
        };

        if next == '-' {
            if fragment.is_empty() || last_was_dash {
                continue;
            }
            last_was_dash = true;
        } else {
            last_was_dash = false;
        }
        fragment.push(next);
        if fragment.len() == max_len {
            break;
        }
    }

    while fragment.ends_with('-') {
        fragment.pop();
    }

    if fragment.is_empty() {
        "unknown".to_string()
    } else {
        fragment
    }
}

fn stable_hash8(value: &str) -> String {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in value.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{:08x}", hash as u32)
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
    #[error("worker shard command executor failed: {message}")]
    CommandExecutor { message: String },
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
    pub(crate) fn new(publisher: CheckpointPublisher) -> Self {
        Self { publisher }
    }

    pub(crate) fn from_authority_parts(
        store: Arc<dyn object_store::ObjectStore>,
        capabilities: Arc<AuthoritativeObjectStoreCapabilitiesV1>,
    ) -> Result<Self, WorkerShardError> {
        let profile = capabilities
            .validate_namespace(AuthoritativeNamespace::Ownership)
            .map_err(|error| WorkerShardError::EpochStore(error.to_string()))?;
        let publisher = CheckpointPublisher::new_checked(store, profile)
            .map_err(|error| WorkerShardError::EpochStore(error.to_string()))?;

        Ok(Self::new(publisher))
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

pub async fn execute_worker_shard_commands<X>(
    output: &WorkerShardReconcileOutput,
    executor: &X,
) -> Result<(), WorkerShardError>
where
    X: WorkerShardCommandExecutor + ?Sized,
{
    for command in &output.commands {
        match command {
            WorkerShardCommand::AcquireLease { .. }
            | WorkerShardCommand::PersistEpochRecord { .. } => {}
            WorkerShardCommand::StopWorker {
                owner_id,
                owner_epoch,
            } => executor
                .stop_worker(owner_id, *owner_epoch)
                .await
                .map_err(WorkerShardError::command_executor)?,
            WorkerShardCommand::StartWorker {
                owner_id,
                owner_epoch,
            } => executor
                .start_worker(owner_id, *owner_epoch)
                .await
                .map_err(WorkerShardError::command_executor)?,
        }
    }
    Ok(())
}

pub async fn handle_worker_shard_event_with_command_executor<L, E, X>(
    event: WorkerShardEvent,
    lease_client: &L,
    epoch_store: &E,
    input: WorkerShardReconcileInput,
    executor: &X,
) -> Result<Option<WorkerShardReconcileOutput>, WorkerShardError>
where
    L: PartitionLeaseClient,
    E: WorkerShardEpochStore,
    X: WorkerShardCommandExecutor + ?Sized,
{
    let output = handle_worker_shard_event(event, lease_client, epoch_store, input).await?;
    if let Some(output) = &output {
        execute_worker_shard_commands(output, executor).await?;
    }
    Ok(output)
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

pub async fn watch_worker_shards_with_command_executor<L, E, X>(
    client: Client,
    namespace: &str,
    lease_client: L,
    epoch_store: E,
    mut input_for_shard: impl FnMut(&VelorixWorkerShard) -> WorkerShardReconcileInput + Send,
    executor: X,
) -> Result<(), WorkerShardError>
where
    L: PartitionLeaseClient,
    E: WorkerShardEpochStore,
    X: WorkerShardCommandExecutor,
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
            handle_worker_shard_event_with_command_executor(
                event,
                &lease_client,
                &epoch_store,
                input,
                &executor,
            )
            .await?;
        }
    }
    Ok(())
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

    pub fn command_executor(error: impl std::fmt::Display) -> Self {
        Self::CommandExecutor {
            message: error.to_string(),
        }
    }
}
