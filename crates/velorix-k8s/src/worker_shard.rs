use std::{collections::BTreeMap, future::Future, pin::Pin, sync::Arc, time::Duration};

use async_trait::async_trait;
use futures::{FutureExt, Stream, StreamExt};
use k8s_openapi::{
    api::core::v1::{
        Container, EnvVar, Pod, PodSpec, Probe, ResourceRequirements, SecurityContext,
        TCPSocketAction,
    },
    apimachinery::pkg::api::resource::Quantity,
    apimachinery::pkg::apis::meta::v1::ObjectMeta,
    apimachinery::pkg::util::intstr::IntOrString,
};
use kube::{
    api::{Api, DeleteParams, ListParams, PostParams},
    runtime::watcher::{self, Event},
    Client, ResourceExt,
};
use thiserror::Error;
use tokio::{
    process::Command,
    time::{self, Instant, MissedTickBehavior},
};
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
    storage_admin::{
        AuthoritativeNamespace, AuthoritativeObjectStoreCapabilitiesV1, CheckpointPublishError,
        CheckpointPublisher, OwnershipEpochRecord,
    },
};

use crate::{
    crd::{ObjectStoreAuthorityRef, VelorixWorkerShard, WorkerShardStatus},
    lease::{
        ownership_epoch_record_from_grant, partition_lease_identity, KubeLeaseApi,
        KubernetesPartitionLeaseClient,
    },
    startup::{OperatorAuthorityStartupComponents, ValidatedStartupAuthorityToken},
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
pub struct WorkerShardRuntimeIdentity {
    pub namespace: String,
    pub view_id: String,
    pub stream_id: String,
    pub partition_id: u32,
    pub authority: ObjectStoreAuthorityRef,
    pub lease_identity: String,
    pub owner_id: String,
    pub owner_epoch: u64,
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

#[async_trait]
pub trait WorkerShardScopedCommandExecutor: Send + Sync {
    async fn stop_worker(
        &self,
        identity: &WorkerShardRuntimeIdentity,
    ) -> Result<(), WorkerShardCommandExecutorError>;

    async fn start_worker(
        &self,
        identity: &WorkerShardRuntimeIdentity,
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
                    resources: Some(ResourceRequirements {
                        requests: Some(
                            [
                                ("cpu".to_string(), Quantity("500m".to_string())),
                                ("memory".to_string(), Quantity("512Mi".to_string())),
                            ]
                            .into(),
                        ),
                        limits: Some(
                            [
                                ("cpu".to_string(), Quantity("2".to_string())),
                                ("memory".to_string(), Quantity("2Gi".to_string())),
                            ]
                            .into(),
                        ),
                        ..Default::default()
                    }),
                    liveness_probe: Some(Probe {
                        tcp_socket: Some(TCPSocketAction {
                            port: IntOrString::Int(9090),
                            ..Default::default()
                        }),
                        initial_delay_seconds: Some(10),
                        period_seconds: Some(15),
                        ..Default::default()
                    }),
                    readiness_probe: Some(Probe {
                        tcp_socket: Some(TCPSocketAction {
                            port: IntOrString::Int(9090),
                            ..Default::default()
                        }),
                        initial_delay_seconds: Some(5),
                        period_seconds: Some(10),
                        ..Default::default()
                    }),
                    security_context: Some(SecurityContext {
                        run_as_non_root: Some(true),
                        read_only_root_filesystem: Some(true),
                        allow_privilege_escalation: Some(false),
                        ..Default::default()
                    }),
                    ..Container::default()
                }],
                restart_policy: Some("Never".to_string()),
                service_account_name: self.service_account_name.clone(),
                security_context: Some(k8s_openapi::api::core::v1::PodSecurityContext {
                    run_as_non_root: Some(true),
                    seccomp_profile: Some(k8s_openapi::api::core::v1::SeccompProfile {
                        type_: "RuntimeDefault".to_string(),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                termination_grace_period_seconds: Some(60),
                ..PodSpec::default()
            }),
            status: None,
        }
    }

    pub fn pod_for_identity(&self, identity: &WorkerShardRuntimeIdentity) -> Pod {
        let pod_name = worker_shard_pod_name_for_identity(identity);
        let labels = self.labels_for_identity(identity);

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
                    env: Some(env_for_identity(identity)),
                    resources: Some(ResourceRequirements {
                        requests: Some(
                            [
                                ("cpu".to_string(), Quantity("500m".to_string())),
                                ("memory".to_string(), Quantity("512Mi".to_string())),
                            ]
                            .into(),
                        ),
                        limits: Some(
                            [
                                ("cpu".to_string(), Quantity("2".to_string())),
                                ("memory".to_string(), Quantity("2Gi".to_string())),
                            ]
                            .into(),
                        ),
                        ..Default::default()
                    }),
                    liveness_probe: Some(Probe {
                        tcp_socket: Some(TCPSocketAction {
                            port: IntOrString::Int(9090),
                            ..Default::default()
                        }),
                        initial_delay_seconds: Some(10),
                        period_seconds: Some(15),
                        ..Default::default()
                    }),
                    readiness_probe: Some(Probe {
                        tcp_socket: Some(TCPSocketAction {
                            port: IntOrString::Int(9090),
                            ..Default::default()
                        }),
                        initial_delay_seconds: Some(5),
                        period_seconds: Some(10),
                        ..Default::default()
                    }),
                    security_context: Some(SecurityContext {
                        run_as_non_root: Some(true),
                        read_only_root_filesystem: Some(true),
                        allow_privilege_escalation: Some(false),
                        ..Default::default()
                    }),
                    ..Container::default()
                }],
                restart_policy: Some("Never".to_string()),
                service_account_name: self.service_account_name.clone(),
                security_context: Some(k8s_openapi::api::core::v1::PodSecurityContext {
                    run_as_non_root: Some(true),
                    seccomp_profile: Some(k8s_openapi::api::core::v1::SeccompProfile {
                        type_: "RuntimeDefault".to_string(),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                termination_grace_period_seconds: Some(60),
                ..PodSpec::default()
            }),
            status: None,
        }
    }

    fn labels_for_identity(
        &self,
        identity: &WorkerShardRuntimeIdentity,
    ) -> BTreeMap<String, String> {
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
            dns_label_fragment(&identity.owner_id, 63),
        );
        labels.insert(
            "control.velorix.io/owner-epoch".to_string(),
            identity.owner_epoch.to_string(),
        );
        labels.insert(
            "control.velorix.io/view-id".to_string(),
            dns_label_fragment(&identity.view_id, 63),
        );
        labels.insert(
            "control.velorix.io/stream-id".to_string(),
            dns_label_fragment(&identity.stream_id, 63),
        );
        labels.insert(
            "control.velorix.io/partition-id".to_string(),
            identity.partition_id.to_string(),
        );
        labels.insert(
            "control.velorix.io/identity-hash".to_string(),
            worker_shard_identity_hash(identity),
        );
        labels
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

#[derive(Clone, Debug)]
pub struct KubernetesPodWorkerShardScopedCommandExecutor {
    pods: Api<Pod>,
    template: WorkerShardPodTemplate,
}

impl KubernetesPodWorkerShardScopedCommandExecutor {
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
        identity: &WorkerShardRuntimeIdentity,
        error: kube::Error,
    ) -> WorkerShardCommandExecutorError {
        WorkerShardCommandExecutorError::new(format!(
            "kubernetes pod worker {action} failed for shard `{}`/{} owner `{}` epoch {}: {error}",
            identity.stream_id, identity.partition_id, identity.owner_id, identity.owner_epoch
        ))
    }

    fn identity_mismatch_error(
        action: &str,
        identity: &WorkerShardRuntimeIdentity,
    ) -> WorkerShardCommandExecutorError {
        WorkerShardCommandExecutorError::new(format!(
            "kubernetes pod worker {action} identity mismatch for shard `{}`/{} owner `{}` epoch {}",
            identity.stream_id, identity.partition_id, identity.owner_id, identity.owner_epoch
        ))
    }
}

#[async_trait]
impl WorkerShardScopedCommandExecutor for KubernetesPodWorkerShardScopedCommandExecutor {
    async fn stop_worker(
        &self,
        identity: &WorkerShardRuntimeIdentity,
    ) -> Result<(), WorkerShardCommandExecutorError> {
        let pod_name = worker_shard_pod_name_for_identity(identity);
        match self
            .pods
            .delete(&pod_name, &DeleteParams::default().grace_period(0))
            .await
        {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(response)) if response.code == 404 => Ok(()),
            Err(error) => Err(Self::api_error("stop", identity, error)),
        }
    }

    async fn start_worker(
        &self,
        identity: &WorkerShardRuntimeIdentity,
    ) -> Result<(), WorkerShardCommandExecutorError> {
        let pod = self.template.pod_for_identity(identity);
        match self.pods.create(&PostParams::default(), &pod).await {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(response)) if response.code == 409 => {
                let pod_name = worker_shard_pod_name_for_identity(identity);
                let existing = self
                    .pods
                    .get(&pod_name)
                    .await
                    .map_err(|error| Self::api_error("read-existing", identity, error))?;
                if pod_matches_identity(&existing, &self.template, identity) {
                    Ok(())
                } else {
                    Err(Self::identity_mismatch_error("start", identity))
                }
            }
            Err(error) => Err(Self::api_error("start", identity, error)),
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

pub fn worker_shard_pod_name_for_identity(identity: &WorkerShardRuntimeIdentity) -> String {
    let prefix = "velorix-worker";
    let epoch = identity.owner_epoch.to_string();
    let hash = worker_shard_identity_hash(identity);
    let max_fragment_len = 63usize
        .saturating_sub(prefix.len())
        .saturating_sub(epoch.len())
        .saturating_sub(hash.len())
        .saturating_sub(3);
    let fragment = dns_label_fragment(
        &format!(
            "{}-{}-{}",
            identity.stream_id, identity.partition_id, identity.owner_id
        ),
        max_fragment_len.max(1),
    );
    format!("{prefix}-{fragment}-{hash}-{epoch}")
}

fn worker_shard_identity_hash(identity: &WorkerShardRuntimeIdentity) -> String {
    stable_hash8(&format!(
        "{}/{}/{}/{}/{}/{}/{}",
        identity.namespace,
        identity.view_id,
        identity.stream_id,
        identity.partition_id,
        identity.authority.store_id,
        identity.authority.namespace,
        identity.owner_id,
    ))
}

fn env_var(name: &str, value: &str) -> EnvVar {
    EnvVar {
        name: name.to_string(),
        value: Some(value.to_string()),
        value_from: None,
    }
}

fn env_for_identity(identity: &WorkerShardRuntimeIdentity) -> Vec<EnvVar> {
    vec![
        env_var("VELORIX_WORKER_NAMESPACE", &identity.namespace),
        env_var("VELORIX_WORKER_VIEW_ID", &identity.view_id),
        env_var("VELORIX_WORKER_STREAM_ID", &identity.stream_id),
        env_var(
            "VELORIX_WORKER_PARTITION_ID",
            &identity.partition_id.to_string(),
        ),
        env_var(
            "VELORIX_WORKER_AUTHORITY_STORE_ID",
            &identity.authority.store_id,
        ),
        env_var(
            "VELORIX_WORKER_AUTHORITY_NAMESPACE",
            &identity.authority.namespace,
        ),
        env_var("VELORIX_WORKER_LEASE_IDENTITY", &identity.lease_identity),
        env_var("VELORIX_WORKER_OWNER_ID", &identity.owner_id),
        env_var(
            "VELORIX_WORKER_OWNER_EPOCH",
            &identity.owner_epoch.to_string(),
        ),
    ]
}

fn pod_matches_identity(
    pod: &Pod,
    template: &WorkerShardPodTemplate,
    identity: &WorkerShardRuntimeIdentity,
) -> bool {
    let Some(metadata_name) = pod.metadata.name.as_deref() else {
        return false;
    };
    if metadata_name != worker_shard_pod_name_for_identity(identity) {
        return false;
    }

    let expected_labels = template.labels_for_identity(identity);
    let Some(labels) = pod.metadata.labels.as_ref() else {
        return false;
    };
    for (key, expected) in expected_labels {
        if labels.get(&key) != Some(&expected) {
            return false;
        }
    }

    let Some(spec) = pod.spec.as_ref() else {
        return false;
    };
    if spec.service_account_name != template.service_account_name {
        return false;
    }
    let Some(container) = spec.containers.first() else {
        return false;
    };
    if container.image != Some(template.image.clone())
        || container.command != template.command
        || container.args != template.args
    {
        return false;
    }

    let expected_env = env_for_identity(identity);
    let Some(env) = container.env.as_ref() else {
        return false;
    };
    expected_env
        .iter()
        .all(|expected| env.iter().any(|actual| actual == expected))
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
    #[error("worker shard authority failed: {message}")]
    Authority { message: String },
    #[error("worker shard authority mismatch: shard references {actual:?}, operator validated {expected:?}")]
    AuthorityMismatch {
        actual: ObjectStoreAuthorityRef,
        expected: ObjectStoreAuthorityRef,
    },
    #[error("kubernetes worker shard watcher failed: {message}")]
    Watcher { message: String },
    #[error("kubernetes worker shard API failed: {message}")]
    KubernetesApi { message: String },
    #[error("worker shard resync exceeded {bound} bound of {limit}")]
    ResyncBoundExceeded { bound: &'static str, limit: usize },
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
        _token: ValidatedStartupAuthorityToken,
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
            Err(CheckpointPublishError::ObjectStore(object_store::Error::NotFound { .. })) => {
                Ok(None)
            }
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
        WorkerShardEvent::Deleted(shard) => Ok(deleted_worker_shard_output(&shard, &input)),
    }
}

fn deleted_worker_shard_output(
    shard: &VelorixWorkerShard,
    input: &WorkerShardReconcileInput,
) -> Option<WorkerShardReconcileOutput> {
    let worker = input.running_worker.as_ref()?;
    let current_owner = shard
        .status
        .as_ref()
        .and_then(|status| status.current_owner_epoch.as_ref())?;
    if current_owner.stream_id != shard.spec.stream_id
        || current_owner.partition_id != shard.spec.partition_id
        || current_owner.owner_id != worker.owner_id
        || current_owner.owner_epoch != worker.owner_epoch
    {
        return None;
    }

    Some(WorkerShardReconcileOutput {
        plan: ReconcilePlan {
            actions: vec![ReconcileAction::StopWorker {
                owner_id: worker.owner_id.clone(),
                owner_epoch: worker.owner_epoch,
            }],
            block_reason: None,
        },
        facts: ObservedControlPlaneFacts {
            lease: None,
            epoch_record: None,
            worker: Some(worker.clone()),
        },
        commands: vec![WorkerShardCommand::StopWorker {
            owner_id: worker.owner_id.clone(),
            owner_epoch: worker.owner_epoch,
        }],
        command_error: None,
    })
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

pub async fn execute_scoped_worker_shard_commands<X>(
    shard: &VelorixWorkerShard,
    output: &WorkerShardReconcileOutput,
    executor: &X,
) -> Result<(), WorkerShardError>
where
    X: WorkerShardScopedCommandExecutor + ?Sized,
{
    execute_scoped_worker_shard_commands_with_authority(shard, output, executor, None).await
}

pub async fn execute_scoped_worker_shard_commands_with_authority<X>(
    shard: &VelorixWorkerShard,
    output: &WorkerShardReconcileOutput,
    executor: &X,
    expected_authority: Option<&ObjectStoreAuthorityRef>,
) -> Result<(), WorkerShardError>
where
    X: WorkerShardScopedCommandExecutor + ?Sized,
{
    validate_worker_shard_authority(shard, expected_authority)?;
    let mut deferred_stop_error = None;

    for command in output.commands.iter().filter(|command| {
        matches!(
            command,
            WorkerShardCommand::StopWorker {
                owner_id: _,
                owner_epoch: _
            }
        )
    }) {
        let WorkerShardCommand::StopWorker {
            owner_id,
            owner_epoch,
        } = command
        else {
            unreachable!();
        };
        let identity = runtime_identity_from_worker_shard(shard, owner_id, *owner_epoch)?;
        if let Err(error) = executor.stop_worker(&identity).await {
            deferred_stop_error.get_or_insert(error);
        }
    }

    if let Some(error) = &output.command_error {
        return Err(WorkerShardError::Authority {
            message: error.clone(),
        });
    }

    if let Some(error) = deferred_stop_error {
        return Err(WorkerShardError::command_executor(error));
    }

    for command in &output.commands {
        match command {
            WorkerShardCommand::AcquireLease { .. }
            | WorkerShardCommand::PersistEpochRecord { .. }
            | WorkerShardCommand::StopWorker { .. } => {}
            WorkerShardCommand::StartWorker {
                owner_id,
                owner_epoch,
            } => {
                let identity = runtime_identity_from_worker_shard(shard, owner_id, *owner_epoch)?;
                executor
                    .start_worker(&identity)
                    .await
                    .map_err(WorkerShardError::command_executor)?;
            }
        }
    }
    Ok(())
}

pub async fn handle_worker_shard_event_with_scoped_command_executor<L, E, X>(
    event: WorkerShardEvent,
    lease_client: &L,
    epoch_store: &E,
    input: WorkerShardReconcileInput,
    executor: &X,
) -> Result<Option<WorkerShardReconcileOutput>, WorkerShardError>
where
    L: PartitionLeaseClient,
    E: WorkerShardEpochStore,
    X: WorkerShardScopedCommandExecutor + ?Sized,
{
    handle_worker_shard_event_with_scoped_command_executor_and_authority(
        event,
        lease_client,
        epoch_store,
        input,
        executor,
        None,
    )
    .await
}

pub async fn handle_worker_shard_event_with_scoped_command_executor_and_authority<L, E, X>(
    event: WorkerShardEvent,
    lease_client: &L,
    epoch_store: &E,
    input: WorkerShardReconcileInput,
    executor: &X,
    expected_authority: Option<&ObjectStoreAuthorityRef>,
) -> Result<Option<WorkerShardReconcileOutput>, WorkerShardError>
where
    L: PartitionLeaseClient,
    E: WorkerShardEpochStore,
    X: WorkerShardScopedCommandExecutor + ?Sized,
{
    match event {
        WorkerShardEvent::Applied(shard) => {
            validate_worker_shard_authority(&shard, expected_authority)?;
            let output = reconcile_worker_shard(&shard, lease_client, epoch_store, input).await?;
            execute_scoped_worker_shard_commands_with_authority(
                &shard,
                &output,
                executor,
                expected_authority,
            )
            .await?;
            Ok(Some(output))
        }
        WorkerShardEvent::Deleted(shard) => {
            validate_worker_shard_authority(&shard, expected_authority)?;
            let output = deleted_worker_shard_output(&shard, &input);
            if let Some(output) = &output {
                execute_scoped_worker_shard_commands_with_authority(
                    &shard,
                    output,
                    executor,
                    expected_authority,
                )
                .await?;
            }
            Ok(output)
        }
    }
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
    // Delete stops need shard/authority identity; keep them on the scoped executor path.
    let execute_commands = matches!(&event, WorkerShardEvent::Applied(_));
    let output = handle_worker_shard_event(event, lease_client, epoch_store, input).await?;
    if execute_commands {
        if let Some(output) = &output {
            execute_worker_shard_commands(output, executor).await?;
        }
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

pub type KubernetesWorkerShardOperatorRuntime = WorkerShardOperatorRuntime<
    KubernetesPartitionLeaseClient<KubeLeaseApi>,
    CheckpointPublisherEpochStore,
    KubernetesPodWorkerShardScopedCommandExecutor,
>;

pub struct WorkerShardOperatorRuntime<L, E, X> {
    lease_client: L,
    epoch_store: E,
    executor: X,
    authority: Option<ObjectStoreAuthorityRef>,
}

impl<L, E, X> WorkerShardOperatorRuntime<L, E, X> {
    pub fn new(lease_client: L, epoch_store: E, executor: X) -> Self {
        Self {
            lease_client,
            epoch_store,
            executor,
            authority: None,
        }
    }

    pub fn with_authority(
        lease_client: L,
        epoch_store: E,
        executor: X,
        authority: ObjectStoreAuthorityRef,
    ) -> Self {
        Self {
            lease_client,
            epoch_store,
            executor,
            authority: Some(authority),
        }
    }

    fn require_authority(&self) -> Result<(), WorkerShardError> {
        if self.authority.is_some() {
            Ok(())
        } else {
            Err(WorkerShardError::Authority {
                message: "worker-shard resync requires validated operator authority".to_string(),
            })
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkerShardResyncOptions {
    pub page_size: u32,
    pub max_pages: usize,
    pub max_shards: usize,
}

impl Default for WorkerShardResyncOptions {
    fn default() -> Self {
        Self {
            page_size: 128,
            max_pages: 32,
            max_shards: 1024,
        }
    }
}

impl WorkerShardResyncOptions {
    fn validate(&self) -> Result<(), WorkerShardError> {
        if self.max_pages == 0 {
            return Err(WorkerShardError::ResyncBoundExceeded {
                bound: "worker shard list pages",
                limit: self.max_pages,
            });
        }
        if self.max_shards == 0 {
            return Err(WorkerShardError::ResyncBoundExceeded {
                bound: "worker shards",
                limit: self.max_shards,
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkerShardResyncSummary {
    pub listed: usize,
    pub applied: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkerShardPeriodicResyncOptions {
    pub interval: Duration,
    pub resync: WorkerShardResyncOptions,
    pub max_cycles: Option<usize>,
}

impl Default for WorkerShardPeriodicResyncOptions {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(60),
            resync: WorkerShardResyncOptions::default(),
            max_cycles: Some(1),
        }
    }
}

impl WorkerShardPeriodicResyncOptions {
    fn validate(&self) -> Result<(), WorkerShardError> {
        if self.max_cycles.is_none() {
            return Err(WorkerShardError::ResyncBoundExceeded {
                bound: "worker shard periodic resync cycles",
                limit: 0,
            });
        }
        if self.interval.is_zero() {
            return Err(WorkerShardError::ResyncBoundExceeded {
                bound: "worker shard periodic resync interval",
                limit: 0,
            });
        }
        self.resync.validate()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkerShardPeriodicResyncSchedule {
    pub interval: Duration,
    pub resync: WorkerShardResyncOptions,
}

impl Default for WorkerShardPeriodicResyncSchedule {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(60),
            resync: WorkerShardResyncOptions::default(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkerShardLifecycleOptions {
    pub initial_resync: WorkerShardResyncOptions,
    pub periodic_resync: Option<WorkerShardPeriodicResyncSchedule>,
}

impl Default for WorkerShardLifecycleOptions {
    fn default() -> Self {
        Self {
            initial_resync: WorkerShardResyncOptions::default(),
            periodic_resync: Some(WorkerShardPeriodicResyncSchedule::default()),
        }
    }
}

impl WorkerShardLifecycleOptions {
    fn validate(&self) -> Result<(), WorkerShardError> {
        self.initial_resync.validate()?;
        if self
            .periodic_resync
            .as_ref()
            .is_some_and(|periodic| periodic.interval.is_zero())
        {
            return Err(WorkerShardError::ResyncBoundExceeded {
                bound: "worker shard periodic resync interval",
                limit: 0,
            });
        }
        if let Some(periodic) = &self.periodic_resync {
            periodic.resync.validate()?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WorkerShardLifecycleExit {
    Shutdown,
    WatchEnded,
}

impl<L, E, X> WorkerShardOperatorRuntime<L, E, X>
where
    L: PartitionLeaseClient,
    E: WorkerShardEpochStore,
    X: WorkerShardScopedCommandExecutor,
{
    pub async fn handle_event(
        &self,
        event: WorkerShardEvent,
        input: WorkerShardReconcileInput,
    ) -> Result<Option<WorkerShardReconcileOutput>, WorkerShardError> {
        handle_worker_shard_event_with_scoped_command_executor_and_authority(
            event,
            &self.lease_client,
            &self.epoch_store,
            input,
            &self.executor,
            self.authority.as_ref(),
        )
        .await
    }
}

pub fn build_kubernetes_worker_shard_operator_runtime(
    client: Client,
    namespace: &str,
    startup_components: &OperatorAuthorityStartupComponents,
    pod_template: WorkerShardPodTemplate,
) -> Result<KubernetesWorkerShardOperatorRuntime, WorkerShardError> {
    let lease_client = KubernetesPartitionLeaseClient::new(KubeLeaseApi::new(client.clone()));
    let epoch_store = startup_components.worker_shard_epoch_store()?;
    let executor =
        KubernetesPodWorkerShardScopedCommandExecutor::new(client, namespace, pod_template);

    Ok(WorkerShardOperatorRuntime::with_authority(
        lease_client,
        epoch_store,
        executor,
        startup_components.authority().clone(),
    ))
}

pub async fn watch_worker_shards_with_operator_runtime<L, E, X>(
    client: Client,
    namespace: &str,
    runtime: WorkerShardOperatorRuntime<L, E, X>,
    mut input_for_shard: impl FnMut(&VelorixWorkerShard) -> WorkerShardReconcileInput + Send,
) -> Result<(), WorkerShardError>
where
    L: PartitionLeaseClient,
    E: WorkerShardEpochStore,
    X: WorkerShardScopedCommandExecutor,
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
            runtime.handle_event(event, input).await?;
        }
    }
    Ok(())
}

pub async fn resync_worker_shards_once_with_operator_runtime<L, E, X>(
    client: Client,
    namespace: &str,
    runtime: &WorkerShardOperatorRuntime<L, E, X>,
    mut input_for_shard: impl FnMut(&VelorixWorkerShard) -> WorkerShardReconcileInput + Send,
    options: WorkerShardResyncOptions,
) -> Result<WorkerShardResyncSummary, WorkerShardError>
where
    L: PartitionLeaseClient,
    E: WorkerShardEpochStore,
    X: WorkerShardScopedCommandExecutor,
{
    runtime.require_authority()?;
    options.validate()?;

    let api: Api<VelorixWorkerShard> = Api::namespaced(client, namespace);
    let mut shards = Vec::new();
    let mut continue_token: Option<String> = None;
    for _ in 0..options.max_pages {
        let mut params = ListParams::default().limit(options.page_size.max(1));
        if let Some(token) = continue_token.as_ref() {
            params = params.continue_token(token);
        }
        let page = api
            .list(&params)
            .await
            .map_err(WorkerShardError::kubernetes_api)?;
        for shard in page.items {
            if shards.len() >= options.max_shards {
                return Err(WorkerShardError::ResyncBoundExceeded {
                    bound: "worker shards",
                    limit: options.max_shards,
                });
            }
            shards.push(shard);
        }
        continue_token = page.metadata.continue_.filter(|token| !token.is_empty());
        if continue_token.is_none() {
            break;
        }
    }
    if continue_token.is_some() {
        return Err(WorkerShardError::ResyncBoundExceeded {
            bound: "worker shard list pages",
            limit: options.max_pages,
        });
    }

    shards.sort_by(worker_shard_resync_order);
    let listed = shards.len();
    let mut applied = 0;
    for shard in shards {
        let input = input_for_shard(&shard);
        runtime
            .handle_event(WorkerShardEvent::Applied(shard), input)
            .await?;
        applied += 1;
    }

    Ok(WorkerShardResyncSummary { listed, applied })
}

pub async fn resync_worker_shards_periodically_with_operator_runtime<L, E, X>(
    client: Client,
    namespace: &str,
    runtime: &WorkerShardOperatorRuntime<L, E, X>,
    mut input_for_shard: impl FnMut(&VelorixWorkerShard) -> WorkerShardReconcileInput + Send,
    options: WorkerShardPeriodicResyncOptions,
) -> Result<Vec<WorkerShardResyncSummary>, WorkerShardError>
where
    L: PartitionLeaseClient,
    E: WorkerShardEpochStore,
    X: WorkerShardScopedCommandExecutor,
{
    runtime.require_authority()?;
    options.validate()?;
    if options.max_cycles == Some(0) {
        return Ok(Vec::new());
    }

    let mut interval = time::interval(options.interval);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut summaries = Vec::new();

    loop {
        interval.tick().await;
        summaries.push(
            resync_worker_shards_once_with_operator_runtime(
                client.clone(),
                namespace,
                runtime,
                &mut input_for_shard,
                options.resync.clone(),
            )
            .await?,
        );

        if options
            .max_cycles
            .is_some_and(|max_cycles| summaries.len() >= max_cycles)
        {
            return Ok(summaries);
        }
    }
}

pub async fn run_worker_shard_lifecycle_with_operator_event_stream<L, E, X, S, Shutdown>(
    client: Client,
    namespace: &str,
    runtime: &WorkerShardOperatorRuntime<L, E, X>,
    mut input_for_shard: impl FnMut(&VelorixWorkerShard) -> WorkerShardReconcileInput + Send,
    options: WorkerShardLifecycleOptions,
    events: S,
    shutdown: Shutdown,
) -> Result<WorkerShardLifecycleExit, WorkerShardError>
where
    L: PartitionLeaseClient,
    E: WorkerShardEpochStore,
    X: WorkerShardScopedCommandExecutor,
    S: Stream<Item = Result<WorkerShardEvent, WorkerShardError>>,
    Shutdown: Future<Output = ()>,
{
    options.validate()?;
    runtime.require_authority()?;
    let mut events = std::pin::pin!(events);
    let mut shutdown = std::pin::pin!(shutdown);

    if shutdown_ready(shutdown.as_mut()) {
        return Ok(WorkerShardLifecycleExit::Shutdown);
    }

    resync_worker_shards_once_with_operator_runtime(
        client.clone(),
        namespace,
        runtime,
        &mut input_for_shard,
        options.initial_resync,
    )
    .await?;

    match options.periodic_resync {
        Some(periodic_options) => {
            run_worker_shard_lifecycle_with_periodic_resync(
                client,
                namespace,
                runtime,
                &mut input_for_shard,
                events.as_mut(),
                shutdown.as_mut(),
                periodic_options,
            )
            .await
        }
        None => {
            run_worker_shard_lifecycle_without_periodic_resync(
                runtime,
                &mut input_for_shard,
                events.as_mut(),
                shutdown.as_mut(),
            )
            .await
        }
    }
}

async fn run_worker_shard_lifecycle_with_periodic_resync<L, E, X, F, S, Shutdown>(
    client: Client,
    namespace: &str,
    runtime: &WorkerShardOperatorRuntime<L, E, X>,
    input_for_shard: &mut F,
    mut events: Pin<&mut S>,
    mut shutdown: Pin<&mut Shutdown>,
    periodic_options: WorkerShardPeriodicResyncSchedule,
) -> Result<WorkerShardLifecycleExit, WorkerShardError>
where
    L: PartitionLeaseClient,
    E: WorkerShardEpochStore,
    X: WorkerShardScopedCommandExecutor,
    F: FnMut(&VelorixWorkerShard) -> WorkerShardReconcileInput + Send,
    S: Stream<Item = Result<WorkerShardEvent, WorkerShardError>>,
    Shutdown: Future<Output = ()>,
{
    let mut interval = time::interval_at(
        Instant::now() + periodic_options.interval,
        periodic_options.interval,
    );
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        if shutdown_ready(shutdown.as_mut()) {
            return Ok(WorkerShardLifecycleExit::Shutdown);
        }

        let mut next_event_stream = events.as_mut();
        let event = next_event_stream.next();
        let tick = interval.tick();
        tokio::pin!(event);
        tokio::pin!(tick);

        tokio::select! {
            biased;
            _ = shutdown.as_mut() => return Ok(WorkerShardLifecycleExit::Shutdown),
            _ = &mut tick => {
                resync_worker_shards_once_with_operator_runtime(
                    client.clone(),
                    namespace,
                    runtime,
                    &mut *input_for_shard,
                    periodic_options.resync.clone(),
                )
                .await?;
            }
            event = &mut event => match event {
                Some(Ok(event)) => {
                    let input = match &event {
                        WorkerShardEvent::Applied(shard) | WorkerShardEvent::Deleted(shard) => {
                            input_for_shard(shard)
                        }
                    };
                    runtime.handle_event(event, input).await?;
                    tokio::task::yield_now().await;
                }
                Some(Err(error)) => return Err(error),
                None => return Ok(WorkerShardLifecycleExit::WatchEnded),
            },
        }
    }
}

async fn run_worker_shard_lifecycle_without_periodic_resync<L, E, X, F, S, Shutdown>(
    runtime: &WorkerShardOperatorRuntime<L, E, X>,
    input_for_shard: &mut F,
    mut events: Pin<&mut S>,
    mut shutdown: Pin<&mut Shutdown>,
) -> Result<WorkerShardLifecycleExit, WorkerShardError>
where
    L: PartitionLeaseClient,
    E: WorkerShardEpochStore,
    X: WorkerShardScopedCommandExecutor,
    F: FnMut(&VelorixWorkerShard) -> WorkerShardReconcileInput + Send,
    S: Stream<Item = Result<WorkerShardEvent, WorkerShardError>>,
    Shutdown: Future<Output = ()>,
{
    loop {
        if shutdown_ready(shutdown.as_mut()) {
            return Ok(WorkerShardLifecycleExit::Shutdown);
        }

        let mut next_event_stream = events.as_mut();
        let event = next_event_stream.next();
        tokio::pin!(event);

        tokio::select! {
            biased;
            _ = shutdown.as_mut() => return Ok(WorkerShardLifecycleExit::Shutdown),
            event = &mut event => match event {
                Some(Ok(event)) => {
                    let input = match &event {
                        WorkerShardEvent::Applied(shard) | WorkerShardEvent::Deleted(shard) => {
                            input_for_shard(shard)
                        }
                    };
                    runtime.handle_event(event, input).await?;
                }
                Some(Err(error)) => return Err(error),
                None => return Ok(WorkerShardLifecycleExit::WatchEnded),
            },
        }
    }
}

fn worker_shard_resync_order(
    left: &VelorixWorkerShard,
    right: &VelorixWorkerShard,
) -> std::cmp::Ordering {
    (
        left.namespace().unwrap_or_default(),
        left.name_any(),
        left.spec.view_id.as_str(),
        left.spec.stream_id.as_str(),
        left.spec.partition_id,
    )
        .cmp(&(
            right.namespace().unwrap_or_default(),
            right.name_any(),
            right.spec.view_id.as_str(),
            right.spec.stream_id.as_str(),
            right.spec.partition_id,
        ))
}

pub async fn watch_worker_shards_with_kubernetes_runtime(
    client: Client,
    namespace: &str,
    startup_components: &OperatorAuthorityStartupComponents,
    pod_template: WorkerShardPodTemplate,
    input_for_shard: impl FnMut(&VelorixWorkerShard) -> WorkerShardReconcileInput + Send,
) -> Result<(), WorkerShardError> {
    let runtime = build_kubernetes_worker_shard_operator_runtime(
        client.clone(),
        namespace,
        startup_components,
        pod_template,
    )?;
    watch_worker_shards_with_operator_runtime(client, namespace, runtime, input_for_shard).await
}

pub async fn resync_worker_shards_before_watch_with_kubernetes_runtime(
    client: Client,
    namespace: &str,
    startup_components: &OperatorAuthorityStartupComponents,
    pod_template: WorkerShardPodTemplate,
    input_for_shard: impl FnMut(&VelorixWorkerShard) -> WorkerShardReconcileInput + Send,
    resync_options: WorkerShardResyncOptions,
) -> Result<
    (
        KubernetesWorkerShardOperatorRuntime,
        WorkerShardResyncSummary,
    ),
    WorkerShardError,
> {
    let runtime = build_kubernetes_worker_shard_operator_runtime(
        client.clone(),
        namespace,
        startup_components,
        pod_template,
    )?;
    let summary = resync_worker_shards_once_with_operator_runtime(
        client,
        namespace,
        &runtime,
        input_for_shard,
        resync_options,
    )
    .await?;

    Ok((runtime, summary))
}

pub async fn resync_worker_shards_periodically_with_kubernetes_runtime(
    client: Client,
    namespace: &str,
    startup_components: &OperatorAuthorityStartupComponents,
    pod_template: WorkerShardPodTemplate,
    input_for_shard: impl FnMut(&VelorixWorkerShard) -> WorkerShardReconcileInput + Send,
    periodic_options: WorkerShardPeriodicResyncOptions,
) -> Result<Vec<WorkerShardResyncSummary>, WorkerShardError> {
    let runtime = build_kubernetes_worker_shard_operator_runtime(
        client.clone(),
        namespace,
        startup_components,
        pod_template,
    )?;
    resync_worker_shards_periodically_with_operator_runtime(
        client,
        namespace,
        &runtime,
        input_for_shard,
        periodic_options,
    )
    .await
}

pub async fn run_worker_shards_with_kubernetes_runtime_lifecycle<Shutdown>(
    client: Client,
    namespace: &str,
    startup_components: &OperatorAuthorityStartupComponents,
    pod_template: WorkerShardPodTemplate,
    input_for_shard: impl FnMut(&VelorixWorkerShard) -> WorkerShardReconcileInput + Send,
    lifecycle_options: WorkerShardLifecycleOptions,
    shutdown: Shutdown,
) -> Result<WorkerShardLifecycleExit, WorkerShardError>
where
    Shutdown: Future<Output = ()>,
{
    lifecycle_options.validate()?;
    let runtime = build_kubernetes_worker_shard_operator_runtime(
        client.clone(),
        namespace,
        startup_components,
        pod_template,
    )?;
    let api: Api<VelorixWorkerShard> = Api::namespaced(client.clone(), namespace);
    let events = watcher::watcher(api, watcher::Config::default()).filter_map(|event| async {
        match event {
            Ok(event) => worker_shard_watch_event(event).map(Ok),
            Err(error) => Some(Err(WorkerShardError::watcher(error))),
        }
    });

    run_worker_shard_lifecycle_with_operator_event_stream(
        client,
        namespace,
        &runtime,
        input_for_shard,
        lifecycle_options,
        events,
        shutdown,
    )
    .await
}

fn shutdown_ready<Shutdown>(shutdown: Pin<&mut Shutdown>) -> bool
where
    Shutdown: Future<Output = ()>,
{
    shutdown.now_or_never().is_some()
}

pub async fn watch_worker_shards_with_kubernetes_runtime_after_initial_resync(
    client: Client,
    namespace: &str,
    startup_components: &OperatorAuthorityStartupComponents,
    pod_template: WorkerShardPodTemplate,
    mut input_for_shard: impl FnMut(&VelorixWorkerShard) -> WorkerShardReconcileInput + Send,
    resync_options: WorkerShardResyncOptions,
) -> Result<(), WorkerShardError> {
    let (runtime, _) = resync_worker_shards_before_watch_with_kubernetes_runtime(
        client.clone(),
        namespace,
        startup_components,
        pod_template,
        &mut input_for_shard,
        resync_options,
    )
    .await?;
    watch_worker_shards_with_operator_runtime(client, namespace, runtime, input_for_shard).await
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

pub fn runtime_identity_from_worker_shard(
    shard: &VelorixWorkerShard,
    owner_id: &str,
    owner_epoch: u64,
) -> Result<WorkerShardRuntimeIdentity, WorkerShardError> {
    let key = partition_lease_key_from_worker_shard(shard)?;
    Ok(WorkerShardRuntimeIdentity {
        namespace: key.namespace,
        view_id: key.view_id,
        stream_id: key.stream_id,
        partition_id: key.partition_id,
        authority: shard.spec.authority.clone(),
        lease_identity: partition_lease_identity(&PartitionLeaseKey {
            namespace: shard
                .namespace()
                .ok_or(WorkerShardError::MissingObjectField {
                    field: "metadata.namespace",
                })?,
            view_id: shard.spec.view_id.clone(),
            stream_id: shard.spec.stream_id.clone(),
            partition_id: shard.spec.partition_id,
        }),
        owner_id: owner_id.to_string(),
        owner_epoch,
    })
}

fn validate_worker_shard_authority(
    shard: &VelorixWorkerShard,
    expected_authority: Option<&ObjectStoreAuthorityRef>,
) -> Result<(), WorkerShardError> {
    let Some(expected_authority) = expected_authority else {
        return Ok(());
    };
    if &shard.spec.authority == expected_authority {
        Ok(())
    } else {
        Err(WorkerShardError::AuthorityMismatch {
            actual: shard.spec.authority.clone(),
            expected: expected_authority.clone(),
        })
    }
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
            ReconcileAction::RenewLease { owner_id } => {
                let request = LeaseAcquireRequest {
                    key: key.clone(),
                    owner_id,
                    now_unix_ms: input.now_unix_ms,
                    ttl_ms: input.ttl_ms,
                };
                if let Err(err) = lease_client.acquire_or_renew(request).await {
                    output.command_error = Some(err.to_string());
                }
            }
        }
    }

    Ok(())
}

fn append_non_acquire_commands(plan: &ReconcilePlan, output: &mut WorkerShardReconcileOutput) {
    for action in &plan.actions {
        match action {
            ReconcileAction::AcquireLease { .. } => {}
            ReconcileAction::RenewLease { .. } => {}
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

    fn kubernetes_api(error: kube::Error) -> Self {
        Self::KubernetesApi {
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
