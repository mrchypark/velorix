use std::{collections::BTreeMap, fmt};

use async_trait::async_trait;
use bytes::Bytes;
use k8s_openapi::{
    api::core::v1::{Container, EnvVar, Pod, PodSpec, Volume, VolumeMount},
    apimachinery::pkg::apis::meta::v1::ObjectMeta,
};
use kube::{
    api::{Api, PostParams},
    Client,
};
use thiserror::Error;
use velorix_storage::log::{
    AppendValidatedEnvelopeOutcome, IngestAdmissionCoordinator,
    IngestAdmissionReconstructionReport, IngestCommitGuard,
};

use crate::{
    crd::ObjectStoreAuthorityRef, startup::OperatorAuthorityStartupComponents,
    stream_watch::StreamWatchError,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IngestWriterRuntimeIdentity {
    pub namespace: String,
    pub authority: ObjectStoreAuthorityRef,
    pub operator_id: String,
    pub writer_id: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct IngestWriterPodTemplate {
    image: String,
    command: Option<Vec<String>>,
    args: Option<Vec<String>>,
    env: Vec<EnvVar>,
    volume_mounts: Vec<VolumeMount>,
    volumes: Vec<Volume>,
    labels: BTreeMap<String, String>,
    service_account_name: Option<String>,
}

impl IngestWriterPodTemplate {
    pub fn new(image: impl Into<String>) -> Result<Self, IngestWriterPodError> {
        let image = image.into();
        if image.trim().is_empty() {
            return Err(IngestWriterPodError::InvalidTemplate {
                message: "ingest writer pod image must not be empty".to_string(),
            });
        }

        Ok(Self {
            image,
            command: None,
            args: None,
            env: Vec::new(),
            volume_mounts: Vec::new(),
            volumes: Vec::new(),
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

    pub fn with_checked_append_entrypoint(
        self,
        payload_file: impl Into<String>,
    ) -> Result<Self, IngestWriterPodError> {
        let payload_file = payload_file.into();
        if payload_file.trim().is_empty() {
            return Err(IngestWriterPodError::InvalidTemplate {
                message: "ingest writer payload file must not be empty".to_string(),
            });
        }

        Ok(self
            .with_command(["/usr/local/bin/velorix-ingest-writer-entrypoint"])
            .with_env_var(env_var("VELORIX_INGEST_WRITER_PAYLOAD_FILE", &payload_file)))
    }

    pub fn with_env_var(mut self, env: EnvVar) -> Self {
        self.env.push(env);
        self
    }

    pub fn with_volume_mount(mut self, mount: VolumeMount) -> Self {
        self.volume_mounts.push(mount);
        self
    }

    pub fn with_volume(mut self, volume: Volume) -> Self {
        self.volumes.push(volume);
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

    pub fn pod_for_identity(&self, identity: &IngestWriterRuntimeIdentity) -> Pod {
        Pod {
            metadata: ObjectMeta {
                name: Some(ingest_writer_pod_name_for_identity(identity)),
                labels: Some(self.labels_for_identity(identity)),
                ..ObjectMeta::default()
            },
            spec: Some(PodSpec {
                containers: vec![Container {
                    name: "velorix-ingest-writer".to_string(),
                    image: Some(self.image.clone()),
                    command: self.command.clone(),
                    args: self.args.clone(),
                    env: Some(self.env_for_identity(identity)),
                    volume_mounts: if self.volume_mounts.is_empty() {
                        None
                    } else {
                        Some(self.volume_mounts.clone())
                    },
                    ..Container::default()
                }],
                restart_policy: Some("Never".to_string()),
                service_account_name: self.service_account_name.clone(),
                volumes: if self.volumes.is_empty() {
                    None
                } else {
                    Some(self.volumes.clone())
                },
                ..PodSpec::default()
            }),
            status: None,
        }
    }

    fn labels_for_identity(
        &self,
        identity: &IngestWriterRuntimeIdentity,
    ) -> BTreeMap<String, String> {
        let mut labels = self.labels.clone();
        labels.insert(
            "app.kubernetes.io/name".to_string(),
            "velorix-ingest-writer".to_string(),
        );
        labels.insert(
            "app.kubernetes.io/component".to_string(),
            "ingest-writer".to_string(),
        );
        labels.insert(
            "control.velorix.io/operator-id".to_string(),
            dns_label_fragment(&identity.operator_id, 63),
        );
        labels.insert(
            "control.velorix.io/writer-id".to_string(),
            dns_label_fragment(&identity.writer_id, 63),
        );
        labels.insert(
            "control.velorix.io/authority-store-id".to_string(),
            dns_label_fragment(&identity.authority.store_id, 63),
        );
        labels.insert(
            "control.velorix.io/authority-namespace".to_string(),
            dns_label_fragment(&identity.authority.namespace, 63),
        );
        labels.insert(
            "control.velorix.io/identity-hash".to_string(),
            ingest_writer_identity_hash(identity),
        );
        labels
    }

    fn env_for_identity(&self, identity: &IngestWriterRuntimeIdentity) -> Vec<EnvVar> {
        let mut env = env_for_identity(identity);
        env.extend(self.env.clone());
        env
    }
}

#[derive(Clone, Debug)]
pub struct KubernetesPodIngestWriterExecutor {
    pods: Api<Pod>,
    template: IngestWriterPodTemplate,
    identity: IngestWriterRuntimeIdentity,
}

impl KubernetesPodIngestWriterExecutor {
    pub fn new(
        client: Client,
        namespace: &str,
        template: IngestWriterPodTemplate,
        identity: IngestWriterRuntimeIdentity,
    ) -> Self {
        Self {
            pods: Api::namespaced(client, namespace),
            template,
            identity,
        }
    }

    pub fn from_api(
        pods: Api<Pod>,
        template: IngestWriterPodTemplate,
        identity: IngestWriterRuntimeIdentity,
    ) -> Self {
        Self {
            pods,
            template,
            identity,
        }
    }

    pub fn identity(&self) -> &IngestWriterRuntimeIdentity {
        &self.identity
    }

    pub fn pod_name(&self) -> String {
        ingest_writer_pod_name_for_identity(&self.identity)
    }

    pub async fn create_writer_pod(&self) -> Result<(), IngestWriterPodError> {
        let pod = self.template.pod_for_identity(&self.identity);
        match self.pods.create(&PostParams::default(), &pod).await {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(response)) if response.code == 409 => {
                let pod_name = self.pod_name();
                let existing = self.pods.get(&pod_name).await.map_err(|error| {
                    IngestWriterPodError::KubernetesApi {
                        action: "read-existing".to_string(),
                        pod_name: pod_name.clone(),
                        message: error.to_string(),
                    }
                })?;
                if pod_matches_identity(&existing, &self.template, &self.identity) {
                    Ok(())
                } else {
                    Err(IngestWriterPodError::IdentityMismatch { pod_name })
                }
            }
            Err(error) => Err(IngestWriterPodError::KubernetesApi {
                action: "create".to_string(),
                pod_name: self.pod_name(),
                message: error.to_string(),
            }),
        }
    }
}

#[derive(Debug, Error)]
pub enum IngestWriterPodError {
    #[error("{message}")]
    InvalidTemplate { message: String },
    #[error("kubernetes ingest writer pod {action} failed for `{pod_name}`: {message}")]
    KubernetesApi {
        action: String,
        pod_name: String,
        message: String,
    },
    #[error("kubernetes ingest writer pod identity mismatch for `{pod_name}`")]
    IdentityMismatch { pod_name: String },
}

#[async_trait]
pub trait IngestWriterPodExecutor: Send + Sync {
    async fn create_writer_pod(&self) -> Result<(), IngestWriterPodError>;
}

#[async_trait]
impl IngestWriterPodExecutor for KubernetesPodIngestWriterExecutor {
    async fn create_writer_pod(&self) -> Result<(), IngestWriterPodError> {
        KubernetesPodIngestWriterExecutor::create_writer_pod(self).await
    }
}

#[derive(Debug)]
pub struct KubernetesIngestWriterOperatorRuntime {
    deployed_runtime: DeployedIngestWriterRuntime,
    pod_executor: KubernetesPodIngestWriterExecutor,
}

impl KubernetesIngestWriterOperatorRuntime {
    pub fn deployed_runtime(&self) -> &DeployedIngestWriterRuntime {
        &self.deployed_runtime
    }

    pub fn pod_executor(&self) -> &KubernetesPodIngestWriterExecutor {
        &self.pod_executor
    }
}

pub async fn build_kubernetes_ingest_writer_operator_runtime(
    client: Client,
    namespace: &str,
    startup_components: &OperatorAuthorityStartupComponents,
    pod_template: IngestWriterPodTemplate,
    operator_id: impl Into<String>,
    writer_id: impl Into<String>,
) -> Result<KubernetesIngestWriterOperatorRuntime, StreamWatchError> {
    let deployed_runtime =
        DeployedIngestWriterRuntime::from_startup_components(startup_components).await?;
    let identity = IngestWriterRuntimeIdentity {
        namespace: namespace.to_string(),
        authority: startup_components.authority().clone(),
        operator_id: operator_id.into(),
        writer_id: writer_id.into(),
    };
    let pod_executor =
        KubernetesPodIngestWriterExecutor::new(client, namespace, pod_template, identity);

    Ok(KubernetesIngestWriterOperatorRuntime {
        deployed_runtime,
        pod_executor,
    })
}

pub fn ingest_writer_pod_name_for_identity(identity: &IngestWriterRuntimeIdentity) -> String {
    let prefix = "velorix-ingest-writer";
    let hash = ingest_writer_identity_hash(identity);
    let max_fragment_len = 63usize
        .saturating_sub(prefix.len())
        .saturating_sub(hash.len())
        .saturating_sub(2);
    let fragment = dns_label_fragment(
        &format!("{}-{}", identity.operator_id, identity.writer_id),
        max_fragment_len.max(1),
    );
    format!("{prefix}-{fragment}-{hash}")
}

fn env_var(name: &str, value: &str) -> EnvVar {
    EnvVar {
        name: name.to_string(),
        value: Some(value.to_string()),
        value_from: None,
    }
}

fn env_for_identity(identity: &IngestWriterRuntimeIdentity) -> Vec<EnvVar> {
    vec![
        env_var("VELORIX_INGEST_WRITER_NAMESPACE", &identity.namespace),
        env_var(
            "VELORIX_INGEST_WRITER_AUTHORITY_STORE_ID",
            &identity.authority.store_id,
        ),
        env_var(
            "VELORIX_INGEST_WRITER_AUTHORITY_NAMESPACE",
            &identity.authority.namespace,
        ),
        env_var("VELORIX_INGEST_WRITER_OPERATOR_ID", &identity.operator_id),
        env_var("VELORIX_INGEST_WRITER_ID", &identity.writer_id),
    ]
}

fn pod_matches_identity(
    pod: &Pod,
    template: &IngestWriterPodTemplate,
    identity: &IngestWriterRuntimeIdentity,
) -> bool {
    let Some(metadata_name) = pod.metadata.name.as_deref() else {
        return false;
    };
    if metadata_name != ingest_writer_pod_name_for_identity(identity) {
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
        || container.volume_mounts
            != if template.volume_mounts.is_empty() {
                None
            } else {
                Some(template.volume_mounts.clone())
            }
    {
        return false;
    }

    if spec.volumes
        != if template.volumes.is_empty() {
            None
        } else {
            Some(template.volumes.clone())
        }
    {
        return false;
    }

    let expected_env = template.env_for_identity(identity);
    let Some(env) = container.env.as_ref() else {
        return false;
    };
    expected_env
        .iter()
        .all(|expected| env.iter().any(|actual| actual == expected))
}

fn ingest_writer_identity_hash(identity: &IngestWriterRuntimeIdentity) -> String {
    stable_hash8(&format!(
        "{}/{}/{}/{}/{}",
        identity.namespace,
        identity.authority.store_id,
        identity.authority.namespace,
        identity.operator_id,
        identity.writer_id,
    ))
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

pub struct DeployedIngestWriterRuntime {
    authority: ObjectStoreAuthorityRef,
    coordinator: IngestAdmissionCoordinator,
    startup_report: IngestAdmissionReconstructionReport,
}

impl fmt::Debug for DeployedIngestWriterRuntime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeployedIngestWriterRuntime")
            .field("authority", &self.authority)
            .field("startup_report", &self.startup_report)
            .finish_non_exhaustive()
    }
}

impl DeployedIngestWriterRuntime {
    pub async fn from_startup_components(
        startup_components: &OperatorAuthorityStartupComponents,
    ) -> Result<Self, StreamWatchError> {
        let provider = startup_components.ingest_admission_coordinator_provider();
        let (coordinator, startup_report) =
            provider.coordinator_after_startup_reconstruction().await?;

        Ok(Self {
            authority: startup_components.authority().clone(),
            coordinator,
            startup_report,
        })
    }

    pub fn from_startup_components_without_reconstruction(
        startup_components: &OperatorAuthorityStartupComponents,
    ) -> Result<Self, StreamWatchError> {
        let provider = startup_components.ingest_admission_coordinator_provider();
        let coordinator = provider.coordinator_without_startup_reconstruction()?;
        let startup_report = IngestAdmissionReconstructionReport {
            active_admission_records: 0,
            expired_orphan_admission_records: 0,
        };

        Ok(Self {
            authority: startup_components.authority().clone(),
            coordinator,
            startup_report,
        })
    }

    pub fn authority(&self) -> &ObjectStoreAuthorityRef {
        &self.authority
    }

    pub fn startup_report(&self) -> &IngestAdmissionReconstructionReport {
        &self.startup_report
    }

    pub async fn append_catalog_validated_envelope(
        &self,
        payload: Bytes,
    ) -> Result<AppendValidatedEnvelopeOutcome, StreamWatchError> {
        self.coordinator
            .append_catalog_validated_envelope(payload)
            .await
            .map_err(|error| StreamWatchError::snapshot(error.to_string()))
    }

    pub async fn append_catalog_validated_envelope_with_commit_guard(
        &self,
        payload: Bytes,
        commit_guard: &dyn IngestCommitGuard,
    ) -> Result<AppendValidatedEnvelopeOutcome, StreamWatchError> {
        self.coordinator
            .append_catalog_validated_envelope_with_commit_guard(payload, commit_guard)
            .await
            .map_err(|error| StreamWatchError::snapshot(error.to_string()))
    }

    pub async fn append_catalog_validated_envelope_after_external_admission(
        &self,
        payload: Bytes,
    ) -> Result<AppendValidatedEnvelopeOutcome, StreamWatchError> {
        self.coordinator
            .append_catalog_validated_envelope_after_external_admission(payload)
            .await
            .map_err(|error| StreamWatchError::snapshot(error.to_string()))
    }

    pub async fn append_validated_envelope_after_external_admission(
        &self,
        payload: Bytes,
    ) -> Result<AppendValidatedEnvelopeOutcome, StreamWatchError> {
        self.coordinator
            .append_validated_envelope_after_external_admission(payload)
            .await
            .map_err(|error| StreamWatchError::snapshot(error.to_string()))
    }
}
