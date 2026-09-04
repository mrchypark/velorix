#![forbid(unsafe_code)]

use std::{
    env, fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{bail, Context};
use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use bytes::Bytes;
use clap::{Parser, Subcommand};
use kube::Client;
use object_store::{
    aws::AmazonS3Builder, path::Path as ObjectStorePath, prefix::PrefixStore, ObjectStore,
    ObjectStoreExt,
};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{watch, Mutex, RwLock},
    task::JoinHandle,
};
use velorix_control::lease::{
    LeaseAcquireRequest, PartitionLeaseClient, PartitionLeaseGrant, PartitionLeaseKey,
};
use velorix_k8s::{
    crd::ObjectStoreAuthorityRef,
    ingest_writer::DeployedIngestWriterRuntime,
    lease::{partition_lease_identity, KubeLeaseApi, KubernetesPartitionLeaseClient},
    startup::{validate_operator_authority, OperatorAuthorityStartupComponents},
};
use velorix_meta::{
    AcquirePartitionAuthorityOutcome, AcquirePartitionAuthorityRequest, GrpcMetaStore, MetaStore,
    PartitionAuthorityKey, PartitionAuthorityToken, PartitionCheckpointPointer,
    PublishPartitionCheckpointPointerRequest,
};
use velorix_storage::{
    ingest_envelope::{IngestEnvelope, IngestEnvelopeEncodeRequest},
    log::{
        AppendValidatedEnvelopeOutcome, DurableIngestAdmissionRecordV1, IngestBatch,
        IngestBatchDescriptor, IngestCommitGuard, IngestCommitGuardBindingV1,
        IngestCommitGuardPhase,
    },
    object_key::ObjectKey,
};

#[derive(Debug, Parser)]
#[command(name = "velorix-ingest-writer")]
#[command(about = "Bounded Velorix ingest-writer runtime")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Append {
        #[arg(long)]
        payload_file: PathBuf,
        #[arg(long)]
        authority_store_id: String,
        #[arg(long)]
        authority_namespace: String,
        #[arg(long)]
        operator_id: String,
        #[arg(long)]
        writer_id: String,
        #[arg(long)]
        json: bool,
    },
    EncodeDefaultScoresPayload {
        #[arg(long)]
        output: PathBuf,
        #[arg(long)]
        schema_fingerprint: String,
        #[arg(long, default_value = "scores")]
        stream_id: String,
        #[arg(long, default_value_t = 0)]
        partition_id: u32,
        #[arg(long)]
        start_offset_inclusive: u64,
        #[arg(long)]
        rows_json: String,
    },
    #[command(name = "probe-kubernetes-lease-handoff", alias = "lease-handoff-probe")]
    ProbeKubernetesLeaseHandoff {
        #[arg(long)]
        namespace: String,
        #[arg(long, default_value = "ingest-writer-lifecycle")]
        view_id: String,
        #[arg(long, default_value = "lease-handoff")]
        stream_id: String,
        #[arg(long, default_value_t = 0)]
        partition_id: u32,
        #[arg(long, default_value = "ingest-writer-handoff-owner-a")]
        owner_a: String,
        #[arg(long, default_value = "ingest-writer-handoff-owner-b")]
        owner_b: String,
        #[arg(long, default_value_t = 60_000)]
        ttl_ms: u64,
        #[arg(long)]
        json: bool,
    },
    #[command(name = "probe-kubernetes-lease-acquire")]
    ProbeKubernetesLeaseAcquire {
        #[arg(long)]
        namespace: String,
        #[arg(long, default_value = "ingest-writer-lifecycle")]
        view_id: String,
        #[arg(long, default_value = "lease-handoff")]
        stream_id: String,
        #[arg(long, default_value_t = 0)]
        partition_id: u32,
        #[arg(long)]
        owner_id: String,
        #[arg(long, default_value_t = 60_000)]
        ttl_ms: u64,
        #[arg(long)]
        json: bool,
    },
    #[command(name = "lease-guarded-append", alias = "probe-lease-guarded-append")]
    ProbeLeaseGuardedAppend {
        #[arg(long)]
        payload_file: PathBuf,
        #[arg(long)]
        authority_store_id: String,
        #[arg(long)]
        authority_namespace: String,
        #[arg(long)]
        operator_id: String,
        #[arg(long)]
        writer_id: String,
        #[arg(long)]
        lease_namespace: String,
        #[arg(long, default_value = "ingest-writer-lifecycle")]
        lease_view_id: String,
        #[arg(long, default_value = "lease-handoff")]
        lease_stream_id: String,
        #[arg(long, default_value_t = 0)]
        lease_partition_id: u32,
        #[arg(long)]
        owner_id: String,
        #[arg(long)]
        expected_owner_epoch: Option<u64>,
        #[arg(long, default_value_t = 60_000)]
        ttl_ms: u64,
        #[arg(long)]
        acquire_lease: bool,
        #[arg(long)]
        expected_outcome: String,
        #[arg(long)]
        json: bool,
    },
    #[command(name = "probe-meta-stale-token")]
    ProbeMetaStaleToken {
        #[arg(long)]
        namespace: String,
        #[arg(long, default_value = "ingest-writer-lifecycle")]
        view_id: String,
        #[arg(long, default_value = "scores")]
        stream_id: String,
        #[arg(long, default_value_t = 0)]
        partition_id: u32,
        #[arg(long, default_value_t = 2_000)]
        ttl_ms: u64,
        #[arg(long, default_value_t = 60_000)]
        takeover_ttl_ms: u64,
        #[arg(long)]
        json: bool,
    },
    #[command(name = "probe-ingest-admission-crash-restart")]
    ProbeIngestAdmissionCrashRestart {
        #[arg(long)]
        payload_file: PathBuf,
        #[arg(long)]
        authority_store_id: String,
        #[arg(long)]
        authority_namespace: String,
        #[arg(long)]
        operator_id: String,
        #[arg(long)]
        writer_id: String,
        #[arg(long)]
        json: bool,
    },
    #[command(name = "probe-lease-loss-during-reservation")]
    ProbeLeaseLossDuringReservation {
        #[arg(long)]
        payload_file: PathBuf,
        #[arg(long)]
        authority_store_id: String,
        #[arg(long)]
        authority_namespace: String,
        #[arg(long)]
        operator_id: String,
        #[arg(long)]
        writer_id: String,
        #[arg(long)]
        lease_namespace: String,
        #[arg(long, default_value = "ingest-writer-lifecycle")]
        lease_view_id: String,
        #[arg(long, default_value = "lease-handoff")]
        lease_stream_id: String,
        #[arg(long, default_value_t = 0)]
        lease_partition_id: u32,
        #[arg(long)]
        owner_id: String,
        #[arg(long, default_value_t = 60_000)]
        ttl_ms: u64,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct S3CompatibleAuthorityConfig {
    endpoint: String,
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
    region: String,
    bucket: String,
    prefix: String,
    allow_http: bool,
    force_path_style: bool,
}

#[derive(Debug)]
struct IngestWriterAppendRequest {
    authority_store_id: String,
    authority_namespace: String,
    operator_id: String,
    writer_id: String,
    payload: Bytes,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct IngestWriterAppendArtifactV1 {
    schema_version: u16,
    evidence_kind: String,
    status: String,
    authority_store_id: String,
    authority_namespace: String,
    operator_id: String,
    writer_id: String,
    startup_active_admission_records: usize,
    startup_expired_orphan_admission_records: usize,
    outcome: String,
    descriptor: IngestWriterAppendDescriptorV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct IngestWriterAppendDescriptorV1 {
    stream_id: String,
    partition_id: u32,
    start_offset_inclusive: u64,
    end_offset_exclusive: u64,
    object_key: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct IngestIdentity {
    stream_id: String,
    partition_id: u32,
    start_offset_inclusive: u64,
    end_offset_exclusive: u64,
    payload_digest: String,
    relation_id: String,
    relation_version: String,
    schema_fingerprint: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct LeaseHandoffProbeRequest {
    key: PartitionLeaseKey,
    owner_a: String,
    owner_b: String,
    ttl_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct LeaseHandoffProbeArtifactV1 {
    schema_version: u16,
    evidence_kind: String,
    status: String,
    namespace: String,
    view_id: String,
    stream_id: String,
    partition_id: u32,
    owner_a: String,
    owner_a_epoch: u64,
    owner_b: String,
    owner_b_epoch: u64,
    lease_identity: String,
    leader_handoff_checked: bool,
    product_complete_eligible: bool,
    handoff_model: String,
    lease: LeaseHandoffProbeLeaseV1,
    acquire_owner_a: LeaseGrantEvidenceV1,
    release_owner_a: LeaseReleaseEvidenceV1,
    acquire_owner_b: LeaseGrantEvidenceV1,
    verified_current_owner: LeaseGrantEvidenceV1,
    best_effort_release_owner_b: LeaseReleaseEvidenceV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct LeaseHandoffProbeLeaseV1 {
    lease_identity: String,
    namespace: String,
    view_id: String,
    stream_id: String,
    partition_id: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct LeaseGrantEvidenceV1 {
    owner_id: String,
    owner_epoch: u64,
    expires_at_unix_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct LeaseReleaseEvidenceV1 {
    owner_id: String,
    owner_epoch: u64,
    status: String,
    error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct KubernetesLeaseAcquireProbeRequest {
    key: PartitionLeaseKey,
    owner_id: String,
    ttl_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct KubernetesLeaseAcquireProbeArtifactV1 {
    schema_version: u16,
    evidence_kind: String,
    status: String,
    lease_identity: String,
    namespace: String,
    view_id: String,
    stream_id: String,
    partition_id: u32,
    owner_id: String,
    owner_epoch: u64,
    expires_at_unix_ms: u64,
    released: bool,
}

#[derive(Debug)]
struct LeaseGuardedAppendProbeRequest {
    authority_store_id: String,
    authority_namespace: String,
    operator_id: String,
    writer_id: String,
    payload: Bytes,
    lease_key: PartitionLeaseKey,
    owner_id: String,
    expected_owner_epoch: Option<u64>,
    ttl_ms: u64,
    acquire_lease: bool,
    expected_outcome: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct LeaseGuardedAppendProbeArtifactV1 {
    schema_version: u16,
    evidence_kind: String,
    status: String,
    expected_outcome: String,
    outcome: String,
    authority_store_id: String,
    authority_namespace: String,
    operator_id: String,
    writer_id: String,
    lease_identity: String,
    owner_id: String,
    expected_owner_epoch: Option<u64>,
    acquired_grant: Option<LeaseGrantEvidenceV1>,
    current_owner: Option<LeaseGrantEvidenceV1>,
    post_append_current_owner: Option<LeaseGrantEvidenceV1>,
    commit_guard_enforced: bool,
    admission_commit_guard_bound: bool,
    admission_commit_guard_binding: Option<IngestCommitGuardBindingV1>,
    lease_held_through_append: bool,
    stale_owner_rejected: bool,
    append_completed: bool,
    descriptor: IngestWriterAppendDescriptorV1,
}

/// A fail-closed, Meta-backed authority session for one ingest partition.
///
/// Kubernetes leases are deliberately not consulted here: they may help a
/// scheduler decide where to run a writer, but only Meta owns the authority
/// epoch and expiry used to admit durable ingest work.
#[derive(Clone)]
struct PartitionAuthoritySession {
    meta_store: Arc<dyn MetaStore>,
    key: PartitionAuthorityKey,
    owner_id: String,
    token: Arc<RwLock<PartitionAuthorityToken>>,
    admission_binding: IngestCommitGuardBindingV1,
    fenced: Arc<AtomicBool>,
    safety_deadline: Arc<RwLock<Instant>>,
    cancellation: watch::Sender<bool>,
    renewal_task: Arc<Mutex<Option<JoinHandle<()>>>>,
    ttl: Duration,
    rpc_timeout: Duration,
}

impl PartitionAuthoritySession {
    async fn acquire(
        meta_store: Arc<dyn MetaStore>,
        key: PartitionAuthorityKey,
        owner_id: String,
        ttl_ms: u64,
    ) -> anyhow::Result<Self> {
        if ttl_ms < 3 {
            bail!("partition authority ttl-ms must be at least 3");
        }
        if owner_id.trim().is_empty() {
            bail!("partition authority requires a non-empty owner id");
        }
        if key.namespace.trim().is_empty()
            || key.view_id.trim().is_empty()
            || key.stream_id.trim().is_empty()
        {
            bail!("partition authority namespace, view-id, and stream-id must be non-empty");
        }

        let rpc_timeout = Duration::from_millis((ttl_ms / 3).clamp(1, 5_000));
        let capability_started = Instant::now();
        let capability = tokio::time::timeout(
            rpc_timeout,
            meta_store.read_partition_authority_capability(),
        )
        .await
        .context("metadata capability check timed out")??;
        if Instant::now() >= authority_safety_deadline(capability_started, ttl_ms) {
            bail!("partition authority capability check exceeded local safety deadline");
        }
        if !capability.production_safe {
            bail!(
                "Meta partition authority backend `{}` is not production_safe",
                capability.backend_name
            );
        }

        let acquire_started = Instant::now();
        let acquire_deadline = authority_safety_deadline(acquire_started, ttl_ms);
        let token = tokio::time::timeout(
            rpc_timeout,
            meta_store.acquire_partition_authority(AcquirePartitionAuthorityRequest {
                key: key.clone(),
                owner_id: owner_id.clone(),
                current_token: None,
                ttl_ms,
            }),
        )
        .await
        .context("partition authority acquire timed out")??;
        if Instant::now() >= acquire_deadline {
            bail!("partition authority acquire response arrived after local safety deadline");
        }
        let token = match token {
            AcquirePartitionAuthorityOutcome::Acquired(token) => token,
            AcquirePartitionAuthorityOutcome::Renewed(_) => {
                bail!("partition authority acquire unexpectedly returned a renewal")
            }
            AcquirePartitionAuthorityOutcome::Conflict(current) => {
                bail!(
                    "partition authority is held by {}#{} for {}/{}/{}/p{}",
                    current.owner_id,
                    current.owner_epoch,
                    current.key.namespace,
                    current.key.view_id,
                    current.key.stream_id,
                    current.key.partition_id
                )
            }
        };
        validate_partition_authority_token(&token, &key, &owner_id)?;
        let admission_binding = IngestCommitGuardBindingV1::new(
            "meta_partition_authority",
            partition_authority_identity(&token.key),
            token.owner_id.clone(),
            token.owner_epoch,
        );

        let (cancellation, receiver) = watch::channel(false);
        let session = Self {
            meta_store,
            key,
            owner_id,
            token: Arc::new(RwLock::new(token)),
            admission_binding,
            fenced: Arc::new(AtomicBool::new(false)),
            safety_deadline: Arc::new(RwLock::new(acquire_deadline)),
            cancellation,
            renewal_task: Arc::new(Mutex::new(None)),
            ttl: Duration::from_millis(ttl_ms),
            rpc_timeout,
        };
        session.start_renewal(receiver).await;
        Ok(session)
    }

    async fn start_renewal(&self, mut cancellation: watch::Receiver<bool>) {
        let session = self.clone();
        let cadence = Duration::from_millis((session.ttl.as_millis() as u64 / 3).max(1));
        let task = tokio::spawn(async move {
            loop {
                if session
                    .ensure_local_safety("renewal scheduling resume")
                    .await
                    .is_err()
                {
                    return;
                }
                tokio::select! {
                    _ = tokio::time::sleep(cadence) => {},
                    changed = cancellation.changed() => {
                        if changed.is_err() || *cancellation.borrow() { return; }
                        continue;
                    }
                }
                if session.renew_once().await.is_err() {
                    return;
                }
            }
        });
        *self.renewal_task.lock().await = Some(task);
    }

    fn fence(&self) {
        self.fenced.store(true, Ordering::SeqCst);
        let _ = self.cancellation.send(true);
    }

    async fn shutdown(&self) {
        self.fence();
        if let Some(task) = self.renewal_task.lock().await.take() {
            let _ = task.await;
        }
        // Release is intentionally deferred: expiry is owned by Meta and a
        // shutdown must never turn a release failure into continued authority.
    }

    async fn renew_once(&self) -> Result<(), String> {
        let result = self.renew_once_inner().await;
        if result.is_err() {
            self.fence();
        }
        result
    }

    async fn renew_once_inner(&self) -> Result<(), String> {
        self.ensure_local_safety("renewal").await?;
        let current = self.token.read().await.clone();
        let renewal_started = Instant::now();
        let renewal_deadline =
            authority_safety_deadline(renewal_started, self.ttl.as_millis() as u64);
        let outcome = tokio::time::timeout(
            self.rpc_timeout,
            self.meta_store
                .acquire_partition_authority(AcquirePartitionAuthorityRequest {
                    key: self.key.clone(),
                    owner_id: self.owner_id.clone(),
                    current_token: Some(current.clone()),
                    ttl_ms: self.ttl.as_millis() as u64,
                }),
        )
        .await
        .map_err(|_| "partition authority renewal timed out".to_string())?
        .map_err(|error| format!("partition authority renewal failed: {error}"))?;
        if Instant::now() >= renewal_deadline {
            return Err(
                "partition authority renewal response arrived after local safety deadline"
                    .to_string(),
            );
        }
        let renewed = match outcome {
            AcquirePartitionAuthorityOutcome::Renewed(token) => token,
            AcquirePartitionAuthorityOutcome::Acquired(_) => {
                return Err(
                    "partition authority renewal unexpectedly acquired a new epoch".to_string(),
                )
            }
            AcquirePartitionAuthorityOutcome::Conflict(_) => {
                return Err("partition authority renewal lost ownership".to_string())
            }
        };
        validate_partition_authority_token(&renewed, &self.key, &self.owner_id)
            .map_err(|error| error.to_string())?;
        if renewed.owner_epoch != current.owner_epoch {
            return Err("partition authority renewal changed its owner epoch".to_string());
        }
        *self.token.write().await = renewed;
        *self.safety_deadline.write().await = renewal_deadline;
        Ok(())
    }

    async fn ensure_local_safety(&self, operation: &str) -> Result<(), String> {
        if self.fenced.load(Ordering::SeqCst) {
            return Err(format!(
                "partition authority session is fenced before {operation}"
            ));
        }
        if Instant::now() >= *self.safety_deadline.read().await {
            self.fence();
            return Err(format!(
                "partition authority local safety deadline elapsed before {operation}"
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl IngestCommitGuard for PartitionAuthoritySession {
    async fn verify(
        &self,
        phase: IngestCommitGuardPhase,
        descriptor: &IngestBatchDescriptor,
    ) -> Result<(), String> {
        self.ensure_local_safety(phase.as_str()).await?;
        if self.key.stream_id != descriptor.stream_id
            || self.key.partition_id != descriptor.partition_id
        {
            self.fence();
            return Err(format!(
                "partition authority key {}/p{} does not match descriptor {}/p{}",
                self.key.stream_id,
                self.key.partition_id,
                descriptor.stream_id,
                descriptor.partition_id
            ));
        }
        let expected = self.token.read().await.clone();
        let current = tokio::time::timeout(
            self.rpc_timeout,
            self.meta_store.read_partition_authority(&self.key),
        )
        .await;
        let current = match current {
            Ok(Ok(current)) => current,
            Ok(Err(error)) => {
                self.fence();
                return Err(format!(
                    "partition authority verification failed at {}: {error}",
                    phase.as_str()
                ));
            }
            Err(_) => {
                self.fence();
                return Err(format!(
                    "partition authority verification timed out at {}",
                    phase.as_str()
                ));
            }
        };
        if Instant::now() >= *self.safety_deadline.read().await {
            self.fence();
            return Err(format!(
                "partition authority local safety deadline elapsed during {}",
                phase.as_str()
            ));
        }
        if current.as_ref() != Some(&expected) {
            self.fence();
            return Err(format!(
                "partition authority token is stale at {}",
                phase.as_str()
            ));
        }
        // Meta filters expired authority records using its authoritative clock.
        // A writer's wall clock is deliberately not consulted here: it may be
        // skewed in either direction and must not grant or revoke authority.
        Ok(())
    }

    fn admission_binding(
        &self,
        _descriptor: &IngestBatchDescriptor,
    ) -> Option<IngestCommitGuardBindingV1> {
        Some(self.admission_binding.clone())
    }
}

fn authority_safety_deadline(rpc_started: Instant, ttl_ms: u64) -> Instant {
    // Meta controls the true expiry. This shorter monotonic deadline is only a
    // conservative local fail-closed bound while waiting for a renewal.
    rpc_started + Duration::from_millis((ttl_ms / 3).saturating_mul(2).max(1))
}

fn partition_authority_identity(key: &PartitionAuthorityKey) -> String {
    format!(
        "{}/{}/{}/p{}",
        key.namespace, key.view_id, key.stream_id, key.partition_id
    )
}

fn validate_partition_authority_token(
    token: &PartitionAuthorityToken,
    key: &PartitionAuthorityKey,
    owner_id: &str,
) -> anyhow::Result<()> {
    if token.key != *key
        || token.owner_id != owner_id
        || token.owner_epoch == 0
        || token.expires_at_unix_ms == 0
    {
        bail!("metadata returned a malformed or mismatched partition authority token");
    }
    Ok(())
}

async fn grpc_meta_store_from_env() -> anyhow::Result<Arc<dyn MetaStore>> {
    let endpoint = env::var("VELORIX_META_GRPC_ENDPOINT")
        .context("VELORIX_META_GRPC_ENDPOINT is required for lease-guarded append")?;
    if endpoint.trim().is_empty() {
        bail!("VELORIX_META_GRPC_ENDPOINT is required for lease-guarded append");
    }
    let store: Arc<dyn MetaStore> = match env::var("VELORIX_META_BEARER_TOKEN") {
        Ok(token) if !token.trim().is_empty() => {
            Arc::new(GrpcMetaStore::connect_with_bearer_token(&endpoint, token).await?)
        }
        Ok(_) => bail!("VELORIX_META_BEARER_TOKEN must not be empty when set"),
        Err(env::VarError::NotPresent) => Arc::new(GrpcMetaStore::connect(&endpoint).await?),
        Err(error) => {
            return Err(anyhow::Error::from(error).context("invalid VELORIX_META_BEARER_TOKEN"))
        }
    };
    Ok(store)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct MetaStaleTokenProbeArtifactV1 {
    schema_version: u16,
    evidence_kind: String,
    status: String,
    owner_a_epoch: u64,
    owner_b_epoch: u64,
    owner_b_epoch_higher: bool,
    before_admission_rejected: bool,
    before_commit_rejected: bool,
    stale_pointer_publish_rejected: bool,
    pointer_unchanged: bool,
    session_fenced: bool,
}

async fn run_meta_stale_token_probe(
    meta_store: Arc<dyn MetaStore>,
    key: PartitionAuthorityKey,
    ttl_ms: u64,
    takeover_ttl_ms: u64,
) -> anyhow::Result<MetaStaleTokenProbeArtifactV1> {
    if ttl_ms < 3 || takeover_ttl_ms < ttl_ms {
        bail!("stale-token probe requires ttl-ms >= 3 and takeover-ttl-ms >= ttl-ms");
    }
    let owner_a = "stale-token-probe-owner-a".to_string();
    let owner_b = "stale-token-probe-owner-b".to_string();
    let token_a = match meta_store
        .acquire_partition_authority(AcquirePartitionAuthorityRequest {
            key: key.clone(),
            owner_id: owner_a.clone(),
            current_token: None,
            ttl_ms,
        })
        .await?
    {
        AcquirePartitionAuthorityOutcome::Acquired(token) => token,
        AcquirePartitionAuthorityOutcome::Conflict(_)
        | AcquirePartitionAuthorityOutcome::Renewed(_) => {
            bail!("stale-token probe could not acquire owner A authority")
        }
    };
    tokio::time::sleep(Duration::from_millis(ttl_ms.saturating_add(250))).await;
    let token_b = match meta_store
        .acquire_partition_authority(AcquirePartitionAuthorityRequest {
            key: key.clone(),
            owner_id: owner_b.clone(),
            current_token: None,
            ttl_ms: takeover_ttl_ms,
        })
        .await?
    {
        AcquirePartitionAuthorityOutcome::Acquired(token) => token,
        AcquirePartitionAuthorityOutcome::Conflict(_)
        | AcquirePartitionAuthorityOutcome::Renewed(_) => {
            bail!("stale-token probe could not acquire owner B authority after expiry")
        }
    };
    if token_b.owner_epoch <= token_a.owner_epoch {
        bail!("stale-token probe owner B epoch did not advance");
    }
    let owner_a_epoch = token_a.owner_epoch;
    let (cancellation, _) = watch::channel(false);
    let stale_session = PartitionAuthoritySession {
        meta_store: Arc::clone(&meta_store),
        key: key.clone(),
        owner_id: owner_a.clone(),
        token: Arc::new(RwLock::new(token_a.clone())),
        admission_binding: IngestCommitGuardBindingV1::new(
            "meta_partition_authority",
            partition_authority_identity(&key),
            owner_a,
            token_a.owner_epoch,
        ),
        fenced: Arc::new(AtomicBool::new(false)),
        safety_deadline: Arc::new(RwLock::new(Instant::now() + Duration::from_secs(60))),
        cancellation,
        renewal_task: Arc::new(Mutex::new(None)),
        ttl: Duration::from_millis(takeover_ttl_ms),
        rpc_timeout: Duration::from_secs(5),
    };
    let descriptor = IngestBatchDescriptor {
        stream_id: key.stream_id.clone(),
        partition_id: key.partition_id,
        start_offset_inclusive: 0,
        end_offset_exclusive: 1,
        object_key: ObjectKey::ingest_batch(&key.stream_id, key.partition_id, 0, 1)?,
    };
    let before_admission_rejected = stale_session
        .verify(IngestCommitGuardPhase::BeforeAdmission, &descriptor)
        .await
        .is_err();
    let before_commit_rejected = stale_session
        .verify(IngestCommitGuardPhase::BeforeCommit, &descriptor)
        .await
        .is_err();
    let pointer_before = meta_store.read_partition_checkpoint_pointer(&key).await?;
    let stale_pointer_publish_rejected = meta_store
        .publish_partition_checkpoint_pointer(PublishPartitionCheckpointPointerRequest {
            expected_previous: pointer_before.clone(),
            candidate: PartitionCheckpointPointer {
                key: key.clone(),
                checkpoint_key: "v1/diagnostics/stale-token-pointer".to_string(),
            },
            authority: token_a,
        })
        .await
        .is_err();
    let pointer_unchanged =
        meta_store.read_partition_checkpoint_pointer(&key).await? == pointer_before;
    let session_fenced = stale_session.fenced.load(Ordering::SeqCst);
    stale_session.shutdown().await;
    if !before_admission_rejected
        || !before_commit_rejected
        || !stale_pointer_publish_rejected
        || !pointer_unchanged
        || !session_fenced
    {
        bail!("stale-token probe did not fail closed");
    }
    Ok(MetaStaleTokenProbeArtifactV1 {
        schema_version: 1,
        evidence_kind: "ingest_writer_meta_stale_token_probe".to_string(),
        status: "pass".to_string(),
        owner_a_epoch,
        owner_b_epoch: token_b.owner_epoch,
        owner_b_epoch_higher: true,
        before_admission_rejected,
        before_commit_rejected,
        stale_pointer_publish_rejected,
        pointer_unchanged,
        session_fenced,
    })
}

#[derive(Clone)]
struct LeaseLossDuringReservationCommitGuard {
    lease_client: KubernetesPartitionLeaseClient<KubeLeaseApi>,
    lease_key: PartitionLeaseKey,
    owner_id: String,
    owner_epoch: u64,
    before_admission_verified: Arc<AtomicBool>,
    released_before_commit: Arc<AtomicBool>,
}

#[async_trait]
impl IngestCommitGuard for LeaseLossDuringReservationCommitGuard {
    async fn verify(
        &self,
        phase: IngestCommitGuardPhase,
        descriptor: &IngestBatchDescriptor,
    ) -> Result<(), String> {
        if self.lease_key.stream_id != descriptor.stream_id
            || self.lease_key.partition_id != descriptor.partition_id
        {
            return Err(format!(
                "lease key {}/p{} does not match descriptor {}/p{}",
                self.lease_key.stream_id,
                self.lease_key.partition_id,
                descriptor.stream_id,
                descriptor.partition_id
            ));
        }

        match phase {
            IngestCommitGuardPhase::BeforeAdmission => {
                verify_kubernetes_lease_holder(
                    &self.lease_client,
                    &self.lease_key,
                    &self.owner_id,
                    self.owner_epoch,
                    phase,
                )
                .await?;
                self.before_admission_verified.store(true, Ordering::SeqCst);
                Ok(())
            }
            IngestCommitGuardPhase::BeforeCommit => {
                if !self.released_before_commit.swap(true, Ordering::SeqCst) {
                    self.lease_client
                        .release(
                            &self.lease_key,
                            &self.owner_id,
                            self.owner_epoch,
                            unix_ms().map_err(|error| error.to_string())?,
                        )
                        .await
                        .map_err(|error| error.to_string())?;
                }

                let current = self
                    .lease_client
                    .current(
                        &self.lease_key,
                        unix_ms().map_err(|error| error.to_string())?,
                    )
                    .await
                    .map_err(|error| error.to_string())?;
                if current.as_ref().map(|grant| {
                    grant.owner_id == self.owner_id && grant.owner_epoch == self.owner_epoch
                }) == Some(true)
                {
                    return Err(format!(
                        "lease still held at {} after forced release: {}#{}",
                        phase.as_str(),
                        self.owner_id,
                        self.owner_epoch
                    ));
                }

                Err(format!(
                    "lease intentionally lost at {} before batch commit: expected {}#{} current={:?}",
                    phase.as_str(),
                    self.owner_id,
                    self.owner_epoch,
                    current
                ))
            }
        }
    }

    fn admission_binding(
        &self,
        _descriptor: &IngestBatchDescriptor,
    ) -> Option<IngestCommitGuardBindingV1> {
        Some(IngestCommitGuardBindingV1::new(
            "kubernetes_partition_lease",
            partition_lease_identity(&self.lease_key),
            self.owner_id.clone(),
            self.owner_epoch,
        ))
    }
}

#[derive(Debug)]
struct IngestAdmissionCrashRestartProbeRequest {
    authority_store_id: String,
    authority_namespace: String,
    operator_id: String,
    writer_id: String,
    payload: Bytes,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct IngestAdmissionCrashRestartProbeArtifactV1 {
    schema_version: u16,
    evidence_kind: String,
    status: String,
    authority_store_id: String,
    authority_namespace: String,
    operator_id: String,
    writer_id: String,
    orphan_admission_created: bool,
    restart_reconstructed_active_admission: bool,
    recovered_append_completed: bool,
    committed_admission_not_expirable: bool,
    before_restart_active_admission_records: usize,
    after_restart_active_admission_records: usize,
    final_active_admission_records: usize,
    reserve_outcome: String,
    append_outcome: String,
    descriptor: IngestWriterAppendDescriptorV1,
    admission_record_key: String,
    batch_key: String,
}

#[derive(Debug)]
struct LeaseLossDuringReservationProbeRequest {
    authority_store_id: String,
    authority_namespace: String,
    operator_id: String,
    writer_id: String,
    payload: Bytes,
    lease_key: PartitionLeaseKey,
    owner_id: String,
    ttl_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct LeaseLossDuringReservationProbeArtifactV1 {
    schema_version: u16,
    evidence_kind: String,
    status: String,
    authority_store_id: String,
    authority_namespace: String,
    operator_id: String,
    writer_id: String,
    lease_identity: String,
    owner_id: String,
    owner_epoch: u64,
    acquired_grant: LeaseGrantEvidenceV1,
    before_admission_lease_verified: bool,
    lease_released_before_commit: bool,
    commit_guard_rejected_before_batch_commit: bool,
    batch_object_absent_after_rejection: bool,
    admission_commit_guard_bound: bool,
    admission_commit_guard_binding: IngestCommitGuardBindingV1,
    restart_reconstructed_active_admission: bool,
    target_admission_rejected_overlapping_reservation_before_expiry: bool,
    orphan_expired: bool,
    expired_target_rejected_original_retry: bool,
    final_active_admission_records: usize,
    descriptor: IngestWriterAppendDescriptorV1,
    admission_record_key: String,
    batch_key: String,
    expiry_decision_key: String,
}

#[derive(Clone, Debug, Deserialize)]
struct DefaultScoreRow {
    user_id: String,
    score: i64,
    delta: i64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Append {
            payload_file,
            authority_store_id,
            authority_namespace,
            operator_id,
            writer_id,
            json,
        } => {
            let payload = fs::read(&payload_file)
                .with_context(|| format!("failed to read {}", payload_file.display()))?;
            let artifact = run_ingest_writer_append(
                s3_compatible_authority_store_from_env()?,
                IngestWriterAppendRequest {
                    authority_store_id,
                    authority_namespace,
                    operator_id,
                    writer_id,
                    payload: Bytes::from(payload),
                },
            )
            .await?;

            if json {
                println!("{}", serde_json::to_string_pretty(&artifact)?);
            } else {
                print!("{}", format_ingest_writer_append(&artifact));
            }
        }
        Command::EncodeDefaultScoresPayload {
            output,
            schema_fingerprint,
            stream_id,
            partition_id,
            start_offset_inclusive,
            rows_json,
        } => {
            let payload = encode_default_scores_payload(
                &schema_fingerprint,
                &stream_id,
                partition_id,
                start_offset_inclusive,
                &rows_json,
            )?;
            fs::write(&output, payload)
                .with_context(|| format!("failed to write {}", output.display()))?;
        }
        Command::ProbeKubernetesLeaseHandoff {
            namespace,
            view_id,
            stream_id,
            partition_id,
            owner_a,
            owner_b,
            ttl_ms,
            json: _,
        } => {
            let artifact = run_kubernetes_lease_handoff_probe(LeaseHandoffProbeRequest {
                key: PartitionLeaseKey {
                    namespace,
                    view_id,
                    stream_id,
                    partition_id,
                },
                owner_a,
                owner_b,
                ttl_ms,
            })
            .await?;
            println!("{}", serde_json::to_string_pretty(&artifact)?);
        }
        Command::ProbeKubernetesLeaseAcquire {
            namespace,
            view_id,
            stream_id,
            partition_id,
            owner_id,
            ttl_ms,
            json: _,
        } => {
            let artifact = run_kubernetes_lease_acquire_probe(KubernetesLeaseAcquireProbeRequest {
                key: PartitionLeaseKey {
                    namespace,
                    view_id,
                    stream_id,
                    partition_id,
                },
                owner_id,
                ttl_ms,
            })
            .await?;
            println!("{}", serde_json::to_string_pretty(&artifact)?);
        }
        Command::ProbeLeaseGuardedAppend {
            payload_file,
            authority_store_id,
            authority_namespace,
            operator_id,
            writer_id,
            lease_namespace,
            lease_view_id,
            lease_stream_id,
            lease_partition_id,
            owner_id,
            expected_owner_epoch,
            ttl_ms,
            acquire_lease,
            expected_outcome,
            json: _,
        } => {
            let payload = fs::read(&payload_file)
                .with_context(|| format!("failed to read {}", payload_file.display()))?;
            let artifact = run_lease_guarded_append_probe(
                s3_compatible_authority_store_from_env()?,
                LeaseGuardedAppendProbeRequest {
                    authority_store_id,
                    authority_namespace,
                    operator_id,
                    writer_id,
                    payload: Bytes::from(payload),
                    lease_key: PartitionLeaseKey {
                        namespace: lease_namespace,
                        view_id: lease_view_id,
                        stream_id: lease_stream_id,
                        partition_id: lease_partition_id,
                    },
                    owner_id,
                    expected_owner_epoch,
                    ttl_ms,
                    acquire_lease,
                    expected_outcome,
                },
            )
            .await?;
            println!("{}", serde_json::to_string_pretty(&artifact)?);
        }
        Command::ProbeMetaStaleToken {
            namespace,
            view_id,
            stream_id,
            partition_id,
            ttl_ms,
            takeover_ttl_ms,
            json: _,
        } => {
            let artifact = run_meta_stale_token_probe(
                grpc_meta_store_from_env().await?,
                PartitionAuthorityKey {
                    namespace,
                    view_id,
                    stream_id,
                    partition_id,
                },
                ttl_ms,
                takeover_ttl_ms,
            )
            .await?;
            println!("{}", serde_json::to_string_pretty(&artifact)?);
        }
        Command::ProbeIngestAdmissionCrashRestart {
            payload_file,
            authority_store_id,
            authority_namespace,
            operator_id,
            writer_id,
            json: _,
        } => {
            let payload = fs::read(&payload_file)
                .with_context(|| format!("failed to read {}", payload_file.display()))?;
            let artifact = run_ingest_admission_crash_restart_probe(
                s3_compatible_authority_store_from_env()?,
                IngestAdmissionCrashRestartProbeRequest {
                    authority_store_id,
                    authority_namespace,
                    operator_id,
                    writer_id,
                    payload: Bytes::from(payload),
                },
            )
            .await?;
            println!("{}", serde_json::to_string_pretty(&artifact)?);
        }
        Command::ProbeLeaseLossDuringReservation {
            payload_file,
            authority_store_id,
            authority_namespace,
            operator_id,
            writer_id,
            lease_namespace,
            lease_view_id,
            lease_stream_id,
            lease_partition_id,
            owner_id,
            ttl_ms,
            json: _,
        } => {
            let payload = fs::read(&payload_file)
                .with_context(|| format!("failed to read {}", payload_file.display()))?;
            let artifact = run_lease_loss_during_reservation_probe(
                s3_compatible_authority_store_from_env()?,
                LeaseLossDuringReservationProbeRequest {
                    authority_store_id,
                    authority_namespace,
                    operator_id,
                    writer_id,
                    payload: Bytes::from(payload),
                    lease_key: PartitionLeaseKey {
                        namespace: lease_namespace,
                        view_id: lease_view_id,
                        stream_id: lease_stream_id,
                        partition_id: lease_partition_id,
                    },
                    owner_id,
                    ttl_ms,
                },
            )
            .await?;
            println!("{}", serde_json::to_string_pretty(&artifact)?);
        }
    }

    Ok(())
}

async fn run_ingest_admission_crash_restart_probe(
    store: Arc<dyn ObjectStore>,
    request: IngestAdmissionCrashRestartProbeRequest,
) -> anyhow::Result<IngestAdmissionCrashRestartProbeArtifactV1> {
    validate_ingest_writer_authority_store_id(&request.authority_store_id)?;
    if request.authority_namespace.trim().is_empty() {
        bail!("ingest admission crash/restart probe requires --authority-namespace");
    }
    if request.operator_id.trim().is_empty() {
        bail!("ingest admission crash/restart probe requires --operator-id");
    }
    if request.writer_id.trim().is_empty() {
        bail!("ingest admission crash/restart probe requires --writer-id");
    }
    if request.payload.is_empty() {
        bail!("ingest admission crash/restart probe requires a non-empty --payload-file");
    }

    let envelope = IngestEnvelope::decode(request.payload.clone())?;
    let header = envelope.header();
    let admission = velorix_storage::log::DurableIngestAdmissionRecordV1::for_external_admission(
        header.stream_id.clone(),
        header.partition_id,
        header.start_offset_inclusive,
        header.end_offset_exclusive,
        header.payload_digest.clone(),
        header.relation_id.clone(),
        header.relation_version.clone(),
        header.schema_fingerprint.clone(),
    )?;

    let components = startup_components_for_probe(
        store.clone(),
        &request.authority_store_id,
        &request.authority_namespace,
        &request.writer_id,
        "ingest-admission-crash-restart-probe",
        "crash-restart-seed",
    )
    .await?;
    let provider = components.ingest_admission_coordinator_provider();
    let (coordinator, before_restart_report) = provider
        .coordinator_after_startup_reconstruction()
        .await
        .map_err(anyhow::Error::from)?;
    let reserve_outcome = coordinator
        .reserve_external_ingest_range_admission(admission.clone())
        .await
        .map_err(anyhow::Error::from)?;
    if !matches!(
        reserve_outcome,
        velorix_storage::log::ReserveIngestRangeAdmissionOutcome::Reserved
    ) {
        bail!(
            "crash/restart probe expected to create a fresh orphan admission, got {}",
            reserve_ingest_range_admission_outcome_label(&reserve_outcome)
        );
    }

    let restarted_components = startup_components_for_probe(
        store.clone(),
        &request.authority_store_id,
        &request.authority_namespace,
        &request.writer_id,
        "ingest-admission-crash-restart-probe",
        "crash-restart-restarted",
    )
    .await?;
    let restarted_provider = restarted_components.ingest_admission_coordinator_provider();
    let (restarted_coordinator, after_restart_report) = restarted_provider
        .coordinator_after_startup_reconstruction()
        .await
        .map_err(anyhow::Error::from)?;

    if after_restart_report.active_admission_records
        <= before_restart_report.active_admission_records
    {
        bail!(
            "crash/restart probe did not reconstruct the seeded orphan admission: before={} after={}",
            before_restart_report.active_admission_records,
            after_restart_report.active_admission_records
        );
    }

    let append_outcome = restarted_coordinator
        .append_catalog_validated_envelope_after_external_admission(request.payload.clone())
        .await
        .map_err(anyhow::Error::from)?;
    let (append_outcome_label, descriptor) = ingest_writer_append_outcome_parts(append_outcome)?;
    if append_outcome_label != "appended" {
        bail!("crash/restart probe expected recovered append to create a fresh batch, got {append_outcome_label}");
    }

    let final_report = restarted_coordinator
        .reconstruct_active_admissions()
        .await
        .map_err(anyhow::Error::from)?;
    let committed_admission_not_expirable = restarted_coordinator
        .expire_orphan_admission(
            &header.stream_id,
            header.partition_id,
            header.start_offset_inclusive,
            header.end_offset_exclusive,
            &format!(
                "crash-restart-probe-{}",
                sanitize_probe_id(&request.writer_id)
            ),
            "crash_restart_probe_committed_guard",
            &request.operator_id,
        )
        .await
        .is_err();
    if !committed_admission_not_expirable {
        bail!("crash/restart probe unexpectedly expired an admission after the batch committed");
    }

    Ok(IngestAdmissionCrashRestartProbeArtifactV1 {
        schema_version: 1,
        evidence_kind: "ingest_writer_admission_crash_restart_probe".to_string(),
        status: "pass".to_string(),
        authority_store_id: request.authority_store_id,
        authority_namespace: request.authority_namespace,
        operator_id: request.operator_id,
        writer_id: request.writer_id,
        orphan_admission_created: matches!(
            reserve_outcome,
            velorix_storage::log::ReserveIngestRangeAdmissionOutcome::Reserved
        ),
        restart_reconstructed_active_admission: true,
        recovered_append_completed: true,
        committed_admission_not_expirable,
        before_restart_active_admission_records: before_restart_report.active_admission_records,
        after_restart_active_admission_records: after_restart_report.active_admission_records,
        final_active_admission_records: final_report.active_admission_records,
        reserve_outcome: reserve_ingest_range_admission_outcome_label(&reserve_outcome).to_string(),
        append_outcome: append_outcome_label,
        descriptor,
        admission_record_key: admission.admission_record_key.as_str().to_string(),
        batch_key: admission.batch_key.as_str().to_string(),
    })
}

async fn run_lease_loss_during_reservation_probe(
    store: Arc<dyn ObjectStore>,
    request: LeaseLossDuringReservationProbeRequest,
) -> anyhow::Result<LeaseLossDuringReservationProbeArtifactV1> {
    validate_ingest_writer_authority_store_id(&request.authority_store_id)?;
    validate_kubernetes_lease_probe_request(&request.lease_key, &request.owner_id, request.ttl_ms)?;
    if request.authority_namespace.trim().is_empty() {
        bail!("lease-loss probe requires --authority-namespace");
    }
    if request.operator_id.trim().is_empty() {
        bail!("lease-loss probe requires --operator-id");
    }
    if request.writer_id.trim().is_empty() {
        bail!("lease-loss probe requires --writer-id");
    }
    if request.payload.is_empty() {
        bail!("lease-loss probe requires a non-empty --payload-file");
    }

    let batch = IngestBatch::from_validated_envelope(request.payload.clone())?;
    let descriptor = batch.descriptor();
    if request.lease_key.stream_id != descriptor.stream_id
        || request.lease_key.partition_id != descriptor.partition_id
    {
        bail!(
            "lease-loss probe lease key must match payload stream/partition: lease={}/p{} payload={}/p{}",
            request.lease_key.stream_id,
            request.lease_key.partition_id,
            descriptor.stream_id,
            descriptor.partition_id
        );
    }
    let descriptor = ingest_writer_descriptor(&descriptor);

    let client = Client::try_default()
        .await
        .with_context(|| "failed to create Kubernetes client from runtime environment")?;
    let lease_client = KubernetesPartitionLeaseClient::new(KubeLeaseApi::new(client));
    let lease_identity = partition_lease_identity(&request.lease_key);
    let grant = lease_client
        .acquire_or_renew(LeaseAcquireRequest {
            key: request.lease_key.clone(),
            owner_id: request.owner_id.clone(),
            now_unix_ms: unix_ms()?,
            ttl_ms: request.ttl_ms,
        })
        .await
        .map_err(anyhow::Error::from)
        .with_context(|| "failed to acquire Kubernetes lease before lease-loss probe")?;

    let commit_guard = LeaseLossDuringReservationCommitGuard {
        lease_client: lease_client.clone(),
        lease_key: request.lease_key.clone(),
        owner_id: request.owner_id.clone(),
        owner_epoch: grant.owner_epoch,
        before_admission_verified: Arc::new(AtomicBool::new(false)),
        released_before_commit: Arc::new(AtomicBool::new(false)),
    };
    let expected_commit_guard_binding = commit_guard
        .admission_binding(
            &IngestBatch::from_validated_envelope(request.payload.clone())?.descriptor(),
        )
        .ok_or_else(|| {
            anyhow::anyhow!("lease-loss commit guard did not provide an admission binding")
        })?;

    let components = startup_components_for_probe(
        Arc::clone(&store),
        &request.authority_store_id,
        &request.authority_namespace,
        &request.writer_id,
        "ingest-writer-lease-loss-during-reservation-probe",
        "lease-loss-during-reservation",
    )
    .await?;
    let runtime = DeployedIngestWriterRuntime::from_startup_components(&components)
        .await
        .map_err(anyhow::Error::from)?;
    let append_error = runtime
        .append_catalog_validated_envelope_with_commit_guard(request.payload.clone(), &commit_guard)
        .await
        .expect_err("lease-loss probe must reject append before batch commit");
    let append_error_text = append_error.to_string();
    if !append_error_text.contains("ingest commit guard rejected")
        || !append_error_text.contains(IngestCommitGuardPhase::BeforeCommit.as_str())
    {
        bail!(
            "lease-loss probe expected BeforeCommit guard rejection, got: {}",
            append_error_text
        );
    }

    let (target_admission_record, admission_commit_guard_binding) =
        read_admission_record_and_commit_guard_binding(
            Arc::clone(&store),
            &descriptor,
            &expected_commit_guard_binding,
        )
        .await?;
    if target_admission_record.admission_record_key.as_str()
        != ObjectKey::ingest_admission_record(
            &descriptor.stream_id,
            descriptor.partition_id,
            descriptor.start_offset_inclusive,
            descriptor.end_offset_exclusive,
        )?
        .as_str()
    {
        bail!("lease-loss probe read an admission record for the wrong range");
    }
    let batch_object_absent_after_rejection =
        !object_store_key_exists(Arc::clone(&store), &descriptor.object_key).await?;
    if !batch_object_absent_after_rejection {
        bail!(
            "lease-loss probe rejected the commit guard but batch object exists: {}",
            descriptor.object_key
        );
    }

    let restarted_provider = components.ingest_admission_coordinator_provider();
    let (restarted_coordinator, restarted_report) = restarted_provider
        .coordinator_after_startup_reconstruction()
        .await
        .map_err(anyhow::Error::from)?;

    let mut conflicting_record = target_admission_record.clone();
    conflicting_record.payload_digest =
        "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff".to_string();
    conflicting_record.commit_guard_binding = None;
    let target_admission_rejected_overlapping_reservation_before_expiry = matches!(
        restarted_coordinator
            .reserve_external_ingest_range_admission(conflicting_record)
            .await
            .map_err(anyhow::Error::from)?,
        velorix_storage::log::ReserveIngestRangeAdmissionOutcome::Conflict {
            object_key,
            ..
        } if object_key == target_admission_record.batch_key
    );
    if !target_admission_rejected_overlapping_reservation_before_expiry {
        bail!("lease-loss probe did not prove the target admission blocked an overlapping reservation before expiry");
    }
    if restarted_report.active_admission_records == 0 {
        bail!("lease-loss probe did not reconstruct any active orphan admission after rejection");
    }

    let expiry_decision = restarted_coordinator
        .expire_orphan_admission(
            &descriptor.stream_id,
            descriptor.partition_id,
            descriptor.start_offset_inclusive,
            descriptor.end_offset_exclusive,
            &format!("lease-loss-probe-{}", sanitize_probe_id(&request.writer_id)),
            "lease_lost_before_batch_commit",
            &request.operator_id,
        )
        .await
        .map_err(anyhow::Error::from)?;
    if expiry_decision.admission_record_key != target_admission_record.admission_record_key
        || expiry_decision.batch_key != target_admission_record.batch_key
    {
        bail!("lease-loss probe expiry decision did not target the rejected admission");
    }

    let expired_target_rejected_original_retry = matches!(
        restarted_coordinator
            .append_catalog_validated_envelope(request.payload.clone())
            .await
            .map_err(anyhow::Error::from)?,
        AppendValidatedEnvelopeOutcome::Conflict {
            object_key,
            reason,
            ..
        } if object_key == target_admission_record.batch_key && reason == "admission_expired"
    );
    if !expired_target_rejected_original_retry {
        bail!("lease-loss probe did not prove the expired target admission rejects the original retry");
    }
    let final_report = restarted_coordinator
        .reconstruct_active_admissions()
        .await
        .map_err(anyhow::Error::from)?;

    Ok(LeaseLossDuringReservationProbeArtifactV1 {
        schema_version: 1,
        evidence_kind: "ingest_writer_lease_loss_during_reservation_probe".to_string(),
        status: "pass".to_string(),
        authority_store_id: request.authority_store_id,
        authority_namespace: request.authority_namespace,
        operator_id: request.operator_id,
        writer_id: request.writer_id,
        lease_identity,
        owner_id: request.owner_id,
        owner_epoch: grant.owner_epoch,
        acquired_grant: lease_grant_evidence(&grant),
        before_admission_lease_verified: commit_guard
            .before_admission_verified
            .load(Ordering::SeqCst),
        lease_released_before_commit: commit_guard.released_before_commit.load(Ordering::SeqCst),
        commit_guard_rejected_before_batch_commit: true,
        batch_object_absent_after_rejection,
        admission_commit_guard_bound: true,
        admission_commit_guard_binding,
        restart_reconstructed_active_admission: true,
        target_admission_rejected_overlapping_reservation_before_expiry,
        orphan_expired: true,
        expired_target_rejected_original_retry,
        final_active_admission_records: final_report.active_admission_records,
        admission_record_key: expiry_decision.admission_record_key.as_str().to_string(),
        batch_key: expiry_decision.batch_key.as_str().to_string(),
        expiry_decision_key: expiry_decision.expiry_decision_key.as_str().to_string(),
        descriptor,
    })
}

async fn run_kubernetes_lease_acquire_probe(
    request: KubernetesLeaseAcquireProbeRequest,
) -> anyhow::Result<KubernetesLeaseAcquireProbeArtifactV1> {
    validate_kubernetes_lease_probe_request(&request.key, &request.owner_id, request.ttl_ms)?;

    let client = Client::try_default()
        .await
        .with_context(|| "failed to create Kubernetes client from runtime environment")?;
    let lease_client = KubernetesPartitionLeaseClient::new(KubeLeaseApi::new(client));
    let lease_identity = partition_lease_identity(&request.key);
    let grant = lease_client
        .acquire_or_renew(LeaseAcquireRequest {
            key: request.key.clone(),
            owner_id: request.owner_id.clone(),
            now_unix_ms: unix_ms()?,
            ttl_ms: request.ttl_ms,
        })
        .await
        .map_err(anyhow::Error::from)
        .with_context(|| "failed to acquire Kubernetes lease")?;

    Ok(KubernetesLeaseAcquireProbeArtifactV1 {
        schema_version: 1,
        evidence_kind: "ingest_writer_kubernetes_lease_acquire_probe".to_string(),
        status: "pass".to_string(),
        lease_identity,
        namespace: request.key.namespace,
        view_id: request.key.view_id,
        stream_id: request.key.stream_id,
        partition_id: request.key.partition_id,
        owner_id: grant.owner_id,
        owner_epoch: grant.owner_epoch,
        expires_at_unix_ms: grant.expires_at_unix_ms,
        released: false,
    })
}

async fn run_lease_guarded_append_probe(
    store: Arc<dyn ObjectStore>,
    request: LeaseGuardedAppendProbeRequest,
) -> anyhow::Result<LeaseGuardedAppendProbeArtifactV1> {
    validate_ingest_writer_authority_store_id(&request.authority_store_id)?;
    validate_partition_authority_request(&request.lease_key, &request.owner_id, request.ttl_ms)?;
    if request.authority_namespace.trim().is_empty() {
        bail!("lease-guarded append probe requires --authority-namespace");
    }
    if request.operator_id.trim().is_empty() {
        bail!("lease-guarded append probe requires --operator-id");
    }
    if request.writer_id.trim().is_empty() {
        bail!("lease-guarded append probe requires --writer-id");
    }
    let expected_outcome = request.expected_outcome.trim();
    if !matches!(
        expected_outcome,
        "appended" | "duplicate" | "appended-or-duplicate"
    ) {
        bail!(
            "lease-guarded append probe expected-outcome must be appended, duplicate, or appended-or-duplicate"
        );
    }
    if !request.acquire_lease {
        bail!("lease-guarded append requires --acquire-lease for Meta authority");
    }

    let descriptor = ingest_writer_descriptor(
        &IngestBatch::from_validated_envelope(request.payload.clone())?.descriptor(),
    );
    if request.lease_key.stream_id != descriptor.stream_id
        || request.lease_key.partition_id != descriptor.partition_id
    {
        bail!(
            "lease-guarded append authority key must match payload stream/partition: authority={}/p{} payload={}/p{}",
            request.lease_key.stream_id,
            request.lease_key.partition_id,
            descriptor.stream_id,
            descriptor.partition_id
        );
    }
    let key = PartitionAuthorityKey {
        namespace: request.lease_key.namespace.clone(),
        view_id: request.lease_key.view_id.clone(),
        stream_id: request.lease_key.stream_id.clone(),
        partition_id: request.lease_key.partition_id,
    };
    let lease_identity = partition_authority_identity(&key);
    let commit_guard = PartitionAuthoritySession::acquire(
        grpc_meta_store_from_env().await?,
        key,
        request.owner_id.clone(),
        request.ttl_ms,
    )
    .await?;
    let token = commit_guard.token.read().await.clone();
    if let Some(expected_epoch) = request.expected_owner_epoch {
        if expected_epoch != token.owner_epoch {
            commit_guard.shutdown().await;
            bail!(
                "Meta authority epoch mismatch: expected {expected_epoch}, acquired {}",
                token.owner_epoch
            );
        }
    }
    let expected_owner_epoch = Some(token.owner_epoch);
    let expected_commit_guard_binding = commit_guard
        .admission_binding(
            &IngestBatch::from_validated_envelope(request.payload.clone())?.descriptor(),
        )
        .ok_or_else(|| {
            anyhow::anyhow!("Meta authority commit guard did not provide an admission binding")
        })?;
    let expected_identity = ingest_identity_from_payload(request.payload.clone())?;
    let append_result = run_ingest_writer_append_with_commit_guard(
        Arc::clone(&store),
        IngestWriterAppendRequest {
            authority_store_id: request.authority_store_id.clone(),
            authority_namespace: request.authority_namespace.clone(),
            operator_id: request.operator_id.clone(),
            writer_id: request.writer_id.clone(),
            payload: request.payload,
        },
        Some(&commit_guard),
    )
    .await;
    commit_guard.shutdown().await;
    let append_artifact = append_result?;
    if !lease_guarded_append_outcome_matches(expected_outcome, &append_artifact.outcome) {
        bail!(
            "lease-guarded append expected outcome {expected_outcome}, got {}",
            append_artifact.outcome,
        );
    }
    let admission_commit_guard_binding = match append_artifact.outcome.as_str() {
        "appended" => {
            read_admission_commit_guard_binding(
                Arc::clone(&store),
                &append_artifact.descriptor,
                &expected_commit_guard_binding,
            )
            .await?
        }
        "duplicate" => {
            verify_duplicate_admission_evidence(
                Arc::clone(&store),
                &append_artifact.descriptor,
                &expected_identity,
                &lease_identity,
            )
            .await?
        }
        outcome => bail!("lease-guarded append returned unsupported outcome {outcome}"),
    };
    Ok(LeaseGuardedAppendProbeArtifactV1 {
        schema_version: 1,
        evidence_kind: "ingest_writer_lease_guarded_append_probe".to_string(),
        status: "pass".to_string(),
        expected_outcome: expected_outcome.to_string(),
        outcome: append_artifact.outcome,
        authority_store_id: request.authority_store_id,
        authority_namespace: request.authority_namespace,
        operator_id: request.operator_id,
        writer_id: request.writer_id,
        lease_identity,
        owner_id: request.owner_id,
        expected_owner_epoch,
        acquired_grant: Some(partition_authority_grant_evidence(&token)),
        current_owner: Some(partition_authority_grant_evidence(&token)),
        post_append_current_owner: Some(partition_authority_grant_evidence(&token)),
        commit_guard_enforced: true,
        admission_commit_guard_bound: true,
        admission_commit_guard_binding: Some(admission_commit_guard_binding),
        lease_held_through_append: true,
        stale_owner_rejected: false,
        append_completed: true,
        descriptor: append_artifact.descriptor,
    })
}

async fn read_admission_commit_guard_binding(
    store: Arc<dyn ObjectStore>,
    descriptor: &IngestWriterAppendDescriptorV1,
    expected: &IngestCommitGuardBindingV1,
) -> anyhow::Result<IngestCommitGuardBindingV1> {
    let (_, binding) =
        read_admission_record_and_commit_guard_binding(store, descriptor, expected).await?;
    Ok(binding)
}

async fn read_admission_record_and_commit_guard_binding(
    store: Arc<dyn ObjectStore>,
    descriptor: &IngestWriterAppendDescriptorV1,
    expected: &IngestCommitGuardBindingV1,
) -> anyhow::Result<(DurableIngestAdmissionRecordV1, IngestCommitGuardBindingV1)> {
    let (record, actual) =
        read_admission_record_and_commit_guard_binding_unchecked(store, descriptor).await?;
    if &actual != expected {
        bail!(
            "durable admission record `{}` commit_guard_binding mismatch: expected {:?}, got {:?}",
            record.admission_record_key,
            expected,
            actual
        );
    }

    Ok((record, actual))
}

async fn read_admission_record_and_commit_guard_binding_unchecked(
    store: Arc<dyn ObjectStore>,
    descriptor: &IngestWriterAppendDescriptorV1,
) -> anyhow::Result<(DurableIngestAdmissionRecordV1, IngestCommitGuardBindingV1)> {
    let admission_record_key = ObjectKey::ingest_admission_record(
        &descriptor.stream_id,
        descriptor.partition_id,
        descriptor.start_offset_inclusive,
        descriptor.end_offset_exclusive,
    )?;
    let bytes = store
        .get(&ObjectStorePath::from(admission_record_key.as_str()))
        .await
        .map_err(anyhow::Error::from)
        .with_context(|| {
            format!(
                "failed to read durable admission record `{}` after guarded append",
                admission_record_key
            )
        })?
        .bytes()
        .await
        .map_err(anyhow::Error::from)?;
    let record: DurableIngestAdmissionRecordV1 = serde_json::from_slice(&bytes)?;
    let Some(actual) = record.commit_guard_binding.clone() else {
        bail!(
            "durable admission record `{}` is missing commit_guard_binding",
            admission_record_key
        );
    };
    Ok((record, actual))
}

async fn verify_duplicate_admission_evidence(
    store: Arc<dyn ObjectStore>,
    descriptor: &IngestWriterAppendDescriptorV1,
    expected: &IngestIdentity,
    lease_identity: &str,
) -> anyhow::Result<IngestCommitGuardBindingV1> {
    let (record, binding) =
        read_admission_record_and_commit_guard_binding_unchecked(store, descriptor).await?;
    let expected_admission_key = ObjectKey::ingest_admission_record(
        &expected.stream_id,
        expected.partition_id,
        expected.start_offset_inclusive,
        expected.end_offset_exclusive,
    )?;
    let expected_batch_key = ObjectKey::ingest_batch(
        &expected.stream_id,
        expected.partition_id,
        expected.start_offset_inclusive,
        expected.end_offset_exclusive,
    )?;
    for (field, expected, actual) in [
        (
            "stream_id",
            expected.stream_id.clone(),
            record.stream_id.clone(),
        ),
        (
            "partition_id",
            expected.partition_id.to_string(),
            record.partition_id.to_string(),
        ),
        (
            "start_offset_inclusive",
            expected.start_offset_inclusive.to_string(),
            record.start_offset_inclusive.to_string(),
        ),
        (
            "end_offset_exclusive",
            expected.end_offset_exclusive.to_string(),
            record.end_offset_exclusive.to_string(),
        ),
        (
            "payload_digest",
            expected.payload_digest.clone(),
            record.payload_digest.clone(),
        ),
        (
            "relation_id",
            expected.relation_id.clone(),
            record.relation_id.clone(),
        ),
        (
            "relation_version",
            expected.relation_version.clone(),
            record.relation_version.clone(),
        ),
        (
            "schema_fingerprint",
            expected.schema_fingerprint.clone(),
            record.schema_fingerprint.clone(),
        ),
        (
            "batch_key",
            expected_batch_key.as_str().to_string(),
            record.batch_key.as_str().to_string(),
        ),
        (
            "admission_record_key",
            expected_admission_key.as_str().to_string(),
            record.admission_record_key.as_str().to_string(),
        ),
        (
            "descriptor_object_key",
            expected_batch_key.as_str().to_string(),
            descriptor.object_key.clone(),
        ),
    ] {
        if expected != actual {
            bail!(
                "ingest-writer duplicate admission evidence mismatch for {field}: expected={expected}, actual={actual}"
            );
        }
    }
    if binding.schema_version != 1
        || binding.binding_kind != "meta_partition_authority"
        || binding.subject != lease_identity
        || binding.owner_id.trim().is_empty()
        || binding.owner_epoch == 0
    {
        bail!(
            "ingest-writer duplicate admission evidence has invalid original Meta authority binding: {:?}",
            binding
        );
    }

    Ok(binding)
}

async fn object_store_key_exists(
    store: Arc<dyn ObjectStore>,
    object_key: &str,
) -> anyhow::Result<bool> {
    match store.get(&ObjectStorePath::from(object_key)).await {
        Ok(_) => Ok(true),
        Err(object_store::Error::NotFound { .. }) => Ok(false),
        Err(error) => Err(anyhow::Error::from(error)),
    }
}

fn validate_kubernetes_lease_probe_request(
    key: &PartitionLeaseKey,
    owner_id: &str,
    ttl_ms: u64,
) -> anyhow::Result<()> {
    if key.namespace.trim().is_empty() {
        bail!("Kubernetes lease probe requires namespace");
    }
    if key.view_id.trim().is_empty() {
        bail!("Kubernetes lease probe requires view-id");
    }
    if key.stream_id.trim().is_empty() {
        bail!("Kubernetes lease probe requires stream-id");
    }
    if owner_id.trim().is_empty() {
        bail!("Kubernetes lease probe requires non-empty owner id");
    }
    if ttl_ms == 0 {
        bail!("Kubernetes lease probe requires ttl-ms greater than zero");
    }
    Ok(())
}

fn validate_partition_authority_request(
    key: &PartitionLeaseKey,
    owner_id: &str,
    ttl_ms: u64,
) -> anyhow::Result<()> {
    if key.namespace.trim().is_empty()
        || key.view_id.trim().is_empty()
        || key.stream_id.trim().is_empty()
    {
        bail!("Meta partition authority requires namespace, view-id, and stream-id");
    }
    if owner_id.trim().is_empty() {
        bail!("Meta partition authority requires a non-empty owner id");
    }
    if ttl_ms < 3 {
        bail!("Meta partition authority requires ttl-ms of at least 3");
    }
    Ok(())
}

fn partition_authority_grant_evidence(token: &PartitionAuthorityToken) -> LeaseGrantEvidenceV1 {
    LeaseGrantEvidenceV1 {
        owner_id: token.owner_id.clone(),
        owner_epoch: token.owner_epoch,
        expires_at_unix_ms: token.expires_at_unix_ms,
    }
}

async fn startup_components_for_probe(
    store: Arc<dyn ObjectStore>,
    authority_store_id: &str,
    authority_namespace: &str,
    writer_id: &str,
    capability_holder_id: &str,
    probe_suffix: &str,
) -> anyhow::Result<OperatorAuthorityStartupComponents> {
    let validated = validate_operator_authority(
        ObjectStoreAuthorityRef {
            store_id: authority_store_id.to_string(),
            namespace: authority_namespace.to_string(),
        },
        store,
        capability_holder_id,
        format!(
            "v1/ingest-writer-capability-probes/{}-{}",
            sanitize_probe_id(writer_id),
            probe_suffix
        ),
    )
    .await
    .map_err(anyhow::Error::from)?;

    Ok(OperatorAuthorityStartupComponents::from_validated_authority(validated))
}

async fn verify_kubernetes_lease_holder(
    lease_client: &KubernetesPartitionLeaseClient<KubeLeaseApi>,
    lease_key: &PartitionLeaseKey,
    owner_id: &str,
    owner_epoch: u64,
    phase: IngestCommitGuardPhase,
) -> Result<(), String> {
    let current = lease_client
        .current(lease_key, unix_ms().map_err(|error| error.to_string())?)
        .await
        .map_err(|error| error.to_string())?;
    match current {
        Some(grant) if grant.owner_id == owner_id && grant.owner_epoch == owner_epoch => Ok(()),
        Some(grant) => Err(format!(
            "lease owner/epoch mismatch at {}: expected {}#{}, current {}#{}",
            phase.as_str(),
            owner_id,
            owner_epoch,
            grant.owner_id,
            grant.owner_epoch
        )),
        None => Err(format!(
            "lease not held at {}: expected {}#{}",
            phase.as_str(),
            owner_id,
            owner_epoch
        )),
    }
}

fn reserve_ingest_range_admission_outcome_label(
    outcome: &velorix_storage::log::ReserveIngestRangeAdmissionOutcome,
) -> &'static str {
    match outcome {
        velorix_storage::log::ReserveIngestRangeAdmissionOutcome::Reserved => "reserved",
        velorix_storage::log::ReserveIngestRangeAdmissionOutcome::Duplicate => "duplicate",
        velorix_storage::log::ReserveIngestRangeAdmissionOutcome::Conflict { .. } => "conflict",
    }
}

async fn run_kubernetes_lease_handoff_probe(
    request: LeaseHandoffProbeRequest,
) -> anyhow::Result<LeaseHandoffProbeArtifactV1> {
    validate_lease_handoff_probe_request(&request)?;

    let client = Client::try_default()
        .await
        .with_context(|| "failed to create Kubernetes client from runtime environment")?;
    let lease_client = KubernetesPartitionLeaseClient::new(KubeLeaseApi::new(client));
    let lease_identity = partition_lease_identity(&request.key);

    let owner_a = lease_client
        .acquire_or_renew(LeaseAcquireRequest {
            key: request.key.clone(),
            owner_id: request.owner_a.clone(),
            now_unix_ms: unix_ms()?,
            ttl_ms: request.ttl_ms,
        })
        .await
        .map_err(anyhow::Error::from)
        .with_context(|| "failed to acquire Kubernetes lease as owner A")?;

    lease_client
        .release(
            &request.key,
            &owner_a.owner_id,
            owner_a.owner_epoch,
            unix_ms()?,
        )
        .await
        .map_err(anyhow::Error::from)
        .with_context(|| "failed to release Kubernetes lease as owner A")?;

    let owner_b = lease_client
        .acquire_or_renew(LeaseAcquireRequest {
            key: request.key.clone(),
            owner_id: request.owner_b.clone(),
            now_unix_ms: unix_ms()?,
            ttl_ms: request.ttl_ms,
        })
        .await
        .map_err(anyhow::Error::from)
        .with_context(|| "failed to acquire Kubernetes lease as owner B")?;

    let current = lease_client
        .current(&request.key, unix_ms()?)
        .await
        .map_err(anyhow::Error::from)
        .with_context(|| "failed to read current Kubernetes lease holder after owner B acquire")?
        .with_context(|| "Kubernetes lease is not held after owner B acquire")?;

    let release_owner_b = lease_client
        .release(
            &request.key,
            &owner_b.owner_id,
            owner_b.owner_epoch,
            unix_ms()?,
        )
        .await
        .map_err(|error| error.to_string());

    lease_handoff_probe_artifact(
        &request.key,
        &lease_identity,
        owner_a,
        owner_b,
        current,
        release_owner_b,
    )
}

fn validate_lease_handoff_probe_request(request: &LeaseHandoffProbeRequest) -> anyhow::Result<()> {
    if request.key.namespace.trim().is_empty() {
        bail!("lease handoff probe requires --namespace");
    }
    if request.key.view_id.trim().is_empty() {
        bail!("lease handoff probe requires --view-id");
    }
    if request.key.stream_id.trim().is_empty() {
        bail!("lease handoff probe requires --stream-id");
    }
    if request.owner_a.trim().is_empty() {
        bail!("lease handoff probe requires --owner-a");
    }
    if request.owner_b.trim().is_empty() {
        bail!("lease handoff probe requires --owner-b");
    }
    if request.owner_a == request.owner_b {
        bail!("lease handoff probe requires distinct --owner-a and --owner-b");
    }
    if request.ttl_ms == 0 {
        bail!("lease handoff probe requires --ttl-ms greater than zero");
    }
    Ok(())
}

fn lease_handoff_probe_artifact(
    key: &PartitionLeaseKey,
    lease_identity: &str,
    owner_a: PartitionLeaseGrant,
    owner_b: PartitionLeaseGrant,
    current: PartitionLeaseGrant,
    release_owner_b: Result<(), String>,
) -> anyhow::Result<LeaseHandoffProbeArtifactV1> {
    if owner_a.key != *key || owner_b.key != *key || current.key != *key {
        bail!("lease handoff probe observed a grant for a different partition lease key");
    }
    if owner_b.owner_epoch <= owner_a.owner_epoch {
        bail!(
            "lease handoff probe expected owner B epoch to exceed owner A epoch: owner_a_epoch={} owner_b_epoch={}",
            owner_a.owner_epoch,
            owner_b.owner_epoch
        );
    }
    if current.owner_id != owner_b.owner_id || current.owner_epoch != owner_b.owner_epoch {
        bail!(
            "lease handoff probe expected current holder to be owner B at epoch {}, got owner_id={} epoch={}",
            owner_b.owner_epoch,
            current.owner_id,
            current.owner_epoch
        );
    }

    let release_owner_b = match release_owner_b {
        Ok(()) => LeaseReleaseEvidenceV1 {
            owner_id: owner_b.owner_id.clone(),
            owner_epoch: owner_b.owner_epoch,
            status: "released".to_string(),
            error: None,
        },
        Err(error) => LeaseReleaseEvidenceV1 {
            owner_id: owner_b.owner_id.clone(),
            owner_epoch: owner_b.owner_epoch,
            status: "release_failed".to_string(),
            error: Some(error),
        },
    };

    Ok(LeaseHandoffProbeArtifactV1 {
        schema_version: 1,
        evidence_kind: "ingest_writer_kubernetes_lease_release_handoff_probe".to_string(),
        status: "pass".to_string(),
        namespace: key.namespace.clone(),
        view_id: key.view_id.clone(),
        stream_id: key.stream_id.clone(),
        partition_id: key.partition_id,
        owner_a: owner_a.owner_id.clone(),
        owner_a_epoch: owner_a.owner_epoch,
        owner_b: owner_b.owner_id.clone(),
        owner_b_epoch: owner_b.owner_epoch,
        lease_identity: lease_identity.to_string(),
        leader_handoff_checked: false,
        product_complete_eligible: false,
        handoff_model: "single_process_release".to_string(),
        lease: LeaseHandoffProbeLeaseV1 {
            lease_identity: lease_identity.to_string(),
            namespace: key.namespace.clone(),
            view_id: key.view_id.clone(),
            stream_id: key.stream_id.clone(),
            partition_id: key.partition_id,
        },
        acquire_owner_a: lease_grant_evidence(&owner_a),
        release_owner_a: LeaseReleaseEvidenceV1 {
            owner_id: owner_a.owner_id,
            owner_epoch: owner_a.owner_epoch,
            status: "released".to_string(),
            error: None,
        },
        acquire_owner_b: lease_grant_evidence(&owner_b),
        verified_current_owner: lease_grant_evidence(&current),
        best_effort_release_owner_b: release_owner_b,
    })
}

fn lease_grant_evidence(grant: &PartitionLeaseGrant) -> LeaseGrantEvidenceV1 {
    LeaseGrantEvidenceV1 {
        owner_id: grant.owner_id.clone(),
        owner_epoch: grant.owner_epoch,
        expires_at_unix_ms: grant.expires_at_unix_ms,
    }
}

fn unix_ms() -> anyhow::Result<u64> {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .with_context(|| "system clock is before Unix epoch")?
            .as_millis(),
    )
    .with_context(|| "current Unix timestamp does not fit in u64 milliseconds")
}

fn encode_default_scores_payload(
    schema_fingerprint: &str,
    stream_id: &str,
    partition_id: u32,
    start_offset_inclusive: u64,
    rows_json: &str,
) -> anyhow::Result<Bytes> {
    let rows: Vec<DefaultScoreRow> = serde_json::from_str(rows_json)
        .with_context(|| "failed to parse --rows-json as default scores rows")?;
    if rows.is_empty() {
        bail!("default scores payload requires at least one row");
    }
    let end_offset_exclusive = start_offset_inclusive
        .checked_add(rows.len() as u64)
        .with_context(|| "default scores payload offset range overflow")?;

    let batch = default_scores_record_batch(&rows)?;
    IngestEnvelope::encode_batches(
        IngestEnvelopeEncodeRequest {
            relation_id: "scores".to_string(),
            relation_version: "2026-05-24.v1".to_string(),
            schema_fingerprint: schema_fingerprint.to_string(),
            stream_id: stream_id.to_string(),
            partition_id,
            start_offset_inclusive,
            end_offset_exclusive,
            event_time_watermark: None,
        },
        &[batch],
    )
    .map_err(anyhow::Error::from)
}

fn default_scores_record_batch(rows: &[DefaultScoreRow]) -> anyhow::Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("score", DataType::Int64, false),
        Field::new("delta", DataType::Int64, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.user_id.as_str())
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.score).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.delta).collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
    )
    .map_err(anyhow::Error::from)
}

async fn run_ingest_writer_append(
    store: Arc<dyn ObjectStore>,
    request: IngestWriterAppendRequest,
) -> anyhow::Result<IngestWriterAppendArtifactV1> {
    run_ingest_writer_append_with_commit_guard(store, request, None).await
}

async fn run_ingest_writer_append_with_commit_guard(
    store: Arc<dyn ObjectStore>,
    request: IngestWriterAppendRequest,
    commit_guard: Option<&dyn IngestCommitGuard>,
) -> anyhow::Result<IngestWriterAppendArtifactV1> {
    validate_ingest_writer_authority_store_id(&request.authority_store_id)?;
    if request.authority_namespace.trim().is_empty() {
        bail!("ingest-writer append requires --authority-namespace");
    }
    if request.operator_id.trim().is_empty() {
        bail!("ingest-writer append requires --operator-id");
    }
    if request.writer_id.trim().is_empty() {
        bail!("ingest-writer append requires --writer-id");
    }
    if request.payload.is_empty() {
        bail!("ingest-writer append requires a non-empty --payload-file");
    }

    let probe_id = sanitize_probe_id(&format!("{}-{}", request.operator_id, request.writer_id));
    let validated = validate_operator_authority(
        ObjectStoreAuthorityRef {
            store_id: request.authority_store_id.clone(),
            namespace: request.authority_namespace.clone(),
        },
        Arc::clone(&store),
        "ingest-writer-append",
        format!("v1/ingest-writer-capability-probes/{probe_id}"),
    )
    .await
    .map_err(anyhow::Error::from)?;
    let components = OperatorAuthorityStartupComponents::from_validated_authority(validated);
    let runtime = DeployedIngestWriterRuntime::from_startup_components(&components)
        .await
        .map_err(anyhow::Error::from)?;
    let startup_report = runtime.startup_report().clone();
    let expected_identity = ingest_identity_from_payload(request.payload.clone())?;
    let outcome = match commit_guard {
        Some(commit_guard) => {
            runtime
                .append_catalog_validated_envelope_with_commit_guard(request.payload, commit_guard)
                .await
        }
        None => {
            runtime
                .append_catalog_validated_envelope(request.payload)
                .await
        }
    }
    .map_err(anyhow::Error::from)?;
    if let AppendValidatedEnvelopeOutcome::Duplicate { descriptor } = &outcome {
        verify_committed_duplicate_identity(Arc::clone(&store), &expected_identity, descriptor)
            .await?;
    }
    let (outcome, descriptor) = ingest_writer_append_outcome_parts(outcome)?;

    Ok(IngestWriterAppendArtifactV1 {
        schema_version: 1,
        evidence_kind: "ingest_writer_checked_runtime_append".to_string(),
        status: "pass".to_string(),
        authority_store_id: request.authority_store_id,
        authority_namespace: request.authority_namespace,
        operator_id: request.operator_id,
        writer_id: request.writer_id,
        startup_active_admission_records: startup_report.active_admission_records,
        startup_expired_orphan_admission_records: startup_report.expired_orphan_admission_records,
        outcome,
        descriptor,
    })
}

fn lease_guarded_append_outcome_matches(expected_outcome: &str, actual_outcome: &str) -> bool {
    matches!(
        (expected_outcome, actual_outcome),
        ("appended", "appended")
            | ("duplicate", "duplicate")
            | ("appended-or-duplicate", "appended" | "duplicate")
    )
}

fn ingest_identity_from_payload(payload: Bytes) -> anyhow::Result<IngestIdentity> {
    let envelope = IngestEnvelope::decode(payload).map_err(anyhow::Error::from)?;
    let header = envelope.header();
    Ok(IngestIdentity {
        stream_id: header.stream_id.clone(),
        partition_id: header.partition_id,
        start_offset_inclusive: header.start_offset_inclusive,
        end_offset_exclusive: header.end_offset_exclusive,
        payload_digest: header.payload_digest.clone(),
        relation_id: header.relation_id.clone(),
        relation_version: header.relation_version.clone(),
        schema_fingerprint: header.schema_fingerprint.clone(),
    })
}

async fn verify_committed_duplicate_identity(
    store: Arc<dyn ObjectStore>,
    expected: &IngestIdentity,
    descriptor: &IngestBatchDescriptor,
) -> anyhow::Result<()> {
    let existing = store
        .get(&ObjectStorePath::from(descriptor.object_key.as_str()))
        .await
        .with_context(|| {
            format!(
                "failed to read committed duplicate batch {}",
                descriptor.object_key.as_str()
            )
        })?
        .bytes()
        .await?;
    let actual = ingest_identity_from_payload(existing)?;

    for (field, expected, actual) in [
        (
            "stream_id",
            expected.stream_id.clone(),
            actual.stream_id.clone(),
        ),
        (
            "partition_id",
            expected.partition_id.to_string(),
            actual.partition_id.to_string(),
        ),
        (
            "start_offset_inclusive",
            expected.start_offset_inclusive.to_string(),
            actual.start_offset_inclusive.to_string(),
        ),
        (
            "end_offset_exclusive",
            expected.end_offset_exclusive.to_string(),
            actual.end_offset_exclusive.to_string(),
        ),
        (
            "payload_digest",
            expected.payload_digest.clone(),
            actual.payload_digest.clone(),
        ),
        (
            "relation_id",
            expected.relation_id.clone(),
            actual.relation_id.clone(),
        ),
        (
            "relation_version",
            expected.relation_version.clone(),
            actual.relation_version.clone(),
        ),
        (
            "schema_fingerprint",
            expected.schema_fingerprint.clone(),
            actual.schema_fingerprint.clone(),
        ),
    ] {
        if expected != actual {
            bail!(
                "ingest-writer duplicate committed identity mismatch for {field}: expected={expected}, actual={actual}"
            );
        }
    }

    Ok(())
}

fn validate_ingest_writer_authority_store_id(authority_store_id: &str) -> anyhow::Result<()> {
    let trimmed = authority_store_id.trim();
    if trimmed.is_empty() {
        bail!("ingest-writer append requires --authority-store-id");
    }
    if trimmed.starts_with("file:")
        || trimmed.starts_with("local:")
        || trimmed.eq_ignore_ascii_case("local")
        || trimmed.eq_ignore_ascii_case("dev")
    {
        bail!(
            "ingest-writer append authority_store_id must not be local/dev: {authority_store_id}"
        );
    }
    Ok(())
}

fn ingest_writer_append_outcome_parts(
    outcome: AppendValidatedEnvelopeOutcome,
) -> anyhow::Result<(String, IngestWriterAppendDescriptorV1)> {
    match outcome {
        AppendValidatedEnvelopeOutcome::Appended { descriptor } => Ok((
            "appended".to_string(),
            ingest_writer_descriptor(&descriptor),
        )),
        AppendValidatedEnvelopeOutcome::Duplicate { descriptor } => Ok((
            "duplicate".to_string(),
            ingest_writer_descriptor(&descriptor),
        )),
        AppendValidatedEnvelopeOutcome::Conflict {
            descriptor,
            object_key,
            reason,
        } => bail!(
            "ingest-writer append conflicted before append: stream={} partition={} offsets={}-{} object_key={} reason={}",
            descriptor.stream_id,
            descriptor.partition_id,
            descriptor.start_offset_inclusive,
            descriptor.end_offset_exclusive,
            object_key.as_str(),
            reason
        ),
    }
}

fn ingest_writer_descriptor(descriptor: &IngestBatchDescriptor) -> IngestWriterAppendDescriptorV1 {
    IngestWriterAppendDescriptorV1 {
        stream_id: descriptor.stream_id.clone(),
        partition_id: descriptor.partition_id,
        start_offset_inclusive: descriptor.start_offset_inclusive,
        end_offset_exclusive: descriptor.end_offset_exclusive,
        object_key: descriptor.object_key.as_str().to_string(),
    }
}

fn s3_compatible_authority_store_from_env() -> anyhow::Result<Arc<dyn ObjectStore>> {
    s3_compatible_authority_store(s3_compatible_authority_config_from_lookup(|name| {
        env::var(name).ok()
    })?)
}

fn s3_compatible_authority_config_from_lookup(
    mut lookup: impl FnMut(&str) -> Option<String>,
) -> anyhow::Result<S3CompatibleAuthorityConfig> {
    if lookup("VELORIX_S3_COMPAT").as_deref() != Some("1") {
        bail!("S3-compatible authority requires VELORIX_S3_COMPAT=1");
    }

    let endpoint = required_s3_compatible_authority_env(&mut lookup, "AWS_ENDPOINT_URL")?;
    let allow_http = endpoint.starts_with("http://");
    let force_path_style =
        bool_s3_compatible_authority_env(&mut lookup, "VELORIX_S3_FORCE_PATH_STYLE", true)?;

    Ok(S3CompatibleAuthorityConfig {
        endpoint,
        access_key_id: required_s3_compatible_authority_env(&mut lookup, "AWS_ACCESS_KEY_ID")?,
        secret_access_key: required_s3_compatible_authority_env(
            &mut lookup,
            "AWS_SECRET_ACCESS_KEY",
        )?,
        session_token: optional_s3_compatible_authority_env(&mut lookup, "AWS_SESSION_TOKEN"),
        region: required_s3_compatible_authority_env(&mut lookup, "AWS_REGION")?,
        bucket: required_s3_compatible_authority_env(&mut lookup, "VELORIX_S3_BUCKET")?,
        prefix: lookup("VELORIX_S3_PREFIX").unwrap_or_default(),
        allow_http,
        force_path_style,
    })
}

fn optional_s3_compatible_authority_env(
    lookup: &mut impl FnMut(&str) -> Option<String>,
    name: &str,
) -> Option<String> {
    lookup(name)
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn bool_s3_compatible_authority_env(
    lookup: &mut impl FnMut(&str) -> Option<String>,
    name: &str,
    default: bool,
) -> anyhow::Result<bool> {
    match lookup(name).map(|value| value.trim().to_string()) {
        Some(value) if value == "1" || value == "true" => Ok(true),
        Some(value) if value == "0" || value == "false" => Ok(false),
        Some(value) if value.is_empty() => Ok(default),
        Some(value) => bail!("{name} must be 0, 1, true, or false; got `{value}`"),
        None => Ok(default),
    }
}

fn required_s3_compatible_authority_env(
    lookup: &mut impl FnMut(&str) -> Option<String>,
    name: &str,
) -> anyhow::Result<String> {
    lookup(name)
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .with_context(|| format!("S3-compatible authority requires {name}"))
}

fn s3_compatible_authority_store(
    config: S3CompatibleAuthorityConfig,
) -> anyhow::Result<Arc<dyn ObjectStore>> {
    let builder = AmazonS3Builder::new()
        .with_endpoint(config.endpoint)
        .with_access_key_id(config.access_key_id)
        .with_secret_access_key(config.secret_access_key)
        .with_region(config.region)
        .with_bucket_name(config.bucket)
        .with_allow_http(config.allow_http)
        .with_virtual_hosted_style_request(!config.force_path_style);
    let builder = if let Some(session_token) = config.session_token {
        builder.with_token(session_token)
    } else {
        builder
    };
    let store = builder.build().map_err(anyhow::Error::from)?;

    let prefix = config.prefix.trim().trim_matches('/').to_string();
    if prefix.is_empty() {
        Ok(Arc::new(store))
    } else {
        Ok(Arc::new(PrefixStore::new(
            store,
            ObjectStorePath::from(prefix),
        )))
    }
}

fn format_ingest_writer_append(artifact: &IngestWriterAppendArtifactV1) -> String {
    format!(
        "ingest_writer_append status={} outcome={} authority_store_id={} authority_namespace={} operator_id={} writer_id={} stream_id={} partition_id={} offsets={}-{} object_key={}\n",
        artifact.status,
        artifact.outcome,
        artifact.authority_store_id,
        artifact.authority_namespace,
        artifact.operator_id,
        artifact.writer_id,
        artifact.descriptor.stream_id,
        artifact.descriptor.partition_id,
        artifact.descriptor.start_offset_inclusive,
        artifact.descriptor.end_offset_exclusive,
        artifact.descriptor.object_key
    )
}

fn sanitize_probe_id(value: &str) -> String {
    let probe_id = value
        .trim()
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.') {
                character
            } else {
                '-'
            }
        })
        .collect::<String>();
    if probe_id.is_empty() {
        "unknown".to_string()
    } else {
        probe_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_partition_authority_session_for_token(
        meta_store: Arc<dyn MetaStore>,
        key: PartitionAuthorityKey,
        owner_id: &str,
        token: PartitionAuthorityToken,
    ) -> PartitionAuthoritySession {
        let (cancellation, _) = watch::channel(false);
        PartitionAuthoritySession {
            meta_store,
            key: key.clone(),
            owner_id: owner_id.to_string(),
            admission_binding: IngestCommitGuardBindingV1::new(
                "meta_partition_authority",
                partition_authority_identity(&key),
                owner_id,
                token.owner_epoch,
            ),
            token: Arc::new(RwLock::new(token)),
            fenced: Arc::new(AtomicBool::new(false)),
            safety_deadline: Arc::new(RwLock::new(Instant::now() + Duration::from_secs(60))),
            cancellation,
            renewal_task: Arc::new(Mutex::new(None)),
            ttl: Duration::from_secs(60),
            rpc_timeout: Duration::from_secs(1),
        }
    }

    async fn test_partition_authority_session() -> PartitionAuthoritySession {
        let store = Arc::new(velorix_meta::InMemoryMetaStore::default());
        let key = PartitionAuthorityKey {
            namespace: "tenant-a".to_string(),
            view_id: "scores-view".to_string(),
            stream_id: "scores".to_string(),
            partition_id: 0,
        };
        let outcome = store
            .acquire_partition_authority(AcquirePartitionAuthorityRequest {
                key: key.clone(),
                owner_id: "writer-a".to_string(),
                current_token: None,
                ttl_ms: 60_000,
            })
            .await
            .unwrap();
        let AcquirePartitionAuthorityOutcome::Acquired(token) = outcome else {
            panic!("test store must acquire an authority token");
        };
        let (cancellation, _) = watch::channel(false);
        PartitionAuthoritySession {
            meta_store: store,
            key,
            owner_id: "writer-a".to_string(),
            token: Arc::new(RwLock::new(token)),
            admission_binding: IngestCommitGuardBindingV1::new(
                "meta_partition_authority",
                "tenant-a/scores-view/scores/p0",
                "writer-a",
                1,
            ),
            fenced: Arc::new(AtomicBool::new(false)),
            safety_deadline: Arc::new(RwLock::new(Instant::now() + Duration::from_secs(60))),
            cancellation,
            renewal_task: Arc::new(Mutex::new(None)),
            ttl: Duration::from_secs(60),
            rpc_timeout: Duration::from_secs(1),
        }
    }

    fn test_partition_descriptor() -> IngestBatchDescriptor {
        IngestBatch::from_validated_envelope(
            encode_default_scores_payload(
                &format!("sha256:{}", "a".repeat(64)),
                "scores",
                0,
                0,
                r#"[{"user_id":"u1","score":1,"delta":1}]"#,
            )
            .unwrap(),
        )
        .unwrap()
        .descriptor()
    }

    #[tokio::test]
    async fn meta_authority_fence_rejects_admission_and_commit_with_exact_binding() {
        let session = test_partition_authority_session().await;
        let descriptor = test_partition_descriptor();
        let binding = session.admission_binding(&descriptor).unwrap();
        assert_eq!(binding.binding_kind, "meta_partition_authority");
        assert_eq!(binding.subject, "tenant-a/scores-view/scores/p0");
        assert_eq!(binding.owner_id, "writer-a");
        assert_eq!(binding.owner_epoch, 1);

        session.fence();
        for phase in [
            IngestCommitGuardPhase::BeforeAdmission,
            IngestCommitGuardPhase::BeforeCommit,
        ] {
            let error = session.verify(phase, &descriptor).await.unwrap_err();
            assert!(error.contains("fenced"), "{error}");
        }
    }

    #[tokio::test]
    async fn meta_authority_stale_token_and_local_deadline_fence_without_watch() {
        let session = test_partition_authority_session().await;
        let descriptor = test_partition_descriptor();
        session.token.write().await.owner_epoch = 9;
        let error = session
            .verify(IngestCommitGuardPhase::BeforeAdmission, &descriptor)
            .await
            .unwrap_err();
        assert!(error.contains("stale"), "{error}");
        assert!(session.fenced.load(Ordering::SeqCst));

        let session = test_partition_authority_session().await;
        *session.safety_deadline.write().await = Instant::now();
        let error = session
            .verify(IngestCommitGuardPhase::BeforeCommit, &descriptor)
            .await
            .unwrap_err();
        assert!(error.contains("deadline"), "{error}");
        assert!(session.fenced.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn renewal_scheduling_pause_fences_with_nonzero_rpc_timeout() {
        let session = test_partition_authority_session().await;
        let session = PartitionAuthoritySession {
            ttl: Duration::from_secs(60),
            rpc_timeout: Duration::from_millis(10),
            ..session
        };
        // Model a writer that was not scheduled until after its monotonic
        // safety window elapsed. The renewal task must fence before issuing a
        // renewal RPC, regardless of the nonzero RPC timeout.
        *session.safety_deadline.write().await = Instant::now() - Duration::from_millis(1);
        session
            .start_renewal(session.cancellation.subscribe())
            .await;
        tokio::task::yield_now().await;
        assert!(session.fenced.load(Ordering::SeqCst));
        session.shutdown().await;
    }

    #[tokio::test]
    async fn delayed_success_cannot_extend_an_rpc_start_deadline() {
        let started = Instant::now();
        let deadline = authority_safety_deadline(started, 30);
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(Instant::now() >= deadline);

        // The session uses the deadline derived above, rather than deriving a
        // fresh TTL from a delayed Meta success response.
        let session = test_partition_authority_session().await;
        *session.safety_deadline.write().await = deadline;
        let error = session
            .ensure_local_safety("delayed_success")
            .await
            .unwrap_err();
        assert!(error.contains("deadline"), "{error}");
        assert!(session.fenced.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn admission_binding_is_available_while_renewal_token_lock_is_held() {
        let session = test_partition_authority_session().await;
        let expected = session
            .admission_binding(&test_partition_descriptor())
            .unwrap();
        let token_lock = session.token.write().await;
        let binding = session
            .admission_binding(&test_partition_descriptor())
            .unwrap();
        assert_eq!(binding, expected);
        drop(token_lock);
    }

    #[tokio::test]
    async fn stale_meta_token_rejects_both_guard_phases_and_pointer_publish() {
        let store = Arc::new(velorix_meta::InMemoryMetaStore::default());
        store.set_partition_authority_clock_for_test(10).await;
        let key = PartitionAuthorityKey {
            namespace: "tenant-a".to_string(),
            view_id: "scores-view".to_string(),
            stream_id: "scores".to_string(),
            partition_id: 0,
        };
        let AcquirePartitionAuthorityOutcome::Acquired(token_a) = store
            .acquire_partition_authority(AcquirePartitionAuthorityRequest {
                key: key.clone(),
                owner_id: "owner-a".to_string(),
                current_token: None,
                ttl_ms: 10,
            })
            .await
            .unwrap()
        else {
            panic!("owner A should acquire");
        };
        store.set_partition_authority_clock_for_test(21).await;
        let AcquirePartitionAuthorityOutcome::Acquired(token_b) = store
            .acquire_partition_authority(AcquirePartitionAuthorityRequest {
                key: key.clone(),
                owner_id: "owner-b".to_string(),
                current_token: None,
                ttl_ms: 60_000,
            })
            .await
            .unwrap()
        else {
            panic!("owner B should take over");
        };
        assert!(token_b.owner_epoch > token_a.owner_epoch);
        let (cancellation, _) = watch::channel(false);
        let session = PartitionAuthoritySession {
            meta_store: Arc::clone(&store) as Arc<dyn MetaStore>,
            key: key.clone(),
            owner_id: "owner-a".to_string(),
            token: Arc::new(RwLock::new(token_a.clone())),
            admission_binding: IngestCommitGuardBindingV1::new(
                "meta_partition_authority",
                partition_authority_identity(&key),
                "owner-a",
                token_a.owner_epoch,
            ),
            fenced: Arc::new(AtomicBool::new(false)),
            safety_deadline: Arc::new(RwLock::new(Instant::now() + Duration::from_secs(60))),
            cancellation,
            renewal_task: Arc::new(Mutex::new(None)),
            ttl: Duration::from_secs(60),
            rpc_timeout: Duration::from_secs(1),
        };
        let descriptor = test_partition_descriptor();
        assert!(session
            .verify(IngestCommitGuardPhase::BeforeAdmission, &descriptor)
            .await
            .is_err());
        assert!(session
            .verify(IngestCommitGuardPhase::BeforeCommit, &descriptor)
            .await
            .is_err());
        assert!(session.fenced.load(Ordering::SeqCst));
        let before = store.read_partition_checkpoint_pointer(&key).await.unwrap();
        assert!(store
            .publish_partition_checkpoint_pointer(PublishPartitionCheckpointPointerRequest {
                expected_previous: before.clone(),
                candidate: PartitionCheckpointPointer {
                    key: key.clone(),
                    checkpoint_key: "v1/diagnostics/stale".to_string()
                },
                authority: token_a,
            })
            .await
            .is_err());
        assert_eq!(
            store.read_partition_checkpoint_pointer(&key).await.unwrap(),
            before
        );
    }

    #[tokio::test]
    async fn meta_server_time_ignores_writer_clock_skew_in_both_directions() {
        const FIVE_MINUTES_MS: u64 = 5 * 60 * 1_000;
        let writer_now = unix_ms().unwrap();

        for (label, server_now) in [
            (
                "writer_clock_plus_five_minutes",
                writer_now - FIVE_MINUTES_MS,
            ),
            (
                "writer_clock_minus_five_minutes",
                writer_now + FIVE_MINUTES_MS,
            ),
        ] {
            let store = Arc::new(velorix_meta::InMemoryMetaStore::default());
            store
                .set_partition_authority_clock_for_test(server_now)
                .await;
            let key = PartitionAuthorityKey {
                namespace: format!("tenant-{label}"),
                view_id: "scores-view".to_string(),
                stream_id: "scores".to_string(),
                partition_id: 0,
            };
            let AcquirePartitionAuthorityOutcome::Acquired(token) = store
                .acquire_partition_authority(AcquirePartitionAuthorityRequest {
                    key: key.clone(),
                    owner_id: "writer-a".to_string(),
                    current_token: None,
                    ttl_ms: 60_000,
                })
                .await
                .unwrap()
            else {
                panic!("{label} should acquire authority");
            };
            let session = test_partition_authority_session_for_token(
                Arc::clone(&store) as Arc<dyn MetaStore>,
                key,
                "writer-a",
                token,
            );
            let descriptor = test_partition_descriptor();

            for phase in [
                IngestCommitGuardPhase::BeforeAdmission,
                IngestCommitGuardPhase::BeforeCommit,
            ] {
                session.verify(phase, &descriptor).await.unwrap();
            }
            assert!(
                !session.fenced.load(Ordering::SeqCst),
                "{label} must not fence a Meta-current authority token"
            );
        }
    }

    #[tokio::test]
    async fn writer_clock_behind_fences_both_phases_after_meta_expiry_without_takeover() {
        const FIVE_MINUTES_MS: u64 = 5 * 60 * 1_000;
        const TTL_MS: u64 = 60_000;
        let writer_now = unix_ms().unwrap();
        let meta_now = writer_now + FIVE_MINUTES_MS;
        let store = Arc::new(velorix_meta::InMemoryMetaStore::default());
        store.set_partition_authority_clock_for_test(meta_now).await;
        let key = PartitionAuthorityKey {
            namespace: "tenant-writer-behind".to_string(),
            view_id: "scores-view".to_string(),
            stream_id: "scores".to_string(),
            partition_id: 0,
        };
        let AcquirePartitionAuthorityOutcome::Acquired(token) = store
            .acquire_partition_authority(AcquirePartitionAuthorityRequest {
                key: key.clone(),
                owner_id: "writer-a".to_string(),
                current_token: None,
                ttl_ms: TTL_MS,
            })
            .await
            .unwrap()
        else {
            panic!("writer A should acquire authority");
        };
        store
            .set_partition_authority_clock_for_test(meta_now + TTL_MS)
            .await;
        assert!(
            store
                .read_partition_authority(&key)
                .await
                .unwrap()
                .is_none(),
            "the Meta clock alone must expire the token without an owner handoff"
        );

        let descriptor = test_partition_descriptor();
        for phase in [
            IngestCommitGuardPhase::BeforeAdmission,
            IngestCommitGuardPhase::BeforeCommit,
        ] {
            let session = test_partition_authority_session_for_token(
                Arc::clone(&store) as Arc<dyn MetaStore>,
                key.clone(),
                "writer-a",
                token.clone(),
            );
            let error = session.verify(phase, &descriptor).await.unwrap_err();
            assert!(error.contains("stale"), "{error}");
            assert!(
                session.fenced.load(Ordering::SeqCst),
                "writer-behind must fence at {} after Meta expiry",
                phase.as_str()
            );
        }
    }

    #[tokio::test]
    async fn renewal_loss_fences_writer_and_blocks_admission_commit_and_checkpoint_publication() {
        let store = Arc::new(velorix_meta::InMemoryMetaStore::default());
        store.set_partition_authority_clock_for_test(10).await;
        let key = PartitionAuthorityKey {
            namespace: "tenant-a".to_string(),
            view_id: "scores-view".to_string(),
            stream_id: "scores".to_string(),
            partition_id: 0,
        };
        let AcquirePartitionAuthorityOutcome::Acquired(token_a) = store
            .acquire_partition_authority(AcquirePartitionAuthorityRequest {
                key: key.clone(),
                owner_id: "writer-a".to_string(),
                current_token: None,
                ttl_ms: 10,
            })
            .await
            .unwrap()
        else {
            panic!("writer A should acquire");
        };
        let session = test_partition_authority_session_for_token(
            Arc::clone(&store) as Arc<dyn MetaStore>,
            key.clone(),
            "writer-a",
            token_a.clone(),
        );

        store.set_partition_authority_clock_for_test(20).await;
        let AcquirePartitionAuthorityOutcome::Acquired(token_b) = store
            .acquire_partition_authority(AcquirePartitionAuthorityRequest {
                key: key.clone(),
                owner_id: "writer-b".to_string(),
                current_token: None,
                ttl_ms: 60_000,
            })
            .await
            .unwrap()
        else {
            panic!("writer B should acquire after Meta expiry");
        };
        assert!(token_b.owner_epoch > token_a.owner_epoch);

        let renewal_error = session.renew_once().await.unwrap_err();
        assert!(renewal_error.contains("lost ownership"), "{renewal_error}");
        assert!(session.fenced.load(Ordering::SeqCst));

        let descriptor = test_partition_descriptor();
        for phase in [
            IngestCommitGuardPhase::BeforeAdmission,
            IngestCommitGuardPhase::BeforeCommit,
        ] {
            let error = session.verify(phase, &descriptor).await.unwrap_err();
            assert!(error.contains("fenced"), "{error}");
        }

        let before = store.read_partition_checkpoint_pointer(&key).await.unwrap();
        let stale_publish = store
            .publish_partition_checkpoint_pointer(PublishPartitionCheckpointPointerRequest {
                expected_previous: before.clone(),
                candidate: PartitionCheckpointPointer {
                    key: key.clone(),
                    checkpoint_key: "v1/checkpoints/stale-writer-a".to_string(),
                },
                authority: token_a,
            })
            .await;
        assert!(stale_publish.is_err());
        assert_eq!(
            store.read_partition_checkpoint_pointer(&key).await.unwrap(),
            before
        );

        let owner_b_publish = store
            .publish_partition_checkpoint_pointer(PublishPartitionCheckpointPointerRequest {
                expected_previous: None,
                candidate: PartitionCheckpointPointer {
                    key,
                    checkpoint_key: "v1/checkpoints/owner-b".to_string(),
                },
                authority: token_b,
            })
            .await
            .unwrap();
        assert_eq!(
            owner_b_publish,
            velorix_meta::PublishPartitionCheckpointPointerOutcome::Published
        );
    }

    #[test]
    fn default_scores_payload_encodes_expected_envelope() {
        let payload = encode_default_scores_payload(
            &format!("sha256:{}", "1".repeat(64)),
            "scores",
            2,
            10,
            r#"[{"user_id":"u3","score":11,"delta":1},{"user_id":"u2","score":-4,"delta":1}]"#,
        )
        .unwrap();
        let envelope = IngestEnvelope::decode(payload).unwrap();

        assert_eq!(envelope.header().relation_id, "scores");
        assert_eq!(envelope.header().relation_version, "2026-05-24.v1");
        assert_eq!(envelope.header().stream_id, "scores");
        assert_eq!(envelope.header().partition_id, 2);
        assert_eq!(envelope.header().start_offset_inclusive, 10);
        assert_eq!(envelope.header().end_offset_exclusive, 12);
        assert_eq!(envelope.record_batches().unwrap()[0].num_rows(), 2);
    }

    #[test]
    fn s3_compatible_authority_config_reads_required_env_when_enabled() {
        let config = s3_compatible_authority_config_from_lookup(|name| match name {
            "VELORIX_S3_COMPAT" => Some("1".to_string()),
            "AWS_ENDPOINT_URL" => Some("http://rustfs:9000".to_string()),
            "AWS_ACCESS_KEY_ID" => Some("access".to_string()),
            "AWS_SECRET_ACCESS_KEY" => Some("secret".to_string()),
            "AWS_REGION" => Some("us-east-1".to_string()),
            "VELORIX_S3_BUCKET" => Some("velorix".to_string()),
            "VELORIX_S3_PREFIX" => Some("/tenant-a/".to_string()),
            _ => None,
        })
        .unwrap();

        assert_eq!(
            config,
            S3CompatibleAuthorityConfig {
                endpoint: "http://rustfs:9000".to_string(),
                access_key_id: "access".to_string(),
                secret_access_key: "secret".to_string(),
                session_token: None,
                region: "us-east-1".to_string(),
                bucket: "velorix".to_string(),
                prefix: "/tenant-a/".to_string(),
                allow_http: true,
                force_path_style: true,
            }
        );
    }

    #[test]
    fn s3_compatible_authority_config_can_disable_path_style() {
        let config = s3_compatible_authority_config_from_lookup(|name| match name {
            "VELORIX_S3_COMPAT" => Some("1".to_string()),
            "AWS_ENDPOINT_URL" => Some("https://s3.amazonaws.com".to_string()),
            "AWS_ACCESS_KEY_ID" => Some("access".to_string()),
            "AWS_SECRET_ACCESS_KEY" => Some("secret".to_string()),
            "AWS_SESSION_TOKEN" => Some("token".to_string()),
            "AWS_REGION" => Some("us-east-1".to_string()),
            "VELORIX_S3_BUCKET" => Some("velorix".to_string()),
            "VELORIX_S3_FORCE_PATH_STYLE" => Some("0".to_string()),
            _ => None,
        })
        .unwrap();

        assert!(!config.force_path_style);
        assert!(!config.allow_http);
        assert_eq!(config.session_token.as_deref(), Some("token"));
    }

    #[test]
    fn s3_compatible_authority_config_rejects_missing_gate() {
        let error = s3_compatible_authority_config_from_lookup(|_| None).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("S3-compatible authority requires VELORIX_S3_COMPAT=1"),
            "{error}"
        );
    }

    #[test]
    fn authority_store_id_rejects_local_values() {
        let error = validate_ingest_writer_authority_store_id("local").unwrap_err();
        assert!(
            error.to_string().contains("must not be local/dev"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn duplicate_retry_succeeds_only_for_the_identical_committed_identity() {
        let schema_fingerprint = format!("sha256:{}", "1".repeat(64));
        let committed_payload = encode_default_scores_payload(
            &schema_fingerprint,
            "scores",
            0,
            10,
            r#"[{"user_id":"u1","score":7,"delta":1}]"#,
        )
        .unwrap();
        let retry_payload = committed_payload.clone();
        let conflicting_payload = encode_default_scores_payload(
            &schema_fingerprint,
            "scores",
            0,
            10,
            r#"[{"user_id":"u1","score":8,"delta":1}]"#,
        )
        .unwrap();
        let descriptor = IngestBatch::from_validated_envelope(committed_payload.clone())
            .unwrap()
            .descriptor();
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        store
            .put(
                &ObjectStorePath::from(descriptor.object_key.as_str()),
                committed_payload.into(),
            )
            .await
            .unwrap();

        verify_committed_duplicate_identity(
            Arc::clone(&store),
            &ingest_identity_from_payload(retry_payload).unwrap(),
            &descriptor,
        )
        .await
        .unwrap();

        let error = verify_committed_duplicate_identity(
            store,
            &ingest_identity_from_payload(conflicting_payload).unwrap(),
            &descriptor,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("payload_digest"), "{error}");
    }

    #[tokio::test]
    async fn guarded_duplicate_retry_accepts_a_handoff_only_with_original_immutable_evidence() {
        let schema_fingerprint = format!("sha256:{}", "2".repeat(64));
        let payload = encode_default_scores_payload(
            &schema_fingerprint,
            "scores",
            0,
            20,
            r#"[{"user_id":"u1","score":7,"delta":1}]"#,
        )
        .unwrap();
        let conflicting_payload = encode_default_scores_payload(
            &schema_fingerprint,
            "scores",
            0,
            20,
            r#"[{"user_id":"u1","score":8,"delta":1}]"#,
        )
        .unwrap();
        let conflicting_range_payload = encode_default_scores_payload(
            &schema_fingerprint,
            "scores",
            0,
            21,
            r#"[{"user_id":"u1","score":7,"delta":1}]"#,
        )
        .unwrap();
        let identity = ingest_identity_from_payload(payload.clone()).unwrap();
        let batch_descriptor = IngestBatch::from_validated_envelope(payload.clone())
            .unwrap()
            .descriptor();
        let descriptor = ingest_writer_descriptor(&batch_descriptor);
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        store
            .put(
                &ObjectStorePath::from(batch_descriptor.object_key.as_str()),
                payload.into(),
            )
            .await
            .unwrap();

        let lease_identity = "velorix-product/scores-view/scores/p0";
        let mut original_admission = DurableIngestAdmissionRecordV1::for_external_admission(
            identity.stream_id.clone(),
            identity.partition_id,
            identity.start_offset_inclusive,
            identity.end_offset_exclusive,
            identity.payload_digest.clone(),
            identity.relation_id.clone(),
            identity.relation_version.clone(),
            identity.schema_fingerprint.clone(),
        )
        .unwrap();
        original_admission.commit_guard_binding = Some(IngestCommitGuardBindingV1::new(
            "meta_partition_authority",
            lease_identity,
            "owner-a",
            1,
        ));
        store
            .put(
                &ObjectStorePath::from(original_admission.admission_record_key.as_str()),
                Bytes::from(serde_json::to_vec(&original_admission).unwrap()).into(),
            )
            .await
            .unwrap();

        // Owner B at epoch 2 may acknowledge the immutable owner-A commit after handoff.
        let original_binding = verify_duplicate_admission_evidence(
            Arc::clone(&store),
            &descriptor,
            &identity,
            lease_identity,
        )
        .await
        .unwrap();
        assert_eq!(original_binding.owner_id, "owner-a");
        assert_eq!(original_binding.owner_epoch, 1);
        assert!(lease_guarded_append_outcome_matches(
            "appended-or-duplicate",
            "duplicate"
        ));

        let error = verify_duplicate_admission_evidence(
            Arc::clone(&store),
            &descriptor,
            &ingest_identity_from_payload(conflicting_payload).unwrap(),
            lease_identity,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("payload_digest"), "{error}");

        let error = verify_duplicate_admission_evidence(
            store,
            &descriptor,
            &ingest_identity_from_payload(conflicting_range_payload).unwrap(),
            lease_identity,
        )
        .await
        .unwrap_err();
        assert!(
            error.to_string().contains("start_offset_inclusive"),
            "{error}"
        );
    }

    #[test]
    fn entrypoint_and_expected_outcomes_allow_a_lost_response_retry() {
        let entrypoint = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../scripts/velorix-ingest-writer-entrypoint.sh"
        ));

        assert!(entrypoint.contains("--expected-outcome appended-or-duplicate"));
        assert!(lease_guarded_append_outcome_matches(
            "appended-or-duplicate",
            "appended"
        ));
        assert!(lease_guarded_append_outcome_matches(
            "appended-or-duplicate",
            "duplicate"
        ));
        assert!(!lease_guarded_append_outcome_matches(
            "appended-or-duplicate",
            "conflict"
        ));
        assert!(!lease_guarded_append_outcome_matches(
            "appended",
            "duplicate"
        ));
    }

    #[test]
    fn lease_handoff_probe_artifact_records_verified_owner_b_with_higher_epoch() {
        let key = PartitionLeaseKey {
            namespace: "velorix-live".to_string(),
            view_id: "product-readiness".to_string(),
            stream_id: "ingest-writer-handoff".to_string(),
            partition_id: 0,
        };
        let owner_a = PartitionLeaseGrant {
            key: key.clone(),
            owner_id: "writer-a".to_string(),
            owner_epoch: 1,
            expires_at_unix_ms: 1_700_000_060_000,
        };
        let owner_b = PartitionLeaseGrant {
            key: key.clone(),
            owner_id: "writer-b".to_string(),
            owner_epoch: 2,
            expires_at_unix_ms: 1_700_000_061_000,
        };

        let artifact = lease_handoff_probe_artifact(
            &key,
            "coordination.k8s.io/v1/namespaces/velorix-live/leases/velorix-product-readiness-ingest-writer-handoff-p0",
            owner_a,
            owner_b.clone(),
            owner_b,
            Ok(()),
        )
        .unwrap();

        assert_eq!(artifact.schema_version, 1);
        assert_eq!(
            artifact.evidence_kind,
            "ingest_writer_kubernetes_lease_release_handoff_probe"
        );
        assert_eq!(artifact.status, "pass");
        assert!(!artifact.leader_handoff_checked);
        assert!(!artifact.product_complete_eligible);
        assert_eq!(artifact.handoff_model, "single_process_release");
        assert_eq!(artifact.owner_a, "writer-a");
        assert_eq!(artifact.owner_a_epoch, 1);
        assert_eq!(artifact.owner_b, "writer-b");
        assert_eq!(artifact.owner_b_epoch, 2);
        assert_eq!(artifact.lease.namespace, "velorix-live");
        assert_eq!(artifact.acquire_owner_a.owner_id, "writer-a");
        assert_eq!(artifact.acquire_owner_a.owner_epoch, 1);
        assert_eq!(artifact.acquire_owner_b.owner_id, "writer-b");
        assert_eq!(artifact.acquire_owner_b.owner_epoch, 2);
        assert_eq!(artifact.verified_current_owner.owner_id, "writer-b");
        assert_eq!(artifact.best_effort_release_owner_b.status, "released");
    }
}
