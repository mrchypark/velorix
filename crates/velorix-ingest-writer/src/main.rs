#![forbid(unsafe_code)]

use std::{
    env, fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{SystemTime, UNIX_EPOCH},
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
use velorix_control::lease::{
    LeaseAcquireRequest, PartitionLeaseClient, PartitionLeaseGrant, PartitionLeaseKey,
};
use velorix_k8s::{
    crd::ObjectStoreAuthorityRef,
    ingest_writer::DeployedIngestWriterRuntime,
    lease::{partition_lease_identity, KubeLeaseApi, KubernetesPartitionLeaseClient},
    startup::{validate_operator_authority, OperatorAuthorityStartupComponents},
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

#[derive(Clone)]
struct KubernetesLeaseCommitGuard {
    lease_client: KubernetesPartitionLeaseClient<KubeLeaseApi>,
    lease_key: PartitionLeaseKey,
    owner_id: String,
    owner_epoch: u64,
}

#[async_trait]
impl IngestCommitGuard for KubernetesLeaseCommitGuard {
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
        verify_kubernetes_lease_holder(
            &self.lease_client,
            &self.lease_key,
            &self.owner_id,
            self.owner_epoch,
            phase,
        )
        .await
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
    validate_kubernetes_lease_probe_request(&request.lease_key, &request.owner_id, request.ttl_ms)?;
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
    if !matches!(expected_outcome, "appended" | "stale-owner-rejected") {
        bail!(
            "lease-guarded append probe expected-outcome must be appended or stale-owner-rejected"
        );
    }
    if !request.acquire_lease && request.expected_owner_epoch.is_none() {
        bail!("lease-guarded append requires --acquire-lease or --expected-owner-epoch");
    }

    let descriptor = ingest_writer_descriptor(
        &IngestBatch::from_validated_envelope(request.payload.clone())?.descriptor(),
    );
    if request.lease_key.stream_id != descriptor.stream_id
        || request.lease_key.partition_id != descriptor.partition_id
    {
        bail!(
            "lease-guarded append lease key must match payload stream/partition: lease={}/p{} payload={}/p{}",
            request.lease_key.stream_id,
            request.lease_key.partition_id,
            descriptor.stream_id,
            descriptor.partition_id
        );
    }
    let client = Client::try_default()
        .await
        .with_context(|| "failed to create Kubernetes client from runtime environment")?;
    let lease_client = KubernetesPartitionLeaseClient::new(KubeLeaseApi::new(client));
    let lease_identity = partition_lease_identity(&request.lease_key);
    let acquired_grant = if request.acquire_lease {
        Some(
            lease_client
                .acquire_or_renew(LeaseAcquireRequest {
                    key: request.lease_key.clone(),
                    owner_id: request.owner_id.clone(),
                    now_unix_ms: unix_ms()?,
                    ttl_ms: request.ttl_ms,
                })
                .await
                .map_err(anyhow::Error::from)
                .with_context(|| "failed to acquire Kubernetes lease before guarded append")?,
        )
    } else {
        None
    };
    let current_owner = lease_client
        .current(&request.lease_key, unix_ms()?)
        .await
        .map_err(anyhow::Error::from)
        .with_context(|| "failed to read current Kubernetes lease holder before guarded append")?;

    let expected_owner_epoch = acquired_grant
        .as_ref()
        .map(|grant| grant.owner_epoch)
        .or(request.expected_owner_epoch);
    let current_matches_owner = current_owner
        .as_ref()
        .map(|grant| {
            grant.owner_id == request.owner_id
                && expected_owner_epoch
                    .map(|epoch| grant.owner_epoch == epoch)
                    .unwrap_or(true)
        })
        .unwrap_or(false);
    if !current_matches_owner {
        if expected_outcome != "stale-owner-rejected" {
            bail!(
                "lease-guarded append expected owner {}, but current owner was {:?}",
                request.owner_id,
                current_owner.as_ref().map(|grant| &grant.owner_id)
            );
        }
        return Ok(LeaseGuardedAppendProbeArtifactV1 {
            schema_version: 1,
            evidence_kind: "ingest_writer_lease_guarded_append_probe".to_string(),
            status: "pass".to_string(),
            expected_outcome: expected_outcome.to_string(),
            outcome: "stale-owner-rejected".to_string(),
            authority_store_id: request.authority_store_id,
            authority_namespace: request.authority_namespace,
            operator_id: request.operator_id,
            writer_id: request.writer_id,
            lease_identity,
            owner_id: request.owner_id,
            expected_owner_epoch,
            acquired_grant: acquired_grant.as_ref().map(lease_grant_evidence),
            current_owner: current_owner.as_ref().map(lease_grant_evidence),
            post_append_current_owner: None,
            commit_guard_enforced: false,
            admission_commit_guard_bound: false,
            admission_commit_guard_binding: None,
            lease_held_through_append: false,
            stale_owner_rejected: true,
            append_completed: false,
            descriptor,
        });
    }

    if expected_outcome == "stale-owner-rejected" {
        bail!("lease-guarded append expected stale-owner rejection, but owner still holds lease");
    }

    let commit_owner_epoch = expected_owner_epoch
        .ok_or_else(|| anyhow::anyhow!("lease-guarded append requires an owner epoch"))?;
    let commit_guard = KubernetesLeaseCommitGuard {
        lease_client: lease_client.clone(),
        lease_key: request.lease_key.clone(),
        owner_id: request.owner_id.clone(),
        owner_epoch: commit_owner_epoch,
    };
    let expected_commit_guard_binding = commit_guard
        .admission_binding(
            &IngestBatch::from_validated_envelope(request.payload.clone())?.descriptor(),
        )
        .ok_or_else(|| {
            anyhow::anyhow!("lease commit guard did not provide an admission binding")
        })?;
    let append_artifact = run_ingest_writer_append_with_commit_guard(
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
    .await?;
    if append_artifact.outcome != "appended" {
        bail!(
            "lease-guarded append expected a fresh append outcome, got {}",
            append_artifact.outcome
        );
    }
    let admission_commit_guard_binding = read_admission_commit_guard_binding(
        Arc::clone(&store),
        &append_artifact.descriptor,
        &expected_commit_guard_binding,
    )
    .await?;
    let post_append_current_owner = lease_client
        .current(&request.lease_key, unix_ms()?)
        .await
        .map_err(anyhow::Error::from)
        .with_context(|| "failed to read current Kubernetes lease holder after guarded append")?;
    if post_append_current_owner
        .as_ref()
        .map(|grant| grant.owner_id == request.owner_id && grant.owner_epoch == commit_owner_epoch)
        != Some(true)
    {
        bail!(
            "lease-guarded append owner lost lease before post-append verification: current={:?}",
            post_append_current_owner
        );
    }

    Ok(LeaseGuardedAppendProbeArtifactV1 {
        schema_version: 1,
        evidence_kind: "ingest_writer_lease_guarded_append_probe".to_string(),
        status: "pass".to_string(),
        expected_outcome: expected_outcome.to_string(),
        outcome: "appended".to_string(),
        authority_store_id: request.authority_store_id,
        authority_namespace: request.authority_namespace,
        operator_id: request.operator_id,
        writer_id: request.writer_id,
        lease_identity,
        owner_id: request.owner_id,
        expected_owner_epoch,
        acquired_grant: acquired_grant.as_ref().map(lease_grant_evidence),
        current_owner: current_owner.as_ref().map(lease_grant_evidence),
        post_append_current_owner: post_append_current_owner.as_ref().map(lease_grant_evidence),
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
    if &actual != expected {
        bail!(
            "durable admission record `{}` commit_guard_binding mismatch: expected {:?}, got {:?}",
            admission_record_key,
            expected,
            actual
        );
    }

    Ok((record, actual))
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
        store,
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
