use std::fmt;

use serde::{de, Deserialize, Deserializer, Serialize};
use thiserror::Error;

const PARTITION_WIDTH: usize = 10;
const CHECKPOINT_WIDTH: usize = 20;
const OFFSET_WIDTH: usize = 20;

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[serde(transparent)]
pub struct ObjectKey(String);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IngestBatchKeyParts {
    pub stream_id: String,
    pub partition_id: u32,
    pub start_offset_inclusive: u64,
    pub end_offset_exclusive: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutputObjectKeyParts {
    pub stream_id: String,
    pub partition_id: u32,
    pub checkpoint_version: u64,
    pub start_offset_inclusive: u64,
    pub end_offset_exclusive: u64,
    pub object_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnershipEpochRecordKeyParts {
    pub stream_id: String,
    pub partition_id: u32,
    pub owner_epoch: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StandingRuntimeCheckpointKeyParts {
    pub tenant_id: String,
    pub program_id: String,
    pub view_id: String,
    pub logical_epoch: u64,
    pub content_hash: String,
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ObjectKeyError {
    #[error("object key segment `{0}` is empty")]
    EmptySegment(&'static str),
    #[error("object key segment `{name}` contains path-unsafe value `{value}`")]
    UnsafeSegment { name: &'static str, value: String },
    #[error(
        "offset range must be nonempty: start={start_offset_inclusive}, end={end_offset_exclusive}"
    )]
    InvalidOffsetRange {
        start_offset_inclusive: u64,
        end_offset_exclusive: u64,
    },
    #[error("object key must use the v1 namespace and have no leading slash: {0}")]
    InvalidExternalKey(String),
}

impl ObjectKey {
    pub fn ingest_batch(
        stream_id: &str,
        partition_id: u32,
        start_offset_inclusive: u64,
        end_offset_exclusive: u64,
    ) -> Result<Self, ObjectKeyError> {
        validate_segment("stream_id", stream_id)?;
        validate_offset_range(start_offset_inclusive, end_offset_exclusive)?;

        Ok(Self(format!(
            "v1/ingest/{stream_id}/p={partition_id:0PARTITION_WIDTH$}/{start_offset_inclusive:0OFFSET_WIDTH$}-{end_offset_exclusive:0OFFSET_WIDTH$}.batch"
        )))
    }

    pub fn ingest_admission_record(
        stream_id: &str,
        partition_id: u32,
        start_offset_inclusive: u64,
        end_offset_exclusive: u64,
    ) -> Result<Self, ObjectKeyError> {
        validate_segment("stream_id", stream_id)?;
        validate_offset_range(start_offset_inclusive, end_offset_exclusive)?;

        Ok(Self(format!(
            "v1/ingest-admission/{stream_id}/p={partition_id:0PARTITION_WIDTH$}/ranges/{start_offset_inclusive:0OFFSET_WIDTH$}-{end_offset_exclusive:0OFFSET_WIDTH$}.admission.json"
        )))
    }

    pub fn ingest_admission_orphan_expiry_decision(
        stream_id: &str,
        partition_id: u32,
        start_offset_inclusive: u64,
        end_offset_exclusive: u64,
        decision_id: &str,
    ) -> Result<Self, ObjectKeyError> {
        validate_segment("stream_id", stream_id)?;
        validate_segment("decision_id", decision_id)?;
        validate_offset_range(start_offset_inclusive, end_offset_exclusive)?;

        Ok(Self(format!(
            "v1/ingest-admission/{stream_id}/p={partition_id:0PARTITION_WIDTH$}/ranges/{start_offset_inclusive:0OFFSET_WIDTH$}-{end_offset_exclusive:0OFFSET_WIDTH$}/expiry-decisions/{decision_id}.expiry.json"
        )))
    }

    pub fn state_object(
        owner: &str,
        partition_id: u32,
        checkpoint_version: u64,
        object_id: &str,
    ) -> Result<Self, ObjectKeyError> {
        validate_segment("owner", owner)?;
        validate_segment("object_id", object_id)?;

        Ok(Self(format!(
            "v1/state/{owner}/p={partition_id:0PARTITION_WIDTH$}/chk={checkpoint_version:0CHECKPOINT_WIDTH$}/{object_id}.state"
        )))
    }

    pub fn output_object(
        stream_id: &str,
        partition_id: u32,
        checkpoint_version: u64,
        start_offset_inclusive: u64,
        end_offset_exclusive: u64,
        object_id: &str,
    ) -> Result<Self, ObjectKeyError> {
        validate_segment("stream_id", stream_id)?;
        validate_segment("object_id", object_id)?;
        validate_offset_range(start_offset_inclusive, end_offset_exclusive)?;

        Ok(Self(format!(
            "v1/outputs/{stream_id}/p={partition_id:0PARTITION_WIDTH$}/chk={checkpoint_version:0CHECKPOINT_WIDTH$}/{start_offset_inclusive:0OFFSET_WIDTH$}-{end_offset_exclusive:0OFFSET_WIDTH$}/{object_id}.output"
        )))
    }

    pub fn temp_publish(
        checkpoint_version: u64,
        attempt_or_object_id: &str,
        kind: &str,
    ) -> Result<Self, ObjectKeyError> {
        validate_segment("attempt_or_object_id", attempt_or_object_id)?;
        validate_segment("kind", kind)?;

        Ok(Self(format!(
            "v1/tmp/{checkpoint_version:0CHECKPOINT_WIDTH$}/{attempt_or_object_id}/{kind}"
        )))
    }

    pub fn checkpoint_manifest(checkpoint_version: u64) -> Self {
        Self(format!(
            "v1/checkpoints/{checkpoint_version:0CHECKPOINT_WIDTH$}.manifest"
        ))
    }

    pub fn checkpoint_latest_candidate_marker() -> Self {
        Self("v1/checkpoint-index/latest-candidate.json".to_string())
    }

    pub fn checkpoint_lifecycle_record(checkpoint_version: u64) -> Self {
        Self(format!(
            "v1/checkpoint-lifecycle/{checkpoint_version:0CHECKPOINT_WIDTH$}.status.json"
        ))
    }

    pub fn checkpoint_gc_transition_record(
        checkpoint_version: u64,
        transition_id: &str,
    ) -> Result<Self, ObjectKeyError> {
        validate_segment("transition_id", transition_id)?;

        Ok(Self(format!(
            "v1/checkpoint-gc-transitions/{checkpoint_version:0CHECKPOINT_WIDTH$}/transitions/{transition_id}.transition.json"
        )))
    }

    pub fn checkpoint_retention_record(checkpoint_version: u64) -> Self {
        Self(format!(
            "v1/checkpoint-retention/{checkpoint_version:0CHECKPOINT_WIDTH$}.retention.json"
        ))
    }

    pub fn checkpoint_recovery_transition_record(
        checkpoint_version: u64,
        transition_id: &str,
    ) -> Result<Self, ObjectKeyError> {
        validate_segment("transition_id", transition_id)?;

        Ok(Self(format!(
            "v1/checkpoint-recovery/{checkpoint_version:0CHECKPOINT_WIDTH$}/transitions/{transition_id}.transition.json"
        )))
    }

    pub fn garbage_collection_run(run_id: &str) -> Result<Self, ObjectKeyError> {
        validate_segment("run_id", run_id)?;

        Ok(Self(format!("v1/gc-runs/{run_id}.run.json")))
    }

    pub fn ownership_epoch_record(
        stream_id: &str,
        partition_id: u32,
        owner_epoch: u64,
    ) -> Result<Self, ObjectKeyError> {
        validate_segment("stream_id", stream_id)?;

        Ok(Self(format!(
            "v1/ownership/{stream_id}/p={partition_id:0PARTITION_WIDTH$}/epoch={owner_epoch:0CHECKPOINT_WIDTH$}.claim"
        )))
    }

    pub fn persisted_query(query_id: &str) -> Result<Self, ObjectKeyError> {
        validate_segment("query_id", query_id)?;

        Ok(Self(format!("v1/queries/{query_id}.query.json")))
    }

    pub fn query_table(table_id: &str) -> Result<Self, ObjectKeyError> {
        validate_segment("table_id", table_id)?;

        Ok(Self(format!("v1/tables/{table_id}.table.json")))
    }

    pub fn query_policy(tenant_id: &str, query_policy_id: &str) -> Result<Self, ObjectKeyError> {
        validate_segment("tenant_id", tenant_id)?;
        validate_segment("query_policy_id", query_policy_id)?;

        Ok(Self(format!(
            "v1/query-policy/{tenant_id}/{query_policy_id}.json"
        )))
    }

    pub fn feldera_artifact(
        artifact_id: &str,
        artifact_hash: &str,
    ) -> Result<Self, ObjectKeyError> {
        validate_segment("artifact_id", artifact_id)?;
        let hash = parse_sha256_hash_segment(artifact_hash)?;

        Ok(Self(format!(
            "v1/feldera-artifacts/{artifact_id}/sha256/{hash}.artifact.json"
        )))
    }

    pub fn materialized_view(view_id: &str, spec_hash: &str) -> Result<Self, ObjectKeyError> {
        validate_segment("view_id", view_id)?;
        let hash = parse_feldera_spec_hash_segment(spec_hash)?;

        Ok(Self(format!(
            "v1/views/{view_id}/spec-sha256/{hash}.view.json"
        )))
    }

    pub fn active_materialized_view(view_id: &str) -> Result<Self, ObjectKeyError> {
        validate_segment("view_id", view_id)?;

        Ok(Self(format!("v1/views/{view_id}/active.json")))
    }

    pub fn view_compile_deploy_job(view_id: &str, spec_hash: &str) -> Result<Self, ObjectKeyError> {
        validate_segment("view_id", view_id)?;
        let hash = parse_feldera_spec_hash_segment(spec_hash)?;

        Ok(Self(format!(
            "v1/view-compile-deploy-jobs/{view_id}/spec-sha256/{hash}.job.json"
        )))
    }

    pub fn standing_runtime_checkpoint(
        tenant_id: &str,
        program_id: &str,
        view_id: &str,
        logical_epoch: u64,
        content_hash: &str,
    ) -> Result<Self, ObjectKeyError> {
        validate_segment("tenant_id", tenant_id)?;
        validate_segment("program_id", program_id)?;
        validate_segment("view_id", view_id)?;
        let hash = parse_sha256_hash_segment(content_hash)?;

        Ok(Self(format!(
            "v1/standing-runtime-checkpoints/{tenant_id}/{program_id}/{view_id}/epochs/{logical_epoch:0CHECKPOINT_WIDTH$}/sha256/{hash}.checkpoint.json"
        )))
    }

    pub fn standing_runtime_latest_checkpoint(
        tenant_id: &str,
        program_id: &str,
        view_id: &str,
    ) -> Result<Self, ObjectKeyError> {
        validate_segment("tenant_id", tenant_id)?;
        validate_segment("program_id", program_id)?;
        validate_segment("view_id", view_id)?;

        Ok(Self(format!(
            "v1/standing-runtime-checkpoints/{tenant_id}/{program_id}/{view_id}/latest.json"
        )))
    }

    pub fn relation_catalog(
        relation_id: &str,
        relation_version: &str,
    ) -> Result<Self, ObjectKeyError> {
        validate_segment("relation_id", relation_id)?;
        validate_segment("relation_version", relation_version)?;

        Ok(Self(format!(
            "v1/relations/{relation_id}/versions/{relation_version}.relation.json"
        )))
    }

    pub fn parse(value: impl Into<String>) -> Result<Self, ObjectKeyError> {
        let value = value.into();
        if value.starts_with('/')
            || !value.starts_with("v1/")
            || value.split('/').any(str::is_empty)
        {
            return Err(ObjectKeyError::InvalidExternalKey(value));
        }

        validate_known_layout(&value)?;

        Ok(Self(value))
    }

    pub fn parse_ingest_batch(
        value: impl Into<String>,
    ) -> Result<(Self, IngestBatchKeyParts), ObjectKeyError> {
        let value = value.into();
        let key = Self::parse(value.clone())?;
        let parts = parse_ingest_batch_layout(&value)?;

        Ok((key, parts))
    }

    pub fn parse_output_object(
        value: impl Into<String>,
    ) -> Result<(Self, OutputObjectKeyParts), ObjectKeyError> {
        let value = value.into();
        let key = Self::parse(value.clone())?;
        let parts = parse_output_object_layout(&value)?;

        Ok((key, parts))
    }

    pub fn parse_ownership_epoch_record(
        value: impl Into<String>,
    ) -> Result<(Self, OwnershipEpochRecordKeyParts), ObjectKeyError> {
        let value = value.into();
        let key = Self::parse(value.clone())?;
        let parts = parse_ownership_epoch_record_layout(&value)?;

        Ok((key, parts))
    }

    pub fn parse_standing_runtime_checkpoint(
        value: impl Into<String>,
    ) -> Result<(Self, StandingRuntimeCheckpointKeyParts), ObjectKeyError> {
        let value = value.into();
        let key = Self::parse(value.clone())?;
        let parts = parse_standing_runtime_checkpoint_layout(&value)?;

        Ok((key, parts))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ObjectKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ObjectKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(value).map_err(de::Error::custom)
    }
}

fn validate_known_layout(value: &str) -> Result<(), ObjectKeyError> {
    let segments: Vec<_> = value.split('/').collect();

    match segments.as_slice() {
        ["v1", "ingest", stream_id, partition, range] => {
            parse_ingest_batch_parts(value, stream_id, partition, range)?;
        }
        ["v1", "ingest-admission", stream_id, partition, "ranges", admission_file] => {
            validate_segment("stream_id", stream_id)?;
            parse_prefixed_u32("partition_id", partition, "p=", PARTITION_WIDTH)?;
            let range = admission_file
                .strip_suffix(".admission.json")
                .ok_or_else(|| ObjectKeyError::InvalidExternalKey(value.to_string()))?;
            parse_offset_range(value, range)?;
        }
        ["v1", "ingest-admission", stream_id, partition, "ranges", range, "expiry-decisions", decision_file] =>
        {
            validate_segment("stream_id", stream_id)?;
            parse_prefixed_u32("partition_id", partition, "p=", PARTITION_WIDTH)?;
            parse_offset_range(value, range)?;
            let decision_id = decision_file
                .strip_suffix(".expiry.json")
                .ok_or_else(|| ObjectKeyError::InvalidExternalKey(value.to_string()))?;
            validate_segment("decision_id", decision_id)?;
        }
        ["v1", "state", owner, partition, checkpoint, object_file] => {
            validate_segment("owner", owner)?;
            parse_prefixed_u32("partition_id", partition, "p=", PARTITION_WIDTH)?;
            parse_prefixed_u64("checkpoint_version", checkpoint, "chk=", CHECKPOINT_WIDTH)?;

            let object_id = object_file
                .strip_suffix(".state")
                .ok_or_else(|| ObjectKeyError::InvalidExternalKey(value.to_string()))?;
            validate_segment("object_id", object_id)?;
        }
        ["v1", "outputs", stream_id, partition, checkpoint, range, object_file] => {
            parse_output_object_parts(value, stream_id, partition, checkpoint, range, object_file)?;
        }
        ["v1", "tmp", checkpoint, attempt_or_object_id, kind] => {
            parse_fixed_u64("checkpoint_version", checkpoint, CHECKPOINT_WIDTH)?;
            validate_segment("attempt_or_object_id", attempt_or_object_id)?;
            validate_segment("kind", kind)?;
        }
        ["v1", "checkpoints", manifest_file] => {
            let checkpoint = manifest_file
                .strip_suffix(".manifest")
                .ok_or_else(|| ObjectKeyError::InvalidExternalKey(value.to_string()))?;
            parse_fixed_u64("checkpoint_version", checkpoint, CHECKPOINT_WIDTH)?;
        }
        ["v1", "checkpoint-index", "latest-candidate.json"] => {}
        ["v1", "checkpoint-lifecycle", status_file] => {
            let checkpoint = status_file
                .strip_suffix(".status.json")
                .ok_or_else(|| ObjectKeyError::InvalidExternalKey(value.to_string()))?;
            parse_fixed_u64("checkpoint_version", checkpoint, CHECKPOINT_WIDTH)?;
        }
        ["v1", "checkpoint-gc-transitions", checkpoint, "transitions", transition_file] => {
            parse_fixed_u64("checkpoint_version", checkpoint, CHECKPOINT_WIDTH)?;
            let transition_id = transition_file
                .strip_suffix(".transition.json")
                .ok_or_else(|| ObjectKeyError::InvalidExternalKey(value.to_string()))?;
            validate_segment("transition_id", transition_id)?;
        }
        ["v1", "checkpoint-retention", retention_file] => {
            let checkpoint = retention_file
                .strip_suffix(".retention.json")
                .ok_or_else(|| ObjectKeyError::InvalidExternalKey(value.to_string()))?;
            parse_fixed_u64("checkpoint_version", checkpoint, CHECKPOINT_WIDTH)?;
        }
        ["v1", "checkpoint-recovery", checkpoint, "transitions", transition_file] => {
            parse_fixed_u64("checkpoint_version", checkpoint, CHECKPOINT_WIDTH)?;
            let transition_id = transition_file
                .strip_suffix(".transition.json")
                .ok_or_else(|| ObjectKeyError::InvalidExternalKey(value.to_string()))?;
            validate_segment("transition_id", transition_id)?;
        }
        ["v1", "gc-runs", run_file] => {
            let run_id = run_file
                .strip_suffix(".run.json")
                .ok_or_else(|| ObjectKeyError::InvalidExternalKey(value.to_string()))?;
            validate_segment("run_id", run_id)?;
        }
        ["v1", "ownership", stream_id, partition, epoch_file] => {
            parse_ownership_epoch_record_parts(value, stream_id, partition, epoch_file)?;
        }
        ["v1", "queries", query_file] => {
            let query_id = query_file
                .strip_suffix(".query.json")
                .ok_or_else(|| ObjectKeyError::InvalidExternalKey(value.to_string()))?;
            validate_segment("query_id", query_id)?;
        }
        ["v1", "tables", table_file] => {
            let table_id = table_file
                .strip_suffix(".table.json")
                .ok_or_else(|| ObjectKeyError::InvalidExternalKey(value.to_string()))?;
            validate_segment("table_id", table_id)?;
        }
        ["v1", "query-policy", tenant_id, policy_file] => {
            validate_segment("tenant_id", tenant_id)?;
            let query_policy_id = policy_file
                .strip_suffix(".json")
                .ok_or_else(|| ObjectKeyError::InvalidExternalKey(value.to_string()))?;
            validate_segment("query_policy_id", query_policy_id)?;
        }
        ["v1", "feldera-artifacts", artifact_id, "sha256", artifact_file] => {
            validate_segment("artifact_id", artifact_id)?;
            let hash = artifact_file
                .strip_suffix(".artifact.json")
                .ok_or_else(|| ObjectKeyError::InvalidExternalKey(value.to_string()))?;
            validate_sha256_hex(hash)
                .map_err(|_| ObjectKeyError::InvalidExternalKey(value.to_string()))?;
        }
        ["v1", "views", view_id, "active.json"] => {
            validate_segment("view_id", view_id)?;
        }
        ["v1", "views", view_id, "spec-sha256", view_file] => {
            validate_segment("view_id", view_id)?;
            let hash = view_file
                .strip_suffix(".view.json")
                .ok_or_else(|| ObjectKeyError::InvalidExternalKey(value.to_string()))?;
            validate_sha256_hex(hash)
                .map_err(|_| ObjectKeyError::InvalidExternalKey(value.to_string()))?;
        }
        ["v1", "view-compile-deploy-jobs", view_id, "spec-sha256", job_file] => {
            validate_segment("view_id", view_id)?;
            let hash = job_file
                .strip_suffix(".job.json")
                .ok_or_else(|| ObjectKeyError::InvalidExternalKey(value.to_string()))?;
            validate_sha256_hex(hash)
                .map_err(|_| ObjectKeyError::InvalidExternalKey(value.to_string()))?;
        }
        ["v1", "standing-runtime-checkpoints", tenant_id, program_id, view_id, "epochs", logical_epoch, "sha256", checkpoint_file] =>
        {
            parse_standing_runtime_checkpoint_parts(
                value,
                tenant_id,
                program_id,
                view_id,
                logical_epoch,
                checkpoint_file,
            )?;
        }
        ["v1", "standing-runtime-checkpoints", tenant_id, program_id, view_id, "latest.json"] => {
            validate_segment("tenant_id", tenant_id)?;
            validate_segment("program_id", program_id)?;
            validate_segment("view_id", view_id)?;
        }
        ["v1", "relations", relation_id, "versions", relation_file] => {
            validate_segment("relation_id", relation_id)?;
            let relation_version = relation_file
                .strip_suffix(".relation.json")
                .ok_or_else(|| ObjectKeyError::InvalidExternalKey(value.to_string()))?;
            validate_segment("relation_version", relation_version)?;
        }
        _ => return Err(ObjectKeyError::InvalidExternalKey(value.to_string())),
    }

    Ok(())
}

fn parse_ingest_batch_layout(value: &str) -> Result<IngestBatchKeyParts, ObjectKeyError> {
    let segments: Vec<_> = value.split('/').collect();

    let ["v1", "ingest", stream_id, partition, range] = segments.as_slice() else {
        return Err(ObjectKeyError::InvalidExternalKey(value.to_string()));
    };

    parse_ingest_batch_parts(value, stream_id, partition, range)
}

fn parse_output_object_layout(value: &str) -> Result<OutputObjectKeyParts, ObjectKeyError> {
    let segments: Vec<_> = value.split('/').collect();

    let ["v1", "outputs", stream_id, partition, checkpoint, range, object_file] =
        segments.as_slice()
    else {
        return Err(ObjectKeyError::InvalidExternalKey(value.to_string()));
    };

    parse_output_object_parts(value, stream_id, partition, checkpoint, range, object_file)
}

fn parse_ownership_epoch_record_layout(
    value: &str,
) -> Result<OwnershipEpochRecordKeyParts, ObjectKeyError> {
    let segments: Vec<_> = value.split('/').collect();

    let ["v1", "ownership", stream_id, partition, epoch_file] = segments.as_slice() else {
        return Err(ObjectKeyError::InvalidExternalKey(value.to_string()));
    };

    parse_ownership_epoch_record_parts(value, stream_id, partition, epoch_file)
}

fn parse_standing_runtime_checkpoint_layout(
    value: &str,
) -> Result<StandingRuntimeCheckpointKeyParts, ObjectKeyError> {
    let segments: Vec<_> = value.split('/').collect();

    let ["v1", "standing-runtime-checkpoints", tenant_id, program_id, view_id, "epochs", logical_epoch, "sha256", checkpoint_file] =
        segments.as_slice()
    else {
        return Err(ObjectKeyError::InvalidExternalKey(value.to_string()));
    };

    parse_standing_runtime_checkpoint_parts(
        value,
        tenant_id,
        program_id,
        view_id,
        logical_epoch,
        checkpoint_file,
    )
}

fn parse_ingest_batch_parts(
    value: &str,
    stream_id: &str,
    partition: &str,
    range: &str,
) -> Result<IngestBatchKeyParts, ObjectKeyError> {
    validate_segment("stream_id", stream_id)?;
    let partition_id = parse_prefixed_u32("partition_id", partition, "p=", PARTITION_WIDTH)?;

    let range = range
        .strip_suffix(".batch")
        .ok_or_else(|| ObjectKeyError::InvalidExternalKey(value.to_string()))?;
    let (start_offset_inclusive, end_offset_exclusive) = parse_offset_range(value, range)?;

    Ok(IngestBatchKeyParts {
        stream_id: stream_id.to_string(),
        partition_id,
        start_offset_inclusive,
        end_offset_exclusive,
    })
}

fn parse_standing_runtime_checkpoint_parts(
    value: &str,
    tenant_id: &str,
    program_id: &str,
    view_id: &str,
    logical_epoch: &str,
    checkpoint_file: &str,
) -> Result<StandingRuntimeCheckpointKeyParts, ObjectKeyError> {
    validate_segment("tenant_id", tenant_id)?;
    validate_segment("program_id", program_id)?;
    validate_segment("view_id", view_id)?;
    let logical_epoch = parse_fixed_u64("logical_epoch", logical_epoch, CHECKPOINT_WIDTH)?;
    let hash = checkpoint_file
        .strip_suffix(".checkpoint.json")
        .ok_or_else(|| ObjectKeyError::InvalidExternalKey(value.to_string()))?;
    validate_sha256_hex(hash).map_err(|_| ObjectKeyError::InvalidExternalKey(value.to_string()))?;

    Ok(StandingRuntimeCheckpointKeyParts {
        tenant_id: tenant_id.to_string(),
        program_id: program_id.to_string(),
        view_id: view_id.to_string(),
        logical_epoch,
        content_hash: format!("sha256:{hash}"),
    })
}

fn parse_output_object_parts(
    value: &str,
    stream_id: &str,
    partition: &str,
    checkpoint: &str,
    range: &str,
    object_file: &str,
) -> Result<OutputObjectKeyParts, ObjectKeyError> {
    validate_segment("stream_id", stream_id)?;
    let partition_id = parse_prefixed_u32("partition_id", partition, "p=", PARTITION_WIDTH)?;
    let checkpoint_version =
        parse_prefixed_u64("checkpoint_version", checkpoint, "chk=", CHECKPOINT_WIDTH)?;
    let (start_offset_inclusive, end_offset_exclusive) = parse_offset_range(value, range)?;
    let object_id = object_file
        .strip_suffix(".output")
        .ok_or_else(|| ObjectKeyError::InvalidExternalKey(value.to_string()))?;
    validate_segment("object_id", object_id)?;

    Ok(OutputObjectKeyParts {
        stream_id: stream_id.to_string(),
        partition_id,
        checkpoint_version,
        start_offset_inclusive,
        end_offset_exclusive,
        object_id: object_id.to_string(),
    })
}

fn parse_ownership_epoch_record_parts(
    value: &str,
    stream_id: &str,
    partition: &str,
    epoch_file: &str,
) -> Result<OwnershipEpochRecordKeyParts, ObjectKeyError> {
    validate_segment("stream_id", stream_id)?;
    let partition_id = parse_prefixed_u32("partition_id", partition, "p=", PARTITION_WIDTH)?;
    let owner_epoch = epoch_file
        .strip_suffix(".claim")
        .ok_or_else(|| ObjectKeyError::InvalidExternalKey(value.to_string()))?;
    let owner_epoch = parse_prefixed_u64("owner_epoch", owner_epoch, "epoch=", CHECKPOINT_WIDTH)?;

    Ok(OwnershipEpochRecordKeyParts {
        stream_id: stream_id.to_string(),
        partition_id,
        owner_epoch,
    })
}

fn parse_offset_range(value: &str, range: &str) -> Result<(u64, u64), ObjectKeyError> {
    let (start, end) = range
        .split_once('-')
        .ok_or_else(|| ObjectKeyError::InvalidExternalKey(value.to_string()))?;
    let start_offset_inclusive = parse_fixed_u64("start_offset_inclusive", start, OFFSET_WIDTH)?;
    let end_offset_exclusive = parse_fixed_u64("end_offset_exclusive", end, OFFSET_WIDTH)?;
    validate_offset_range(start_offset_inclusive, end_offset_exclusive)?;

    Ok((start_offset_inclusive, end_offset_exclusive))
}

fn parse_prefixed_u32(
    name: &'static str,
    value: &str,
    prefix: &str,
    width: usize,
) -> Result<u32, ObjectKeyError> {
    let value = value
        .strip_prefix(prefix)
        .ok_or_else(|| ObjectKeyError::InvalidExternalKey(value.to_string()))?;
    parse_fixed_u32(name, value, width)
}

fn parse_prefixed_u64(
    name: &'static str,
    value: &str,
    prefix: &str,
    width: usize,
) -> Result<u64, ObjectKeyError> {
    let value = value
        .strip_prefix(prefix)
        .ok_or_else(|| ObjectKeyError::InvalidExternalKey(value.to_string()))?;
    parse_fixed_u64(name, value, width)
}

fn parse_fixed_u32(name: &'static str, value: &str, width: usize) -> Result<u32, ObjectKeyError> {
    if !is_fixed_width_digits(value, width) {
        return Err(ObjectKeyError::UnsafeSegment {
            name,
            value: value.to_string(),
        });
    }

    value
        .parse()
        .map_err(|_| ObjectKeyError::InvalidExternalKey(value.to_string()))
}

fn parse_fixed_u64(name: &'static str, value: &str, width: usize) -> Result<u64, ObjectKeyError> {
    if !is_fixed_width_digits(value, width) {
        return Err(ObjectKeyError::UnsafeSegment {
            name,
            value: value.to_string(),
        });
    }

    value
        .parse()
        .map_err(|_| ObjectKeyError::InvalidExternalKey(value.to_string()))
}

fn is_fixed_width_digits(value: &str, width: usize) -> bool {
    value.len() == width && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn validate_offset_range(
    start_offset_inclusive: u64,
    end_offset_exclusive: u64,
) -> Result<(), ObjectKeyError> {
    if start_offset_inclusive >= end_offset_exclusive {
        return Err(ObjectKeyError::InvalidOffsetRange {
            start_offset_inclusive,
            end_offset_exclusive,
        });
    }

    Ok(())
}

fn validate_segment(name: &'static str, value: &str) -> Result<(), ObjectKeyError> {
    if value.is_empty() {
        return Err(ObjectKeyError::EmptySegment(name));
    }

    if value == "."
        || value == ".."
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(ObjectKeyError::UnsafeSegment {
            name,
            value: value.to_string(),
        });
    }

    Ok(())
}

fn parse_sha256_hash_segment(artifact_hash: &str) -> Result<&str, ObjectKeyError> {
    let Some(hash) = artifact_hash.strip_prefix("sha256:") else {
        return Err(ObjectKeyError::UnsafeSegment {
            name: "artifact_hash",
            value: artifact_hash.to_string(),
        });
    };
    validate_sha256_hex(hash)?;

    Ok(hash)
}

fn parse_feldera_spec_hash_segment(spec_hash: &str) -> Result<&str, ObjectKeyError> {
    let Some(hash) = spec_hash.strip_prefix("velorix-feldera-spec-sha256-v1:") else {
        return Err(ObjectKeyError::UnsafeSegment {
            name: "spec_hash",
            value: spec_hash.to_string(),
        });
    };
    validate_sha256_hex(hash).map_err(|_| ObjectKeyError::UnsafeSegment {
        name: "spec_hash",
        value: hash.to_string(),
    })?;

    Ok(hash)
}

fn validate_sha256_hex(hash: &str) -> Result<(), ObjectKeyError> {
    if hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(ObjectKeyError::UnsafeSegment {
            name: "artifact_hash",
            value: hash.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::ObjectKey;

    #[test]
    fn ingest_batch_key_is_deterministic_and_lexicographically_ordered() {
        let key = ObjectKey::ingest_batch("orders", 7, 42, 100).unwrap();
        let restarted = ObjectKey::ingest_batch("orders", 7, 42, 100).unwrap();

        assert_eq!(
            key.as_str(),
            "v1/ingest/orders/p=0000000007/00000000000000000042-00000000000000000100.batch"
        );
        assert_eq!(key, restarted);
        assert_eq!(key.to_string(), key.as_str());
    }

    #[test]
    fn ingest_admission_record_key_is_range_scoped_and_parseable() {
        let key = ObjectKey::ingest_admission_record("orders", 7, 42, 100).unwrap();

        assert_eq!(
            key.as_str(),
            "v1/ingest-admission/orders/p=0000000007/ranges/00000000000000000042-00000000000000000100.admission.json"
        );
        assert_eq!(ObjectKey::parse(key.as_str()).unwrap(), key);
    }

    #[test]
    fn ingest_admission_orphan_expiry_decision_key_is_range_scoped_and_parseable() {
        let key =
            ObjectKey::ingest_admission_orphan_expiry_decision("orders", 7, 42, 100, "repair-1")
                .unwrap();

        assert_eq!(
            key.as_str(),
            "v1/ingest-admission/orders/p=0000000007/ranges/00000000000000000042-00000000000000000100/expiry-decisions/repair-1.expiry.json"
        );
        assert_eq!(ObjectKey::parse(key.as_str()).unwrap(), key);
    }

    #[test]
    fn partition_key_width_preserves_full_u32_lexicographic_order() {
        let before = ObjectKey::ingest_batch("orders", 999_999, 0, 1).unwrap();
        let after = ObjectKey::ingest_batch("orders", 1_000_000, 0, 1).unwrap();
        let max = ObjectKey::ingest_batch("orders", u32::MAX, 0, 1).unwrap();

        assert!(before < after);
        assert!(after < max);
        assert!(before.as_str().contains("/p=0000999999/"));
        assert!(after.as_str().contains("/p=0001000000/"));
        assert!(max.as_str().contains("/p=4294967295/"));
    }

    #[test]
    fn state_object_key_is_deterministic_and_names_checkpoint_context() {
        let key = ObjectKey::state_object("balances_by_account", 12, 9, "state-0001").unwrap();
        let restarted =
            ObjectKey::state_object("balances_by_account", 12, 9, "state-0001").unwrap();

        assert_eq!(
            key.as_str(),
            "v1/state/balances_by_account/p=0000000012/chk=00000000000000000009/state-0001.state"
        );
        assert_eq!(key, restarted);
    }

    #[test]
    fn output_object_key_is_deterministic_and_checkpoint_scoped() {
        let key = ObjectKey::output_object("settlements", 7, 9, 20, 25, "out-0001").unwrap();
        let restarted = ObjectKey::output_object("settlements", 7, 9, 20, 25, "out-0001").unwrap();

        assert_eq!(
            key.as_str(),
            "v1/outputs/settlements/p=0000000007/chk=00000000000000000009/00000000000000000020-00000000000000000025/out-0001.output"
        );
        assert_eq!(key, restarted);
        assert_eq!(ObjectKey::parse(key.as_str()).unwrap(), key);

        let (parsed_key, parts) = ObjectKey::parse_output_object(key.as_str()).unwrap();
        assert_eq!(parsed_key, key);
        assert_eq!(parts.stream_id, "settlements");
        assert_eq!(parts.partition_id, 7);
        assert_eq!(parts.checkpoint_version, 9);
        assert_eq!(parts.start_offset_inclusive, 20);
        assert_eq!(parts.end_offset_exclusive, 25);
        assert_eq!(parts.object_id, "out-0001");
    }

    #[test]
    fn parse_ingest_batch_rejects_output_object_keys() {
        let output_key = ObjectKey::output_object("orders", 0, 1, 0, 10, "out-0001").unwrap();

        assert!(ObjectKey::parse_ingest_batch(output_key.as_str()).is_err());
    }

    #[test]
    fn temp_publish_key_uses_caller_supplied_attempt_id() {
        let key = ObjectKey::temp_publish(9, "attempt-abc", "manifest").unwrap();
        let restarted = ObjectKey::temp_publish(9, "attempt-abc", "manifest").unwrap();

        assert_eq!(
            key.as_str(),
            "v1/tmp/00000000000000000009/attempt-abc/manifest"
        );
        assert_eq!(key, restarted);
    }

    #[test]
    fn checkpoint_manifest_key_is_deterministic_and_version_ordered() {
        let key = ObjectKey::checkpoint_manifest(9);
        let restarted = ObjectKey::checkpoint_manifest(9);

        assert_eq!(key.as_str(), "v1/checkpoints/00000000000000000009.manifest");
        assert_eq!(key, restarted);
    }

    #[test]
    fn checkpoint_latest_candidate_marker_key_is_deterministic() {
        let key = ObjectKey::checkpoint_latest_candidate_marker();
        let restarted = ObjectKey::checkpoint_latest_candidate_marker();

        assert_eq!(key.as_str(), "v1/checkpoint-index/latest-candidate.json");
        assert_eq!(key, restarted);
        assert_eq!(ObjectKey::parse(key.as_str()).unwrap(), key);
    }

    #[test]
    fn checkpoint_retention_record_key_is_deterministic_and_parseable() {
        let key = ObjectKey::checkpoint_retention_record(9);
        let restarted = ObjectKey::checkpoint_retention_record(9);

        assert_eq!(
            key.as_str(),
            "v1/checkpoint-retention/00000000000000000009.retention.json"
        );
        assert_eq!(key, restarted);
        assert_eq!(ObjectKey::parse(key.as_str()).unwrap(), key);
    }

    #[test]
    fn checkpoint_recovery_transition_record_key_is_deterministic_and_parseable() {
        let key =
            ObjectKey::checkpoint_recovery_transition_record(9, "recovery-test-0001").unwrap();
        let restarted =
            ObjectKey::checkpoint_recovery_transition_record(9, "recovery-test-0001").unwrap();

        assert_eq!(
            key.as_str(),
            "v1/checkpoint-recovery/00000000000000000009/transitions/recovery-test-0001.transition.json"
        );
        assert_eq!(key, restarted);
        assert_eq!(ObjectKey::parse(key.as_str()).unwrap(), key);
    }

    #[test]
    fn checkpoint_gc_transition_record_key_is_deterministic_and_parseable() {
        let key = ObjectKey::checkpoint_gc_transition_record(9, "gc-retired-run-0001").unwrap();
        let restarted =
            ObjectKey::checkpoint_gc_transition_record(9, "gc-retired-run-0001").unwrap();

        assert_eq!(
            key.as_str(),
            "v1/checkpoint-gc-transitions/00000000000000000009/transitions/gc-retired-run-0001.transition.json"
        );
        assert_eq!(key, restarted);
        assert_eq!(ObjectKey::parse(key.as_str()).unwrap(), key);
    }

    #[test]
    fn garbage_collection_run_key_is_deterministic_and_parseable() {
        let key = ObjectKey::garbage_collection_run("run-0001").unwrap();
        let restarted = ObjectKey::garbage_collection_run("run-0001").unwrap();

        assert_eq!(key.as_str(), "v1/gc-runs/run-0001.run.json");
        assert_eq!(key, restarted);
        assert_eq!(ObjectKey::parse(key.as_str()).unwrap(), key);
    }

    #[test]
    fn ownership_epoch_record_key_is_deterministic_and_parseable() {
        let key = ObjectKey::ownership_epoch_record("orders", 7, 42).unwrap();
        let restarted = ObjectKey::ownership_epoch_record("orders", 7, 42).unwrap();

        assert_eq!(
            key.as_str(),
            "v1/ownership/orders/p=0000000007/epoch=00000000000000000042.claim"
        );
        assert_eq!(key, restarted);
        assert_eq!(ObjectKey::parse(key.as_str()).unwrap(), key);

        let (parsed_key, parts) = ObjectKey::parse_ownership_epoch_record(key.as_str()).unwrap();
        assert_eq!(parsed_key, key);
        assert_eq!(parts.stream_id, "orders");
        assert_eq!(parts.partition_id, 7);
        assert_eq!(parts.owner_epoch, 42);
    }

    #[test]
    fn persisted_query_key_is_deterministic() {
        let key = ObjectKey::persisted_query("orders-by-account").unwrap();
        let restarted = ObjectKey::persisted_query("orders-by-account").unwrap();

        assert_eq!(key.as_str(), "v1/queries/orders-by-account.query.json");
        assert_eq!(key, restarted);
        assert_eq!(ObjectKey::parse(key.as_str()).unwrap(), key);
    }

    #[test]
    fn persisted_query_key_rejects_path_unsafe_query_ids() {
        for query_id in ["", ".", "..", "orders/by-account", "orders by account"] {
            assert!(
                ObjectKey::persisted_query(query_id).is_err(),
                "accepted invalid query id: {query_id}"
            );
        }
    }

    #[test]
    fn query_table_key_is_deterministic() {
        let key = ObjectKey::query_table("orders-current").unwrap();
        let restarted = ObjectKey::query_table("orders-current").unwrap();

        assert_eq!(key.as_str(), "v1/tables/orders-current.table.json");
        assert_eq!(key, restarted);
        assert_eq!(ObjectKey::parse(key.as_str()).unwrap(), key);
    }

    #[test]
    fn query_table_key_rejects_path_unsafe_table_ids() {
        for table_id in ["", ".", "..", "orders/current", "orders current"] {
            assert!(
                ObjectKey::query_table(table_id).is_err(),
                "accepted invalid table id: {table_id}"
            );
        }
    }

    #[test]
    fn feldera_artifact_key_is_deterministic_and_parseable() {
        let hash = format!("sha256:{}", "a".repeat(64));
        let key = ObjectKey::feldera_artifact("orders-by-region", &hash).unwrap();
        let restarted = ObjectKey::feldera_artifact("orders-by-region", &hash).unwrap();

        assert_eq!(
            key.as_str(),
            "v1/feldera-artifacts/orders-by-region/sha256/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.artifact.json"
        );
        assert_eq!(key, restarted);
        assert_eq!(ObjectKey::parse(key.as_str()).unwrap(), key);
    }

    #[test]
    fn feldera_artifact_key_rejects_unsafe_identity() {
        for (artifact_id, artifact_hash) in [
            (
                "",
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            ),
            (
                "orders/current",
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            ),
            (
                "orders",
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            ),
            ("orders", "sha256:not-hex"),
        ] {
            assert!(
                ObjectKey::feldera_artifact(artifact_id, artifact_hash).is_err(),
                "accepted invalid artifact identity: {artifact_id}/{artifact_hash}"
            );
        }
    }

    #[test]
    fn materialized_view_key_is_deterministic_and_parseable() {
        let spec_hash = format!("velorix-feldera-spec-sha256-v1:{}", "a".repeat(64));
        let key = ObjectKey::materialized_view("orders-by-region", &spec_hash).unwrap();
        let restarted = ObjectKey::materialized_view("orders-by-region", &spec_hash).unwrap();

        assert_eq!(
            key.as_str(),
            "v1/views/orders-by-region/spec-sha256/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.view.json"
        );
        assert_eq!(key, restarted);
        assert_eq!(ObjectKey::parse(key.as_str()).unwrap(), key);
    }

    #[test]
    fn materialized_view_key_rejects_unsafe_identity() {
        for (view_id, spec_hash) in [
            (
                "",
                "velorix-feldera-spec-sha256-v1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            ),
            (
                "orders/current",
                "velorix-feldera-spec-sha256-v1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            ),
            (
                "orders",
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            ),
            ("orders", "velorix-feldera-spec-sha256-v1:not-hex"),
        ] {
            assert!(
                ObjectKey::materialized_view(view_id, spec_hash).is_err(),
                "accepted invalid materialized view identity: {view_id}/{spec_hash}"
            );
        }
    }

    #[test]
    fn standing_runtime_checkpoint_keys_are_deterministic_and_parseable() {
        let content_hash = format!("sha256:{}", "d".repeat(64));
        let checkpoint = ObjectKey::standing_runtime_checkpoint(
            "tenant-a",
            "program-a",
            "scores-by-user",
            42,
            &content_hash,
        )
        .unwrap();
        let restarted = ObjectKey::standing_runtime_checkpoint(
            "tenant-a",
            "program-a",
            "scores-by-user",
            42,
            &content_hash,
        )
        .unwrap();
        let latest = ObjectKey::standing_runtime_latest_checkpoint(
            "tenant-a",
            "program-a",
            "scores-by-user",
        )
        .unwrap();

        assert_eq!(
            checkpoint.as_str(),
            "v1/standing-runtime-checkpoints/tenant-a/program-a/scores-by-user/epochs/00000000000000000042/sha256/dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd.checkpoint.json"
        );
        assert_eq!(checkpoint, restarted);
        assert_eq!(ObjectKey::parse(checkpoint.as_str()).unwrap(), checkpoint);
        let (_, parts) = ObjectKey::parse_standing_runtime_checkpoint(checkpoint.as_str()).unwrap();
        assert_eq!(parts.tenant_id, "tenant-a");
        assert_eq!(parts.program_id, "program-a");
        assert_eq!(parts.view_id, "scores-by-user");
        assert_eq!(parts.logical_epoch, 42);
        assert_eq!(parts.content_hash, content_hash);
        assert_eq!(
            latest.as_str(),
            "v1/standing-runtime-checkpoints/tenant-a/program-a/scores-by-user/latest.json"
        );
        assert_eq!(ObjectKey::parse(latest.as_str()).unwrap(), latest);
    }

    #[test]
    fn relation_catalog_key_is_deterministic_and_parseable() {
        let key = ObjectKey::relation_catalog("orders", "2026-05-05.v1").unwrap();
        let restarted = ObjectKey::relation_catalog("orders", "2026-05-05.v1").unwrap();

        assert_eq!(
            key.as_str(),
            "v1/relations/orders/versions/2026-05-05.v1.relation.json"
        );
        assert_eq!(key, restarted);
        assert_eq!(ObjectKey::parse(key.as_str()).unwrap(), key);
    }

    #[test]
    fn relation_catalog_key_rejects_unsafe_identity() {
        for (relation_id, relation_version) in [
            ("", "2026-05-05.v1"),
            ("orders/current", "2026-05-05.v1"),
            ("orders", ""),
            ("orders", "2026/05/05"),
        ] {
            assert!(
                ObjectKey::relation_catalog(relation_id, relation_version).is_err(),
                "accepted invalid relation identity: {relation_id}/{relation_version}"
            );
        }
    }

    #[test]
    fn query_policy_key_is_deterministic_and_parseable() {
        let key = ObjectKey::query_policy("tenant-a", "standard").unwrap();
        let restarted = ObjectKey::query_policy("tenant-a", "standard").unwrap();

        assert_eq!(key.as_str(), "v1/query-policy/tenant-a/standard.json");
        assert_eq!(key, restarted);
        assert_eq!(ObjectKey::parse(key.as_str()).unwrap(), key);
    }

    #[test]
    fn query_policy_key_rejects_unsafe_identity() {
        for (tenant_id, query_policy_id) in [
            ("", "standard"),
            ("tenant/a", "standard"),
            ("tenant-a", ""),
            ("tenant-a", "standard/base"),
        ] {
            assert!(
                ObjectKey::query_policy(tenant_id, query_policy_id).is_err(),
                "accepted invalid policy identity: {tenant_id}/{query_policy_id}"
            );
        }
    }

    #[test]
    fn parse_rejects_invalid_or_unrecognized_external_keys() {
        for invalid in [
            "v1/ingest/./p=0000000000/00000000000000000000-00000000000000000001.batch",
            "v1/ingest/../p=0000000000/00000000000000000000-00000000000000000001.batch",
            "v1/ingest/orders!/p=0000000000/00000000000000000000-00000000000000000001.batch",
            "v1/ingest/orders//00000000000000000000-00000000000000000001.batch",
            "/v1/checkpoints/00000000000000000001.manifest",
            "v2/checkpoints/00000000000000000001.manifest",
            "v1/unknown/orders/p=0000000000/object",
            "v1/ownership/orders/p=0000000000/epoch=1.claim",
            "v1/ownership/orders/p=0000000000/epoch=00000000000000000001.json",
            "v1/queries/../orders.query.json",
            "v1/queries/orders.txt",
            "v1/tables/../orders.table.json",
            "v1/tables/orders.txt",
            "v1/query-policy/tenant-a/.json",
            "v1/query-policy/tenant-a/standard/base.json",
            "v1/feldera-artifacts/orders/sha256/not-hex.artifact.json",
            "v1/feldera-artifacts/orders/sha512/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.artifact.json",
            "v1/views/orders/spec-sha256/not-hex.view.json",
            "v1/views/orders/sha256/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.view.json",
            "v1/views/orders/spec-sha256/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.json",
            "v1/relations/orders/versions/.relation.json",
            "v1/relations/orders/versions/2026/05/05.relation.json",
            "v1/relations/orders/versions/2026-05-05.v1.json",
        ] {
            assert!(
                ObjectKey::parse(invalid).is_err(),
                "accepted invalid key: {invalid}"
            );
        }
    }

    #[test]
    fn deserialize_rejects_invalid_key_strings() {
        assert!(
            serde_json::from_str::<ObjectKey>("\"v1/unknown/orders/p=0000000000/object\"").is_err()
        );
    }
}
