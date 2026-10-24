//! Authenticate eager checkpoints before importing their applied prefixes.
//! This module never replays a baseline or changes an authoritative pointer.
use super::*;

fn insufficient(message: &str) -> ApiError {
    ApiError::conflict(format!("demand migration evidence insufficient: {message}"))
}

pub(super) async fn read_checkpoint(
    state: &ApiState,
    active: &ActiveMaterializedView,
    max_bytes: usize,
) -> Result<(Option<StandingRuntimeCheckpointRecord>, usize), ApiError> {
    let identity = active_standing_runtime_identity(active)
        .ok_or_else(|| insufficient("runtime identity missing"))?;
    let meta = state
        .meta_store
        .as_ref()
        .ok_or_else(|| insufficient("authoritative metadata required"))?;
    let pointer = meta
        .read_standing_runtime_checkpoint(
            &identity.tenant_id,
            &identity.program_id,
            &active.spec.view_id,
        )
        .await
        .map_err(meta_error_to_api)?;
    read_checkpoint_from_pointer(state, active, max_bytes, pointer).await
}

pub(super) async fn read_checkpoint_from_pointer(
    state: &ApiState,
    active: &ActiveMaterializedView,
    max_bytes: usize,
    pointer: Option<StandingRuntimeCheckpointPointer>,
) -> Result<(Option<StandingRuntimeCheckpointRecord>, usize), ApiError> {
    let identity = active_standing_runtime_identity(active)
        .ok_or_else(|| insufficient("runtime identity missing"))?;
    let Some(pointer) = pointer else {
        return Ok((None, 0));
    };
    let path = ObjectPath::from(pointer.checkpoint_key.clone());
    let size = match checkpoint_publication::cached_checkpoint_size(state, &path).await {
        Some(size) => size,
        None => {
            state
                .store
                .head(&path)
                .await
                .map_err(ApiError::internal)?
                .size
        }
    };
    if size > max_bytes as u64 {
        return Err(ApiError {
            status: StatusCode::PAYLOAD_TOO_LARGE,
            message: "checkpoint authentication exceeds demand budget".into(),
            details: Some(json!({"code":"DEMAND_MIGRATION_BUDGET_REQUIRED",
                "required_min_bytes":size,"supplied_max_bytes":max_bytes})),
            retryable_materialization: false,
        });
    }
    let bytes = checkpoint_fragments::read_bounded(
        state,
        &path,
        usize::try_from(size).map_err(ApiError::bad_request)?,
    )
    .await
    .map_err(|_| insufficient("checkpoint size changed"))?;
    if bytes.len() as u64 != size || stable_bytes_hash(&bytes) != pointer.manifest_hash {
        return Err(insufficient("checkpoint commitment mismatch"));
    }
    let mut record =
        standing_runtime_checkpoint_record_from_slice(&bytes).map_err(ApiError::bad_request)?;
    if record.checkpoint_key.is_empty() {
        record.checkpoint_key = pointer.checkpoint_key.clone();
    }
    checkpoint_publication::validate_standing_runtime_checkpoint_record(
        identity,
        &active.spec.view_id,
        &pointer,
        &record,
    )?;
    checkpoint_publication::validate_standing_runtime_checkpoint_replay_frontiers(&record)?;
    if record.demand_materialization.is_none() {
        let binding = published_relation_binding_for_active_view(active)?
            .ok_or_else(|| insufficient("published identity missing"))?;
        if pointer.bootstrap_generation != binding.producer_view_generation
            || pointer.plan_hash != binding.producer_plan_hash
        {
            return Err(insufficient(
                "checkpoint generation/plan differs from declaration",
            ));
        }
        if record
            .published_input_bootstrap
            .as_ref()
            .is_some_and(|p| !p.is_complete())
        {
            return Err(insufficient(
                "partial baseline cannot be imported as complete",
            ));
        }
    }
    let mut charged = bytes.len();
    if record.demand_materialization.is_none() {
        // Missing durable operator state must never enter recovery's source
        // rebuild branch. Reserve restoration reads before any mutation.
        let key = &record.checkpoint.state_root.object_key;
        ObjectKey::parse_standing_runtime_state_payload(key.clone())
            .map_err(|_| insufficient("durable operator state root missing"))?;
        let path = ObjectPath::from(key.clone());
        let root_size = match checkpoint_publication::cached_checkpoint_size(state, &path).await {
            Some(size) => size,
            None => {
                state
                    .store
                    .head(&path)
                    .await
                    .map_err(ApiError::internal)?
                    .size
            }
        };
        let required = (charged as u64)
            .checked_add(root_size)
            .ok_or_else(|| insufficient("state size overflow"))?;
        if required > max_bytes as u64 {
            return Err(budget(required, max_bytes));
        }
        let root = checkpoint_fragments::read_bounded(
            state,
            &path,
            usize::try_from(root_size).map_err(ApiError::bad_request)?,
        )
        .await
        .map_err(|_| insufficient("state root size changed or exceeds its authenticated limit"))?;
        let state_key = ObjectKey::parse_standing_runtime_state_payload(key.clone())
            .map_err(ApiError::bad_request)?
            .0;
        let fragment_bytes = checkpoint_fragments::verify_manifest_fragments(
            state,
            &state_key,
            &root,
            max_bytes
                .saturating_sub(charged)
                .saturating_sub(usize::try_from(root_size).unwrap_or(usize::MAX)),
        )
        .await?
        .unwrap_or(0);
        // Restore re-reads the immutable header/root; the shared resident cache
        // may avoid these reads, but conservative reservation remains bounded.
        // Targeted restore can read the checkpoint for its existence check,
        // factory restoration, and committed-pointer bookkeeping.
        let restore_reads = root_size
            .checked_add(fragment_bytes as u64)
            .and_then(|n| n.checked_add(bytes.len() as u64))
            .and_then(|n| n.checked_mul(3))
            .ok_or_else(|| insufficient("restore byte count overflow"))?;
        let required = required
            .checked_add(restore_reads)
            .ok_or_else(|| insufficient("restore byte count overflow"))?;
        if required > max_bytes as u64 {
            return Err(budget(required, max_bytes));
        }
        charged = usize::try_from(required).map_err(ApiError::bad_request)?;
    }
    Ok((Some(record), charged))
}

fn budget(required: u64, supplied: usize) -> ApiError {
    ApiError {
        status: StatusCode::PAYLOAD_TOO_LARGE,
        message: "migration authentication and restore require a larger byte budget".into(),
        details: Some(json!({"code":"DEMAND_MIGRATION_BUDGET_REQUIRED",
            "required_min_bytes":required,"supplied_max_bytes":supplied})),
        retryable_materialization: false,
    }
}

pub(super) fn validate_source_prefixes(
    active: &ActiveMaterializedView,
    record: Option<&StandingRuntimeCheckpointRecord>,
    artifacts: &[crate::demand_source::DemandSourceArtifact],
) -> Result<(), ApiError> {
    let Some(record) = record.filter(|r| r.demand_materialization.is_none()) else {
        return Ok(());
    };
    let mut seen = BTreeSet::new();
    for frontier in &record.checkpoint.input_frontiers {
        if !seen.insert((
            &frontier.relation_id,
            &frontier.relation_version,
            &frontier.stream_id,
            frontier.partition_id,
        )) {
            return Err(insufficient("duplicate source frontier"));
        }
        let input = active
            .spec
            .input_relations
            .iter()
            .find(|input| {
                input.relation_id == frontier.relation_id
                    && input.relation_version == frontier.relation_version
            })
            .ok_or_else(|| insufficient("frontier references undeclared input"))?;
        let mut ranges = artifacts
            .iter()
            .filter(|a| {
                a.relation.relation_id == frontier.relation_id
                    && a.relation.relation_version == frontier.relation_version
                    && a.stream_id == frontier.stream_id
                    && a.partition_id == frontier.partition_id
            })
            .collect::<Vec<_>>();
        ranges.sort_by_key(|a| a.publication.start_offset_inclusive);
        let mut end = 0;
        for artifact in ranges {
            if end == frontier.committed_offset_exclusive {
                break;
            }
            if artifact.relation.schema_fingerprint != input.schema_fingerprint
                || artifact.publication.start_offset_inclusive != end
                || artifact.publication.end_offset_exclusive > frontier.committed_offset_exclusive
            {
                return Err(insufficient(
                    "source prefix is gapped or splits an immutable publication",
                ));
            }
            end = artifact.publication.end_offset_exclusive;
        }
        if end != frontier.committed_offset_exclusive {
            return Err(insufficient(
                "source prefix is absent from authoritative inventory",
            ));
        }
    }
    Ok(())
}

pub(super) fn validate_published_prefixes(
    active: &ActiveMaterializedView,
    record: Option<&StandingRuntimeCheckpointRecord>,
    artifacts: &[crate::demand_published::DemandPublishedArtifact],
) -> Result<(), ApiError> {
    let Some(record) = record.filter(|r| r.demand_materialization.is_none()) else {
        return Ok(());
    };
    let inputs = view_dependencies::consumer_direct_view_inputs(active, record)?;
    if record.checkpoint.input_frontiers.len() != inputs.len() {
        return Err(insufficient(
            "published frontier set differs from direct cursor set",
        ));
    }
    for input in inputs {
        if !artifacts
            .iter()
            .any(|a| a.cursor == input.cursor && a.binding == input.published_relation)
        {
            return Err(insufficient(
                "consumed cursor is absent from authenticated producer lineage",
            ));
        }
        let matches = record
            .checkpoint
            .input_frontiers
            .iter()
            .filter(|f| {
                f.relation_id == input.published_relation.relation.relation_id
                    && f.relation_version == input.published_relation.relation.relation_version
                    && f.stream_id == input.published_relation.output_stream_id
                    && f.partition_id == 0
                    && f.committed_offset_exclusive == input.cursor.output_epoch
            })
            .count();
        if matches != 1 {
            return Err(insufficient("published frontier/cursor mismatch"));
        }
    }
    Ok(())
}
