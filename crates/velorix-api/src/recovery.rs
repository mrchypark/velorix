use super::*;

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct StandingRuntimeBackfillReplayOutcome {
    pub(super) applied_batches: usize,
    pub(super) remaining_batches: usize,
}

pub(super) async fn replay_committed_ingest_into_standing_runtime(
    state: &ApiState,
    active: &ActiveMaterializedView,
    replay_plan: &StandingRuntimeReplayPlan,
) -> Result<(), ApiError> {
    replay_committed_ingest_into_standing_runtime_limited(
        state,
        active,
        replay_plan,
        None,
        None,
        None,
    )
    .await
    .map(|_| ())
}

pub(super) async fn replay_committed_ingest_into_standing_runtime_limited(
    state: &ApiState,
    active: &ActiveMaterializedView,
    replay_plan: &StandingRuntimeReplayPlan,
    batch_limit: Option<usize>,
    range: Option<&BackfillRangeRequest>,
    scope: Option<&BackfillScopeRequest>,
) -> Result<StandingRuntimeBackfillReplayOutcome, ApiError> {
    if active.spec.input_relations.is_empty() {
        return Ok(StandingRuntimeBackfillReplayOutcome::default());
    }
    let Some(identity) = active_standing_runtime_identity(active) else {
        return Ok(StandingRuntimeBackfillReplayOutcome::default());
    };
    if batch_limit.is_some_and(|limit| limit == 0) {
        return Err(ApiError::bad_request(
            "backfill batch_limit must be a positive integer",
        ));
    }
    if let Some(range) = range {
        validate_backfill_range(range)?;
    }
    if let Some(scope) = scope {
        validate_backfill_scope(scope)?;
    }
    let operation_lock = state.standing_runtime_operation_lock(identity, &active.spec.view_id)?;
    let _operation_guard = operation_lock.lock().await;
    let Some(runtime) = state.standing_runtime(identity, &active.spec.view_id)? else {
        return Ok(StandingRuntimeBackfillReplayOutcome::default());
    };
    let replay_plan =
        match read_latest_standing_runtime_checkpoint(state, identity, &active.spec.view_id).await?
        {
            Some(latest_checkpoint) => {
                standing_runtime_replay_plan_from_record_ref(&latest_checkpoint)
            }
            None => replay_plan.clone(),
        };
    let batches = read_replay_ingest_batches(state, active, &replay_plan, range).await?;

    let mut outcome = StandingRuntimeBackfillReplayOutcome::default();
    let coalesce_replay = range.is_none() && scope.is_none();
    let mut coalesced_input_batches = Vec::new();
    let mut coalesced_replay_checkpoints = Vec::new();
    let mut coalesced_idempotency_parts = Vec::new();
    let mut coalesced_lower_bound_epoch = 0_u64;
    for batch in batches {
        let descriptor = batch.descriptor();
        let envelope =
            IngestEnvelope::decode(batch.payload().clone()).map_err(ApiError::bad_request)?;
        let header = envelope.header();
        if !active.spec.input_relations.iter().any(|input| {
            header.relation_id == input.relation_id
                && header.relation_version == input.relation_version
                && header.schema_fingerprint == input.schema_fingerprint
        }) {
            continue;
        }
        if range.is_some_and(|range| {
            !backfill_range_matches_batch(
                range,
                header,
                descriptor.stream_id.as_str(),
                descriptor.partition_id,
                descriptor.start_offset_inclusive,
                descriptor.end_offset_exclusive,
            )
        }) {
            continue;
        }
        if let Some(scope) = scope {
            if !backfill_scope_matches_batches(
                scope,
                &envelope.record_batches().map_err(ApiError::bad_request)?,
            )
            .await?
            {
                continue;
            }
        }
        if replay_plan_covers_replayed_batch(
            &replay_plan,
            header.relation_id.as_str(),
            header.relation_version.as_str(),
            descriptor.stream_id.as_str(),
            descriptor.partition_id,
            descriptor.end_offset_exclusive,
        ) {
            continue;
        }
        if batch_limit.is_some_and(|limit| outcome.applied_batches >= limit) {
            outcome.remaining_batches += 1;
            continue;
        }
        let input_batch = RelationInputBatch {
            encoding: RelationInputEncodingV1::SourceRelationV1,
            relation_id: header.relation_id.clone(),
            relation_version: header.relation_version.clone(),
            stream_id: descriptor.stream_id.clone(),
            partition_id: descriptor.partition_id,
            schema_fingerprint: header.schema_fingerprint.clone(),
            start_offset_inclusive: descriptor.start_offset_inclusive,
            end_offset_exclusive: descriptor.end_offset_exclusive,
            event_time_watermark: header.event_time_watermark.clone(),
            batches: envelope.record_batches().map_err(ApiError::bad_request)?,
        };

        if coalesce_replay {
            coalesced_idempotency_parts.push(format!(
                "{}:{}:{}-{}",
                descriptor.stream_id,
                descriptor.partition_id,
                descriptor.start_offset_inclusive,
                descriptor.end_offset_exclusive
            ));
            coalesced_lower_bound_epoch =
                coalesced_lower_bound_epoch.max(descriptor.end_offset_exclusive);
            coalesced_replay_checkpoints.push(ReplayCheckpoint::for_relation(
                header.relation_id.clone(),
                header.relation_version.clone(),
                descriptor.stream_id.clone(),
                descriptor.partition_id,
                descriptor.end_offset_exclusive,
            ));
            coalesced_input_batches.push(input_batch);
        } else {
            let owner = state
                .acquire_standing_runtime_owner(identity, &active.spec.view_id)
                .await?;
            let idempotency_key = EpochIdempotencyKey::new(format!(
                "{}:{}:{}-{}",
                descriptor.stream_id,
                descriptor.partition_id,
                descriptor.start_offset_inclusive,
                descriptor.end_offset_exclusive
            ))
            .map_err(ApiError::bad_request)?;
            let apply_result = match apply_standing_runtime_changes_and_checkpoint(
                Arc::clone(&runtime),
                descriptor.end_offset_exclusive,
                idempotency_key,
                input_batch,
                StandingRuntimeBudgetLimits::from_state(state),
            )
            .await
            {
                Ok(apply_result) => apply_result,
                Err(error) => {
                    remove_standing_runtime(state, identity, &active.spec.view_id)?;
                    return Err(error);
                }
            };
            if let Err(error) = persist_standing_runtime_checkpoint(
                state,
                &active.spec.view_id,
                &apply_result.checkpoint,
                &apply_result.output_deltas,
                StandingRuntimeCheckpointPersistContext::new(
                    None,
                    vec![ReplayCheckpoint::for_relation(
                        header.relation_id.clone(),
                        header.relation_version.clone(),
                        descriptor.stream_id.clone(),
                        descriptor.partition_id,
                        descriptor.end_offset_exclusive,
                    )],
                    owner,
                )
                .with_published_relation(published_relation_binding_for_active_view(active)?),
                None,
            )
            .await
            {
                remove_standing_runtime(state, identity, &active.spec.view_id)?;
                return Err(error);
            }
        }
        outcome.applied_batches += 1;
    }

    if !coalesced_input_batches.is_empty() {
        let owner = state
            .acquire_standing_runtime_owner(identity, &active.spec.view_id)
            .await?;
        let idempotency_hash = stable_bytes_hash(coalesced_idempotency_parts.join("|").as_bytes());
        let idempotency_key = EpochIdempotencyKey::new(format!(
            "{}:coalesced-replay:{idempotency_hash}",
            active.spec.view_id
        ))
        .map_err(ApiError::bad_request)?;
        let apply_result = match apply_standing_runtime_changes_and_checkpoint_many(
            Arc::clone(&runtime),
            coalesced_lower_bound_epoch,
            idempotency_key,
            coalesced_input_batches,
            StandingRuntimeBudgetLimits::from_state(state),
        )
        .await
        {
            Ok(apply_result) => apply_result,
            Err(error) => {
                remove_standing_runtime(state, identity, &active.spec.view_id)?;
                return Err(error);
            }
        };
        if let Err(error) = persist_standing_runtime_checkpoint(
            state,
            &active.spec.view_id,
            &apply_result.checkpoint,
            &apply_result.output_deltas,
            StandingRuntimeCheckpointPersistContext::new(None, coalesced_replay_checkpoints, owner)
                .with_published_relation(published_relation_binding_for_active_view(active)?),
            None,
        )
        .await
        {
            remove_standing_runtime(state, identity, &active.spec.view_id)?;
            return Err(error);
        }
    }

    Ok(outcome)
}

pub(super) fn validate_backfill_range(range: &BackfillRangeRequest) -> Result<(), ApiError> {
    if range.relation_id.trim().is_empty()
        || range.relation_version.trim().is_empty()
        || range.start_offset_inclusive >= range.end_offset_exclusive
    {
        return Err(ApiError::bad_request(
            "backfill range requires relation_id, relation_version, and start_offset_inclusive < end_offset_exclusive",
        ));
    }
    Ok(())
}

pub(super) fn validate_backfill_scope(scope: &BackfillScopeRequest) -> Result<(), ApiError> {
    if scope.where_clause.trim().is_empty() {
        return Err(ApiError::bad_request(
            "backfill scope.where must be a non-empty SQL predicate",
        ));
    }
    Ok(())
}

pub(super) async fn backfill_scope_matches_batches(
    scope: &BackfillScopeRequest,
    batches: &[RecordBatch],
) -> Result<bool, ApiError> {
    validate_backfill_scope(scope)?;
    if batches.is_empty() || batches.iter().all(|batch| batch.num_rows() == 0) {
        return Ok(false);
    }
    let sql = format!(
        "select * from __velorix_backfill_scope where {}",
        scope.where_clause
    );
    let filtered = query_record_batches_table_with_bindings_and_policy_and_limiter(
        "__velorix_backfill_scope",
        batches.to_vec(),
        &sql,
        &[],
        QueryPolicy::default(),
        None,
    )
    .await
    .map_err(|error| ApiError::bad_request(format!("invalid backfill scope.where: {error}")))?;
    Ok(filtered.iter().any(|batch| batch.num_rows() > 0))
}

pub(super) fn backfill_range_matches_batch(
    range: &BackfillRangeRequest,
    header: &IngestEnvelopeHeader,
    stream_id: &str,
    partition_id: u32,
    batch_start_offset_inclusive: u64,
    batch_end_offset_exclusive: u64,
) -> bool {
    header.relation_id == range.relation_id
        && header.relation_version == range.relation_version
        && range
            .stream_id
            .as_deref()
            .is_none_or(|requested| requested == stream_id)
        && range
            .partition_id
            .is_none_or(|requested| requested == partition_id)
        && batch_start_offset_inclusive < range.end_offset_exclusive
        && batch_end_offset_exclusive > range.start_offset_inclusive
}

pub(super) async fn committed_backfill_progress(
    state: &ApiState,
    active: &ActiveMaterializedView,
) -> Result<BackfillProgressResponse, ApiError> {
    if active.spec.input_relations.is_empty() {
        return Ok(backfill_progress_response(0, 0));
    }
    let replay_plan = match active_standing_runtime_identity(active) {
        Some(identity) => {
            read_latest_standing_runtime_checkpoint(state, identity, &active.spec.view_id)
                .await?
                .as_ref()
                .map(standing_runtime_replay_plan_from_record_ref)
                .unwrap_or_default()
        }
        None => StandingRuntimeReplayPlan::default(),
    };
    let empty_replay_plan;
    let read_plan = if state.authoritative_relation_ingest_enabled() {
        &replay_plan
    } else {
        // Progress counts all committed canonical batches; replay coverage is
        // applied below when computing `remaining`.
        empty_replay_plan = StandingRuntimeReplayPlan::default();
        &empty_replay_plan
    };
    let batches = read_replay_ingest_batches(state, active, read_plan, None).await?;
    let mut total = 0usize;
    let mut remaining = 0usize;
    for batch in batches {
        let descriptor = batch.descriptor();
        let envelope =
            IngestEnvelope::decode(batch.payload().clone()).map_err(ApiError::bad_request)?;
        let header = envelope.header();
        if !active.spec.input_relations.iter().any(|input| {
            header.relation_id == input.relation_id
                && header.relation_version == input.relation_version
                && header.schema_fingerprint == input.schema_fingerprint
        }) {
            continue;
        }
        total += 1;
        if replay_plan_covers_replayed_batch(
            &replay_plan,
            header.relation_id.as_str(),
            header.relation_version.as_str(),
            descriptor.stream_id.as_str(),
            descriptor.partition_id,
            descriptor.end_offset_exclusive,
        ) {
            continue;
        }
        remaining += 1;
    }
    Ok(backfill_progress_response(total, remaining))
}

pub(super) fn backfill_progress_response(
    total: usize,
    remaining: usize,
) -> BackfillProgressResponse {
    let processed = total.saturating_sub(remaining);
    let percent = if total == 0 {
        100.0
    } else {
        (processed as f64 / total as f64) * 100.0
    };
    BackfillProgressResponse {
        processed_batches: processed,
        remaining_batches: remaining,
        total_batches: total,
        percent,
    }
}

pub(super) async fn read_relation_catalog(
    state: &ApiState,
    relation_id: &str,
    relation_version: &str,
) -> Result<VelorixRelationCatalogV1, ApiError> {
    if let Some(meta_store) = &state.meta_store {
        match meta_store
            .read_relation_catalog(relation_id, relation_version)
            .await
        {
            Ok(catalog) => return Ok(catalog),
            Err(MetaStoreError::RelationCatalogNotFound { .. }) => {
                if state.authoritative_relation_ingest_enabled() {
                    return Err(ApiError::bad_request(format!(
                        "relation catalog `{relation_id}` version `{relation_version}` is not present in authoritative metadata"
                    )));
                }
            }
            Err(error) => return Err(meta_error_to_api(error)),
        }
    }
    state
        .relation_registry()?
        .read(relation_id, relation_version)
        .await
        .map_err(ApiError::bad_request)
}

/// Reads authoritative ingest only through Meta's committed relation source
/// cut. Staging objects are opaque until a committed publication reference is
/// returned, and every referenced object is read with its exact digest.
async fn read_replay_ingest_batches(
    state: &ApiState,
    active: &ActiveMaterializedView,
    replay_plan: &StandingRuntimeReplayPlan,
    range: Option<&BackfillRangeRequest>,
) -> Result<Vec<IngestBatch>, ApiError> {
    if !state.authoritative_relation_ingest_enabled() {
        let ingest_log =
            IngestLog::new_catalog_checked(Arc::clone(&state.store), state.capabilities.as_ref())
                .map_err(ApiError::internal)?;
        return ingest_log
            .replay_admitted_validated_envelopes_from(&replay_plan.replay_checkpoints)
            .await
            .map_err(ApiError::internal);
    }
    let Some(meta_store) = state.meta_store.as_ref() else {
        return Err(ApiError::service_unavailable(
            "authoritative relation ingest recovery requires metadata service",
        ));
    };
    let mut keys = BTreeSet::new();
    for checkpoint in &replay_plan.replay_checkpoints {
        let Some(relation_id) = checkpoint.relation_id.as_deref() else {
            return Err(ApiError::bad_request(
                "authoritative replay checkpoint is missing relation identity",
            ));
        };
        let Some(relation_version) = checkpoint.relation_version.as_deref() else {
            return Err(ApiError::bad_request(format!(
                "authoritative replay checkpoint for relation `{relation_id}` is missing relation version"
            )));
        };
        let Some(input) = active.spec.input_relations.iter().find(|input| {
            input.relation_id == relation_id && input.relation_version == relation_version
        }) else {
            return Err(ApiError::bad_request(format!(
                "authoritative replay checkpoint relation identity does not match active view: {relation_id}/{relation_version}"
            )));
        };
        keys.insert((
            relation_id.to_string(),
            relation_version.to_string(),
            input.schema_fingerprint.clone(),
            checkpoint.stream_id.clone(),
            checkpoint.partition_id,
        ));
    }
    for frontier in &replay_plan.input_frontiers {
        if let Some(input) = active.spec.input_relations.iter().find(|input| {
            input.relation_id == frontier.relation_id
                && input.relation_version == frontier.relation_version
        }) {
            keys.insert((
                frontier.relation_id.clone(),
                frontier.relation_version.clone(),
                input.schema_fingerprint.clone(),
                frontier.stream_id.clone(),
                frontier.partition_id,
            ));
        }
    }
    if let Some(range) = range {
        if let Some(partition_id) = range.partition_id {
            if let Some(input) = active.spec.input_relations.iter().find(|input| {
                input.relation_id == range.relation_id
                    && input.relation_version == range.relation_version
            }) {
                keys.insert((
                    range.relation_id.clone(),
                    range.relation_version.clone(),
                    input.schema_fingerprint.clone(),
                    range.stream_id.clone().unwrap_or_default(),
                    partition_id,
                ));
            }
        }
    }
    let mut batches = Vec::new();
    for (relation_id, relation_version, schema_fingerprint, stream_id, partition_id) in keys {
        if stream_id.is_empty() {
            continue;
        }
        let cut = meta_store
            .capture_relation_ingest_source_cut(CaptureRelationIngestSourceCutRequest {
                authority: RelationPartitionAuthorityKey {
                    namespace: state
                        .relation_ingest_config
                        .as_ref()
                        .map(|config| config.namespace.clone())
                        .unwrap_or_default(),
                    relation_id: relation_id.clone(),
                    stream_id: stream_id.clone(),
                    partition_id,
                },
                relation_version: relation_version.clone(),
                schema_fingerprint: schema_fingerprint.clone(),
            })
            .await
            .map_err(meta_error_to_api)?;
        if cut.schema_version != RELATION_INGEST_SOURCE_CUT_SCHEMA_VERSION_V1
            || cut.namespace
                != state
                    .relation_ingest_config
                    .as_ref()
                    .map(|config| config.namespace.as_str())
                    .unwrap_or_default()
            || cut.relation_id != relation_id
            || cut.partitions.is_empty()
        {
            return Err(ApiError::service_unavailable(format!(
                "authoritative relation source cut identity is invalid for `{relation_id}`"
            )));
        }
        let Some(input) = active.spec.input_relations.iter().find(|input| {
            input.relation_id == relation_id
                && input.relation_version == relation_version
                && input.schema_fingerprint == schema_fingerprint
        }) else {
            return Err(ApiError::bad_request(format!(
                "authoritative relation `{relation_id}` is not an active view input"
            )));
        };
        let catalog = read_relation_catalog(state, &relation_id, &relation_version).await?;
        let catalog_schema =
            catalog_input_relation_schema(&catalog).map_err(ApiError::bad_request)?;
        if catalog_schema != *input {
            return Err(ApiError::bad_request(format!(
                "authoritative relation catalog identity does not match active view input `{relation_id}`"
            )));
        }
        let Some(partition) = cut.partitions.into_iter().find(|partition| {
            partition.stream_id == stream_id && partition.partition_id == partition_id
        }) else {
            return Err(ApiError::service_unavailable(format!(
                "authoritative relation source cut is missing {relation_id}/{stream_id}/p={partition_id}"
            )));
        };
        for publication in partition.publications {
            let batch = validate_relation_publication_ref(
                state,
                &relation_id,
                &input.relation_version,
                &input.schema_fingerprint,
                &stream_id,
                partition_id,
                &publication,
            )
            .await?;
            batches.push(batch);
        }
    }
    batches.sort_by_key(|batch| {
        let descriptor = batch.descriptor();
        (
            descriptor.stream_id,
            descriptor.partition_id,
            descriptor.start_offset_inclusive,
        )
    });
    Ok(batches)
}

/// Validates one committed relation publication all the way through its
/// staged bytes. Both replay and checkpoint publication use this boundary so
/// source-cut refs cannot silently point at another relation version, range,
/// request, or digest.
pub(super) async fn validate_relation_publication_ref(
    state: &ApiState,
    relation_id: &str,
    relation_version: &str,
    schema_fingerprint: &str,
    stream_id: &str,
    partition_id: u32,
    publication: &RelationIngestPublicationRefV1,
) -> Result<IngestBatch, ApiError> {
    if publication.relation_version != relation_version
        || publication.schema_fingerprint != schema_fingerprint
    {
        return Err(ApiError::bad_request(format!(
            "authoritative relation publication `{}` has mismatched relation identity",
            publication.request_id
        )));
    }
    let (_, staging_parts) = ObjectKey::parse_ingest_staging(&publication.object_key)
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let expected_batch_key = ObjectKey::ingest_batch(
        &staging_parts.stream_id,
        staging_parts.partition_id,
        staging_parts.start_offset_inclusive,
        staging_parts.end_offset_exclusive,
    )
    .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let ingest_log =
        IngestLog::new_catalog_checked(Arc::clone(&state.store), state.capabilities.as_ref())
            .map_err(ApiError::internal)?;
    let payload = ingest_log
        .read_staging(&publication.object_key, &publication.object_digest)
        .await
        .map_err(ApiError::internal)?;
    let batch = IngestBatch::from_validated_envelope(payload).map_err(ApiError::bad_request)?;
    let envelope =
        IngestEnvelope::decode(batch.payload().clone()).map_err(ApiError::bad_request)?;
    let header = envelope.header();
    if header.relation_id != relation_id
        || header.relation_version != relation_version
        || header.schema_fingerprint != schema_fingerprint
        || header.stream_id != stream_id
        || header.partition_id != partition_id
        || header.start_offset_inclusive != publication.start_offset_inclusive
        || header.end_offset_exclusive != publication.end_offset_exclusive
        || stable_bytes_hash(batch.payload().as_ref()) != publication.payload_digest
        || publication.object_digest != publication.payload_digest
        || publication.request_id != staging_parts.staging_id
        || publication.batch_key != expected_batch_key.as_str()
        || staging_parts.stream_id != stream_id
        || staging_parts.partition_id != partition_id
        || staging_parts.start_offset_inclusive != publication.start_offset_inclusive
        || staging_parts.end_offset_exclusive != publication.end_offset_exclusive
    {
        return Err(ApiError::bad_request(format!(
            "authoritative relation publication `{}` does not match its staged envelope",
            publication.request_id
        )));
    }
    Ok(batch)
}

pub(super) async fn read_relation_catalogs_for_input_schemas(
    state: &ApiState,
    schemas: &[RelationSchema],
) -> Result<Vec<VelorixRelationCatalogV1>, ApiError> {
    if schemas.is_empty() {
        return Err(ApiError::bad_request("view has no input relation"));
    }
    let mut catalogs = Vec::with_capacity(schemas.len());
    for schema in schemas {
        let catalog =
            read_relation_catalog(state, &schema.relation_id, &schema.relation_version).await?;
        let expected = catalog_input_relation_schema(&catalog).map_err(ApiError::bad_request)?;
        if &expected != schema {
            return Err(ApiError::bad_request(format!(
                "input relation schema does not match registered relation `{}` version `{}`",
                schema.relation_id, schema.relation_version
            )));
        }
        catalogs.push(catalog);
    }
    Ok(catalogs)
}
