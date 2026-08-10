use super::*;

pub(super) async fn persist_standing_runtime_checkpoint(
    state: &ApiState,
    view_id: &str,
    checkpoint: &RuntimeCheckpoint,
    output_deltas: &[ViewOutputDelta],
    context: StandingRuntimeCheckpointPersistContext,
    mut timer: Option<&mut IngestTimer>,
) -> Result<StandingRuntimeCheckpointWriteSummary, ApiError> {
    let checkpoint =
        standing_runtime_checkpoint_with_authoritative_input_coverage(state, view_id, checkpoint)
            .await?;
    let checkpoint = &checkpoint;
    if !checkpoint.identity.view_ids.iter().any(|id| id == view_id) {
        return Err(ApiError::bad_request(format!(
            "standing runtime checkpoint identity does not include view `{view_id}`"
        )));
    }
    let checkpoint_key = ObjectKey::standing_runtime_checkpoint(
        &checkpoint.identity.tenant_id,
        &checkpoint.identity.program_id,
        view_id,
        checkpoint.logical_epoch,
        &checkpoint.state_root.content_hash,
    )
    .map_err(ApiError::bad_request)?;
    let StandingRuntimeCheckpointPersistContext {
        previous_record,
        replay_checkpoints_to_merge,
        owner,
        published_relation,
        direct_view_inputs,
    } = context;
    let previous_record = match previous_record {
        Some(record) => Some(record),
        None => {
            read_latest_standing_runtime_checkpoint(state, &checkpoint.identity, view_id).await?
        }
    };
    let checkpoint = standing_runtime_checkpoint_with_prior_input_coverage(
        checkpoint,
        previous_record.as_ref(),
        published_relation.as_ref(),
    )?;
    let resolved_view_cursors = resolve_authoritative_direct_view_inputs(
        state,
        &checkpoint.identity.tenant_id,
        &direct_view_inputs,
    )
    .await?;
    let checkpoint = standing_runtime_checkpoint_with_causal_cut(
        &checkpoint,
        &resolved_view_cursors,
        published_relation.as_ref(),
    )?;
    let checkpoint = &checkpoint;
    let expected_previous = previous_record
        .as_ref()
        .map(standing_runtime_checkpoint_pointer_from_record);
    let output_delta_publications =
        standing_runtime_output_delta_records_for_checkpoint_with_binding(
            checkpoint,
            view_id,
            output_deltas,
            &checkpoint_key,
            published_relation.as_ref(),
        )?;
    let mut write_summary = StandingRuntimeCheckpointWriteSummary {
        output_delta_writes: output_delta_publications.len(),
        state_payload_writes: 1,
        checkpoint_record_writes: 1,
        checkpoint_pointer_writes: usize::from(state.meta_store.is_some()),
        latest_cache_writes: 1,
        compaction_scheduled: 0,
        output_refs: Vec::new(),
    };
    for publication in &output_delta_publications {
        persist_standing_runtime_output_delta(
            state,
            &publication.delta_key,
            &publication.delta_record,
        )
        .await?;
    }
    if let Some(timer) = timer.as_mut() {
        timer.mark("checkpoint_output_delta");
    }
    let (state_payload_key, state_payload_record) =
        standing_runtime_state_payload_record_for_checkpoint(checkpoint, view_id)?;
    persist_standing_runtime_state_payload(state, &state_payload_key, &state_payload_record)
        .await?;
    if let Some(timer) = timer.as_mut() {
        timer.mark("checkpoint_state_payload");
    }
    let checkpoint_for_record = standing_runtime_checkpoint_with_durable_publication_refs(
        checkpoint,
        None,
        &output_delta_publications,
        &state_payload_key,
    );
    let duplicate_checkpoint = expected_previous.as_ref().is_some_and(|pointer| {
        pointer.checkpoint_key == checkpoint_key.as_str()
            && pointer.logical_epoch == checkpoint.logical_epoch
            && pointer.content_hash == checkpoint.state_root.content_hash
            && pointer.output_manifest_refs == checkpoint_for_record.output_manifest_refs
            && pointer.input_coverage == checkpoint_for_record.input_coverage
            && previous_record.as_ref().is_some_and(|record| {
                record.checkpoint.causal_cut == checkpoint_for_record.causal_cut
            })
    });
    write_summary.output_refs = checkpoint_for_record.output_manifest_refs.clone();
    let previous_checkpoint = if duplicate_checkpoint {
        previous_record
            .as_ref()
            .and_then(|record| record.previous_checkpoint.clone())
    } else {
        expected_previous.clone()
    };
    let latest_key = ObjectKey::standing_runtime_latest_checkpoint(
        &checkpoint.identity.tenant_id,
        &checkpoint.identity.program_id,
        view_id,
    )
    .map_err(ApiError::bad_request)?;
    let replay_checkpoints = merged_standing_runtime_replay_checkpoints(
        previous_record.as_ref(),
        replay_checkpoints_to_merge,
    );
    let record = StandingRuntimeCheckpointRecord {
        schema_version: 1,
        record_kind: "standing_runtime_checkpoint_v1".to_string(),
        view_id: view_id.to_string(),
        checkpoint_key: checkpoint_key.as_str().to_string(),
        previous_checkpoint,
        checkpoint: checkpoint_for_record,
        replay_checkpoints,
        manifest_hash: String::new(),
    };
    let bytes =
        serde_json::to_vec(&record).map_err(|source| ApiError::internal(source.to_string()))?;
    let candidate = standing_runtime_checkpoint_pointer_from_record(&record);
    let checkpoint_path = ObjectPath::from(checkpoint_key.as_str());
    let result = state
        .store
        .put_opts(
            &checkpoint_path,
            bytes::Bytes::from(bytes.clone()).into(),
            PutMode::Create.into(),
        )
        .await;
    match result {
        Ok(_) => {}
        Err(object_store::Error::AlreadyExists { .. }) => {
            let existing = state
                .store
                .get(&checkpoint_path)
                .await
                .map_err(ApiError::internal)?
                .bytes()
                .await
                .map_err(ApiError::internal)?;
            if existing.as_ref() != bytes.as_slice() {
                return Err(ApiError::conflict(format!(
                    "standing runtime checkpoint conflict at {}",
                    checkpoint_key.as_str()
                )));
            }
        }
        Err(error) => return Err(ApiError::internal(error)),
    }
    if let Some(timer) = timer.as_mut() {
        timer.mark("checkpoint_record");
    }
    if state.meta_store.is_some() {
        validate_checkpoint_pointer_object_exists_for_meta_rehydration(state, &candidate).await?;
    }
    publish_standing_runtime_checkpoint_pointer(state, expected_previous, candidate.clone(), owner)
        .await?;
    if let Some(timer) = timer.as_mut() {
        timer.mark("checkpoint_pointer");
    }
    state.set_standing_runtime_committed_checkpoint(
        &checkpoint.identity,
        view_id,
        Some(candidate),
    )?;
    let latest_write = state
        .store
        .put(
            &ObjectPath::from(latest_key.as_str()),
            bytes::Bytes::from(bytes).into(),
        )
        .await;
    if let Err(error) = latest_write {
        if state.meta_store.is_none() {
            return Err(ApiError::internal(error));
        }
    }
    if let Some(timer) = timer.as_mut() {
        timer.mark("checkpoint_latest_cache");
    }
    if maybe_spawn_background_view_output_compaction_after_checkpoint(
        state,
        view_id,
        checkpoint.logical_epoch,
    ) {
        write_summary.compaction_scheduled = 1;
    }

    Ok(write_summary)
}

fn standing_runtime_checkpoint_with_prior_input_coverage(
    checkpoint: &RuntimeCheckpoint,
    previous_record: Option<&StandingRuntimeCheckpointRecord>,
    published_relation: Option<&PublishedRelationBindingV1>,
) -> Result<RuntimeCheckpoint, ApiError> {
    if checkpoint.input_coverage.is_some() || published_relation.is_none() {
        return Ok(checkpoint.clone());
    }
    let previous_coverage = previous_record
        .and_then(|record| record.checkpoint.input_coverage.as_ref())
        .ok_or_else(|| {
            ApiError::bad_request(
                "published relation output commit requires checkpoint input coverage",
            )
        })?;
    let binding = published_relation.expect("checked above");
    if previous_coverage.view_generation != binding.producer_view_generation
        || previous_coverage.plan_hash != binding.producer_plan_hash
    {
        return Err(ApiError::bad_request(
            "published relation prior checkpoint generation or plan mismatch",
        ));
    }
    let mut coverage = previous_coverage.clone();
    for relation in &mut coverage.relations {
        for partition in &mut relation.partitions {
            if let Some(frontier) = checkpoint.input_frontiers.iter().find(|frontier| {
                frontier.relation_id == relation.relation_id
                    && frontier.relation_version == relation.relation_version
                    && frontier.stream_id == partition.stream_id
                    && frontier.partition_id == partition.partition_id
            }) {
                if frontier.committed_offset_exclusive < partition.processed_offset_exclusive {
                    return Err(ApiError::bad_request(
                        "published relation checkpoint input coverage regressed",
                    ));
                }
                partition.processed_offset_exclusive = frontier.committed_offset_exclusive;
            }
        }
    }
    let mut checkpoint = checkpoint.clone();
    checkpoint.input_coverage = Some(
        coverage
            .canonicalized()
            .map_err(|error| ApiError::bad_request(error.to_string()))?,
    );
    Ok(checkpoint)
}

fn standing_runtime_checkpoint_with_causal_cut(
    checkpoint: &RuntimeCheckpoint,
    direct_view_cursors: &[ResolvedCausalViewCursorV1],
    published_relation: Option<&PublishedRelationBindingV1>,
) -> Result<RuntimeCheckpoint, ApiError> {
    if published_relation.is_none() {
        return Ok(checkpoint.clone());
    }
    let coverage = checkpoint.input_coverage.as_ref().ok_or_else(|| {
        ApiError::bad_request("published relation output commit requires checkpoint input coverage")
    })?;
    if direct_view_cursors.iter().any(|resolved| {
        resolved.authoritative_head_checkpoint_key.is_empty()
            || !is_sha256_identity(&resolved.authoritative_head_manifest_hash)
    }) {
        return Err(ApiError::bad_request(
            "resolved view cursor is missing its authoritative head commitment",
        ));
    }
    let causal_cut = CausalCutV1::from_input_coverage(
        coverage,
        direct_view_cursors
            .iter()
            .map(|resolved| resolved.cursor.clone())
            .collect(),
    )
    .map_err(|error| ApiError::bad_request(error.to_string()))?;
    if checkpoint
        .causal_cut
        .as_ref()
        .is_some_and(|existing| existing != &causal_cut)
    {
        return Err(ApiError::bad_request(
            "published relation checkpoint causal cut mismatch",
        ));
    }
    let mut checkpoint = checkpoint.clone();
    checkpoint.causal_cut = Some(causal_cut);
    Ok(checkpoint)
}

async fn resolve_authoritative_direct_view_inputs(
    state: &ApiState,
    tenant_id: &str,
    direct_view_inputs: &[StandingRuntimeDirectViewInputV1],
) -> Result<Vec<ResolvedCausalViewCursorV1>, ApiError> {
    let mut resolved = Vec::with_capacity(direct_view_inputs.len());
    for input in direct_view_inputs {
        let cursor = resolve_authoritative_view_cursor(
            state,
            tenant_id,
            &input.published_relation,
            &input.cursor,
        )
        .await?;
        resolved.push(cursor);
    }
    Ok(resolved)
}

#[derive(Clone, Debug)]
pub(super) struct ResolvedCausalViewCursorV1 {
    cursor: CausalViewCursorV1,
    authoritative_head_checkpoint_key: String,
    authoritative_head_manifest_hash: String,
}

pub(super) async fn resolve_authoritative_view_cursor(
    state: &ApiState,
    tenant_id: &str,
    binding: &PublishedRelationBindingV1,
    cursor: &CausalViewCursorV1,
) -> Result<ResolvedCausalViewCursorV1, ApiError> {
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        resolve_authoritative_view_cursor_within_budget(state, tenant_id, binding, cursor),
    )
    .await
    .map_err(|_| {
        ApiError::service_unavailable("authoritative view cursor resolution deadline exceeded")
    })?
}

async fn resolve_authoritative_view_cursor_within_budget(
    state: &ApiState,
    tenant_id: &str,
    binding: &PublishedRelationBindingV1,
    cursor: &CausalViewCursorV1,
) -> Result<ResolvedCausalViewCursorV1, ApiError> {
    validate_published_relation_binding_v1(binding).map_err(ApiError::bad_request)?;
    cursor
        .validate()
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    if cursor.producer_tenant_id != tenant_id
        || cursor.producer_program_id != binding.producer_view_id
        || cursor.producer_view_id != binding.producer_view_id
        || cursor.producer_generation != binding.producer_view_generation
        || cursor.output_stream != binding.output_stream_id
    {
        return Err(ApiError::bad_request(format!(
            "view cursor binding mismatch on edge `{}`",
            cursor.input_edge
        )));
    }
    let meta_store = state.meta_store.as_ref().ok_or_else(|| {
        ApiError::service_unavailable(
            "authoritative metadata is required to resolve a direct view cursor",
        )
    })?;
    let producer_view_id = &cursor.producer_view_id;
    let producer_program_id = &cursor.producer_program_id;
    let mut pointer = meta_store
        .read_standing_runtime_checkpoint(tenant_id, producer_program_id, producer_view_id)
        .await
        .map_err(meta_error_to_api)?
        .ok_or_else(|| {
            ApiError::bad_request(format!(
                "view cursor edge `{}` has no authoritative producer checkpoint",
                cursor.input_edge
            ))
        })?;
    let authoritative_head_checkpoint_key = pointer.checkpoint_key.clone();
    let authoritative_head_manifest_hash = pointer.manifest_hash.clone();

    #[cfg(not(test))]
    const MAX_LINEAGE_HOPS: usize = 4_096;
    #[cfg(test)]
    const MAX_LINEAGE_HOPS: usize = 8;
    const MAX_OUTPUT_REFS_PER_CHECKPOINT: usize = 64;
    let mut lineage_hops = 0usize;
    loop {
        lineage_hops += 1;
        if lineage_hops > MAX_LINEAGE_HOPS {
            return Err(ApiError::bad_request(format!(
                "view cursor edge `{}` exceeds the authoritative checkpoint lineage budget",
                cursor.input_edge
            )));
        }
        if pointer.output_manifest_refs.len() > MAX_OUTPUT_REFS_PER_CHECKPOINT {
            return Err(ApiError::bad_request(format!(
                "view cursor edge `{}` exceeds the authoritative checkpoint output-ref budget",
                cursor.input_edge
            )));
        }
        if pointer.logical_epoch < cursor.output_epoch {
            return Err(ApiError::bad_request(format!(
                "view cursor edge `{}` is ahead of the authoritative producer frontier",
                cursor.input_edge
            )));
        }
        let record = read_standing_runtime_checkpoint_record_from_pointer_for_scope(
            state,
            tenant_id,
            producer_program_id,
            producer_view_id,
            &pointer,
        )
        .await?;
        if record.checkpoint.logical_epoch == cursor.output_epoch {
            validate_authoritative_view_cursor_commit(state, binding, cursor, &record).await?;
            return Ok(ResolvedCausalViewCursorV1 {
                cursor: cursor.clone(),
                authoritative_head_checkpoint_key,
                authoritative_head_manifest_hash,
            });
        }
        let previous = record.previous_checkpoint.ok_or_else(|| {
            ApiError::bad_request(format!(
                "view cursor edge `{}` references a producer epoch missing from the authoritative checkpoint lineage",
                cursor.input_edge
            ))
        })?;
        if previous.logical_epoch >= pointer.logical_epoch
            || previous.logical_epoch < cursor.output_epoch
        {
            return Err(ApiError::bad_request(format!(
                "view cursor edge `{}` has a non-contiguous authoritative checkpoint lineage",
                cursor.input_edge
            )));
        }
        pointer = previous;
    }
}

async fn validate_authoritative_view_cursor_commit(
    state: &ApiState,
    binding: &PublishedRelationBindingV1,
    cursor: &CausalViewCursorV1,
    record: &StandingRuntimeCheckpointRecord,
) -> Result<(), ApiError> {
    let commit_refs = record
        .checkpoint
        .output_manifest_refs
        .iter()
        .filter(|output_ref| output_ref.starts_with(STANDING_RUNTIME_OUTPUT_COMMIT_REF_PREFIX))
        .collect::<Vec<_>>();
    if commit_refs.len() != 1 {
        return Err(ApiError::bad_request(format!(
            "view cursor edge `{}` requires exactly one authoritative producer commit",
            cursor.input_edge
        )));
    }
    let (_key, delta_record) =
        read_standing_runtime_output_delta_record(state, commit_refs[0], &binding.producer_view_id)
            .await?;
    let commit = delta_record.producer_commit.as_ref().ok_or_else(|| {
        ApiError::bad_request(format!(
            "view cursor edge `{}` points to a legacy output delta",
            cursor.input_edge
        ))
    })?;
    if record.checkpoint.logical_epoch != cursor.output_epoch
        || delta_record.logical_epoch != cursor.output_epoch
        || delta_record.schema_fingerprint != binding.relation.schema_fingerprint
        || commit.producer_view_generation != binding.producer_view_generation
        || commit.producer_view_generation != cursor.producer_generation
        || commit.producer_plan_hash != binding.producer_plan_hash
        || commit.output_stream_id != binding.output_stream_id
        || commit.output_stream_id != cursor.output_stream
        || commit.output_schema_hash != binding.output_schema_hash
        || commit.key_descriptor_hash != binding.key_descriptor_hash
        || commit.delta_codec_identity != binding.delta_codec_identity
        || commit.frontier_kind != binding.frontier_kind
        || commit.producer_commit_digest != cursor.commit_digest
    {
        return Err(ApiError::bad_request(format!(
            "view cursor edge `{}` does not match its authoritative producer commit",
            cursor.input_edge
        )));
    }
    Ok(())
}

async fn standing_runtime_checkpoint_with_authoritative_input_coverage(
    state: &ApiState,
    view_id: &str,
    checkpoint: &RuntimeCheckpoint,
) -> Result<RuntimeCheckpoint, ApiError> {
    let Some(meta_store) = state.view_bootstrap_meta_store.as_ref() else {
        return Ok(checkpoint.clone());
    };
    let Some(control) = meta_store
        .read_view_bootstrap(
            &checkpoint.identity.tenant_id,
            &checkpoint.identity.program_id,
            view_id,
        )
        .await
        .map_err(meta_error_to_api)?
    else {
        return Ok(checkpoint.clone());
    };
    let coverage_cut = meta_store
        .capture_ingest_source_cut(CaptureIngestSourceCutRequest {
            relations: control
                .bootstrap_cut
                .relations
                .iter()
                .map(|relation| relation.relation.clone())
                .collect(),
        })
        .await
        .map_err(meta_error_to_api)?;
    let relations = coverage_cut
        .relations
        .iter()
        .map(|relation_cut| {
            let partitions = relation_cut
                .partitions
                .iter()
                .map(|partition_cut| RuntimeCheckpointPartitionCoverageV1 {
                    stream_id: partition_cut.stream_id.clone(),
                    stream_generation: partition_cut.stream_generation,
                    partition_id: partition_cut.partition_id,
                    partition_generation: partition_cut.partition_generation,
                    covered_from_offset_inclusive: partition_cut.base_offset_inclusive,
                    processed_offset_exclusive: checkpoint
                        .input_frontiers
                        .iter()
                        .find(|frontier| {
                            frontier.relation_id == relation_cut.relation.relation_id
                                && frontier.relation_version
                                    == relation_cut.relation.relation_version
                                && frontier.stream_id == partition_cut.stream_id
                                && frontier.partition_id == partition_cut.partition_id
                        })
                        .map(|frontier| frontier.committed_offset_exclusive)
                        .unwrap_or(partition_cut.base_offset_inclusive),
                })
                .collect();
            RuntimeCheckpointRelationCoverageV1 {
                relation_id: relation_cut.relation.relation_id.clone(),
                relation_version: relation_cut.relation.relation_version.clone(),
                relation_generation: relation_cut.relation.relation_generation,
                schema_fingerprint: relation_cut.relation.schema_fingerprint.clone(),
                partitions,
            }
        })
        .collect();
    let coverage = RuntimeCheckpointInputCoverageV1 {
        schema_version: RUNTIME_CHECKPOINT_INPUT_COVERAGE_SCHEMA_VERSION_V1,
        view_generation: control.bootstrap_generation,
        plan_hash: control.plan_hash,
        input_catalog_epoch: coverage_cut.input_catalog_epoch,
        relations,
    }
    .canonicalized()
    .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let mut checkpoint = checkpoint.clone();
    checkpoint.input_coverage = Some(coverage);
    Ok(checkpoint)
}

pub(super) fn standing_runtime_checkpoint_with_publication_output_refs(
    checkpoint: &RuntimeCheckpoint,
    output_manifest_key: Option<&ObjectKey>,
    output_delta_keys: &[&ObjectKey],
) -> RuntimeCheckpoint {
    let mut checkpoint = checkpoint.clone();
    checkpoint.output_manifest_refs.clear();
    if let Some(output_manifest_key) = output_manifest_key {
        checkpoint.output_manifest_refs.push(format!(
            "{STANDING_RUNTIME_OUTPUT_MANIFEST_REF_PREFIX}{}",
            output_manifest_key.as_str()
        ));
    }
    checkpoint
        .output_manifest_refs
        .extend(output_delta_keys.iter().map(|output_delta_key| {
            format!(
                "{STANDING_RUNTIME_OUTPUT_DELTA_REF_PREFIX}{}",
                output_delta_key.as_str()
            )
        }));
    checkpoint
}

pub(super) fn standing_runtime_checkpoint_with_durable_publication_refs(
    checkpoint: &RuntimeCheckpoint,
    output_manifest_key: Option<&ObjectKey>,
    output_delta_publications: &[StandingRuntimeDeltaPublication],
    state_payload_key: &ObjectKey,
) -> RuntimeCheckpoint {
    let output_delta_keys = output_delta_publications
        .iter()
        .map(|publication| &publication.delta_key)
        .collect::<Vec<_>>();
    let mut checkpoint = standing_runtime_checkpoint_with_publication_output_refs(
        checkpoint,
        output_manifest_key,
        &output_delta_keys,
    );
    for (output_ref, publication) in checkpoint
        .output_manifest_refs
        .iter_mut()
        .filter(|output_ref| output_ref.starts_with(STANDING_RUNTIME_OUTPUT_DELTA_REF_PREFIX))
        .zip(output_delta_publications)
    {
        if publication.delta_record.producer_commit.is_some() {
            *output_ref = format!(
                "{STANDING_RUNTIME_OUTPUT_COMMIT_REF_PREFIX}{}",
                publication.delta_key.as_str()
            );
        }
    }
    checkpoint.state_root.object_key = state_payload_key.as_str().to_string();
    checkpoint.state_payload = None;
    checkpoint
}

pub(super) fn standing_runtime_state_payload_record_for_checkpoint(
    checkpoint: &RuntimeCheckpoint,
    view_id: &str,
) -> Result<(ObjectKey, StandingRuntimeStatePayloadRecord), ApiError> {
    let Some(payload) = checkpoint.state_payload.clone() else {
        return Err(ApiError::bad_request(format!(
            "standing runtime checkpoint for view `{view_id}` is missing state payload"
        )));
    };
    if payload.codec_identity != checkpoint.checkpoint_codec_identity {
        return Err(ApiError::bad_request(format!(
            "standing runtime checkpoint state payload codec mismatch for view `{view_id}`"
        )));
    }
    let actual_state_hash = stable_bytes_hash(payload.payload.as_bytes());
    if actual_state_hash != checkpoint.state_root.content_hash {
        return Err(ApiError::bad_request(format!(
            "standing runtime checkpoint state payload hash mismatch for view `{view_id}`"
        )));
    }
    let key = ObjectKey::standing_runtime_state_payload(
        &checkpoint.identity.tenant_id,
        &checkpoint.identity.program_id,
        view_id,
        checkpoint.logical_epoch,
        &checkpoint.state_root.content_hash,
    )
    .map_err(ApiError::bad_request)?;
    let record = StandingRuntimeStatePayloadRecord {
        schema_version: 1,
        record_kind: "standing_runtime_state_payload_v1".to_string(),
        tenant_id: checkpoint.identity.tenant_id.clone(),
        program_id: checkpoint.identity.program_id.clone(),
        view_id: view_id.to_string(),
        logical_epoch: checkpoint.logical_epoch,
        checkpoint_codec_identity: checkpoint.checkpoint_codec_identity.clone(),
        state_content_hash: checkpoint.state_root.content_hash.clone(),
        source_kind: "standing_runtime_checkpoint_state_payload".to_string(),
        payload,
    };
    validate_standing_runtime_state_payload_record(&key, &record)?;
    Ok((key, record))
}

pub(super) async fn persist_standing_runtime_state_payload(
    state: &ApiState,
    state_payload_key: &ObjectKey,
    record: &StandingRuntimeStatePayloadRecord,
) -> Result<(), ApiError> {
    let bytes =
        serde_json::to_vec(record).map_err(|source| ApiError::internal(source.to_string()))?;
    let path = ObjectPath::from(state_payload_key.as_str());
    let result = state
        .store
        .put_opts(
            &path,
            bytes::Bytes::from(bytes.clone()).into(),
            PutMode::Create.into(),
        )
        .await;
    match result {
        Ok(_) => Ok(()),
        Err(object_store::Error::AlreadyExists { .. }) => {
            let existing = state
                .store
                .get(&path)
                .await
                .map_err(ApiError::internal)?
                .bytes()
                .await
                .map_err(ApiError::internal)?;
            if existing.as_ref() == bytes.as_slice() {
                Ok(())
            } else {
                Err(ApiError::conflict(format!(
                    "standing runtime state payload conflict at {}",
                    state_payload_key.as_str()
                )))
            }
        }
        Err(error) => Err(ApiError::internal(error)),
    }
}

pub(super) fn standing_runtime_output_manifest_record_for_checkpoint(
    checkpoint: &RuntimeCheckpoint,
    view_id: &str,
    checkpoint_key: &ObjectKey,
) -> Result<Option<StandingRuntimeOutputPublication>, ApiError> {
    let Some(published_output) = standing_runtime_checkpoint_published_output(checkpoint) else {
        return Ok(None);
    };
    standing_runtime_output_publication(checkpoint, view_id, checkpoint_key, published_output)
        .map(Some)
}

pub(super) fn standing_runtime_output_publication(
    checkpoint: &RuntimeCheckpoint,
    view_id: &str,
    checkpoint_key: &ObjectKey,
    published_output: Value,
) -> Result<StandingRuntimeOutputPublication, ApiError> {
    let output_row_count = standing_runtime_published_output_row_count(&published_output)?;
    let output_bytes = serde_json::to_vec(&published_output)
        .map_err(|source| ApiError::internal(source.to_string()))?;
    let output_content_hash = stable_bytes_hash(&output_bytes);
    let output_page_key = ObjectKey::standing_runtime_output_page(
        &checkpoint.identity.tenant_id,
        &checkpoint.identity.program_id,
        view_id,
        checkpoint.logical_epoch,
        0,
        &output_content_hash,
    )
    .map_err(ApiError::bad_request)?;
    let page_ref = StandingRuntimeOutputPageRef {
        page_index: 0,
        page_key: output_page_key.as_str().to_string(),
        page_content_hash: output_content_hash.clone(),
        row_count: output_row_count,
        output_encoding: "velorix-delta-batch-json-v1".to_string(),
    };
    let page_record = StandingRuntimeOutputPageRecord {
        schema_version: 1,
        record_kind: "standing_runtime_output_page_v1".to_string(),
        tenant_id: checkpoint.identity.tenant_id.clone(),
        program_id: checkpoint.identity.program_id.clone(),
        view_id: view_id.to_string(),
        logical_epoch: checkpoint.logical_epoch,
        output_content_hash: output_content_hash.clone(),
        page_index: 0,
        page_content_hash: output_content_hash.clone(),
        row_count: output_row_count,
        output_encoding: "velorix-delta-batch-json-v1".to_string(),
        source_kind: "standing_runtime_checkpoint_published_output".to_string(),
        published_output: published_output.clone(),
    };
    let output_manifest_key = ObjectKey::standing_runtime_output_manifest(
        &checkpoint.identity.tenant_id,
        &checkpoint.identity.program_id,
        view_id,
        checkpoint.logical_epoch,
        &output_content_hash,
    )
    .map_err(ApiError::bad_request)?;
    let record = StandingRuntimeOutputManifestRecord {
        schema_version: 1,
        record_kind: "standing_runtime_output_manifest_v1".to_string(),
        tenant_id: checkpoint.identity.tenant_id.clone(),
        program_id: checkpoint.identity.program_id.clone(),
        view_id: view_id.to_string(),
        checkpoint_key: checkpoint_key.as_str().to_string(),
        logical_epoch: checkpoint.logical_epoch,
        checkpoint_content_hash: checkpoint.state_root.content_hash.clone(),
        output_content_hash,
        output_encoding: "velorix-delta-batch-json-v1".to_string(),
        output_row_count,
        source_kind: "standing_runtime_checkpoint_published_output".to_string(),
        pages: vec![page_ref],
        published_output,
    };
    validate_standing_runtime_output_page_record(&output_page_key, &page_record)?;
    validate_standing_runtime_output_manifest_record(&output_manifest_key, &record)?;
    Ok(StandingRuntimeOutputPublication {
        manifest_key: output_manifest_key,
        manifest_record: record,
        page_records: vec![(output_page_key, page_record)],
    })
}

pub(super) async fn persist_standing_runtime_output_page(
    state: &ApiState,
    output_page_key: &ObjectKey,
    record: &StandingRuntimeOutputPageRecord,
) -> Result<(), ApiError> {
    let bytes =
        serde_json::to_vec(record).map_err(|source| ApiError::internal(source.to_string()))?;
    let path = ObjectPath::from(output_page_key.as_str());
    let result = state
        .store
        .put_opts(
            &path,
            bytes::Bytes::from(bytes.clone()).into(),
            PutMode::Create.into(),
        )
        .await;
    match result {
        Ok(_) => Ok(()),
        Err(object_store::Error::AlreadyExists { .. }) => {
            let existing = state
                .store
                .get(&path)
                .await
                .map_err(ApiError::internal)?
                .bytes()
                .await
                .map_err(ApiError::internal)?;
            if existing.as_ref() == bytes.as_slice() {
                Ok(())
            } else {
                Err(ApiError::conflict(format!(
                    "standing runtime output page conflict at {}",
                    output_page_key.as_str()
                )))
            }
        }
        Err(error) => Err(ApiError::internal(error)),
    }
}

#[cfg(test)]
pub(super) async fn put_standing_runtime_output_page(
    state: &ApiState,
    output_page_key: &ObjectKey,
    record: &StandingRuntimeOutputPageRecord,
) -> Result<(), ApiError> {
    validate_standing_runtime_output_page_record(output_page_key, record)?;
    let bytes =
        serde_json::to_vec(record).map_err(|source| ApiError::internal(source.to_string()))?;
    state
        .store
        .put(
            &ObjectPath::from(output_page_key.as_str()),
            bytes::Bytes::from(bytes).into(),
        )
        .await
        .map_err(ApiError::internal)?;
    Ok(())
}

pub(super) async fn persist_standing_runtime_output_manifest(
    state: &ApiState,
    output_manifest_key: &ObjectKey,
    record: &StandingRuntimeOutputManifestRecord,
) -> Result<(), ApiError> {
    let bytes =
        serde_json::to_vec(record).map_err(|source| ApiError::internal(source.to_string()))?;
    let path = ObjectPath::from(output_manifest_key.as_str());
    let result = state
        .store
        .put_opts(
            &path,
            bytes::Bytes::from(bytes.clone()).into(),
            PutMode::Create.into(),
        )
        .await;
    match result {
        Ok(_) => Ok(()),
        Err(object_store::Error::AlreadyExists { .. }) => {
            let existing = state
                .store
                .get(&path)
                .await
                .map_err(ApiError::internal)?
                .bytes()
                .await
                .map_err(ApiError::internal)?;
            if existing.as_ref() == bytes.as_slice() {
                Ok(())
            } else {
                Err(ApiError::conflict(format!(
                    "standing runtime output manifest conflict at {}",
                    output_manifest_key.as_str()
                )))
            }
        }
        Err(error) => Err(ApiError::internal(error)),
    }
}

#[cfg(test)]
pub(super) async fn put_standing_runtime_output_manifest(
    state: &ApiState,
    output_manifest_key: &ObjectKey,
    record: &StandingRuntimeOutputManifestRecord,
) -> Result<(), ApiError> {
    validate_standing_runtime_output_manifest_record(output_manifest_key, record)?;
    let bytes =
        serde_json::to_vec(record).map_err(|source| ApiError::internal(source.to_string()))?;
    state
        .store
        .put(
            &ObjectPath::from(output_manifest_key.as_str()),
            bytes::Bytes::from(bytes).into(),
        )
        .await
        .map_err(ApiError::internal)?;
    Ok(())
}

#[cfg(test)]
pub(super) fn standing_runtime_output_delta_records_for_checkpoint(
    checkpoint: &RuntimeCheckpoint,
    view_id: &str,
    output_deltas: &[ViewOutputDelta],
) -> Result<Vec<StandingRuntimeDeltaPublication>, ApiError> {
    let checkpoint_key = ObjectKey::standing_runtime_checkpoint(
        &checkpoint.identity.tenant_id,
        &checkpoint.identity.program_id,
        view_id,
        checkpoint.logical_epoch,
        &checkpoint.state_root.content_hash,
    )
    .map_err(ApiError::bad_request)?;
    standing_runtime_output_delta_records_for_checkpoint_with_binding(
        checkpoint,
        view_id,
        output_deltas,
        &checkpoint_key,
        None,
    )
}

pub(super) fn standing_runtime_output_delta_records_for_checkpoint_with_binding(
    checkpoint: &RuntimeCheckpoint,
    view_id: &str,
    output_deltas: &[ViewOutputDelta],
    checkpoint_key: &ObjectKey,
    published_relation: Option<&PublishedRelationBindingV1>,
) -> Result<Vec<StandingRuntimeDeltaPublication>, ApiError> {
    if published_relation.is_some() && output_deltas.len() != 1 {
        return Err(ApiError::bad_request(format!(
            "published relation `{view_id}` requires exactly one output commit per logical epoch, including empty deltas"
        )));
    }
    let mut publications = Vec::new();
    for output_delta in output_deltas {
        if output_delta.view_id != view_id {
            return Err(ApiError::bad_request(format!(
                "standing runtime output delta identity does not match view `{view_id}`"
            )));
        }
        let consolidated_delta =
            DeltaBatch::from_records(output_delta.delta.net_rows().map_err(|_| {
                ApiError::bad_request("standing runtime output delta is malformed")
            })?);
        let output_delta_value = serde_json::to_value(&consolidated_delta)
            .map_err(|source| ApiError::internal(source.to_string()))?;
        let delta_bytes = serde_json::to_vec(&output_delta_value)
            .map_err(|source| ApiError::internal(source.to_string()))?;
        let delta_content_hash = stable_bytes_hash(&delta_bytes);
        let delta_key = ObjectKey::standing_runtime_output_delta(
            &checkpoint.identity.tenant_id,
            &checkpoint.identity.program_id,
            view_id,
            checkpoint.logical_epoch,
            &delta_content_hash,
        )
        .map_err(ApiError::bad_request)?;
        let delta_row_count = consolidated_delta
            .net_rows()
            .map_err(|_| ApiError::bad_request("standing runtime output delta is malformed"))?
            .len();
        let producer_commit = published_relation
            .map(|binding| {
                standing_runtime_producer_commit_v1(
                    checkpoint,
                    checkpoint_key,
                    output_delta,
                    binding,
                    &delta_content_hash,
                )
            })
            .transpose()?;
        let delta_record = StandingRuntimeOutputDeltaRecord {
            schema_version: if producer_commit.is_some() { 2 } else { 1 },
            record_kind: if producer_commit.is_some() {
                "standing_runtime_output_commit_v1".to_string()
            } else {
                "standing_runtime_output_delta_v1".to_string()
            },
            tenant_id: checkpoint.identity.tenant_id.clone(),
            program_id: checkpoint.identity.program_id.clone(),
            view_id: view_id.to_string(),
            logical_epoch: checkpoint.logical_epoch,
            schema_fingerprint: output_delta.schema_fingerprint.clone(),
            delta_content_hash,
            delta_encoding: "velorix-delta-batch-json-v1".to_string(),
            delta_row_count,
            source_kind: if producer_commit.is_some() {
                "standing_runtime_epoch_output_commit".to_string()
            } else {
                "standing_runtime_epoch_output_delta".to_string()
            },
            output_delta: output_delta_value,
            producer_commit,
        };
        validate_standing_runtime_output_delta_record(&delta_key, &delta_record)?;
        publications.push(StandingRuntimeDeltaPublication {
            delta_key,
            delta_record,
        });
    }
    Ok(publications)
}

#[derive(Serialize)]
struct StandingRuntimeProducerCommitDigestInputV1<'a> {
    schema_version: u32,
    tenant_id: &'a str,
    program_id: &'a str,
    view_id: &'a str,
    logical_epoch: u64,
    producer_view_generation: u64,
    producer_plan_hash: &'a str,
    output_stream_id: &'a str,
    output_schema_hash: &'a str,
    key_descriptor_hash: &'a str,
    delta_codec_identity: &'a str,
    frontier_kind: &'a str,
    schema_fingerprint: &'a str,
    delta_content_hash: &'a str,
    checkpoint_key: &'a str,
    checkpoint_content_hash: &'a str,
    causal_cut_digest: &'a str,
}

fn standing_runtime_producer_commit_v1(
    checkpoint: &RuntimeCheckpoint,
    checkpoint_key: &ObjectKey,
    output_delta: &ViewOutputDelta,
    binding: &PublishedRelationBindingV1,
    delta_content_hash: &str,
) -> Result<StandingRuntimeProducerCommitV1, ApiError> {
    validate_published_relation_binding_v1(binding).map_err(ApiError::bad_request)?;
    let coverage = checkpoint.input_coverage.as_ref().ok_or_else(|| {
        ApiError::bad_request("published relation output commit requires checkpoint input coverage")
    })?;
    if binding.schema_version != PUBLISHED_RELATION_BINDING_SCHEMA_VERSION_V1
        || binding.producer_view_id != output_delta.view_id
        || binding.relation.schema_fingerprint != output_delta.schema_fingerprint
        || binding.producer_view_generation != coverage.view_generation
        || binding.producer_plan_hash != coverage.plan_hash
        || checkpoint_key.as_str()
            != ObjectKey::standing_runtime_checkpoint(
                &checkpoint.identity.tenant_id,
                &checkpoint.identity.program_id,
                &output_delta.view_id,
                checkpoint.logical_epoch,
                &checkpoint.state_root.content_hash,
            )
            .map_err(ApiError::bad_request)?
            .as_str()
    {
        return Err(ApiError::bad_request(format!(
            "published relation output commit identity mismatch for `{}`",
            output_delta.view_id
        )));
    }
    let causal_cut_digest = checkpoint
        .causal_cut
        .as_ref()
        .ok_or_else(|| {
            ApiError::bad_request("published relation output commit requires a causal cut")
        })?
        .stable_digest()
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let digest_input = StandingRuntimeProducerCommitDigestInputV1 {
        schema_version: 1,
        tenant_id: &checkpoint.identity.tenant_id,
        program_id: &checkpoint.identity.program_id,
        view_id: &output_delta.view_id,
        logical_epoch: checkpoint.logical_epoch,
        producer_view_generation: binding.producer_view_generation,
        producer_plan_hash: &binding.producer_plan_hash,
        output_stream_id: &binding.output_stream_id,
        output_schema_hash: &binding.output_schema_hash,
        key_descriptor_hash: &binding.key_descriptor_hash,
        delta_codec_identity: &binding.delta_codec_identity,
        frontier_kind: &binding.frontier_kind,
        schema_fingerprint: &output_delta.schema_fingerprint,
        delta_content_hash,
        checkpoint_key: checkpoint_key.as_str(),
        checkpoint_content_hash: &checkpoint.state_root.content_hash,
        causal_cut_digest: &causal_cut_digest,
    };
    let digest_bytes = serde_json::to_vec(&digest_input)
        .map_err(|source| ApiError::internal(source.to_string()))?;
    Ok(StandingRuntimeProducerCommitV1 {
        schema_version: 1,
        producer_view_generation: binding.producer_view_generation,
        producer_plan_hash: binding.producer_plan_hash.clone(),
        output_stream_id: binding.output_stream_id.clone(),
        output_schema_hash: binding.output_schema_hash.clone(),
        key_descriptor_hash: binding.key_descriptor_hash.clone(),
        delta_codec_identity: binding.delta_codec_identity.clone(),
        frontier_kind: binding.frontier_kind.clone(),
        checkpoint_key: checkpoint_key.as_str().to_string(),
        checkpoint_content_hash: checkpoint.state_root.content_hash.clone(),
        causal_cut_digest,
        producer_commit_digest: stable_bytes_hash(&digest_bytes),
    })
}

pub(super) async fn persist_standing_runtime_output_delta(
    state: &ApiState,
    output_delta_key: &ObjectKey,
    record: &StandingRuntimeOutputDeltaRecord,
) -> Result<(), ApiError> {
    let bytes =
        serde_json::to_vec(record).map_err(|source| ApiError::internal(source.to_string()))?;
    let path = ObjectPath::from(output_delta_key.as_str());
    let result = state
        .store
        .put_opts(
            &path,
            bytes::Bytes::from(bytes.clone()).into(),
            PutMode::Create.into(),
        )
        .await;
    match result {
        Ok(_) => Ok(()),
        Err(object_store::Error::AlreadyExists { .. }) => {
            let existing = state
                .store
                .get(&path)
                .await
                .map_err(ApiError::internal)?
                .bytes()
                .await
                .map_err(ApiError::internal)?;
            if existing.as_ref() == bytes.as_slice() {
                Ok(())
            } else {
                Err(ApiError::conflict(format!(
                    "standing runtime output delta conflict at {}",
                    output_delta_key.as_str()
                )))
            }
        }
        Err(error) => Err(ApiError::internal(error)),
    }
}

pub(super) fn standing_runtime_checkpoint_published_output(
    checkpoint: &RuntimeCheckpoint,
) -> Option<Value> {
    let Some(state_payload) = &checkpoint.state_payload else {
        return None;
    };
    let Ok(payload) = serde_json::from_str::<Value>(&state_payload.payload) else {
        return None;
    };
    payload
        .get("published_output")
        .filter(|published_output| !published_output.is_null())
        .cloned()
}

pub(super) fn standing_runtime_published_output_row_count(
    published_output: &Value,
) -> Result<usize, ApiError> {
    let output: DeltaBatch = serde_json::from_value(published_output.clone())
        .map_err(|source| ApiError::bad_request(source.to_string()))?;
    let rows = output
        .net_rows()
        .map_err(|source| ApiError::bad_request(source.to_string()))?;
    if rows.iter().any(|row| row.weight != 1) {
        return Err(ApiError::bad_request(
            "standing runtime published output contains non-materialized row weights",
        ));
    }
    Ok(rows.len())
}

pub(super) async fn publish_standing_runtime_checkpoint_pointer(
    state: &ApiState,
    expected_previous: Option<StandingRuntimeCheckpointPointer>,
    candidate: StandingRuntimeCheckpointPointer,
    owner: Option<StandingRuntimeOwnerToken>,
) -> Result<(), ApiError> {
    let Some(meta_store) = &state.meta_store else {
        return Ok(());
    };
    let owner = owner.ok_or_else(|| {
        ApiError::service_unavailable("standing runtime owner is required for checkpoint publish")
    })?;
    let retry_expected_previous = expected_previous.clone();
    match meta_store
        .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
            expected_previous,
            candidate: candidate.clone(),
            owner: owner.clone(),
        })
        .await
        .map_err(meta_error_to_api)?
    {
        PublishStandingRuntimeCheckpointOutcome::Published
        | PublishStandingRuntimeCheckpointOutcome::Duplicate => Ok(()),
        PublishStandingRuntimeCheckpointOutcome::Conflict => {
            let Some(previous) = retry_expected_previous else {
                return Err(standing_runtime_checkpoint_publish_conflict(&candidate));
            };
            rehydrate_empty_meta_checkpoint_pointer_and_retry_publish(
                state, &previous, candidate, owner,
            )
            .await
        }
    }
}

pub(super) async fn rehydrate_empty_meta_checkpoint_pointer_and_retry_publish(
    state: &ApiState,
    previous: &StandingRuntimeCheckpointPointer,
    candidate: StandingRuntimeCheckpointPointer,
    owner: StandingRuntimeOwnerToken,
) -> Result<(), ApiError> {
    let Some(meta_store) = &state.meta_store else {
        return Ok(());
    };
    let current = meta_store
        .read_standing_runtime_checkpoint(
            &candidate.tenant_id,
            &candidate.program_id,
            &candidate.view_id,
        )
        .await
        .map_err(meta_error_to_api)?;
    if current.is_some() {
        return Err(standing_runtime_checkpoint_publish_conflict(&candidate));
    }
    validate_checkpoint_pointer_object_exists_for_meta_rehydration(state, previous).await?;

    match meta_store
        .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
            expected_previous: None,
            candidate: previous.clone(),
            owner: owner.clone(),
        })
        .await
        .map_err(meta_error_to_api)?
    {
        PublishStandingRuntimeCheckpointOutcome::Published
        | PublishStandingRuntimeCheckpointOutcome::Duplicate => {}
        PublishStandingRuntimeCheckpointOutcome::Conflict => {
            return Err(standing_runtime_checkpoint_publish_conflict(&candidate));
        }
    }

    match meta_store
        .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
            expected_previous: Some(previous.clone()),
            candidate: candidate.clone(),
            owner,
        })
        .await
        .map_err(meta_error_to_api)?
    {
        PublishStandingRuntimeCheckpointOutcome::Published
        | PublishStandingRuntimeCheckpointOutcome::Duplicate => Ok(()),
        PublishStandingRuntimeCheckpointOutcome::Conflict => {
            Err(standing_runtime_checkpoint_publish_conflict(&candidate))
        }
    }
}

pub(super) async fn validate_checkpoint_pointer_object_exists_for_meta_rehydration(
    state: &ApiState,
    pointer: &StandingRuntimeCheckpointPointer,
) -> Result<(), ApiError> {
    let bytes = state
        .store
        .get(&ObjectPath::from(pointer.checkpoint_key.clone()))
        .await
        .map_err(ApiError::internal)?
        .bytes()
        .await
        .map_err(ApiError::internal)?;
    let actual_manifest_hash = stable_bytes_hash(&bytes);
    if actual_manifest_hash != pointer.manifest_hash {
        return Err(ApiError::bad_request(format!(
            "standing runtime checkpoint manifest hash mismatch for `{}/{}/{}`",
            pointer.tenant_id, pointer.program_id, pointer.view_id
        )));
    }
    let record = standing_runtime_checkpoint_record_from_slice(&bytes)
        .map_err(|source| ApiError::bad_request(source.to_string()))?;
    if record.view_id != pointer.view_id
        || record.checkpoint.identity.tenant_id != pointer.tenant_id
        || record.checkpoint.identity.program_id != pointer.program_id
        || record.checkpoint.logical_epoch != pointer.logical_epoch
        || record.checkpoint.state_root.content_hash != pointer.content_hash
        || record.checkpoint_key != pointer.checkpoint_key
        || pointer.previous_checkpoint_key
            != record
                .previous_checkpoint
                .as_ref()
                .map(|previous| previous.checkpoint_key.clone())
                .unwrap_or_default()
        || pointer.previous_manifest_hash
            != record
                .previous_checkpoint
                .as_ref()
                .map(|previous| previous.manifest_hash.clone())
                .unwrap_or_default()
    {
        return Err(ApiError::bad_request(format!(
            "standing runtime checkpoint pointer/body mismatch for `{}/{}/{}`",
            pointer.tenant_id, pointer.program_id, pointer.view_id
        )));
    }
    Ok(())
}

pub(super) fn standing_runtime_checkpoint_publish_conflict(
    candidate: &StandingRuntimeCheckpointPointer,
) -> ApiError {
    ApiError::conflict(format!(
        "standing runtime checkpoint publish conflict for `{}/{}/{}` at epoch {}",
        candidate.tenant_id, candidate.program_id, candidate.view_id, candidate.logical_epoch
    ))
}

pub(super) fn standing_runtime_checkpoint_pointer_from_record(
    record: &StandingRuntimeCheckpointRecord,
) -> StandingRuntimeCheckpointPointer {
    let manifest_hash = if record.manifest_hash.is_empty() {
        serde_json::to_vec(record)
            .map(|bytes| stable_bytes_hash(&bytes))
            .unwrap_or_default()
    } else {
        record.manifest_hash.clone()
    };
    let (bootstrap_generation, plan_hash, coverage_hash, input_coverage) =
        standing_runtime_checkpoint_coverage_commitment(&record.checkpoint);
    StandingRuntimeCheckpointPointer {
        tenant_id: record.checkpoint.identity.tenant_id.clone(),
        program_id: record.checkpoint.identity.program_id.clone(),
        view_id: record.view_id.clone(),
        checkpoint_key: record.checkpoint_key.clone(),
        logical_epoch: record.checkpoint.logical_epoch,
        content_hash: record.checkpoint.state_root.content_hash.clone(),
        manifest_hash,
        output_manifest_refs: record.checkpoint.output_manifest_refs.clone(),
        bootstrap_generation,
        plan_hash,
        coverage_hash,
        input_coverage,
        previous_checkpoint_key: record
            .previous_checkpoint
            .as_ref()
            .map(|pointer| pointer.checkpoint_key.clone())
            .unwrap_or_default(),
        previous_manifest_hash: record
            .previous_checkpoint
            .as_ref()
            .map(|pointer| pointer.manifest_hash.clone())
            .unwrap_or_default(),
    }
}

fn standing_runtime_checkpoint_coverage_commitment(
    checkpoint: &RuntimeCheckpoint,
) -> (
    u64,
    String,
    String,
    Option<RuntimeCheckpointInputCoverageV1>,
) {
    let Some(coverage) = checkpoint.input_coverage.clone() else {
        return (0, String::new(), String::new(), None);
    };
    (
        coverage.view_generation,
        coverage.plan_hash.clone(),
        coverage.stable_hash().unwrap_or_default(),
        Some(coverage),
    )
}

pub(super) fn standing_runtime_replay_plan_from_record_ref(
    record: &StandingRuntimeCheckpointRecord,
) -> StandingRuntimeReplayPlan {
    StandingRuntimeReplayPlan {
        replay_checkpoints: record.replay_checkpoints.clone(),
        input_frontiers: record.checkpoint.input_frontiers.clone(),
    }
}

pub(super) async fn read_latest_standing_runtime_checkpoint(
    state: &ApiState,
    identity: &StandingProgramIdentity,
    view_id: &str,
) -> Result<Option<StandingRuntimeCheckpointRecord>, ApiError> {
    let latest_key = ObjectKey::standing_runtime_latest_checkpoint(
        &identity.tenant_id,
        &identity.program_id,
        view_id,
    )
    .map_err(ApiError::bad_request)?;
    if let Some(meta_store) = &state.meta_store {
        if let Some(pointer) = meta_store
            .read_standing_runtime_checkpoint(&identity.tenant_id, &identity.program_id, view_id)
            .await
            .map_err(meta_error_to_api)?
        {
            return read_standing_runtime_checkpoint_record_from_pointer(
                state, identity, view_id, &pointer,
            )
            .await
            .map(Some);
        }
        return Ok(None);
    }
    match read_standing_runtime_checkpoint_record_from_latest_cache(
        state,
        identity,
        view_id,
        &latest_key,
    )
    .await
    {
        Ok(Some(record)) => return Ok(Some(record)),
        Ok(None) => {}
        Err(error) => return Err(error),
    }
    let prefix = ObjectPath::from(format!(
        "v1/standing-runtime-checkpoints/{}/{}/{view_id}/epochs",
        identity.tenant_id, identity.program_id
    ));
    let mut stream = state.store.list(Some(&prefix));
    let mut latest_checkpoint: Option<(String, StandingRuntimeCheckpointKeyParts)> = None;
    while let Some(meta) = stream.try_next().await.map_err(ApiError::internal)? {
        let location = meta.location.to_string();
        if location.ends_with(".checkpoint.json") {
            let (_, parts) = ObjectKey::parse_standing_runtime_checkpoint(location.clone())
                .map_err(ApiError::bad_request)?;
            latest_checkpoint = Some(match latest_checkpoint {
                Some((current, current_parts))
                    if current_parts.logical_epoch > parts.logical_epoch =>
                {
                    (current, current_parts)
                }
                Some((_current, current_parts))
                    if current_parts.logical_epoch == parts.logical_epoch =>
                {
                    return Err(ApiError::bad_request(format!(
                        "multiple standing runtime checkpoints for `{}/{}/{view_id}` epoch {}",
                        identity.tenant_id, identity.program_id, parts.logical_epoch
                    )));
                }
                _ => (location, parts),
            });
        }
    }

    let Some((latest_checkpoint_path, checkpoint_key_parts)) = latest_checkpoint else {
        return Ok(None);
    };
    let bytes = state
        .store
        .get(&ObjectPath::from(latest_checkpoint_path.clone()))
        .await
        .map_err(ApiError::internal)?
        .bytes()
        .await
        .map_err(ApiError::internal)?;
    let mut record = standing_runtime_checkpoint_record_from_slice(&bytes)
        .map_err(|source| ApiError::bad_request(source.to_string()))?;
    if record.checkpoint_key.is_empty() {
        record.checkpoint_key = latest_checkpoint_path.clone();
    }
    if record.schema_version != 1
        || record.record_kind != "standing_runtime_checkpoint_v1"
        || record.view_id != view_id
        || record.checkpoint.identity != *identity
    {
        return Err(ApiError::bad_request(format!(
            "standing runtime checkpoint record identity mismatch for `{}/{}/{view_id}`",
            identity.tenant_id, identity.program_id
        )));
    }
    if checkpoint_key_parts.tenant_id != record.checkpoint.identity.tenant_id
        || checkpoint_key_parts.program_id != record.checkpoint.identity.program_id
        || checkpoint_key_parts.view_id != record.view_id
        || checkpoint_key_parts.logical_epoch != record.checkpoint.logical_epoch
        || checkpoint_key_parts.content_hash != record.checkpoint.state_root.content_hash
        || record.checkpoint_key != latest_checkpoint_path
    {
        return Err(ApiError::bad_request(format!(
            "standing runtime checkpoint object key/body mismatch for `{}/{}/{view_id}`",
            identity.tenant_id, identity.program_id
        )));
    }
    let (bootstrap_generation, plan_hash, coverage_hash, input_coverage) =
        standing_runtime_checkpoint_coverage_commitment(&record.checkpoint);
    let pointer = StandingRuntimeCheckpointPointer {
        tenant_id: checkpoint_key_parts.tenant_id,
        program_id: checkpoint_key_parts.program_id,
        view_id: checkpoint_key_parts.view_id,
        checkpoint_key: latest_checkpoint_path,
        logical_epoch: checkpoint_key_parts.logical_epoch,
        content_hash: checkpoint_key_parts.content_hash,
        manifest_hash: stable_bytes_hash(&bytes),
        output_manifest_refs: record.checkpoint.output_manifest_refs.clone(),
        bootstrap_generation,
        plan_hash,
        coverage_hash,
        input_coverage,
        previous_checkpoint_key: record
            .previous_checkpoint
            .as_ref()
            .map(|previous| previous.checkpoint_key.clone())
            .unwrap_or_default(),
        previous_manifest_hash: record
            .previous_checkpoint
            .as_ref()
            .map(|previous| previous.manifest_hash.clone())
            .unwrap_or_default(),
    };
    validate_standing_runtime_checkpoint_output_refs(&record, &pointer)?;
    hydrate_standing_runtime_checkpoint_state_payload(state, &mut record).await?;
    validate_standing_runtime_checkpoint_output_manifest_records(state, &record).await?;
    validate_standing_runtime_checkpoint_replay_frontiers(&record)?;

    Ok(Some(record))
}

pub(super) async fn read_standing_runtime_checkpoint_record_from_latest_cache(
    state: &ApiState,
    identity: &StandingProgramIdentity,
    view_id: &str,
    latest_key: &ObjectKey,
) -> Result<Option<StandingRuntimeCheckpointRecord>, ApiError> {
    let bytes = match state
        .store
        .get(&ObjectPath::from(latest_key.as_str()))
        .await
    {
        Ok(object) => object.bytes().await.map_err(ApiError::internal)?,
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(error) => return Err(ApiError::internal(error)),
    };
    let mut record = standing_runtime_checkpoint_record_from_slice(&bytes)
        .map_err(|source| ApiError::bad_request(source.to_string()))?;
    let pointer = standing_runtime_checkpoint_pointer_from_record(&record);
    validate_standing_runtime_checkpoint_record(identity, view_id, &pointer, &record)?;
    hydrate_standing_runtime_checkpoint_state_payload(state, &mut record).await?;
    validate_standing_runtime_checkpoint_output_manifest_records(state, &record).await?;
    validate_standing_runtime_checkpoint_replay_frontiers(&record)?;
    Ok(Some(record))
}

pub(super) async fn read_standing_runtime_checkpoint_record_from_pointer(
    state: &ApiState,
    identity: &StandingProgramIdentity,
    view_id: &str,
    pointer: &StandingRuntimeCheckpointPointer,
) -> Result<StandingRuntimeCheckpointRecord, ApiError> {
    let record = read_standing_runtime_checkpoint_record_from_pointer_for_scope(
        state,
        &identity.tenant_id,
        &identity.program_id,
        view_id,
        pointer,
    )
    .await?;
    if record.checkpoint.identity != *identity {
        return Err(ApiError::bad_request(format!(
            "standing runtime checkpoint record identity mismatch for `{}/{}/{view_id}`",
            identity.tenant_id, identity.program_id
        )));
    }
    Ok(record)
}

async fn read_standing_runtime_checkpoint_record_from_pointer_for_scope(
    state: &ApiState,
    tenant_id: &str,
    program_id: &str,
    view_id: &str,
    pointer: &StandingRuntimeCheckpointPointer,
) -> Result<StandingRuntimeCheckpointRecord, ApiError> {
    if pointer.tenant_id != tenant_id
        || pointer.program_id != program_id
        || pointer.view_id != view_id
    {
        return Err(ApiError::bad_request(format!(
            "standing runtime checkpoint pointer scope mismatch for `{tenant_id}/{program_id}/{view_id}`"
        )));
    }
    let bytes = state
        .store
        .get(&ObjectPath::from(pointer.checkpoint_key.clone()))
        .await
        .map_err(ApiError::internal)?
        .bytes()
        .await
        .map_err(ApiError::internal)?;
    const MAX_CHECKPOINT_RECORD_BYTES: usize = 16 * 1024 * 1024;
    if bytes.len() > MAX_CHECKPOINT_RECORD_BYTES {
        return Err(ApiError::bad_request(format!(
            "standing runtime checkpoint exceeds the record-size budget for `{tenant_id}/{program_id}/{view_id}`"
        )));
    }
    let actual_manifest_hash = stable_bytes_hash(&bytes);
    if actual_manifest_hash != pointer.manifest_hash {
        return Err(ApiError::bad_request(format!(
            "standing runtime checkpoint manifest hash mismatch for `{}/{}/{}`",
            pointer.tenant_id, pointer.program_id, pointer.view_id
        )));
    }
    let mut record = standing_runtime_checkpoint_record_from_slice(&bytes)
        .map_err(|source| ApiError::bad_request(source.to_string()))?;
    if record.checkpoint_key.is_empty() {
        record.checkpoint_key = pointer.checkpoint_key.clone();
    }
    let identity = record.checkpoint.identity.clone();
    if identity.tenant_id != tenant_id
        || identity.program_id != program_id
        || !identity
            .view_ids
            .iter()
            .any(|candidate| candidate == view_id)
    {
        return Err(ApiError::bad_request(format!(
            "standing runtime checkpoint record scope mismatch for `{tenant_id}/{program_id}/{view_id}`"
        )));
    }
    validate_standing_runtime_checkpoint_record(&identity, view_id, pointer, &record)?;
    hydrate_standing_runtime_checkpoint_state_payload(state, &mut record).await?;
    record
        .checkpoint
        .validate_identity(&identity)
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    validate_standing_runtime_checkpoint_output_manifest_records(state, &record).await?;
    validate_standing_runtime_checkpoint_replay_frontiers(&record)?;
    Ok(record)
}

pub(super) fn validate_standing_runtime_checkpoint_record(
    identity: &StandingProgramIdentity,
    view_id: &str,
    pointer: &StandingRuntimeCheckpointPointer,
    record: &StandingRuntimeCheckpointRecord,
) -> Result<(), ApiError> {
    let (bootstrap_generation, plan_hash, coverage_hash, input_coverage) =
        standing_runtime_checkpoint_coverage_commitment(&record.checkpoint);
    if record.schema_version != 1
        || record.record_kind != "standing_runtime_checkpoint_v1"
        || record.view_id != view_id
        || record.checkpoint.identity != *identity
    {
        return Err(ApiError::bad_request(format!(
            "standing runtime checkpoint record identity mismatch for `{}/{}/{view_id}`",
            identity.tenant_id, identity.program_id
        )));
    }
    if pointer.tenant_id != record.checkpoint.identity.tenant_id
        || pointer.program_id != record.checkpoint.identity.program_id
        || pointer.view_id != record.view_id
        || pointer.checkpoint_key != record.checkpoint_key
        || pointer.logical_epoch != record.checkpoint.logical_epoch
        || pointer.content_hash != record.checkpoint.state_root.content_hash
        || pointer.manifest_hash != record.manifest_hash
        || pointer.bootstrap_generation != bootstrap_generation
        || pointer.plan_hash != plan_hash
        || pointer.coverage_hash != coverage_hash
        || pointer.input_coverage != input_coverage
        || pointer.previous_checkpoint_key
            != record
                .previous_checkpoint
                .as_ref()
                .map(|previous| previous.checkpoint_key.clone())
                .unwrap_or_default()
        || pointer.previous_manifest_hash
            != record
                .previous_checkpoint
                .as_ref()
                .map(|previous| previous.manifest_hash.clone())
                .unwrap_or_default()
        || (!pointer.output_manifest_refs.is_empty()
            && pointer.output_manifest_refs != record.checkpoint.output_manifest_refs)
    {
        return Err(ApiError::bad_request(format!(
            "standing runtime checkpoint pointer/body mismatch for `{}/{}/{view_id}`",
            identity.tenant_id, identity.program_id
        )));
    }
    let (checkpoint_key, checkpoint_key_parts) =
        ObjectKey::parse_standing_runtime_checkpoint(pointer.checkpoint_key.clone())
            .map_err(ApiError::bad_request)?;
    if checkpoint_key_parts.tenant_id != pointer.tenant_id
        || checkpoint_key_parts.program_id != pointer.program_id
        || checkpoint_key_parts.view_id != pointer.view_id
        || checkpoint_key_parts.logical_epoch != pointer.logical_epoch
        || checkpoint_key_parts.content_hash != pointer.content_hash
        || checkpoint_key.as_str() != pointer.checkpoint_key
    {
        return Err(ApiError::bad_request(format!(
            "standing runtime checkpoint pointer key/body mismatch for `{}/{}/{view_id}`",
            identity.tenant_id, identity.program_id
        )));
    }
    validate_standing_runtime_checkpoint_output_refs(record, pointer)?;
    Ok(())
}

pub(super) async fn hydrate_standing_runtime_checkpoint_state_payload(
    state: &ApiState,
    record: &mut StandingRuntimeCheckpointRecord,
) -> Result<(), ApiError> {
    let state_root_key = record.checkpoint.state_root.object_key.clone();
    let parsed_state_key = ObjectKey::parse_standing_runtime_state_payload(state_root_key.clone());
    let Ok((state_payload_key, parts)) = parsed_state_key else {
        if record.checkpoint.state_payload.is_some() {
            return Ok(());
        }
        return Err(ApiError::bad_request(format!(
            "standing runtime checkpoint for view `{}` is missing durable state payload root",
            record.view_id
        )));
    };
    if parts.tenant_id != record.checkpoint.identity.tenant_id
        || parts.program_id != record.checkpoint.identity.program_id
        || parts.view_id != record.view_id
        || parts.logical_epoch != record.checkpoint.logical_epoch
        || parts.state_content_hash != record.checkpoint.state_root.content_hash
    {
        return Err(ApiError::bad_request(format!(
            "standing runtime checkpoint state payload key/body mismatch for `{}/{}/{}`",
            record.checkpoint.identity.tenant_id,
            record.checkpoint.identity.program_id,
            record.view_id
        )));
    }
    let bytes = state
        .store
        .get(&ObjectPath::from(state_payload_key.as_str()))
        .await
        .map_err(ApiError::internal)?
        .bytes()
        .await
        .map_err(ApiError::internal)?;
    let state_payload_record: StandingRuntimeStatePayloadRecord = serde_json::from_slice(&bytes)
        .map_err(|source| ApiError::bad_request(source.to_string()))?;
    validate_standing_runtime_state_payload_record(&state_payload_key, &state_payload_record)?;
    if state_payload_record.tenant_id != record.checkpoint.identity.tenant_id
        || state_payload_record.program_id != record.checkpoint.identity.program_id
        || state_payload_record.view_id != record.view_id
        || state_payload_record.logical_epoch != record.checkpoint.logical_epoch
        || state_payload_record.checkpoint_codec_identity
            != record.checkpoint.checkpoint_codec_identity
        || state_payload_record.state_content_hash != record.checkpoint.state_root.content_hash
    {
        return Err(ApiError::bad_request(format!(
            "standing runtime checkpoint state payload record mismatch for `{}/{}/{}`",
            record.checkpoint.identity.tenant_id,
            record.checkpoint.identity.program_id,
            record.view_id
        )));
    }
    if let Some(existing_payload) = &record.checkpoint.state_payload {
        if existing_payload != &state_payload_record.payload {
            return Err(ApiError::bad_request(format!(
                "standing runtime checkpoint embedded state payload mismatch for `{}/{}/{}`",
                record.checkpoint.identity.tenant_id,
                record.checkpoint.identity.program_id,
                record.view_id
            )));
        }
    }
    record.checkpoint.state_payload = Some(state_payload_record.payload);
    Ok(())
}

pub(super) fn validate_standing_runtime_checkpoint_output_refs(
    record: &StandingRuntimeCheckpointRecord,
    pointer: &StandingRuntimeCheckpointPointer,
) -> Result<(), ApiError> {
    for output_ref in &record.checkpoint.output_manifest_refs {
        if let Some(output_manifest_key) =
            output_ref.strip_prefix(STANDING_RUNTIME_OUTPUT_MANIFEST_REF_PREFIX)
        {
            let (parsed_key, parts) =
                ObjectKey::parse_standing_runtime_output_manifest(output_manifest_key.to_string())
                    .map_err(ApiError::bad_request)?;
            if parsed_key.as_str() != output_manifest_key
                || parts.tenant_id != pointer.tenant_id
                || parts.program_id != pointer.program_id
                || parts.view_id != pointer.view_id
                || parts.logical_epoch != pointer.logical_epoch
            {
                return Err(ApiError::bad_request(format!(
                    "standing runtime checkpoint output manifest ref mismatch for `{}/{}/{}`",
                    pointer.tenant_id, pointer.program_id, pointer.view_id
                )));
            }
        } else if let Some(output_delta_key) = output_ref
            .strip_prefix(STANDING_RUNTIME_OUTPUT_DELTA_REF_PREFIX)
            .or_else(|| output_ref.strip_prefix(STANDING_RUNTIME_OUTPUT_COMMIT_REF_PREFIX))
        {
            let (parsed_key, parts) =
                ObjectKey::parse_standing_runtime_output_delta(output_delta_key.to_string())
                    .map_err(ApiError::bad_request)?;
            if parsed_key.as_str() != output_delta_key
                || parts.tenant_id != pointer.tenant_id
                || parts.program_id != pointer.program_id
                || parts.view_id != pointer.view_id
                || parts.logical_epoch != pointer.logical_epoch
            {
                return Err(ApiError::bad_request(format!(
                    "standing runtime checkpoint output delta ref mismatch for `{}/{}/{}`",
                    pointer.tenant_id, pointer.program_id, pointer.view_id
                )));
            }
        } else {
            return Err(ApiError::bad_request(format!(
                "unsupported standing runtime checkpoint output ref for view `{}`",
                record.view_id
            )));
        }
    }
    Ok(())
}

pub(super) async fn validate_standing_runtime_checkpoint_output_manifest_records(
    state: &ApiState,
    record: &StandingRuntimeCheckpointRecord,
) -> Result<(), ApiError> {
    for output_ref in &record.checkpoint.output_manifest_refs {
        let is_commit_ref = output_ref
            .strip_prefix(STANDING_RUNTIME_OUTPUT_COMMIT_REF_PREFIX)
            .is_some();
        if is_commit_ref
            || output_ref
                .strip_prefix(STANDING_RUNTIME_OUTPUT_DELTA_REF_PREFIX)
                .is_some()
        {
            let (_key, delta) =
                read_standing_runtime_output_delta_record(state, output_ref, &record.view_id)
                    .await?;
            if delta.tenant_id != record.checkpoint.identity.tenant_id
                || delta.program_id != record.checkpoint.identity.program_id
                || delta.view_id != record.view_id
                || delta.logical_epoch != record.checkpoint.logical_epoch
            {
                return Err(ApiError::bad_request(format!(
                    "standing runtime output delta body/checkpoint mismatch for `{}/{}/{}`",
                    record.checkpoint.identity.tenant_id,
                    record.checkpoint.identity.program_id,
                    record.view_id
                )));
            }
            if is_commit_ref {
                let commit = delta.producer_commit.as_ref().ok_or_else(|| {
                    ApiError::bad_request(format!(
                        "standing runtime output commit ref points to a legacy delta for `{}/{}/{}`",
                        record.checkpoint.identity.tenant_id,
                        record.checkpoint.identity.program_id,
                        record.view_id
                    ))
                })?;
                let causal_cut_digest = record
                    .checkpoint
                    .causal_cut
                    .as_ref()
                    .ok_or_else(|| {
                        ApiError::bad_request(
                            "standing runtime output commit checkpoint is missing causal cut",
                        )
                    })?
                    .stable_digest()
                    .map_err(|error| ApiError::bad_request(error.to_string()))?;
                if commit.checkpoint_key != record.checkpoint_key
                    || commit.checkpoint_content_hash != record.checkpoint.state_root.content_hash
                    || commit.causal_cut_digest != causal_cut_digest
                {
                    return Err(ApiError::bad_request(format!(
                        "standing runtime output commit body/checkpoint binding mismatch for `{}/{}/{}`",
                        record.checkpoint.identity.tenant_id,
                        record.checkpoint.identity.program_id,
                        record.view_id
                    )));
                }
            }
            continue;
        }
        let (_key, manifest) =
            read_standing_runtime_output_manifest_record(state, output_ref, &record.view_id)
                .await?;
        if manifest.tenant_id != record.checkpoint.identity.tenant_id
            || manifest.program_id != record.checkpoint.identity.program_id
            || manifest.view_id != record.view_id
            || manifest.checkpoint_key != record.checkpoint_key
            || manifest.logical_epoch != record.checkpoint.logical_epoch
            || manifest.checkpoint_content_hash != record.checkpoint.state_root.content_hash
        {
            return Err(ApiError::bad_request(format!(
                "standing runtime output manifest body/checkpoint mismatch for `{}/{}/{}`",
                record.checkpoint.identity.tenant_id,
                record.checkpoint.identity.program_id,
                record.view_id
            )));
        }
        for page in &manifest.pages {
            let (_page_key, page_record) =
                read_standing_runtime_output_page_record(state, page, &record.view_id).await?;
            if page_record.tenant_id != record.checkpoint.identity.tenant_id
                || page_record.program_id != record.checkpoint.identity.program_id
                || page_record.view_id != record.view_id
                || page_record.logical_epoch != record.checkpoint.logical_epoch
                || page_record.output_content_hash != manifest.output_content_hash
            {
                return Err(ApiError::bad_request(format!(
                    "standing runtime output page body/checkpoint mismatch for `{}/{}/{}`",
                    record.checkpoint.identity.tenant_id,
                    record.checkpoint.identity.program_id,
                    record.view_id
                )));
            }
        }
    }
    Ok(())
}

pub(super) async fn read_standing_runtime_output_manifest_record(
    state: &ApiState,
    output_ref: &str,
    view_id: &str,
) -> Result<(ObjectKey, StandingRuntimeOutputManifestRecord), ApiError> {
    maybe_read_standing_runtime_output_manifest_record(state, output_ref, view_id)
        .await?
        .ok_or_else(|| {
            ApiError::internal(format!(
                "standing runtime output manifest is missing for view `{view_id}`"
            ))
        })
}

pub(super) async fn maybe_read_standing_runtime_output_manifest_record(
    state: &ApiState,
    output_ref: &str,
    view_id: &str,
) -> Result<Option<(ObjectKey, StandingRuntimeOutputManifestRecord)>, ApiError> {
    let output_manifest_key = output_ref
        .strip_prefix(STANDING_RUNTIME_OUTPUT_MANIFEST_REF_PREFIX)
        .ok_or_else(|| {
            ApiError::bad_request(format!(
                "unsupported standing runtime checkpoint output manifest ref for view `{view_id}`"
            ))
        })?;
    let object = match state
        .store
        .get(&ObjectPath::from(output_manifest_key.to_string()))
        .await
    {
        Ok(object) => object,
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(error) => return Err(ApiError::internal(error)),
    };
    let bytes = object.bytes().await.map_err(ApiError::internal)?;
    let manifest: StandingRuntimeOutputManifestRecord = serde_json::from_slice(&bytes)
        .map_err(|source| ApiError::bad_request(source.to_string()))?;
    let (key, _) =
        ObjectKey::parse_standing_runtime_output_manifest(output_manifest_key.to_string())
            .map_err(ApiError::bad_request)?;
    validate_standing_runtime_output_manifest_record(&key, &manifest)?;
    Ok(Some((key, manifest)))
}

pub(super) async fn read_standing_runtime_output_page_record(
    state: &ApiState,
    page: &StandingRuntimeOutputPageRef,
    view_id: &str,
) -> Result<(ObjectKey, StandingRuntimeOutputPageRecord), ApiError> {
    let bytes = state
        .store
        .get(&ObjectPath::from(page.page_key.clone()))
        .await
        .map_err(ApiError::internal)?
        .bytes()
        .await
        .map_err(ApiError::internal)?;
    let record: StandingRuntimeOutputPageRecord = serde_json::from_slice(&bytes)
        .map_err(|source| ApiError::bad_request(source.to_string()))?;
    let (key, _) = ObjectKey::parse_standing_runtime_output_page(page.page_key.clone())
        .map_err(ApiError::bad_request)?;
    validate_standing_runtime_output_page_ref(page, view_id)?;
    validate_standing_runtime_output_page_record(&key, &record)?;
    if page.page_index != record.page_index
        || page.page_content_hash != record.page_content_hash
        || page.row_count != record.row_count
        || page.output_encoding != record.output_encoding
    {
        return Err(ApiError::bad_request(format!(
            "standing runtime output page ref/body mismatch for view `{view_id}`"
        )));
    }
    Ok((key, record))
}

pub(super) async fn read_standing_runtime_output_delta_record(
    state: &ApiState,
    output_ref: &str,
    view_id: &str,
) -> Result<(ObjectKey, StandingRuntimeOutputDeltaRecord), ApiError> {
    let output_delta_key = output_ref
        .strip_prefix(STANDING_RUNTIME_OUTPUT_DELTA_REF_PREFIX)
        .or_else(|| output_ref.strip_prefix(STANDING_RUNTIME_OUTPUT_COMMIT_REF_PREFIX))
        .ok_or_else(|| {
            ApiError::bad_request(format!(
                "unsupported standing runtime checkpoint output delta ref for view `{view_id}`"
            ))
        })?;
    let bytes = state
        .store
        .get(&ObjectPath::from(output_delta_key.to_string()))
        .await
        .map_err(ApiError::internal)?
        .bytes()
        .await
        .map_err(ApiError::internal)?;
    let record: StandingRuntimeOutputDeltaRecord = serde_json::from_slice(&bytes)
        .map_err(|source| ApiError::bad_request(source.to_string()))?;
    let (key, _) = ObjectKey::parse_standing_runtime_output_delta(output_delta_key.to_string())
        .map_err(ApiError::bad_request)?;
    validate_standing_runtime_output_delta_record(&key, &record)?;
    Ok((key, record))
}

pub(super) fn validate_standing_runtime_output_manifest_record(
    key: &ObjectKey,
    record: &StandingRuntimeOutputManifestRecord,
) -> Result<(), ApiError> {
    if record.schema_version != 1 || record.record_kind != "standing_runtime_output_manifest_v1" {
        return Err(ApiError::bad_request(format!(
            "standing runtime output manifest record identity mismatch for `{}/{}/{}`",
            record.tenant_id, record.program_id, record.view_id
        )));
    }
    if record.output_encoding != "velorix-delta-batch-json-v1"
        || record.source_kind != "standing_runtime_checkpoint_published_output"
    {
        return Err(ApiError::bad_request(format!(
            "standing runtime output manifest codec/source mismatch for `{}/{}/{}`",
            record.tenant_id, record.program_id, record.view_id
        )));
    }
    let (_, parts) = ObjectKey::parse_standing_runtime_output_manifest(key.as_str().to_string())
        .map_err(ApiError::bad_request)?;
    let output_bytes = serde_json::to_vec(&record.published_output)
        .map_err(|source| ApiError::internal(source.to_string()))?;
    let actual_output_hash = stable_bytes_hash(&output_bytes);
    let output_row_count = standing_runtime_published_output_row_count(&record.published_output)?;
    if record.pages.is_empty() {
        return Err(ApiError::bad_request(format!(
            "standing runtime output manifest has no page index for `{}/{}/{}`",
            record.tenant_id, record.program_id, record.view_id
        )));
    }
    let mut page_indexes = BTreeSet::new();
    let mut indexed_row_count = 0usize;
    for page in &record.pages {
        validate_standing_runtime_output_page_ref(page, &record.view_id)?;
        if !page_indexes.insert(page.page_index) {
            return Err(ApiError::bad_request(format!(
                "duplicate standing runtime output page index for `{}/{}/{}`",
                record.tenant_id, record.program_id, record.view_id
            )));
        }
        let (_, page_parts) = ObjectKey::parse_standing_runtime_output_page(page.page_key.clone())
            .map_err(ApiError::bad_request)?;
        if page_parts.tenant_id != record.tenant_id
            || page_parts.program_id != record.program_id
            || page_parts.view_id != record.view_id
            || page_parts.logical_epoch != record.logical_epoch
            || page_parts.page_index != page.page_index
            || page_parts.page_content_hash != page.page_content_hash
        {
            return Err(ApiError::bad_request(format!(
                "standing runtime output page ref mismatch for `{}/{}/{}`",
                record.tenant_id, record.program_id, record.view_id
            )));
        }
        indexed_row_count = indexed_row_count
            .checked_add(page.row_count)
            .ok_or_else(|| ApiError::bad_request("standing runtime output page row overflow"))?;
    }
    if parts.tenant_id != record.tenant_id
        || parts.program_id != record.program_id
        || parts.view_id != record.view_id
        || parts.logical_epoch != record.logical_epoch
        || parts.output_content_hash != record.output_content_hash
        || record.output_content_hash != actual_output_hash
        || record.output_row_count != output_row_count
        || indexed_row_count != record.output_row_count
    {
        return Err(ApiError::bad_request(format!(
            "standing runtime output manifest key/body mismatch for `{}/{}/{}`",
            record.tenant_id, record.program_id, record.view_id
        )));
    }
    Ok(())
}

pub(super) fn validate_standing_runtime_output_delta_record(
    key: &ObjectKey,
    record: &StandingRuntimeOutputDeltaRecord,
) -> Result<(), ApiError> {
    let is_legacy_delta =
        record.schema_version == 1 && record.record_kind == "standing_runtime_output_delta_v1";
    let is_producer_commit = record.schema_version == 2
        && record.record_kind == "standing_runtime_output_commit_v1"
        && record.producer_commit.is_some();
    if !is_legacy_delta && !is_producer_commit {
        return Err(ApiError::bad_request(format!(
            "standing runtime output delta record identity mismatch for `{}/{}/{}`",
            record.tenant_id, record.program_id, record.view_id
        )));
    }
    if record.delta_encoding != "velorix-delta-batch-json-v1"
        || (is_legacy_delta && record.source_kind != "standing_runtime_epoch_output_delta")
        || (is_producer_commit && record.source_kind != "standing_runtime_epoch_output_commit")
        || record.schema_fingerprint.is_empty()
    {
        return Err(ApiError::bad_request(format!(
            "standing runtime output delta codec/source mismatch for `{}/{}/{}`",
            record.tenant_id, record.program_id, record.view_id
        )));
    }
    let (_, parts) = ObjectKey::parse_standing_runtime_output_delta(key.as_str().to_string())
        .map_err(ApiError::bad_request)?;
    let delta_bytes = serde_json::to_vec(&record.output_delta)
        .map_err(|source| ApiError::internal(source.to_string()))?;
    let actual_delta_hash = stable_bytes_hash(&delta_bytes);
    let output_delta: DeltaBatch = serde_json::from_value(record.output_delta.clone())
        .map_err(|source| ApiError::bad_request(source.to_string()))?;
    let delta_row_count = output_delta
        .net_rows()
        .map_err(|_| ApiError::bad_request("standing runtime output delta is malformed"))?
        .len();
    if parts.tenant_id != record.tenant_id
        || parts.program_id != record.program_id
        || parts.view_id != record.view_id
        || parts.logical_epoch != record.logical_epoch
        || parts.delta_content_hash != record.delta_content_hash
        || actual_delta_hash != record.delta_content_hash
        || delta_row_count != record.delta_row_count
    {
        return Err(ApiError::bad_request(format!(
            "standing runtime output delta key/body mismatch for `{}/{}/{}`",
            record.tenant_id, record.program_id, record.view_id
        )));
    }
    if is_legacy_delta && record.producer_commit.is_some() {
        return Err(ApiError::bad_request(format!(
            "legacy standing runtime output delta carries a producer commit for `{}/{}/{}`",
            record.tenant_id, record.program_id, record.view_id
        )));
    }
    if let Some(commit) = &record.producer_commit {
        validate_standing_runtime_producer_commit_v1(record, commit)?;
    }
    Ok(())
}

fn validate_standing_runtime_producer_commit_v1(
    record: &StandingRuntimeOutputDeltaRecord,
    commit: &StandingRuntimeProducerCommitV1,
) -> Result<(), ApiError> {
    let (_, checkpoint_parts) =
        ObjectKey::parse_standing_runtime_checkpoint(commit.checkpoint_key.clone())
            .map_err(ApiError::bad_request)?;
    let expected_stream_prefix = format!(
        "view/{}/generation/{}/output/",
        record.view_id, commit.producer_view_generation
    );
    let digest_input = StandingRuntimeProducerCommitDigestInputV1 {
        schema_version: commit.schema_version,
        tenant_id: &record.tenant_id,
        program_id: &record.program_id,
        view_id: &record.view_id,
        logical_epoch: record.logical_epoch,
        producer_view_generation: commit.producer_view_generation,
        producer_plan_hash: &commit.producer_plan_hash,
        output_stream_id: &commit.output_stream_id,
        output_schema_hash: &commit.output_schema_hash,
        key_descriptor_hash: &commit.key_descriptor_hash,
        delta_codec_identity: &commit.delta_codec_identity,
        frontier_kind: &commit.frontier_kind,
        schema_fingerprint: &record.schema_fingerprint,
        delta_content_hash: &record.delta_content_hash,
        checkpoint_key: &commit.checkpoint_key,
        checkpoint_content_hash: &commit.checkpoint_content_hash,
        causal_cut_digest: &commit.causal_cut_digest,
    };
    let digest_bytes = serde_json::to_vec(&digest_input)
        .map_err(|source| ApiError::internal(source.to_string()))?;
    let actual_commit_digest = stable_bytes_hash(&digest_bytes);
    if commit.schema_version != 1
        || commit.producer_view_generation == 0
        || commit.producer_plan_hash.trim().is_empty()
        || !commit.output_stream_id.starts_with(&expected_stream_prefix)
        || commit.delta_codec_identity != "velorix-published-relation-delta-v1"
        || commit.frontier_kind != "producer_commit_epoch"
        || checkpoint_parts.tenant_id != record.tenant_id
        || checkpoint_parts.program_id != record.program_id
        || checkpoint_parts.view_id != record.view_id
        || checkpoint_parts.logical_epoch != record.logical_epoch
        || checkpoint_parts.content_hash != commit.checkpoint_content_hash
        || commit.producer_commit_digest != actual_commit_digest
        || !is_sha256_identity(&commit.output_schema_hash)
        || !is_sha256_identity(&commit.key_descriptor_hash)
        || !is_sha256_identity(&commit.causal_cut_digest)
        || !is_sha256_identity(&commit.producer_commit_digest)
    {
        return Err(ApiError::bad_request(format!(
            "standing runtime producer commit mismatch for `{}/{}/{}`",
            record.tenant_id, record.program_id, record.view_id
        )));
    }
    Ok(())
}

fn is_sha256_identity(value: &str) -> bool {
    value
        .strip_prefix("sha256:")
        .is_some_and(|hex| hex.len() == 64 && hex.bytes().all(|byte| byte.is_ascii_hexdigit()))
}

pub(super) fn validate_standing_runtime_state_payload_record(
    key: &ObjectKey,
    record: &StandingRuntimeStatePayloadRecord,
) -> Result<(), ApiError> {
    if record.schema_version != 1 || record.record_kind != "standing_runtime_state_payload_v1" {
        return Err(ApiError::bad_request(format!(
            "standing runtime state payload record identity mismatch for `{}/{}/{}`",
            record.tenant_id, record.program_id, record.view_id
        )));
    }
    if record.source_kind != "standing_runtime_checkpoint_state_payload"
        || record.payload.codec_identity != record.checkpoint_codec_identity
    {
        return Err(ApiError::bad_request(format!(
            "standing runtime state payload codec/source mismatch for `{}/{}/{}`",
            record.tenant_id, record.program_id, record.view_id
        )));
    }
    let (_, parts) = ObjectKey::parse_standing_runtime_state_payload(key.as_str().to_string())
        .map_err(ApiError::bad_request)?;
    let actual_state_hash = stable_bytes_hash(record.payload.payload.as_bytes());
    if parts.tenant_id != record.tenant_id
        || parts.program_id != record.program_id
        || parts.view_id != record.view_id
        || parts.logical_epoch != record.logical_epoch
        || parts.state_content_hash != record.state_content_hash
        || record.state_content_hash != actual_state_hash
    {
        return Err(ApiError::bad_request(format!(
            "standing runtime state payload key/body mismatch for `{}/{}/{}`",
            record.tenant_id, record.program_id, record.view_id
        )));
    }
    Ok(())
}

pub(super) fn validate_standing_runtime_output_page_ref(
    page: &StandingRuntimeOutputPageRef,
    view_id: &str,
) -> Result<(), ApiError> {
    if page.output_encoding != "velorix-delta-batch-json-v1" {
        return Err(ApiError::bad_request(format!(
            "standing runtime output page codec mismatch for view `{view_id}`"
        )));
    }
    ObjectKey::parse_standing_runtime_output_page(page.page_key.clone())
        .map_err(ApiError::bad_request)?;
    Ok(())
}

pub(super) fn validate_standing_runtime_output_page_record(
    key: &ObjectKey,
    record: &StandingRuntimeOutputPageRecord,
) -> Result<(), ApiError> {
    if record.schema_version != 1 || record.record_kind != "standing_runtime_output_page_v1" {
        return Err(ApiError::bad_request(format!(
            "standing runtime output page record identity mismatch for `{}/{}/{}`",
            record.tenant_id, record.program_id, record.view_id
        )));
    }
    if record.output_encoding != "velorix-delta-batch-json-v1"
        || record.source_kind != "standing_runtime_checkpoint_published_output"
    {
        return Err(ApiError::bad_request(format!(
            "standing runtime output page codec/source mismatch for `{}/{}/{}`",
            record.tenant_id, record.program_id, record.view_id
        )));
    }
    let (_, parts) = ObjectKey::parse_standing_runtime_output_page(key.as_str().to_string())
        .map_err(ApiError::bad_request)?;
    let page_bytes = serde_json::to_vec(&record.published_output)
        .map_err(|source| ApiError::internal(source.to_string()))?;
    let actual_page_hash = stable_bytes_hash(&page_bytes);
    let row_count = standing_runtime_published_output_row_count(&record.published_output)?;
    if parts.tenant_id != record.tenant_id
        || parts.program_id != record.program_id
        || parts.view_id != record.view_id
        || parts.logical_epoch != record.logical_epoch
        || parts.page_index != record.page_index
        || parts.page_content_hash != record.page_content_hash
        || record.page_content_hash != actual_page_hash
        || record.row_count != row_count
    {
        return Err(ApiError::bad_request(format!(
            "standing runtime output page key/body mismatch for `{}/{}/{}`",
            record.tenant_id, record.program_id, record.view_id
        )));
    }
    Ok(())
}

pub(super) fn validate_standing_runtime_checkpoint_replay_frontiers(
    record: &StandingRuntimeCheckpointRecord,
) -> Result<(), ApiError> {
    if record.checkpoint.state_payload.is_none() {
        return Ok(());
    }

    let legacy_checkpoint_input_frontier = record
        .checkpoint
        .input_frontiers
        .iter()
        .map(|frontier| frontier.committed_offset_exclusive)
        .max()
        .ok_or_else(|| {
            ApiError::bad_request(format!(
                "standing runtime checkpoint has no input frontier for view `{}`",
                record.view_id
            ))
        })?;
    let mut input_frontiers_by_relation = BTreeMap::new();
    for frontier in &record.checkpoint.input_frontiers {
        let key = (
            frontier.relation_id.as_str(),
            frontier.relation_version.as_str(),
            frontier.stream_id.as_str(),
            frontier.partition_id,
        );
        if input_frontiers_by_relation
            .insert(key, frontier.committed_offset_exclusive)
            .is_some()
        {
            return Err(ApiError::bad_request(format!(
                "duplicate standing runtime checkpoint input frontier for view `{}` relation={} version={} stream={} partition={}",
                record.view_id,
                frontier.relation_id,
                frontier.relation_version,
                frontier.stream_id,
                frontier.partition_id
            )));
        }
    }
    let mut seen = BTreeSet::new();
    for replay in &record.replay_checkpoints {
        if !seen.insert((
            replay.stream_id.as_str(),
            replay.partition_id,
            replay.relation_id.as_deref(),
            replay.relation_version.as_deref(),
        )) {
            return Err(ApiError::bad_request(format!(
                "duplicate standing runtime checkpoint replay frontier for view `{}` stream={} partition={}",
                record.view_id, replay.stream_id, replay.partition_id
            )));
        }
        match (
            replay.relation_id.as_deref(),
            replay.relation_version.as_deref(),
        ) {
            (Some(relation_id), Some(relation_version)) => {
                let Some(checkpoint_input_frontier) = input_frontiers_by_relation
                    .get(&(
                        relation_id,
                        relation_version,
                        replay.stream_id.as_str(),
                        replay.partition_id,
                    ))
                    .or_else(|| {
                        input_frontiers_by_relation.get(&(relation_id, relation_version, "", 0))
                    })
                else {
                    continue;
                };
                if replay.end_offset_exclusive > *checkpoint_input_frontier {
                    return Err(ApiError::bad_request(format!(
                        "standing runtime checkpoint replay frontier is ahead of checkpoint input frontier for view `{}` relation={} version={} stream={} partition={} replay_end={} checkpoint_end={}",
                        record.view_id,
                        relation_id,
                        relation_version,
                        replay.stream_id,
                        replay.partition_id,
                        replay.end_offset_exclusive,
                        checkpoint_input_frontier
                    )));
                }
            }
            (None, None) if record.checkpoint.input_frontiers.len() == 1 => {
                if replay.end_offset_exclusive > legacy_checkpoint_input_frontier {
                    return Err(ApiError::bad_request(format!(
                        "standing runtime checkpoint replay frontier is ahead of checkpoint input frontier for view `{}` stream={} partition={} replay_end={} checkpoint_end={}",
                        record.view_id,
                        replay.stream_id,
                        replay.partition_id,
                        replay.end_offset_exclusive,
                        legacy_checkpoint_input_frontier
                    )));
                }
            }
            (None, None) => {
                return Err(ApiError::bad_request(format!(
                    "standing runtime checkpoint replay frontier lacks relation metadata for multi-relation view `{}` stream={} partition={}",
                    record.view_id, replay.stream_id, replay.partition_id
                )));
            }
            _ => {
                return Err(ApiError::bad_request(format!(
                    "standing runtime checkpoint replay frontier has partial relation metadata for view `{}` stream={} partition={}",
                    record.view_id, replay.stream_id, replay.partition_id
                )));
            }
        }
    }

    Ok(())
}

pub(super) fn merged_standing_runtime_replay_checkpoints(
    previous_record: Option<&StandingRuntimeCheckpointRecord>,
    replay_checkpoints_to_merge: Vec<ReplayCheckpoint>,
) -> Vec<ReplayCheckpoint> {
    let mut replay_checkpoints = previous_record
        .map(|record| record.replay_checkpoints.clone())
        .unwrap_or_default();
    for replay_checkpoint in replay_checkpoints_to_merge {
        if let Some(existing) = replay_checkpoints.iter_mut().find(|existing| {
            existing.stream_id == replay_checkpoint.stream_id
                && existing.partition_id == replay_checkpoint.partition_id
                && existing.relation_id == replay_checkpoint.relation_id
                && existing.relation_version == replay_checkpoint.relation_version
        }) {
            existing.end_offset_exclusive = existing
                .end_offset_exclusive
                .max(replay_checkpoint.end_offset_exclusive);
        } else {
            replay_checkpoints.push(replay_checkpoint);
        }
    }
    replay_checkpoints.sort_by(|left, right| {
        left.stream_id
            .cmp(&right.stream_id)
            .then(left.partition_id.cmp(&right.partition_id))
            .then(left.relation_id.cmp(&right.relation_id))
            .then(left.relation_version.cmp(&right.relation_version))
    });

    replay_checkpoints
}

pub(super) fn replay_plan_covers_replayed_batch(
    replay_plan: &StandingRuntimeReplayPlan,
    relation_id: &str,
    relation_version: &str,
    stream_id: &str,
    partition_id: u32,
    batch_end_offset_exclusive: u64,
) -> bool {
    replay_plan.replay_checkpoints.iter().any(|checkpoint| {
        checkpoint.relation_id.as_deref() == Some(relation_id)
            && checkpoint.relation_version.as_deref() == Some(relation_version)
            && checkpoint.stream_id == stream_id
            && checkpoint.partition_id == partition_id
            && checkpoint.end_offset_exclusive >= batch_end_offset_exclusive
    }) || replay_plan.input_frontiers.iter().any(|frontier| {
        frontier.relation_id == relation_id
            && frontier.relation_version == relation_version
            && ((frontier.stream_id == stream_id && frontier.partition_id == partition_id)
                || frontier.stream_id.is_empty())
            && frontier.committed_offset_exclusive >= batch_end_offset_exclusive
    })
}

pub(super) fn prepared_batches_are_covered_by_replay_plan(
    replay_plan: &StandingRuntimeReplayPlan,
    prepared_batches: &[&PreparedIngestBatch],
) -> bool {
    prepared_batches
        .iter()
        .all(|prepared| prepared_batch_is_covered_by_replay_plan(replay_plan, prepared))
}

pub(super) fn prepared_batch_is_covered_by_replay_plan(
    replay_plan: &StandingRuntimeReplayPlan,
    prepared: &PreparedIngestBatch,
) -> bool {
    replay_plan_covers_replayed_batch(
        replay_plan,
        prepared.request.relation_id.as_str(),
        prepared.request.relation_version.as_str(),
        prepared.request.stream_id.as_str(),
        prepared.request.partition_id,
        prepared.end_offset_exclusive,
    )
}
