use super::*;

// Opt-in metadata-only diagnostics for bounded cursor resolution. Drop also
// reports elapsed time when the deadline cancels a phase awaiting blocking work.
pub(super) struct CursorPhaseTimer {
    started: std::time::Instant,
    label: Option<String>,
}

impl CursorPhaseTimer {
    pub(super) fn new(view: &str, epoch: u64, phase: &str) -> Self {
        let label = (std::env::var_os("VELORIX_CURSOR_DIAGNOSTICS").is_some())
            .then(|| format!("view={view} epoch={epoch} phase={phase}"));
        if let Some(label) = &label {
            eprintln!("cursor_diagnostic start {label}");
        }
        Self {
            started: std::time::Instant::now(),
            label,
        }
    }
}

impl Drop for CursorPhaseTimer {
    fn drop(&mut self) {
        if let Some(label) = &self.label {
            eprintln!(
                "cursor_diagnostic end {label} elapsed_ms={}",
                self.started.elapsed().as_millis()
            );
        }
    }
}

pub(super) async fn persist_standing_runtime_checkpoint(
    state: &ApiState,
    view_id: &str,
    checkpoint: &RuntimeCheckpoint,
    output_deltas: &[ViewOutputDelta],
    context: StandingRuntimeCheckpointPersistContext,
    mut timer: Option<&mut IngestTimer>,
) -> Result<StandingRuntimeCheckpointWriteSummary, ApiError> {
    let StandingRuntimeCheckpointPersistContext {
        demand_materialization,
        published_input_bootstrap,
        previous_record,
        replay_checkpoints_to_merge,
        owner,
        owner_renewal_failure,
        published_relation,
        direct_view_inputs,
        validated_cursor_proofs,
        input_coverage,
        replace_replay_coverage,
        expected_relation_source_cuts,
        validated_relation_publications,
        staged_event_time_state,
    } = context;
    let previous_record = match previous_record {
        Some(record) => Some(record),
        None => {
            read_latest_standing_runtime_checkpoint(state, &checkpoint.identity, view_id).await?
        }
    };
    let published_input_bootstrap = published_input_bootstrap.or_else(|| {
        previous_record
            .as_ref()
            .and_then(|record| record.published_input_bootstrap.clone())
    });
    if previous_record.as_ref().is_some_and(|previous| {
        checkpoint.logical_epoch <= previous.checkpoint.logical_epoch
            && demand_materialization != previous.demand_materialization
    }) {
        return Err(ApiError::conflict(
            "changed demand progress requires a new logical epoch",
        ));
    }
    if let Some(progress) = &published_input_bootstrap {
        progress.validate().map_err(meta_error_to_api)?;
    }
    let partial_bootstrap = demand_materialization
        .as_ref()
        .is_some_and(|demand| !demand.full_complete)
        || published_input_bootstrap
            .as_ref()
            .is_some_and(|progress| !progress.is_complete());
    if partial_bootstrap
        && (!direct_view_inputs.is_empty()
            || input_coverage.is_some()
            || !replay_checkpoints_to_merge.is_empty())
    {
        return Err(ApiError::bad_request(
            "partial published baseline cannot claim completed input coverage",
        ));
    }
    // Partial state is durable, but it is not an authoritative producer commit.
    let published_relation = if partial_bootstrap {
        None
    } else {
        published_relation
    };
    let checkpoint = match input_coverage {
        Some(input_coverage) => {
            let mut checkpoint = checkpoint.clone();
            checkpoint.input_coverage = Some(input_coverage);
            checkpoint
        }
        None if partial_bootstrap => {
            let mut checkpoint = checkpoint.clone();
            checkpoint.input_coverage = None;
            checkpoint.causal_cut = None;
            if checkpoint.input_frontiers.iter().any(|frontier| {
                let previous_offset = previous_record
                    .as_ref()
                    .and_then(|record| {
                        record.checkpoint.input_frontiers.iter().find(|previous| {
                            previous.relation_id == frontier.relation_id
                                && previous.relation_version == frontier.relation_version
                                && previous.stream_id == frontier.stream_id
                                && previous.partition_id == frontier.partition_id
                        })
                    })
                    .map_or(0, |previous| previous.committed_offset_exclusive);
                frontier.committed_offset_exclusive != previous_offset
            }) {
                return Err(ApiError::bad_request(
                    "partial baseline advanced a producer frontier",
                ));
            }
            checkpoint
        }
        None => {
            standing_runtime_checkpoint_with_authoritative_input_coverage(
                state,
                view_id,
                checkpoint,
                expected_relation_source_cuts.as_deref(),
                &validated_relation_publications,
                previous_record.as_ref(),
                demand_materialization.as_ref(),
            )
            .await?
        }
    };
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
    // A partial checkpoint is a state/CAS predecessor, not a producer commit.
    // Consolidate its unpublished changes before constructing the next commit.
    let completed_demand = demand_materialization
        .as_ref()
        .is_some_and(|progress| progress.full_complete)
        && published_relation.is_some();
    let demand_publication = if completed_demand
        && previous_record
            .as_ref()
            .is_none_or(|previous| previous.checkpoint.logical_epoch < checkpoint.logical_epoch)
    {
        Some(
            crate::demand_publication::consolidate_demand_publication_deltas(
                state,
                previous_record.as_ref(),
                checkpoint,
                view_id,
                published_relation.as_ref().expect("checked above"),
                output_deltas,
                crate::demand_publication::DemandPublicationLimits {
                    max_bytes: state.max_standing_runtime_expansion_bytes,
                    max_delta_records: state.max_standing_runtime_output_delta_records,
                },
            )
            .await?,
        )
    } else {
        None
    };
    let prior_coverage = match &demand_publication {
        Some(publication) => publication
            .last_published
            .as_ref()
            .and_then(|baseline| baseline.pointer.input_coverage.as_ref()),
        None => previous_record
            .as_ref()
            .and_then(|record| record.checkpoint.input_coverage.as_ref()),
    };
    if let Some(publication) = &demand_publication {
        if std::env::var_os("VELORIX_CURSOR_DIAGNOSTICS").is_some() {
            eprintln!(
                "demand_publication view={view_id} epoch={} baseline_epoch={:?} baseline_digest={:?} lineage={} fetched_bytes={}",
                checkpoint.logical_epoch,
                publication
                    .last_published
                    .as_ref()
                    .map(|baseline| baseline.pointer.logical_epoch),
                publication
                    .last_published
                    .as_ref()
                    .map(|baseline| baseline.commit_digest.as_str()),
                publication.visited_checkpoints,
                publication.fetched_bytes
            );
        }
    }
    let checkpoint = standing_runtime_checkpoint_with_prior_input_coverage(
        checkpoint,
        prior_coverage,
        published_relation.as_ref(),
    )?;
    let resolved_view_cursors = resolve_authoritative_direct_view_inputs(
        state,
        &checkpoint.identity.tenant_id,
        &direct_view_inputs,
        &validated_cursor_proofs,
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
    let delta_checkpoint = checkpoint.clone();
    let delta_view = view_id.to_string();
    let delta_output = if let Some(publication) = demand_publication {
        publication.output_deltas
    } else if completed_demand {
        // An identical completed checkpoint retry must reuse its authenticated
        // published delta, not consolidate that commit into itself.
        let previous = previous_record.as_ref().ok_or_else(|| {
            ApiError::bad_request("completed demand retry is missing its checkpoint")
        })?;
        if previous.checkpoint.logical_epoch != checkpoint.logical_epoch
            || previous.checkpoint.state_root.content_hash != checkpoint.state_root.content_hash
        {
            return Err(ApiError::conflict(
                "completed demand checkpoint retry changed state",
            ));
        }
        authenticated_demand_retry_output(
            state,
            previous,
            published_relation.as_ref().expect("checked above"),
        )
        .await?
    } else {
        output_deltas.to_vec()
    };
    let delta_key = checkpoint_key.clone();
    let delta_binding = published_relation.clone();
    let output_delta_publications = tokio::task::spawn_blocking(move || {
        standing_runtime_output_delta_records_for_checkpoint_with_binding(
            &delta_checkpoint,
            &delta_view,
            &delta_output,
            &delta_key,
            delta_binding.as_ref(),
        )
    })
    .await
    .map_err(ApiError::internal)??;
    let mut write_summary = StandingRuntimeCheckpointWriteSummary {
        output_delta_writes: output_delta_publications.len(),
        state_payload_writes: 1,
        checkpoint_record_writes: 1,
        checkpoint_pointer_writes: usize::from(state.meta_store.is_some()),
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
    let payload_checkpoint = checkpoint.clone();
    let payload_view = view_id.to_string();
    let (state_payload_key, state_payload_record) = tokio::task::spawn_blocking(move || {
        standing_runtime_state_payload_record_for_checkpoint(&payload_checkpoint, &payload_view)
    })
    .await
    .map_err(ApiError::internal)??;
    persist_standing_runtime_state_payload(state, &state_payload_key, &state_payload_record)
        .await?;
    write_summary.state_payload_writes = checkpoint_fragments::durable_state_object_count(
        state_payload_record.payload.payload.len(),
        state.max_standing_runtime_state_fragment_bytes,
    );
    if let Some(timer) = timer.as_mut() {
        timer.mark("checkpoint_state_payload");
    }
    // Demand reads and downstream bootstrap proofs require persisted output
    // even for legacy aggregate codecs.
    let native_output_publication = if native_output_pages::supports_checkpoint(checkpoint)
        || demand_materialization.is_some()
        || published_relation.is_some()
    {
        let snapshot_checkpoint = checkpoint.clone();
        let snapshot_view = view_id.to_string();
        let snapshot_key = checkpoint_key.clone();
        let limits = state.runtime_resource_limits();
        let allow_generic_native = state.meta_store.is_some();
        tokio::task::spawn_blocking(move || {
            standing_runtime_output_manifest_record_for_checkpoint_with_limits(
                &snapshot_checkpoint,
                &snapshot_view,
                &snapshot_key,
                limits,
                allow_generic_native,
            )
        })
        .await
        .map_err(ApiError::internal)??
    } else {
        None
    };
    if let Some(publication) = &native_output_publication {
        for (key, page) in &publication.page_records {
            persist_standing_runtime_output_page(state, key, page).await?;
        }
        persist_standing_runtime_output_manifest(
            state,
            &publication.manifest_key,
            &publication.manifest_record,
        )
        .await?;
    }
    let checkpoint_for_record = standing_runtime_checkpoint_with_durable_publication_refs(
        checkpoint,
        native_output_publication
            .as_ref()
            .map(|publication| &publication.manifest_key),
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
                    && record.demand_materialization == demand_materialization
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
    let replay_checkpoints = if replace_replay_coverage {
        replay_checkpoints_to_merge
    } else {
        merged_standing_runtime_replay_checkpoints(
            previous_record.as_ref(),
            replay_checkpoints_to_merge,
        )
    };
    if let Some(demand) = &demand_materialization {
        demand.validate_checkpoint(&checkpoint_for_record)?;
        let bytes = serde_json::to_vec(demand).map_err(ApiError::internal)?;
        if bytes.len() > state.max_standing_runtime_state_payload_bytes {
            return Err(ApiError::bad_request(
                "demand coverage exceeds checkpoint resource budget",
            ));
        }
    }
    let record = StandingRuntimeCheckpointRecord {
        demand_materialization,
        published_input_bootstrap,
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
    if let Some(failure) = owner_renewal_failure {
        if let Some(error) = failure
            .lock()
            .map_err(|_| ApiError::internal("owner renewal state poisoned"))?
            .as_ref()
        {
            return Err(ApiError::conflict(format!(
                "backfill owner renewal failed before publication: {error}"
            )));
        }
    }
    publish_standing_runtime_checkpoint_pointer(
        state,
        expected_previous,
        candidate.clone(),
        owner,
        expected_relation_source_cuts,
    )
    .await?;
    if let Some(runtime) = state.standing_runtime(&checkpoint.identity, view_id)? {
        let mut runtime = runtime
            .lock()
            .map_err(|_| ApiError::internal("standing runtime lock poisoned"))?;
        if runtime.logical_epoch() == checkpoint.logical_epoch {
            runtime
                .acknowledge_event_time_state(&staged_event_time_state)
                .map_err(ApiError::bad_request)?;
        }
    }
    if let Some(timer) = timer.as_mut() {
        timer.mark("checkpoint_pointer");
    }
    state.set_standing_runtime_committed_checkpoint(
        &checkpoint.identity,
        view_id,
        Some(candidate),
    )?;
    // Publication has succeeded. Residency is a best-effort count bound, and
    // must neither turn that commit into a failure nor evict its current runtime.
    let _current_runtime_pin = state
        .standing_runtime(&checkpoint.identity, view_id)
        .ok()
        .flatten();
    if let Err(error) = state.prune_standing_runtime_residents() {
        eprintln!("standing runtime residency pruning failed after publication: {error}");
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

async fn authenticated_demand_retry_output(
    state: &ApiState,
    previous: &StandingRuntimeCheckpointRecord,
    binding: &PublishedRelationBindingV1,
) -> Result<Vec<ViewOutputDelta>, ApiError> {
    let pointer = standing_runtime_checkpoint_pointer_from_record(previous);
    let header = read_standing_runtime_checkpoint_header_from_pointer(
        state,
        &pointer.tenant_id,
        &pointer.program_id,
        &pointer.view_id,
        &pointer,
    )
    .await?;
    let digest = producer_cursor_digest_from_header(&header, binding)?;
    let key = header
        .checkpoint
        .output_manifest_refs
        .iter()
        .find_map(|reference| reference.strip_prefix(STANDING_RUNTIME_OUTPUT_COMMIT_REF_PREFIX))
        .ok_or_else(|| ApiError::bad_request("completed demand retry has no producer commit"))?;
    let (key, _) = ObjectKey::parse_standing_runtime_output_delta(key.to_string())
        .map_err(ApiError::bad_request)?;
    let bytes = read_object_bytes_bounded(
        state,
        &ObjectPath::from(key.as_str()),
        Some(state.max_standing_runtime_expansion_bytes as u64),
    )
    .await?;
    let record: StandingRuntimeOutputDeltaRecord =
        serde_json::from_slice(&bytes).map_err(ApiError::bad_request)?;
    validate_standing_runtime_output_delta_record(&key, &record)?;
    if record
        .producer_commit
        .as_ref()
        .is_none_or(|commit| commit.producer_commit_digest != digest)
        || record.schema_fingerprint != binding.relation.schema_fingerprint
    {
        return Err(ApiError::bad_request(
            "completed demand retry commit identity mismatch",
        ));
    }
    let delta: DeltaBatch =
        serde_json::from_value(record.output_delta).map_err(ApiError::bad_request)?;
    if delta.records().len() > state.max_standing_runtime_output_delta_records {
        return Err(ApiError::bad_request(
            "completed demand retry delta exceeds record budget",
        ));
    }
    Ok(vec![ViewOutputDelta {
        view_id: pointer.view_id,
        schema_fingerprint: record.schema_fingerprint,
        delta,
    }])
}

fn standing_runtime_checkpoint_with_prior_input_coverage(
    checkpoint: &RuntimeCheckpoint,
    previous_coverage: Option<&RuntimeCheckpointInputCoverageV1>,
    published_relation: Option<&PublishedRelationBindingV1>,
) -> Result<RuntimeCheckpoint, ApiError> {
    if checkpoint.input_coverage.is_some() || published_relation.is_none() {
        return Ok(checkpoint.clone());
    }
    let previous_coverage = previous_coverage.ok_or_else(|| {
        ApiError::bad_request("published relation output commit requires checkpoint input coverage")
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

pub(super) async fn resolve_authoritative_direct_view_inputs(
    state: &ApiState,
    tenant_id: &str,
    direct_view_inputs: &[StandingRuntimeDirectViewInputV1],
    proofs: &[ValidatedCursorProof],
) -> Result<Vec<ResolvedCausalViewCursorV1>, ApiError> {
    let mut resolved = Vec::with_capacity(direct_view_inputs.len());
    for input in direct_view_inputs {
        if let Some(proof) = proofs.iter().find(|proof| {
            proof.binding == input.published_relation
                && proof.resolved.cursor == input.cursor
                && proof.head.tenant_id == tenant_id
        }) {
            if let Some(meta) = &state.meta_store {
                let current = tokio::time::timeout(
                    std::time::Duration::from_secs(10),
                    meta.read_standing_runtime_checkpoint(
                        tenant_id,
                        &input.cursor.producer_program_id,
                        &input.cursor.producer_view_id,
                    ),
                )
                .await
                .map_err(|_| {
                    ApiError::service_unavailable(
                        "authoritative view cursor resolution deadline exceeded",
                    )
                })?
                .map_err(meta_error_to_api)?;
                if current.as_ref() == Some(&proof.head) {
                    resolved.push(proof.resolved.clone());
                    continue;
                }
            }
            // Refresh at the original validation level; a full proof must not
            // silently become a header-only proof when the head advances.
            let (_, refreshed, _) = read_authoritative_cursor_checkpoint_inner(
                state,
                tenant_id,
                &input.published_relation,
                &input.cursor,
                false,
                proof.metadata_only,
            )
            .await?;
            resolved.push(refreshed.resolved);
            continue;
        }
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

/// Request-local authenticated cursor lineage proof. It does not certify
/// snapshot payload validation and is never installed in a cross-request cache.
pub(super) struct ValidatedCursorProof {
    binding: PublishedRelationBindingV1,
    head: StandingRuntimeCheckpointPointer,
    resolved: ResolvedCausalViewCursorV1,
    metadata_only: bool,
}

pub(super) async fn resolve_authoritative_view_cursor(
    state: &ApiState,
    tenant_id: &str,
    binding: &PublishedRelationBindingV1,
    cursor: &CausalViewCursorV1,
) -> Result<ResolvedCausalViewCursorV1, ApiError> {
    let _timer = CursorPhaseTimer::new(
        &cursor.producer_view_id,
        cursor.output_epoch,
        "resolve_total",
    );
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
    read_authoritative_cursor_checkpoint(state, tenant_id, binding, cursor)
        .await
        .map(|(_, proof)| proof.resolved)
}

pub(super) async fn read_authoritative_cursor_checkpoint(
    state: &ApiState,
    tenant_id: &str,
    binding: &PublishedRelationBindingV1,
    cursor: &CausalViewCursorV1,
) -> Result<(StandingRuntimeCheckpointRecord, ValidatedCursorProof), ApiError> {
    read_authoritative_cursor_checkpoint_with_snapshot_pages(
        state, tenant_id, binding, cursor, false,
    )
    .await
    .map(|(record, proof, _)| (record, proof))
}

pub(super) struct ValidatedSnapshotPages {
    pub(super) schema: RelationSchema,
    pub(super) pages: Vec<DeltaBatch>,
}

pub(super) async fn read_authoritative_cursor_checkpoint_with_snapshot_pages(
    state: &ApiState,
    tenant_id: &str,
    binding: &PublishedRelationBindingV1,
    cursor: &CausalViewCursorV1,
    retain_pages: bool,
) -> Result<
    (
        StandingRuntimeCheckpointRecord,
        ValidatedCursorProof,
        Option<ValidatedSnapshotPages>,
    ),
    ApiError,
> {
    Box::pin(read_authoritative_cursor_checkpoint_inner(
        state,
        tenant_id,
        binding,
        cursor,
        retain_pages,
        false,
    ))
    .await
}

pub(super) async fn read_authoritative_cursor_checkpoint_header(
    state: &ApiState,
    tenant_id: &str,
    binding: &PublishedRelationBindingV1,
    cursor: &CausalViewCursorV1,
) -> Result<(StandingRuntimeCheckpointRecord, ValidatedCursorProof), ApiError> {
    let (record, proof, _) =
        read_authoritative_cursor_checkpoint_inner(state, tenant_id, binding, cursor, false, true)
            .await?;
    Ok((record, proof))
}

async fn read_authoritative_cursor_checkpoint_inner(
    state: &ApiState,
    tenant_id: &str,
    binding: &PublishedRelationBindingV1,
    cursor: &CausalViewCursorV1,
    retain_pages: bool,
    metadata_only: bool,
) -> Result<
    (
        StandingRuntimeCheckpointRecord,
        ValidatedCursorProof,
        Option<ValidatedSnapshotPages>,
    ),
    ApiError,
> {
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
    let producer_view_id = &cursor.producer_view_id;
    let producer_program_id = &cursor.producer_program_id;
    let mut pointer = match &state.meta_store {
        Some(meta_store) => meta_store
            .read_standing_runtime_checkpoint(tenant_id, producer_program_id, producer_view_id)
            .await
            .map_err(meta_error_to_api)?
            .ok_or_else(|| {
                ApiError::bad_request(format!(
                    "view cursor edge `{}` has no authoritative producer checkpoint",
                    cursor.input_edge
                ))
            })?,
        None => {
            eprintln!(
                "WARNING: No meta_store configured, falling back to S3 LIST scan for view cursor resolution \
                 (tenant={}, program={}, view={})",
                tenant_id, producer_program_id, producer_view_id
            );
            let prefix = ObjectPath::from(format!(
                "v1/standing-runtime-checkpoints/{}/{}/{}/epochs",
                tenant_id, producer_program_id, producer_view_id
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
                                "multiple standing runtime checkpoints for `{}/{}/{}` epoch {}",
                                tenant_id,
                                producer_program_id,
                                producer_view_id,
                                parts.logical_epoch
                            )));
                        }
                        _ => (location, parts),
                    });
                }
            }
            let Some((latest_checkpoint_path, _)) = latest_checkpoint else {
                return Err(ApiError::bad_request(format!(
                    "view cursor edge `{}` has no producer checkpoint",
                    cursor.input_edge
                )));
            };
            let bytes = state
                .store
                .get(&ObjectPath::from(latest_checkpoint_path.clone()))
                .await
                .map_err(ApiError::internal)?
                .bytes()
                .await
                .map_err(ApiError::internal)?;
            let record = standing_runtime_checkpoint_record_from_slice(&bytes)
                .map_err(|source| ApiError::bad_request(source.to_string()))?;
            standing_runtime_checkpoint_pointer_from_record(&record)
        }
    };
    let authoritative_head_checkpoint_key = pointer.checkpoint_key.clone();
    let authoritative_head_manifest_hash = pointer.manifest_hash.clone();
    let validated_head = pointer.clone();

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
        let (record, validated_deltas, snapshot_pages) = if metadata_only {
            (
                read_standing_runtime_checkpoint_header_from_pointer(
                    state,
                    tenant_id,
                    producer_program_id,
                    producer_view_id,
                    &pointer,
                )
                .await?,
                Vec::new(),
                None,
            )
        } else {
            Box::pin(
                read_standing_runtime_checkpoint_record_and_deltas_from_pointer_for_scope(
                    state,
                    tenant_id,
                    producer_program_id,
                    producer_view_id,
                    &pointer,
                    retain_pages && pointer.logical_epoch == cursor.output_epoch,
                ),
            )
            .await?
        };
        if record.checkpoint.logical_epoch == cursor.output_epoch {
            if metadata_only {
                if record
                    .published_input_bootstrap
                    .as_ref()
                    .is_some_and(|progress| !progress.is_complete())
                    || producer_cursor_digest_from_header(&record, binding)? != cursor.commit_digest
                {
                    return Err(ApiError::bad_request(
                        "captured producer cursor header commitment mismatch",
                    ));
                }
            } else {
                validate_authoritative_view_cursor_commit_from_deltas(
                    binding,
                    cursor,
                    &record,
                    &validated_deltas,
                )?;
            }
            let resolved = ResolvedCausalViewCursorV1 {
                cursor: cursor.clone(),
                authoritative_head_checkpoint_key,
                authoritative_head_manifest_hash,
            };
            return Ok((
                record,
                ValidatedCursorProof {
                    binding: binding.clone(),
                    head: validated_head,
                    resolved,
                    metadata_only,
                },
                snapshot_pages,
            ));
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

pub(super) async fn validate_authoritative_view_cursor_commit(
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
    validate_authoritative_view_cursor_commit_from_deltas(binding, cursor, record, &[delta_record])
}

fn validate_authoritative_view_cursor_commit_from_deltas(
    binding: &PublishedRelationBindingV1,
    cursor: &CausalViewCursorV1,
    record: &StandingRuntimeCheckpointRecord,
    deltas: &[StandingRuntimeOutputDeltaRecord],
) -> Result<(), ApiError> {
    let commit_refs = record
        .checkpoint
        .output_manifest_refs
        .iter()
        .filter(|reference| reference.starts_with(STANDING_RUNTIME_OUTPUT_COMMIT_REF_PREFIX))
        .count();
    let mut commits = deltas
        .iter()
        .filter(|delta| delta.producer_commit.is_some());
    let delta_record = commits
        .next()
        .ok_or_else(|| ApiError::bad_request("authoritative producer commit is missing"))?;
    if commit_refs != 1 || commits.next().is_some() {
        return Err(ApiError::bad_request(
            "authoritative producer commit is ambiguous",
        ));
    }
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
    captured_source_cuts: Option<&[RelationIngestSourceIdentityCutV1]>,
    validated_publications: &[recovery::ValidatedRelationPublication],
    previous_record: Option<&StandingRuntimeCheckpointRecord>,
    demand_progress: Option<&demand_materialization::DemandCheckpoint>,
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
    // Published-only consumers have no physical source cut to refresh. Their
    // progress is carried by the direct producer cursors. Recapturing the
    // global catalog epoch here changes an otherwise identical retry's causal
    // digest when unrelated ingest advances, conflicting with immutable output.
    if control.bootstrap_cut.relations.is_empty() {
        let coverage = RuntimeCheckpointInputCoverageV1 {
            schema_version: RUNTIME_CHECKPOINT_INPUT_COVERAGE_SCHEMA_VERSION_V1,
            view_generation: control.bootstrap_generation,
            plan_hash: control.plan_hash,
            input_catalog_epoch: control.bootstrap_cut.input_catalog_epoch,
            relations: Vec::new(),
        }
        .canonicalized()
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
        let mut checkpoint = checkpoint.clone();
        checkpoint.input_coverage = Some(coverage);
        return Ok(checkpoint);
    }
    if state.authoritative_relation_ingest_enabled() {
        let Some(relation_ingest) = state.relation_ingest_config.as_ref() else {
            return Err(ApiError::service_unavailable(
                "authoritative relation ingest configuration is unavailable",
            ));
        };
        let mut relations = Vec::new();
        for relation in &control.bootstrap_cut.relations {
            let mut partitions = Vec::new();
            let frontiers = checkpoint.input_frontiers.iter().filter(|frontier| {
                frontier.relation_id == relation.relation.relation_id
                    && frontier.relation_version == relation.relation.relation_version
            });
            for frontier in frontiers {
                let captured = captured_source_cuts
                    .and_then(|cuts| cuts.iter().find(|cut| cut.relation == relation.relation));
                if captured_source_cuts.is_some() && captured.is_none() {
                    return Err(ApiError::bad_request(
                        "captured demand source cut is missing a bootstrapped relation",
                    ));
                }
                let cut = if let Some(captured) = captured {
                    captured.cut.clone()
                } else {
                    meta_store
                        .capture_relation_ingest_source_cut(CaptureRelationIngestSourceCutRequest {
                            authority: RelationPartitionAuthorityKey {
                                namespace: relation_ingest.namespace.clone(),
                                relation_id: relation.relation.relation_id.clone(),
                                stream_id: frontier.stream_id.clone(),
                                partition_id: frontier.partition_id,
                            },
                            relation_version: relation.relation.relation_version.clone(),
                            schema_fingerprint: relation.relation.schema_fingerprint.clone(),
                        })
                        .await
                        .map_err(meta_error_to_api)?
                };
                if cut.schema_version != RELATION_INGEST_SOURCE_CUT_SCHEMA_VERSION_V1
                    || cut.namespace != relation_ingest.namespace
                    || cut.relation_id != relation.relation.relation_id
                {
                    return Err(ApiError::service_unavailable(format!(
                        "authoritative relation source cut identity is invalid for `{}`",
                        relation.relation.relation_id
                    )));
                }
                let Some(partition) = cut.partitions.iter().find(|partition| {
                    partition.stream_id == frontier.stream_id
                        && partition.partition_id == frontier.partition_id
                }) else {
                    return Err(ApiError::service_unavailable(format!(
                        "authoritative relation source cut is missing runtime frontier {}/{}/p={} for view `{view_id}`",
                        relation.relation.relation_id, frontier.stream_id, frontier.partition_id
                    )));
                };
                if partition.committed_offset_exclusive < frontier.committed_offset_exclusive
                    || (frontier.committed_offset_exclusive > partition.base_offset_inclusive
                        && partition.publications.is_empty())
                {
                    return Err(ApiError::service_unavailable(format!(
                        "authoritative relation source cut does not cover runtime frontier {}/{}/p={} through offset {} for view `{view_id}`",
                        relation.relation.relation_id,
                        frontier.stream_id,
                        frontier.partition_id,
                        frontier.committed_offset_exclusive
                    )));
                }
                let already_covered = previous_record
                    .and_then(|record| record.checkpoint.input_coverage.as_ref())
                    .and_then(|coverage| {
                        coverage.relations.iter().find(|covered| {
                            covered.relation_id == relation.relation.relation_id
                                && covered.relation_version == relation.relation.relation_version
                        })
                    })
                    .and_then(|covered| {
                        covered.partitions.iter().find(|partition| {
                            partition.stream_id == frontier.stream_id
                                && partition.partition_id == frontier.partition_id
                        })
                    })
                    .map_or(partition.base_offset_inclusive, |covered| {
                        covered.processed_offset_exclusive
                    });
                for publication in &partition.publications {
                    if publication.end_offset_exclusive > frontier.committed_offset_exclusive
                        || publication.end_offset_exclusive <= already_covered
                    {
                        continue;
                    }
                    let proof_matches = validated_publications.iter().any(|proof| {
                        proof.matches(
                            &relation.relation,
                            &frontier.stream_id,
                            frontier.partition_id,
                            publication,
                        )
                    });
                    let has_persisted_proof =
                        |progress: &demand_materialization::DemandCheckpoint| {
                            let artifact = demand_source::DemandSourceArtifact {
                                relation: relation.relation.clone(),
                                stream_id: frontier.stream_id.clone(),
                                partition_id: frontier.partition_id,
                                publication: publication.clone(),
                            };
                            demand_source::demand_source_artifact_key(&artifact)
                                .ok()
                                .and_then(|key| progress.artifacts.get(&key))
                                .is_some_and(|entry| {
                                    entry.artifact_hash == publication.object_digest
                                        && entry.applied.is_complete()
                                        && entry.source_watermark.is_some()
                                })
                        };
                    let durable_demand_proof = demand_progress.is_some_and(has_persisted_proof)
                        || previous_record
                            .and_then(|record| record.demand_materialization.as_ref())
                            .is_some_and(has_persisted_proof);
                    if !proof_matches && !durable_demand_proof {
                        super::recovery::validate_relation_publication_ref(
                            state,
                            &relation.relation.relation_id,
                            &relation.relation.relation_version,
                            &relation.relation.schema_fingerprint,
                            &frontier.stream_id,
                            frontier.partition_id,
                            publication,
                        )
                        .await?;
                    }
                }
                partitions.push(RuntimeCheckpointPartitionCoverageV1 {
                    stream_id: partition.stream_id.clone(),
                    stream_generation: 1,
                    partition_id: partition.partition_id,
                    partition_generation: 1,
                    covered_from_offset_inclusive: partition.base_offset_inclusive,
                    processed_offset_exclusive: frontier.committed_offset_exclusive,
                });
            }
            relations.push(RuntimeCheckpointRelationCoverageV1 {
                relation_id: relation.relation.relation_id.clone(),
                relation_version: relation.relation.relation_version.clone(),
                relation_generation: relation.relation.relation_generation,
                schema_fingerprint: relation.relation.schema_fingerprint.clone(),
                partitions,
            });
        }
        let coverage = RuntimeCheckpointInputCoverageV1 {
            schema_version: RUNTIME_CHECKPOINT_INPUT_COVERAGE_SCHEMA_VERSION_V1,
            view_generation: control.bootstrap_generation,
            plan_hash: control.plan_hash,
            input_catalog_epoch: control.bootstrap_cut.input_catalog_epoch,
            relations,
        }
        .canonicalized()
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
        let mut checkpoint = checkpoint.clone();
        checkpoint.input_coverage = Some(coverage);
        return Ok(checkpoint);
    }
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
    if checkpoint_fragments::persist_fragments(state, state_payload_key, record).await? {
        return Ok(());
    }
    let record = record.clone();
    let bytes = tokio::task::spawn_blocking(move || serde_json::to_vec(&record))
        .await
        .map_err(ApiError::internal)?
        .map_err(ApiError::internal)?;
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

#[cfg(test)]
pub(super) fn standing_runtime_output_manifest_record_for_checkpoint(
    checkpoint: &RuntimeCheckpoint,
    view_id: &str,
    checkpoint_key: &ObjectKey,
) -> Result<Option<StandingRuntimeOutputPublication>, ApiError> {
    standing_runtime_output_manifest_record_for_checkpoint_with_limits(
        checkpoint,
        view_id,
        checkpoint_key,
        velorix_runtime::materialized_view_runtime::RuntimeResourceLimits::default(),
        false,
    )
}

pub(super) fn standing_runtime_output_manifest_record_for_checkpoint_with_limits(
    checkpoint: &RuntimeCheckpoint,
    view_id: &str,
    checkpoint_key: &ObjectKey,
    limits: velorix_runtime::materialized_view_runtime::RuntimeResourceLimits,
    allow_generic_native: bool,
) -> Result<Option<StandingRuntimeOutputPublication>, ApiError> {
    if let Some(output) =
        velorix_runtime::materialized_view_runtime::bag_checkpoint_published_output_with_limits(
            checkpoint, limits,
        )
        .map_err(|error| ApiError::internal(error.to_string()))?
    {
        // The existing native families can select their pages from the
        // checkpoint record; the generic families require the authoritative
        // pointer reader. Without it they keep the legacy JSON publication.
        let generic = native_output_pages::generic_checkpoint_family(checkpoint);
        if allow_generic_native || !generic {
            match native_output_pages::publication(checkpoint, view_id, checkpoint_key, output) {
                Ok(publication) => return Ok(Some(publication)),
                // A generic family that cannot become a canonical bag page keeps
                // the legacy JSON publication instead of failing the commit.
                Err(_) if generic => {}
                Err(error) => return Err(error),
            }
        }
    }
    let Some(published_output) =
        standing_runtime_checkpoint_published_output_with_limits(checkpoint, limits)
    else {
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
    // Both immutable bodies contain this exact snapshot; reuse its checked
    // cardinality and byte hash instead of decoding it again for each wrapper.
    validate_standing_runtime_output_page_record_with_summary(
        &output_page_key,
        &page_record,
        &record.output_content_hash,
        output_row_count,
    )?;
    validate_standing_runtime_output_manifest_record_with_summary(
        &output_manifest_key,
        &record,
        &record.output_content_hash,
        output_row_count,
    )?;
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
    let record = record.clone();
    let bytes = tokio::task::spawn_blocking(move || native_output_pages::encode_page(&record))
        .await
        .map_err(ApiError::internal)??;
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
    let bytes = native_output_pages::encode_page(record)?;
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
    let record = record.clone();
    let bytes = tokio::task::spawn_blocking(move || serde_json::to_vec(&record))
        .await
        .map_err(ApiError::internal)?
        .map_err(ApiError::internal)?;
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

/// Derive the cursor commitment from an authenticated checkpoint header and
/// its content-addressed delta reference. Payload validation is deferred, not
/// represented by this metadata-only result.
pub(super) fn producer_cursor_digest_from_header(
    record: &StandingRuntimeCheckpointRecord,
    binding: &PublishedRelationBindingV1,
) -> Result<String, ApiError> {
    validate_published_relation_binding_v1(binding).map_err(ApiError::bad_request)?;
    let checkpoint = &record.checkpoint;
    let coverage = checkpoint
        .input_coverage
        .as_ref()
        .ok_or_else(|| ApiError::bad_request("producer coverage missing"))?;
    if coverage.view_generation != binding.producer_view_generation
        || coverage.plan_hash != binding.producer_plan_hash
        || record.view_id != binding.producer_view_id
    {
        return Err(ApiError::bad_request("producer header binding mismatch"));
    }
    let refs = checkpoint
        .output_manifest_refs
        .iter()
        .filter_map(|reference| reference.strip_prefix(STANDING_RUNTIME_OUTPUT_COMMIT_REF_PREFIX))
        .collect::<Vec<_>>();
    if refs.len() != 1 {
        return Err(ApiError::bad_request(
            "producer requires exactly one commit reference",
        ));
    }
    let (_, parts) = ObjectKey::parse_standing_runtime_output_delta(refs[0].to_string())
        .map_err(ApiError::bad_request)?;
    if parts.tenant_id != checkpoint.identity.tenant_id
        || parts.program_id != checkpoint.identity.program_id
        || parts.view_id != record.view_id
        || parts.logical_epoch != checkpoint.logical_epoch
    {
        return Err(ApiError::bad_request(
            "producer commit reference scope mismatch",
        ));
    }
    let causal_cut_digest = checkpoint
        .causal_cut
        .as_ref()
        .ok_or_else(|| ApiError::bad_request("producer causal cut missing"))?
        .stable_digest()
        .map_err(ApiError::bad_request)?;
    let digest = StandingRuntimeProducerCommitDigestInputV1 {
        schema_version: 1,
        tenant_id: &checkpoint.identity.tenant_id,
        program_id: &checkpoint.identity.program_id,
        view_id: &record.view_id,
        logical_epoch: checkpoint.logical_epoch,
        producer_view_generation: binding.producer_view_generation,
        producer_plan_hash: &binding.producer_plan_hash,
        output_stream_id: &binding.output_stream_id,
        output_schema_hash: &binding.output_schema_hash,
        key_descriptor_hash: &binding.key_descriptor_hash,
        delta_codec_identity: &binding.delta_codec_identity,
        frontier_kind: &binding.frontier_kind,
        schema_fingerprint: &binding.relation.schema_fingerprint,
        delta_content_hash: &parts.delta_content_hash,
        checkpoint_key: &record.checkpoint_key,
        checkpoint_content_hash: &checkpoint.state_root.content_hash,
        causal_cut_digest: &causal_cut_digest,
    };
    Ok(stable_bytes_hash(
        &serde_json::to_vec(&digest).map_err(ApiError::internal)?,
    ))
}

pub(super) async fn persist_standing_runtime_output_delta(
    state: &ApiState,
    output_delta_key: &ObjectKey,
    record: &StandingRuntimeOutputDeltaRecord,
) -> Result<(), ApiError> {
    let record = record.clone();
    let bytes = tokio::task::spawn_blocking(move || serde_json::to_vec(&record))
        .await
        .map_err(ApiError::internal)?
        .map_err(ApiError::internal)?;
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

#[cfg(test)]
pub(super) fn standing_runtime_checkpoint_published_batch_with_limits(
    checkpoint: &RuntimeCheckpoint,
    limits: velorix_runtime::materialized_view_runtime::RuntimeResourceLimits,
) -> Result<Option<DeltaBatch>, ApiError> {
    if let Some(batch) =
        velorix_runtime::materialized_view_runtime::bag_checkpoint_published_output_with_limits(
            checkpoint, limits,
        )
        .map_err(ApiError::bad_request)?
    {
        return Ok(Some(batch));
    }
    #[derive(Deserialize)]
    struct Snapshot {
        published_output: Option<DeltaBatch>,
    }
    checkpoint
        .state_payload
        .as_ref()
        .map(|payload| {
            serde_json::from_str::<Snapshot>(&payload.payload)
                .map(|snapshot| snapshot.published_output)
                .map_err(ApiError::bad_request)
        })
        .transpose()
        .map(Option::flatten)
}

pub(super) fn standing_runtime_checkpoint_published_output_with_limits(
    checkpoint: &RuntimeCheckpoint,
    limits: velorix_runtime::materialized_view_runtime::RuntimeResourceLimits,
) -> Option<Value> {
    match velorix_runtime::materialized_view_runtime::bag_checkpoint_published_output_with_limits(
        checkpoint, limits,
    ) {
        Ok(Some(output)) => return serde_json::to_value(output).ok(),
        Err(_) => return None,
        Ok(None) => {}
    }
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
    let records = published_output
        .get("records")
        .and_then(Value::as_array)
        .ok_or_else(|| ApiError::bad_request("invalid published DeltaBatch records"))?;
    let count = records.iter().try_fold(0usize, |count, row| {
        if !row.is_object() || row.get("key").is_none() || row.get("value").is_none() {
            return Err(ApiError::bad_request("invalid published DeltaRecord"));
        }
        let weight = row.get("weight").and_then(Value::as_i64)
            .ok_or_else(|| ApiError::bad_request("invalid published DeltaRecord weight"))?;
        let copies = usize::try_from(weight)
            .ok()
            .filter(|copies| *copies > 0)
            .ok_or_else(|| {
                ApiError::bad_request(
                    "standing runtime published output contains non-positive materialized multiplicity",
                )
            })?;
        count.checked_add(copies).ok_or_else(|| {
            ApiError::bad_request("standing runtime published output row count overflow")
        })
    })?;
    // Positive records cannot overflow an individual consolidated i64 weight
    // if their total fits i64. Retain the original net-entry overflow check for
    // larger bags, without sorting/copying ordinary snapshots merely to count.
    if count as u128 > i64::MAX as u128 {
        let output: DeltaBatch = serde_json::from_value(published_output.clone())
            .map_err(|source| ApiError::bad_request(source.to_string()))?;
        output
            .net_rows()
            .map_err(|source| ApiError::bad_request(source.to_string()))?;
    }
    Ok(count)
}

pub(super) async fn publish_standing_runtime_checkpoint_pointer(
    state: &ApiState,
    expected_previous: Option<StandingRuntimeCheckpointPointer>,
    candidate: StandingRuntimeCheckpointPointer,
    owner: Option<StandingRuntimeOwnerToken>,
    expected_relation_source_cuts: Option<Vec<RelationIngestSourceIdentityCutV1>>,
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
            expected_relation_source_cuts: expected_relation_source_cuts.clone(),
        })
        .await
        .map_err(meta_error_to_api)?
    {
        PublishStandingRuntimeCheckpointOutcome::Published
        | PublishStandingRuntimeCheckpointOutcome::Duplicate => Ok(()),
        PublishStandingRuntimeCheckpointOutcome::SourceCutChanged => Err(ApiError::conflict(
            "authoritative relation source cut changed before checkpoint publication; previous checkpoint pointer was left unchanged",
        )),
        PublishStandingRuntimeCheckpointOutcome::Conflict => {
            if expected_relation_source_cuts.is_some() {
                return Err(standing_runtime_checkpoint_publish_conflict(&candidate));
            }
            let Some(previous) = retry_expected_previous else {
                return Err(standing_runtime_checkpoint_publish_conflict(&candidate));
            };
            rehydrate_empty_meta_checkpoint_pointer_and_retry_publish(
                state,
                &previous,
                candidate,
                owner,
                expected_relation_source_cuts,
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
    expected_relation_source_cuts: Option<Vec<RelationIngestSourceIdentityCutV1>>,
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
    match current {
        Some(ref current_pointer) if current_pointer == previous => {
            // Previous was already published by a prior interrupted rehydration.
            // Skip publishing previous and proceed directly to publishing candidate.
        }
        Some(_) => {
            return Err(standing_runtime_checkpoint_publish_conflict(&candidate));
        }
        None => {
            validate_checkpoint_pointer_object_exists_for_meta_rehydration(state, previous).await?;
            match meta_store
                .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
                    expected_previous: None,
                    candidate: previous.clone(),
                    owner: owner.clone(),
                    expected_relation_source_cuts: None,
                })
                .await
                .map_err(meta_error_to_api)?
            {
                PublishStandingRuntimeCheckpointOutcome::Published
                | PublishStandingRuntimeCheckpointOutcome::Duplicate => {}
                PublishStandingRuntimeCheckpointOutcome::SourceCutChanged => {
                    return Err(ApiError::conflict(
                        "authoritative relation source cut changed while rehydrating the previous checkpoint pointer",
                    ));
                }
                PublishStandingRuntimeCheckpointOutcome::Conflict => {
                    return Err(standing_runtime_checkpoint_publish_conflict(&candidate));
                }
            }
        }
    }

    match meta_store
        .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
            expected_previous: Some(previous.clone()),
            candidate: candidate.clone(),
            owner,
            expected_relation_source_cuts,
        })
        .await
        .map_err(meta_error_to_api)?
    {
        PublishStandingRuntimeCheckpointOutcome::Published
        | PublishStandingRuntimeCheckpointOutcome::Duplicate => Ok(()),
        PublishStandingRuntimeCheckpointOutcome::SourceCutChanged => Err(ApiError::conflict(
            "authoritative relation source cut changed before checkpoint publication; previous checkpoint pointer was left unchanged",
        )),
        PublishStandingRuntimeCheckpointOutcome::Conflict => {
            Err(standing_runtime_checkpoint_publish_conflict(&candidate))
        }
    }
}

pub(super) async fn validate_checkpoint_pointer_object_exists_for_meta_rehydration(
    state: &ApiState,
    pointer: &StandingRuntimeCheckpointPointer,
) -> Result<(), ApiError> {
    // shortcut: rehydration must prove the durable object still exists, not a cache copy; revisit only with an authoritative durability proof.
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
        standing_runtime_checkpoint_coverage_commitment(record);
    StandingRuntimeCheckpointPointer {
        published_input_bootstrap: record.published_input_bootstrap.clone(),
        tenant_id: record.checkpoint.identity.tenant_id.clone(),
        program_id: record.checkpoint.identity.program_id.clone(),
        view_id: record.view_id.clone(),
        checkpoint_key: record.checkpoint_key.clone(),
        logical_epoch: record.checkpoint.logical_epoch,
        content_hash: record.checkpoint.state_root.content_hash.clone(),
        manifest_hash,
        output_manifest_refs: record.checkpoint.output_manifest_refs.clone(),
        bootstrap_generation: record
            .published_input_bootstrap
            .as_ref()
            .map_or(bootstrap_generation, |progress| {
                progress.bootstrap_generation
            }),
        plan_hash: record
            .published_input_bootstrap
            .as_ref()
            .map_or(plan_hash, |progress| progress.plan_hash.clone()),
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
    record: &StandingRuntimeCheckpointRecord,
) -> (
    u64,
    String,
    String,
    Option<RuntimeCheckpointInputCoverageV1>,
) {
    let Some(coverage) = record.checkpoint.input_coverage.clone() else {
        if let Some((demand, previous)) = record
            .demand_materialization
            .as_ref()
            .zip(record.previous_checkpoint.as_ref())
            .filter(|(_, previous)| previous.bootstrap_generation != 0)
        {
            // Incomplete new work does not erase the immutable lineage identity.
            return (
                previous.bootstrap_generation,
                demand.plan_hash.clone(),
                String::new(),
                None,
            );
        }
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
    read_latest_standing_runtime_checkpoint_inner(state, identity, view_id, true).await
}

pub(super) async fn read_latest_standing_runtime_checkpoint_header(
    state: &ApiState,
    identity: &StandingProgramIdentity,
    view_id: &str,
) -> Result<Option<StandingRuntimeCheckpointRecord>, ApiError> {
    read_latest_standing_runtime_checkpoint_inner(state, identity, view_id, false).await
}

async fn read_latest_standing_runtime_checkpoint_inner(
    state: &ApiState,
    identity: &StandingProgramIdentity,
    view_id: &str,
    hydrate: bool,
) -> Result<Option<StandingRuntimeCheckpointRecord>, ApiError> {
    if let Some(meta_store) = &state.meta_store {
        if let Some(pointer) = meta_store
            .read_standing_runtime_checkpoint(&identity.tenant_id, &identity.program_id, view_id)
            .await
            .map_err(meta_error_to_api)?
        {
            if !hydrate {
                let record = read_standing_runtime_checkpoint_header_from_pointer(
                    state,
                    &identity.tenant_id,
                    &identity.program_id,
                    view_id,
                    &pointer,
                )
                .await?;
                if record.checkpoint.identity != *identity {
                    return Err(ApiError::bad_request("checkpoint header identity mismatch"));
                }
                return Ok(Some(record));
            }
            return read_standing_runtime_checkpoint_record_from_pointer(
                state, identity, view_id, &pointer,
            )
            .await
            .map(Some);
        }
        return Ok(None);
    }
    eprintln!(
        "WARNING: No meta_store configured, falling back to S3 LIST scan for checkpoint read \
         (tenant={}, program={}, view={})",
        identity.tenant_id, identity.program_id, view_id
    );
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
        standing_runtime_checkpoint_coverage_commitment(&record);
    let pointer = StandingRuntimeCheckpointPointer {
        published_input_bootstrap: record.published_input_bootstrap.clone(),
        tenant_id: checkpoint_key_parts.tenant_id,
        program_id: checkpoint_key_parts.program_id,
        view_id: checkpoint_key_parts.view_id,
        checkpoint_key: latest_checkpoint_path,
        logical_epoch: checkpoint_key_parts.logical_epoch,
        content_hash: checkpoint_key_parts.content_hash,
        manifest_hash: stable_bytes_hash(&bytes),
        output_manifest_refs: record.checkpoint.output_manifest_refs.clone(),
        bootstrap_generation: record
            .published_input_bootstrap
            .as_ref()
            .map_or(bootstrap_generation, |progress| {
                progress.bootstrap_generation
            }),
        plan_hash: record
            .published_input_bootstrap
            .as_ref()
            .map_or(plan_hash, |progress| progress.plan_hash.clone()),
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
    validate_standing_runtime_checkpoint_record(identity, view_id, &pointer, &record)?;
    if !hydrate {
        validate_standing_runtime_checkpoint_replay_frontiers(&record)?;
        return Ok(Some(record));
    }
    Box::pin(hydrate_standing_runtime_checkpoint_state_payload(
        state,
        &mut record,
    ))
    .await?;
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

pub(super) async fn read_standing_runtime_checkpoint_record_from_pointer_for_scope(
    state: &ApiState,
    tenant_id: &str,
    program_id: &str,
    view_id: &str,
    pointer: &StandingRuntimeCheckpointPointer,
) -> Result<StandingRuntimeCheckpointRecord, ApiError> {
    // Cache-miss restoration can be nested below ingest and dependency replay.
    // Keep the validation/hydration future off that caller's poll stack.
    Box::pin(
        read_standing_runtime_checkpoint_record_and_deltas_from_pointer_for_scope(
            state, tenant_id, program_id, view_id, pointer, false,
        ),
    )
    .await
    .map(|(record, _, _)| record)
}

async fn read_standing_runtime_checkpoint_record_and_deltas_from_pointer_for_scope(
    state: &ApiState,
    tenant_id: &str,
    program_id: &str,
    view_id: &str,
    pointer: &StandingRuntimeCheckpointPointer,
    retain_pages: bool,
) -> Result<
    (
        StandingRuntimeCheckpointRecord,
        Vec<StandingRuntimeOutputDeltaRecord>,
        Option<ValidatedSnapshotPages>,
    ),
    ApiError,
> {
    let mut record = read_standing_runtime_checkpoint_header_from_pointer(
        state, tenant_id, program_id, view_id, pointer,
    )
    .await?;
    let identity = record.checkpoint.identity.clone();
    let hydrate_timer = CursorPhaseTimer::new(view_id, pointer.logical_epoch, "state_hydration");
    Box::pin(hydrate_standing_runtime_checkpoint_state_payload(
        state,
        &mut record,
    ))
    .await?;
    drop(hydrate_timer);
    let identity_timer =
        CursorPhaseTimer::new(view_id, pointer.logical_epoch, "hydrated_identity_hash");
    record
        .checkpoint
        .validate_identity(&identity)
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    drop(identity_timer);
    let _outputs_timer =
        CursorPhaseTimer::new(view_id, pointer.logical_epoch, "all_output_validation");
    let (deltas, snapshot_pages) = Box::pin(
        validate_standing_runtime_checkpoint_output_manifest_records_with_deltas(
            state,
            &record,
            retain_pages,
        ),
    )
    .await?;
    validate_standing_runtime_checkpoint_replay_frontiers(&record)?;
    Ok((record, deltas, snapshot_pages))
}

/// Validates the immutable record commitment only. This does not authorize
/// applying snapshot rows or treating their payloads as validated.
pub(super) async fn read_standing_runtime_checkpoint_header_from_pointer(
    state: &ApiState,
    tenant_id: &str,
    program_id: &str,
    view_id: &str,
    pointer: &StandingRuntimeCheckpointPointer,
) -> Result<StandingRuntimeCheckpointRecord, ApiError> {
    let record_timer = CursorPhaseTimer::new(
        view_id,
        pointer.logical_epoch,
        "record_get_decode_pointer_validation",
    );
    if pointer.tenant_id != tenant_id
        || pointer.program_id != program_id
        || pointer.view_id != view_id
    {
        return Err(ApiError::bad_request(format!(
            "standing runtime checkpoint pointer scope mismatch for `{tenant_id}/{program_id}/{view_id}`"
        )));
    }
    const MAX_CHECKPOINT_RECORD_BYTES: usize = 16 * 1024 * 1024;
    let bytes = read_object_bytes_bounded(
        state,
        &ObjectPath::from(pointer.checkpoint_key.clone()),
        Some(MAX_CHECKPOINT_RECORD_BYTES as u64),
    )
    .await?;
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
    drop(record_timer);
    validate_standing_runtime_checkpoint_replay_frontiers(&record)?;
    Ok(record)
}

pub(super) fn validate_standing_runtime_checkpoint_record(
    identity: &StandingProgramIdentity,
    view_id: &str,
    pointer: &StandingRuntimeCheckpointPointer,
    record: &StandingRuntimeCheckpointRecord,
) -> Result<(), ApiError> {
    if let Some((demand, previous)) = record
        .demand_materialization
        .as_ref()
        .zip(record.previous_checkpoint.as_ref())
    {
        if previous.bootstrap_generation != 0 && demand.plan_hash != previous.plan_hash {
            return Err(ApiError::bad_request(
                "demand checkpoint plan differs from its predecessor lineage",
            ));
        }
    }
    let (bootstrap_generation, plan_hash, coverage_hash, input_coverage) =
        standing_runtime_checkpoint_coverage_commitment(record);
    let (bootstrap_generation, plan_hash) =
        if let Some(progress) = &record.published_input_bootstrap {
            progress.validate().map_err(meta_error_to_api)?;
            if !progress.is_complete()
                && (record.checkpoint.input_coverage.is_some()
                    || record.checkpoint.causal_cut.is_some()
                    || record
                        .checkpoint
                        .input_frontiers
                        .iter()
                        .any(|frontier| frontier.committed_offset_exclusive != 0)
                    || !record.replay_checkpoints.is_empty())
            {
                return Err(ApiError::bad_request(
                    "partial published baseline claims completed input coverage",
                ));
            }
            (progress.bootstrap_generation, progress.plan_hash.clone())
        } else {
            (bootstrap_generation, plan_hash)
        };
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
        || pointer.published_input_bootstrap != record.published_input_bootstrap
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
    let bytes =
        read_object_bytes_bounded(state, &ObjectPath::from(state_payload_key.as_str()), None)
            .await?;
    let state_payload_record =
        match checkpoint_fragments::hydrate_fragments(state, &state_payload_key, &bytes).await? {
            // Native hydration already validated the assembled immutable payload.
            Some(record) => record,
            None => {
                let validation_key = state_payload_key.clone();
                tokio::task::spawn_blocking(move || {
                    let record: StandingRuntimeStatePayloadRecord =
                        serde_json::from_slice(&bytes).map_err(ApiError::bad_request)?;
                    validate_standing_runtime_state_payload_record(&validation_key, &record)?;
                    Ok::<_, ApiError>(record)
                })
                .await
                .map_err(ApiError::internal)??
            }
        };
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
    validate_standing_runtime_checkpoint_output_manifest_records_with_deltas(state, record, false)
        .await
        .map(|_| ())
}

async fn validate_standing_runtime_checkpoint_output_manifest_records_with_deltas(
    state: &ApiState,
    record: &StandingRuntimeCheckpointRecord,
    retain_pages: bool,
) -> Result<
    (
        Vec<StandingRuntimeOutputDeltaRecord>,
        Option<ValidatedSnapshotPages>,
    ),
    ApiError,
> {
    let mut validated_deltas = Vec::new();
    let mut snapshot_pages = None;
    for output_ref in &record.checkpoint.output_manifest_refs {
        let is_commit_ref = output_ref
            .strip_prefix(STANDING_RUNTIME_OUTPUT_COMMIT_REF_PREFIX)
            .is_some();
        if is_commit_ref
            || output_ref
                .strip_prefix(STANDING_RUNTIME_OUTPUT_DELTA_REF_PREFIX)
                .is_some()
        {
            let _delta_timer = CursorPhaseTimer::new(
                &record.view_id,
                record.checkpoint.logical_epoch,
                "commit_get_decode_validate",
            );
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
            validated_deltas.push(delta);
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
        let _pages_timer = CursorPhaseTimer::new(
            &record.view_id,
            record.checkpoint.logical_epoch,
            &format!("pages_validate_count_{}", manifest.pages.len()),
        );
        let native_pages = native_output_pages::is_native_codec(&manifest.output_encoding);
        let mut previous_native_pair = None;
        let mut previous_native_weight = 0i64;
        if native_pages {
            native_output_pages::validate_checkpoint_binding(&manifest, &record.checkpoint)?;
            if retain_pages {
                if snapshot_pages.is_some() {
                    return Err(ApiError::bad_request(
                        "ambiguous producer snapshot manifests",
                    ));
                }
                snapshot_pages = Some(ValidatedSnapshotPages {
                    schema: serde_json::from_value(manifest.published_output["schema"].clone())
                        .map_err(ApiError::bad_request)?,
                    pages: Vec::with_capacity(manifest.pages.len()),
                });
            }
        }
        for page in &manifest.pages {
            let (_page_key, page_record) =
                read_standing_runtime_output_page_record(state, page, &record.view_id).await?;
            if native_pages {
                let rows = native_output_pages::validate_page_binding(&manifest, &page_record)?;
                for row in rows.records() {
                    let pair = velorix_core::delta::encode_kv_ordered(
                        row.key.as_json(),
                        row.value.as_json(),
                    );
                    if previous_native_pair
                        .as_ref()
                        .is_some_and(|previous| previous > &pair)
                    {
                        return Err(ApiError::bad_request(
                            "native output pages are not globally ordered",
                        ));
                    }
                    previous_native_weight = if previous_native_pair.as_ref() == Some(&pair) {
                        previous_native_weight
                            .checked_add(row.weight)
                            .ok_or_else(|| {
                                ApiError::bad_request("producer snapshot multiplicity overflow")
                            })?
                    } else {
                        row.weight
                    };
                    previous_native_pair = Some(pair);
                }
                if let Some(snapshot) = snapshot_pages.as_mut() {
                    snapshot.pages.push(rows);
                }
            }
            if page_record.tenant_id != record.checkpoint.identity.tenant_id
                || page_record.program_id != record.checkpoint.identity.program_id
                || page_record.view_id != record.view_id
                || page_record.logical_epoch != record.checkpoint.logical_epoch
                || (page_record.output_content_hash != manifest.output_content_hash
                    && !(page_record.output_encoding == native_output_pages::CODEC
                        && page_record.output_content_hash.is_empty()))
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
    Ok((validated_deltas, snapshot_pages))
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
    let output_manifest_key = output_manifest_key.to_string();
    tokio::task::spawn_blocking(move || {
        let manifest: StandingRuntimeOutputManifestRecord = serde_json::from_slice(&bytes)
            .map_err(|source| ApiError::bad_request(source.to_string()))?;
        let (key, _) =
            ObjectKey::parse_standing_runtime_output_manifest(output_manifest_key.to_string())
                .map_err(ApiError::bad_request)?;
        validate_standing_runtime_output_manifest_record(&key, &manifest)?;
        Ok(Some((key, manifest)))
    })
    .await
    .map_err(ApiError::internal)?
}

pub(super) async fn read_standing_runtime_output_page_record(
    state: &ApiState,
    page: &StandingRuntimeOutputPageRef,
    view_id: &str,
) -> Result<(ObjectKey, StandingRuntimeOutputPageRecord), ApiError> {
    let (key, record, _) =
        read_standing_runtime_output_page_record_bounded(state, page, view_id, None).await?;
    Ok((key, record))
}

pub(super) async fn read_standing_runtime_output_page_record_bounded(
    state: &ApiState,
    page: &StandingRuntimeOutputPageRef,
    view_id: &str,
    max_bytes: Option<u64>,
) -> Result<(ObjectKey, StandingRuntimeOutputPageRecord, u64), ApiError> {
    let bytes =
        read_object_bytes_bounded(state, &ObjectPath::from(page.page_key.clone()), max_bytes)
            .await?;
    let byte_count = bytes.len() as u64;
    let page = page.clone();
    let view_id = view_id.to_string();
    tokio::task::spawn_blocking(move || {
        let record = native_output_pages::decode_page(&bytes, &page.output_encoding)?;
        let (key, _) = ObjectKey::parse_standing_runtime_output_page(page.page_key.clone())
            .map_err(ApiError::bad_request)?;
        validate_standing_runtime_output_page_ref(&page, &view_id)?;
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
        Ok((key, record, byte_count))
    })
    .await
    .map_err(ApiError::internal)?
}

pub(super) async fn read_object_bytes_bounded(
    state: &ApiState,
    path: &ObjectPath,
    max_bytes: Option<u64>,
) -> Result<Vec<u8>, ApiError> {
    if let Some(cache) = &state.native_read_cache {
        if let Ok(key) = checkpoint_read_cache_key(state, path) {
            let content_hash = key.content_hash.clone();
            let object_path = path.to_string();
            let bytes = cache
                .read_through(
                    key,
                    || async { read_object_bytes_bounded_uncached(state, path, max_bytes).await },
                    |bytes| {
                        if checkpoint_cache_bytes_match(state, &object_path, &content_hash, bytes) {
                            Ok(())
                        } else {
                            Err(ApiError::bad_request(format!(
                                "checkpoint cache content hash mismatch for `{object_path}`"
                            )))
                        }
                    },
                )
                .await?;
            if max_bytes.is_some_and(|limit| bytes.len() as u64 > limit) {
                return Err(ApiError::bad_request(
                    "native read exceeds query scan or memory byte budget",
                ));
            }
            return Ok(bytes);
        }
    }
    read_object_bytes_bounded_uncached(state, path, max_bytes).await
}

fn checkpoint_read_cache_key(
    state: &ApiState,
    path: &ObjectPath,
) -> Result<native_read_cache::NativeReadCacheKey, ApiError> {
    // shortcut: output manifests/deltas/pages need codec-specific commitments, not checkpoint hashes; revisit with their own validated cache keys.
    // shortcut: LIST discovery and its initial GET lack a fresh pointer commitment; keep them live until authoritative metadata replaces discovery.
    let raw = path.as_ref();
    let raw_name = raw.rsplit('/').next().unwrap_or(raw);
    let fragment = raw_name.split_once(".fragment-");
    let fragment_hash =
        fragment.and_then(|(_, suffix)| suffix.rsplit_once('-').map(|(_, hash)| hash.to_string()));
    let (output_id, content_hash, checkpoint_epoch) =
        if let Ok((_, p)) = ObjectKey::parse_standing_runtime_checkpoint(raw.to_string()) {
            (p.view_id, p.content_hash, p.logical_epoch)
        } else {
            let root = fragment.map_or(raw, |(base, _)| {
                &raw[..raw.len() - raw_name.len() + base.len()]
            });
            let (_, p) = ObjectKey::parse_standing_runtime_state_payload(root.to_string())
                .map_err(ApiError::bad_request)?;
            (
                p.view_id,
                fragment_hash.unwrap_or(p.state_content_hash),
                p.logical_epoch,
            )
        };
    Ok(native_read_cache::NativeReadCacheKey {
        store_id: state.native_read_cache_store_id().to_string(),
        namespace: state.native_read_cache_namespace().to_string(),
        tenant_id: String::new(),
        program_id: String::new(),
        output_id,
        object_path: raw.to_string(),
        content_hash,
        codec: "checkpoint-json".into(),
        checkpoint_epoch,
        root_hash: String::new(),
    })
}

fn checkpoint_cache_bytes_match(
    state: &ApiState,
    path: &str,
    content_hash: &str,
    bytes: &[u8],
) -> bool {
    if path
        .rsplit('/')
        .next()
        .is_some_and(|name| name.contains(".fragment-"))
    {
        return stable_bytes_hash(bytes) == content_hash;
    }
    if path.ends_with(".state-payload.json") {
        if serde_json::from_slice::<StandingRuntimeStatePayloadRecord>(bytes)
            .ok()
            .is_some_and(|record| {
                stable_bytes_hash(record.payload.payload.as_bytes()) == content_hash
            })
        {
            return true;
        }
        return ObjectKey::parse_standing_runtime_state_payload(path.to_string())
            .ok()
            .is_some_and(|(key, _)| {
                checkpoint_fragments::validate_manifest(state, &key, bytes)
                    .is_ok_and(|manifest| manifest.is_some())
            });
    }
    if path.ends_with(".checkpoint.json") {
        return standing_runtime_checkpoint_record_from_slice(bytes)
            .ok()
            .is_some_and(|record| record.checkpoint.state_root.content_hash == content_hash);
    }
    stable_bytes_hash(bytes) == content_hash
}

pub(super) async fn cached_checkpoint_size(state: &ApiState, path: &ObjectPath) -> Option<u64> {
    let cache = state.native_read_cache.as_ref()?;
    let key = checkpoint_read_cache_key(state, path).ok()?;
    let hash = key.content_hash.clone();
    let object_path = path.to_string();
    cache
        .get(&key, |bytes| {
            if checkpoint_cache_bytes_match(state, &object_path, &hash, bytes) {
                Ok(())
            } else {
                Err(())
            }
        })
        .await
        .map(|bytes| bytes.len() as u64)
}

async fn read_object_bytes_bounded_uncached(
    state: &ApiState,
    path: &ObjectPath,
    max_bytes: Option<u64>,
) -> Result<Vec<u8>, ApiError> {
    use futures::StreamExt;
    let result = state.store.get(path).await.map_err(ApiError::internal)?;
    if max_bytes.is_some_and(|limit| result.meta.size > limit) {
        return Err(ApiError::bad_request(
            "native read exceeds query scan or memory byte budget",
        ));
    }
    let mut stream = result.into_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(ApiError::internal)?;
        let length = bytes
            .len()
            .checked_add(chunk.len())
            .ok_or_else(|| ApiError::bad_request("native read byte count overflow"))?;
        if max_bytes.is_some_and(|limit| length as u64 > limit) {
            return Err(ApiError::bad_request(
                "native read exceeds query scan or memory byte budget",
            ));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
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
    let output_delta_key = output_delta_key.to_string();
    tokio::task::spawn_blocking(move || {
        let (_, parts) = ObjectKey::parse_standing_runtime_output_delta(output_delta_key.clone())
            .map_err(ApiError::bad_request)?;
        let decode_timer = CursorPhaseTimer::new(
            &parts.view_id,
            parts.logical_epoch,
            &format!("commit_json_decode_bytes_{}", bytes.len()),
        );
        let record: StandingRuntimeOutputDeltaRecord = serde_json::from_slice(&bytes)
            .map_err(|source| ApiError::bad_request(source.to_string()))?;
        drop(decode_timer);
        let (key, _) = ObjectKey::parse_standing_runtime_output_delta(output_delta_key.to_string())
            .map_err(ApiError::bad_request)?;
        validate_standing_runtime_output_delta_record(&key, &record)?;
        Ok((key, record))
    })
    .await
    .map_err(ApiError::internal)?
}

pub(super) fn validate_standing_runtime_output_manifest_record(
    key: &ObjectKey,
    record: &StandingRuntimeOutputManifestRecord,
) -> Result<(), ApiError> {
    if native_output_pages::is_native_codec(&record.output_encoding) {
        return native_output_pages::validate_manifest(key, record);
    }
    let bytes = serde_json::to_vec(&record.published_output)
        .map_err(|source| ApiError::internal(source.to_string()))?;
    let hash = stable_bytes_hash(&bytes);
    let count = standing_runtime_published_output_row_count(&record.published_output)?;
    validate_standing_runtime_output_manifest_record_with_summary(key, record, &hash, count)
}

fn validate_standing_runtime_output_manifest_record_with_summary(
    key: &ObjectKey,
    record: &StandingRuntimeOutputManifestRecord,
    actual_output_hash: &str,
    output_row_count: usize,
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
    let hash_timer = CursorPhaseTimer::new(
        &record.view_id,
        record.logical_epoch,
        "commit_delta_serialize_hash",
    );
    let delta_bytes = serde_json::to_vec(&record.output_delta)
        .map_err(|source| ApiError::internal(source.to_string()))?;
    let actual_delta_hash = stable_bytes_hash(&delta_bytes);
    drop(hash_timer);
    let decode_timer = CursorPhaseTimer::new(
        &record.view_id,
        record.logical_epoch,
        "commit_delta_typed_decode",
    );
    let output_delta: DeltaBatch = serde_json::from_value(record.output_delta.clone())
        .map_err(|source| ApiError::bad_request(source.to_string()))?;
    drop(decode_timer);
    let count_timer = CursorPhaseTimer::new(
        &record.view_id,
        record.logical_epoch,
        "commit_delta_signed_consolidation_count",
    );
    let delta_row_count = output_delta
        .net_row_count()
        .map_err(|_| ApiError::bad_request("standing runtime output delta is malformed"))?;
    drop(count_timer);
    if parts.tenant_id != record.tenant_id
        || parts.program_id != record.program_id
        || parts.view_id != record.view_id
        || parts.logical_epoch != record.logical_epoch
        || parts.delta_content_hash != record.delta_content_hash
        || actual_delta_hash != record.delta_content_hash
        || delta_row_count != record.delta_row_count
    {
        return Err(ApiError::bad_request(format!(
            "standing runtime output delta key/body mismatch for `{}/{}/{}` (body_hash_matches={}, key_hash_matches={}, row_count_matches={})",
            record.tenant_id,
            record.program_id,
            record.view_id,
            actual_delta_hash == record.delta_content_hash,
            parts.delta_content_hash == record.delta_content_hash,
            delta_row_count == record.delta_row_count
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

pub(super) fn is_sha256_identity(value: &str) -> bool {
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
    if page.output_encoding != "velorix-delta-batch-json-v1"
        && !native_output_pages::is_native_codec(&page.output_encoding)
    {
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
    if native_output_pages::is_native_codec(&record.output_encoding) {
        return native_output_pages::validate_page(key, record);
    }
    let bytes = serde_json::to_vec(&record.published_output)
        .map_err(|source| ApiError::internal(source.to_string()))?;
    let hash = stable_bytes_hash(&bytes);
    let count = standing_runtime_published_output_row_count(&record.published_output)?;
    validate_standing_runtime_output_page_record_with_summary(key, record, &hash, count)
}

fn validate_standing_runtime_output_page_record_with_summary(
    key: &ObjectKey,
    record: &StandingRuntimeOutputPageRecord,
    actual_page_hash: &str,
    row_count: usize,
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

    // A checkpoint before any source partition exists has no replay coverage.
    // Keep rejecting replay claims without a corresponding input frontier.
    if record.checkpoint.input_frontiers.is_empty() && record.replay_checkpoints.is_empty() {
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
#[cfg(test)]
mod published_bag_count_tests {
    use super::*;
    use velorix_core::delta::{DeltaKey, DeltaRecord, DeltaValue};

    fn snapshot(weights: &[i64]) -> Value {
        serde_json::to_value(DeltaBatch::from_records(weights.iter().enumerate().map(
            |(key, weight)| DeltaRecord {
                key: DeltaKey::from_json(json!(key)),
                value: DeltaValue::from_json(json!({"value": 1})),
                weight: *weight,
            },
        )))
        .unwrap()
    }

    #[test]
    fn materialized_count_preserves_bag_copies_and_rejects_invalid_totals() {
        assert_eq!(
            standing_runtime_published_output_row_count(&snapshot(&[2, 3])).unwrap(),
            5
        );
        assert!(standing_runtime_published_output_row_count(&snapshot(&[-1])).is_err());
        assert!(standing_runtime_published_output_row_count(&snapshot(&[0])).is_err());
        let mixed_snapshot = DeltaBatch::from_records([2, -1].map(|weight| DeltaRecord {
            key: DeltaKey::from_json(json!("same-key")),
            value: DeltaValue::from_json(json!({"value": 1})),
            weight,
        }));
        assert!(standing_runtime_published_output_row_count(
            &serde_json::to_value(mixed_snapshot).unwrap()
        )
        .is_err());
        assert!(standing_runtime_published_output_row_count(&snapshot(&[
            i64::MAX,
            i64::MAX,
            i64::MAX
        ]))
        .is_err());
    }

    #[test]
    fn published_snapshot_count_matches_consolidation_without_decoding_payloads() {
        let output = DeltaBatch::from_records((0..4096).flat_map(|key| {
            [2, 3].map(|weight| DeltaRecord {
                key: DeltaKey::from_json(json!({"entity": key, "date": 1})),
                value: DeltaValue::from_json(json!({"measurement": f64::from_bits(0x723f_4114_006c_08c7), "flags": [null, true, false]})),
                weight,
            })
        }));
        let expected: usize = output
            .net_rows()
            .unwrap()
            .iter()
            .map(|row| row.weight as usize)
            .sum();
        assert_eq!(
            standing_runtime_published_output_row_count(&serde_json::to_value(output).unwrap())
                .unwrap(),
            expected
        );
        let overflowed_entry = DeltaBatch::from_records([i64::MAX, 1].map(|weight| DeltaRecord {
            key: DeltaKey::from_json(json!("same")),
            value: DeltaValue::from_json(Value::Null),
            weight,
        }));
        assert!(standing_runtime_published_output_row_count(
            &serde_json::to_value(overflowed_entry).unwrap()
        )
        .is_err());
        // Distinct entries may exceed i64 collectively while fitting usize.
        if usize::BITS == 64 {
            assert_eq!(
                standing_runtime_published_output_row_count(&snapshot(&[i64::MAX, 1])).unwrap(),
                (i64::MAX as usize) + 1
            );
        }
        for malformed in [
            json!({}),
            json!({"records": null}),
            json!({"records": [{"key": 1, "weight": 1}]}),
            json!({"records": [{"key": 1, "value": null, "weight": 1.5}]}),
        ] {
            assert!(standing_runtime_published_output_row_count(&malformed).is_err());
        }
    }

    #[test]
    fn persisted_float_json_preserves_bits_and_content_hash() {
        // Pre-feature parser reproduced a one-ULP change for this public,
        // generated value: ...08c7 serialized as 2.08403533043113e+242 and
        // parsed as ...08c6. Exercise the actual published snapshot envelope.
        let value = f64::from_bits(0x723f_4114_006c_08c7);
        let snapshot = DeltaBatch::from_records([DeltaRecord {
            key: DeltaKey::from_json(json!({"series_key": "generated-series", "date": 1})),
            value: DeltaValue::from_json(json!({"power": value})),
            weight: 2,
        }]);
        let bytes = serde_json::to_vec(&snapshot).unwrap();
        let restored: DeltaBatch = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            restored.records()[0].value.as_json()["power"]
                .as_f64()
                .unwrap()
                .to_bits(),
            value.to_bits()
        );
        assert_eq!(restored.records()[0].weight, 2);
        assert_eq!(
            stable_bytes_hash(&bytes),
            stable_bytes_hash(&serde_json::to_vec(&restored).unwrap())
        );
        // Generated finite values exercise JSON's decimal-to-binary boundary
        // without private oracle data or changes to comparison tolerances.
        let mut bits = 0x1234_5678_9abc_def0u64;
        for _ in 0..4096 {
            bits = bits.wrapping_mul(6364136223846793005).wrapping_add(1);
            let value = f64::from_bits(bits);
            if !value.is_finite() {
                continue;
            }
            let encoded = serde_json::to_vec(&value).unwrap();
            let decoded: f64 = serde_json::from_slice(&encoded).unwrap();
            assert_eq!(
                value.to_bits(),
                decoded.to_bits(),
                "persisted float bits differ for decimal {}",
                String::from_utf8_lossy(&encoded)
            );
            assert_eq!(
                stable_bytes_hash(&encoded),
                stable_bytes_hash(&serde_json::to_vec(&decoded).unwrap())
            );
        }
    }
}
