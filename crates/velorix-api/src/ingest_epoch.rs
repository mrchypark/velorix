use super::*;

#[derive(Clone)]
pub(super) struct PreparedIngestBatch {
    pub(super) request: IngestRowsRequest,
    catalog: VelorixRelationCatalogV1,
    record_batch: RecordBatch,
    pub(super) end_offset_exclusive: u64,
    event_time_watermark: Option<InputEventTimeWatermark>,
    payload_digest: String,
    pub(super) envelope: bytes::Bytes,
}

pub(super) struct PreparedIngestAppendOutcome {
    index: usize,
    response: IngestResponse,
    appended: bool,
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct PreparedStandingRuntimeApplySummary {
    pub(super) active_views: usize,
    pub(super) applied_batches: usize,
    materialized_through: Option<u64>,
    checkpoint_writes: usize,
    output_delta_writes: usize,
    state_payload_writes: usize,
    checkpoint_record_writes: usize,
    checkpoint_pointer_writes: usize,
    convergence_writes: usize,
    compaction_scheduled: usize,
}

#[derive(Clone, Debug, Default)]
pub(super) struct StandingRuntimeCheckpointWriteSummary {
    pub(super) output_delta_writes: usize,
    pub(super) state_payload_writes: usize,
    pub(super) checkpoint_record_writes: usize,
    pub(super) checkpoint_pointer_writes: usize,
    pub(super) compaction_scheduled: usize,
    pub(super) output_refs: Vec<String>,
}

pub(super) struct StandingRuntimeCheckpointPersistContext {
    pub(super) previous_record: Option<StandingRuntimeCheckpointRecord>,
    pub(super) replay_checkpoints_to_merge: Vec<ReplayCheckpoint>,
    pub(super) owner: Option<StandingRuntimeOwnerToken>,
    pub(super) published_relation: Option<PublishedRelationBindingV1>,
    pub(super) direct_view_inputs: Vec<StandingRuntimeDirectViewInputV1>,
}

#[derive(Clone, Debug)]
pub(super) struct StandingRuntimeDirectViewInputV1 {
    pub(super) published_relation: PublishedRelationBindingV1,
    pub(super) cursor: CausalViewCursorV1,
}

impl StandingRuntimeCheckpointPersistContext {
    pub(super) fn new(
        previous_record: Option<StandingRuntimeCheckpointRecord>,
        replay_checkpoints_to_merge: Vec<ReplayCheckpoint>,
        owner: Option<StandingRuntimeOwnerToken>,
    ) -> Self {
        Self {
            previous_record,
            replay_checkpoints_to_merge,
            owner,
            published_relation: None,
            direct_view_inputs: Vec::new(),
        }
    }

    pub(super) fn with_published_relation(
        mut self,
        published_relation: Option<PublishedRelationBindingV1>,
    ) -> Self {
        self.published_relation = published_relation;
        self
    }

    pub(super) fn with_direct_view_inputs(
        mut self,
        direct_view_inputs: Vec<StandingRuntimeDirectViewInputV1>,
    ) -> Self {
        self.direct_view_inputs = direct_view_inputs;
        self
    }
}

#[derive(Clone, Debug)]
pub(super) struct PersistedIngestEpochManifest {
    pub(super) epoch_manifest_id: String,
    pub(super) epoch_manifest_key: String,
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct IngestEpochManifestRecord {
    schema_version: u16,
    record_kind: String,
    epoch_manifest_id: String,
    batches: Vec<IngestEpochManifestBatchRecord>,
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct IngestEpochManifestBatchRecord {
    relation_id: String,
    relation_version: String,
    schema_fingerprint: String,
    stream_id: String,
    partition_id: u32,
    start_offset_inclusive: u64,
    end_offset_exclusive: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    event_time_watermark: Option<InputEventTimeWatermark>,
    payload_digest: String,
    batch_key: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(super) struct IngestEpochViewConvergenceRecord {
    schema_version: u16,
    record_kind: String,
    epoch_manifest_id: String,
    tenant_id: String,
    program_id: String,
    view_id: String,
    logical_epoch: u64,
    checkpoint_key: String,
    checkpoint_content_hash: String,
    pub(super) output_publication_protocol_id: String,
    pub(super) output_refs: Vec<String>,
    replay_checkpoints: Vec<ReplayCheckpoint>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(super) struct IngestEpochViewRuntimeFailureRecord {
    schema_version: u16,
    record_kind: String,
    epoch_manifest_id: String,
    tenant_id: String,
    program_id: String,
    view_id: String,
    pub(super) failure_reason: String,
    replay_checkpoints: Vec<ReplayCheckpoint>,
}

#[cfg(test)]
pub(super) async fn ingest_rows_test_compat(
    State(state): State<ApiState>,
    Json(request): Json<IngestRowsRequest>,
) -> Result<(StatusCode, Json<IngestResponse>), ApiError> {
    let (status, Json(epoch_response)) = ingest_epoch(
        State(state),
        Json(IngestEpochRequest {
            batches: vec![IngestRowsRequest { ..request }],
        }),
    )
    .await?;
    let Some(batch) = epoch_response.batches.into_iter().next() else {
        return Err(ApiError::internal(
            "single ingest produced no batch response",
        ));
    };
    Ok((
        status,
        Json(IngestResponse {
            outcome: batch.outcome,
            descriptor: batch.descriptor,
            epoch_manifest_id: epoch_response.epoch_manifest_id,
            ingest_epoch: epoch_response.ingest_epoch,
            materialized_through: epoch_response.materialized_through,
            ack_mode: epoch_response.ack_mode,
            materialization: epoch_response.materialization,
            timings: epoch_response.timings,
        }),
    ))
}

pub(super) async fn ingest_relation_rows(
    State(state): State<ApiState>,
    AxumPath(relation_id): AxumPath<String>,
    Json(request): Json<IngestRelationRowsRequest>,
) -> Result<(StatusCode, Json<IngestEpochResponse>), ApiError> {
    if relation_id.trim().is_empty() {
        return Err(ApiError::bad_request(
            "relation_id path segment is required",
        ));
    }
    ingest_epoch(
        State(state),
        Json(IngestEpochRequest {
            batches: vec![IngestRowsRequest {
                relation_id,
                relation_version: request.relation_version,
                stream_id: request.stream_id,
                partition_id: request.partition_id,
                start_offset_inclusive: request.start_offset_inclusive,
                event_time_watermark: request.event_time_watermark,
                rows: request.rows,
            }],
        }),
    )
    .await
}

/// Ingest an epoch of batches atomically.
///
/// This endpoint processes all batches in the request as a single atomic transaction.
/// If any batch fails (validation, append, or commit), the entire epoch fails and
/// no batches are persisted. The caller receives an error response indicating the
/// failure reason.
///
/// On success, all batches are durably persisted and the response includes
/// per-batch details. On failure, the caller should retry the entire epoch
/// with the same idempotency key to ensure exactly-once semantics.
pub(super) async fn ingest_epoch(
    State(state): State<ApiState>,
    Json(request): Json<IngestEpochRequest>,
) -> Result<(StatusCode, Json<IngestEpochResponse>), ApiError> {
    let ack_mode = IngestAckMode::Materialized;
    let mut timer = IngestTimer::start();
    let batch_count = request.batches.len();
    if request.batches.is_empty() {
        return Err(ApiError::bad_request(
            "ingest epoch must contain at least one batch",
        ));
    }
    if request.batches.iter().any(|batch| batch.rows.is_empty()) {
        return Err(ApiError::bad_request(
            "ingest epoch batches must contain at least one row",
        ));
    }
    let total_rows = request
        .batches
        .iter()
        .try_fold(0usize, |total, batch| total.checked_add(batch.rows.len()))
        .ok_or_else(|| ApiError::bad_request("ingest epoch row count overflow"))?;
    if total_rows > state.max_ingest_rows {
        return Err(ApiError::payload_too_large(format!(
            "ingest epoch row count {total_rows} exceeds configured limit {}",
            state.max_ingest_rows
        )));
    }
    timer.set_workload(batch_count, total_rows);

    let mut prepared_batches = Vec::with_capacity(request.batches.len());
    let mut catalog_cache: HashMap<(String, String), VelorixRelationCatalogV1> = HashMap::new();
    for batch in request.batches {
        let catalog_key = (batch.relation_id.clone(), batch.relation_version.clone());
        let catalog = match catalog_cache.get(&catalog_key) {
            Some(catalog) => catalog.clone(),
            None => {
                let catalog =
                    read_relation_catalog(&state, &batch.relation_id, &batch.relation_version)
                        .await?;
                catalog_cache.insert(catalog_key, catalog.clone());
                catalog
            }
        };
        prepared_batches.push(prepare_ingest_batch_with_catalog(
            &state,
            batch,
            catalog,
            Some(&mut timer),
        )?);
    }
    timer.mark("prepare");
    let canonical_total_rows = prepared_batches
        .iter()
        .try_fold(0usize, |total, batch| {
            total.checked_add(batch.request.rows.len())
        })
        .ok_or_else(|| ApiError::bad_request("canonical ingest epoch row count overflow"))?;
    if canonical_total_rows > state.max_ingest_rows {
        return Err(ApiError::payload_too_large(format!(
            "canonical ingest epoch row count {canonical_total_rows} exceeds configured limit {}",
            state.max_ingest_rows
        )));
    }
    validate_ingest_epoch_batch_ranges(&prepared_batches)?;
    let epoch_manifest = persist_ingest_epoch_manifest(&state, &prepared_batches).await?;
    timer.mark("epoch_manifest");
    ensure_no_ingest_epoch_view_runtime_failures(&state, &epoch_manifest, &prepared_batches)
        .await?;
    let mut ensured_ingest_relations = BTreeSet::new();
    for prepared in &prepared_batches {
        let key = (
            prepared.request.relation_id.as_str(),
            prepared.request.relation_version.as_str(),
        );
        if ensured_ingest_relations.insert(key) {
            ensure_standing_runtimes_for_ingest(&state, &prepared.request).await?;
        }
    }
    timer.mark("ensure_runtime");
    let mut preacquired_ingest_relations = BTreeSet::new();
    for prepared in &prepared_batches {
        let key = (
            prepared.request.relation_id.as_str(),
            prepared.request.relation_version.as_str(),
        );
        if preacquired_ingest_relations.insert(key) {
            preacquire_standing_runtime_owners_for_ingest(&state, &prepared.request).await?;
        }
    }
    timer.mark("preacquire_owner");
    if state.meta_store.is_some() {
        for prepared in &prepared_batches {
            reserve_ingest_range(
                &state,
                &prepared.request,
                &prepared.catalog,
                prepared.end_offset_exclusive,
                &prepared.envelope,
            )
            .await?;
        }
    }
    timer.mark("reserve_range");
    let (appended, responses) = append_prepared_ingest_epoch_batches(
        &state,
        &prepared_batches,
        ack_mode,
        &epoch_manifest.epoch_manifest_id,
    )
    .await?;
    timer.mark("append_parallel");
    timer.mark("append");

    let materialization = materialize_prepared_ingest_epoch_for_ack_mode(
        &state,
        &epoch_manifest,
        prepared_batches.clone(),
        ack_mode,
        &mut timer,
    )
    .await?;

    let (status, outcome) = if appended > 0 {
        (StatusCode::CREATED, "appended")
    } else {
        (StatusCode::OK, "duplicate")
    };
    Ok((
        status,
        Json(IngestEpochResponse {
            outcome: outcome.to_string(),
            epoch_manifest_id: epoch_manifest.epoch_manifest_id.clone(),
            epoch_manifest_key: epoch_manifest.epoch_manifest_key,
            ingest_epoch: epoch_manifest.epoch_manifest_id,
            materialized_through: materialization.materialized_through,
            ack_mode,
            materialization,
            timings: timer.finish(),
            batches: responses,
        }),
    ))
}

/// Append prepared ingest batches atomically.
///
/// All batches are appended concurrently in two phases:
/// Phase 1: Object PUT for all batches (concurrent)
/// Phase 2: Range commit for all batches (concurrent, only if phase 1
///          fully succeeds)
///
/// If any batch fails in phase 1, no range commits are performed and
/// the epoch remains invisible to readers. The content-addressed object
/// PUTs are idempotent and will be cleaned up by GC if unused.
pub(super) async fn append_prepared_ingest_epoch_batches(
    state: &ApiState,
    prepared_batches: &[PreparedIngestBatch],
    ack_mode: IngestAckMode,
    ingest_epoch: &str,
) -> Result<(usize, Vec<IngestResponse>), ApiError> {
    let concurrency = prepared_batches
        .len()
        .clamp(1, MAX_CONCURRENT_EPOCH_APPENDS);

    // Phase 1: Concurrent object PUTs for all batches
    let append_results: Vec<_> = futures::stream::iter(prepared_batches.iter().cloned().enumerate())
        .map(|(index, prepared)| {
            let state = state.clone();
            async move {
                let outcome = append_ingest_envelope(&state, prepared.envelope.clone()).await?;
                let (_status, outcome_str, descriptor) = ingest_outcome_parts(outcome)?;
                Ok::<_, ApiError>((
                    index,
                    prepared,
                    outcome_str.to_string(),
                    descriptor,
                ))
            }
        })
        .buffer_unordered(concurrency)
        .try_collect::<Vec<_>>()
        .await?;

    // Phase 2: Concurrent range commits (only if all PUTs succeeded)
    let owned_epoch = ingest_epoch.to_string();
    let mut commit_results = futures::stream::iter(append_results.into_iter().map(|item| {
        let (index, prepared, outcome_str, descriptor) = item;
        let state = state.clone();
        let ingest_epoch = owned_epoch.clone();
        async move {
            commit_ingest_range(
                &state,
                &prepared.request,
                &prepared.catalog,
                prepared.end_offset_exclusive,
                &prepared.envelope,
            )
            .await?;
            Ok::<_, ApiError>(PreparedIngestAppendOutcome {
                index,
                appended: outcome_str == "appended",
                response: IngestResponse {
                    outcome: outcome_str.to_string(),
                    descriptor: ingest_descriptor_response(&descriptor),
                    epoch_manifest_id: ingest_epoch.clone(),
                    ingest_epoch,
                    materialized_through: None,
                    ack_mode,
                    materialization: IngestMaterializationResponse {
                        status: "epoch_scoped".to_string(),
                        active_views: 0,
                        applied_batches: 0,
                        materialized_through: None,
                        checkpoint_writes: 0,
                        applied_batches_per_checkpoint_write: None,
                        output_delta_writes: 0,
                        state_payload_writes: 0,
                        checkpoint_record_writes: 0,
                        checkpoint_pointer_writes: 0,
                        checkpoint_publication_writes: 0,
                    },
                    timings: IngestTimingResponse {
                        total_ms: 0,
                        total_us: 0,
                        avg_batch_us: None,
                        avg_row_us: None,
                        rows_per_second: None,
                        batch_count: 1,
                        row_count: prepared.request.rows.len(),
                    },
                },
            })
        }
    }))
    .buffer_unordered(concurrency)
    .try_collect::<Vec<_>>()
    .await?;

    commit_results.sort_by_key(|outcome| outcome.index);
    let appended = commit_results.iter().filter(|outcome| outcome.appended).count();
    Ok((
        appended,
        commit_results
            .into_iter()
            .map(|outcome| outcome.response)
            .collect(),
    ))
}

pub(super) async fn materialize_prepared_ingest_epoch_for_ack_mode(
    state: &ApiState,
    epoch_manifest: &PersistedIngestEpochManifest,
    prepared_batches: Vec<PreparedIngestBatch>,
    ack_mode: IngestAckMode,
    timer: &mut IngestTimer,
) -> Result<IngestMaterializationResponse, ApiError> {
    match ack_mode {
        IngestAckMode::Materialized => {
            let summary = apply_standing_runtime_ingest_epoch(
                state,
                epoch_manifest,
                &prepared_batches,
                Some(timer),
            )
            .await?;
            timer.mark("materialize");
            // Producer checkpoints are durable now; pull every pending
            // producer commit into dependent consumer views before the
            // materialized acknowledgement returns.
            drain_published_view_dependencies(state).await?;
            timer.mark("materialize_view_dependencies");
            Ok(materialization_response("completed", summary))
        }
    }
}

pub(super) fn materialization_response(
    status: &str,
    summary: PreparedStandingRuntimeApplySummary,
) -> IngestMaterializationResponse {
    IngestMaterializationResponse {
        status: status.to_string(),
        active_views: summary.active_views,
        applied_batches: summary.applied_batches,
        materialized_through: summary.materialized_through,
        checkpoint_writes: summary.checkpoint_writes,
        applied_batches_per_checkpoint_write: nonzero_div_usize(
            summary.applied_batches,
            summary.checkpoint_writes,
        ),
        output_delta_writes: summary.output_delta_writes,
        state_payload_writes: summary.state_payload_writes,
        checkpoint_record_writes: summary.checkpoint_record_writes,
        checkpoint_pointer_writes: summary.checkpoint_pointer_writes,
        checkpoint_publication_writes: summary.convergence_writes,
    }
}

pub(super) async fn repair_ingest_epoch_runtime_failure(
    State(state): State<ApiState>,
    Json(request): Json<RepairIngestEpochRuntimeFailureRequest>,
) -> Result<Json<RepairIngestEpochRuntimeFailureResponse>, ApiError> {
    if !request.confirm_standing_runtime_repaired {
        return Err(ApiError::bad_request(
            "confirm_standing_runtime_repaired must be true after the native standing runtime has been repaired or cleared",
        ));
    }
    let repair_reason = request.repair_reason.trim();
    if repair_reason.is_empty() {
        return Err(ApiError::bad_request("repair_reason must not be empty"));
    }
    let active = state
        .view_registry()?
        .read_active(&request.view_id)
        .await
        .map_err(materialized_view_registry_error_to_api)?;
    let identity = active_standing_runtime_identity(&active).ok_or_else(|| {
        ApiError::bad_request(format!(
            "view `{}` is not backed by a standing runtime identity",
            request.view_id
        ))
    })?;
    if identity.tenant_id != request.tenant_id || identity.program_id != request.program_id {
        return Err(ApiError::bad_request(format!(
            "repair request identity does not match active view `{}`",
            request.view_id
        )));
    }
    let epoch_manifest = PersistedIngestEpochManifest {
        epoch_manifest_id: request.epoch_manifest_id.clone(),
        epoch_manifest_key: ObjectKey::ingest_epoch_manifest(&request.epoch_manifest_id)
            .map_err(ApiError::bad_request)?
            .as_str()
            .to_string(),
    };
    let marker_key = ObjectKey::ingest_epoch_view_runtime_failure(
        &request.epoch_manifest_id,
        &request.tenant_id,
        &request.program_id,
        &request.view_id,
    )
    .map_err(ApiError::bad_request)?;
    let failure =
        read_ingest_epoch_view_runtime_failure(&state, &epoch_manifest, identity, &request.view_id)
            .await?
            .ok_or_else(|| {
                ApiError::bad_request(format!(
                    "ingest epoch runtime failure marker does not exist at {}",
                    marker_key.as_str()
                ))
            })?;
    state
        .store
        .delete(&ObjectPath::from(marker_key.as_str()))
        .await
        .map_err(ApiError::internal)?;
    let removed_runtime_cache =
        remove_standing_runtime_if_present(&state, identity, &request.view_id)?;

    Ok(Json(RepairIngestEpochRuntimeFailureResponse {
        outcome: "repaired".to_string(),
        marker_key: marker_key.as_str().to_string(),
        tenant_id: request.tenant_id,
        program_id: request.program_id,
        view_id: request.view_id,
        epoch_manifest_id: request.epoch_manifest_id,
        removed_runtime_cache,
        failure_reason: failure.failure_reason,
        repair_reason: repair_reason.to_string(),
    }))
}

#[cfg(test)]
pub(super) async fn prepare_ingest_batch(
    state: &ApiState,
    request: IngestRowsRequest,
    timer: Option<&mut IngestTimer>,
) -> Result<PreparedIngestBatch, ApiError> {
    let catalog =
        read_relation_catalog(state, &request.relation_id, &request.relation_version).await?;
    prepare_ingest_batch_with_catalog(state, request, catalog, timer)
}

pub(super) fn prepare_ingest_batch_with_catalog(
    state: &ApiState,
    mut request: IngestRowsRequest,
    catalog: VelorixRelationCatalogV1,
    mut timer: Option<&mut IngestTimer>,
) -> Result<PreparedIngestBatch, ApiError> {
    if request.rows.len() > state.max_ingest_rows {
        return Err(ApiError::payload_too_large(format!(
            "ingest row count {} exceeds configured limit {}",
            request.rows.len(),
            state.max_ingest_rows
        )));
    }
    if catalog.relation_schema.relation_id != request.relation_id
        || catalog.relation_schema.relation_version != request.relation_version
    {
        return Err(ApiError::bad_request(format!(
            "relation catalog identity mismatch: request relation={}/{} catalog relation={}/{}",
            request.relation_id,
            request.relation_version,
            catalog.relation_schema.relation_id,
            catalog.relation_schema.relation_version
        )));
    }
    if let Some(timer) = timer.as_mut() {
        timer.mark("prepare_catalog");
    }
    request.rows = normalize_ingest_operation_envelopes(&catalog, &request.rows)?;
    if let Some(timer) = timer.as_mut() {
        timer.mark("prepare_normalize");
    }
    if request.rows.len() > state.max_ingest_rows {
        return Err(ApiError::payload_too_large(format!(
            "canonical ingest row count {} exceeds configured limit {}",
            request.rows.len(),
            state.max_ingest_rows
        )));
    }
    let batch = rows_to_record_batch(&catalog, &request.rows)?;
    if let Some(timer) = timer.as_mut() {
        timer.mark("prepare_arrow");
    }
    let end_offset_exclusive = request
        .start_offset_inclusive
        .checked_add(request.rows.len() as u64)
        .ok_or_else(|| ApiError::bad_request("ingest offset range overflow"))?;
    let event_time_watermark = ingest_event_time_watermark(&catalog, &request, &batch)?;
    let encoded = IngestEnvelope::encode_batches_with_header(
        IngestEnvelopeEncodeRequest {
            relation_id: request.relation_id.clone(),
            relation_version: request.relation_version.clone(),
            schema_fingerprint: catalog.schema_fingerprint.as_str().to_string(),
            stream_id: request.stream_id.clone(),
            partition_id: request.partition_id,
            start_offset_inclusive: request.start_offset_inclusive,
            end_offset_exclusive,
            event_time_watermark: event_time_watermark.clone(),
        },
        std::slice::from_ref(&batch),
    )
    .map_err(ApiError::bad_request)?;
    if let Some(timer) = timer.as_mut() {
        timer.mark("prepare_envelope");
    }
    let payload_digest = encoded.header.payload_digest.clone();
    if let Some(timer) = timer.as_mut() {
        timer.mark("prepare_digest");
    }
    Ok(PreparedIngestBatch {
        request,
        catalog,
        record_batch: batch,
        end_offset_exclusive,
        event_time_watermark,
        payload_digest,
        envelope: encoded.bytes,
    })
}

pub(super) fn ingest_event_time_watermark(
    catalog: &VelorixRelationCatalogV1,
    request: &IngestRowsRequest,
    batch: &RecordBatch,
) -> Result<Option<InputEventTimeWatermark>, ApiError> {
    let Some(request_watermark) = &request.event_time_watermark else {
        return Ok(None);
    };
    let Some(event_time_column_id) = &catalog.relation_schema.event_time_column_id else {
        return Err(ApiError::bad_request(
            "event_time_watermark requires relation_schema.event_time_column_id",
        ));
    };
    if request_watermark.event_time_column_id != *event_time_column_id {
        return Err(ApiError::bad_request(format!(
            "event_time_watermark.event_time_column_id must match relation event_time_column_id `{event_time_column_id}`"
        )));
    }
    let column = catalog
        .relation_schema
        .columns
        .iter()
        .find(|column| column.column_id == *event_time_column_id)
        .ok_or_else(|| ApiError::bad_request("relation event_time_column_id column is missing"))?;
    match &column.physical_arrow_type {
        ArrowPhysicalTypeV1::Int64
        | ArrowPhysicalTypeV1::Date32
        | ArrowPhysicalTypeV1::TimestampNanosecond { .. } => {}
        _ => {
            return Err(ApiError::bad_request(
                "event_time_watermark currently supports Int64, Date32, or TimestampNanosecond event-time columns",
            ));
        }
    }
    if request_watermark.event_time_column_id.trim().is_empty() {
        return Err(ApiError::bad_request(
            "event_time_watermark.event_time_column_id must be nonempty",
        ));
    }
    if request_watermark.watermark_ns > request_watermark.max_observed_event_time_ns {
        return Err(ApiError::bad_request(
            "event_time_watermark.watermark_ns must be <= max_observed_event_time_ns",
        ));
    }
    let actual_max_observed = event_time_column_max_value(column, batch)?;
    if request_watermark.max_observed_event_time_ns < actual_max_observed {
        return Err(ApiError::bad_request(format!(
            "event_time_watermark.max_observed_event_time_ns must be >= actual max event-time value {actual_max_observed}"
        )));
    }
    Ok(Some(InputEventTimeWatermark {
        stream_id: request.stream_id.clone(),
        partition_id: request.partition_id,
        event_time_column_id: request_watermark.event_time_column_id.clone(),
        max_observed_event_time_ns: request_watermark.max_observed_event_time_ns,
        watermark_ns: request_watermark.watermark_ns,
    }))
}

pub(super) fn event_time_column_max_value(
    column: &RelationColumnV1,
    batch: &RecordBatch,
) -> Result<i64, ApiError> {
    match &column.physical_arrow_type {
        ArrowPhysicalTypeV1::Int64 => {
            let array = batch
                .column_by_name(&column.name)
                .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
                .ok_or_else(|| ApiError::bad_request("event-time column must be Int64"))?;
            max_int64_array_value(&column.name, array)
        }
        ArrowPhysicalTypeV1::Date32 => {
            let array = batch
                .column_by_name(&column.name)
                .and_then(|column| column.as_any().downcast_ref::<Date32Array>())
                .ok_or_else(|| ApiError::bad_request("event-time column must be Date32"))?;
            max_i32_array_value(&column.name, array)
                .map(|days| i64::from(days) * 86_400_000_000_000)
        }
        ArrowPhysicalTypeV1::TimestampNanosecond { .. } => {
            let array = batch
                .column_by_name(&column.name)
                .and_then(|column| {
                    column.as_any().downcast_ref::<TimestampNanosecondArray>()
                })
                .ok_or_else(|| {
                    ApiError::bad_request("event-time column must be TimestampNanosecond")
                })?;
            max_timestamp_array_value(&column.name, array)
        }
        _ => Err(ApiError::bad_request(
            "event_time_watermark currently supports Int64, Date32, or TimestampNanosecond event-time columns",
        )),
    }
}

pub(super) fn max_int64_array_value(name: &str, array: &Int64Array) -> Result<i64, ApiError> {
    let mut max_value = None;
    for row in 0..array.len() {
        if !array.is_null(row) {
            max_value = Some(max_value.map_or(array.value(row), |current: i64| {
                current.max(array.value(row))
            }));
        }
    }
    max_value.ok_or_else(|| {
        ApiError::bad_request(format!(
            "event-time column `{name}` must contain at least one non-null value"
        ))
    })
}

pub(super) fn max_timestamp_array_value(
    name: &str,
    array: &TimestampNanosecondArray,
) -> Result<i64, ApiError> {
    let mut max_value = None;
    for row in 0..array.len() {
        if !array.is_null(row) {
            max_value = Some(max_value.map_or(array.value(row), |current: i64| {
                current.max(array.value(row))
            }));
        }
    }
    max_value.ok_or_else(|| {
        ApiError::bad_request(format!(
            "event-time column `{name}` must contain at least one non-null value"
        ))
    })
}

pub(super) fn max_i32_array_value(name: &str, array: &Date32Array) -> Result<i32, ApiError> {
    let mut max_value = None;
    for row in 0..array.len() {
        if !array.is_null(row) {
            max_value = Some(max_value.map_or(array.value(row), |current: i32| {
                current.max(array.value(row))
            }));
        }
    }
    max_value.ok_or_else(|| {
        ApiError::bad_request(format!(
            "event-time column `{name}` must contain at least one non-null value"
        ))
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct IngestEpochRange {
    relation_id: String,
    relation_version: String,
    schema_fingerprint: String,
    stream_id: String,
    partition_id: u32,
    start_offset_inclusive: u64,
    end_offset_exclusive: u64,
}

pub(super) fn validate_ingest_epoch_batch_ranges(
    prepared_batches: &[PreparedIngestBatch],
) -> Result<(), ApiError> {
    let mut ranges = prepared_batches
        .iter()
        .map(|prepared| IngestEpochRange {
            relation_id: prepared.request.relation_id.clone(),
            relation_version: prepared.request.relation_version.clone(),
            schema_fingerprint: prepared.catalog.schema_fingerprint.as_str().to_string(),
            stream_id: prepared.request.stream_id.clone(),
            partition_id: prepared.request.partition_id,
            start_offset_inclusive: prepared.request.start_offset_inclusive,
            end_offset_exclusive: prepared.end_offset_exclusive,
        })
        .collect::<Vec<_>>();
    ranges.sort_by(|left, right| {
        (
            left.relation_id.as_str(),
            left.relation_version.as_str(),
            left.schema_fingerprint.as_str(),
            left.stream_id.as_str(),
            left.partition_id,
            left.start_offset_inclusive,
            left.end_offset_exclusive,
        )
            .cmp(&(
                right.relation_id.as_str(),
                right.relation_version.as_str(),
                right.schema_fingerprint.as_str(),
                right.stream_id.as_str(),
                right.partition_id,
                right.start_offset_inclusive,
                right.end_offset_exclusive,
            ))
    });

    for pair in ranges.windows(2) {
        let previous = &pair[0];
        let current = &pair[1];
        if !same_ingest_epoch_source(previous, current) {
            continue;
        }
        if previous.start_offset_inclusive == current.start_offset_inclusive
            && previous.end_offset_exclusive == current.end_offset_exclusive
        {
            return Err(ApiError::bad_request(format!(
                "duplicate ingest epoch range for relation={} version={} stream={} partition={} offsets={}-{}",
                current.relation_id,
                current.relation_version,
                current.stream_id,
                current.partition_id,
                current.start_offset_inclusive,
                current.end_offset_exclusive
            )));
        }
        if current.start_offset_inclusive < previous.end_offset_exclusive {
            return Err(ApiError::bad_request(format!(
                "overlapping ingest epoch ranges for relation={} version={} stream={} partition={} previous_offsets={}-{} current_offsets={}-{}",
                current.relation_id,
                current.relation_version,
                current.stream_id,
                current.partition_id,
                previous.start_offset_inclusive,
                previous.end_offset_exclusive,
                current.start_offset_inclusive,
                current.end_offset_exclusive
            )));
        }
    }

    Ok(())
}

pub(super) fn same_ingest_epoch_source(left: &IngestEpochRange, right: &IngestEpochRange) -> bool {
    left.relation_id == right.relation_id
        && left.relation_version == right.relation_version
        && left.schema_fingerprint == right.schema_fingerprint
        && left.stream_id == right.stream_id
        && left.partition_id == right.partition_id
}

pub(super) async fn persist_ingest_epoch_manifest(
    state: &ApiState,
    prepared_batches: &[PreparedIngestBatch],
) -> Result<PersistedIngestEpochManifest, ApiError> {
    let mut batch_records = prepared_batches
        .iter()
        .map(ingest_epoch_manifest_batch_record)
        .collect::<Result<Vec<_>, _>>()?;
    batch_records.sort_by(|left, right| {
        (
            left.relation_id.as_str(),
            left.relation_version.as_str(),
            left.schema_fingerprint.as_str(),
            left.stream_id.as_str(),
            left.partition_id,
            left.start_offset_inclusive,
            left.end_offset_exclusive,
            left.payload_digest.as_str(),
        )
            .cmp(&(
                right.relation_id.as_str(),
                right.relation_version.as_str(),
                right.schema_fingerprint.as_str(),
                right.stream_id.as_str(),
                right.partition_id,
                right.start_offset_inclusive,
                right.end_offset_exclusive,
                right.payload_digest.as_str(),
            ))
    });
    let epoch_manifest_id = ingest_epoch_manifest_id(&batch_records)?;
    let record = IngestEpochManifestRecord {
        schema_version: 1,
        record_kind: "ingest_epoch_manifest_v1".to_string(),
        epoch_manifest_id: epoch_manifest_id.clone(),
        batches: batch_records,
    };
    let bytes =
        serde_json::to_vec(&record).map_err(|source| ApiError::internal(source.to_string()))?;
    let key =
        ObjectKey::ingest_epoch_manifest(&epoch_manifest_id).map_err(ApiError::bad_request)?;
    let path = ObjectPath::from(key.as_str());
    let result = state
        .store
        .put_opts(
            &path,
            bytes::Bytes::from(bytes.clone()).into(),
            PutMode::Create.into(),
        )
        .await;
    match result {
        Ok(_) => {}
        Err(object_store::Error::AlreadyExists { .. }) => {
            let existing = state
                .store
                .get(&path)
                .await
                .map_err(ApiError::internal)?
                .bytes()
                .await
                .map_err(ApiError::internal)?;
            if existing.as_ref() != bytes.as_slice() {
                return Err(ApiError::conflict(format!(
                    "ingest epoch manifest conflict at {}",
                    key.as_str()
                )));
            }
        }
        Err(error) => return Err(ApiError::internal(error)),
    }
    Ok(PersistedIngestEpochManifest {
        epoch_manifest_id,
        epoch_manifest_key: key.as_str().to_string(),
    })
}

pub(super) fn ingest_epoch_manifest_batch_record(
    prepared: &PreparedIngestBatch,
) -> Result<IngestEpochManifestBatchRecord, ApiError> {
    let batch_key = ObjectKey::ingest_batch(
        &prepared.request.stream_id,
        prepared.request.partition_id,
        prepared.request.start_offset_inclusive,
        prepared.end_offset_exclusive,
    )
    .map_err(ApiError::bad_request)?;
    Ok(IngestEpochManifestBatchRecord {
        relation_id: prepared.request.relation_id.clone(),
        relation_version: prepared.request.relation_version.clone(),
        schema_fingerprint: prepared.catalog.schema_fingerprint.as_str().to_string(),
        stream_id: prepared.request.stream_id.clone(),
        partition_id: prepared.request.partition_id,
        start_offset_inclusive: prepared.request.start_offset_inclusive,
        end_offset_exclusive: prepared.end_offset_exclusive,
        event_time_watermark: prepared.event_time_watermark.clone(),
        payload_digest: prepared.payload_digest.clone(),
        batch_key: batch_key.as_str().to_string(),
    })
}

pub(super) fn ingest_epoch_manifest_id(
    batch_records: &[IngestEpochManifestBatchRecord],
) -> Result<String, ApiError> {
    let bytes = serde_json::to_vec(batch_records)
        .map_err(|source| ApiError::internal(source.to_string()))?;
    let mut hasher = Sha256::new();
    hasher.update(b"velorix.ingest-epoch.manifest.v1\0");
    hasher.update(&bytes);
    Ok(format!("sha256:{:x}", hasher.finalize()))
}

pub(super) async fn persist_ingest_epoch_view_convergence(
    state: &ApiState,
    epoch_manifest: &PersistedIngestEpochManifest,
    view_id: &str,
    checkpoint: &RuntimeCheckpoint,
    output_refs: Vec<String>,
    replay_checkpoints: Vec<ReplayCheckpoint>,
) -> Result<(), ApiError> {
    let key = ObjectKey::ingest_epoch_view_convergence(
        &epoch_manifest.epoch_manifest_id,
        &checkpoint.identity.tenant_id,
        &checkpoint.identity.program_id,
        view_id,
    )
    .map_err(ApiError::bad_request)?;
    let checkpoint_key = ObjectKey::standing_runtime_checkpoint(
        &checkpoint.identity.tenant_id,
        &checkpoint.identity.program_id,
        view_id,
        checkpoint.logical_epoch,
        &checkpoint.state_root.content_hash,
    )
    .map_err(ApiError::bad_request)?;
    let record = IngestEpochViewConvergenceRecord {
        schema_version: 2,
        record_kind: "ingest_epoch_view_convergence_v2".to_string(),
        epoch_manifest_id: epoch_manifest.epoch_manifest_id.clone(),
        tenant_id: checkpoint.identity.tenant_id.clone(),
        program_id: checkpoint.identity.program_id.clone(),
        view_id: view_id.to_string(),
        logical_epoch: checkpoint.logical_epoch,
        checkpoint_key: checkpoint_key.as_str().to_string(),
        checkpoint_content_hash: checkpoint.state_root.content_hash.clone(),
        output_publication_protocol_id: OUTPUT_PUBLICATION_PROTOCOL_VERSION_V1.to_string(),
        output_refs,
        replay_checkpoints,
    };
    let bytes =
        serde_json::to_vec(&record).map_err(|source| ApiError::internal(source.to_string()))?;
    let path = ObjectPath::from(key.as_str());
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
                    "ingest epoch view convergence conflict at {}",
                    key.as_str()
                )))
            }
        }
        Err(error) => Err(ApiError::internal(error)),
    }
}

pub(super) async fn read_ingest_epoch_view_convergence(
    state: &ApiState,
    epoch_manifest: &PersistedIngestEpochManifest,
    identity: &StandingProgramIdentity,
    view_id: &str,
) -> Result<Option<IngestEpochViewConvergenceRecord>, ApiError> {
    let key = ObjectKey::ingest_epoch_view_convergence(
        &epoch_manifest.epoch_manifest_id,
        &identity.tenant_id,
        &identity.program_id,
        view_id,
    )
    .map_err(ApiError::bad_request)?;
    let path = ObjectPath::from(key.as_str());
    let bytes = match state.store.get(&path).await {
        Ok(result) => result.bytes().await.map_err(ApiError::internal)?,
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(error) => return Err(ApiError::internal(error)),
    };
    let record: IngestEpochViewConvergenceRecord = serde_json::from_slice(&bytes)
        .map_err(|source| ApiError::bad_request(source.to_string()))?;
    validate_ingest_epoch_view_convergence_record(
        &record,
        epoch_manifest,
        identity,
        view_id,
        key.as_str(),
    )?;
    let checkpoint = if let Some(meta_store) = state.meta_store.as_ref() {
        let pointer = meta_store
            .read_standing_runtime_checkpoint(
                &record.tenant_id,
                &record.program_id,
                &record.view_id,
            )
            .await
            .map_err(meta_error_to_api)?
            .ok_or_else(|| {
                ApiError::service_unavailable(format!(
                    "standing runtime checkpoint metadata is unavailable for `{}/{}/{}`",
                    record.tenant_id, record.program_id, record.view_id
                ))
            })?;
        read_standing_runtime_checkpoint_record_from_pointer(state, identity, view_id, &pointer)
            .await?
    } else {
        read_latest_standing_runtime_checkpoint(state, identity, view_id)
            .await?
            .ok_or_else(|| {
                ApiError::service_unavailable(format!(
                    "standing runtime local checkpoint is unavailable for `{}/{}/{}`",
                    record.tenant_id, record.program_id, record.view_id
                ))
            })?
    };
    if checkpoint.checkpoint_key != record.checkpoint_key {
        return Err(ApiError::bad_request(format!(
            "ingest epoch view convergence checkpoint pointer mismatch at {}",
            key.as_str()
        )));
    }
    if checkpoint.checkpoint.logical_epoch != record.logical_epoch
        || checkpoint.checkpoint.state_root.content_hash != record.checkpoint_content_hash
        || checkpoint.checkpoint.output_manifest_refs != record.output_refs
    {
        return Err(ApiError::bad_request(format!(
            "ingest epoch view convergence checkpoint mismatch at {}",
            key.as_str()
        )));
    }
    Ok(Some(record))
}

pub(super) fn validate_ingest_epoch_view_convergence_record(
    record: &IngestEpochViewConvergenceRecord,
    epoch_manifest: &PersistedIngestEpochManifest,
    identity: &StandingProgramIdentity,
    view_id: &str,
    key: &str,
) -> Result<(), ApiError> {
    if record.schema_version != 2
        || record.record_kind != "ingest_epoch_view_convergence_v2"
        || record.epoch_manifest_id != epoch_manifest.epoch_manifest_id
        || record.tenant_id != identity.tenant_id
        || record.program_id != identity.program_id
        || record.view_id != view_id
        || record.output_publication_protocol_id != OUTPUT_PUBLICATION_PROTOCOL_VERSION_V1
    {
        return Err(ApiError::bad_request(format!(
            "ingest epoch view convergence body/key mismatch at {key}"
        )));
    }
    Ok(())
}

pub(super) async fn persist_ingest_epoch_view_runtime_failure(
    state: &ApiState,
    epoch_manifest: &PersistedIngestEpochManifest,
    identity: &StandingProgramIdentity,
    view_id: &str,
    failure_reason: String,
    replay_checkpoints: Vec<ReplayCheckpoint>,
) -> Result<(), ApiError> {
    let key = ObjectKey::ingest_epoch_view_runtime_failure(
        &epoch_manifest.epoch_manifest_id,
        &identity.tenant_id,
        &identity.program_id,
        view_id,
    )
    .map_err(ApiError::bad_request)?;
    let record = IngestEpochViewRuntimeFailureRecord {
        schema_version: 1,
        record_kind: "ingest_epoch_view_runtime_failure_v1".to_string(),
        epoch_manifest_id: epoch_manifest.epoch_manifest_id.clone(),
        tenant_id: identity.tenant_id.clone(),
        program_id: identity.program_id.clone(),
        view_id: view_id.to_string(),
        failure_reason,
        replay_checkpoints,
    };
    let bytes =
        serde_json::to_vec(&record).map_err(|source| ApiError::internal(source.to_string()))?;
    let path = ObjectPath::from(key.as_str());
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
                    "ingest epoch view runtime failure conflict at {}",
                    key.as_str()
                )))
            }
        }
        Err(error) => Err(ApiError::internal(error)),
    }
}

pub(super) async fn read_ingest_epoch_view_runtime_failure(
    state: &ApiState,
    epoch_manifest: &PersistedIngestEpochManifest,
    identity: &StandingProgramIdentity,
    view_id: &str,
) -> Result<Option<IngestEpochViewRuntimeFailureRecord>, ApiError> {
    let key = ObjectKey::ingest_epoch_view_runtime_failure(
        &epoch_manifest.epoch_manifest_id,
        &identity.tenant_id,
        &identity.program_id,
        view_id,
    )
    .map_err(ApiError::bad_request)?;
    let path = ObjectPath::from(key.as_str());
    let bytes = match state.store.get(&path).await {
        Ok(result) => result.bytes().await.map_err(ApiError::internal)?,
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(error) => return Err(ApiError::internal(error)),
    };
    let record: IngestEpochViewRuntimeFailureRecord = serde_json::from_slice(&bytes)
        .map_err(|source| ApiError::bad_request(source.to_string()))?;
    validate_ingest_epoch_view_runtime_failure_record(
        &record,
        epoch_manifest,
        identity,
        view_id,
        key.as_str(),
    )?;
    Ok(Some(record))
}

pub(super) fn validate_ingest_epoch_view_runtime_failure_record(
    record: &IngestEpochViewRuntimeFailureRecord,
    epoch_manifest: &PersistedIngestEpochManifest,
    identity: &StandingProgramIdentity,
    view_id: &str,
    key: &str,
) -> Result<(), ApiError> {
    if record.schema_version != 1
        || record.record_kind != "ingest_epoch_view_runtime_failure_v1"
        || record.epoch_manifest_id != epoch_manifest.epoch_manifest_id
        || record.tenant_id != identity.tenant_id
        || record.program_id != identity.program_id
        || record.view_id != view_id
    {
        return Err(ApiError::bad_request(format!(
            "ingest epoch view runtime failure body/key mismatch at {key}"
        )));
    }
    Ok(())
}

pub(super) fn ingest_epoch_view_runtime_failure_error(
    epoch_manifest: &PersistedIngestEpochManifest,
    failure: &IngestEpochViewRuntimeFailureRecord,
) -> ApiError {
    ApiError::service_unavailable(format!(
        "standing runtime ingest epoch `{}` for view `{}` has a durable runtime failure marker and will not be retried automatically; repair the native standing runtime before replaying this epoch: {}",
        epoch_manifest.epoch_manifest_id, failure.view_id, failure.failure_reason
    ))
}

pub(super) async fn standing_runtime_create_requires_backfill(
    state: &ApiState,
    spec: &StandingViewSpec,
) -> Result<bool, ApiError> {
    if spec.input_relations.is_empty() {
        return Ok(false);
    }
    let ingest_log =
        IngestLog::new_catalog_checked(Arc::clone(&state.store), state.capabilities.as_ref())
            .map_err(ApiError::internal)?;
    let batches = ingest_log
        .replay_admitted_validated_envelopes_from(&[])
        .await
        .map_err(ApiError::internal)?;
    for batch in batches {
        let envelope =
            IngestEnvelope::decode(batch.payload().clone()).map_err(ApiError::bad_request)?;
        let header = envelope.header();
        if spec.input_relations.iter().any(|input| {
            header.relation_id == input.relation_id
                && header.relation_version == input.relation_version
                && header.schema_fingerprint == input.schema_fingerprint
        }) {
            return Ok(true);
        }
    }

    Ok(false)
}

pub(super) async fn ensure_standing_runtimes_for_ingest(
    state: &ApiState,
    request: &IngestRowsRequest,
) -> Result<(), ApiError> {
    let active_views = state
        .view_registry()?
        .list_active()
        .await
        .map_err(materialized_view_registry_error_to_api)?;
    let mut needs_restore = false;
    for active in &active_views {
        if !standing_runtime_can_accept_incremental_ingest(active) {
            continue;
        }
        let Some(identity) = active_standing_runtime_identity(active) else {
            continue;
        };
        if !view_uses_ingest_relation(active, request) {
            continue;
        }
        if state
            .standing_runtime(identity, &active.spec.view_id)?
            .is_none()
        {
            needs_restore = true;
            break;
        }
    }

    if needs_restore {
        state
            .restore_standing_program_runtimes_from_active_views()
            .await?;
    }

    for active in active_views {
        if !standing_runtime_can_accept_incremental_ingest(&active) {
            continue;
        }
        let Some(identity) = active_standing_runtime_identity(&active) else {
            continue;
        };
        if !view_uses_ingest_relation(&active, request) {
            continue;
        }
        if state
            .standing_runtime(identity, &active.spec.view_id)?
            .is_none()
        {
            return Err(ApiError::service_unavailable(format!(
                "standing runtime is unavailable for active view `{}`",
                active.spec.view_id
            )));
        }
    }

    Ok(())
}

pub(super) async fn preacquire_standing_runtime_owners_for_ingest(
    state: &ApiState,
    request: &IngestRowsRequest,
) -> Result<(), ApiError> {
    let active_views = state
        .view_registry()?
        .list_active()
        .await
        .map_err(materialized_view_registry_error_to_api)?;
    for active in active_views {
        if !standing_runtime_can_accept_incremental_ingest(&active) {
            continue;
        }
        let Some(identity) = active_standing_runtime_identity(&active) else {
            continue;
        };
        if !view_uses_ingest_relation(&active, request) {
            continue;
        }
        state
            .acquire_standing_runtime_owner(identity, &active.spec.view_id)
            .await?;
    }

    Ok(())
}

pub(super) async fn ensure_no_ingest_epoch_view_runtime_failures(
    state: &ApiState,
    epoch_manifest: &PersistedIngestEpochManifest,
    prepared_batches: &[PreparedIngestBatch],
) -> Result<(), ApiError> {
    let active_views = state
        .view_registry()?
        .list_active()
        .await
        .map_err(materialized_view_registry_error_to_api)?;
    for active in active_views {
        if !standing_runtime_can_accept_incremental_ingest(&active) {
            continue;
        }
        let Some(identity) = active_standing_runtime_identity(&active) else {
            continue;
        };
        if !prepared_batches
            .iter()
            .any(|prepared| view_uses_prepared_ingest_batch(&active, prepared))
        {
            continue;
        }
        if read_ingest_epoch_view_convergence(state, epoch_manifest, identity, &active.spec.view_id)
            .await?
            .is_some()
        {
            continue;
        }
        if let Some(failure) = read_ingest_epoch_view_runtime_failure(
            state,
            epoch_manifest,
            identity,
            &active.spec.view_id,
        )
        .await?
        {
            return Err(ingest_epoch_view_runtime_failure_error(
                epoch_manifest,
                &failure,
            ));
        }
    }

    Ok(())
}

pub(super) async fn apply_standing_runtime_ingest_epoch(
    state: &ApiState,
    epoch_manifest: &PersistedIngestEpochManifest,
    prepared_batches: &[PreparedIngestBatch],
    timer: Option<&mut IngestTimer>,
) -> Result<PreparedStandingRuntimeApplySummary, ApiError> {
    apply_standing_runtime_prepared_ingests(state, Some(epoch_manifest), prepared_batches, timer)
        .await
}

pub(super) async fn apply_standing_runtime_prepared_ingests(
    state: &ApiState,
    epoch_manifest: Option<&PersistedIngestEpochManifest>,
    prepared_batches: &[PreparedIngestBatch],
    mut timer: Option<&mut IngestTimer>,
) -> Result<PreparedStandingRuntimeApplySummary, ApiError> {
    state.validate_standing_runtime_fencing_or_evict().await?;
    let active_views = state
        .view_registry()?
        .list_active()
        .await
        .map_err(materialized_view_registry_error_to_api)?;
    let mut summary = PreparedStandingRuntimeApplySummary::default();
    for active in active_views {
        if !standing_runtime_can_accept_incremental_ingest(&active) {
            continue;
        }
        let Some(identity) = active_standing_runtime_identity(&active) else {
            continue;
        };
        let matching_prepared_batches = prepared_batches
            .iter()
            .filter(|prepared| view_uses_prepared_ingest_batch(&active, prepared))
            .collect::<Vec<_>>();
        if matching_prepared_batches.is_empty() {
            continue;
        }
        summary.active_views += 1;
        if let Some(epoch_manifest) = epoch_manifest {
            if let Some(convergence) = read_ingest_epoch_view_convergence(
                state,
                epoch_manifest,
                identity,
                &active.spec.view_id,
            )
            .await?
            {
                record_materialized_through(
                    &mut summary.materialized_through,
                    convergence.logical_epoch,
                );
                continue;
            }
            if let Some(failure) = read_ingest_epoch_view_runtime_failure(
                state,
                epoch_manifest,
                identity,
                &active.spec.view_id,
            )
            .await?
            {
                return Err(ingest_epoch_view_runtime_failure_error(
                    epoch_manifest,
                    &failure,
                ));
            }
        }
        let epoch_replay_checkpoints = matching_prepared_batches
            .iter()
            .copied()
            .map(replay_checkpoint_from_prepared_ingest)
            .collect::<Vec<_>>();
        let operation_lock =
            state.standing_runtime_operation_lock(identity, &active.spec.view_id)?;
        let _operation_guard = operation_lock.lock().await;
        let mut uncovered_prepared_batches = matching_prepared_batches.clone();
        let previous_checkpoint =
            read_latest_standing_runtime_checkpoint(state, identity, &active.spec.view_id).await?;
        if let Some(timer) = timer.as_mut() {
            timer.mark("materialize_checkpoint_lookup");
        }
        if let Some(latest_checkpoint) = &previous_checkpoint {
            let replay_plan = standing_runtime_replay_plan_from_record_ref(latest_checkpoint);
            if prepared_batches_are_covered_by_replay_plan(&replay_plan, &matching_prepared_batches)
            {
                if let Some(epoch_manifest) = epoch_manifest {
                    persist_ingest_epoch_view_convergence(
                        state,
                        epoch_manifest,
                        &active.spec.view_id,
                        &latest_checkpoint.checkpoint,
                        latest_checkpoint.checkpoint.output_manifest_refs.clone(),
                        epoch_replay_checkpoints,
                    )
                    .await?;
                }
                record_materialized_through(
                    &mut summary.materialized_through,
                    latest_checkpoint.checkpoint.logical_epoch,
                );
                continue;
            }
            uncovered_prepared_batches.retain(|prepared| {
                !prepared_batch_is_covered_by_replay_plan(&replay_plan, prepared)
            });
        }
        let input_batches = uncovered_prepared_batches
            .iter()
            .copied()
            .map(relation_input_batch_from_prepared_ingest)
            .collect::<Vec<_>>();
        if input_batches.is_empty() {
            continue;
        }
        let runtime = state
            .standing_runtime(identity, &active.spec.view_id)?
            .ok_or_else(|| {
                ApiError::service_unavailable(format!(
                    "standing runtime disappeared for active view `{}`",
                    active.spec.view_id
                ))
            })?;
        let owner = state
            .acquire_standing_runtime_owner(identity, &active.spec.view_id)
            .await?;
        let idempotency_key = epoch_ingest_idempotency_key(
            &active.spec.view_id,
            uncovered_prepared_batches.iter().copied(),
        )
        .map_err(ApiError::bad_request)?;
        let lower_bound_epoch = uncovered_prepared_batches
            .iter()
            .map(|prepared| prepared.end_offset_exclusive)
            .max()
            .unwrap_or(0);
        let apply_result = apply_standing_runtime_changes_and_checkpoint_many(
            Arc::clone(&runtime),
            lower_bound_epoch,
            idempotency_key,
            input_batches,
            StandingRuntimeBudgetLimits::from_state(state),
        )
        .await;
        if let Some(timer) = timer.as_mut() {
            if epoch_manifest.is_some() {
                timer.mark("materialize_apply");
            } else {
                timer.mark("materialize_direct_apply");
            }
        }
        let apply_result = match apply_result {
            Ok(apply_result) => apply_result,
            Err(error) => {
                if let Some(epoch_manifest) = epoch_manifest {
                    persist_ingest_epoch_view_runtime_failure(
                        state,
                        epoch_manifest,
                        identity,
                        &active.spec.view_id,
                        error.message.clone(),
                        epoch_replay_checkpoints.clone(),
                    )
                    .await?;
                }
                remove_standing_runtime(state, identity, &active.spec.view_id)?;
                return Err(error);
            }
        };
        let checkpoint_write_summary = match persist_standing_runtime_checkpoint(
            state,
            &active.spec.view_id,
            &apply_result.checkpoint,
            &apply_result.output_deltas,
            StandingRuntimeCheckpointPersistContext::new(
                previous_checkpoint,
                epoch_replay_checkpoints.clone(),
                owner,
            )
            .with_published_relation(published_relation_binding_for_active_view(&active)?),
            timer.as_deref_mut(),
        )
        .await
        {
            Ok(summary) => summary,
            Err(error) => {
                remove_standing_runtime(state, identity, &active.spec.view_id)?;
                return Err(error);
            }
        };
        if let Some(timer) = timer.as_mut() {
            timer.mark("materialize_checkpoint");
        }
        summary.applied_batches += uncovered_prepared_batches.len();
        summary.checkpoint_writes += 1;
        summary.output_delta_writes += checkpoint_write_summary.output_delta_writes;
        summary.state_payload_writes += checkpoint_write_summary.state_payload_writes;
        summary.checkpoint_record_writes += checkpoint_write_summary.checkpoint_record_writes;
        summary.checkpoint_pointer_writes += checkpoint_write_summary.checkpoint_pointer_writes;
        summary.compaction_scheduled += checkpoint_write_summary.compaction_scheduled;
        record_materialized_through(
            &mut summary.materialized_through,
            apply_result.checkpoint.logical_epoch,
        );
        if let Some(epoch_manifest) = epoch_manifest {
            persist_ingest_epoch_view_convergence(
                state,
                epoch_manifest,
                &active.spec.view_id,
                &apply_result.checkpoint,
                checkpoint_write_summary.output_refs.clone(),
                epoch_replay_checkpoints,
            )
            .await?;
            summary.convergence_writes += 1;
            if let Some(timer) = timer.as_mut() {
                timer.mark("materialize_checkpoint_publication");
            }
        }
    }

    Ok(summary)
}

pub(super) fn view_uses_ingest_relation(
    active: &ActiveMaterializedView,
    request: &IngestRowsRequest,
) -> bool {
    active.spec.input_relations.iter().any(|input| {
        input.relation_id == request.relation_id
            && input.relation_version == request.relation_version
    })
}

pub(super) fn view_uses_prepared_ingest_batch(
    active: &ActiveMaterializedView,
    prepared: &PreparedIngestBatch,
) -> bool {
    active.spec.input_relations.iter().any(|input| {
        input.relation_id == prepared.request.relation_id
            && input.relation_version == prepared.request.relation_version
            && input.schema_fingerprint == prepared.catalog.schema_fingerprint.as_str()
    })
}

pub(super) fn relation_input_batch_from_prepared_ingest(
    prepared: &PreparedIngestBatch,
) -> RelationInputBatch {
    RelationInputBatch {
        encoding: RelationInputEncodingV1::SourceRelationV1,
        relation_id: prepared.request.relation_id.clone(),
        relation_version: prepared.request.relation_version.clone(),
        stream_id: prepared.request.stream_id.clone(),
        partition_id: prepared.request.partition_id,
        schema_fingerprint: prepared.catalog.schema_fingerprint.as_str().to_string(),
        start_offset_inclusive: prepared.request.start_offset_inclusive,
        end_offset_exclusive: prepared.end_offset_exclusive,
        event_time_watermark: prepared.event_time_watermark.clone(),
        batches: vec![prepared.record_batch.clone()],
    }
}

pub(super) fn replay_checkpoint_from_prepared_ingest(
    prepared: &PreparedIngestBatch,
) -> ReplayCheckpoint {
    ReplayCheckpoint::for_relation(
        prepared.request.relation_id.clone(),
        prepared.request.relation_version.clone(),
        prepared.request.stream_id.clone(),
        prepared.request.partition_id,
        prepared.end_offset_exclusive,
    )
}

pub(super) fn epoch_ingest_idempotency_key<'a>(
    view_id: &str,
    prepared_batches: impl IntoIterator<Item = &'a PreparedIngestBatch>,
) -> Result<EpochIdempotencyKey, StandingProgramRuntimeError> {
    let mut parts = prepared_batches
        .into_iter()
        .map(|prepared| {
            format!(
                "{}\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{}",
                prepared.request.relation_id,
                prepared.request.relation_version,
                prepared.catalog.schema_fingerprint.as_str(),
                prepared.request.stream_id,
                prepared.request.partition_id,
                prepared.request.start_offset_inclusive,
                prepared.end_offset_exclusive,
                prepared.payload_digest
            )
        })
        .collect::<Vec<_>>();
    parts.sort();
    let mut hasher = Sha256::new();
    hasher.update(b"velorix.ingest-epoch.runtime-idempotency.v1\0");
    hasher.update(view_id.as_bytes());
    for part in parts {
        hasher.update(b"\0");
        hasher.update(part.as_bytes());
    }
    EpochIdempotencyKey::new(format!("epoch:sha256:{:x}", hasher.finalize()))
}

pub(super) fn next_standing_runtime_logical_epoch(
    runtime: &(dyn StandingProgramRuntime + Send),
    lower_bound: u64,
) -> Result<u64, ApiError> {
    let next = runtime
        .logical_epoch()
        .checked_add(1)
        .ok_or_else(|| ApiError::bad_request("standing runtime logical epoch overflow"))?;
    Ok(next.max(lower_bound))
}

pub(super) async fn apply_standing_runtime_changes_and_checkpoint(
    runtime: SharedStandingRuntime,
    lower_bound_epoch: u64,
    idempotency_key: EpochIdempotencyKey,
    input_batch: RelationInputBatch,
    budget_limits: StandingRuntimeBudgetLimits,
) -> Result<StandingRuntimeApplyResult, ApiError> {
    apply_standing_runtime_changes_and_checkpoint_many(
        runtime,
        lower_bound_epoch,
        idempotency_key,
        vec![input_batch],
        budget_limits,
    )
    .await
}

#[derive(Clone, Copy, Debug)]
pub(super) struct StandingRuntimeBudgetLimits {
    pub(super) max_output_delta_records: usize,
    pub(super) max_state_payload_bytes: usize,
}

impl StandingRuntimeBudgetLimits {
    pub(super) fn from_state(state: &ApiState) -> Self {
        Self {
            max_output_delta_records: state.max_standing_runtime_output_delta_records,
            max_state_payload_bytes: state.max_standing_runtime_state_payload_bytes,
        }
    }
}

pub(super) async fn apply_standing_runtime_changes_and_checkpoint_many(
    runtime: SharedStandingRuntime,
    lower_bound_epoch: u64,
    idempotency_key: EpochIdempotencyKey,
    input_batches: Vec<RelationInputBatch>,
    budget_limits: StandingRuntimeBudgetLimits,
) -> Result<StandingRuntimeApplyResult, ApiError> {
    tokio::task::spawn_blocking(move || {
        let mut runtime = runtime
            .lock()
            .map_err(|_| ApiError::internal("standing runtime lock poisoned"))?;
        let logical_epoch =
            next_standing_runtime_logical_epoch(runtime.as_ref(), lower_bound_epoch)?;
        let before = runtime.checkpoint().map_err(ApiError::bad_request)?;
        let commit = match runtime.apply_changes(logical_epoch, idempotency_key, input_batches) {
            Ok(commit) => commit,
            Err(error) => {
                *runtime = velorix_runtime::materialized_view_runtime::restore_standing_runtime(
                    before,
                )
                .map_err(|restore| {
                    ApiError::internal(format!(
                        "standing runtime apply failed with {error}; rollback failed with {restore}"
                    ))
                })?;
                return Err(ApiError::bad_request(error));
            }
        };
        let checkpoint = runtime.checkpoint().map_err(ApiError::bad_request)?;
        if let Err(error) =
            validate_standing_runtime_budget(&commit.output_deltas, &checkpoint, budget_limits)
        {
            *runtime = velorix_runtime::materialized_view_runtime::restore_standing_runtime(before)
                .map_err(|restore| {
                    ApiError::internal(format!(
                        "standing runtime budget rejected the epoch with {error}; rollback failed with {restore}"
                    ))
                })?;
            return Err(error);
        }
        Ok(StandingRuntimeApplyResult {
            checkpoint,
            output_deltas: commit.output_deltas,
        })
    })
    .await
    .map_err(ApiError::internal)?
}

pub(super) fn validate_standing_runtime_budget(
    output_deltas: &[ViewOutputDelta],
    checkpoint: &RuntimeCheckpoint,
    limits: StandingRuntimeBudgetLimits,
) -> Result<(), ApiError> {
    let output_delta_records = output_deltas
        .iter()
        .map(|delta| delta.delta.records().len())
        .sum::<usize>();
    if output_delta_records > limits.max_output_delta_records {
        return Err(ApiError::payload_too_large(format!(
            "standing runtime output delta record count {output_delta_records} exceeds configured limit {}",
            limits.max_output_delta_records
        )));
    }
    let state_payload_bytes = checkpoint
        .state_payload
        .as_ref()
        .map(|payload| payload.payload.len())
        .unwrap_or(0);
    if state_payload_bytes > limits.max_state_payload_bytes {
        return Err(ApiError::payload_too_large(format!(
            "standing runtime checkpoint state payload size {state_payload_bytes} exceeds configured limit {}",
            limits.max_state_payload_bytes
        )));
    }
    Ok(())
}

pub(super) async fn reserve_ingest_range(
    state: &ApiState,
    request: &IngestRowsRequest,
    catalog: &VelorixRelationCatalogV1,
    end_offset_exclusive: u64,
    envelope: &bytes::Bytes,
) -> Result<(), ApiError> {
    let meta_store = state
        .meta_store
        .as_ref()
        .ok_or_else(|| ApiError::internal("metadata store is not configured"))?;
    let reservation = ingest_range_reservation(request, catalog, end_offset_exclusive, envelope)?;
    let outcome = meta_store
        .reserve_ingest_range(reservation)
        .await
        .map_err(meta_error_to_api)?;

    match outcome {
        ReserveIngestRangeOutcome::Reserved | ReserveIngestRangeOutcome::Duplicate => Ok(()),
        ReserveIngestRangeOutcome::Conflict => Err(ApiError::conflict(format!(
            "ingest range conflict from metadata service for stream={} partition={} offsets={}-{}",
            request.stream_id,
            request.partition_id,
            request.start_offset_inclusive,
            end_offset_exclusive
        ))),
    }
}

async fn commit_ingest_range(
    state: &ApiState,
    request: &IngestRowsRequest,
    catalog: &VelorixRelationCatalogV1,
    end_offset_exclusive: u64,
    envelope: &bytes::Bytes,
) -> Result<(), ApiError> {
    let Some(meta_store) = state.meta_store.as_ref() else {
        return Ok(());
    };
    let reservation = ingest_range_reservation(request, catalog, end_offset_exclusive, envelope)?;
    match meta_store
        .commit_ingest_range(reservation)
        .await
        .map_err(meta_error_to_api)?
    {
        CommitIngestRangeOutcome::Committed | CommitIngestRangeOutcome::Duplicate => Ok(()),
        CommitIngestRangeOutcome::Conflict => Err(ApiError::conflict(format!(
            "ingest commit conflict from metadata service for stream={} partition={} offsets={}-{}",
            request.stream_id,
            request.partition_id,
            request.start_offset_inclusive,
            end_offset_exclusive
        ))),
    }
}

fn ingest_range_reservation(
    request: &IngestRowsRequest,
    catalog: &VelorixRelationCatalogV1,
    end_offset_exclusive: u64,
    envelope: &bytes::Bytes,
) -> Result<IngestRangeReservation, ApiError> {
    let header = IngestEnvelope::decode(envelope.clone())
        .map_err(ApiError::bad_request)?
        .header()
        .clone();
    let batch_key = ObjectKey::ingest_batch(
        &request.stream_id,
        request.partition_id,
        request.start_offset_inclusive,
        end_offset_exclusive,
    )
    .map_err(ApiError::bad_request)?;
    Ok(IngestRangeReservation {
        stream_id: request.stream_id.clone(),
        partition_id: request.partition_id,
        start_offset_inclusive: request.start_offset_inclusive,
        end_offset_exclusive,
        batch_key: batch_key.as_str().to_string(),
        payload_digest: header.payload_digest,
        relation_id: request.relation_id.clone(),
        relation_version: request.relation_version.clone(),
        schema_fingerprint: catalog.schema_fingerprint.as_str().to_string(),
        writer_epoch: 0,
    })
}

pub(super) async fn append_ingest_envelope(
    state: &ApiState,
    envelope: bytes::Bytes,
) -> Result<AppendValidatedEnvelopeOutcome, ApiError> {
    if state.meta_store.is_some() {
        state
            .ingest_writer
            .append_validated_envelope_after_external_admission(envelope)
            .await
            .map_err(ApiError::internal)
    } else {
        state
            .ingest_writer
            .append_catalog_validated_envelope(envelope)
            .await
            .map_err(ApiError::internal)
    }
}
