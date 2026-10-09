use super::*;

pub(super) async fn prepare_event_time_state(
    state: &ApiState,
    runtime: &SharedStandingRuntime,
    inputs: &[RelationInputBatch],
) -> Result<(), ApiError> {
    let requests = {
        let runtime = runtime
            .lock()
            .map_err(|_| ApiError::internal("standing runtime lock poisoned"))?;
        runtime.event_time_state_requests(inputs).map_err(|error| match error {
            StandingProgramRuntimeError::InvalidProgramIdentity { field: "window_correction_watermark_regression" } =>
                ApiError::bad_request("window correction watermark cannot move backwards; the entire ingest epoch was rejected before source publication"),
            error => ApiError::bad_request(error),
        })?
    };
    if requests.is_empty() {
        return Ok(());
    }
    let history = if requests.iter().any(|request| request.reference.is_none()) {
        let checkpoint = runtime
            .lock()
            .map_err(|_| ApiError::internal("standing runtime lock poisoned"))?
            .checkpoint()
            .map_err(ApiError::bad_request)?;
        Some(retained_event_time_inputs(state, &checkpoint, &requests).await?)
    } else {
        None
    };
    for request in requests {
        if let Some(reference) = &request.reference {
            let path = validate_event_time_state_reference(reference)?;
            let bytes = state
                .store
                .get(&path)
                .await
                .map_err(|error| {
                    ApiError::service_unavailable(format!(
                        "retained window state unavailable at {}: {error}",
                        reference.state_root.object_key
                    ))
                })?
                .bytes()
                .await
                .map_err(ApiError::internal)?;
            if stable_bytes_hash(&bytes) != reference.state_root.content_hash {
                return Err(ApiError::bad_request(
                    "retained window state digest mismatch",
                ));
            }
            let payload = std::str::from_utf8(&bytes).map_err(ApiError::bad_request)?;
            runtime
                .lock()
                .map_err(|_| ApiError::internal("standing runtime lock poisoned"))?
                .hydrate_event_time_state(&request, payload)
                .map_err(ApiError::bad_request)?;
        } else {
            runtime
                .lock()
                .map_err(|_| ApiError::internal("standing runtime lock poisoned"))?
                .reconstruct_event_time_state(
                    &request,
                    history.as_deref().expect("history requested"),
                )
                .map_err(|error| {
                    ApiError::service_unavailable(format!(
                        "retained input history cannot reconstruct window {}: {error}",
                        request.window_key
                    ))
                })?;
        }
    }
    Ok(())
}

fn validate_event_time_state_reference(
    reference: &EventTimeStateRef,
) -> Result<ObjectPath, ApiError> {
    if reference.window_key.is_empty()
        || !reference
            .state_root
            .content_hash
            .strip_prefix("sha256:")
            .is_some_and(|hash| {
                hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit())
            })
    {
        return Err(ApiError::bad_request(
            "invalid retained window state reference",
        ));
    }
    let path =
        ObjectPath::parse(&reference.state_root.object_key).map_err(ApiError::bad_request)?;
    if path.as_ref() != reference.state_root.object_key {
        return Err(ApiError::bad_request(
            "noncanonical retained window state key",
        ));
    }
    Ok(path)
}

pub(super) async fn persist_event_time_state(
    state: &ApiState,
    runtime: &SharedStandingRuntime,
) -> Result<Vec<EventTimeStateRef>, ApiError> {
    let mut references = Vec::new();
    loop {
        let objects = runtime
            .lock()
            .map_err(|_| ApiError::internal("standing runtime lock poisoned"))?
            .export_event_time_state()
            .map_err(ApiError::bad_request)?;
        if objects.is_empty() {
            break;
        }
        for object in &objects {
            persist_event_time_state_object(state, object).await?;
        }
        let chunk = objects
            .into_iter()
            .map(|object| object.reference)
            .collect::<Vec<_>>();
        runtime
            .lock()
            .map_err(|_| ApiError::internal("standing runtime lock poisoned"))?
            .stage_event_time_state(&chunk)
            .map_err(ApiError::bad_request)?;
        references.extend(chunk);
    }
    Ok(references)
}

pub(super) async fn persist_event_time_state_object(
    state: &ApiState,
    object: &EventTimeStateObject,
) -> Result<(), ApiError> {
    let path = validate_event_time_state_reference(&object.reference)?;
    if stable_bytes_hash(object.payload.as_bytes()) != object.reference.state_root.content_hash {
        return Err(ApiError::bad_request(
            "retained window state export digest mismatch",
        ));
    }
    match state
        .store
        .put_opts(
            &path,
            bytes::Bytes::from(object.payload.clone()).into(),
            PutMode::Create.into(),
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(object_store::Error::AlreadyExists { .. }) => {
            let existing = state
                .store
                .get(&path)
                .await
                .map_err(ApiError::retryable_materialization_io)?
                .bytes()
                .await
                .map_err(ApiError::retryable_materialization_io)?;
            if existing.as_ref() != object.payload.as_bytes() {
                return Err(ApiError::conflict(format!(
                    "retained window state conflict at {path}"
                )));
            }
            Ok(())
        }
        Err(error) => Err(ApiError::retryable_materialization_io(error)),
    }
}

async fn retained_event_time_inputs(
    state: &ApiState,
    checkpoint: &RuntimeCheckpoint,
    requests: &[velorix_core::standing_program::EventTimeStateRequest],
) -> Result<Vec<RelationInputBatch>, ApiError> {
    let view_id = checkpoint
        .identity
        .view_ids
        .first()
        .ok_or_else(|| ApiError::bad_request("window runtime has no view identity"))?;
    let active = state
        .view_registry()?
        .read_active(view_id)
        .await
        .map_err(materialized_view_registry_error_to_api)?;
    let Some(VelorixLogicalViewExecutionV1::TumblingEventTimeAggregate { plan }) = active
        .runtime
        .as_ref()
        .and_then(|binding| binding.logical_plan.as_ref())
        .map(|logical| &logical.execution)
    else {
        return Err(ApiError::bad_request(
            "retained window reconstruction requires an admitted fixed-window plan",
        ));
    };
    let catalog = read_relation_catalog(
        state,
        &plan.input_relation_id,
        &active
            .spec
            .input_relations
            .first()
            .ok_or_else(|| ApiError::bad_request("retained window view has no input schema"))?
            .relation_version,
    )
    .await?;
    let key_name = &catalog
        .relation_schema
        .columns
        .iter()
        .find(|column| column.column_id == plan.group_key_column_id)
        .ok_or_else(|| ApiError::bad_request("retained window group key is absent from catalog"))?
        .name;
    let event_name = &catalog
        .relation_schema
        .columns
        .iter()
        .find(|column| column.column_id == plan.event_time_column_id)
        .ok_or_else(|| ApiError::bad_request("retained window event time is absent from catalog"))?
        .name;
    let mut affected = Vec::new();
    for request in requests
        .iter()
        .filter(|request| request.reference.is_none())
    {
        let key: Value =
            serde_json::from_str(&request.window_key).map_err(ApiError::bad_request)?;
        let (Some(group), Some(start), Some(end)) = (
            key.get(0),
            key.get(1).and_then(Value::as_i64),
            key.get(2).and_then(Value::as_i64),
        ) else {
            return Err(ApiError::bad_request(
                "invalid retained window reconstruction key",
            ));
        };
        affected.push((group.clone(), start, end));
    }
    let replay_plan = StandingRuntimeReplayPlan {
        input_frontiers: checkpoint.input_frontiers.clone(),
        ..Default::default()
    };
    // ponytail: legacy checkpoints lack a window input index; scan retained envelopes only during migration.
    let batches = recovery::read_replay_ingest_batches(state, &active, &replay_plan, None)
        .await
        .map_err(|error| {
            ApiError::service_unavailable(format!(
                "retained window input history unavailable: {error}"
            ))
        })?;
    let mut inputs = Vec::new();
    let mut covered = BTreeMap::new();
    for batch in batches {
        let envelope =
            IngestEnvelope::decode(batch.payload().clone()).map_err(ApiError::bad_request)?;
        let header = envelope.header();
        let Some(frontier) = checkpoint.input_frontiers.iter().find(|frontier| {
            frontier.relation_id == header.relation_id
                && frontier.relation_version == header.relation_version
                && frontier.stream_id == header.stream_id
                && frontier.partition_id == header.partition_id
        }) else {
            continue;
        };
        if header.start_offset_inclusive >= frontier.committed_offset_exclusive {
            continue;
        }
        let key = (
            header.relation_id.clone(),
            header.relation_version.clone(),
            header.stream_id.clone(),
            header.partition_id,
        );
        let next = covered.entry(key).or_insert(0);
        if header.start_offset_inclusive != *next
            || header.end_offset_exclusive > frontier.committed_offset_exclusive
        {
            return Err(ApiError::service_unavailable(
                "retained window input history has a gap, overlap, or incompatible checkpoint cut",
            ));
        }
        if !active.spec.input_relations.iter().any(|input| {
            input.relation_id == header.relation_id
                && input.relation_version == header.relation_version
                && input.schema_fingerprint == header.schema_fingerprint
        }) {
            return Err(ApiError::bad_request(
                "retained window input history schema mismatch",
            ));
        }
        *next = header.end_offset_exclusive;
        let mut selected_batches = Vec::new();
        for batch in envelope.record_batches().map_err(ApiError::bad_request)? {
            let key_column = batch.column_by_name(key_name).ok_or_else(|| {
                ApiError::bad_request("retained input group key column is missing")
            })?;
            let event_column = batch.column_by_name(event_name).ok_or_else(|| {
                ApiError::bad_request("retained input event-time column is missing")
            })?;
            let mut selected = Vec::with_capacity(batch.num_rows());
            for row in 0..batch.num_rows() {
                if event_column.is_null(row) {
                    return Err(ApiError::bad_request(
                        "retained window input event time cannot be null",
                    ));
                }
                let event = if let Some(times) = event_column.as_any().downcast_ref::<Int64Array>()
                {
                    times.value(row)
                } else if let Some(times) = event_column
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                {
                    times.value(row)
                } else if let Some(days) = event_column.as_any().downcast_ref::<Date32Array>() {
                    i64::from(days.value(row))
                        .checked_mul(86_400_000_000_000)
                        .ok_or_else(|| {
                            ApiError::bad_request(
                                "retained window event date exceeds nanosecond range",
                            )
                        })?
                } else {
                    return Err(ApiError::bad_request(
                        "unsupported retained event-time Arrow type",
                    ));
                };
                let group = arrow_value_to_json(key_column, row)?;
                selected.push(
                    affected
                        .iter()
                        .any(|(key, start, end)| *key == group && *start <= event && event < *end),
                );
            }
            let filtered =
                arrow::compute::filter_record_batch(&batch, &BooleanArray::from(selected))
                    .map_err(ApiError::bad_request)?;
            if filtered.num_rows() > 0 {
                selected_batches.push(filtered)
            }
        }
        inputs.push(RelationInputBatch {
            encoding: RelationInputEncodingV1::SourceRelationV1,
            relation_id: header.relation_id.clone(),
            relation_version: header.relation_version.clone(),
            stream_id: header.stream_id.clone(),
            partition_id: header.partition_id,
            schema_fingerprint: header.schema_fingerprint.clone(),
            start_offset_inclusive: header.start_offset_inclusive,
            end_offset_exclusive: header.end_offset_exclusive,
            event_time_watermark: header.event_time_watermark.clone(),
            batches: selected_batches,
        });
    }
    for frontier in &checkpoint.input_frontiers {
        let key = (
            frontier.relation_id.clone(),
            frontier.relation_version.clone(),
            frontier.stream_id.clone(),
            frontier.partition_id,
        );
        if covered.get(&key).copied().unwrap_or(0) != frontier.committed_offset_exclusive {
            return Err(ApiError::service_unavailable(
                "retained window input history is incomplete at the checkpoint frontier",
            ));
        }
    }
    Ok(inputs)
}
