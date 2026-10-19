//! Sparse execution of authenticated producer pages and signed commits.
use super::*;
use crate::demand_published::{
    capture_demand_published_artifacts, demand_published_artifact_key,
    load_demand_published_artifact, PositiveBaselinePageGroup,
};

struct PositiveBaselineCommitContext<'a> {
    state: &'a ApiState,
    active: &'a ActiveMaterializedView,
    identity: &'a StandingProgramIdentity,
    bootstrap: &'a velorix_control::meta_admin::PublishedInputBootstrapProgressV1,
}

async fn flush_positive_baseline_group(
    context: &PositiveBaselineCommitContext<'_>,
    previous: &mut Option<StandingRuntimeCheckpointRecord>,
    progress: &DemandCheckpoint,
    inputs: &mut Vec<RelationInputBatch>,
    guard: tokio::sync::OwnedMutexGuard<()>,
) -> Result<tokio::sync::OwnedMutexGuard<()>, ApiError> {
    if inputs.is_empty() {
        return Ok(guard);
    }
    let guard = commit_demand_unit_locked(
        context.state,
        context.active,
        previous.take(),
        progress.clone(),
        std::mem::take(inputs),
        DemandPublication {
            bootstrap: Some(context.bootstrap.clone()),
            ..DemandPublication::default()
        },
        guard,
    )
    .await?;
    *previous = checkpoint_publication::read_latest_standing_runtime_checkpoint_header(
        context.state,
        context.identity,
        &context.active.spec.view_id,
    )
    .await?;
    Ok(guard)
}

pub(super) async fn materialize_published_demand(
    state: &ApiState,
    active: &ActiveMaterializedView,
    request: &DemandRequest,
    query_priority: bool,
) -> Result<DemandOutcome, ApiError> {
    request.budget.validate()?;
    let identity = active_standing_runtime_identity(active)
        .ok_or_else(|| ApiError::bad_request("published demand runtime missing"))?;
    let operation = state.standing_runtime_operation_lock(identity, &active.spec.view_id)?;
    let mut guard = operation.lock_owned().await;
    let (mut previous, migration_bytes) =
        migration::read_checkpoint(state, active, request.budget.max_bytes).await?;
    let inputs = if previous
        .as_ref()
        .is_some_and(|r| r.demand_materialization.is_none())
    {
        // Legacy declarations must already retain their frozen baseline. The
        // budgeted inventory below authenticates it; do not recapture today's
        // head or perform unaccounted header reads to fill missing evidence.
        view_dependencies::persisted_consumer_bootstrap_inputs(active)?
    } else {
        view_dependencies::resolve_consumer_bootstrap_inputs_locked(
            state,
            active,
            previous.as_ref(),
        )
        .await?
    };
    let published = published_relation_binding_for_active_view(active)?
        .ok_or_else(|| ApiError::bad_request("consumer published binding missing"))?;
    let (artifacts, inventory_bytes) = capture_demand_published_artifacts(
        state,
        &inputs,
        request.budget.max_bytes.saturating_sub(migration_bytes),
        previous.as_ref(),
        &published,
    )
    .await?;
    let mut read_bytes = migration_bytes
        .checked_add(inventory_bytes)
        .ok_or_else(|| ApiError::bad_request("migration read byte overflow"))?;
    migration::validate_published_prefixes(active, previous.as_ref(), &artifacts)?;
    let mut progress = previous
        .as_ref()
        .and_then(|record| record.demand_materialization.clone())
        .unwrap_or_else(|| DemandCheckpoint {
            schema_version: 1,
            spec_hash: active.spec_hash.clone(),
            plan_hash: published.producer_plan_hash.clone(),
            output_schema_hash: identity.output_schema_hash.clone(),
            paused: false,
            order: DemandOrder::Forward,
            artifacts: BTreeMap::new(),
            certificates: Vec::new(),
            full_complete: false,
            event_time_replay_id: None,
            event_time_output_readiness: Vec::new(),
        });
    if progress.spec_hash != active.spec_hash || progress.plan_hash != published.producer_plan_hash
    {
        return Err(ApiError::conflict(
            "published demand checkpoint identity mismatch",
        ));
    }
    let mut heads = BTreeMap::new();
    let mut keyed = Vec::new();
    let mut covered = BTreeSet::new();
    for artifact in &artifacts {
        let key = demand_published_artifact_key(artifact)?;
        if previous.as_ref().is_some_and(|record| {
            record.checkpoint.input_frontiers.iter().any(|frontier| {
                frontier.relation_id == artifact.binding.relation.relation_id
                    && frontier.relation_version == artifact.binding.relation.relation_version
                    && frontier.stream_id == artifact.binding.output_stream_id
                    && frontier.committed_offset_exclusive >= artifact.cursor.output_epoch
            })
        }) {
            covered.insert(key.clone());
        }
        heads.insert(artifact.edge.edge_id.clone(), artifact.cursor.clone());
        keyed.push((key, artifact));
    }
    if heads.len() != inputs.len() {
        return Err(ApiError::bad_request(
            "published inventory is missing an input edge",
        ));
    }
    let cut_hash = stable_bytes_hash(&serde_json::to_vec(&heads).map_err(ApiError::internal)?);
    let scope_sql = request.scope.as_ref().map(|scope| scope.predicate.as_str());
    let requested_scope = scope_hash(scope_sql)?;
    if progress.certificates.iter().any(|certificate| {
        certificate.scope_hash == requested_scope && certificate.captured_cut_hash == cut_hash
    }) && keyed.iter().all(|(key, _)| {
        covered.contains(key)
            || progress
                .artifacts
                .get(key)
                .is_some_and(|entry| entry.artifact_hash == *key && entry.applied.is_complete())
    }) {
        return Ok(DemandOutcome {
            checkpoint_epoch: previous
                .as_ref()
                .map(|record| record.checkpoint.logical_epoch),
            work_units: 0,
            status: "complete",
            complete: true,
            processed_rows: 0,
            read_bytes,
            remaining_rows: 0,
            dependency_mode: "full_closure",
        });
    }
    let scan_scope = scope_hash(None)?;
    let mut bootstrap = match previous
        .as_ref()
        .and_then(|record| record.published_input_bootstrap.clone())
    {
        Some(progress) => progress,
        None => {
            let mut entries = Vec::new();
            for input in &inputs {
                let summary = artifacts
                    .iter()
                    .filter(|artifact| artifact.edge.edge_id == input.edge.edge_id)
                    .find_map(|artifact| artifact.baseline_manifest_summary())
                    .ok_or_else(|| {
                        ApiError::bad_request("published baseline manifest summary missing")
                    })?;
                entries.push(
                    velorix_control::meta_admin::PublishedInputBootstrapCursorV1 {
                        input_edge: input.edge.edge_id.clone(),
                        captured_cursor: input.bootstrap_cursor.clone(),
                        output_manifest_ref: summary.0.to_owned(),
                        output_manifest_hash: summary.1.to_owned(),
                        output_schema_hash: input.binding.output_schema_hash.clone(),
                        total_pages: summary.2,
                        total_rows: summary.3,
                        // Migration validated the consumed cursor against the
                        // immutable lineage. Its baseline is already applied.
                        next_page_index: if previous
                            .as_ref()
                            .is_some_and(|r| r.demand_materialization.is_none())
                        {
                            summary.2
                        } else {
                            0
                        },
                        applied_rows: if previous
                            .as_ref()
                            .is_some_and(|r| r.demand_materialization.is_none())
                        {
                            summary.3
                        } else {
                            0
                        },
                    },
                );
            }
            velorix_control::meta_admin::PublishedInputBootstrapProgressV1 {
                schema_version:
                    velorix_control::meta_admin::PUBLISHED_INPUT_BOOTSTRAP_SCHEMA_VERSION_V1,
                bootstrap_generation: published.producer_view_generation,
                plan_hash: published.producer_plan_hash.clone(),
                inputs: entries,
            }
        }
    };
    bootstrap.validate().map_err(meta_error_to_api)?;
    let batch_commit_context = PositiveBaselineCommitContext {
        state,
        active,
        identity,
        bootstrap: &bootstrap,
    };
    let started = std::time::Instant::now();
    let mut processed_rows = 0u64;
    let mut units = 0usize;
    let mut pending_group: Option<PositiveBaselinePageGroup> = None;
    let mut pending_inputs = Vec::new();
    for (key, artifact) in &keyed {
        let artifact_group = artifact.positive_baseline_page_group()?;
        if !pending_inputs.is_empty() && artifact_group != pending_group {
            guard = flush_positive_baseline_group(
                &batch_commit_context,
                &mut previous,
                &progress,
                &mut pending_inputs,
                guard,
            )
            .await?;
        }
        if covered.contains(key)
            || progress
                .artifacts
                .get(key)
                .is_some_and(|entry| entry.applied.is_complete())
        {
            continue;
        }
        if (!query_priority && state.demand_scheduler.foreground.load(Ordering::Acquire) != 0)
            || units >= request.budget.batch_limit
            || processed_rows >= request.budget.max_rows as u64
            || (units > 0
                && started.elapsed().as_millis() >= u128::from(request.budget.max_duration_ms))
        {
            break;
        }
        let loaded = load_demand_published_artifact(
            state,
            artifact,
            request.budget.max_bytes.saturating_sub(read_bytes),
        )
        .await;
        let (input, bytes) = match loaded {
            Ok(loaded) => loaded,
            Err(error)
                if !pending_inputs.is_empty()
                    && error.status == StatusCode::PAYLOAD_TOO_LARGE
                    && error
                        .details
                        .as_ref()
                        .and_then(|details| details.get("code"))
                        .and_then(Value::as_str)
                        == Some("DEMAND_PUBLISHED_BUDGET_REQUIRED") =>
            {
                // Keep the successfully authenticated prefix useful when a
                // later page cannot fit this request's remaining byte budget.
                // The next request resumes from the committed per-page spans.
                break;
            }
            Err(error) => return Err(error),
        };
        read_bytes = read_bytes
            .checked_add(bytes)
            .ok_or_else(|| ApiError::bad_request("published read count overflow"))?;
        let rows_total = input.batches.iter().try_fold(0u64, |sum, batch| {
            sum.checked_add(batch.num_rows() as u64)
                .ok_or_else(|| ApiError::bad_request("published ordinal count overflow"))
        })?;
        // A signed producer commit is indivisible: a replacement may retract
        // and insert the same retained identity in different record positions.
        // Snapshot pages are positive bags and remain safely sliceable.
        if !artifact.is_baseline() && rows_total > request.budget.max_rows as u64 {
            return Err(ApiError {
                status: StatusCode::PAYLOAD_TOO_LARGE,
                message: "signed producer commit requires a larger atomic row budget".into(),
                details: Some(
                    serde_json::json!({"code":"DEMAND_PUBLISHED_BUDGET_REQUIRED",
                    "required_min_rows":rows_total,"supplied_max_rows":request.budget.max_rows}),
                ),
                retryable_materialization: false,
            });
        }
        if !artifact.is_baseline() && rows_total > (request.budget.max_rows as u64 - processed_rows)
        {
            break;
        }
        let entry =
            progress
                .artifacts
                .entry(key.clone())
                .or_insert_with(|| DemandArtifactProgress {
                    artifact_key: key.clone(),
                    artifact_hash: key.clone(),
                    applied: ProcessedSpans::new(rows_total),
                    inspected: BTreeMap::new(),
                    source_watermark: None,
                });
        if entry.applied.rows_total() != rows_total || &entry.artifact_hash != key {
            return Err(ApiError::bad_request(
                "published artifact ordinal identity changed",
            ));
        }
        let mut selected = input.clone();
        let mut count = 0;
        if let Some(interval) = entry.next_inspection(
            &scan_scope,
            DemandOrder::Forward,
            request.budget.max_rows - processed_rows as usize,
        ) {
            count = interval[1] - interval[0];
            let (staged, slice) = entry.stage_inspection(
                &input,
                &scan_scope,
                interval,
                &vec![true; count as usize],
            )?;
            *entry = staged;
            selected = slice;
        } else {
            selected.batches.clear();
        }
        let frontier = previous
            .as_ref()
            .and_then(|record| {
                record.checkpoint.input_frontiers.iter().find(|frontier| {
                    frontier.relation_id == selected.relation_id
                        && frontier.relation_version == selected.relation_version
                        && frontier.stream_id == selected.stream_id
                        && frontier.partition_id == selected.partition_id
                })
            })
            .map_or(0, |frontier| frontier.committed_offset_exclusive);
        selected.start_offset_inclusive = frontier;
        selected.end_offset_exclusive = frontier;
        selected.event_time_watermark = None;
        progress.full_complete = false;
        units += 1;
        processed_rows += count;
        if let Some(group) = artifact_group {
            pending_group = Some(group);
            pending_inputs.push(selected);
        } else {
            guard = commit_demand_unit_locked(
                state,
                active,
                previous.take(),
                progress.clone(),
                vec![selected],
                DemandPublication {
                    bootstrap: Some(bootstrap.clone()),
                    ..DemandPublication::default()
                },
                guard,
            )
            .await?;
            previous = checkpoint_publication::read_latest_standing_runtime_checkpoint_header(
                state,
                identity,
                &active.spec.view_id,
            )
            .await?;
        }
        // Do not process a later signed commit before this artifact is complete.
        if !progress.artifacts[key].applied.is_complete() {
            break;
        }
    }
    guard = flush_positive_baseline_group(
        &batch_commit_context,
        &mut previous,
        &progress,
        &mut pending_inputs,
        guard,
    )
    .await?;
    let remaining_rows = keyed.iter().try_fold(0u64, |sum, (key, _)| {
        let remaining = if covered.contains(key) {
            0
        } else {
            progress.artifacts.get(key).map_or(1, |entry| {
                entry.applied.rows_total() - entry.applied.processed_len()
            })
        };
        sum.checked_add(remaining)
            .ok_or_else(|| ApiError::bad_request("published remaining work overflow"))
    })?;
    let complete = remaining_rows == 0;
    let mut checkpoint_epoch = None;
    if complete {
        for input in &mut bootstrap.inputs {
            input.next_page_index = input.total_pages;
            input.applied_rows = input.total_rows;
        }
        bootstrap.validate().map_err(meta_error_to_api)?;
        progress.full_complete = true;
        progress
            .certificates
            .retain(|certificate| certificate.scope_hash != requested_scope);
        progress.certificates.push(DemandCertificate {
            scope_sql: scope_sql.map(str::to_owned),
            scope_hash: requested_scope,
            captured_cut_hash: cut_hash,
        });
        let mut relation_inputs = Vec::new();
        let mut direct_inputs = Vec::new();
        let mut proofs = Vec::new();
        for input in &inputs {
            let cursor = heads[&input.edge.edge_id].clone();
            let (_, proof) = checkpoint_publication::read_authoritative_cursor_checkpoint_header(
                state,
                &cursor.producer_tenant_id,
                &input.binding,
                &cursor,
            )
            .await?;
            proofs.push(proof);
            let binding = &input.binding;
            let start = previous
                .as_ref()
                .and_then(|record| {
                    record.checkpoint.input_frontiers.iter().find(|frontier| {
                        frontier.relation_id == binding.relation.relation_id
                            && frontier.stream_id == binding.output_stream_id
                    })
                })
                .map_or(0, |frontier| frontier.committed_offset_exclusive);
            relation_inputs.push(RelationInputBatch {
                relation_id: binding.relation.relation_id.clone(),
                relation_version: binding.relation.relation_version.clone(),
                schema_fingerprint: binding.relation.schema_fingerprint.clone(),
                stream_id: binding.output_stream_id.clone(),
                partition_id: 0,
                start_offset_inclusive: start,
                end_offset_exclusive: cursor.output_epoch,
                event_time_watermark: None,
                encoding: RelationInputEncodingV1::PublishedRelationDeltaV1 {
                    delta_codec_identity: binding.delta_codec_identity.clone(),
                    output_schema_hash: binding.output_schema_hash.clone(),
                    weight_field_name: PUBLISHED_DELTA_WEIGHT_FIELD_V1.to_owned(),
                    weight_field_index: binding.relation.columns.len(),
                },
                batches: Vec::new(),
            });
            direct_inputs.push(StandingRuntimeDirectViewInputV1 {
                published_relation: binding.clone(),
                cursor,
            });
        }
        guard = commit_demand_unit_locked(
            state,
            active,
            previous,
            progress,
            relation_inputs,
            DemandPublication {
                bootstrap: Some(bootstrap),
                direct_inputs,
                cursor_proofs: proofs,
                source_cuts: None,
                validated_source_publications: Vec::new(),
            },
            guard,
        )
        .await?;
        checkpoint_epoch = checkpoint_publication::read_latest_standing_runtime_checkpoint_header(
            state,
            identity,
            &active.spec.view_id,
        )
        .await?
        .map(|record| record.checkpoint.logical_epoch);
    }
    drop(guard);
    Ok(DemandOutcome {
        checkpoint_epoch,
        work_units: units,
        status: if complete { "complete" } else { "incomplete" },
        complete,
        processed_rows,
        read_bytes,
        remaining_rows,
        dependency_mode: "full_closure",
    })
}
