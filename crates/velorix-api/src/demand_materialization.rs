//! Request-driven materialization controls. No task is spawned by this module.
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use velorix_core::processed_spans::ProcessedSpans;

#[path = "demand_materialization_published.rs"]
mod published;

#[path = "demand_migration.rs"]
mod migration;

#[cfg(test)]
pub(super) async fn migration_read_bytes_for_test(
    state: &ApiState,
    active: &ActiveMaterializedView,
    max_bytes: usize,
) -> Result<(Option<StandingRuntimeCheckpointRecord>, usize), ApiError> {
    migration::read_checkpoint(state, active, max_bytes).await
}

/// Sparse work is attached to the same fenced checkpoint as its operator state.
/// Artifact keys are hashes of authenticated immutable publication identities.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(super) struct DemandCheckpoint {
    pub(super) schema_version: u32,
    pub(super) spec_hash: String,
    pub(super) plan_hash: String,
    pub(super) output_schema_hash: String,
    pub(super) paused: bool,
    pub(super) order: DemandOrder,
    pub(super) artifacts: BTreeMap<String, DemandArtifactProgress>,
    pub(super) certificates: Vec<DemandCertificate>,
    /// Only full dependency completion permits the ordinary producer protocol.
    pub(super) full_complete: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) event_time_replay_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(super) event_time_output_readiness:
        Vec<velorix_core::standing_program::InputEventTimeFrontier>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(super) struct DemandArtifactProgress {
    pub(super) artifact_key: String,
    pub(super) artifact_hash: String,
    pub(super) applied: ProcessedSpans,
    /// Every row in these spans was inspected for this scope. Rows outside the
    /// scope are not added to `applied`, so a different scope can process them.
    pub(super) inspected: BTreeMap<String, ProcessedSpans>,
    /// Present only after authenticating the complete immutable source envelope.
    /// The wrapper distinguishes an authenticated absent watermark from legacy
    /// progress which has not retained watermark evidence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) source_watermark: Option<DemandSourceWatermark>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(super) struct DemandSourceWatermark {
    pub(super) watermark: Option<InputEventTimeWatermark>,
}

impl DemandArtifactProgress {
    fn selected_source_runs(
        &self,
        input: &RelationInputBatch,
        interval: [u64; 2],
        may_match: &[bool],
    ) -> Result<Vec<RelationInputBatch>, ApiError> {
        let mut runs = Vec::new();
        let mut ordinal = interval[0];
        while ordinal < interval[1] {
            if !may_match[(ordinal - interval[0]) as usize] || self.applied.contains(ordinal) {
                ordinal += 1;
                continue;
            }
            let start = ordinal;
            while ordinal < interval[1]
                && may_match[(ordinal - interval[0]) as usize]
                && !self.applied.contains(ordinal)
            {
                ordinal += 1;
            }
            let mut run = input.clone();
            run.start_offset_inclusive = input
                .start_offset_inclusive
                .checked_add(start)
                .ok_or_else(|| ApiError::bad_request("source run offset overflow"))?;
            run.end_offset_exclusive = input
                .start_offset_inclusive
                .checked_add(ordinal)
                .ok_or_else(|| ApiError::bad_request("source run offset overflow"))?;
            run.batches.clear();
            let mut base = 0u64;
            for batch in &input.batches {
                let end = base
                    .checked_add(batch.num_rows() as u64)
                    .ok_or_else(|| ApiError::bad_request("source ordinal overflow"))?;
                let lo = base.max(start);
                let hi = end.min(ordinal);
                if lo < hi {
                    run.batches
                        .push(batch.slice((lo - base) as usize, (hi - lo) as usize));
                }
                base = end;
            }
            runs.push(run);
        }
        if runs.is_empty() {
            // Retain authenticated partition watermark evidence without
            // claiming that any inspected-but-excluded ordinal was applied.
            let mut evidence = input.clone();
            evidence.batches.clear();
            evidence.end_offset_exclusive = evidence.start_offset_inclusive;
            runs.push(evidence);
        }
        Ok(runs)
    }

    /// Select one bounded inspection interval. Selection does not mutate durable
    /// progress; the caller installs the updated clone only with its checkpoint.
    pub(super) fn next_inspection(
        &self,
        scope: &str,
        order: DemandOrder,
        max_rows: usize,
    ) -> Option<[u64; 2]> {
        if max_rows == 0 {
            return None;
        }
        let empty = ProcessedSpans::new(self.applied.rows_total());
        let scan = self.inspected.get(scope).unwrap_or(&empty);
        let runs = scan.unprocessed_runs();
        let limit = u64::try_from(max_rows).unwrap_or(u64::MAX);
        match order {
            DemandOrder::Forward => runs
                .first()
                .map(|&[start, end]| [start, start + (end - start).min(limit)]),
            DemandOrder::Reverse => runs
                .last()
                .map(|&[start, end]| [end - (end - start).min(limit), end]),
        }
    }

    /// Slice an authenticated artifact without changing its physical row order.
    /// Predicate evidence must contain exactly one entry per inspected ordinal.
    /// The returned progress remains speculative until the enclosing fenced CAS.
    pub(super) fn stage_inspection(
        &self,
        input: &RelationInputBatch,
        scope: &str,
        interval: [u64; 2],
        may_match: &[bool],
    ) -> Result<(Self, RelationInputBatch), ApiError> {
        let [start, end] = interval;
        let rows_total = input.batches.iter().try_fold(0u64, |sum, batch| {
            sum.checked_add(u64::try_from(batch.num_rows()).map_err(ApiError::internal)?)
                .ok_or_else(|| ApiError::bad_request("demand artifact row count overflow"))
        })?;
        if rows_total != self.applied.rows_total()
            || start >= end
            || end > rows_total
            || u64::try_from(may_match.len()).map_err(ApiError::internal)? != end - start
        {
            return Err(ApiError::bad_request(
                "demand inspection ordinals do not match authenticated artifact",
            ));
        }
        let mut staged = self.clone();
        let mut batches = Vec::new();
        let mut base = 0u64;
        for batch in &input.batches {
            let batch_end = base + u64::try_from(batch.num_rows()).map_err(ApiError::internal)?;
            let mut ordinal = start.max(base);
            let bound = end.min(batch_end);
            while ordinal < bound {
                let selected = may_match
                    [usize::try_from(ordinal - start).map_err(ApiError::internal)?]
                    && !self.applied.contains(ordinal);
                if !selected {
                    ordinal += 1;
                    continue;
                }
                let run_start = ordinal;
                ordinal += 1;
                while ordinal < bound
                    && may_match[usize::try_from(ordinal - start).map_err(ApiError::internal)?]
                    && !self.applied.contains(ordinal)
                {
                    ordinal += 1;
                }
                batches.push(batch.slice(
                    usize::try_from(run_start - base).map_err(ApiError::internal)?,
                    usize::try_from(ordinal - run_start).map_err(ApiError::internal)?,
                ));
                staged
                    .applied
                    .insert(run_start, ordinal)
                    .map_err(ApiError::bad_request)?;
            }
            base = batch_end;
        }
        staged
            .inspected
            .entry(scope.to_owned())
            .or_insert_with(|| ProcessedSpans::new(rows_total))
            .insert(start, end)
            .map_err(ApiError::bad_request)?;
        let mut selected = input.clone();
        if batches.is_empty() {
            let schema = input
                .batches
                .first()
                .ok_or_else(|| {
                    ApiError::bad_request("authenticated source artifact has no batch schema")
                })?
                .schema();
            batches.push(RecordBatch::new_empty(schema));
        }
        selected.batches = batches;
        // Sparse ordinals are tracked above; a contiguous source frontier must
        // not move until all preceding artifacts and their gaps are processed.
        selected.start_offset_inclusive = 0;
        selected.end_offset_exclusive = 0;
        selected.event_time_watermark = None;
        Ok((staged, selected))
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(super) struct DemandCertificate {
    pub(super) scope_sql: Option<String>,
    pub(super) scope_hash: String,
    pub(super) captured_cut_hash: String,
}

impl DemandCheckpoint {
    pub(super) fn validate_checkpoint(
        &self,
        checkpoint: &RuntimeCheckpoint,
    ) -> Result<(), ApiError> {
        if self.schema_version != 1
            || self.spec_hash.is_empty()
            || self.plan_hash.is_empty()
            || self
                .event_time_replay_id
                .as_ref()
                .is_some_and(|id| id != &format!("demand:{}:{}", self.spec_hash, self.plan_hash))
            || self.output_schema_hash != checkpoint.identity.output_schema_hash
        {
            return Err(ApiError::bad_request(
                "demand checkpoint identity is invalid",
            ));
        }
        for (key, progress) in &self.artifacts {
            if key != &progress.artifact_key
                || progress.artifact_key.is_empty()
                || progress.artifact_hash.is_empty()
                || progress
                    .inspected
                    .values()
                    .any(|scan| scan.rows_total() != progress.applied.rows_total())
            {
                return Err(ApiError::bad_request("demand artifact progress is invalid"));
            }
        }
        if self.full_complete
            && self
                .artifacts
                .values()
                .any(|artifact| !artifact.applied.is_complete())
        {
            return Err(ApiError::bad_request(
                "complete demand checkpoint contains unapplied artifact ordinals",
            ));
        }
        let mut certified_scopes = BTreeSet::new();
        for certificate in &self.certificates {
            if certificate.scope_hash != scope_hash(certificate.scope_sql.as_deref())?
                || certificate.captured_cut_hash.is_empty()
                || !certified_scopes.insert(&certificate.scope_hash)
            {
                return Err(ApiError::bad_request("demand scope certificate is invalid"));
            }
        }
        Ok(())
    }
}

pub(super) fn scope_hash(scope: Option<&str>) -> Result<String, ApiError> {
    Ok(stable_bytes_hash(
        &serde_json::to_vec(&(1u32, scope)).map_err(ApiError::internal)?,
    ))
}

#[derive(Debug, Serialize)]
pub(super) struct DemandOutcome {
    #[serde(skip)]
    pub(super) checkpoint_epoch: Option<u64>,
    #[serde(skip)]
    pub(super) work_units: usize,
    pub(super) status: &'static str,
    pub(super) complete: bool,
    pub(super) processed_rows: u64,
    pub(super) read_bytes: usize,
    pub(super) remaining_rows: u64,
    pub(super) dependency_mode: &'static str,
}

/// Resolve only the requested view's transitive dependencies. Repeated edges
/// share producer work, but retain distinct edge identities at application.
pub(super) async fn demand_dependency_order(
    state: &ApiState,
    target: &ActiveMaterializedView,
) -> Result<Vec<ActiveMaterializedView>, ApiError> {
    let mut pending = vec![(target.clone(), false)];
    let mut visiting = BTreeSet::new();
    let mut complete = BTreeSet::new();
    let mut ordered = Vec::new();
    while let Some((active, exiting)) = pending.pop() {
        let id = active.spec.view_id.clone();
        if exiting {
            visiting.remove(&id);
            if complete.insert(id) {
                ordered.push(active);
            }
            continue;
        }
        if complete.contains(&id) {
            continue;
        }
        if !visiting.insert(id.clone()) {
            return Err(ApiError::bad_request("cycle in demand dependency closure"));
        }
        let binding = active
            .runtime
            .as_ref()
            .ok_or_else(|| ApiError::bad_request("demand dependency has no standing runtime"))?;
        let mut producers = BTreeMap::new();
        for input in &binding.input_bindings {
            if let StandingInputBindingV1::PublishedView {
                published_relation, ..
            } = input
            {
                let producer = state
                    .view_registry()?
                    .read_active(&published_relation.producer_view_id)
                    .await
                    .map_err(materialized_view_registry_error_to_api)?;
                if published_relation_binding_for_active_view(&producer)?.as_ref()
                    != Some(published_relation)
                {
                    return Err(ApiError::conflict(
                        "demand producer generation or schema differs from captured declaration",
                    ));
                }
                producers.insert(producer.spec.view_id.clone(), producer);
            }
        }
        pending.push((active, true));
        for producer in producers.into_values().rev() {
            pending.push((producer, false));
        }
    }
    Ok(ordered)
}

/// Share one work budget across the requested dependency closure. Ancestors
/// are conservatively materialized in full; the target may use a narrow proof.
pub(super) async fn materialize_demand(
    state: &ApiState,
    target: &ActiveMaterializedView,
    request: &DemandRequest,
    order: DemandOrder,
    query_priority: bool,
) -> Result<DemandOutcome, ApiError> {
    request.budget.validate()?;
    let closure = demand_dependency_order(state, target).await?;
    let started = std::time::Instant::now();
    let mut used_rows = 0u64;
    let mut used_bytes = 0usize;
    let mut used_units = 0usize;
    for active in closure {
        let elapsed = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        if (!query_priority && state.demand_scheduler.foreground.load(Ordering::Acquire) != 0)
            || used_rows >= request.budget.max_rows as u64
            || used_bytes >= request.budget.max_bytes
            || used_units >= request.budget.batch_limit
            || (used_units > 0 && elapsed >= request.budget.max_duration_ms)
        {
            return Ok(DemandOutcome {
                checkpoint_epoch: None,
                work_units: used_units,
                status: "dependency_pending",
                complete: false,
                processed_rows: used_rows,
                read_bytes: used_bytes,
                remaining_rows: 1,
                dependency_mode: "full_closure",
            });
        }
        let is_target = active.spec.view_id == target.spec.view_id;
        let step = DemandRequest {
            scope: if is_target {
                request.scope.clone()
            } else {
                None
            },
            budget: DemandBudget {
                batch_limit: request.budget.batch_limit - used_units,
                max_rows: request.budget.max_rows - used_rows as usize,
                max_bytes: request.budget.max_bytes - used_bytes,
                max_duration_ms: request
                    .budget
                    .max_duration_ms
                    .saturating_sub(elapsed)
                    .max(1),
            },
        };
        let has_published = active
            .runtime
            .as_ref()
            .is_some_and(view_has_published_view_inputs);
        let mut outcome = if has_published {
            published::materialize_published_demand(state, &active, &step, query_priority).await?
        } else {
            materialize_source_demand(
                state,
                &active,
                &step,
                if is_target {
                    order
                } else {
                    DemandOrder::Forward
                },
                query_priority,
            )
            .await?
        };
        used_rows = used_rows
            .checked_add(outcome.processed_rows)
            .ok_or_else(|| ApiError::bad_request("demand row budget overflow"))?;
        used_bytes = used_bytes
            .checked_add(outcome.read_bytes)
            .ok_or_else(|| ApiError::bad_request("demand byte budget overflow"))?;
        used_units = used_units
            .checked_add(outcome.work_units)
            .ok_or_else(|| ApiError::bad_request("demand work unit overflow"))?;
        if !outcome.complete || is_target {
            outcome.processed_rows = used_rows;
            outcome.read_bytes = used_bytes;
            outcome.work_units = used_units;
            if !is_target {
                outcome.checkpoint_epoch = None;
                outcome.dependency_mode = "full_closure";
            }
            return Ok(outcome);
        }
    }
    Err(ApiError::bad_request(
        "demand dependency closure omitted target",
    ))
}

fn source_cut_watermark(
    progress: &DemandCheckpoint,
    artifacts: &[(String, &demand_source::DemandSourceArtifact)],
    relation_id: &str,
    stream_id: &str,
    partition_id: u32,
) -> Result<Option<InputEventTimeWatermark>, ApiError> {
    let mut ordered = artifacts
        .iter()
        .filter(|(_, artifact)| {
            artifact.relation.relation_id == relation_id
                && artifact.stream_id == stream_id
                && artifact.partition_id == partition_id
        })
        .collect::<Vec<_>>();
    ordered.sort_by_key(|(_, artifact)| artifact.publication.start_offset_inclusive);
    let mut latest: Option<InputEventTimeWatermark> = None;
    for (key, _) in ordered {
        let entry = &progress.artifacts[key];
        if progress.full_complete && !entry.applied.is_complete() {
            return Err(ApiError::bad_request(
                "source watermark cannot cross sparse holes",
            ));
        }
        let evidence = entry
            .source_watermark
            .as_ref()
            .ok_or_else(|| ApiError::bad_request("source watermark evidence missing"))?;
        let Some(watermark) = &evidence.watermark else {
            continue;
        };
        if watermark.stream_id != stream_id
            || watermark.partition_id != partition_id
            || latest.as_ref().is_some_and(|prior| {
                prior.event_time_column_id != watermark.event_time_column_id
                    || prior.watermark_ns > watermark.watermark_ns
                    || prior.max_observed_event_time_ns > watermark.max_observed_event_time_ns
            })
        {
            return Err(ApiError::bad_request(
                "authenticated source watermark identity or monotonicity mismatch",
            ));
        }
        latest = Some(watermark.clone());
    }
    Ok(latest)
}

/// Physical-source demand. The caller serializes request scheduling; this
/// function holds the view operation lock across restore, apply and publication.
pub(super) async fn materialize_source_demand(
    state: &ApiState,
    active: &ActiveMaterializedView,
    request: &DemandRequest,
    order: DemandOrder,
    query_priority: bool,
) -> Result<DemandOutcome, ApiError> {
    use super::demand_source::{
        capture_demand_source_artifacts, demand_source_artifact_key, load_demand_source_artifact,
    };
    request.budget.validate()?;
    if active.runtime.as_ref().is_some_and(|runtime| {
        runtime
            .input_bindings
            .iter()
            .any(|input| matches!(input, StandingInputBindingV1::PublishedView { .. }))
    }) {
        return Err(ApiError::service_unavailable(
            "published dependencies must complete before physical-source demand",
        ));
    }
    let identity = active_standing_runtime_identity(active)
        .ok_or_else(|| ApiError::bad_request("demand requires a standing runtime"))?;
    let operation = state.standing_runtime_operation_lock(identity, &active.spec.view_id)?;
    let mut operation_guard = operation.lock_owned().await;
    let (cuts, artifacts) = capture_demand_source_artifacts(state, active).await?;
    let cut_hash = stable_bytes_hash(&serde_json::to_vec(&cuts).map_err(ApiError::internal)?);
    let scope_sql = request.scope.as_ref().map(|scope| scope.predicate.as_str());
    let requested_scope = scope_hash(scope_sql)?;
    let (mut previous, migration_bytes) =
        migration::read_checkpoint(state, active, request.budget.max_bytes).await?;
    migration::validate_source_prefixes(active, previous.as_ref(), &artifacts)?;
    let plan_hash = active
        .runtime
        .as_ref()
        .and_then(|runtime| runtime.logical_plan.as_ref())
        .and_then(|plan| plan.plan_hash.clone())
        .ok_or_else(|| ApiError::bad_request("demand requires a plan hash"))?;
    let mut progress = previous
        .as_ref()
        .and_then(|record| record.demand_materialization.clone())
        .unwrap_or_else(|| DemandCheckpoint {
            schema_version: 1,
            spec_hash: active.spec_hash.clone(),
            plan_hash: plan_hash.clone(),
            output_schema_hash: identity.output_schema_hash.clone(),
            paused: false,
            order,
            artifacts: BTreeMap::new(),
            certificates: Vec::new(),
            full_complete: false,
            event_time_replay_id: None,
            event_time_output_readiness: Vec::new(),
        });
    if progress.spec_hash != active.spec_hash || progress.plan_hash != plan_hash {
        return Err(ApiError::conflict(
            "persisted demand identity does not match active view",
        ));
    }
    let mut dependencies = scope_dependencies::ScopeDependencies::for_active(active)?;
    if let Some(dependencies) = dependencies.as_mut() {
        let [input] = active.spec.input_relations.as_slice() else {
            return Err(ApiError::bad_request(
                "source dependency proof requires one input",
            ));
        };
        let catalog =
            read_relation_catalog(state, &input.relation_id, &input.relation_version).await?;
        dependencies.bind_source_catalog(&catalog)?;
    }
    let output_schema = active
        .spec
        .output_relations
        .first()
        .ok_or_else(|| ApiError::bad_request("demand output schema unavailable"))?;
    let predicate = scope_sql.map(|sql| {
        scope_dependencies::parse_predicate(sql, &[], output_schema)
            .unwrap_or(velorix_core::query::PagePredicate::Unknown)
    });
    // Watermark-sensitive operators must observe each original atomic ingest
    // unit in causal order, including its watermark and late-row decisions.
    let causal_event_time = active
        .runtime
        .as_ref()
        .and_then(|runtime| runtime.logical_plan.as_ref())
        .is_some_and(|logical| {
            matches!(
                logical.execution,
                VelorixLogicalViewExecutionV1::TumblingEventTimeAggregate { .. }
            )
        });
    let narrow =
        dependencies
            .as_ref()
            .zip(predicate.as_ref())
            .is_some_and(|(dependencies, predicate)| {
                predicate_columns_known(predicate, dependencies.eligible_known_columns())
            });
    let mode = if narrow { "scope" } else { "full_closure" };
    let scan_scope = if narrow {
        requested_scope.clone()
    } else {
        scope_hash(None)?
    };
    let mut keyed = Vec::with_capacity(artifacts.len());
    for artifact in &artifacts {
        let key = demand_source_artifact_key(artifact)?;
        let rows = artifact
            .publication
            .end_offset_exclusive
            .checked_sub(artifact.publication.start_offset_inclusive)
            .ok_or_else(|| ApiError::bad_request("invalid demand artifact offsets"))?;
        let entry = progress.artifacts.entry(key.clone()).or_insert_with(|| {
            let previously_applied = previous.as_ref().is_some_and(|record| {
                record.checkpoint.input_frontiers.iter().any(|frontier| {
                    frontier.relation_id == artifact.relation.relation_id
                        && frontier.relation_version == artifact.relation.relation_version
                        && frontier.stream_id == artifact.stream_id
                        && frontier.partition_id == artifact.partition_id
                        && frontier.committed_offset_exclusive
                            >= artifact.publication.end_offset_exclusive
                })
            });
            DemandArtifactProgress {
                artifact_key: key.clone(),
                artifact_hash: artifact.publication.object_digest.clone(),
                applied: ProcessedSpans::from_prefix(
                    rows,
                    if previously_applied { rows } else { 0 },
                )
                .expect("bounded prefix"),
                inspected: BTreeMap::new(),
                source_watermark: None,
            }
        });
        if entry.artifact_hash != artifact.publication.object_digest
            || entry.applied.rows_total() != rows
        {
            return Err(ApiError::bad_request(
                "demand artifact differs from persisted identity",
            ));
        }
        if entry.applied.is_complete() {
            entry
                .inspected
                .insert(scan_scope.clone(), entry.applied.clone());
        }
        keyed.push((key, artifact));
    }
    // Certificates are cache hints. Prove coverage against this authenticated
    // inventory after reconciling immutable identities and ordinal counts.
    if progress.certificates.iter().any(|certificate| {
        certificate.scope_hash == requested_scope && certificate.captured_cut_hash == cut_hash
    }) && keyed.iter().all(|(key, _)| {
        let entry = &progress.artifacts[key];
        (entry.applied.is_complete() && (narrow || entry.source_watermark.is_some()))
            || narrow
                && entry
                    .inspected
                    .get(&scan_scope)
                    .is_some_and(ProcessedSpans::is_complete)
    }) {
        return Ok(DemandOutcome {
            work_units: 0,
            checkpoint_epoch: previous
                .as_ref()
                .map(|record| record.checkpoint.logical_epoch),
            status: "complete",
            complete: true,
            processed_rows: 0,
            read_bytes: migration_bytes,
            remaining_rows: 0,
            dependency_mode: mode,
        });
    }
    if causal_event_time && progress.event_time_replay_id.is_none() {
        progress.event_time_replay_id = Some(format!(
            "demand:{}:{}",
            progress.spec_hash, progress.plan_hash
        ));
    }
    if order == DemandOrder::Reverse {
        keyed.reverse();
    } else if causal_event_time {
        keyed.sort_by(|(_, left), (_, right)| {
            (
                &left.relation.relation_id,
                &left.stream_id,
                left.partition_id,
                left.publication.start_offset_inclusive,
            )
                .cmp(&(
                    &right.relation.relation_id,
                    &right.stream_id,
                    right.partition_id,
                    right.publication.start_offset_inclusive,
                ))
        });
    }
    let started = std::time::Instant::now();
    let mut processed_rows = 0u64;
    let mut read_bytes = migration_bytes;
    let mut units = 0usize;
    let mut validated_source_publications = Vec::new();
    for (key, artifact) in &keyed {
        if (!query_priority && state.demand_scheduler.foreground.load(Ordering::Acquire) != 0)
            || units >= request.budget.batch_limit
            || read_bytes >= request.budget.max_bytes
            || (units > 0
                && started.elapsed().as_millis() >= u128::from(request.budget.max_duration_ms))
        {
            break;
        }
        let remaining = request
            .budget
            .max_rows
            .saturating_sub(usize::try_from(processed_rows).map_err(ApiError::internal)?);
        let Some(interval) = progress.artifacts[key].next_inspection(&scan_scope, order, remaining)
        else {
            continue;
        };
        let (input, bytes, proof) =
            load_demand_source_artifact(state, artifact, request.budget.max_bytes - read_bytes)
                .await?;
        validated_source_publications.push(proof);
        read_bytes = read_bytes
            .checked_add(bytes)
            .ok_or_else(|| ApiError::bad_request("demand read byte overflow"))?;
        let count = usize::try_from(interval[1] - interval[0]).map_err(ApiError::internal)?;
        let may_match = if narrow {
            let mut slices = Vec::new();
            let mut base = 0u64;
            for batch in &input.batches {
                let end = base + batch.num_rows() as u64;
                let lo = base.max(interval[0]);
                let hi = end.min(interval[1]);
                if lo < hi {
                    slices.push(batch.slice((lo - base) as usize, (hi - lo) as usize));
                }
                base = end;
            }
            let rows = dependencies
                .as_ref()
                .expect("narrow dependency proof")
                .source_dependency_rows(&input.relation_id, &slices)?;
            if rows.len() != count {
                return Err(ApiError::bad_request(
                    "dependency projection changed source ordinal count",
                ));
            }
            rows.iter()
                .map(|row| {
                    scope_dependencies::predicate_may_match(
                        predicate.as_ref().expect("narrow predicate"),
                        row,
                    )
                })
                .collect()
        } else {
            vec![true; count]
        };
        let (mut staged, mut selected) =
            progress.artifacts[key].stage_inspection(&input, &scan_scope, interval, &may_match)?;
        staged.source_watermark = Some(DemandSourceWatermark {
            watermark: input.event_time_watermark.clone(),
        });
        // Existing completed prefixes remain pinned during sparse extension.
        let frontier = previous
            .as_ref()
            .and_then(|record| {
                record.checkpoint.input_frontiers.iter().find(|frontier| {
                    frontier.relation_id == input.relation_id
                        && frontier.relation_version == input.relation_version
                        && frontier.stream_id == input.stream_id
                        && frontier.partition_id == input.partition_id
                })
            })
            .map_or(0, |frontier| frontier.committed_offset_exclusive);
        selected.start_offset_inclusive = frontier;
        selected.end_offset_exclusive = frontier;
        let selected_inputs = if causal_event_time {
            progress.artifacts[key].selected_source_runs(&input, interval, &may_match)?
        } else {
            vec![selected]
        };
        progress.artifacts.insert((*key).clone(), staged);
        // Full producer coverage is installed only by the final frontier step.
        progress.full_complete = false;
        operation_guard = commit_demand_unit_locked(
            state,
            active,
            previous.take(),
            progress.clone(),
            selected_inputs,
            DemandPublication {
                source_cuts: Some(cuts.clone()),
                validated_source_publications: validated_source_publications.clone(),
                ..DemandPublication::default()
            },
            operation_guard,
        )
        .await?;
        previous = checkpoint_publication::read_latest_standing_runtime_checkpoint_header(
            state,
            identity,
            &active.spec.view_id,
        )
        .await?;
        processed_rows += count as u64;
        units += 1;
    }
    let remaining_rows = keyed.iter().try_fold(0u64, |sum, (key, _)| {
        let entry = &progress.artifacts[key];
        let inspected = entry
            .inspected
            .get(&scan_scope)
            .map_or(0, ProcessedSpans::processed_len);
        sum.checked_add(entry.applied.rows_total() - inspected)
            .ok_or_else(|| ApiError::bad_request("demand remaining row count overflow"))
    })?;
    let complete = remaining_rows == 0;
    let mut checkpoint_epoch = None;
    if complete {
        progress.full_complete = keyed
            .iter()
            .all(|(key, _)| progress.artifacts[key].applied.is_complete());
        if progress.full_complete || causal_event_time {
            // Legacy sparse progress may have applied rows without retaining
            // envelope watermark evidence. Reauthenticate before advancing a
            // frontier; the source loader charges the complete immutable unit.
            for (key, artifact) in &keyed {
                if progress.artifacts[key].source_watermark.is_none() {
                    let (input, bytes, proof) = load_demand_source_artifact(
                        state,
                        artifact,
                        request.budget.max_bytes.saturating_sub(read_bytes),
                    )
                    .await?;
                    validated_source_publications.push(proof);
                    read_bytes = read_bytes
                        .checked_add(bytes)
                        .ok_or_else(|| ApiError::bad_request("demand read byte overflow"))?;
                    progress
                        .artifacts
                        .get_mut(key)
                        .expect("reconciled artifact")
                        .source_watermark = Some(DemandSourceWatermark {
                        watermark: input.event_time_watermark,
                    });
                }
            }
        }
        progress
            .certificates
            .retain(|certificate| certificate.scope_hash != requested_scope);
        progress.certificates.push(DemandCertificate {
            scope_sql: scope_sql.map(str::to_owned),
            scope_hash: requested_scope,
            captured_cut_hash: cut_hash,
        });
        if causal_event_time {
            let mut readiness = Vec::new();
            for cut in &cuts {
                for partition in &cut.cut.partitions {
                    if let Some(watermark) = source_cut_watermark(
                        &progress,
                        &keyed,
                        &cut.relation.relation_id,
                        &partition.stream_id,
                        partition.partition_id,
                    )? {
                        readiness.push(velorix_core::standing_program::InputEventTimeFrontier {
                            relation_id: cut.relation.relation_id.clone(),
                            relation_version: cut.relation.relation_version.clone(),
                            schema_fingerprint: cut.relation.schema_fingerprint.clone(),
                            stream_id: partition.stream_id.clone(),
                            partition_id: partition.partition_id,
                            event_time_column_id: watermark.event_time_column_id,
                            max_observed_event_time_ns: watermark.max_observed_event_time_ns,
                            watermark_ns: watermark.watermark_ns,
                        });
                    }
                }
            }
            progress.event_time_output_readiness = readiness;
        }
        let inputs = if progress.full_complete {
            cuts.iter()
                .flat_map(|cut| {
                    cut.cut.partitions.iter().map(|partition| {
                        let frontier = previous
                            .as_ref()
                            .and_then(|record| {
                                record.checkpoint.input_frontiers.iter().find(|frontier| {
                                    frontier.relation_id == cut.relation.relation_id
                                        && frontier.stream_id == partition.stream_id
                                        && frontier.partition_id == partition.partition_id
                                })
                            })
                            .map_or(0, |frontier| frontier.committed_offset_exclusive);
                        Ok(RelationInputBatch {
                            relation_id: cut.relation.relation_id.clone(),
                            relation_version: cut.relation.relation_version.clone(),
                            stream_id: partition.stream_id.clone(),
                            partition_id: partition.partition_id,
                            schema_fingerprint: cut.relation.schema_fingerprint.clone(),
                            start_offset_inclusive: frontier,
                            end_offset_exclusive: partition.committed_offset_exclusive,
                            event_time_watermark: source_cut_watermark(
                                &progress,
                                &keyed,
                                &cut.relation.relation_id,
                                &partition.stream_id,
                                partition.partition_id,
                            )?,
                            encoding: RelationInputEncodingV1::SourceRelationV1,
                            batches: vec![RecordBatch::new_empty(
                                arrow_schema_from_incremental_relation_schema(
                                    active
                                        .spec
                                        .input_relations
                                        .iter()
                                        .find(|relation| {
                                            relation.relation_id == cut.relation.relation_id
                                                && relation.relation_version
                                                    == cut.relation.relation_version
                                        })
                                        .ok_or_else(|| {
                                            ApiError::bad_request("demand source schema missing")
                                        })?,
                                )?,
                            )],
                        })
                    })
                })
                .collect::<Result<Vec<_>, ApiError>>()?
        } else {
            Vec::new()
        };
        operation_guard = commit_demand_unit_locked(
            state,
            active,
            previous,
            progress,
            inputs,
            DemandPublication {
                source_cuts: Some(cuts),
                validated_source_publications,
                ..DemandPublication::default()
            },
            operation_guard,
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
    drop(operation_guard);
    Ok(DemandOutcome {
        work_units: units,
        checkpoint_epoch,
        status: if complete { "complete" } else { "incomplete" },
        complete,
        processed_rows,
        read_bytes,
        remaining_rows,
        dependency_mode: mode,
    })
}

fn predicate_columns_known(
    predicate: &velorix_core::query::PagePredicate,
    columns: &[String],
) -> bool {
    use velorix_core::query::PagePredicate;
    match predicate {
        PagePredicate::Unknown => false,
        PagePredicate::Compare { column, .. } | PagePredicate::Null { column, .. } => {
            columns.contains(column)
        }
        PagePredicate::And(left, right) | PagePredicate::Or(left, right) => {
            predicate_columns_known(left, columns) && predicate_columns_known(right, columns)
        }
    }
}

pub(super) async fn set_demand_control(
    state: &ApiState,
    active: &ActiveMaterializedView,
    paused: bool,
    order: DemandOrder,
) -> Result<(), ApiError> {
    let identity = active_standing_runtime_identity(active)
        .ok_or_else(|| ApiError::bad_request("demand requires a standing runtime"))?;
    let operation = state.standing_runtime_operation_lock(identity, &active.spec.view_id)?;
    let operation_guard = operation.lock_owned().await;
    let previous = checkpoint_publication::read_latest_standing_runtime_checkpoint_header(
        state,
        identity,
        &active.spec.view_id,
    )
    .await?;
    let mut progress = match previous
        .as_ref()
        .and_then(|record| record.demand_materialization.clone())
    {
        Some(progress) => progress,
        None => DemandCheckpoint {
            schema_version: 1,
            spec_hash: active.spec_hash.clone(),
            plan_hash: active
                .runtime
                .as_ref()
                .and_then(|runtime| runtime.logical_plan.as_ref())
                .and_then(|plan| plan.plan_hash.clone())
                .ok_or_else(|| ApiError::bad_request("demand requires a plan hash"))?,
            output_schema_hash: identity.output_schema_hash.clone(),
            paused,
            order,
            artifacts: BTreeMap::new(),
            certificates: Vec::new(),
            full_complete: false,
            event_time_replay_id: None,
            event_time_output_readiness: Vec::new(),
        },
    };
    if previous.as_ref().is_some_and(|record| {
        record
            .demand_materialization
            .as_ref()
            .is_some_and(|saved| saved.paused == paused && saved.order == order)
    }) {
        return Ok(());
    }
    progress.paused = paused;
    progress.order = order;
    let is_published_consumer = active
        .runtime
        .as_ref()
        .is_some_and(view_has_published_view_inputs);
    let publication = if is_published_consumer {
        let record = previous.as_ref().ok_or_else(|| {
            ApiError::conflict(
                "published demand control requires an existing validated bootstrap checkpoint",
            )
        })?;
        // An unfinished baseline intentionally has no committed causal cursor.
        // Preserve its validated page progress without claiming consumed input.
        let direct_inputs = if record
            .published_input_bootstrap
            .as_ref()
            .is_some_and(|bootstrap| !bootstrap.is_complete())
        {
            if record.checkpoint.causal_cut.is_some() || record.checkpoint.input_coverage.is_some()
            {
                return Err(ApiError::conflict(
                    "partial bootstrap control cannot claim completed input coverage",
                ));
            }
            Vec::new()
        } else {
            view_dependencies::consumer_direct_view_inputs(active, record)?
        };
        let mut proofs = Vec::with_capacity(direct_inputs.len());
        for input in &direct_inputs {
            let (_, proof) = checkpoint_publication::read_authoritative_cursor_checkpoint_header(
                state,
                &input.cursor.producer_tenant_id,
                &input.published_relation,
                &input.cursor,
            )
            .await?;
            proofs.push(proof);
        }
        DemandPublication {
            // A derived consumer has no physical source cut. Keep its frozen
            // baseline, direct cursors, and validated proofs on the pointer.
            source_cuts: None,
            bootstrap: record.published_input_bootstrap.clone(),
            direct_inputs,
            cursor_proofs: proofs,
            validated_source_publications: Vec::new(),
        }
    } else {
        let (cuts, _) = demand_source::capture_demand_source_artifacts(state, active).await?;
        cuts.into()
    };
    commit_demand_unit_locked(
        state,
        active,
        previous,
        progress,
        Vec::new(),
        publication,
        operation_guard,
    )
    .await
    .map(|_| ())
}

/// Commit one already bounded unit while the caller holds the view operation
/// lock. Progress and operator state share the existing fenced pointer CAS.
/// The caller must not install its speculative progress before this returns.
#[derive(Default)]
pub(super) struct DemandPublication {
    pub(super) source_cuts: Option<Vec<RelationIngestSourceIdentityCutV1>>,
    pub(super) validated_source_publications: Vec<recovery::ValidatedRelationPublication>,
    pub(super) bootstrap: Option<velorix_control::meta_admin::PublishedInputBootstrapProgressV1>,
    pub(super) direct_inputs: Vec<StandingRuntimeDirectViewInputV1>,
    pub(super) cursor_proofs: Vec<checkpoint_publication::ValidatedCursorProof>,
}

impl From<Vec<RelationIngestSourceIdentityCutV1>> for DemandPublication {
    fn from(source_cuts: Vec<RelationIngestSourceIdentityCutV1>) -> Self {
        Self {
            source_cuts: Some(source_cuts),
            ..Self::default()
        }
    }
}

pub(super) async fn commit_demand_unit_locked(
    state: &ApiState,
    active: &ActiveMaterializedView,
    previous: Option<StandingRuntimeCheckpointRecord>,
    progress: DemandCheckpoint,
    inputs: Vec<RelationInputBatch>,
    publication: impl Into<DemandPublication>,
    operation_guard: tokio::sync::OwnedMutexGuard<()>,
) -> Result<tokio::sync::OwnedMutexGuard<()>, ApiError> {
    let state = state.clone();
    let active = active.clone();
    let publication = publication.into();
    // Dropping the HTTP future detaches this already-started atomic unit. The
    // task retains the operation lock until blocking apply and publication
    // have both finished; no subsequent unit is scheduled by this task.
    tokio::spawn(async move {
        let result =
            commit_demand_unit_inner(&state, &active, previous, progress, inputs, publication)
                .await;
        if let Err(error) = result {
            if let Some(identity) = active_standing_runtime_identity(&active) {
                let removed = state
                    .standing_runtimes
                    .runtimes
                    .lock()
                    .map_err(|_| ApiError::internal("standing runtime registry lock poisoned"))?
                    .remove(&standing_runtime_key(identity, &active.spec.view_id));
                // Preserve local lease and committed-pointer bookkeeping.
                drop(removed);
            }
            return Err(error);
        }
        Ok(operation_guard)
    })
    .await
    .map_err(ApiError::internal)?
}

async fn commit_demand_unit_inner(
    state: &ApiState,
    active: &ActiveMaterializedView,
    previous: Option<StandingRuntimeCheckpointRecord>,
    progress: DemandCheckpoint,
    inputs: Vec<RelationInputBatch>,
    publication: DemandPublication,
) -> Result<(), ApiError> {
    let identity = active_standing_runtime_identity(active).ok_or_else(|| {
        ApiError::bad_request("demand materialization requires a standing runtime")
    })?;
    if progress.spec_hash != active.spec_hash {
        return Err(ApiError::conflict(
            "demand progress belongs to a different view specification",
        ));
    }
    let plan_hash = active
        .runtime
        .as_ref()
        .and_then(|binding| binding.logical_plan.as_ref())
        .and_then(|plan| plan.plan_hash.as_ref())
        .ok_or_else(|| ApiError::bad_request("demand materialization requires a plan hash"))?;
    if &progress.plan_hash != plan_hash {
        return Err(ApiError::conflict(
            "demand progress belongs to a different plan",
        ));
    }
    let next_epoch = previous
        .as_ref()
        .map_or(0, |record| record.checkpoint.logical_epoch)
        .checked_add(1)
        .ok_or_else(|| ApiError::bad_request("demand logical epoch overflow"))?;
    let progress_bytes = serde_json::to_vec(&progress).map_err(ApiError::internal)?;
    let key = EpochIdempotencyKey::new(format!(
        "demand:{next_epoch}:{}",
        stable_bytes_hash(&progress_bytes),
    ))
    .map_err(ApiError::bad_request)?;
    let lease =
        owner_lease::BackfillOwnerLease::start(state, identity, &active.spec.view_id).await?;
    if state
        .standing_runtime(identity, &active.spec.view_id)?
        .is_none()
    {
        // A demand unit applies only its selected delta. Cache-miss restore
        // must not independently replay historical eager input.
        if previous.is_some() {
            restore_standing_runtime_resident_locked(state, active).await?;
        } else {
            ensure_standing_runtime_for_active_view(state, active).await?;
        }
    }
    let runtime = state
        .standing_runtime(identity, &active.spec.view_id)?
        .ok_or_else(|| ApiError::service_unavailable("demand runtime unavailable"))?;
    let already_installed = progress.full_complete
        && (progress.artifacts.is_empty()
            || previous.as_ref().is_some_and(|record| {
                record
                    .demand_materialization
                    .as_ref()
                    .is_some_and(|saved| saved.full_complete)
            }));
    let demand_mode = progress
        .event_time_replay_id
        .as_ref()
        .filter(|_| !already_installed)
        .map(|replay_id| {
            use velorix_core::standing_program::{
                DemandFrontierMode, InputEventTimeFrontier, RelationFrontier,
            };
            if progress.full_complete {
                DemandFrontierMode::Install {
                    replay_id: replay_id.clone(),
                    input_frontiers: inputs
                        .iter()
                        .map(|input| RelationFrontier {
                            relation_id: input.relation_id.clone(),
                            relation_version: input.relation_version.clone(),
                            stream_id: input.stream_id.clone(),
                            partition_id: input.partition_id,
                            committed_offset_exclusive: input.end_offset_exclusive,
                        })
                        .collect(),
                    input_event_time_frontiers: inputs
                        .iter()
                        .filter_map(|input| {
                            input.event_time_watermark.as_ref().map(|watermark| {
                                InputEventTimeFrontier {
                                    relation_id: input.relation_id.clone(),
                                    relation_version: input.relation_version.clone(),
                                    schema_fingerprint: input.schema_fingerprint.clone(),
                                    stream_id: input.stream_id.clone(),
                                    partition_id: input.partition_id,
                                    event_time_column_id: watermark.event_time_column_id.clone(),
                                    max_observed_event_time_ns: watermark
                                        .max_observed_event_time_ns,
                                    watermark_ns: watermark.watermark_ns,
                                }
                            })
                        })
                        .collect(),
                }
            } else {
                DemandFrontierMode::Defer {
                    replay_id: replay_id.clone(),
                    output_readiness: progress.event_time_output_readiness.clone(),
                }
            }
        });
    // Install authenticates the completed frontier vectors in the mode; it
    // carries no row or watermark batch which could be interpreted as replay.
    let inputs = if matches!(
        &demand_mode,
        Some(velorix_core::standing_program::DemandFrontierMode::Install { .. })
    ) {
        Vec::new()
    } else {
        inputs
    };
    let applied = ingest_epoch::apply_standing_runtime_changes_and_checkpoint_with_demand_mode(
        state,
        runtime,
        next_epoch,
        key,
        inputs,
        StandingRuntimeBudgetLimits::from_state(state),
        demand_mode,
    )
    .await?;
    let publication = async {
        lease.ensure_owned()?;
        let progress_bytes = velorix_core::arrow_state::ArrowState::measure_serializable(
            &progress,
            state.max_standing_runtime_state_payload_bytes,
        )
        .map_err(ApiError::bad_request)?;
        let state_bytes = applied
            .checkpoint
            .state_payload
            .as_ref()
            .map_or(0, |payload| payload.payload.len());
        if progress_bytes
            .checked_add(state_bytes)
            .is_none_or(|bytes| bytes > state.max_standing_runtime_state_payload_bytes)
        {
            return Err(ApiError::payload_too_large(
                "operator state plus demand progress exceeds configured state budget",
            ));
        }
        progress.validate_checkpoint(&applied.checkpoint)?;
        let mut context =
            StandingRuntimeCheckpointPersistContext::new(previous, Vec::new(), lease.owner.clone())
                .with_demand_materialization(progress)
                .with_owner_renewal(Arc::clone(&lease.failure))
                .with_event_time_state(applied.staged_event_time_state)
                .with_published_relation(published_relation_binding_for_active_view(active)?);
        if let Some(cuts) = publication.source_cuts {
            context = context.with_expected_relation_source_cuts(cuts);
        }
        context =
            context.with_validated_relation_publications(publication.validated_source_publications);
        context.published_input_bootstrap = publication.bootstrap;
        context.direct_view_inputs = publication.direct_inputs;
        context.validated_cursor_proofs = publication.cursor_proofs;
        persist_standing_runtime_checkpoint(
            state,
            &active.spec.view_id,
            &applied.checkpoint,
            &applied.output_deltas,
            context,
            None,
        )
        .await?;
        Ok::<(), ApiError>(())
    }
    .await;
    // A failed/ambiguous CAS is resolved by the existing validated restore
    // path. No speculative rows or scope certificate remain resident.
    publication?;
    Ok(())
}

/// HTTP-only envelope: existing Rust callers of QueryViewRequest stay compatible.
#[derive(Clone, Debug, Deserialize)]
pub(super) struct DemandQueryRequest {
    #[serde(flatten)]
    pub(super) query: QueryViewRequest,
    #[serde(default)]
    pub(super) materialization: DemandRequest,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DemandRequest {
    #[serde(default)]
    pub(super) scope: Option<DemandScope>,
    #[serde(default)]
    pub(super) budget: DemandBudget,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DemandScope {
    #[serde(rename = "where")]
    pub(super) predicate: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub(super) struct DemandBudget {
    pub(super) batch_limit: usize,
    pub(super) max_rows: usize,
    pub(super) max_bytes: usize,
    /// Cooperative deadline, checked between atomic checkpoint steps.
    pub(super) max_duration_ms: u64,
}

impl Default for DemandBudget {
    fn default() -> Self {
        Self {
            batch_limit: 1,
            max_rows: 4_000,
            max_bytes: 64 * 1024 * 1024,
            max_duration_ms: 1_000,
        }
    }
}

impl DemandBudget {
    pub(super) fn validate(&self) -> Result<(), ApiError> {
        if self.batch_limit == 0
            || self.max_rows == 0
            || self.max_bytes == 0
            || self.max_duration_ms == 0
        {
            return Err(ApiError::bad_request(
                "materialization budgets must be positive",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(super) enum DemandOrder {
    #[default]
    Forward,
    Reverse,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(super) enum DemandAction {
    #[default]
    Step,
    Pause,
    Resume,
}

/// Foreground demand has precedence at checkpoint boundaries. Explicit work
/// yields instead of entering a background queue or interrupting a commit.
#[derive(Default)]
pub(super) struct DemandScheduler {
    foreground: AtomicUsize,
    step: Arc<AsyncMutex<()>>,
}

pub(super) struct DemandTurn {
    _step: tokio::sync::OwnedMutexGuard<()>,
    _foreground: Option<ForegroundDemand>,
}

struct ForegroundDemand(Arc<DemandScheduler>);

impl Drop for ForegroundDemand {
    fn drop(&mut self) {
        self.0.foreground.fetch_sub(1, Ordering::AcqRel);
    }
}

impl DemandScheduler {
    pub(super) async fn query_turn(self: &Arc<Self>) -> DemandTurn {
        self.foreground.fetch_add(1, Ordering::AcqRel);
        // Keep this guard across the await so cancellation removes the waiter.
        let foreground = ForegroundDemand(Arc::clone(self));
        DemandTurn {
            _step: Arc::clone(&self.step).lock_owned().await,
            _foreground: Some(foreground),
        }
    }

    pub(super) fn explicit_turn(self: &Arc<Self>) -> Option<DemandTurn> {
        if self.foreground.load(Ordering::Acquire) != 0 {
            return None;
        }
        let step = Arc::clone(&self.step).try_lock_owned().ok()?;
        if self.foreground.load(Ordering::Acquire) != 0 {
            return None;
        }
        Some(DemandTurn {
            _step: step,
            _foreground: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sparse_inspection_preserves_signed_rows_and_does_not_reapply_overlap() {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("value", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("weight", arrow::datatypes::DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow::array::Int64Array::from(vec![7, 9, 7, 11])),
                Arc::new(arrow::array::Int64Array::from(vec![1, 2, -1, 1])),
            ],
        )
        .unwrap();
        let input = RelationInputBatch {
            relation_id: "events".into(),
            relation_version: "v1".into(),
            stream_id: "events".into(),
            partition_id: 0,
            schema_fingerprint: "schema".into(),
            start_offset_inclusive: 10,
            end_offset_exclusive: 14,
            event_time_watermark: None,
            encoding: RelationInputEncodingV1::SourceRelationV1,
            batches: vec![batch],
        };
        let initial = DemandArtifactProgress {
            artifact_key: "artifact".into(),
            artifact_hash: "hash".into(),
            applied: ProcessedSpans::new(4),
            inspected: BTreeMap::new(),
            source_watermark: None,
        };
        let (a, selected) = initial
            .stage_inspection(&input, "a", [0, 4], &[true, false, true, false])
            .unwrap();
        let runs = initial
            .selected_source_runs(&input, [0, 4], &[true, false, true, false])
            .unwrap();
        assert_eq!(
            runs.iter()
                .map(|run| (run.start_offset_inclusive, run.end_offset_exclusive))
                .collect::<Vec<_>>(),
            vec![(10, 11), (12, 13)]
        );
        assert!(runs.iter().all(|run| run
            .batches
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>()
            == 1));
        let uncovered = a.selected_source_runs(&input, [0, 4], &[true; 4]).unwrap();
        assert_eq!(
            uncovered
                .iter()
                .map(|run| (run.start_offset_inclusive, run.end_offset_exclusive))
                .collect::<Vec<_>>(),
            vec![(11, 12), (13, 14)]
        );
        let evidence_only = a.selected_source_runs(&input, [0, 4], &[false; 4]).unwrap();
        assert!(evidence_only[0].batches.is_empty());
        assert_eq!(
            evidence_only[0].start_offset_inclusive,
            evidence_only[0].end_offset_exclusive
        );
        assert_eq!(
            selected
                .batches
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            2
        );
        assert_eq!(
            selected.batches[1]
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .value(0),
            -1
        );
        assert_eq!(selected.end_offset_exclusive, 0);
        assert_eq!(a.applied.contiguous_frontier(), 1);
        assert_eq!(initial.applied.processed_len(), 0);
        let (both, selected) = a
            .stage_inspection(&input, "all", [0, 4], &[true; 4])
            .unwrap();
        assert_eq!(
            selected
                .batches
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            2
        );
        assert!(both.applied.is_complete());
        let restored: DemandArtifactProgress =
            serde_json::from_slice(&serde_json::to_vec(&both).unwrap()).unwrap();
        let (_, replay) = restored
            .stage_inspection(&input, "all", [0, 4], &[true; 4])
            .unwrap();
        assert_eq!(replay.batches.len(), 1);
        assert_eq!(replay.batches[0].num_rows(), 0);
        assert_eq!(replay.batches[0].schema(), input.batches[0].schema());
        assert!(restored
            .stage_inspection(&input, "bad", [0, 5], &[true; 5])
            .is_err());
    }

    #[test]
    fn inspection_order_preserves_gaps_and_scope_independence() {
        let progress = DemandArtifactProgress {
            artifact_key: "artifact".into(),
            artifact_hash: "digest".into(),
            applied: ProcessedSpans::from_spans(8, vec![[1, 3], [6, 8]]).unwrap(),
            inspected: BTreeMap::from([(
                "a".into(),
                ProcessedSpans::from_spans(8, vec![[0, 3], [6, 8]]).unwrap(),
            )]),
            source_watermark: None,
        };
        assert_eq!(
            progress.next_inspection("a", DemandOrder::Forward, 2),
            Some([3, 5])
        );
        assert_eq!(
            progress.next_inspection("a", DemandOrder::Reverse, 2),
            Some([4, 6])
        );
        // An unrelated scope still inspects previously excluded rows. Already
        // applied rows are skipped at application, not at predicate inspection.
        assert_eq!(
            progress.next_inspection("b", DemandOrder::Forward, 2),
            Some([0, 2])
        );
        assert_eq!(progress.next_inspection("a", DemandOrder::Forward, 0), None);
        assert_eq!(progress.applied.contiguous_frontier(), 0);
    }
}
