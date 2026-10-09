use super::*;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ColdWindowAccumulator {
    schema_version: u32,
    runtime_binding: String,
    window_key: String,
    logical_epoch: LogicalEpoch,
    row: TumblingWindowStateRow,
}

pub struct TumblingEventTimeAggregateRuntime {
    identity: StandingProgramIdentity,
    catalog: VelorixRelationCatalogV1,
    input_schema: RelationSchema,
    output_schema: RelationSchema,
    view_sql: String,
    plan: SupportedTumblingWindowPlan,
    logical_plan: VelorixLogicalViewPlanV1,
    state: TumblingWindowState,
    published_output: DeltaBatch,
    correction_output: BTreeMap<String, DeltaRecord>,
    windows_by_end: BTreeMap<i64, BTreeSet<String>>,
    cold_state_refs: BTreeMap<String, EventTimeStateRef>,
    legacy_replay_before_ns: Option<i64>,
    input_frontiers: Vec<RelationFrontier>,
    input_event_time_frontiers: Vec<InputEventTimeFrontier>,
    applied_epochs: BTreeMap<String, LogicalEpoch>,
    logical_epoch: LogicalEpoch,
}

impl TumblingEventTimeAggregateRuntime {
    pub fn new_with_logical_plan(
        identity: StandingProgramIdentity,
        catalog: VelorixRelationCatalogV1,
        input_schema: RelationSchema,
        output_schema: RelationSchema,
        view_sql: String,
        plan: SupportedTumblingWindowPlan,
        logical_plan: VelorixLogicalViewPlanV1,
    ) -> Result<Self, StandingProgramRuntimeError> {
        identity.validate()?;
        validate_builtin_runtime_identity(&identity)?;
        validate_view_sql_hash(&identity, view_sql.as_str())?;
        validate_logical_view_plan(&logical_plan).map_err(|_| {
            StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "logical_tumbling_window_view_plan",
            }
        })?;
        validate_tumbling_supported_schemas(&catalog, &input_schema, &output_schema, &plan)?;
        let compiled_plan = validate_supported_tumbling_window_sql_with_policy(
            view_sql.as_str(),
            &catalog,
            plan.late_row_policy,
        )
        .map_err(|_| StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "tumbling_window_view_plan",
        })?;
        if compiled_plan != plan {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "tumbling_window_view_plan",
            });
        }
        let compiled_logical_plan =
            lower_supported_tumbling_window_sql_to_logical_plan_with_policy(
                view_sql.as_str(),
                &catalog,
                &output_schema,
                plan.late_row_policy,
            )
            .map_err(|_| StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "logical_tumbling_window_view_plan",
            })?;
        if compiled_logical_plan != logical_plan {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "logical_tumbling_window_view_plan",
            });
        }
        Ok(Self {
            identity,
            catalog,
            input_schema,
            output_schema,
            view_sql,
            plan,
            logical_plan,
            state: TumblingWindowState::default(),
            published_output: DeltaBatch::default(),
            correction_output: BTreeMap::new(),
            windows_by_end: BTreeMap::new(),
            cold_state_refs: BTreeMap::new(),
            legacy_replay_before_ns: None,
            input_frontiers: Vec::new(),
            input_event_time_frontiers: Vec::new(),
            applied_epochs: BTreeMap::new(),
            logical_epoch: 0,
        })
    }

    fn output_schema_fingerprint(&self) -> String {
        self.output_schema.schema_fingerprint.clone()
    }

    fn correction_horizon(&self) -> Option<i64> {
        match self.plan.late_row_policy {
            Some(LateRowPolicy::CorrectWithinHorizon { horizon_ns }) => Some(horizon_ns),
            _ => None,
        }
    }

    fn runtime_binding(&self) -> Result<String, StandingProgramRuntimeError> {
        serde_json::to_vec(&(
            &self.identity,
            &self.catalog,
            &self.input_schema,
            &self.output_schema,
            &self.logical_plan,
            &self.plan,
            "fixed_window_cold_v1",
        ))
        .map(|bytes| stable_bytes_hash(&bytes))
        .map_err(|_| invalid_runtime_state())
    }

    fn cold_object_key(binding: &str, reference: &EventTimeStateRef) -> String {
        // ponytail: cold objects bypass legacy GC; require retained-checkpoint ref traversal before reclamation.
        format!(
            "v1/state/materialized-view-runtime/{binding}/windows/{}/{}/{}",
            stable_bytes_hash(reference.window_key.as_bytes()),
            reference.logical_epoch,
            reference.state_root.content_hash,
        )
    }

    fn cold_object(
        &self,
        key: &str,
        row: &TumblingWindowStateRow,
    ) -> Result<EventTimeStateObject, StandingProgramRuntimeError> {
        self.cold_object_at(key, row, self.logical_epoch)
    }

    fn cold_object_at(
        &self,
        key: &str,
        row: &TumblingWindowStateRow,
        logical_epoch: LogicalEpoch,
    ) -> Result<EventTimeStateObject, StandingProgramRuntimeError> {
        let binding = self.runtime_binding()?;
        let payload = serde_json::to_string(&ColdWindowAccumulator {
            schema_version: 1,
            runtime_binding: binding.clone(),
            window_key: key.to_string(),
            logical_epoch,
            row: row.clone(),
        })
        .map_err(|_| invalid_runtime_state())?;
        let mut reference = EventTimeStateRef {
            window_key: key.to_string(),
            logical_epoch,
            state_root: DurableStateRoot {
                object_key: String::new(),
                content_hash: stable_bytes_hash(payload.as_bytes()),
            },
        };
        reference.state_root.object_key = Self::cold_object_key(&binding, &reference);
        Ok(EventTimeStateObject { reference, payload })
    }

    fn state_request(&self, key: &str) -> Option<EventTimeStateRequest> {
        if self.state.rows.contains_key(key) {
            return None;
        }
        let reference = self.cold_state_refs.get(key).cloned();
        let needs_replay = self
            .legacy_replay_before_ns
            .is_some_and(|cutoff| window_key_bounds(key).is_ok_and(|(_, end)| end <= cutoff));
        (reference.is_some() || needs_replay).then(|| EventTimeStateRequest {
            window_key: key.to_string(),
            logical_epoch: self.logical_epoch,
            reference,
        })
    }

    fn input_is_committed(&self, input: &RelationInputBatch) -> bool {
        input.start_offset_inclusive <= input.end_offset_exclusive
            && self.input_frontiers.iter().any(|frontier| {
                frontier.relation_id == input.relation_id
                    && frontier.relation_version == input.relation_version
                    && frontier.stream_id == input.stream_id
                    && frontier.partition_id == input.partition_id
                    && input.end_offset_exclusive <= frontier.committed_offset_exclusive
            })
    }

    fn validate_hydrated_row(
        &self,
        key: &str,
        row: &TumblingWindowStateRow,
    ) -> Result<(), StandingProgramRuntimeError> {
        validate_correction_state_row(key, row, &self.plan)?;
        let selected = TumblingWindowState {
            rows: BTreeMap::from([(key.to_string(), row.clone())]),
            ..Default::default()
        };
        let output = selected.closed_delta(
            &self.plan,
            &self.output_schema,
            min_event_time_watermark(&self.input_event_time_frontiers),
        )?;
        if output.records().first() != self.correction_output.get(key) {
            return Err(event_time_state_error("event_time_state_output_mismatch"));
        }
        Ok(())
    }

    fn install_hydrated_row(&mut self, key: String, row: TumblingWindowStateRow) {
        self.windows_by_end
            .entry(row.window_end_ns)
            .or_default()
            .insert(key.clone());
        self.state.rows.insert(key, row);
    }

    fn snapshot_output(&self) -> std::borrow::Cow<'_, DeltaBatch> {
        if self.correction_horizon().is_some() {
            std::borrow::Cow::Owned(DeltaBatch::from_records(
                self.correction_output.values().cloned(),
            ))
        } else {
            std::borrow::Cow::Borrowed(&self.published_output)
        }
    }

    fn correction_delta(
        &self,
        undo: &TumblingEpochUndoLog,
        watermark: Option<i64>,
    ) -> Result<DeltaBatch, StandingProgramRuntimeError> {
        let affected = correction_affected_windows(
            undo.rows.keys().cloned(),
            &self.windows_by_end,
            min_event_time_watermark(&self.input_event_time_frontiers),
            watermark,
        );
        let selected = TumblingWindowState {
            rows: affected
                .iter()
                .filter_map(|key| {
                    self.state
                        .rows
                        .get(key)
                        .map(|row| (key.clone(), row.clone()))
                })
                .collect(),
            ..Default::default()
        };
        let next = selected.closed_delta(&self.plan, &self.output_schema, watermark)?;
        let before = DeltaBatch::from_records(
            affected
                .iter()
                .filter_map(|key| self.correction_output.get(key).cloned()),
        );
        before
            .diff(&next)
            .and_then(|delta| delta.net_rows())
            .map(DeltaBatch::from_records)
            .map_err(|_| invalid_runtime_state())
    }

    fn commit_correction(&mut self, undo: &TumblingEpochUndoLog, delta: &DeltaBatch) {
        // Remove old versions before inserting replacements for the same window.
        for record in delta.records().iter().filter(|record| record.weight < 0) {
            self.correction_output
                .remove(&canonical_json(record.key.as_json()));
        }
        for record in delta.records().iter().filter(|record| record.weight > 0) {
            self.correction_output
                .insert(canonical_json(record.key.as_json()), record.clone());
        }
        for key in undo.rows.keys() {
            if let Some(row) = self.state.rows.get(key) {
                self.windows_by_end
                    .entry(row.window_end_ns)
                    .or_default()
                    .insert(key.clone());
            }
        }
        // Updated state stays hot until its replacement object is durably acknowledged.
        for key in undo.rows.keys() {
            self.cold_state_refs.remove(key);
        }
    }

    fn materialized_batch(&self) -> Result<RecordBatch, StandingProgramRuntimeError> {
        materialized_tumbling_delta_to_record_batch(
            &self.output_schema,
            &self.snapshot_output(),
            &self.plan.aggregate_outputs,
        )
    }

    fn materialized_page_batch(
        &self,
        page: SnapshotPageRequest,
    ) -> Result<(RecordBatch, Option<String>), StandingProgramRuntimeError> {
        materialized_tumbling_delta_page_batch(
            &self.output_schema,
            &self.snapshot_output(),
            &self.plan.aggregate_outputs,
            self.logical_epoch,
            page,
        )
    }

    fn checkpoint_payload(&self) -> Result<String, StandingProgramRuntimeError> {
        let payload = TumblingWindowCheckpointPayload {
            schema_version: CHECKPOINT_PAYLOAD_SCHEMA_VERSION,
            runtime_kind: TUMBLING_WINDOW_RUNTIME_KIND.to_string(),
            catalog: self.catalog.clone(),
            input_schema: self.input_schema.clone(),
            output_schema: self.output_schema.clone(),
            view_sql: self.view_sql.clone(),
            plan: self.plan.clone(),
            logical_plan: self.logical_plan.clone(),
            input_frontiers: self.input_frontiers.clone(),
            input_event_time_frontiers: self.input_event_time_frontiers.clone(),
            state: TumblingWindowState {
                rows: self
                    .state
                    .rows
                    .iter()
                    .filter(|(key, _)| !self.cold_state_refs.contains_key(*key))
                    .map(|(key, row)| (key.clone(), row.clone()))
                    .collect(),
                session_events: self.state.session_events.clone(),
                late_rows_dropped: self.state.late_rows_dropped,
            },
            cold_state_version: self.correction_horizon().map(|_| 1),
            cold_state_refs: self.cold_state_refs.clone(),
            legacy_replay_before_ns: self.legacy_replay_before_ns,
            published_output: self.snapshot_output().into_owned(),
            applied_epochs: self
                .applied_epochs
                .iter()
                .map(|(idempotency_key, logical_epoch)| GenericAppliedEpoch {
                    idempotency_key: idempotency_key.clone(),
                    logical_epoch: *logical_epoch,
                })
                .collect(),
            logical_epoch: self.logical_epoch,
        };
        serde_json::to_string(&payload).map_err(|_| invalid_checkpoint())
    }

    fn restore_payload(
        checkpoint: &RuntimeCheckpoint,
    ) -> Result<TumblingWindowCheckpointPayload, StandingProgramRuntimeError> {
        let Some(state_payload) = &checkpoint.state_payload else {
            return Err(invalid_checkpoint());
        };
        if state_payload.codec_identity != checkpoint.checkpoint_codec_identity {
            return Err(StandingProgramRuntimeError::CheckpointCodecMismatch {
                expected: checkpoint.checkpoint_codec_identity.clone(),
                actual: state_payload.codec_identity.clone(),
            });
        }
        let mut payload: TumblingWindowCheckpointPayload =
            serde_json::from_str(&state_payload.payload).map_err(|_| invalid_checkpoint())?;
        if payload.schema_version != CHECKPOINT_PAYLOAD_SCHEMA_VERSION
            || payload.runtime_kind != TUMBLING_WINDOW_RUNTIME_KIND
        {
            return Err(invalid_checkpoint());
        }
        validate_tumbling_supported_schemas(
            &payload.catalog,
            &payload.input_schema,
            &payload.output_schema,
            &payload.plan,
        )?;
        if let Some(LateRowPolicy::CorrectWithinHorizon { horizon_ns }) =
            payload.plan.late_row_policy
        {
            if stable_bytes_hash(state_payload.payload.as_bytes())
                != checkpoint.state_root.content_hash
            {
                return Err(invalid_checkpoint());
            }
            let watermark = min_event_time_watermark(&payload.input_event_time_frontiers);
            let cutoff = watermark.map(|watermark| watermark.saturating_sub(horizon_ns));
            match payload.cold_state_version {
                None if payload.cold_state_refs.is_empty()
                    && payload.legacy_replay_before_ns.is_none() =>
                {
                    // Older draft checkpoints discarded auxiliary state at the horizon.
                    // Even HAVING-hidden windows need replay, so preserve the entire old boundary.
                    payload.legacy_replay_before_ns = cutoff;
                }
                Some(1) => {}
                _ => return Err(invalid_checkpoint()),
            }
            if payload
                .legacy_replay_before_ns
                .is_some_and(|legacy| cutoff.is_none_or(|cutoff| legacy > cutoff))
            {
                return Err(invalid_checkpoint());
            }
            let binding = stable_bytes_hash(
                &serde_json::to_vec(&(
                    &checkpoint.identity,
                    &payload.catalog,
                    &payload.input_schema,
                    &payload.output_schema,
                    &payload.logical_plan,
                    &payload.plan,
                    "fixed_window_cold_v1",
                ))
                .map_err(|_| invalid_checkpoint())?,
            );
            for (key, reference) in &payload.cold_state_refs {
                let (start, end) = window_key_bounds(key)?;
                if reference.window_key != *key
                    || payload.state.rows.contains_key(key)
                    || reference.logical_epoch > payload.logical_epoch
                    || !reference
                        .state_root
                        .content_hash
                        .strip_prefix("sha256:")
                        .is_some_and(|hash| {
                            hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit())
                        })
                    || reference.state_root.object_key != Self::cold_object_key(&binding, reference)
                    || cutoff.is_none_or(|cutoff| end > cutoff)
                    || !fixed_window_assignments(&payload.plan, start)?.contains(&(start, end))
                {
                    return Err(invalid_checkpoint());
                }
            }
            let expected =
                payload
                    .state
                    .closed_delta(&payload.plan, &payload.output_schema, watermark)?;
            let expected: BTreeMap<String, DeltaRecord> = expected
                .records()
                .iter()
                .map(|record| (canonical_json(record.key.as_json()), record.clone()))
                .collect();
            let mut output = BTreeMap::new();
            for record in payload.published_output.records() {
                let key = canonical_json(record.key.as_json());
                let Some(end) = record.key.as_json().get(2).and_then(Value::as_i64) else {
                    return Err(invalid_checkpoint());
                };
                let Some(value) = record.value.as_json().as_object() else {
                    return Err(invalid_checkpoint());
                };
                if record.weight != 1
                    || value.len() != payload.plan.aggregate_outputs.len()
                    || payload
                        .plan
                        .aggregate_outputs
                        .iter()
                        .any(|aggregate| !value.contains_key(&aggregate.output_column_id))
                    || output.insert(key.clone(), record).is_some()
                    || watermark.is_none_or(|watermark| end > watermark)
                    || (!payload.state.rows.contains_key(&key)
                        && !payload.cold_state_refs.contains_key(&key)
                        && !payload
                            .legacy_replay_before_ns
                            .is_some_and(|cutoff| end <= cutoff))
                {
                    return Err(invalid_checkpoint());
                }
            }
            for (key, row) in &payload.state.rows {
                validate_correction_state_row(key, row, &payload.plan)?;
                if expected.get(key) != output.get(key).copied() {
                    return Err(invalid_checkpoint());
                }
            }
            materialized_tumbling_delta_to_record_batch(
                &payload.output_schema,
                &payload.published_output,
                &payload.plan.aggregate_outputs,
            )?;
        } else if payload.cold_state_version.is_some()
            || !payload.cold_state_refs.is_empty()
            || payload.legacy_replay_before_ns.is_some()
        {
            return Err(invalid_checkpoint());
        }
        Ok(payload)
    }
}

fn correction_affected_windows(
    touched: impl Iterator<Item = String>,
    windows_by_end: &BTreeMap<i64, BTreeSet<String>>,
    previous_watermark: Option<i64>,
    watermark: Option<i64>,
) -> BTreeSet<String> {
    let mut affected: BTreeSet<String> = touched.collect();
    if let Some(watermark) = watermark {
        if previous_watermark.is_some_and(|previous| watermark <= previous) {
            return affected;
        }
        let lower =
            previous_watermark.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded);
        for keys in windows_by_end
            .range((lower, std::ops::Bound::Included(watermark)))
            .map(|(_, keys)| keys)
        {
            affected.extend(keys.iter().cloned());
        }
    }
    affected
}

fn event_time_state_error(field: &'static str) -> StandingProgramRuntimeError {
    StandingProgramRuntimeError::InvalidProgramIdentity { field }
}

fn window_key_bounds(key: &str) -> Result<(i64, i64), StandingProgramRuntimeError> {
    let value: Value = serde_json::from_str(key).map_err(|_| invalid_checkpoint())?;
    let array = value.as_array().ok_or_else(invalid_checkpoint)?;
    if array.len() != 3 || canonical_json(&value) != key {
        return Err(invalid_checkpoint());
    }
    Ok((
        array[1].as_i64().ok_or_else(invalid_checkpoint)?,
        array[2].as_i64().ok_or_else(invalid_checkpoint)?,
    ))
}

fn validate_correction_state_row(
    key: &str,
    row: &TumblingWindowStateRow,
    plan: &SupportedTumblingWindowPlan,
) -> Result<(), StandingProgramRuntimeError> {
    let bounds = window_key_bounds(key)?;
    if key != window_state_key(&row.group_key, row.window_start_ns, row.window_end_ns)
        || !fixed_window_assignments(plan, row.window_start_ns)?.contains(&bounds)
        || row.net_count < 0
        || row.avg_counts.values().any(|count| *count < 0)
        || row
            .extrema_values
            .values()
            .any(|values| values.values().any(|count| *count <= 0))
        || plan.aggregate_outputs.iter().any(|aggregate| {
            aggregate.function == LogicalPlanAggregateFunctionV1::Count
                && row
                    .values
                    .get(&aggregate.output_column_id)
                    .is_some_and(|count| *count < 0)
        })
    {
        return Err(invalid_checkpoint());
    }
    let ids: BTreeSet<_> = plan
        .aggregate_outputs
        .iter()
        .map(|aggregate| &aggregate.output_column_id)
        .collect();
    if row
        .values
        .keys()
        .chain(row.avg_sums.keys())
        .chain(row.avg_counts.keys())
        .chain(row.extrema_values.keys())
        .any(|id| !ids.contains(id))
        || row.avg_sums.keys().ne(row.avg_counts.keys())
    {
        return Err(invalid_checkpoint());
    }
    Ok(())
}

impl StandingProgramRuntime for TumblingEventTimeAggregateRuntime {
    fn program_identity(&self) -> &StandingProgramIdentity {
        &self.identity
    }

    fn input_schemas(&self) -> Vec<RelationSchema> {
        vec![self.input_schema.clone()]
    }

    fn output_schemas(&self) -> Vec<RelationSchema> {
        vec![self.output_schema.clone()]
    }

    fn logical_epoch(&self) -> LogicalEpoch {
        self.logical_epoch
    }

    fn event_time_state_requests(
        &self,
        inputs: &[RelationInputBatch],
    ) -> Result<Vec<EventTimeStateRequest>, StandingProgramRuntimeError> {
        self.validate_event_time_corrections(inputs)?;
        if self.correction_horizon().is_none() {
            return Ok(Vec::new());
        }
        let key_column = catalog_primary_key_column(&self.catalog)?;
        let value_column = catalog_column_by_id(&self.catalog, &self.plan.sum_value_column_id)?;
        let event_column = catalog_column_by_id(&self.catalog, &self.plan.event_time_column_id)?;
        let weight_column = catalog_column_by_id(
            &self.catalog,
            &self.catalog.relation_schema.weight_column_id,
        )?;
        let mut keys = BTreeSet::new();
        for input in inputs
            .iter()
            .filter(|input| !self.input_is_committed(input))
        {
            for batch in &input.batches {
                let key_index = batch_column_index(batch, &key_column.name)?;
                let value_index = batch_column_index(batch, &value_column.name)?;
                let event_index = batch_column_index(batch, &event_column.name)?;
                let weight_index = batch_column_index(batch, &weight_column.name)?;
                for index in 0..batch.num_rows() {
                    if batch_int64_value(batch, weight_index, index)? == 0 {
                        continue;
                    }
                    let group =
                        batch_key_value(batch, key_index, &key_column.physical_arrow_type, index)?;
                    let amount = batch_nullable_int64_value(batch, value_index, index)?;
                    let event = batch_event_time_ns(
                        batch,
                        event_index,
                        &event_column.physical_arrow_type,
                        index,
                    )?;
                    if !tumbling_predicate_matches_row(
                        &self.plan.predicate_expr,
                        &self.catalog,
                        &self.plan,
                        &group,
                        amount,
                        event,
                    )? || !window_row_matches_any_aggregate_filter(
                        &self.catalog,
                        &self.plan,
                        &group,
                        amount,
                        event,
                    )? {
                        continue;
                    }
                    for (start, end) in fixed_window_assignments(&self.plan, event)? {
                        keys.insert(window_state_key(&group, start, end));
                    }
                }
            }
        }
        Ok(keys
            .iter()
            .filter_map(|key| self.state_request(key))
            .collect())
    }

    fn hydrate_event_time_state(
        &mut self,
        request: &EventTimeStateRequest,
        payload: &str,
    ) -> Result<(), StandingProgramRuntimeError> {
        if self.state_request(&request.window_key).as_ref() != Some(request) {
            return Err(event_time_state_error("event_time_state_stale_request"));
        }
        let reference = request
            .reference
            .as_ref()
            .ok_or_else(|| event_time_state_error("event_time_state_requires_source_replay"))?;
        if stable_bytes_hash(payload.as_bytes()) != reference.state_root.content_hash {
            return Err(event_time_state_error("event_time_state_content_hash"));
        }
        let accumulator: ColdWindowAccumulator =
            serde_json::from_str(payload).map_err(|_| invalid_checkpoint())?;
        let binding = self.runtime_binding()?;
        if accumulator.schema_version != 1
            || accumulator.runtime_binding != binding
            || accumulator.window_key != request.window_key
            || accumulator.logical_epoch != reference.logical_epoch
            || reference.state_root.object_key != Self::cold_object_key(&binding, reference)
        {
            return Err(event_time_state_error("event_time_state_identity"));
        }
        self.validate_hydrated_row(&request.window_key, &accumulator.row)?;
        self.install_hydrated_row(request.window_key.clone(), accumulator.row);
        Ok(())
    }

    fn reconstruct_event_time_state(
        &mut self,
        request: &EventTimeStateRequest,
        inputs: &[RelationInputBatch],
    ) -> Result<(), StandingProgramRuntimeError> {
        if request.reference.is_some()
            || self.state_request(&request.window_key).as_ref() != Some(request)
        {
            return Err(event_time_state_error(
                "event_time_state_stale_replay_request",
            ));
        }
        let (start, end) = window_key_bounds(&request.window_key)?;
        let key: Value =
            serde_json::from_str(&request.window_key).map_err(|_| invalid_checkpoint())?;
        let mut state = TumblingWindowState::default();
        window_state_row_mut(&mut state, key[0].clone(), start, end);
        let selected = BTreeSet::from([request.window_key.clone()]);
        let mut undo = TumblingEpochUndoLog::default();
        let mut covered = BTreeMap::new();
        for input in inputs {
            validate_input_matches_schema(
                input,
                &self.input_schema,
                "tumbling_event_time_replay_relation",
            )?;
            let frontier = self
                .input_frontiers
                .iter()
                .find(|frontier| {
                    frontier.relation_id == input.relation_id
                        && frontier.relation_version == input.relation_version
                        && frontier.stream_id == input.stream_id
                        && frontier.partition_id == input.partition_id
                })
                .ok_or_else(|| event_time_state_error("event_time_state_replay_coverage"))?;
            let next = covered
                .entry((input.stream_id.clone(), input.partition_id))
                .or_insert(0);
            if input.start_offset_inclusive != *next
                || input.end_offset_exclusive < *next
                || input.end_offset_exclusive > frontier.committed_offset_exclusive
            {
                return Err(event_time_state_error("event_time_state_replay_coverage"));
            }
            *next = input.end_offset_exclusive;
            apply_tumbling_input(
                &mut state,
                &mut undo,
                &self.catalog,
                &self.plan,
                &[],
                input,
                Some(&selected),
            )?;
        }
        if self.input_frontiers.iter().any(|frontier| {
            covered
                .get(&(frontier.stream_id.clone(), frontier.partition_id))
                .copied()
                .unwrap_or(0)
                != frontier.committed_offset_exclusive
        }) {
            return Err(event_time_state_error("event_time_state_replay_coverage"));
        }
        let row = state
            .rows
            .remove(&request.window_key)
            .ok_or_else(invalid_runtime_state)?;
        self.validate_hydrated_row(&request.window_key, &row)?;
        self.install_hydrated_row(request.window_key.clone(), row);
        Ok(())
    }

    fn export_event_time_state(
        &self,
    ) -> Result<Vec<EventTimeStateObject>, StandingProgramRuntimeError> {
        let (Some(watermark), Some(horizon)) = (
            min_event_time_watermark(&self.input_event_time_frontiers),
            self.correction_horizon(),
        ) else {
            return Ok(Vec::new());
        };
        let mut objects = Vec::new();
        let mut bytes = 0usize;
        for key in self
            .windows_by_end
            .range(..=watermark.saturating_sub(horizon))
            .flat_map(|(_, keys)| keys)
            .filter(|key| !self.cold_state_refs.contains_key(*key))
        {
            let object =
                self.cold_object(key, self.state.rows.get(key).expect("hot window index"))?;
            // A single accumulator is indivisible; batches otherwise stay below 4 MiB / 64 windows.
            if !objects.is_empty() && bytes.saturating_add(object.payload.len()) > 4 * 1024 * 1024 {
                break;
            }
            bytes = bytes.saturating_add(object.payload.len());
            objects.push(object);
            if objects.len() == 64 {
                break;
            }
        }
        Ok(objects)
    }

    fn stage_event_time_state(
        &mut self,
        references: &[EventTimeStateRef],
    ) -> Result<(), StandingProgramRuntimeError> {
        let (Some(watermark), Some(horizon)) = (
            min_event_time_watermark(&self.input_event_time_frontiers),
            self.correction_horizon(),
        ) else {
            return if references.is_empty() {
                Ok(())
            } else {
                Err(event_time_state_error("event_time_state_stale_export"))
            };
        };
        let mut seen = BTreeSet::new();
        for reference in references {
            let row = self
                .state
                .rows
                .get(&reference.window_key)
                .ok_or_else(|| event_time_state_error("event_time_state_stale_export"))?;
            if row.window_end_ns > watermark.saturating_sub(horizon)
                || !seen.insert(&reference.window_key)
                || self.cold_object(&reference.window_key, row)?.reference != *reference
            {
                return Err(event_time_state_error("event_time_state_stale_export"));
            }
        }
        for reference in references {
            self.cold_state_refs
                .insert(reference.window_key.clone(), reference.clone());
        }
        Ok(())
    }

    fn acknowledge_event_time_state(
        &mut self,
        references: &[EventTimeStateRef],
    ) -> Result<(), StandingProgramRuntimeError> {
        let mut seen = BTreeSet::new();
        for reference in references {
            let row = self
                .state
                .rows
                .get(&reference.window_key)
                .ok_or_else(|| event_time_state_error("event_time_state_stale_export"))?;
            if !seen.insert(&reference.window_key)
                || self.cold_state_refs.get(&reference.window_key) != Some(reference)
                || self
                    .cold_object_at(&reference.window_key, row, reference.logical_epoch)?
                    .reference
                    != *reference
            {
                return Err(event_time_state_error("event_time_state_stale_export"));
            }
        }
        for reference in references {
            let row = self
                .state
                .rows
                .remove(&reference.window_key)
                .expect("validated hot row");
            let keys = self
                .windows_by_end
                .get_mut(&row.window_end_ns)
                .expect("hot window index");
            keys.remove(&reference.window_key);
            if keys.is_empty() {
                self.windows_by_end.remove(&row.window_end_ns);
            }
        }
        Ok(())
    }

    fn validate_event_time_corrections(
        &self,
        inputs: &[RelationInputBatch],
    ) -> Result<(), StandingProgramRuntimeError> {
        if self.correction_horizon().is_none() {
            return Ok(());
        }
        let watermark = min_event_time_watermark(&self.input_event_time_frontiers);
        let mut candidate_frontiers = self.input_event_time_frontiers.clone();
        let column = catalog_column_by_id(&self.catalog, &self.plan.event_time_column_id)?;
        for input in inputs {
            validate_input_matches_schema(
                input,
                &self.input_schema,
                "tumbling_event_time_input_relation",
            )?;
            published_input_empty_delta(input, &self.catalog)?;
            if input.start_offset_inclusive <= input.end_offset_exclusive
                && self.input_frontiers.iter().any(|frontier| {
                    frontier.relation_id == input.relation_id
                        && frontier.relation_version == input.relation_version
                        && frontier.stream_id == input.stream_id
                        && frontier.partition_id == input.partition_id
                        && input.end_offset_exclusive <= frontier.committed_offset_exclusive
                })
            {
                continue;
            }
            if input.event_time_watermark.as_ref().is_none_or(|watermark| {
                watermark.event_time_column_id != self.plan.event_time_column_id
            }) {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "tumbling_event_time_input_batch",
                });
            }
            advance_input_event_time_frontier(&mut candidate_frontiers, input)?;
            for batch in &input.batches {
                let index = batch_column_index(batch, &column.name)?;
                for row in 0..batch.num_rows() {
                    let event_time_ns =
                        batch_event_time_ns(batch, index, &column.physical_arrow_type, row)?;
                    validate_window_correction_horizon(&self.plan, watermark, event_time_ns)?;
                }
            }
        }
        if watermark.is_some_and(|previous| {
            min_event_time_watermark(&candidate_frontiers).is_none_or(|next| next < previous)
        }) {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "window_correction_watermark_regression",
            });
        }
        Ok(())
    }

    fn apply_changes(
        &mut self,
        logical_epoch: LogicalEpoch,
        idempotency_key: EpochIdempotencyKey,
        input_changes: Vec<RelationInputBatch>,
    ) -> Result<EpochCommit, StandingProgramRuntimeError> {
        let idempotency_key_text = idempotency_key.as_str().to_string();
        if let Some(applied_epoch) = self.applied_epochs.get(&idempotency_key_text) {
            if *applied_epoch == logical_epoch {
                return Ok(EpochCommit {
                    logical_epoch,
                    idempotency_key,
                    input_frontiers: self.input_frontiers.clone(),
                    input_event_time_frontiers: self.input_event_time_frontiers.clone(),
                    output_deltas: Vec::new(),
                    output_batches: vec![ViewOutputBatch {
                        view_id: self.identity.view_ids[0].clone(),
                        schema_fingerprint: self.output_schema_fingerprint(),
                        batches: vec![self.materialized_batch()?],
                    }],
                });
            }
            return Err(StandingProgramRuntimeError::IdempotencyKeyConflict {
                idempotency_key: idempotency_key_text,
                first_epoch: *applied_epoch,
                attempted_epoch: logical_epoch,
            });
        }
        if logical_epoch <= self.logical_epoch {
            return Err(StandingProgramRuntimeError::NonMonotonicLogicalEpoch {
                current: self.logical_epoch,
                attempted: logical_epoch,
            });
        }

        self.validate_event_time_corrections(&input_changes)?;
        if !self.event_time_state_requests(&input_changes)?.is_empty() {
            return Err(event_time_state_error(
                "event_time_state_hydration_required",
            ));
        }
        let mut undo = TumblingEpochUndoLog::default();
        let mut next_output = None;
        let result = (|| {
            let mut next_frontiers = self.input_frontiers.clone();
            let mut next_event_time_frontiers = self.input_event_time_frontiers.clone();
            for input in input_changes {
                if self.correction_horizon().is_some() && self.input_is_committed(&input) {
                    continue;
                }
                validate_input_matches_schema(
                    &input,
                    &self.input_schema,
                    "tumbling_event_time_input_relation",
                )?;
                apply_tumbling_input(
                    &mut self.state,
                    &mut undo,
                    &self.catalog,
                    &self.plan,
                    &self.input_event_time_frontiers,
                    &input,
                    None,
                )?;
                advance_input_frontier(&mut next_frontiers, &input)?;
                advance_input_event_time_frontier(&mut next_event_time_frontiers, &input)?;
            }
            // The watermark publishes closed windows; late corrections do not rewind it.
            let effective_watermark = finalization_frontier(
                self.plan.late_row_policy,
                min_event_time_watermark(&next_event_time_frontiers),
            );
            if self.correction_horizon().is_some() {
                for key in undo.rows.keys() {
                    if let Some(row) = self.state.rows.get(key) {
                        validate_correction_state_row(key, row, &self.plan)?;
                    }
                }
                let output_delta = self.correction_delta(&undo, effective_watermark)?;
                let replaced: BTreeSet<String> = output_delta
                    .records()
                    .iter()
                    .filter(|record| record.weight < 0)
                    .map(|record| canonical_json(record.key.as_json()))
                    .collect();
                let staged_output = DeltaBatch::from_records(
                    self.correction_output
                        .iter()
                        .filter(|(key, _)| !replaced.contains(*key))
                        .map(|(_, record)| record.clone())
                        .chain(
                            output_delta
                                .records()
                                .iter()
                                .filter(|record| record.weight > 0)
                                .cloned(),
                        ),
                );
                let output_batches = vec![ViewOutputBatch {
                    view_id: self.identity.view_ids[0].clone(),
                    schema_fingerprint: self.output_schema_fingerprint(),
                    batches: vec![materialized_tumbling_delta_to_record_batch(
                        &self.output_schema,
                        &staged_output,
                        &self.plan.aggregate_outputs,
                    )?],
                }];
                return Ok(EpochCommit {
                    logical_epoch,
                    idempotency_key,
                    input_frontiers: next_frontiers,
                    input_event_time_frontiers: next_event_time_frontiers,
                    output_deltas: vec![ViewOutputDelta {
                        view_id: self.identity.view_ids[0].clone(),
                        schema_fingerprint: self.output_schema_fingerprint(),
                        delta: output_delta,
                    }],
                    output_batches,
                });
            }
            let full_output =
                self.state
                    .closed_delta(&self.plan, &self.output_schema, effective_watermark)?;
            let staged_output = apply_top_k_to_published_output(
                full_output,
                self.plan.top_k.as_ref(),
                &self.plan.aggregate_outputs,
            )?;
            let output_delta = self
                .published_output
                .diff(&staged_output)
                .map_err(|_| invalid_runtime_state())?;
            // Validate output before commit
            let output_batches = vec![ViewOutputBatch {
                view_id: self.identity.view_ids[0].clone(),
                schema_fingerprint: self.output_schema_fingerprint(),
                batches: vec![materialized_tumbling_delta_to_record_batch(
                    &self.output_schema,
                    &staged_output,
                    &self.plan.aggregate_outputs,
                )?],
            }];
            // Commit staged state
            next_output = Some(staged_output);
            Ok(EpochCommit {
                logical_epoch,
                idempotency_key,
                input_frontiers: next_frontiers,
                input_event_time_frontiers: next_event_time_frontiers,
                output_deltas: vec![ViewOutputDelta {
                    view_id: self.identity.view_ids[0].clone(),
                    schema_fingerprint: self.output_schema_fingerprint(),
                    delta: output_delta,
                }],
                output_batches,
            })
        })();
        match result {
            Ok(commit) => {
                if self.correction_horizon().is_some() {
                    self.commit_correction(&undo, &commit.output_deltas[0].delta);
                } else {
                    self.published_output = next_output.expect("successful epoch stages output");
                }
                self.input_frontiers = commit.input_frontiers.clone();
                self.input_event_time_frontiers = commit.input_event_time_frontiers.clone();
                self.applied_epochs
                    .insert(idempotency_key_text, logical_epoch);
                retain_recent_applied_epochs(&mut self.applied_epochs);
                self.logical_epoch = logical_epoch;
                Ok(commit)
            }
            Err(error) => {
                undo.rollback(&mut self.state);
                Err(error)
            }
        }
    }

    fn materialized_view_page(
        &self,
        view: ScopedViewId,
        page: SnapshotPageRequest,
    ) -> Result<MaterializedViewPage, StandingProgramRuntimeError> {
        if view.tenant_id != self.identity.tenant_id
            || view.program_id != self.identity.program_id
            || !self
                .identity
                .view_ids
                .iter()
                .any(|view_id| view_id == &view.view_id)
        {
            return Err(StandingProgramRuntimeError::UnknownView {
                view_id: view.view_id,
            });
        }

        let (batch, next_page_token) = self.materialized_page_batch(page)?;
        Ok(MaterializedViewPage {
            view,
            logical_epoch: self.logical_epoch,
            schema_fingerprint: self.output_schema_fingerprint(),
            batches: vec![batch],
            next_page_token,
        })
    }

    fn checkpoint(&self) -> Result<RuntimeCheckpoint, StandingProgramRuntimeError> {
        let payload = self.checkpoint_payload()?;
        let content_hash = stable_bytes_hash(payload.as_bytes());
        Ok(RuntimeCheckpoint {
            identity: self.identity.clone(),
            logical_epoch: self.logical_epoch,
            input_frontiers: self.input_frontiers.clone(),
            input_event_time_frontiers: self.input_event_time_frontiers.clone(),
            output_frontiers: self
                .identity
                .view_ids
                .iter()
                .map(|view_id| ViewFrontier {
                    view_id: view_id.clone(),
                    committed_epoch: self.logical_epoch,
                })
                .collect(),
            checkpoint_codec_identity: self.identity.checkpoint_codec_identity.clone(),
            state_root: DurableStateRoot {
                object_key: format!(
                    "v1/state/materialized-view-runtime/{}/checkpoint",
                    self.identity.program_id
                ),
                content_hash,
            },
            state_payload: Some(RuntimeCheckpointStatePayload {
                codec_identity: self.identity.checkpoint_codec_identity.clone(),
                payload,
            }),
            output_manifest_refs: Vec::new(),
            owner_epoch: None,
            input_coverage: None,
            causal_cut: None,
        })
    }

    fn restore(checkpoint: RuntimeCheckpoint) -> Result<Self, StandingProgramRuntimeError> {
        checkpoint.validate_identity(&checkpoint.identity)?;
        let payload = Self::restore_payload(&checkpoint)?;
        if payload.logical_epoch != checkpoint.logical_epoch
            || payload.input_frontiers != checkpoint.input_frontiers
            || payload.input_event_time_frontiers != checkpoint.input_event_time_frontiers
        {
            return Err(invalid_checkpoint());
        }
        validate_checkpoint_frontiers_for_schemas(
            &checkpoint,
            std::slice::from_ref(&payload.input_schema),
        )?;
        validate_input_event_time_frontiers_for_catalogs(
            &checkpoint,
            std::slice::from_ref(&payload.catalog),
        )?;
        validate_view_sql_hash(&checkpoint.identity, payload.view_sql.as_str())?;
        let compiled_plan = validate_supported_tumbling_window_sql_with_policy(
            payload.view_sql.as_str(),
            &payload.catalog,
            payload.plan.late_row_policy,
        )
        .map_err(|_| invalid_checkpoint())?;
        if compiled_plan != payload.plan {
            return Err(invalid_checkpoint());
        }
        validate_logical_view_plan(&payload.logical_plan).map_err(|_| invalid_checkpoint())?;
        let compiled_logical_plan =
            lower_supported_tumbling_window_sql_to_logical_plan_with_policy(
                payload.view_sql.as_str(),
                &payload.catalog,
                &payload.output_schema,
                payload.plan.late_row_policy,
            )
            .map_err(|_| invalid_checkpoint())?;
        if compiled_logical_plan != payload.logical_plan {
            return Err(invalid_checkpoint());
        }
        validate_published_output(&payload.published_output)?;
        let mut applied_epochs = payload
            .applied_epochs
            .into_iter()
            .map(|entry| (entry.idempotency_key, entry.logical_epoch))
            .collect();
        retain_recent_applied_epochs(&mut applied_epochs);
        let mut correction_output = BTreeMap::new();
        let mut windows_by_end: BTreeMap<i64, BTreeSet<String>> = BTreeMap::new();
        let correction = matches!(
            payload.plan.late_row_policy,
            Some(LateRowPolicy::CorrectWithinHorizon { .. })
        );
        if correction {
            correction_output = payload
                .published_output
                .records()
                .iter()
                .map(|record| (canonical_json(record.key.as_json()), record.clone()))
                .collect();
            for (key, row) in &payload.state.rows {
                windows_by_end
                    .entry(row.window_end_ns)
                    .or_default()
                    .insert(key.clone());
            }
        }
        Ok(Self {
            identity: checkpoint.identity,
            catalog: payload.catalog,
            input_schema: payload.input_schema,
            output_schema: payload.output_schema,
            view_sql: payload.view_sql,
            plan: payload.plan,
            logical_plan: payload.logical_plan,
            state: payload.state,
            published_output: if correction {
                DeltaBatch::default()
            } else {
                payload.published_output
            },
            correction_output,
            windows_by_end,
            cold_state_refs: payload.cold_state_refs,
            legacy_replay_before_ns: payload.legacy_replay_before_ns,
            input_frontiers: checkpoint.input_frontiers,
            input_event_time_frontiers: checkpoint.input_event_time_frontiers,
            applied_epochs,
            logical_epoch: checkpoint.logical_epoch,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn correction_selection_visits_only_touched_and_newly_closed_windows() {
        let index: BTreeMap<_, _> = (0..10_000)
            .map(|end| (end, BTreeSet::from([format!("window-{end}")])))
            .collect();
        let touched = BTreeSet::from(["window-1".to_string()]);
        assert_eq!(
            correction_affected_windows(
                touched.clone().into_iter(),
                &index,
                Some(5_000),
                Some(5_001)
            ),
            BTreeSet::from(["window-1".to_string(), "window-5001".to_string()]),
        );
        assert_eq!(
            correction_affected_windows(touched.into_iter(), &index, Some(5_000), Some(5_000)),
            BTreeSet::from(["window-1".to_string()]),
        );
        assert_eq!(
            correction_affected_windows(
                std::iter::once("window-1".to_string()),
                &index,
                Some(5_000),
                Some(4_000)
            ),
            BTreeSet::from(["window-1".to_string()]),
        );
        assert!(
            correction_affected_windows(std::iter::empty(), &index, Some(5_000), Some(5_000))
                .is_empty()
        );
    }
}
