use super::*;
use velorix_core::delta::{encode_kv_ordered, TypedBinaryKey};
use velorix_core::view_plan::{
    lower_supported_typed_join_project_sql_to_logical_plan,
    validate_supported_typed_join_project_sql, SupportedTypedJoinProjectPlanV1,
};

pub struct TwoInputJoinRuntime {
    identity: StandingProgramIdentity,
    catalogs: Vec<VelorixRelationCatalogV1>,
    input_schemas: Vec<RelationSchema>,
    output_schema: RelationSchema,
    view_sql: String,
    plan: SupportedJoinViewPlan,
    logical_plan: VelorixLogicalViewPlanV1,
    execution_binding: JoinExecutionBindingV1,
    comparison_graph: Option<JoinSpecializationComparisonGraph>,
    engine: KeyedAggregateKernel,
    join: JoinOperator,
    published_output: DeltaBatch,
    filtered_aggregate_state: DeltaBatch,
    input_frontiers: Vec<RelationFrontier>,
    input_event_time_frontiers: Vec<InputEventTimeFrontier>,
    applied_epochs: BTreeMap<String, LogicalEpoch>,
}

#[cfg(test)]
mod checkpoint_row_cost_tests {
    use super::*;

    #[derive(Serialize)]
    struct RetainedRows {
        logical_epoch: LogicalEpoch,
        left_state: DeltaBatch,
        right_state: DeltaBatch,
        output: DeltaBatch,
    }

    fn record(key: &str, value: &str, weight: i64) -> DeltaRecord {
        DeltaRecord::new(
            DeltaKey::from_json(serde_json::json!({"key": key})),
            DeltaValue::from_json(serde_json::json!({"value": value})),
            weight,
        )
    }

    fn measured_rows(checkpoint: &RetainedRows) -> usize {
        checkpoint_rows_cost(checkpoint.left_state.records()).unwrap()
            + checkpoint_rows_cost(checkpoint.right_state.records()).unwrap()
            + checkpoint_rows_cost(checkpoint.output.records()).unwrap()
    }

    #[test]
    fn row_cost_sum_matches_full_measure_after_signed_updates_and_restore() {
        let mut checkpoint = RetainedRows {
            logical_epoch: 1,
            left_state: DeltaBatch::from_records([record("left-a", "x", 2)]),
            right_state: DeltaBatch::from_records([record("right-a", "y", 1)]),
            output: DeltaBatch::from_records([record("out-a", "z", 2)]),
        };
        let measure = |checkpoint: &RetainedRows| {
            velorix_core::arrow_state::ArrowState::measure_serializable(checkpoint, usize::MAX)
                .unwrap()
        };
        let metadata = RetainedRows {
            logical_epoch: checkpoint.logical_epoch,
            left_state: DeltaBatch::default(),
            right_state: DeltaBatch::default(),
            output: DeltaBatch::default(),
        };
        assert_eq!(
            measure(&checkpoint),
            measure(&metadata) + measured_rows(&checkpoint)
        );

        // Model a signed retraction of one retained row and insertion of a wider row.
        let current_cost = measured_rows(&checkpoint);
        let old_left_cost = checkpoint_rows_cost(checkpoint.left_state.records()).unwrap();
        let old_output_cost = checkpoint_rows_cost(checkpoint.output.records()).unwrap();
        checkpoint.logical_epoch = 2;
        checkpoint.left_state =
            DeltaBatch::from_records([record("left-b-with-more-bytes", "x", 1)]);
        checkpoint.output = DeltaBatch::from_records([record("out-b-with-more-bytes", "z", 1)]);
        let running_cost = adjust_checkpoint_row_cost(
            current_cost,
            old_left_cost + old_output_cost,
            checkpoint_rows_cost(checkpoint.left_state.records()).unwrap()
                + checkpoint_rows_cost(checkpoint.output.records()).unwrap(),
        )
        .unwrap();
        let metadata = RetainedRows {
            logical_epoch: checkpoint.logical_epoch,
            left_state: DeltaBatch::default(),
            right_state: DeltaBatch::default(),
            output: DeltaBatch::default(),
        };
        assert_eq!(
            measure(&checkpoint),
            measure(&metadata) + measured_rows(&checkpoint)
        );
        assert_eq!(running_cost, measured_rows(&checkpoint));

        let restored = RetainedRows {
            logical_epoch: checkpoint.logical_epoch,
            left_state: checkpoint.left_state.clone(),
            right_state: checkpoint.right_state.clone(),
            output: checkpoint.output.clone(),
        };
        assert_eq!(measured_rows(&restored), running_cost);
    }
}

/// Adds the outer-join portion to the inner delta retained by the prepared
/// join.  The sparse token lets this inspect only keys touched by the epoch;
/// no side snapshot or reconstructed join is required.
fn prepared_join_output_for_plan(
    join: &JoinOperator,
    prepared: &velorix_core::operator::PreparedKeyedEquiJoinEpoch,
    kind: SupportedJoinKind,
) -> Result<DeltaBatch, StandingProgramRuntimeError> {
    let mut keys = BTreeMap::new();
    for side in [JoinInputSide::Left, JoinInputSide::Right] {
        for key in prepared.touched_keys(side) {
            keys.insert(canonical_json(key.as_json()), key);
        }
    }
    let mut output = prepared.output_changes().records().to_vec();
    for key in keys.into_values() {
        let left_before = join
            .prepared_side_records(prepared, JoinInputSide::Left, &key, false)
            .map_err(|_| invalid_runtime_state())?;
        let right_before = join
            .prepared_side_records(prepared, JoinInputSide::Right, &key, false)
            .map_err(|_| invalid_runtime_state())?;
        let left_after = join
            .prepared_side_records(prepared, JoinInputSide::Left, &key, true)
            .map_err(|_| invalid_runtime_state())?;
        let right_after = join
            .prepared_side_records(prepared, JoinInputSide::Right, &key, true)
            .map_err(|_| invalid_runtime_state())?;
        if kind != SupportedJoinKind::Inner {
            if join_records_weight(&right_before)? == 0 {
                append_unmatched_records(&mut output, &left_before, false, -1)?;
            }
            if join_records_weight(&right_after)? == 0 {
                append_unmatched_records(&mut output, &left_after, false, 1)?;
            }
        }
        if kind == SupportedJoinKind::Full {
            if join_records_weight(&left_before)? == 0 {
                append_unmatched_records(&mut output, &right_before, true, -1)?;
            }
            if join_records_weight(&left_after)? == 0 {
                append_unmatched_records(&mut output, &right_after, true, 1)?;
            }
        }
    }
    DeltaBatch::from_records(output)
        .net_rows()
        .map(DeltaBatch::from_records)
        .map_err(|_| invalid_runtime_state())
}

fn join_records_weight(records: &[DeltaRecord]) -> Result<i64, StandingProgramRuntimeError> {
    records.iter().try_fold(0_i64, |total, row| {
        if row.weight < 0 {
            return Err(invalid_runtime_state());
        }
        total
            .checked_add(row.weight)
            .ok_or_else(invalid_runtime_state)
    })
}

fn checkpoint_row_cost(row: &DeltaRecord) -> Result<usize, StandingProgramRuntimeError> {
    velorix_core::arrow_state::ArrowState::measure_serializable(row, usize::MAX)
        .map_err(|_| invalid_runtime_state())
}

fn checkpoint_rows_cost(rows: &[DeltaRecord]) -> Result<usize, StandingProgramRuntimeError> {
    rows.iter().try_fold(0_usize, |total, row| {
        total
            .checked_add(checkpoint_row_cost(row)?)
            .ok_or_else(invalid_runtime_state)
    })
}

fn adjust_checkpoint_row_cost(
    current: usize,
    removed: usize,
    added: usize,
) -> Result<usize, StandingProgramRuntimeError> {
    current
        .checked_sub(removed)
        .and_then(|cost| cost.checked_add(added))
        .ok_or_else(invalid_runtime_state)
}

fn append_unmatched_records(
    output: &mut Vec<DeltaRecord>,
    records: &[DeltaRecord],
    right: bool,
    sign: i64,
) -> Result<(), StandingProgramRuntimeError> {
    for row in records {
        let value = if right {
            unmatched_right_join_output_value(&row.value)
        } else {
            unmatched_left_join_output_value(&row.value)
        }
        .map_err(|_| invalid_runtime_state())?;
        let weight = row
            .weight
            .checked_mul(sign)
            .ok_or_else(invalid_runtime_state)?;
        output.push(DeltaRecord::new(row.key.clone(), value, weight));
    }
    Ok(())
}

impl TwoInputJoinRuntime {
    pub fn new_with_plan(
        identity: StandingProgramIdentity,
        catalogs: Vec<VelorixRelationCatalogV1>,
        input_schemas: Vec<RelationSchema>,
        output_schema: RelationSchema,
        view_sql: String,
        plan: SupportedJoinViewPlan,
        logical_plan: VelorixLogicalViewPlanV1,
    ) -> Result<Self, StandingProgramRuntimeError> {
        Self::new_with_logical_plan(
            identity,
            catalogs,
            input_schemas,
            output_schema,
            view_sql,
            plan,
            logical_plan,
        )
    }

    pub fn new_with_logical_plan(
        identity: StandingProgramIdentity,
        catalogs: Vec<VelorixRelationCatalogV1>,
        input_schemas: Vec<RelationSchema>,
        output_schema: RelationSchema,
        view_sql: String,
        plan: SupportedJoinViewPlan,
        logical_plan: VelorixLogicalViewPlanV1,
    ) -> Result<Self, StandingProgramRuntimeError> {
        Self::new_with_execution_mode(
            identity,
            catalogs,
            input_schemas,
            output_schema,
            view_sql,
            plan,
            logical_plan,
            JoinExecutionModeV1::SelectedSpecialization,
        )
    }

    pub fn new_common_dag_reference_with_logical_plan(
        identity: StandingProgramIdentity,
        catalogs: Vec<VelorixRelationCatalogV1>,
        input_schemas: Vec<RelationSchema>,
        output_schema: RelationSchema,
        view_sql: String,
        plan: SupportedJoinViewPlan,
        logical_plan: VelorixLogicalViewPlanV1,
    ) -> Result<Self, StandingProgramRuntimeError> {
        Self::new_with_execution_mode(
            identity,
            catalogs,
            input_schemas,
            output_schema,
            view_sql,
            plan,
            logical_plan,
            JoinExecutionModeV1::CommonDagReference,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new_with_execution_mode(
        identity: StandingProgramIdentity,
        catalogs: Vec<VelorixRelationCatalogV1>,
        input_schemas: Vec<RelationSchema>,
        output_schema: RelationSchema,
        view_sql: String,
        plan: SupportedJoinViewPlan,
        logical_plan: VelorixLogicalViewPlanV1,
        execution_mode: JoinExecutionModeV1,
    ) -> Result<Self, StandingProgramRuntimeError> {
        identity.validate()?;
        validate_builtin_runtime_identity(&identity)?;
        validate_view_sql_hash(&identity, view_sql.as_str())?;
        validate_logical_view_plan(&logical_plan).map_err(|_| {
            StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "logical_join_view_plan",
            }
        })?;
        validate_join_supported_schemas(&catalogs, &input_schemas, &output_schema, &plan)?;
        validate_join_sql_or_logical_plan(
            view_sql.as_str(),
            &catalogs,
            &output_schema,
            &plan,
            &logical_plan,
        )?;
        validate_join_plan_matches_catalogs(&plan, &catalogs)?;
        let left_catalog = join_left_catalog(&plan, &catalogs)?;
        let value_mode =
            aggregate_value_mode_for_column_id(left_catalog, &plan.sum_value_column_id)?;
        let track_extrema = join_plan_tracks_extrema(&plan);
        let execution_binding =
            bind_join_execution_v1(&logical_plan, execution_mode).map_err(|_| {
                StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "join_execution_binding",
                }
            })?;
        let comparison_graph = if execution_mode == JoinExecutionModeV1::CommonDagReference {
            Some(
                JoinSpecializationComparisonGraph::new(
                    catalogs.clone(),
                    plan.clone(),
                    output_schema.clone(),
                )
                .map_err(|_| {
                    StandingProgramRuntimeError::InvalidProgramIdentity {
                        field: "join_execution_binding",
                    }
                })?,
            )
        } else {
            None
        };
        let filtered_aggregate_state = DeltaBatch::default();
        let published_output = publish_join_aggregate_state(&filtered_aggregate_state, &plan)?;
        Ok(Self {
            identity,
            catalogs,
            input_schemas,
            output_schema,
            view_sql,
            plan,
            logical_plan,
            execution_binding,
            comparison_graph,
            engine: KeyedAggregateKernel::with_aggregate_value_mode_and_extrema(
                value_mode,
                track_extrema,
            ),
            join: new_join_operator(),
            published_output,
            filtered_aggregate_state,
            input_frontiers: Vec::new(),
            input_event_time_frontiers: Vec::new(),
            applied_epochs: BTreeMap::new(),
        })
    }

    fn output_schema_fingerprint(&self) -> String {
        self.output_schema.schema_fingerprint.clone()
    }

    fn materialized_batch(&self) -> Result<RecordBatch, StandingProgramRuntimeError> {
        materialized_delta_to_record_batch(
            &self.output_schema,
            &self.published_output,
            Some(&supported_join_view_plan_aggregate_outputs(&self.plan)),
        )
    }

    fn materialized_page_batch(
        &self,
        page: SnapshotPageRequest,
    ) -> Result<(RecordBatch, Option<String>), StandingProgramRuntimeError> {
        materialized_delta_page_batch(
            &self.output_schema,
            &self.published_output,
            self.engine.logical_epoch(),
            page,
            Some(&supported_join_view_plan_aggregate_outputs(&self.plan)),
        )
    }

    fn checkpoint_payload(
        &self,
    ) -> Result<velorix_core::arrow_state::ArrowState, StandingProgramRuntimeError> {
        let payload = JoinCheckpointPayload {
            schema_version: CHECKPOINT_PAYLOAD_SCHEMA_VERSION,
            runtime_kind: match self.execution_binding.mode {
                JoinExecutionModeV1::SelectedSpecialization => JOIN_RUNTIME_KIND,
                JoinExecutionModeV1::CommonDagReference => JOIN_COMMON_DAG_REFERENCE_RUNTIME_KIND,
            }
            .to_string(),
            catalogs: self.catalogs.clone(),
            input_schemas: self.input_schemas.clone(),
            output_schema: self.output_schema.clone(),
            view_sql: self.view_sql.clone(),
            plan: self.plan.clone(),
            logical_plan: self.logical_plan.clone(),
            join_key_codec_id: supported_join_key_codec_id(&self.plan).map(str::to_string),
            execution_binding: Some(self.execution_binding.clone()),
            input_frontiers: self.input_frontiers.clone(),
            input_event_time_frontiers: self.input_event_time_frontiers.clone(),
            left_state: self.join.left_state(),
            right_state: self.join.right_state(),
            engine: self.engine.checkpoint_state().to_payload(),
            published_output: Some(self.published_output.clone()),
            filtered_aggregate_state: self.filtered_aggregate_state.clone(),
            comparison_graph: self
                .comparison_graph
                .as_ref()
                .map(JoinSpecializationComparisonGraph::checkpoint)
                .transpose()
                .map_err(|_| invalid_checkpoint())?,
            applied_epochs: self
                .applied_epochs
                .iter()
                .map(|(idempotency_key, logical_epoch)| GenericAppliedEpoch {
                    idempotency_key: idempotency_key.clone(),
                    logical_epoch: *logical_epoch,
                })
                .collect(),
        };
        velorix_core::arrow_state::to_state(&payload).map_err(|_| invalid_checkpoint())
    }

    fn restore_payload(
        checkpoint: &RuntimeCheckpoint,
        expected_runtime_kind: &str,
    ) -> Result<JoinCheckpointPayload, StandingProgramRuntimeError> {
        let Some(state_payload) = &checkpoint.state_payload else {
            return Err(invalid_checkpoint());
        };
        if state_payload.codec_identity != checkpoint.checkpoint_codec_identity {
            return Err(StandingProgramRuntimeError::CheckpointCodecMismatch {
                expected: checkpoint.checkpoint_codec_identity.clone(),
                actual: state_payload.codec_identity.clone(),
            });
        }
        let payload: JoinCheckpointPayload =
            velorix_core::arrow_state::from_state(&state_payload.payload)
                .map_err(|_| invalid_checkpoint())?;
        if payload.schema_version != CHECKPOINT_PAYLOAD_SCHEMA_VERSION
            || payload.runtime_kind != expected_runtime_kind
        {
            return Err(invalid_checkpoint());
        }
        validate_join_supported_schemas(
            &payload.catalogs,
            &payload.input_schemas,
            &payload.output_schema,
            &payload.plan,
        )?;
        Ok(payload)
    }

    pub fn restore_common_dag_reference(
        checkpoint: RuntimeCheckpoint,
    ) -> Result<Self, StandingProgramRuntimeError> {
        Self::restore_for_execution_mode(checkpoint, JoinExecutionModeV1::CommonDagReference)
    }

    fn restore_for_execution_mode(
        checkpoint: RuntimeCheckpoint,
        execution_mode: JoinExecutionModeV1,
    ) -> Result<Self, StandingProgramRuntimeError> {
        checkpoint.validate_identity(&checkpoint.identity)?;
        let expected_runtime_kind = match execution_mode {
            JoinExecutionModeV1::SelectedSpecialization => JOIN_RUNTIME_KIND,
            JoinExecutionModeV1::CommonDagReference => JOIN_COMMON_DAG_REFERENCE_RUNTIME_KIND,
        };
        let mut payload = Self::restore_payload(&checkpoint, expected_runtime_kind)?;
        normalize_legacy_join_plan_input_relation_sides(&mut payload.plan);
        validate_join_checkpoint_frontiers(&checkpoint, &payload)?;
        let engine_checkpoint = payload.engine.into_checkpoint();
        if engine_checkpoint.logical_epoch() != checkpoint.logical_epoch
            || payload.input_frontiers != checkpoint.input_frontiers
            || payload.input_event_time_frontiers != checkpoint.input_event_time_frontiers
        {
            return Err(invalid_checkpoint());
        }
        validate_input_event_time_frontiers_for_catalogs(&checkpoint, &payload.catalogs)?;
        validate_view_sql_hash(&checkpoint.identity, payload.view_sql.as_str())?;
        validate_logical_view_plan(&payload.logical_plan).map_err(|_| invalid_checkpoint())?;
        validate_join_sql_or_logical_plan(
            payload.view_sql.as_str(),
            &payload.catalogs,
            &payload.output_schema,
            &payload.plan,
            &payload.logical_plan,
        )
        .map_err(|_| invalid_checkpoint())?;
        validate_join_plan_matches_catalogs(&payload.plan, &payload.catalogs)?;
        if payload.join_key_codec_id.as_deref() != supported_join_key_codec_id(&payload.plan) {
            return Err(invalid_checkpoint());
        }
        let expected_binding = bind_join_execution_v1(&payload.logical_plan, execution_mode)
            .map_err(|_| invalid_checkpoint())?;
        if payload
            .execution_binding
            .as_ref()
            .is_some_and(|binding| binding != &expected_binding)
            || (execution_mode == JoinExecutionModeV1::CommonDagReference
                && payload.execution_binding.is_none())
        {
            return Err(invalid_checkpoint());
        }
        let left_catalog = join_left_catalog(&payload.plan, &payload.catalogs)?;
        let value_mode =
            aggregate_value_mode_for_column_id(left_catalog, &payload.plan.sum_value_column_id)?;
        let track_extrema = join_plan_tracks_extrema(&payload.plan);
        let engine = KeyedAggregateKernel::from_checkpoint_with_aggregate_value_mode_and_extrema(
            engine_checkpoint,
            value_mode,
            track_extrema,
        )
        .map_err(|_| invalid_checkpoint())?;
        let Some(published_output) = payload.published_output.clone() else {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "published_output",
            });
        };
        let filtered_aggregate_state = if join_plan_uses_runtime_aggregate_state(&payload.plan)
            && !supported_join_view_plan_is_singleton(&payload.plan)
            && payload.filtered_aggregate_state.records().is_empty()
        {
            published_output.clone()
        } else {
            payload.filtered_aggregate_state
        };
        validate_published_output(&published_output)?;
        validate_published_output(&filtered_aggregate_state)?;
        if supported_join_view_plan_is_singleton(&payload.plan)
            && publish_join_aggregate_state(&filtered_aggregate_state, &payload.plan)?
                != published_output
        {
            return Err(invalid_checkpoint());
        }
        let comparison_graph = match execution_mode {
            JoinExecutionModeV1::SelectedSpecialization => {
                if payload.comparison_graph.is_some() {
                    return Err(invalid_checkpoint());
                }
                None
            }
            JoinExecutionModeV1::CommonDagReference => Some(
                JoinSpecializationComparisonGraph::restore(
                    payload.catalogs.clone(),
                    payload.plan.clone(),
                    payload.output_schema.clone(),
                    payload
                        .comparison_graph
                        .as_ref()
                        .ok_or_else(invalid_checkpoint)?,
                )
                .map_err(|_| invalid_checkpoint())?,
            ),
        };
        let mut applied_epochs = payload
            .applied_epochs
            .into_iter()
            .map(|entry| (entry.idempotency_key, entry.logical_epoch))
            .collect();
        retain_recent_applied_epochs(&mut applied_epochs);
        Ok(Self {
            identity: checkpoint.identity,
            catalogs: payload.catalogs,
            input_schemas: payload.input_schemas,
            output_schema: payload.output_schema,
            view_sql: payload.view_sql,
            plan: payload.plan,
            logical_plan: payload.logical_plan,
            execution_binding: expected_binding,
            comparison_graph,
            engine,
            join: restore_join_operator(&payload.left_state, &payload.right_state)?,
            published_output,
            filtered_aggregate_state,
            input_frontiers: checkpoint.input_frontiers,
            input_event_time_frontiers: checkpoint.input_event_time_frontiers,
            applied_epochs,
        })
    }
}

impl StandingProgramRuntime for TwoInputJoinRuntime {
    fn program_identity(&self) -> &StandingProgramIdentity {
        &self.identity
    }

    fn input_schemas(&self) -> Vec<RelationSchema> {
        self.input_schemas.clone()
    }

    fn output_schemas(&self) -> Vec<RelationSchema> {
        vec![self.output_schema.clone()]
    }

    fn logical_epoch(&self) -> LogicalEpoch {
        self.engine.logical_epoch()
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
        if self.execution_binding.mode == JoinExecutionModeV1::CommonDagReference {
            let mut input_frontiers = self.input_frontiers.clone();
            let mut input_event_time_frontiers = self.input_event_time_frontiers.clone();
            for input in &input_changes {
                validate_input_matches_one_schema(
                    input,
                    &self.input_schemas,
                    "generic_join_input_relation",
                )?;
                advance_input_frontier(&mut input_frontiers, input)?;
                advance_input_event_time_frontier(&mut input_event_time_frontiers, input)?;
            }
            // Save checkpoint for rollback on failure
            let graph_checkpoint = self
                .comparison_graph
                .as_ref()
                .ok_or_else(invalid_runtime_state)?
                .checkpoint()
                .map_err(|_| invalid_runtime_state())?;
            let output_delta = match self
                .comparison_graph
                .as_mut()
                .ok_or_else(invalid_runtime_state)?
                .apply_epoch(logical_epoch, input_changes)
            {
                Ok(delta) => delta,
                Err(_) => {
                    // Rollback comparison_graph from checkpoint
                    if let Some(graph) = self.comparison_graph.as_mut() {
                        if let Ok(restored) = JoinSpecializationComparisonGraph::restore(
                            self.catalogs.clone(),
                            self.plan.clone(),
                            self.output_schema.clone(),
                            &graph_checkpoint,
                        ) {
                            *graph = restored;
                        }
                    }
                    return Err(invalid_runtime_state());
                }
            };
            self.engine
                .push_changes(logical_epoch, &DeltaBatch::default())
                .map_err(|_| invalid_runtime_state())?;
            let staged_output =
                apply_published_output_delta(&self.published_output, &output_delta)?;
            let aggregate_outputs = supported_join_view_plan_aggregate_outputs(&self.plan);
            // Validate output before commit
            let output_batches = vec![ViewOutputBatch {
                view_id: self.identity.view_ids[0].clone(),
                schema_fingerprint: self.output_schema_fingerprint(),
                batches: vec![materialized_delta_to_record_batch(
                    &self.output_schema,
                    &staged_output,
                    Some(&aggregate_outputs),
                )?],
            }];
            // Commit staged state
            self.published_output = staged_output;
            self.input_frontiers = input_frontiers.clone();
            self.input_event_time_frontiers = input_event_time_frontiers.clone();
            self.applied_epochs
                .insert(idempotency_key_text, logical_epoch);
            retain_recent_applied_epochs(&mut self.applied_epochs);
            return Ok(EpochCommit {
                logical_epoch,
                idempotency_key,
                input_frontiers,
                input_event_time_frontiers,
                output_deltas: vec![ViewOutputDelta {
                    view_id: self.identity.view_ids[0].clone(),
                    schema_fingerprint: self.output_schema_fingerprint(),
                    delta: output_delta,
                }],
                output_batches,
            });
        }
        if join_plan_uses_runtime_aggregate_state(&self.plan) {
            let self_join = supported_join_view_plan_is_self_join(&self.plan);
            let mut input_frontiers = self.input_frontiers.clone();
            let mut input_event_time_frontiers = self.input_event_time_frontiers.clone();
            let mut join_inputs = Vec::new();
            for input in input_changes {
                validate_input_matches_one_schema(
                    &input,
                    &self.input_schemas,
                    "generic_join_input_relation",
                )?;
                let catalog = join_catalog_for_relation(&self.catalogs, &input.relation_id)?;
                if self_join {
                    if input.relation_id != self.plan.left_input_relation_id {
                        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                            field: "generic_join_input_relation",
                        });
                    }
                    let left_delta = join_left_input_delta_batch(catalog, &self.plan, &input)?;
                    let left_delta =
                        prefilter_delta_batch_for_join_plan(&left_delta, &self.plan, catalog)?;
                    join_inputs.push((JoinInputSide::Left, left_delta));
                    let right_delta = join_right_input_delta_batch(catalog, &self.plan, &input)?;
                    let right_delta =
                        prefilter_delta_batch_for_join_plan(&right_delta, &self.plan, catalog)?;
                    join_inputs.push((JoinInputSide::Right, right_delta));
                } else if input.relation_id == self.plan.left_input_relation_id {
                    let delta = join_left_input_delta_batch(catalog, &self.plan, &input)?;
                    let delta = prefilter_delta_batch_for_join_plan(&delta, &self.plan, catalog)?;
                    join_inputs.push((JoinInputSide::Left, delta));
                } else if input.relation_id == self.plan.right_input_relation_id {
                    let delta = join_right_input_delta_batch(catalog, &self.plan, &input)?;
                    let delta = prefilter_delta_batch_for_join_plan(&delta, &self.plan, catalog)?;
                    join_inputs.push((JoinInputSide::Right, delta));
                } else {
                    return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                        field: "generic_join_input_relation",
                    });
                }
                advance_input_frontier(&mut input_frontiers, &input)?;
                advance_input_event_time_frontier(&mut input_event_time_frontiers, &input)?;
            }
            let prepared_inputs = join_inputs
                .iter()
                .map(|(side, delta)| (*side, delta))
                .collect::<Vec<_>>();
            let prepared_join = self
                .join
                .prepare_epoch(logical_epoch, &prepared_inputs)
                .map_err(|_| invalid_runtime_state())?;
            let joined_changes =
                prepared_join_output_for_plan(&self.join, &prepared_join, self.plan.join_kind)?;
            let joined_changes = filter_joined_delta_batch_for_join_plan(
                &joined_changes,
                &self.plan,
                &self.catalogs,
            )?;
            let (next_state, _) = apply_filtered_join_aggregate_delta(
                &self.filtered_aggregate_state,
                &joined_changes,
                &self.plan,
                &self.catalogs,
            )?;
            let aggregate_outputs = supported_join_view_plan_aggregate_outputs(&self.plan);
            let published_state = publish_join_aggregate_state(&next_state, &self.plan)?;
            let visible_output = filter_output_delta_for_having(
                &published_state,
                self.plan.having.as_ref(),
                self.plan.having_expr.as_ref(),
                &self.output_schema,
                Some(&aggregate_outputs),
            )?;
            let visible_output = apply_top_k_to_published_output(
                visible_output,
                self.plan.top_k.as_ref(),
                &aggregate_outputs,
            )?;
            let output_delta = self
                .published_output
                .diff(&visible_output)
                .map_err(|_| invalid_runtime_state())?;
            let prepared_engine = self
                .engine
                .prepare_epoch(logical_epoch, &DeltaBatch::default())
                .map_err(|_| invalid_runtime_state())?;
            // Validate output before commit
            let output_batches = vec![ViewOutputBatch {
                view_id: self.identity.view_ids[0].clone(),
                schema_fingerprint: self.output_schema_fingerprint(),
                batches: vec![materialized_delta_to_record_batch(
                    &self.output_schema,
                    &visible_output,
                    Some(&aggregate_outputs),
                )?],
            }];
            // Fence both opaque epochs before either sparse mutation.
            self.join
                .validate_prepared_epoch(&prepared_join)
                .map_err(|_| invalid_runtime_state())?;
            self.engine
                .validate_prepared_epoch(&prepared_engine)
                .map_err(|_| invalid_runtime_state())?;
            self.join
                .commit_prepared_epoch(prepared_join)
                .map_err(|_| invalid_runtime_state())?;
            self.engine
                .commit_prepared_epoch(prepared_engine)
                .map_err(|_| invalid_runtime_state())?;
            self.filtered_aggregate_state = next_state;
            self.published_output = visible_output;
            self.input_frontiers = input_frontiers.clone();
            self.input_event_time_frontiers = input_event_time_frontiers.clone();
            self.applied_epochs
                .insert(idempotency_key_text, logical_epoch);
            retain_recent_applied_epochs(&mut self.applied_epochs);

            return Ok(EpochCommit {
                logical_epoch,
                idempotency_key,
                input_frontiers,
                input_event_time_frontiers,
                output_deltas: vec![ViewOutputDelta {
                    view_id: self.identity.view_ids[0].clone(),
                    schema_fingerprint: self.output_schema_fingerprint(),
                    delta: output_delta,
                }],
                output_batches,
            });
        }
        let mut executor_commit = {
            let mut executor = LogicalPlanExecutor::TwoInputJoin {
                catalogs: &self.catalogs,
                input_schemas: &self.input_schemas,
                plan: &self.plan,
                engine: &mut self.engine,
                join: &mut self.join,
            };
            executor.apply_epoch(
                logical_epoch,
                &self.input_frontiers,
                &self.input_event_time_frontiers,
                input_changes,
            )?
        };
        let prepared_join = executor_commit
            .prepared_join
            .take()
            .ok_or_else(invalid_runtime_state)?;
        let prepared_engine = executor_commit
            .prepared_engine
            .take()
            .ok_or_else(invalid_runtime_state)?;
        let output_delta = filter_output_delta_for_having(
            &executor_commit.output_delta,
            self.plan.having.as_ref(),
            self.plan.having_expr.as_ref(),
            &self.output_schema,
            Some(&supported_join_view_plan_aggregate_outputs(&self.plan)),
        )?;
        if self.plan.top_k.is_some() {
            let previous_output = self.published_output.clone();
            let aggregate_outputs = supported_join_view_plan_aggregate_outputs(&self.plan);
            let staged_engine_state = apply_published_output_delta(
                &self.engine.materialized_state(),
                prepared_engine.output_changes(),
            )?;
            let full_output = filter_output_delta_for_having(
                &staged_engine_state,
                self.plan.having.as_ref(),
                self.plan.having_expr.as_ref(),
                &self.output_schema,
                Some(&aggregate_outputs),
            )?;
            let staged_output = apply_top_k_to_published_output(
                full_output,
                self.plan.top_k.as_ref(),
                &aggregate_outputs,
            )?;
            // Validate output before commit
            let output_batches = vec![ViewOutputBatch {
                view_id: self.identity.view_ids[0].clone(),
                schema_fingerprint: self.output_schema_fingerprint(),
                batches: vec![materialized_delta_to_record_batch(
                    &self.output_schema,
                    &staged_output,
                    Some(&aggregate_outputs),
                )?],
            }];
            self.join
                .validate_prepared_epoch(&prepared_join)
                .map_err(|_| invalid_runtime_state())?;
            self.engine
                .validate_prepared_epoch(&prepared_engine)
                .map_err(|_| invalid_runtime_state())?;
            self.join
                .commit_prepared_epoch(prepared_join)
                .map_err(|_| invalid_runtime_state())?;
            self.engine
                .commit_prepared_epoch(prepared_engine)
                .map_err(|_| invalid_runtime_state())?;
            // Publish only after output and both state tokens validated.
            self.published_output = staged_output;
            self.input_frontiers = executor_commit.input_frontiers.clone();
            self.input_event_time_frontiers = executor_commit.input_event_time_frontiers.clone();
            self.applied_epochs
                .insert(idempotency_key_text, logical_epoch);
            retain_recent_applied_epochs(&mut self.applied_epochs);
            Ok(EpochCommit {
                logical_epoch,
                idempotency_key,
                input_frontiers: executor_commit.input_frontiers,
                input_event_time_frontiers: executor_commit.input_event_time_frontiers,
                output_deltas: vec![ViewOutputDelta {
                    view_id: self.identity.view_ids[0].clone(),
                    schema_fingerprint: self.output_schema_fingerprint(),
                    delta: previous_output
                        .diff(&self.published_output)
                        .map_err(|_| invalid_runtime_state())?,
                }],
                output_batches,
            })
        } else {
            let aggregate_outputs = supported_join_view_plan_aggregate_outputs(&self.plan);
            let staged_output =
                apply_published_output_delta(&self.published_output, &output_delta)?;
            // Validate output before commit
            let output_batches = vec![ViewOutputBatch {
                view_id: self.identity.view_ids[0].clone(),
                schema_fingerprint: self.output_schema_fingerprint(),
                batches: vec![materialized_delta_to_record_batch(
                    &self.output_schema,
                    &staged_output,
                    Some(&aggregate_outputs),
                )?],
            }];
            self.join
                .validate_prepared_epoch(&prepared_join)
                .map_err(|_| invalid_runtime_state())?;
            self.engine
                .validate_prepared_epoch(&prepared_engine)
                .map_err(|_| invalid_runtime_state())?;
            self.join
                .commit_prepared_epoch(prepared_join)
                .map_err(|_| invalid_runtime_state())?;
            self.engine
                .commit_prepared_epoch(prepared_engine)
                .map_err(|_| invalid_runtime_state())?;
            // Publish only after output and both state tokens validated.
            self.published_output = staged_output;
            self.input_frontiers = executor_commit.input_frontiers.clone();
            self.input_event_time_frontiers = executor_commit.input_event_time_frontiers.clone();
            self.applied_epochs
                .insert(idempotency_key_text, logical_epoch);
            retain_recent_applied_epochs(&mut self.applied_epochs);
            Ok(EpochCommit {
                logical_epoch,
                idempotency_key,
                input_frontiers: executor_commit.input_frontiers,
                input_event_time_frontiers: executor_commit.input_event_time_frontiers,
                output_deltas: vec![ViewOutputDelta {
                    view_id: self.identity.view_ids[0].clone(),
                    schema_fingerprint: self.output_schema_fingerprint(),
                    delta: output_delta,
                }],
                output_batches,
            })
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
            logical_epoch: self.engine.logical_epoch(),
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
            logical_epoch: self.engine.logical_epoch(),
            input_frontiers: self.input_frontiers.clone(),
            input_event_time_frontiers: self.input_event_time_frontiers.clone(),
            output_frontiers: self
                .identity
                .view_ids
                .iter()
                .map(|view_id| ViewFrontier {
                    view_id: view_id.clone(),
                    committed_epoch: self.engine.logical_epoch(),
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
        Self::restore_for_execution_mode(checkpoint, JoinExecutionModeV1::SelectedSpecialization)
    }
}

const TYPED_JOIN_PROJECT_RUNTIME_KIND: &str = "typed_join_project_v1";
const TYPED_JOIN_PROJECT_KEY_CODEC: &str = "velorix-typed-full-row-bag-key-v2";

fn typed_join_scalar(
    value: Option<&Value>,
    column: &RelationColumnV1,
    event_time: bool,
) -> super::RuntimeScalarValue {
    use super::RuntimeScalarValue as Scalar;
    use velorix_core::view_plan::RuntimeScalarTypeV1 as Type;
    let ty = if event_time {
        Type::TimestampNanosecond
    } else {
        match &column.physical_arrow_type {
            ArrowPhysicalTypeV1::Boolean => Type::Boolean,
            ArrowPhysicalTypeV1::Int64 => Type::Int64,
            ArrowPhysicalTypeV1::Float64 => Type::Float64,
            ArrowPhysicalTypeV1::Utf8 => Type::Utf8,
            ArrowPhysicalTypeV1::Date32 => Type::Date32,
            ArrowPhysicalTypeV1::TimestampNanosecond { .. } => Type::TimestampNanosecond,
            _ => Type::Int64,
        }
    };
    let Some(value) = value.filter(|value| !value.is_null()) else {
        return Scalar::Null(ty);
    };
    match ty {
        Type::Boolean => value.as_bool().map(Scalar::Boolean),
        Type::Int64 => value.as_i64().map(Scalar::Int64),
        Type::Float64 => value
            .as_f64()
            .filter(|value| value.is_finite())
            .map(Scalar::Float64),
        Type::Utf8 => value.as_str().map(|value| Scalar::Utf8(value.to_owned())),
        Type::Date32 => value
            .as_i64()
            .and_then(|value| i32::try_from(value).ok())
            .map(Scalar::Date32),
        Type::TimestampNanosecond => value.as_i64().map(Scalar::TimestampNanosecond),
        Type::Decimal128 { .. } => None,
    }
    .unwrap_or(Scalar::Null(ty))
}

// NULL-containing tuples inhabit disjoint side namespaces. They retain their
// multiplicity for outer output and retraction but never join across sides.
fn typed_join_equality_key(values: Vec<Value>, side: SupportedAggregateInputRelationSide) -> Value {
    if values.iter().any(Value::is_null) {
        serde_json::json!({"null_side": if side == SupportedAggregateInputRelationSide::Left { "left" } else { "right" }, "tuple": values})
    } else {
        Value::Array(values)
    }
}

pub(super) fn checkpoint_has_typed_join_project_payload(checkpoint: &RuntimeCheckpoint) -> bool {
    checkpoint_kind(checkpoint).as_deref() == Some(TYPED_JOIN_PROJECT_RUNTIME_KIND)
}

/// Typed bag projection over the same sparse internal equijoin used by aggregate joins.
pub struct TypedJoinProjectRuntime {
    limits: RuntimeResourceLimits,
    checkpoint_row_cost: usize,
    identity: StandingProgramIdentity,
    catalogs: Vec<VelorixRelationCatalogV1>,
    input_schemas: Vec<RelationSchema>,
    output_schema: RelationSchema,
    view_sql: String,
    plan: SupportedTypedJoinProjectPlanV1,
    logical_plan: VelorixLogicalViewPlanV1,
    join: JoinOperator,
    output: BTreeMap<TypedBinaryKey, DeltaRecord>,
    input_frontiers: Vec<RelationFrontier>,
    input_event_time_frontiers: Vec<InputEventTimeFrontier>,
    applied_epochs: BTreeMap<String, LogicalEpoch>,
    logical_epoch: LogicalEpoch,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TypedJoinProjectCheckpoint {
    schema_version: u32,
    runtime_kind: String,
    key_codec: String,
    catalogs: Vec<VelorixRelationCatalogV1>,
    input_schemas: Vec<RelationSchema>,
    output_schema: RelationSchema,
    view_sql: String,
    plan: SupportedTypedJoinProjectPlanV1,
    logical_plan: VelorixLogicalViewPlanV1,
    left_state: DeltaBatch,
    right_state: DeltaBatch,
    output: DeltaBatch,
    logical_epoch: LogicalEpoch,
    input_frontiers: Vec<RelationFrontier>,
    input_event_time_frontiers: Vec<InputEventTimeFrontier>,
    applied_epochs: BTreeMap<String, LogicalEpoch>,
}

impl TypedJoinProjectRuntime {
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_logical_plan_and_limits(
        identity: StandingProgramIdentity,
        catalogs: Vec<VelorixRelationCatalogV1>,
        input_schemas: Vec<RelationSchema>,
        output_schema: RelationSchema,
        view_sql: String,
        plan: SupportedTypedJoinProjectPlanV1,
        logical_plan: VelorixLogicalViewPlanV1,
        limits: RuntimeResourceLimits,
    ) -> Result<Self, StandingProgramRuntimeError> {
        limits.validate()?;
        let mut runtime = Self::new_with_logical_plan(
            identity,
            catalogs,
            input_schemas,
            output_schema,
            view_sql,
            plan,
            logical_plan,
        )?;
        runtime.limits = limits;
        runtime.checkpoint()?;
        Ok(runtime)
    }

    pub fn new_with_logical_plan(
        identity: StandingProgramIdentity,
        catalogs: Vec<VelorixRelationCatalogV1>,
        input_schemas: Vec<RelationSchema>,
        output_schema: RelationSchema,
        view_sql: String,
        plan: SupportedTypedJoinProjectPlanV1,
        logical_plan: VelorixLogicalViewPlanV1,
    ) -> Result<Self, StandingProgramRuntimeError> {
        identity.validate()?;
        validate_builtin_runtime_identity(&identity)?;
        validate_view_sql_hash(&identity, &view_sql)?;
        validate_logical_view_plan(&logical_plan).map_err(|_| invalid_runtime_state())?;
        if catalogs.len() != 2
            || input_schemas.len() != 2
            || plan.left_relation_id == plan.right_relation_id
            || !matches!(
                plan.join_kind,
                SupportedJoinKind::Inner | SupportedJoinKind::Left | SupportedJoinKind::Full
            )
            || plan.key_pairs.is_empty()
            || plan.projection.is_empty()
        {
            return Err(invalid_runtime_state());
        }
        for catalog in &catalogs {
            let expected =
                catalog_input_relation_schema(catalog).map_err(|_| invalid_runtime_state())?;
            if !input_schemas.contains(&expected) {
                return Err(invalid_runtime_state());
            }
        }
        let compiled = validate_supported_typed_join_project_sql(&view_sql, &catalogs)
            .map_err(|_| invalid_runtime_state())?;
        let compiled_logical = lower_supported_typed_join_project_sql_to_logical_plan(
            &view_sql,
            &catalogs,
            &output_schema,
        )
        .map_err(|_| invalid_runtime_state())?;
        if compiled != plan || compiled_logical != logical_plan {
            return Err(invalid_runtime_state());
        }
        let runtime = Self {
            limits: RuntimeResourceLimits::default(),
            checkpoint_row_cost: 0,
            identity,
            catalogs,
            input_schemas,
            output_schema,
            view_sql,
            plan,
            logical_plan,
            join: new_join_operator(),
            output: BTreeMap::new(),
            input_frontiers: Vec::new(),
            input_event_time_frontiers: Vec::new(),
            applied_epochs: BTreeMap::new(),
            logical_epoch: 0,
        };
        runtime.validate_schema()?;
        runtime.snapshot_batch(&[])?;
        Ok(runtime)
    }

    fn catalog(
        &self,
        side: SupportedAggregateInputRelationSide,
    ) -> Result<&VelorixRelationCatalogV1, StandingProgramRuntimeError> {
        let id = match side {
            SupportedAggregateInputRelationSide::Left => &self.plan.left_relation_id,
            SupportedAggregateInputRelationSide::Right => &self.plan.right_relation_id,
        };
        self.catalogs
            .iter()
            .find(|catalog| &catalog.relation_schema.relation_id == id)
            .ok_or_else(invalid_runtime_state)
    }

    fn validate_schema(&self) -> Result<(), StandingProgramRuntimeError> {
        for catalog in &self.catalogs {
            if catalog.relation_schema.columns.iter().any(|column| {
                !matches!(
                    column.physical_arrow_type,
                    ArrowPhysicalTypeV1::Boolean
                        | ArrowPhysicalTypeV1::Utf8
                        | ArrowPhysicalTypeV1::JsonUtf8
                        | ArrowPhysicalTypeV1::Int64
                        | ArrowPhysicalTypeV1::Float64
                        | ArrowPhysicalTypeV1::Date32
                        | ArrowPhysicalTypeV1::TimestampNanosecond { timezone: None }
                        | ArrowPhysicalTypeV1::Decimal128 { .. }
                )
            }) {
                return Err(invalid_runtime_state());
            }
        }
        let mut referenced = BTreeSet::new();
        for program in self.plan.predicate.iter().chain(
            self.plan
                .projection
                .iter()
                .filter_map(|projection| projection.expression.as_ref()),
        ) {
            referenced.extend(super::typed_expr_column_ids(program));
        }
        let mut bound = BTreeSet::new();
        for binding in &self.plan.bound_columns {
            let catalog = self.catalog(binding.side)?;
            let column = catalog_column(catalog, &binding.column_id)?;
            let side = if binding.side == SupportedAggregateInputRelationSide::Left {
                "left"
            } else {
                "right"
            };
            let nullable = column.nullable
                || self.plan.join_kind == SupportedJoinKind::Full
                || (binding.side == SupportedAggregateInputRelationSide::Right
                    && self.plan.join_kind == SupportedJoinKind::Left);
            if binding.internal_id != format!("__velorix_join_{side}_{}", binding.column_id)
                || binding.nullable != nullable
                || !referenced.contains(&binding.internal_id)
                || !bound.insert(binding.internal_id.as_str())
            {
                return Err(invalid_runtime_state());
            }
        }
        let bound_ids = bound
            .iter()
            .map(|id| (*id).to_owned())
            .collect::<BTreeSet<_>>();
        if referenced != bound_ids {
            return Err(invalid_runtime_state());
        }
        if self.plan.projection.is_empty()
            || !self.output_schema.primary_key.is_empty()
            || self.output_schema.columns.len() != self.plan.projection.len()
        {
            return Err(invalid_runtime_state());
        }
        let mut names = BTreeSet::new();
        for (projection, output) in self.plan.projection.iter().zip(&self.output_schema.columns) {
            if let Some(program) = &projection.expression {
                if output.name != projection.output_name
                    || output.data_type != super::typed_result_sql_data_type(program)?
                    || output.nullable != program.root.nullable
                    || !names.insert(&output.name)
                {
                    return Err(invalid_runtime_state());
                }
                continue;
            }
            let catalog = self.catalog(projection.side)?;
            let column = catalog_column(catalog, &projection.column_id)?;
            let nullable = if let Some(right_id) = &projection.coalesce_right_column_id {
                if projection.side != SupportedAggregateInputRelationSide::Left
                    || !self.plan.key_pairs.iter().any(|pair| {
                        pair.left_column_id == projection.column_id
                            && pair.right_column_id == *right_id
                    })
                {
                    return Err(invalid_runtime_state());
                }
                let right = catalog_column(
                    self.catalog(SupportedAggregateInputRelationSide::Right)?,
                    right_id,
                )?;
                if right.physical_arrow_type != column.physical_arrow_type {
                    return Err(invalid_runtime_state());
                }
                column.nullable || right.nullable
            } else {
                column.nullable
                    || self.plan.join_kind == SupportedJoinKind::Full
                    || (projection.side == SupportedAggregateInputRelationSide::Right
                        && self.plan.join_kind == SupportedJoinKind::Left)
            };
            if column.column_id == catalog.relation_schema.weight_column_id
                || output.name != projection.output_name
                || output.data_type != sql_type_from_catalog_column(column)?
                || output.nullable != nullable
                || !names.insert(&output.name)
            {
                return Err(invalid_runtime_state());
            }
        }
        let left = self.catalog(SupportedAggregateInputRelationSide::Left)?;
        let right = self.catalog(SupportedAggregateInputRelationSide::Right)?;
        for pair in &self.plan.key_pairs {
            let left_column = catalog_column(left, &pair.left_column_id)?;
            let right_column = catalog_column(right, &pair.right_column_id)?;
            if left_column.physical_arrow_type != right_column.physical_arrow_type
                || left_column.column_id == left.relation_schema.weight_column_id
                || right_column.column_id == right.relation_schema.weight_column_id
            {
                return Err(invalid_runtime_state());
            }
        }
        Ok(())
    }

    fn input_delta(
        &self,
        input: &RelationInputBatch,
        side: SupportedAggregateInputRelationSide,
    ) -> Result<DeltaBatch, StandingProgramRuntimeError> {
        let catalog = self.catalog(side)?;
        if let Some(empty) = published_input_empty_delta(input, catalog)? {
            return Ok(empty);
        }
        let keys = self
            .plan
            .key_pairs
            .iter()
            .map(|pair| {
                if side == SupportedAggregateInputRelationSide::Left {
                    pair.left_column_id.clone()
                } else {
                    pair.right_column_id.clone()
                }
            })
            .collect::<Vec<_>>();
        // Retain every payload column so distinct source rows keep their bag multiplicity.
        let columns = catalog
            .relation_schema
            .columns
            .iter()
            .filter(|column| column.column_id != catalog.relation_schema.weight_column_id)
            .map(|column| column.column_id.clone())
            .collect::<Vec<_>>();
        let mut records = Vec::new();
        if input.batches.is_empty() {
            return Err(invalid_runtime_state());
        }
        for batch in &input.batches {
            validate_record_batch_matches_catalog(catalog, batch)
                .map_err(|_| invalid_runtime_state())?;
            let weight_column = catalog_column(catalog, &catalog.relation_schema.weight_column_id)?;
            let weights = batch
                .column_by_name(&weight_column.name)
                .and_then(|array| array.as_any().downcast_ref::<Int64Array>())
                .ok_or_else(invalid_runtime_state)?;
            for row in 0..batch.num_rows() {
                if weights.is_null(row) {
                    return Err(invalid_runtime_state());
                }
                let values = columns
                    .iter()
                    .map(|id| {
                        let column = catalog_column(catalog, id)?;
                        Ok((id.clone(), typed_join_arrow_value(batch, column, row)?))
                    })
                    .collect::<Result<Map<_, _>, StandingProgramRuntimeError>>()?;
                records.push(DeltaRecord::new(
                    DeltaKey::from_json(Value::Null),
                    DeltaValue::from_json(Value::Object(values)),
                    weights.value(row),
                ));
            }
        }
        let delta = DeltaBatch::from_records(records);
        let records = delta
            .records()
            .iter()
            .map(|row| {
                let value = row
                    .value
                    .as_json()
                    .as_object()
                    .ok_or_else(invalid_runtime_state)?;
                let key = keys
                    .iter()
                    .map(|id| {
                        let mut atom = value.get(id).cloned().ok_or_else(invalid_runtime_state)?;
                        if catalog_column(catalog, id)?.physical_arrow_type
                            == ArrowPhysicalTypeV1::Float64
                            && atom.as_f64() == Some(0.0)
                        {
                            atom = serde_json::json!(0.0);
                        }
                        Ok(atom)
                    })
                    .collect::<Result<Vec<_>, StandingProgramRuntimeError>>()?;
                Ok(DeltaRecord::new(
                    DeltaKey::from_json(typed_join_equality_key(key, side)),
                    row.value.clone(),
                    row.weight,
                ))
            })
            .collect::<Result<Vec<_>, StandingProgramRuntimeError>>()?;
        Ok(DeltaBatch::from_records(records))
    }

    fn project(&self, joined: &DeltaBatch) -> Result<DeltaBatch, StandingProgramRuntimeError> {
        let records = joined
            .records()
            .iter()
            .map(|row| {
                let joined = row
                    .value
                    .as_json()
                    .as_object()
                    .ok_or_else(invalid_runtime_state)?;
                let get_column = |internal_id: &str| {
                    let binding = self
                        .plan
                        .bound_columns
                        .iter()
                        .find(|binding| binding.internal_id == internal_id)?;
                    let catalog = self.catalog(binding.side).ok()?;
                    let column = catalog_column(catalog, &binding.column_id).ok()?;
                    let side_key = if binding.side == SupportedAggregateInputRelationSide::Left {
                        JOIN_LEFT_VALUE_FIELD
                    } else {
                        JOIN_RIGHT_VALUE_FIELD
                    };
                    let payload = joined.get(side_key)?;
                    let value = if payload.is_null() {
                        None
                    } else {
                        payload.get(&binding.column_id)
                    };
                    Some(typed_join_scalar(
                        value,
                        column,
                        catalog.relation_schema.event_time_column_id.as_deref()
                            == Some(column.column_id.as_str()),
                    ))
                };
                if let Some(predicate) = &self.plan.predicate {
                    match super::evaluate_typed_expr(predicate, &get_column)
                        .map_err(|_| invalid_runtime_state())?
                    {
                        super::RuntimeScalarValue::Boolean(true) => {}
                        super::RuntimeScalarValue::Boolean(false)
                        | super::RuntimeScalarValue::Null(
                            velorix_core::view_plan::RuntimeScalarTypeV1::Boolean,
                        ) => return Ok(None),
                        _ => return Err(invalid_runtime_state()),
                    }
                }
                let mut values = Map::new();
                for projection in &self.plan.projection {
                    if let Some(program) = &projection.expression {
                        let value = super::evaluate_typed_expr(program, &get_column)
                            .map_err(|_| invalid_runtime_state())?;
                        values.insert(
                            projection.output_name.clone(),
                            super::typed_scalar_value_to_json(value)?,
                        );
                        continue;
                    }
                    let side = match projection.side {
                        SupportedAggregateInputRelationSide::Left => JOIN_LEFT_VALUE_FIELD,
                        SupportedAggregateInputRelationSide::Right => JOIN_RIGHT_VALUE_FIELD,
                    };
                    let payload = joined.get(side).ok_or_else(invalid_runtime_state)?;
                    let mut value = if payload.is_null() {
                        Value::Null
                    } else {
                        payload
                            .get(&projection.column_id)
                            .cloned()
                            .ok_or_else(invalid_runtime_state)?
                    };
                    if value.is_null() {
                        if let Some(right_id) = &projection.coalesce_right_column_id {
                            let right = joined
                                .get(JOIN_RIGHT_VALUE_FIELD)
                                .ok_or_else(invalid_runtime_state)?;
                            value = if right.is_null() {
                                Value::Null
                            } else {
                                right
                                    .get(right_id)
                                    .cloned()
                                    .ok_or_else(invalid_runtime_state)?
                            };
                        }
                    }
                    values.insert(projection.output_name.clone(), value);
                }
                Ok(Some(DeltaRecord::new(
                    DeltaKey::from_json(Value::Object(values)),
                    DeltaValue::from_json(Value::Null),
                    row.weight,
                )))
            })
            .collect::<Result<Vec<_>, StandingProgramRuntimeError>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        DeltaBatch::from_records(records)
            .net_rows()
            .map(DeltaBatch::from_records)
            .map_err(|_| invalid_runtime_state())
    }

    fn snapshot_batch(
        &self,
        rows: &[DeltaRecord],
    ) -> Result<RecordBatch, StandingProgramRuntimeError> {
        output::materialized_bag_delta_page_batch(
            &self.output_schema,
            &DeltaBatch::from_records(rows.to_vec()),
            self.logical_epoch,
            SnapshotPageRequest::default(),
        )
        .map(|(batch, _)| batch)
    }

    fn apply(
        &mut self,
        logical_epoch: LogicalEpoch,
        idempotency_key: EpochIdempotencyKey,
        inputs: Vec<RelationInputBatch>,
        snapshot: bool,
    ) -> Result<EpochCommit, StandingProgramRuntimeError> {
        let key = idempotency_key.as_str().to_string();
        if let Some(first_epoch) = self.applied_epochs.get(&key) {
            if *first_epoch != logical_epoch {
                return Err(StandingProgramRuntimeError::IdempotencyKeyConflict {
                    idempotency_key: key,
                    first_epoch: *first_epoch,
                    attempted_epoch: logical_epoch,
                });
            }
            return Ok(EpochCommit {
                logical_epoch,
                idempotency_key,
                input_frontiers: self.input_frontiers.clone(),
                input_event_time_frontiers: self.input_event_time_frontiers.clone(),
                output_deltas: Vec::new(),
                output_batches: if snapshot {
                    vec![ViewOutputBatch {
                        view_id: self.identity.view_ids[0].clone(),
                        schema_fingerprint: self.output_schema.schema_fingerprint.clone(),
                        batches: vec![self
                            .snapshot_batch(&self.output.values().cloned().collect::<Vec<_>>())?],
                    }]
                } else {
                    Vec::new()
                },
            });
        }
        if logical_epoch <= self.logical_epoch {
            return Err(StandingProgramRuntimeError::NonMonotonicLogicalEpoch {
                current: self.logical_epoch,
                attempted: logical_epoch,
            });
        }
        let mut frontiers = self.input_frontiers.clone();
        let mut event_frontiers = self.input_event_time_frontiers.clone();
        let mut left = DeltaBatch::default();
        let mut right = DeltaBatch::default();
        for input in &inputs {
            validate_input_matches_one_schema(input, &self.input_schemas, "typed_join_input")?;
            let (side, target) = if input.relation_id == self.plan.left_relation_id {
                (SupportedAggregateInputRelationSide::Left, &mut left)
            } else if input.relation_id == self.plan.right_relation_id {
                (SupportedAggregateInputRelationSide::Right, &mut right)
            } else {
                return Err(invalid_runtime_state());
            };
            *target = target.combine(&self.input_delta(input, side)?);
            advance_input_frontier(&mut frontiers, input)?;
            advance_input_event_time_frontier(&mut event_frontiers, input)?;
        }
        // Consolidation makes same-epoch replacement ordering irrelevant.
        let left = DeltaBatch::from_records(left.net_rows().map_err(|_| invalid_runtime_state())?);
        let right =
            DeltaBatch::from_records(right.net_rows().map_err(|_| invalid_runtime_state())?);
        let prepared = self
            .join
            .prepare_epoch(
                logical_epoch,
                &[(JoinInputSide::Left, &left), (JoinInputSide::Right, &right)],
            )
            .map_err(|_| invalid_runtime_state())?;
        for side in [JoinInputSide::Left, JoinInputSide::Right] {
            for key in prepared.touched_keys(side) {
                let rows = self
                    .join
                    .prepared_side_records(&prepared, side, &key, true)
                    .map_err(|_| invalid_runtime_state())?;
                join_records_weight(&rows)?;
            }
        }
        let delta = self.project(&prepared_join_output_for_plan(
            &self.join,
            &prepared,
            self.plan.join_kind,
        )?)?;
        let mut updates = BTreeMap::new();
        let mut units = Vec::new();
        for row in delta.records() {
            let encoded = encode_kv_ordered(row.key.as_json(), row.value.as_json());
            let weight = self
                .output
                .get(&encoded)
                .map_or(0, |row| row.weight)
                .checked_add(row.weight)
                .ok_or_else(invalid_runtime_state)?;
            if weight < 0 {
                return Err(invalid_runtime_state());
            }
            let mut next = row.clone();
            next.weight = weight;
            updates.insert(encoded, (weight != 0).then_some(next));
            let mut unit = row.clone();
            unit.weight = 1;
            units.push(unit);
        }
        // Type/nullability validation must finish before either retained state changes.
        self.snapshot_batch(&units)?;
        let output_batches = if snapshot {
            let mut rows = self
                .output
                .iter()
                .filter(|(key, _)| !updates.contains_key(*key))
                .map(|(_, row)| row.clone())
                .collect::<Vec<_>>();
            rows.extend(updates.values().filter_map(Clone::clone));
            vec![ViewOutputBatch {
                view_id: self.identity.view_ids[0].clone(),
                schema_fingerprint: self.output_schema.schema_fingerprint.clone(),
                batches: vec![self.snapshot_batch(&rows)?],
            }]
        } else {
            Vec::new()
        };
        let mut next_row_cost = self.checkpoint_row_cost;
        for join_side in [JoinInputSide::Left, JoinInputSide::Right] {
            for touched in prepared.touched_keys(join_side) {
                let before = self
                    .join
                    .prepared_side_records(&prepared, join_side, &touched, false)
                    .map_err(|_| invalid_runtime_state())?;
                let after = self
                    .join
                    .prepared_side_records(&prepared, join_side, &touched, true)
                    .map_err(|_| invalid_runtime_state())?;
                next_row_cost = adjust_checkpoint_row_cost(
                    next_row_cost,
                    checkpoint_rows_cost(&before)?,
                    checkpoint_rows_cost(&after)?,
                )?;
            }
        }
        for (encoded, next) in &updates {
            let previous_cost = self
                .output
                .get(encoded)
                .map_or(Ok(0), checkpoint_row_cost)?;
            let next_cost = next.as_ref().map_or(Ok(0), checkpoint_row_cost)?;
            next_row_cost = adjust_checkpoint_row_cost(next_row_cost, previous_cost, next_cost)?;
        }
        let mut applied_epochs = self.applied_epochs.clone();
        applied_epochs.insert(key.clone(), logical_epoch);
        retain_recent_applied_epochs(&mut applied_epochs);
        let metadata =
            self.checkpoint_metadata(logical_epoch, &frontiers, &event_frontiers, applied_epochs);
        let metadata_cost = self.limits.measure_checkpoint_value(&metadata)?;
        self.limits.validate_checkpoint_size(
            metadata_cost
                .checked_add(next_row_cost)
                .ok_or_else(invalid_runtime_state)?,
        )?;
        self.join
            .commit_prepared_epoch(prepared)
            .map_err(|_| invalid_runtime_state())?;
        for (key, row) in updates {
            if let Some(row) = row {
                self.output.insert(key, row);
            } else {
                self.output.remove(&key);
            }
        }
        self.input_frontiers = frontiers.clone();
        self.input_event_time_frontiers = event_frontiers.clone();
        self.logical_epoch = logical_epoch;
        self.applied_epochs.insert(key, logical_epoch);
        retain_recent_applied_epochs(&mut self.applied_epochs);
        self.checkpoint_row_cost = next_row_cost;
        Ok(EpochCommit {
            logical_epoch,
            idempotency_key,
            input_frontiers: frontiers,
            input_event_time_frontiers: event_frontiers,
            output_deltas: vec![ViewOutputDelta {
                view_id: self.identity.view_ids[0].clone(),
                schema_fingerprint: self.output_schema.schema_fingerprint.clone(),
                delta,
            }],
            output_batches,
        })
    }
}

impl TypedJoinProjectRuntime {
    pub(super) fn materialized_delta(&self) -> DeltaBatch {
        DeltaBatch::from_records(self.output.values().cloned())
    }

    fn checkpoint_metadata(
        &self,
        logical_epoch: LogicalEpoch,
        input_frontiers: &[RelationFrontier],
        input_event_time_frontiers: &[InputEventTimeFrontier],
        applied_epochs: BTreeMap<String, LogicalEpoch>,
    ) -> TypedJoinProjectCheckpoint {
        TypedJoinProjectCheckpoint {
            schema_version: 1,
            runtime_kind: TYPED_JOIN_PROJECT_RUNTIME_KIND.into(),
            key_codec: TYPED_JOIN_PROJECT_KEY_CODEC.into(),
            catalogs: self.catalogs.clone(),
            input_schemas: self.input_schemas.clone(),
            output_schema: self.output_schema.clone(),
            view_sql: self.view_sql.clone(),
            plan: self.plan.clone(),
            logical_plan: self.logical_plan.clone(),
            left_state: DeltaBatch::default(),
            right_state: DeltaBatch::default(),
            output: DeltaBatch::default(),
            logical_epoch,
            input_frontiers: input_frontiers.to_vec(),
            input_event_time_frontiers: input_event_time_frontiers.to_vec(),
            applied_epochs,
        }
    }

    fn checkpoint_data(&self) -> TypedJoinProjectCheckpoint {
        let mut checkpoint = self.checkpoint_metadata(
            self.logical_epoch,
            &self.input_frontiers,
            &self.input_event_time_frontiers,
            self.applied_epochs.clone(),
        );
        checkpoint.left_state = self.join.left_state();
        checkpoint.right_state = self.join.right_state();
        checkpoint.output = DeltaBatch::from_records(self.output.values().cloned());
        checkpoint
    }
}

impl StandingProgramRuntime for TypedJoinProjectRuntime {
    fn program_identity(&self) -> &StandingProgramIdentity {
        &self.identity
    }
    fn input_schemas(&self) -> Vec<RelationSchema> {
        self.input_schemas.clone()
    }
    fn output_schemas(&self) -> Vec<RelationSchema> {
        vec![self.output_schema.clone()]
    }
    fn logical_epoch(&self) -> LogicalEpoch {
        self.logical_epoch
    }

    fn apply_changes(
        &mut self,
        epoch: LogicalEpoch,
        key: EpochIdempotencyKey,
        inputs: Vec<RelationInputBatch>,
    ) -> Result<EpochCommit, StandingProgramRuntimeError> {
        self.apply(epoch, key, inputs, true)
    }

    fn apply_changes_delta_only(
        &mut self,
        epoch: LogicalEpoch,
        key: EpochIdempotencyKey,
        inputs: Vec<RelationInputBatch>,
    ) -> Result<EpochCommit, StandingProgramRuntimeError> {
        self.apply(epoch, key, inputs, false)
    }

    fn materialized_view_page(
        &self,
        view: ScopedViewId,
        page: SnapshotPageRequest,
    ) -> Result<MaterializedViewPage, StandingProgramRuntimeError> {
        if view.tenant_id != self.identity.tenant_id
            || view.program_id != self.identity.program_id
            || !self.identity.view_ids.contains(&view.view_id)
        {
            return Err(StandingProgramRuntimeError::UnknownView {
                view_id: view.view_id,
            });
        }
        if let Some(requested) = page.committed_epoch {
            if requested != self.logical_epoch {
                return Err(StandingProgramRuntimeError::UnavailableCommittedEpoch {
                    requested,
                    current: self.logical_epoch,
                });
            }
        }
        if page.max_rows == Some(0) {
            return Err(invalid_runtime_state());
        }
        let cursor = page
            .page_token
            .as_ref()
            .map(|token| {
                let (key, value, copy): (Value, Value, i64) =
                    serde_json::from_str(token).map_err(|_| invalid_runtime_state())?;
                let encoded = encode_kv_ordered(&key, &value);
                let row = self
                    .output
                    .get(&encoded)
                    .ok_or_else(invalid_runtime_state)?;
                if copy < 0 || copy >= row.weight {
                    return Err(invalid_runtime_state());
                }
                Ok((encoded, copy))
            })
            .transpose()?;
        let limit = page.max_rows.unwrap_or(usize::MAX);
        let mut rows = Vec::new();
        let mut last = None;
        let mut has_next = false;
        'page: for (encoded, row) in &self.output {
            let start = match &cursor {
                Some((key, _)) if encoded < key => continue,
                Some((key, copy)) if encoded == key => copy + 1,
                _ => 0,
            };
            for copy in start..row.weight {
                if rows.len() == limit {
                    has_next = true;
                    break 'page;
                }
                let mut unit = row.clone();
                unit.weight = 1;
                rows.push(unit);
                last = Some((row.key.as_json(), row.value.as_json(), copy));
            }
        }
        let next_page_token = if has_next {
            Some(
                serde_json::to_string(&last.ok_or_else(invalid_runtime_state)?)
                    .map_err(|_| invalid_runtime_state())?,
            )
        } else {
            None
        };
        Ok(MaterializedViewPage {
            view,
            logical_epoch: self.logical_epoch,
            schema_fingerprint: self.output_schema.schema_fingerprint.clone(),
            batches: vec![self.snapshot_batch(&rows)?],
            next_page_token,
        })
    }

    fn checkpoint(&self) -> Result<RuntimeCheckpoint, StandingProgramRuntimeError> {
        let payload = self.limits.checkpoint_arrow(&self.checkpoint_data())?;
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
                content_hash: stable_bytes_hash(payload.as_bytes()),
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
        Self::restore_with_limits(checkpoint, RuntimeResourceLimits::default())
    }
}

impl TypedJoinProjectRuntime {
    pub fn restore_with_limits(
        checkpoint: RuntimeCheckpoint,
        limits: RuntimeResourceLimits,
    ) -> Result<Self, StandingProgramRuntimeError> {
        limits.validate()?;
        checkpoint.validate_identity(&checkpoint.identity)?;
        let state = checkpoint
            .state_payload
            .as_ref()
            .ok_or_else(invalid_checkpoint)?;
        limits.validate_checkpoint(&state.payload)?;
        if state.codec_identity != checkpoint.checkpoint_codec_identity
            || stable_bytes_hash(state.payload.as_bytes()) != checkpoint.state_root.content_hash
        {
            return Err(invalid_checkpoint());
        }
        let payload: TypedJoinProjectCheckpoint =
            velorix_core::arrow_state::from_state(&state.payload)
                .map_err(|_| invalid_checkpoint())?;
        if payload.schema_version != 1
            || payload.runtime_kind != TYPED_JOIN_PROJECT_RUNTIME_KIND
            || payload.key_codec != TYPED_JOIN_PROJECT_KEY_CODEC
            || payload.logical_epoch != checkpoint.logical_epoch
            || payload.input_frontiers != checkpoint.input_frontiers
            || payload.input_event_time_frontiers != checkpoint.input_event_time_frontiers
        {
            return Err(invalid_checkpoint());
        }
        validate_checkpoint_frontiers_for_schemas(&checkpoint, &payload.input_schemas)?;
        validate_input_event_time_frontiers_for_catalogs(&checkpoint, &payload.catalogs)?;
        let mut runtime = Self::new_with_logical_plan_and_limits(
            checkpoint.identity,
            payload.catalogs,
            payload.input_schemas,
            payload.output_schema,
            payload.view_sql,
            payload.plan,
            payload.logical_plan,
            limits,
        )?;
        for (side, state) in [
            (
                SupportedAggregateInputRelationSide::Left,
                &payload.left_state,
            ),
            (
                SupportedAggregateInputRelationSide::Right,
                &payload.right_state,
            ),
        ] {
            runtime.validate_retained_side(side, state)?;
        }
        // Rebuild only at restore, and reject a checkpoint whose materialized bag disagrees with its sides.
        let prepared = runtime
            .join
            .prepare_epoch(
                payload.logical_epoch,
                &[
                    (JoinInputSide::Left, &payload.left_state),
                    (JoinInputSide::Right, &payload.right_state),
                ],
            )
            .map_err(|_| invalid_checkpoint())?;
        let expected = runtime.project(&prepared_join_output_for_plan(
            &runtime.join,
            &prepared,
            runtime.plan.join_kind,
        )?)?;
        let rows = payload
            .output
            .net_rows()
            .map_err(|_| invalid_checkpoint())?;
        if rows.iter().any(|row| row.weight <= 0)
            || rows != expected.net_rows().map_err(|_| invalid_checkpoint())?
        {
            return Err(invalid_checkpoint());
        }
        let left_cost =
            checkpoint_rows_cost(payload.left_state.records()).map_err(|_| invalid_checkpoint())?;
        let right_cost = checkpoint_rows_cost(payload.right_state.records())
            .map_err(|_| invalid_checkpoint())?;
        let output_cost = checkpoint_rows_cost(&rows).map_err(|_| invalid_checkpoint())?;
        let restored_row_cost = left_cost
            .checked_add(right_cost)
            .and_then(|cost| cost.checked_add(output_cost))
            .ok_or_else(invalid_checkpoint)?;
        let units = rows
            .iter()
            .map(|row| {
                let mut row = row.clone();
                row.weight = 1;
                row
            })
            .collect::<Vec<_>>();
        runtime.snapshot_batch(&units)?;
        runtime
            .join
            .commit_prepared_epoch(prepared)
            .map_err(|_| invalid_checkpoint())?;
        runtime.output = rows
            .into_iter()
            .map(|row| {
                (
                    encode_kv_ordered(row.key.as_json(), row.value.as_json()),
                    row,
                )
            })
            .collect();
        for (key, epoch) in &payload.applied_epochs {
            EpochIdempotencyKey::new(key.clone()).map_err(|_| invalid_checkpoint())?;
            if *epoch == 0 || *epoch > payload.logical_epoch {
                return Err(invalid_checkpoint());
            }
        }
        runtime.applied_epochs = payload.applied_epochs;
        retain_recent_applied_epochs(&mut runtime.applied_epochs);
        runtime.logical_epoch = payload.logical_epoch;
        runtime.input_frontiers = payload.input_frontiers;
        runtime.input_event_time_frontiers = payload.input_event_time_frontiers;
        runtime.checkpoint_row_cost = restored_row_cost;
        let metadata = runtime.checkpoint_metadata(
            runtime.logical_epoch,
            &runtime.input_frontiers,
            &runtime.input_event_time_frontiers,
            runtime.applied_epochs.clone(),
        );
        let metadata_cost = runtime
            .limits
            .measure_checkpoint_value(&metadata)
            .map_err(|_| invalid_checkpoint())?;
        runtime
            .limits
            .validate_checkpoint_size(
                metadata_cost
                    .checked_add(restored_row_cost)
                    .ok_or_else(invalid_checkpoint)?,
            )
            .map_err(|_| invalid_checkpoint())?;
        Ok(runtime)
    }
}

impl TypedJoinProjectRuntime {
    fn validate_retained_side(
        &self,
        side: SupportedAggregateInputRelationSide,
        state: &DeltaBatch,
    ) -> Result<(), StandingProgramRuntimeError> {
        let catalog = self.catalog(side)?;
        let keys = self
            .plan
            .key_pairs
            .iter()
            .map(|pair| {
                if side == SupportedAggregateInputRelationSide::Left {
                    &pair.left_column_id
                } else {
                    &pair.right_column_id
                }
            })
            .collect::<Vec<_>>();
        let mut schema =
            catalog_input_relation_schema(catalog).map_err(|_| invalid_checkpoint())?;
        schema
            .columns
            .retain(|column| column.name != catalog.relation_schema.weight_column_id);
        let first = catalog_column(catalog, keys[0])?;
        let index = schema
            .columns
            .iter()
            .position(|column| column.name == first.name)
            .ok_or_else(invalid_checkpoint)?;
        schema.columns.swap(0, index);
        let rows = state.net_rows().map_err(|_| invalid_checkpoint())?;
        let mut units = Vec::new();
        for row in rows {
            if row.weight <= 0 {
                return Err(invalid_checkpoint());
            }
            let values = row
                .value
                .as_json()
                .as_object()
                .ok_or_else(invalid_checkpoint)?;
            if values.len() != schema.columns.len() {
                return Err(invalid_checkpoint());
            }
            let key = keys
                .iter()
                .map(|id| {
                    let atom = values.get(*id).cloned().ok_or_else(invalid_checkpoint)?;
                    if atom.is_null() && !catalog_column(catalog, id)?.nullable {
                        return Err(invalid_checkpoint());
                    }
                    if catalog_column(catalog, id)?.physical_arrow_type
                        == ArrowPhysicalTypeV1::Float64
                        && atom.as_f64() == Some(0.0)
                    {
                        Ok(serde_json::json!(0.0))
                    } else {
                        Ok(atom)
                    }
                })
                .collect::<Result<Vec<_>, StandingProgramRuntimeError>>()?;
            if row.key.as_json() != &typed_join_equality_key(key, side) {
                return Err(invalid_checkpoint());
            }
            let renamed = catalog
                .relation_schema
                .columns
                .iter()
                .filter(|column| column.column_id != catalog.relation_schema.weight_column_id)
                .map(|column| {
                    Ok((
                        column.name.clone(),
                        values
                            .get(&column.column_id)
                            .cloned()
                            .ok_or_else(invalid_checkpoint)?,
                    ))
                })
                .collect::<Result<Map<_, _>, StandingProgramRuntimeError>>()?;
            units.push(DeltaRecord::new(
                DeltaKey::from_json(Value::Object(renamed)),
                DeltaValue::from_json(Value::Null),
                1,
            ));
        }
        schema.primary_key.clear();
        output::materialized_bag_delta_page_batch(
            &schema,
            &DeltaBatch::from_records(units),
            self.logical_epoch,
            SnapshotPageRequest::default(),
        )?;
        Ok(())
    }
}

pub(super) fn typed_join_arrow_value(
    batch: &RecordBatch,
    column: &RelationColumnV1,
    row: usize,
) -> Result<Value, StandingProgramRuntimeError> {
    let array = batch
        .column_by_name(&column.name)
        .ok_or_else(invalid_runtime_state)?;
    if array.is_null(row) {
        return if column.nullable {
            Ok(Value::Null)
        } else {
            Err(invalid_runtime_state())
        };
    }
    macro_rules! primitive {
        ($ty:ty) => {
            array
                .as_any()
                .downcast_ref::<$ty>()
                .ok_or_else(invalid_runtime_state)?
                .value(row)
        };
    }
    Ok(match column.physical_arrow_type {
        ArrowPhysicalTypeV1::Boolean => serde_json::json!(primitive!(BooleanArray)),
        ArrowPhysicalTypeV1::Utf8 | ArrowPhysicalTypeV1::JsonUtf8 => {
            serde_json::json!(primitive!(StringArray))
        }
        ArrowPhysicalTypeV1::Int64 => serde_json::json!(primitive!(Int64Array)),
        ArrowPhysicalTypeV1::Float64 => {
            let value = primitive!(arrow::array::Float64Array);
            if !value.is_finite() {
                return Err(invalid_runtime_state());
            }
            serde_json::json!(if value == 0.0 { 0.0 } else { value })
        }
        ArrowPhysicalTypeV1::Date32 => serde_json::json!(primitive!(Date32Array)),
        ArrowPhysicalTypeV1::TimestampNanosecond { .. } => {
            serde_json::json!(primitive!(TimestampNanosecondArray))
        }
        ArrowPhysicalTypeV1::Decimal128 { .. } => Value::String(
            arrow::util::display::array_value_to_string(array.as_ref(), row)
                .map_err(|_| invalid_runtime_state())?,
        ),
        _ => return Err(invalid_runtime_state()),
    })
}
