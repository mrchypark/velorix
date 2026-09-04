use super::*;

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

    fn checkpoint_payload(&self) -> Result<String, StandingProgramRuntimeError> {
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
        serde_json::to_string(&payload).map_err(|_| invalid_checkpoint())
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
            serde_json::from_str(&state_payload.payload).map_err(|_| invalid_checkpoint())?;
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
