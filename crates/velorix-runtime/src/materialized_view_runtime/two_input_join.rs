use super::*;

pub struct TwoInputJoinRuntime {
    identity: StandingProgramIdentity,
    catalogs: Vec<VelorixRelationCatalogV1>,
    input_schemas: Vec<RelationSchema>,
    output_schema: RelationSchema,
    view_sql: String,
    plan: SupportedJoinViewPlan,
    logical_plan: VelorixLogicalViewPlanV1,
    engine: KeyedAggregateKernel,
    join: JoinOperator,
    published_output: DeltaBatch,
    filtered_aggregate_state: DeltaBatch,
    input_frontiers: Vec<RelationFrontier>,
    input_event_time_frontiers: Vec<InputEventTimeFrontier>,
    applied_epochs: BTreeMap<String, LogicalEpoch>,
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
        Ok(Self {
            identity,
            catalogs,
            input_schemas,
            output_schema,
            view_sql,
            plan,
            logical_plan,
            engine: KeyedAggregateKernel::with_aggregate_value_mode_and_extrema(
                value_mode,
                track_extrema,
            ),
            join: new_join_operator(),
            published_output: DeltaBatch::default(),
            filtered_aggregate_state: DeltaBatch::default(),
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
            runtime_kind: JOIN_RUNTIME_KIND.to_string(),
            catalogs: self.catalogs.clone(),
            input_schemas: self.input_schemas.clone(),
            output_schema: self.output_schema.clone(),
            view_sql: self.view_sql.clone(),
            plan: self.plan.clone(),
            logical_plan: self.logical_plan.clone(),
            input_frontiers: self.input_frontiers.clone(),
            input_event_time_frontiers: self.input_event_time_frontiers.clone(),
            left_state: self.join.left_state(),
            right_state: self.join.right_state(),
            engine: self.engine.checkpoint_state().to_payload(),
            published_output: Some(self.published_output.clone()),
            filtered_aggregate_state: self.filtered_aggregate_state.clone(),
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
            || payload.runtime_kind != JOIN_RUNTIME_KIND
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
        if join_plan_uses_runtime_aggregate_state(&self.plan) {
            let mut joined_changes = DeltaBatch::default();
            let mut input_frontiers = self.input_frontiers.clone();
            let mut input_event_time_frontiers = self.input_event_time_frontiers.clone();
            for input in input_changes {
                validate_input_matches_one_schema(
                    &input,
                    &self.input_schemas,
                    "generic_join_input_relation",
                )?;
                let catalog = join_catalog_for_relation(&self.catalogs, &input.relation_id)?;
                if input.relation_id == self.plan.left_input_relation_id {
                    let delta = join_left_input_delta_batch(catalog, &self.plan, &input)?;
                    let delta = prefilter_delta_batch_for_join_plan(&delta, &self.plan, catalog)?;
                    let joined = if self.plan.join_kind == SupportedJoinKind::Left {
                        left_join_left_delta_batch_to_joined_values(&delta)
                    } else {
                        self.join
                            .apply_left(&delta)
                            .map_err(|_| invalid_runtime_state())?
                    };
                    joined_changes = joined_changes.combine(&joined);
                } else if input.relation_id == self.plan.right_input_relation_id {
                    let delta = join_right_input_delta_batch(catalog, &self.plan, &input)?;
                    let delta = prefilter_delta_batch_for_join_plan(&delta, &self.plan, catalog)?;
                    if self.plan.join_kind != SupportedJoinKind::Left {
                        joined_changes = joined_changes.combine(
                            &self
                                .join
                                .apply_right(&delta)
                                .map_err(|_| invalid_runtime_state())?,
                        );
                    }
                } else {
                    return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                        field: "generic_join_input_relation",
                    });
                }
                advance_input_frontier(&mut input_frontiers, &input)?;
                advance_input_event_time_frontier(&mut input_event_time_frontiers, &input)?;
            }
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
            let visible_output = filter_output_delta_for_having(
                &next_state,
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
                .inverse()
                .map_err(|_| invalid_runtime_state())?
                .combine(&visible_output);
            self.engine
                .push_changes(logical_epoch, &DeltaBatch::default())
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
                output_batches: vec![ViewOutputBatch {
                    view_id: self.identity.view_ids[0].clone(),
                    schema_fingerprint: self.output_schema_fingerprint(),
                    batches: vec![self.materialized_batch()?],
                }],
            });
        }
        let mut executor = LogicalPlanExecutor::TwoInputJoin {
            catalogs: &self.catalogs,
            input_schemas: &self.input_schemas,
            plan: &self.plan,
            engine: &mut self.engine,
            join: &mut self.join,
        };
        let executor_commit = executor.apply_epoch(
            logical_epoch,
            &self.input_frontiers,
            &self.input_event_time_frontiers,
            input_changes,
        )?;
        let output_delta = filter_output_delta_for_having(
            &executor_commit.output_delta,
            self.plan.having.as_ref(),
            self.plan.having_expr.as_ref(),
            &self.output_schema,
            Some(&supported_join_view_plan_aggregate_outputs(&self.plan)),
        )?;
        let output_delta = if self.plan.top_k.is_some() {
            let previous_output = self.published_output.clone();
            let aggregate_outputs = supported_join_view_plan_aggregate_outputs(&self.plan);
            let full_output = filter_output_delta_for_having(
                &self.engine.materialized_state(),
                self.plan.having.as_ref(),
                self.plan.having_expr.as_ref(),
                &self.output_schema,
                Some(&aggregate_outputs),
            )?;
            self.published_output = apply_top_k_to_published_output(
                full_output,
                self.plan.top_k.as_ref(),
                &aggregate_outputs,
            )?;
            previous_output
                .inverse()
                .map_err(|_| invalid_runtime_state())?
                .combine(&self.published_output)
        } else {
            self.published_output =
                apply_published_output_delta(&self.published_output, &output_delta)?;
            output_delta
        };
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
            output_batches: vec![ViewOutputBatch {
                view_id: self.identity.view_ids[0].clone(),
                schema_fingerprint: self.output_schema_fingerprint(),
                batches: vec![self.materialized_batch()?],
            }],
        })
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
        })
    }

    fn restore(checkpoint: RuntimeCheckpoint) -> Result<Self, StandingProgramRuntimeError> {
        checkpoint.validate_identity(&checkpoint.identity)?;
        let mut payload = Self::restore_payload(&checkpoint)?;
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
        let filtered_aggregate_state = if payload.filtered_aggregate_state.records().is_empty() {
            published_output.clone()
        } else {
            payload.filtered_aggregate_state
        };
        validate_published_output(&published_output)?;
        validate_published_output(&filtered_aggregate_state)?;
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
