use super::*;

pub struct AnalyticRowNumberRuntime {
    identity: StandingProgramIdentity,
    catalog: VelorixRelationCatalogV1,
    input_schema: RelationSchema,
    output_schema: RelationSchema,
    view_sql: String,
    plan: SupportedAnalyticRowNumberPlan,
    logical_plan: VelorixLogicalViewPlanV1,
    state: AnalyticRowNumberState,
    published_output: DeltaBatch,
    input_frontiers: Vec<RelationFrontier>,
    input_event_time_frontiers: Vec<InputEventTimeFrontier>,
    applied_epochs: BTreeMap<String, LogicalEpoch>,
    logical_epoch: LogicalEpoch,
}

impl AnalyticRowNumberRuntime {
    pub fn new_with_logical_plan(
        identity: StandingProgramIdentity,
        catalog: VelorixRelationCatalogV1,
        input_schema: RelationSchema,
        output_schema: RelationSchema,
        view_sql: String,
        plan: SupportedAnalyticRowNumberPlan,
        logical_plan: VelorixLogicalViewPlanV1,
    ) -> Result<Self, StandingProgramRuntimeError> {
        identity.validate()?;
        validate_builtin_runtime_identity(&identity)?;
        validate_view_sql_hash(&identity, view_sql.as_str())?;
        validate_logical_view_plan(&logical_plan).map_err(|_| {
            StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "logical_analytic_row_number_view_plan",
            }
        })?;
        validate_analytic_row_number_supported_schemas(
            &catalog,
            &input_schema,
            &output_schema,
            &plan,
        )?;
        let compiled_plan = validate_supported_analytic_row_number_sql(view_sql.as_str(), &catalog)
            .map_err(|_| StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "analytic_row_number_view_plan",
            })?;
        if compiled_plan != plan {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "analytic_row_number_view_plan",
            });
        }
        let compiled_logical_plan = lower_supported_analytic_row_number_sql_to_logical_plan(
            view_sql.as_str(),
            &catalog,
            &output_schema,
        )
        .map_err(|_| StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "logical_analytic_row_number_view_plan",
        })?;
        if compiled_logical_plan != logical_plan {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "logical_analytic_row_number_view_plan",
            });
        }
        if !matches!(
            &logical_plan.execution,
            VelorixLogicalViewExecutionV1::AnalyticRowNumber { plan: logical_plan }
                if logical_plan == &plan
        ) {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "logical_analytic_row_number_view_plan",
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
            state: AnalyticRowNumberState::default(),
            published_output: DeltaBatch::default(),
            input_frontiers: Vec::new(),
            input_event_time_frontiers: Vec::new(),
            applied_epochs: BTreeMap::new(),
            logical_epoch: 0,
        })
    }

    fn output_schema_fingerprint(&self) -> String {
        self.output_schema.schema_fingerprint.clone()
    }

    fn materialized_batch(&self) -> Result<RecordBatch, StandingProgramRuntimeError> {
        materialized_generic_delta_to_record_batch(&self.output_schema, &self.published_output)
    }

    fn materialized_page_batch(
        &self,
        page: SnapshotPageRequest,
    ) -> Result<(RecordBatch, Option<String>), StandingProgramRuntimeError> {
        materialized_generic_delta_page_batch(
            &self.output_schema,
            &self.published_output,
            self.logical_epoch,
            page,
        )
    }

    fn checkpoint_payload(&self) -> Result<String, StandingProgramRuntimeError> {
        let payload = AnalyticRowNumberCheckpointPayload {
            schema_version: CHECKPOINT_PAYLOAD_SCHEMA_VERSION,
            runtime_kind: ANALYTIC_ROW_NUMBER_RUNTIME_KIND.to_string(),
            catalog: self.catalog.clone(),
            input_schema: self.input_schema.clone(),
            output_schema: self.output_schema.clone(),
            view_sql: self.view_sql.clone(),
            plan: self.plan.clone(),
            logical_plan: self.logical_plan.clone(),
            input_frontiers: self.input_frontiers.clone(),
            input_event_time_frontiers: self.input_event_time_frontiers.clone(),
            state: self.state.clone(),
            published_output: self.published_output.clone(),
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
    ) -> Result<AnalyticRowNumberCheckpointPayload, StandingProgramRuntimeError> {
        let Some(state_payload) = &checkpoint.state_payload else {
            return Err(invalid_checkpoint());
        };
        if state_payload.codec_identity != checkpoint.checkpoint_codec_identity {
            return Err(StandingProgramRuntimeError::CheckpointCodecMismatch {
                expected: checkpoint.checkpoint_codec_identity.clone(),
                actual: state_payload.codec_identity.clone(),
            });
        }
        let payload: AnalyticRowNumberCheckpointPayload =
            serde_json::from_str(&state_payload.payload).map_err(|_| invalid_checkpoint())?;
        if payload.schema_version != CHECKPOINT_PAYLOAD_SCHEMA_VERSION
            || payload.runtime_kind != ANALYTIC_ROW_NUMBER_RUNTIME_KIND
        {
            return Err(invalid_checkpoint());
        }
        validate_analytic_row_number_supported_schemas(
            &payload.catalog,
            &payload.input_schema,
            &payload.output_schema,
            &payload.plan,
        )?;
        Ok(payload)
    }
}

impl StandingProgramRuntime for AnalyticRowNumberRuntime {
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

        let previous_state = self.state.clone();
        let mut next_state = self.state.clone();
        let mut affected_partitions = BTreeMap::<String, Value>::new();
        let mut input_frontiers = self.input_frontiers.clone();
        let mut input_event_time_frontiers = self.input_event_time_frontiers.clone();
        let value_column_ids = analytic_row_number_input_column_ids(&self.plan);
        for input in input_changes {
            validate_input_matches_schema(&input, &self.input_schema, "analytic_row_number_input")?;
            let delta =
                if let Some(empty_delta) = published_input_empty_delta(&input, &self.catalog)? {
                    empty_delta
                } else {
                    arrow_record_batches_to_key_multi_value_delta_batch(
                        &self.catalog,
                        &input.relation_id,
                        &input.relation_version,
                        &input.schema_fingerprint,
                        std::slice::from_ref(&self.plan.key_column_id),
                        &value_column_ids,
                        &input.batches,
                    )
                    .map_err(|_| {
                        StandingProgramRuntimeError::InvalidProgramIdentity {
                            field: "analytic_row_number_input_batch",
                        }
                    })?
                };
            let delta =
                filter_delta_batch_for_analytic_row_number_plan(&delta, &self.plan, &self.catalog)?;
            next_state.apply_delta(&delta, &self.plan, &self.catalog, &mut affected_partitions)?;
            advance_input_frontier(&mut input_frontiers, &input)?;
            advance_input_event_time_frontier(&mut input_event_time_frontiers, &input)?;
        }

        let mut output_delta = DeltaBatch::default();
        for partition in affected_partitions.values() {
            let old_output =
                previous_state.partition_output(&self.catalog, &self.plan, partition)?;
            let new_output = next_state.partition_output(&self.catalog, &self.plan, partition)?;
            output_delta = output_delta.combine(
                &old_output
                    .diff(&new_output)
                    .map_err(|_| invalid_runtime_state())?,
            );
        }
        let next_output = apply_published_output_delta(&self.published_output, &output_delta)?;
        // Validate output before commit
        let output_batches = vec![ViewOutputBatch {
            view_id: self.identity.view_ids[0].clone(),
            schema_fingerprint: self.output_schema_fingerprint(),
            batches: vec![materialized_generic_delta_to_record_batch(
                &self.output_schema,
                &next_output,
            )?],
        }];
        // Commit staged state
        self.state = next_state;
        self.published_output = next_output;
        self.input_frontiers = input_frontiers.clone();
        self.input_event_time_frontiers = input_event_time_frontiers.clone();
        self.applied_epochs
            .insert(idempotency_key_text, logical_epoch);
        retain_recent_applied_epochs(&mut self.applied_epochs);
        self.logical_epoch = logical_epoch;

        Ok(EpochCommit {
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
        validate_input_event_time_frontiers_for_catalogs(
            &checkpoint,
            std::slice::from_ref(&payload.catalog),
        )?;
        validate_checkpoint_frontiers_for_schemas(
            &checkpoint,
            std::slice::from_ref(&payload.input_schema),
        )?;
        validate_view_sql_hash(&checkpoint.identity, payload.view_sql.as_str())?;
        validate_logical_view_plan(&payload.logical_plan).map_err(|_| invalid_checkpoint())?;
        let compiled_plan =
            validate_supported_analytic_row_number_sql(payload.view_sql.as_str(), &payload.catalog)
                .map_err(|_| invalid_checkpoint())?;
        if compiled_plan != payload.plan {
            return Err(invalid_checkpoint());
        }
        let compiled_logical_plan = lower_supported_analytic_row_number_sql_to_logical_plan(
            payload.view_sql.as_str(),
            &payload.catalog,
            &payload.output_schema,
        )
        .map_err(|_| invalid_checkpoint())?;
        if compiled_logical_plan != payload.logical_plan {
            return Err(invalid_checkpoint());
        }
        if !matches!(
            &payload.logical_plan.execution,
            VelorixLogicalViewExecutionV1::AnalyticRowNumber { plan } if plan == &payload.plan
        ) {
            return Err(invalid_checkpoint());
        }
        validate_published_output(&payload.published_output)?;
        validate_analytic_row_number_checkpoint_output(
            &payload.state,
            &payload.published_output,
            &payload.catalog,
            &payload.plan,
        )?;
        let mut applied_epochs = payload
            .applied_epochs
            .into_iter()
            .map(|entry| (entry.idempotency_key, entry.logical_epoch))
            .collect();
        retain_recent_applied_epochs(&mut applied_epochs);
        Ok(Self {
            identity: checkpoint.identity,
            catalog: payload.catalog,
            input_schema: payload.input_schema,
            output_schema: payload.output_schema,
            view_sql: payload.view_sql,
            plan: payload.plan,
            logical_plan: payload.logical_plan,
            state: payload.state,
            published_output: payload.published_output,
            input_frontiers: checkpoint.input_frontiers,
            input_event_time_frontiers: checkpoint.input_event_time_frontiers,
            applied_epochs,
            logical_epoch: checkpoint.logical_epoch,
        })
    }
}
