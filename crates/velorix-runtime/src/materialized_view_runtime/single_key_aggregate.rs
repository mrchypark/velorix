use super::*;

/// Input binding for a single-key sum/count runtime.
///
/// The input kind is explicit so a missing catalog for a Source binding fails
/// recovery, while a PublishedView binding legitimately has no physical catalog.
/// Do NOT model this as `catalog: Option<_>` because that cannot distinguish a
/// broken Source checkpoint from a valid published-view input.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SingleKeyRuntimeInputV1 {
    /// A registered physical ingest source.
    Source { catalog: VelorixRelationCatalogV1 },
    /// An upstream published view output delivered as signed delta batches.
    PublishedView {
        binding: PublishedRelationBindingV1,
        primary_key_column_id: String,
    },
}

impl SingleKeyRuntimeInputV1 {
    /// The single primary key column id for this input.
    pub fn primary_key_column_id(&self) -> Result<&str, StandingProgramRuntimeError> {
        match self {
            SingleKeyRuntimeInputV1::Source { catalog } => {
                catalog_primary_key_column(catalog).map(|column| column.column_id.as_str())
            }
            SingleKeyRuntimeInputV1::PublishedView {
                primary_key_column_id,
                ..
            } => Ok(primary_key_column_id.as_str()),
        }
    }

    /// The physical catalog for a Source input.
    pub fn catalog(&self) -> Result<&VelorixRelationCatalogV1, StandingProgramRuntimeError> {
        match self {
            SingleKeyRuntimeInputV1::Source { catalog } => Ok(catalog),
            SingleKeyRuntimeInputV1::PublishedView { .. } => {
                Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "single_key_published_view_input_has_no_catalog",
                })
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct SingleKeySumCountRuntime {
    identity: StandingProgramIdentity,
    input: SingleKeyRuntimeInputV1,
    input_schema: RelationSchema,
    output_schema: RelationSchema,
    view_sql: String,
    plan: SupportedViewPlan,
    logical_plan: VelorixLogicalViewPlanV1,
    engine: KeyedAggregateKernel,
    published_output: DeltaBatch,
    filtered_aggregate_state: DeltaBatch,
    input_frontiers: Vec<RelationFrontier>,
    input_event_time_frontiers: Vec<InputEventTimeFrontier>,
    view_input_cursors: Vec<CausalViewCursorV1>,
    applied_epochs: BTreeMap<String, LogicalEpoch>,
}

impl SingleKeySumCountRuntime {
    /// Validate a view delta input against the admitted published binding.
    ///
    /// Rejects deltas from a different producer view, generation, output
    /// stream, or schema before any state mutation. Cursor monotonicity across
    /// epochs is checked by the controller; here we bind the input to the
    /// admitted producer identity.
    fn validate_view_input_matches_binding(
        &self,
        input: &ViewInputDeltaV1,
    ) -> Result<(), StandingProgramRuntimeError> {
        let SingleKeyRuntimeInputV1::PublishedView {
            binding,
            primary_key_column_id,
        } = &self.input
        else {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "single_key_view_input_on_source_runtime",
            });
        };
        let cursor = &input.producer_cursor;
        if cursor.producer_view_id != binding.producer_view_id {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "single_key_view_input_producer_view_id",
            });
        }
        if cursor.producer_generation != binding.producer_view_generation {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "single_key_view_input_producer_generation",
            });
        }
        if cursor.output_stream != binding.output_stream_id {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "single_key_view_input_output_stream",
            });
        }
        if binding.relation.schema_fingerprint != self.input_schema.schema_fingerprint {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "single_key_view_input_schema_fingerprint",
            });
        }
        let _ = primary_key_column_id;
        Ok(())
    }

    pub fn new_with_plan(
        identity: StandingProgramIdentity,
        catalog: VelorixRelationCatalogV1,
        input_schema: RelationSchema,
        output_schema: RelationSchema,
        view_sql: String,
        plan: SupportedViewPlan,
        logical_plan: VelorixLogicalViewPlanV1,
    ) -> Result<Self, StandingProgramRuntimeError> {
        Self::new_with_logical_plan(
            identity,
            catalog,
            input_schema,
            output_schema,
            view_sql,
            plan,
            logical_plan,
        )
    }

    /// Create a runtime bound to a published view output input.
    ///
    /// This path has no physical catalog. The plan is re-validated through the
    /// published single-key sum/count lowerer, and the aggregate value mode is
    /// derived from the persisted input schema instead of a catalog.
    pub fn new_with_logical_plan_for_published_view(
        identity: StandingProgramIdentity,
        binding: PublishedRelationBindingV1,
        input_schema: RelationSchema,
        output_schema: RelationSchema,
        view_sql: String,
        plan: SupportedViewPlan,
        logical_plan: VelorixLogicalViewPlanV1,
    ) -> Result<Self, StandingProgramRuntimeError> {
        identity.validate()?;
        validate_builtin_runtime_identity(&identity)?;
        validate_view_sql_hash(&identity, view_sql.as_str())?;
        validate_logical_view_plan(&logical_plan).map_err(|_| {
            StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "logical_view_plan",
            }
        })?;
        if binding.relation.schema_fingerprint != input_schema.schema_fingerprint {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "published_view_input_schema",
            });
        }
        let primary_key_column_id = plan.group_key_column_id.clone();
        let input = SingleKeyRuntimeInputV1::PublishedView {
            binding,
            primary_key_column_id,
        };
        let value_mode =
            aggregate_value_mode_for_published_schema(&input_schema, &plan.sum_value_column_id)?;
        let track_extrema = plan_tracks_extrema(&plan);
        let filtered_aggregate_state = DeltaBatch::default();
        let published_output = publish_aggregate_state(&filtered_aggregate_state, &plan)?;
        Ok(Self {
            identity,
            input,
            input_schema,
            output_schema,
            view_sql,
            plan,
            logical_plan,
            engine: KeyedAggregateKernel::with_aggregate_value_mode_and_extrema(
                value_mode,
                track_extrema,
            ),
            published_output,
            filtered_aggregate_state,
            input_frontiers: Vec::new(),
            input_event_time_frontiers: Vec::new(),
            view_input_cursors: Vec::new(),
            applied_epochs: BTreeMap::new(),
        })
    }

    pub fn new_with_logical_plan(
        identity: StandingProgramIdentity,
        catalog: VelorixRelationCatalogV1,
        input_schema: RelationSchema,
        output_schema: RelationSchema,
        view_sql: String,
        plan: SupportedViewPlan,
        logical_plan: VelorixLogicalViewPlanV1,
    ) -> Result<Self, StandingProgramRuntimeError> {
        identity.validate()?;
        validate_builtin_runtime_identity(&identity)?;
        validate_view_sql_hash(&identity, view_sql.as_str())?;
        validate_logical_view_plan(&logical_plan).map_err(|_| {
            StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "logical_view_plan",
            }
        })?;
        validate_supported_schemas(&catalog, &input_schema, &output_schema, &plan)?;
        let compiled_plan =
            validate_supported_view_sql(view_sql.as_str(), &catalog).map_err(|_| {
                StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "generic_view_plan",
                }
            })?;
        if compiled_plan != plan {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "generic_view_plan",
            });
        }
        let compiled_logical_plan =
            lower_supported_view_sql_to_logical_plan(view_sql.as_str(), &catalog, &output_schema)
                .map_err(|_| StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "logical_view_plan",
            })?;
        if compiled_logical_plan != logical_plan {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "logical_view_plan",
            });
        }
        validate_plan_matches_catalog(&plan, &catalog)?;
        let value_mode = aggregate_value_mode_for_plan(&catalog, &plan)?;
        let track_extrema = plan_tracks_extrema(&plan);
        let filtered_aggregate_state = DeltaBatch::default();
        let published_output = publish_aggregate_state(&filtered_aggregate_state, &plan)?;
        Ok(Self {
            identity,
            input: SingleKeyRuntimeInputV1::Source { catalog },
            input_schema,
            output_schema,
            view_sql,
            plan,
            logical_plan,
            engine: KeyedAggregateKernel::with_aggregate_value_mode_and_extrema(
                value_mode,
                track_extrema,
            ),
            published_output,
            filtered_aggregate_state,
            input_frontiers: Vec::new(),
            input_event_time_frontiers: Vec::new(),
            view_input_cursors: Vec::new(),
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
            Some(&supported_view_plan_aggregate_outputs(&self.plan)),
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
            Some(&supported_view_plan_aggregate_outputs(&self.plan)),
        )
    }

    fn checkpoint_payload(&self) -> Result<String, StandingProgramRuntimeError> {
        let payload = GenericCheckpointPayload {
            schema_version: CHECKPOINT_PAYLOAD_SCHEMA_VERSION,
            input: self.input.clone(),
            input_schema: self.input_schema.clone(),
            output_schema: self.output_schema.clone(),
            view_sql: self.view_sql.clone(),
            plan: self.plan.clone(),
            logical_plan: self.logical_plan.clone(),
            input_frontiers: self.input_frontiers.clone(),
            input_event_time_frontiers: self.input_event_time_frontiers.clone(),
            engine: self.engine.checkpoint_state().to_payload(),
            published_output: self.published_output.clone(),
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
    ) -> Result<GenericCheckpointPayload, StandingProgramRuntimeError> {
        let Some(state_payload) = &checkpoint.state_payload else {
            return Err(invalid_checkpoint());
        };
        if state_payload.codec_identity != checkpoint.checkpoint_codec_identity {
            return Err(StandingProgramRuntimeError::CheckpointCodecMismatch {
                expected: checkpoint.checkpoint_codec_identity.clone(),
                actual: state_payload.codec_identity.clone(),
            });
        }
        let payload: GenericCheckpointPayload =
            serde_json::from_str(&state_payload.payload).map_err(|_| invalid_checkpoint())?;
        if payload.schema_version != CHECKPOINT_PAYLOAD_SCHEMA_VERSION {
            return Err(invalid_checkpoint());
        }
        validate_supported_schemas(
            payload.input.catalog()?,
            &payload.input_schema,
            &payload.output_schema,
            &payload.plan,
        )?;
        Ok(payload)
    }
}

impl StandingProgramRuntime for SingleKeySumCountRuntime {
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
        self.engine.logical_epoch()
    }

    fn apply_changes(
        &mut self,
        logical_epoch: LogicalEpoch,
        idempotency_key: EpochIdempotencyKey,
        input_changes: Vec<StandingInputChangeV1>,
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
        if single_key_plan_uses_runtime_aggregate_state(&self.plan) {
            let mut combined = DeltaBatch::default();
            let mut input_frontiers = self.input_frontiers.clone();
            let mut input_event_time_frontiers = self.input_event_time_frontiers.clone();
            for change in input_changes {
                match change {
                    StandingInputChangeV1::Source(input) => {
                        validate_input_matches_schema(
                            &input,
                            &self.input_schema,
                            "generic_input_relation",
                        )?;
                        let catalog = self.input.catalog()?;
                        let delta = aggregate_group_input_delta_batch(catalog, &self.plan, &input)?;
                        let delta = filter_delta_batch_for_plan(&delta, &self.plan, catalog)?;
                        let delta =
                            rekey_delta_batch_for_aggregate_group(&delta, catalog, &self.plan)?;
                        combined = combined.combine(&delta);
                        advance_input_frontier(&mut input_frontiers, &input)?;
                        advance_input_event_time_frontier(&mut input_event_time_frontiers, &input)?;
                    }
                    StandingInputChangeV1::View(input) => {
                        input.validate()?;
                        self.validate_view_input_matches_binding(&input)?;
                        let primary_key = self.input.primary_key_column_id()?;
                        let delta = rekey_delta_batch_for_aggregate_group_with_primary_key(
                            &input.delta,
                            primary_key,
                            &self.plan,
                            &self.input_schema,
                        )?;
                        combined = combined.combine(&delta);
                        self.view_input_cursors.push(input.producer_cursor);
                    }
                }
            }
            let (next_state, _) = apply_filtered_single_key_aggregate_delta(
                &self.filtered_aggregate_state,
                &combined,
                &self.plan,
                self.input.catalog()?,
            )?;
            let aggregate_outputs = supported_view_plan_aggregate_outputs(&self.plan);
            let published_state = publish_aggregate_state(&next_state, &self.plan)?;
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
        let catalog = self.input.catalog()?;
        let mut executor = LogicalPlanExecutor::SingleKeyAggregate {
            catalog,
            input_schema: &self.input_schema,
            plan: &self.plan,
            engine: &mut self.engine,
        };
        let executor_commit = executor.apply_epoch(
            logical_epoch,
            &self.input_frontiers,
            &self.input_event_time_frontiers,
            source_input_batches(input_changes)?,
        )?;
        let output_delta = filter_output_delta_for_having(
            &executor_commit.output_delta,
            self.plan.having.as_ref(),
            self.plan.having_expr.as_ref(),
            &self.output_schema,
            Some(&supported_view_plan_aggregate_outputs(&self.plan)),
        )?;
        let output_delta = if self.plan.top_k.is_some() {
            let previous_output = self.published_output.clone();
            let aggregate_outputs = supported_view_plan_aggregate_outputs(&self.plan);
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
            input_coverage: None,
            causal_cut: None,
        })
    }

    fn restore(checkpoint: RuntimeCheckpoint) -> Result<Self, StandingProgramRuntimeError> {
        checkpoint.validate_identity(&checkpoint.identity)?;
        let payload = Self::restore_payload(&checkpoint)?;
        validate_checkpoint_frontiers(&checkpoint, &payload)?;
        let engine_checkpoint = payload.engine.into_checkpoint();
        if engine_checkpoint.logical_epoch() != checkpoint.logical_epoch {
            return Err(invalid_checkpoint());
        }
        if payload.input_frontiers != checkpoint.input_frontiers {
            return Err(invalid_checkpoint());
        }
        if payload.input_event_time_frontiers != checkpoint.input_event_time_frontiers {
            return Err(invalid_checkpoint());
        }
        validate_input_event_time_frontiers_for_catalogs(
            &checkpoint,
            std::slice::from_ref(payload.input.catalog()?),
        )?;
        let view_sql = payload.view_sql;
        validate_view_sql_hash(&checkpoint.identity, view_sql.as_str())?;
        let plan = payload.plan;
        let compiled_catalog = payload.input.catalog()?;
        let compiled = validate_supported_view_sql(view_sql.as_str(), compiled_catalog)
            .map_err(|_| invalid_checkpoint())?;
        if compiled != plan {
            return Err(invalid_checkpoint());
        }
        validate_plan_matches_catalog(&plan, compiled_catalog)?;
        let value_mode = aggregate_value_mode_for_plan(compiled_catalog, &plan)?;
        let track_extrema = plan_tracks_extrema(&plan);
        let logical_plan = payload.logical_plan;
        validate_logical_view_plan(&logical_plan).map_err(|_| invalid_checkpoint())?;
        let compiled_logical_plan = lower_supported_view_sql_to_logical_plan(
            view_sql.as_str(),
            compiled_catalog,
            &payload.output_schema,
        )
        .map_err(|_| invalid_checkpoint())?;
        if compiled_logical_plan != logical_plan {
            return Err(invalid_checkpoint());
        }
        let engine = KeyedAggregateKernel::from_checkpoint_with_aggregate_value_mode_and_extrema(
            engine_checkpoint,
            value_mode,
            track_extrema,
        )
        .map_err(|_| invalid_checkpoint())?;
        let published_output = payload.published_output;
        let filtered_aggregate_state = if single_key_plan_uses_runtime_aggregate_state(&plan)
            && !supported_view_plan_is_singleton(&plan)
            && payload.filtered_aggregate_state.records().is_empty()
        {
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
            input: payload.input,
            input_schema: payload.input_schema,
            output_schema: payload.output_schema,
            view_sql,
            plan,
            logical_plan,
            engine,
            published_output,
            filtered_aggregate_state,
            input_frontiers: checkpoint.input_frontiers,
            input_event_time_frontiers: checkpoint.input_event_time_frontiers,
            view_input_cursors: Vec::new(),
            applied_epochs,
        })
    }
}
