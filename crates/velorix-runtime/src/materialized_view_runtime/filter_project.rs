use super::*;
use std::borrow::Cow;
use velorix_core::delta::{encode_kv_ordered, TypedBinaryKey};

pub struct FilterProjectRuntime {
    identity: StandingProgramIdentity,
    catalog: VelorixRelationCatalogV1,
    input_schema: RelationSchema,
    output_schema: RelationSchema,
    view_sql: String,
    plan: SupportedFilterProjectPlan,
    logical_plan: VelorixLogicalViewPlanV1,
    full_output: DeltaBatch,
    published_output: DeltaBatch,
    // Authoritative plain-plan state after opting into delta-only apply. The
    // legacy vectors are empty while this map is active, not parallel caches.
    plain_output: Option<BTreeMap<TypedBinaryKey, DeltaRecord>>,
    input_frontiers: Vec<RelationFrontier>,
    input_event_time_frontiers: Vec<InputEventTimeFrontier>,
    applied_epochs: BTreeMap<String, LogicalEpoch>,
    logical_epoch: LogicalEpoch,
}

impl FilterProjectRuntime {
    pub fn new_with_logical_plan(
        identity: StandingProgramIdentity,
        catalog: VelorixRelationCatalogV1,
        input_schema: RelationSchema,
        output_schema: RelationSchema,
        view_sql: String,
        plan: SupportedFilterProjectPlan,
        logical_plan: VelorixLogicalViewPlanV1,
    ) -> Result<Self, StandingProgramRuntimeError> {
        identity.validate()?;
        validate_builtin_runtime_identity(&identity)?;
        validate_view_sql_hash(&identity, view_sql.as_str())?;
        validate_logical_view_plan(&logical_plan).map_err(|_| {
            StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "logical_filter_project_view_plan",
            }
        })?;
        validate_filter_project_supported_schemas(&catalog, &input_schema, &output_schema, &plan)?;
        let compiled_plan = validate_supported_filter_project_sql(view_sql.as_str(), &catalog)
            .map_err(|_| StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "filter_project_view_plan",
            })?;
        if compiled_plan != plan {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "filter_project_view_plan",
            });
        }
        let compiled_logical_plan = lower_supported_filter_project_sql_to_logical_plan(
            view_sql.as_str(),
            &catalog,
            &output_schema,
        )
        .map_err(|_| StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "logical_filter_project_view_plan",
        })?;
        if compiled_logical_plan != logical_plan {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "logical_filter_project_view_plan",
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
            full_output: DeltaBatch::default(),
            published_output: DeltaBatch::default(),
            plain_output: None,
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
        materialized_generic_delta_to_record_batch(&self.output_schema, &self.current_output())
    }

    fn materialized_page_batch(
        &self,
        page: SnapshotPageRequest,
    ) -> Result<(RecordBatch, Option<String>), StandingProgramRuntimeError> {
        materialized_generic_delta_page_batch(
            &self.output_schema,
            &self.current_output(),
            self.logical_epoch,
            page,
        )
    }

    fn checkpoint_payload(&self) -> Result<String, StandingProgramRuntimeError> {
        let published_output = self.current_output();
        let payload = FilterProjectCheckpointPayload {
            schema_version: CHECKPOINT_PAYLOAD_SCHEMA_VERSION,
            runtime_kind: FILTER_PROJECT_RUNTIME_KIND.to_string(),
            catalog: self.catalog.clone(),
            input_schema: self.input_schema.clone(),
            output_schema: self.output_schema.clone(),
            view_sql: self.view_sql.clone(),
            plan: self.plan.clone(),
            logical_plan: self.logical_plan.clone(),
            input_frontiers: self.input_frontiers.clone(),
            input_event_time_frontiers: self.input_event_time_frontiers.clone(),
            full_output: Some(if self.plain_output.is_some() {
                published_output.as_ref().clone()
            } else {
                self.full_output.clone()
            }),
            published_output: published_output.into_owned(),
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

    fn current_output(&self) -> Cow<'_, DeltaBatch> {
        match &self.plain_output {
            Some(rows) => Cow::Owned(DeltaBatch::from_records(rows.values().cloned())),
            None => Cow::Borrowed(&self.published_output),
        }
    }

    fn prepare_changes(
        &self,
        input_changes: Vec<RelationInputBatch>,
    ) -> Result<
        (
            DeltaBatch,
            Vec<RelationFrontier>,
            Vec<InputEventTimeFrontier>,
        ),
        StandingProgramRuntimeError,
    > {
        let mut combined = DeltaBatch::default();
        let mut frontiers = self.input_frontiers.clone();
        let mut event_frontiers = self.input_event_time_frontiers.clone();
        let value_column_ids = filter_project_input_column_ids(&self.plan);
        for input in input_changes {
            validate_input_matches_schema(&input, &self.input_schema, "filter_project_input")?;
            let delta = if let Some(empty) = published_input_empty_delta(&input, &self.catalog)? {
                empty
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
                        field: "filter_project_input_batch",
                    }
                })?
            };
            let delta =
                filter_delta_batch_for_filter_project_plan(&delta, &self.plan, &self.catalog)?;
            let delta = project_filter_project_delta_batch(&delta, &self.plan, &self.catalog)?;
            combined = combined.combine(&delta);
            advance_input_frontier(&mut frontiers, &input)?;
            advance_input_event_time_frontier(&mut event_frontiers, &input)?;
        }
        Ok((combined, frontiers, event_frontiers))
    }

    fn restore_payload(
        checkpoint: &RuntimeCheckpoint,
    ) -> Result<FilterProjectCheckpointPayload, StandingProgramRuntimeError> {
        let Some(state_payload) = &checkpoint.state_payload else {
            return Err(invalid_checkpoint());
        };
        if state_payload.codec_identity != checkpoint.checkpoint_codec_identity {
            return Err(StandingProgramRuntimeError::CheckpointCodecMismatch {
                expected: checkpoint.checkpoint_codec_identity.clone(),
                actual: state_payload.codec_identity.clone(),
            });
        }
        let payload: FilterProjectCheckpointPayload =
            serde_json::from_str(&state_payload.payload).map_err(|_| invalid_checkpoint())?;
        if payload.schema_version != CHECKPOINT_PAYLOAD_SCHEMA_VERSION
            || payload.runtime_kind != FILTER_PROJECT_RUNTIME_KIND
        {
            return Err(invalid_checkpoint());
        }
        validate_filter_project_supported_schemas(
            &payload.catalog,
            &payload.input_schema,
            &payload.output_schema,
            &payload.plan,
        )?;
        Ok(payload)
    }
}

impl StandingProgramRuntime for FilterProjectRuntime {
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

        let (output_delta, input_frontiers, input_event_time_frontiers) =
            self.prepare_changes(input_changes)?;
        // The helper returns staged state without mutating the committed snapshot.
        let full_output = if self.plain_output.is_some() {
            self.current_output()
        } else {
            Cow::Borrowed(&self.full_output)
        };
        let staged_full_output =
            apply_filter_project_full_output_delta(&full_output, &output_delta, &self.plan)?;
        let key_preserving =
            self.plan.output_key_input_column_id.is_none() && self.plan.top_k.is_none();
        let staged_published_output = if key_preserving {
            // Full output was consolidated and validated by the staging helper.
            staged_full_output.clone()
        } else {
            let next_published_output = filter_project_published_output_from_full_output(
                staged_full_output.clone(),
                &self.plan,
            )?;
            let next_published_output = apply_filter_project_top_k_to_published_output(
                next_published_output,
                self.plan.top_k.as_ref(),
                &self.plan,
            )?;
            strip_filter_project_hidden_order_value(next_published_output, &self.plan)?
        };
        // Validate output before commit
        let visible_delta = if key_preserving {
            // A key-preserving projection is linear: unchanged retained rows do
            // not need to be retracted and reinserted on every epoch.
            DeltaBatch::from_records(
                output_delta
                    .net_rows()
                    .map_err(|_| invalid_runtime_state())?,
            )
        } else {
            self.published_output
                .diff(&staged_published_output)
                .map_err(|_| invalid_runtime_state())?
        };
        let output_batches = vec![ViewOutputBatch {
            view_id: self.identity.view_ids[0].clone(),
            schema_fingerprint: self.output_schema_fingerprint(),
            batches: vec![if key_preserving {
                output::materialized_generic_rows_to_record_batch(
                    &self.output_schema,
                    staged_published_output.records(),
                )?
            } else {
                materialized_generic_delta_to_record_batch(
                    &self.output_schema,
                    &staged_published_output,
                )?
            }],
        }];
        // Commit staged state
        self.plain_output = None;
        self.full_output = staged_full_output;
        self.published_output = staged_published_output;
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
                delta: visible_delta,
            }],
            output_batches,
        })
    }

    fn apply_changes_delta_only(
        &mut self,
        logical_epoch: LogicalEpoch,
        idempotency_key: EpochIdempotencyKey,
        input_changes: Vec<RelationInputBatch>,
    ) -> Result<EpochCommit, StandingProgramRuntimeError> {
        if self.plan.output_key_input_column_id.is_some() || self.plan.top_k.is_some() {
            let mut commit = self.apply_changes(logical_epoch, idempotency_key, input_changes)?;
            commit.output_batches.clear();
            return Ok(commit);
        }
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
                output_batches: Vec::new(),
            });
        }
        if logical_epoch <= self.logical_epoch {
            return Err(StandingProgramRuntimeError::NonMonotonicLogicalEpoch {
                current: self.logical_epoch,
                attempted: logical_epoch,
            });
        }
        let (delta, input_frontiers, input_event_time_frontiers) =
            self.prepare_changes(input_changes)?;
        let rows = delta.net_rows().map_err(|_| invalid_runtime_state())?;
        let initial = if self.plain_output.is_none() {
            // Validate legacy/restored state before promoting it to the indexed representation.
            let full = self
                .full_output
                .net_rows()
                .map_err(|_| invalid_runtime_state())?;
            output::materialized_generic_rows_to_record_batch(&self.output_schema, &full)?;
            Some(
                full.into_iter()
                    .map(|row| {
                        (
                            encode_kv_ordered(row.key.as_json(), row.value.as_json()),
                            row,
                        )
                    })
                    .collect::<BTreeMap<_, _>>(),
            )
        } else {
            None
        };
        let current = self
            .plain_output
            .as_ref()
            .or(initial.as_ref())
            .expect("plain state initialized");
        let mut changes = Vec::with_capacity(rows.len());
        let mut validated_rows = Vec::with_capacity(rows.len());
        for row in &rows {
            let encoded = encode_kv_ordered(row.key.as_json(), row.value.as_json());
            let previous = i128::from(current.contains_key(&encoded));
            let next = previous + i128::from(row.weight);
            if next != 0 && next != 1 {
                return Err(invalid_runtime_state());
            }
            let mut unit = row.clone();
            unit.weight = 1;
            validated_rows.push(unit.clone());
            changes.push((encoded, (next == 1).then_some(unit)));
        }
        // All fallible schema/weight/frontier work finishes before changing state.
        output::materialized_generic_rows_to_record_batch(&self.output_schema, &validated_rows)?;
        let delta = DeltaBatch::from_records(rows);
        if let Some(initial) = initial {
            self.plain_output = Some(initial);
            self.full_output = DeltaBatch::default();
            self.published_output = DeltaBatch::default();
        }
        let current = self.plain_output.as_mut().expect("plain state initialized");
        for (encoded, next) in changes {
            match next {
                Some(row) => {
                    current.insert(encoded, row);
                }
                None => {
                    current.remove(&encoded);
                }
            }
        }
        self.input_frontiers = input_frontiers.clone();
        self.input_event_time_frontiers = input_event_time_frontiers.clone();
        self.applied_epochs.insert(key, logical_epoch);
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
                delta,
            }],
            output_batches: Vec::new(),
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
        let compiled_plan =
            validate_supported_filter_project_sql(payload.view_sql.as_str(), &payload.catalog)
                .map_err(|_| invalid_checkpoint())?;
        if compiled_plan != payload.plan {
            return Err(invalid_checkpoint());
        }
        validate_logical_view_plan(&payload.logical_plan).map_err(|_| invalid_checkpoint())?;
        let compiled_logical_plan = lower_supported_filter_project_sql_to_logical_plan(
            payload.view_sql.as_str(),
            &payload.catalog,
            &payload.output_schema,
        )
        .map_err(|_| invalid_checkpoint())?;
        if compiled_logical_plan != payload.logical_plan {
            return Err(invalid_checkpoint());
        }
        let full_output = payload
            .full_output
            .unwrap_or_else(|| payload.published_output.clone());
        if payload.plan.output_key_input_column_id.is_some() {
            filter_project_published_output_from_full_output(full_output.clone(), &payload.plan)
                .map_err(|_| invalid_checkpoint())?;
        } else {
            validate_published_output(&full_output)?;
        }
        validate_published_output(&payload.published_output)?;
        if payload.plan.output_key_input_column_id.is_none()
            && payload.plan.top_k.is_none()
            && full_output.net_rows().map_err(|_| invalid_checkpoint())?
                != payload
                    .published_output
                    .net_rows()
                    .map_err(|_| invalid_checkpoint())?
        {
            return Err(invalid_checkpoint());
        }
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
            full_output,
            published_output: payload.published_output,
            plain_output: None,
            input_frontiers: checkpoint.input_frontiers,
            input_event_time_frontiers: checkpoint.input_event_time_frontiers,
            applied_epochs,
            logical_epoch: checkpoint.logical_epoch,
        })
    }
}
