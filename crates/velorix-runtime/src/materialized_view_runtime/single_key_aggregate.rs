use super::*;

#[derive(Clone, Debug)]
pub struct SingleKeySumCountRuntime {
    identity: StandingProgramIdentity,
    catalog: VelorixRelationCatalogV1,
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
    applied_epochs: BTreeMap<String, LogicalEpoch>,
    #[cfg(test)]
    fail_next_output_encoding: bool,
}

impl SingleKeySumCountRuntime {
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
            catalog,
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
            applied_epochs: BTreeMap::new(),
            #[cfg(test)]
            fail_next_output_encoding: false,
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

    fn encode_output_batch(
        &mut self,
        output: &DeltaBatch,
        aggregate_outputs: &[SupportedAggregateOutput],
    ) -> Result<RecordBatch, StandingProgramRuntimeError> {
        #[cfg(test)]
        if std::mem::replace(&mut self.fail_next_output_encoding, false) {
            return Err(invalid_runtime_state());
        }
        materialized_delta_to_record_batch(&self.output_schema, output, Some(aggregate_outputs))
    }

    #[cfg(test)]
    #[allow(dead_code)] // exercised by colocated runtime fault-injection harnesses.
    fn arm_fail_next_output_encoding(&mut self) {
        self.fail_next_output_encoding = true;
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
            runtime_kind: SINGLE_KEY_SUM_COUNT_RUNTIME_KIND.to_string(),
            catalog: self.catalog.clone(),
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
            &payload.catalog,
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
        if single_key_plan_uses_runtime_aggregate_state(&self.plan) {
            let mut combined = DeltaBatch::default();
            let mut input_frontiers = self.input_frontiers.clone();
            let mut input_event_time_frontiers = self.input_event_time_frontiers.clone();
            for input in input_changes {
                validate_input_matches_schema(
                    &input,
                    &self.input_schema,
                    "generic_input_relation",
                )?;
                let delta = aggregate_group_input_delta_batch(&self.catalog, &self.plan, &input)?;
                let delta = filter_delta_batch_for_plan(&delta, &self.plan, &self.catalog)?;
                let delta =
                    rekey_delta_batch_for_aggregate_group(&delta, &self.catalog, &self.plan)?;
                combined = combined.combine(&delta);
                advance_input_frontier(&mut input_frontiers, &input)?;
                advance_input_event_time_frontier(&mut input_event_time_frontiers, &input)?;
            }
            let (next_state, _) = apply_filtered_single_key_aggregate_delta(
                &self.filtered_aggregate_state,
                &combined,
                &self.plan,
                &self.catalog,
            )?;
            let aggregate_outputs = supported_view_plan_aggregate_outputs(&self.plan);
            let published_state = publish_aggregate_state(&next_state, &self.plan)?;
            let published_state =
                project_aggregate_delta_outputs(published_state, &aggregate_outputs)?;
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
            let prepared_epoch = self
                .engine
                .prepare_epoch(logical_epoch, &DeltaBatch::default())
                .map_err(|_| invalid_runtime_state())?;
            let output_batches = vec![ViewOutputBatch {
                view_id: self.identity.view_ids[0].clone(),
                schema_fingerprint: self.output_schema_fingerprint(),
                batches: vec![self.encode_output_batch(&visible_output, &aggregate_outputs)?],
            }];
            self.engine
                .commit_prepared_epoch(prepared_epoch)
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
        if logical_epoch <= self.engine.logical_epoch() {
            return Err(StandingProgramRuntimeError::NonMonotonicLogicalEpoch {
                current: self.engine.logical_epoch(),
                attempted: logical_epoch,
            });
        }
        let mut combined = DeltaBatch::default();
        let mut input_frontiers = self.input_frontiers.clone();
        let mut input_event_time_frontiers = self.input_event_time_frontiers.clone();
        for input in &input_changes {
            validate_input_matches_schema(input, &self.input_schema, "generic_input_relation")?;
            advance_input_frontier(&mut input_frontiers, input)?;
            advance_input_event_time_frontier(&mut input_event_time_frontiers, input)?;
        }
        for input in input_changes {
            let delta = single_key_input_delta_batch(&self.catalog, &self.plan, &input)?;
            let delta = filter_delta_batch_for_plan(&delta, &self.plan, &self.catalog)?;
            combined = combined.combine(&delta);
        }
        let prepared_epoch = self
            .engine
            .prepare_epoch(logical_epoch, &combined)
            .map_err(|_| invalid_runtime_state())?;
        let aggregate_outputs = supported_view_plan_aggregate_outputs(&self.plan);
        let output_delta = filter_output_delta_for_having(
            prepared_epoch.output_changes(),
            self.plan.having.as_ref(),
            self.plan.having_expr.as_ref(),
            &self.output_schema,
            Some(&aggregate_outputs),
        )?;
        let output_delta = project_aggregate_delta_outputs(output_delta, &aggregate_outputs)?;
        if self.plan.top_k.is_some() {
            let full_state = apply_published_output_delta(
                &self.engine.materialized_state(),
                prepared_epoch.output_changes(),
            )?;
            let full_output = filter_output_delta_for_having(
                &full_state,
                self.plan.having.as_ref(),
                self.plan.having_expr.as_ref(),
                &self.output_schema,
                Some(&aggregate_outputs),
            )?;
            let full_output = project_aggregate_delta_outputs(full_output, &aggregate_outputs)?;
            let staged_output = apply_top_k_to_published_output(
                full_output,
                self.plan.top_k.as_ref(),
                &aggregate_outputs,
            )?;
            let output_delta = self
                .published_output
                .diff(&staged_output)
                .map_err(|_| invalid_runtime_state())?;
            let output_batches = vec![ViewOutputBatch {
                view_id: self.identity.view_ids[0].clone(),
                schema_fingerprint: self.output_schema_fingerprint(),
                batches: vec![self.encode_output_batch(&staged_output, &aggregate_outputs)?],
            }];
            self.engine
                .commit_prepared_epoch(prepared_epoch)
                .map_err(|_| invalid_runtime_state())?;
            self.published_output = staged_output;
            self.input_frontiers = input_frontiers.clone();
            self.input_event_time_frontiers = input_event_time_frontiers.clone();
            self.applied_epochs
                .insert(idempotency_key_text, logical_epoch);
            retain_recent_applied_epochs(&mut self.applied_epochs);
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
        } else {
            let staged_output =
                apply_published_output_delta(&self.published_output, &output_delta)?;
            let output_batches = vec![ViewOutputBatch {
                view_id: self.identity.view_ids[0].clone(),
                schema_fingerprint: self.output_schema_fingerprint(),
                batches: vec![self.encode_output_batch(&staged_output, &aggregate_outputs)?],
            }];
            self.engine
                .commit_prepared_epoch(prepared_epoch)
                .map_err(|_| invalid_runtime_state())?;
            self.published_output = staged_output;
            self.input_frontiers = input_frontiers.clone();
            self.input_event_time_frontiers = input_event_time_frontiers.clone();
            self.applied_epochs
                .insert(idempotency_key_text, logical_epoch);
            retain_recent_applied_epochs(&mut self.applied_epochs);
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
            std::slice::from_ref(&payload.catalog),
        )?;
        let view_sql = payload.view_sql;
        validate_view_sql_hash(&checkpoint.identity, view_sql.as_str())?;
        let plan = payload.plan;
        let compiled = validate_supported_view_sql(view_sql.as_str(), &payload.catalog)
            .map_err(|_| invalid_checkpoint())?;
        if compiled != plan {
            return Err(invalid_checkpoint());
        }
        validate_plan_matches_catalog(&plan, &payload.catalog)?;
        let value_mode = aggregate_value_mode_for_plan(&payload.catalog, &plan)?;
        let track_extrema = plan_tracks_extrema(&plan);
        let logical_plan = payload.logical_plan;
        validate_logical_view_plan(&logical_plan).map_err(|_| invalid_checkpoint())?;
        let compiled_logical_plan = lower_supported_view_sql_to_logical_plan(
            view_sql.as_str(),
            &payload.catalog,
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
            catalog: payload.catalog,
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
            applied_epochs,
            #[cfg(test)]
            fail_next_output_encoding: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    use velorix_core::{
        relation::{
            ArrowPhysicalTypeV1, DataFusionRegistrationModeV1, DataFusionRegistrationV1,
            IncrementalAdapterBindingV1, IncrementalRelationBindingV1, RelationColumnV1,
            RelationOperationV1, RelationSemanticRoleV1, SchemaFingerprintV1, VelorixLogicalTypeV1,
            VelorixRelationCatalogV1, VelorixRelationSchemaV1, VelorixRelationSourceV1,
            CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID, RELATION_SCHEMA_VERSION_V1,
        },
        standing_program::{BuiltinRuntimeIdentity, NativeCodePolicy},
        view_contract::ColumnSchema,
    };

    use super::*;

    #[test]
    fn output_encoding_failure_rolls_back_normal_single_key_epoch() {
        assert_output_encoding_failure_rolls_back(
            "select user_id, sum(amount) as sum, count(*) as count from purchases group by user_id",
            aggregate_output_schema("sum", "count"),
            "normal-output-failure",
        );
    }

    #[test]
    fn output_encoding_failure_rolls_back_runtime_aggregate_state_epoch() {
        assert_output_encoding_failure_rolls_back(
            "select user_id, sum(amount + 1) as total_amount, count(*) as event_count from purchases group by user_id",
            aggregate_output_schema("total_amount", "event_count"),
            "runtime-state-output-failure",
        );
    }

    fn assert_output_encoding_failure_rolls_back(
        sql: &str,
        output_schema: RelationSchema,
        key_prefix: &str,
    ) {
        let catalog = purchases_catalog();
        let input_schema = catalog_input_relation_schema(&catalog).unwrap();
        let plan = validate_supported_view_sql(sql, &catalog).unwrap();
        let logical_plan =
            lower_supported_view_sql_to_logical_plan(sql, &catalog, &output_schema).unwrap();
        let mut runtime = SingleKeySumCountRuntime::new_with_logical_plan(
            test_identity(sql),
            catalog.clone(),
            input_schema,
            output_schema,
            sql.to_string(),
            plan,
            logical_plan,
        )
        .unwrap();

        runtime
            .apply_changes(
                1,
                EpochIdempotencyKey::new(format!("{key_prefix}-1")).unwrap(),
                vec![purchases_input(&catalog, 0, 1, "alice", 10)],
            )
            .unwrap();
        let checkpoint_before = runtime.checkpoint().unwrap();
        let output_before = runtime.published_output.clone();
        let frontiers_before = runtime.input_frontiers.clone();
        let event_time_frontiers_before = runtime.input_event_time_frontiers.clone();
        let idempotency_before = runtime.applied_epochs.clone();

        runtime.arm_fail_next_output_encoding();
        let failed_key = EpochIdempotencyKey::new(format!("{key_prefix}-2")).unwrap();
        let failed_input = purchases_input(&catalog, 1, 2, "bob", 5);
        let error = runtime
            .apply_changes(2, failed_key.clone(), vec![failed_input.clone()])
            .unwrap_err();
        assert!(
            error.to_string().contains("generic_runtime_state"),
            "forced output encoding failure must fail closed: {error}"
        );
        assert_eq!(runtime.checkpoint().unwrap(), checkpoint_before);
        assert_eq!(runtime.published_output, output_before);
        assert_eq!(runtime.input_frontiers, frontiers_before);
        assert_eq!(
            runtime.input_event_time_frontiers,
            event_time_frontiers_before
        );
        assert_eq!(runtime.applied_epochs, idempotency_before);

        let retry = runtime
            .apply_changes(2, failed_key.clone(), vec![failed_input])
            .unwrap();
        assert_eq!(retry.output_deltas.len(), 1);
        let checkpoint_after_retry = runtime.checkpoint().unwrap();
        let duplicate = runtime.apply_changes(2, failed_key, Vec::new()).unwrap();
        assert!(duplicate.output_deltas.is_empty());
        assert_eq!(runtime.checkpoint().unwrap(), checkpoint_after_retry);
    }

    fn purchases_catalog() -> VelorixRelationCatalogV1 {
        let relation_schema = VelorixRelationSchemaV1 {
            relation_id: "purchases".to_string(),
            relation_name: "purchases".to_string(),
            relation_version: "test.v1".to_string(),
            columns: vec![
                RelationColumnV1 {
                    column_id: "user_id".to_string(),
                    name: "user_id".to_string(),
                    logical_type: VelorixLogicalTypeV1::Utf8,
                    physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                    nullable: false,
                    ordinal: 0,
                    semantic_role: RelationSemanticRoleV1::Metadata,
                },
                RelationColumnV1 {
                    column_id: "amount".to_string(),
                    name: "amount".to_string(),
                    logical_type: VelorixLogicalTypeV1::Int64,
                    physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                    nullable: false,
                    ordinal: 1,
                    semantic_role: RelationSemanticRoleV1::Metadata,
                },
                RelationColumnV1 {
                    column_id: "delta".to_string(),
                    name: "delta".to_string(),
                    logical_type: VelorixLogicalTypeV1::Int64,
                    physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                    nullable: false,
                    ordinal: 2,
                    semantic_role: RelationSemanticRoleV1::Weight,
                },
            ],
            primary_key_column_ids: vec!["user_id".to_string()],
            weight_column_id: "delta".to_string(),
            allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
            event_time_column_id: None,
        };
        let schema_fingerprint =
            SchemaFingerprintV1::for_relation_schema(&relation_schema).unwrap();
        VelorixRelationCatalogV1 {
            relation_source: VelorixRelationSourceV1::SourceRelation,
            schema_version: RELATION_SCHEMA_VERSION_V1,
            relation_schema,
            schema_fingerprint: schema_fingerprint.clone(),
            datafusion_registration: DataFusionRegistrationV1 {
                name: "purchases".to_string(),
                mode: DataFusionRegistrationModeV1::Table,
            },
            incremental_relation: IncrementalRelationBindingV1 {
                relation_id: "purchases".to_string(),
                schema_fingerprint,
            },
            incremental_adapter: IncrementalAdapterBindingV1 {
                adapter_id: CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string(),
            },
        }
    }

    fn aggregate_output_schema(sum_name: &str, count_name: &str) -> RelationSchema {
        RelationSchema {
            relation_id: "purchases_by_user".to_string(),
            relation_name: "purchases_by_user".to_string(),
            relation_version: "test.v1".to_string(),
            schema_fingerprint: "test-output-schema-v1".to_string(),
            columns: vec![
                ColumnSchema {
                    name: "user_id".to_string(),
                    data_type: SqlDataType::Utf8,
                    nullable: false,
                },
                ColumnSchema {
                    name: sum_name.to_string(),
                    data_type: SqlDataType::Int64,
                    nullable: false,
                },
                ColumnSchema {
                    name: count_name.to_string(),
                    data_type: SqlDataType::Int64,
                    nullable: false,
                },
            ],
            primary_key: vec!["user_id".to_string()],
        }
    }

    fn purchases_input(
        catalog: &VelorixRelationCatalogV1,
        start_offset_inclusive: u64,
        end_offset_exclusive: u64,
        user_id: &str,
        amount: i64,
    ) -> RelationInputBatch {
        RelationInputBatch {
            encoding: RelationInputEncodingV1::SourceRelationV1,
            relation_id: catalog.relation_schema.relation_id.clone(),
            relation_version: catalog.relation_schema.relation_version.clone(),
            stream_id: "test-stream".to_string(),
            partition_id: 0,
            schema_fingerprint: catalog.schema_fingerprint.to_string(),
            start_offset_inclusive,
            end_offset_exclusive,
            event_time_watermark: None,
            batches: vec![RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("user_id", DataType::Utf8, false),
                    Field::new("amount", DataType::Int64, false),
                    Field::new("delta", DataType::Int64, false),
                ])),
                vec![
                    Arc::new(StringArray::from(vec![user_id])) as _,
                    Arc::new(Int64Array::from(vec![amount])) as _,
                    Arc::new(Int64Array::from(vec![1])) as _,
                ],
            )
            .unwrap()],
        }
    }

    fn test_identity(sql: &str) -> StandingProgramIdentity {
        StandingProgramIdentity {
            tenant_id: "tenant-a".to_string(),
            program_id: "program-purchases".to_string(),
            view_ids: vec!["purchases_by_user".to_string()],
            sql_hash: stable_bytes_hash(sql.as_bytes()),
            input_catalog_hash: format!("sha256:{}", "1".repeat(64)),
            output_schema_hash: format!("sha256:{}", "2".repeat(64)),
            planner_identity: "velorix-logical-view-planner@1".to_string(),
            builtin_runtime_identities: vec![BuiltinRuntimeIdentity {
                name: CRATE_NAME.to_string(),
                version: "0.1.0".to_string(),
            }],
            runtime_capabilities: vec!["materialized-view-runtime-v1".to_string()],
            runtime_compatibility: "velorix-materialized-view-runtime-v1".to_string(),
            checkpoint_codec_identity: "velorix-standing-program-checkpoint-v1".to_string(),
            native_code_policy: NativeCodePolicy::DisabledNoExternalDependencies,
        }
    }
}
