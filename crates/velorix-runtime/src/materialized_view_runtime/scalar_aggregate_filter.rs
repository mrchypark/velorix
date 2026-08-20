//! Scalar aggregate filter runtime (Phase 7.2).
//!
//! Materializes `SELECT ... FROM outer WHERE outer_col <op>
//! (SELECT <agg>(col) FROM inner)` for an uncorrelated scalar aggregate
//! subquery. State: an outer row bag keyed by the outer primary key, the
//! inner aggregate multiset, and the published output. Every epoch applies
//! both inputs atomically, recomputes the scalar, and re-evaluates the
//! outer bag against it; when the scalar changes the FULL outer bag is
//! re-evaluated and diffed (same exactness class as the window full-output
//! diff). Resource contracts fail the epoch closed.

use super::*;

/// Typed value comparison: i64, f64, then canonical JSON string fallback.
/// Used for MIN/MAX extrema and WHERE clause comparison.
fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    if let (Some(a_i64), Some(b_i64)) = (a.as_i64(), b.as_i64()) {
        return a_i64.cmp(&b_i64);
    }
    if let (Some(a_f64), Some(b_f64)) = (a.as_f64(), b.as_f64()) {
        return a_f64
            .partial_cmp(&b_f64)
            .unwrap_or(std::cmp::Ordering::Equal);
    }
    canonical_json(a).cmp(&canonical_json(b))
}

pub struct ScalarAggregateFilterRuntime {
    identity: StandingProgramIdentity,
    outer_catalog: VelorixRelationCatalogV1,
    scalar_catalog: VelorixRelationCatalogV1,
    outer_input_schema: RelationSchema,
    scalar_input_schema: RelationSchema,
    output_schema: RelationSchema,
    view_sql: String,
    plan: SupportedScalarAggregateFilterPlanV1,
    logical_plan: VelorixLogicalViewPlanV1,
    outer_rows: BTreeMap<String, ScalarOuterRow>,
    scalar_state: ScalarAggregateState,
    published_output: DeltaBatch,
    input_frontiers: Vec<RelationFrontier>,
    input_event_time_frontiers: Vec<InputEventTimeFrontier>,
    applied_epochs: BTreeMap<String, LogicalEpoch>,
    logical_epoch: LogicalEpoch,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ScalarOuterRow {
    key: Value,
    values: BTreeMap<String, Value>,
    weight: i64,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ScalarAggregateState {
    /// Per-value multiplicity for MIN/MAX/SUM; also serves COUNT distinct
    /// tracking when needed. Keyed by canonical JSON.
    values: BTreeMap<String, ScalarAggregateEntry>,
    /// COUNT / SUM / AVG accumulators.
    count: i64,
    sum: i64,
    avg_sums: i64,
    avg_counts: i64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ScalarAggregateEntry {
    value: Value,
    weight: i64,
}

impl ScalarAggregateFilterRuntime {
    pub fn new_with_logical_plan(
        identity: StandingProgramIdentity,
        catalogs: Vec<VelorixRelationCatalogV1>,
        input_schemas: Vec<RelationSchema>,
        output_schema: RelationSchema,
        view_sql: String,
        plan: SupportedScalarAggregateFilterPlanV1,
        logical_plan: VelorixLogicalViewPlanV1,
    ) -> Result<Self, StandingProgramRuntimeError> {
        identity.validate()?;
        validate_builtin_runtime_identity(&identity)?;
        validate_view_sql_hash(&identity, view_sql.as_str())?;
        validate_logical_view_plan(&logical_plan).map_err(|_| {
            StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "logical_scalar_aggregate_filter_plan",
            }
        })?;
        validate_scalar_aggregate_filter_contract(
            &catalogs,
            &input_schemas,
            &output_schema,
            &plan,
        )?;
        let compiled = validate_supported_scalar_aggregate_filter_sql(view_sql.as_str(), &catalogs)
            .map_err(|_| StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "scalar_aggregate_filter_plan",
            })?;
        if compiled != plan {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "scalar_aggregate_filter_plan",
            });
        }
        let (outer_catalog, scalar_catalog) = match catalogs.as_slice() {
            [outer, scalar] => (outer.clone(), scalar.clone()),
            _ => {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "scalar_aggregate_filter_catalogs",
                })
            }
        };
        let (outer_input_schema, scalar_input_schema) = match input_schemas.as_slice() {
            [outer, scalar] => (outer.clone(), scalar.clone()),
            _ => {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "scalar_aggregate_filter_input_schemas",
                })
            }
        };
        Ok(Self {
            identity,
            outer_catalog,
            scalar_catalog,
            outer_input_schema,
            scalar_input_schema,
            output_schema,
            view_sql,
            plan,
            logical_plan,
            outer_rows: BTreeMap::new(),
            scalar_state: ScalarAggregateState::default(),
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

    /// Current scalar value of the inner aggregate (NULL when the inner bag
    /// is empty for MIN/MAX/SUM/AVG, 0 for COUNT).
    fn current_scalar(&self) -> Result<Option<Value>, StandingProgramRuntimeError> {
        let scalar = match self.plan.scalar_aggregate.function {
            LogicalPlanAggregateFunctionV1::Count => {
                Value::Number(JsonNumber::from(self.scalar_state.count))
            }
            LogicalPlanAggregateFunctionV1::Sum => {
                if self.scalar_state.count == 0 {
                    return Ok(None);
                }
                Value::Number(JsonNumber::from(self.scalar_state.sum))
            }
            LogicalPlanAggregateFunctionV1::Avg => {
                if self.scalar_state.avg_counts == 0 {
                    return Ok(None);
                }
                Value::from(self.scalar_state.avg_sums as f64 / self.scalar_state.avg_counts as f64)
            }
            LogicalPlanAggregateFunctionV1::Min => self
                .scalar_state
                .values
                .values()
                .filter(|e| e.weight > 0)
                .min_by(|a, b| compare_values(&a.value, &b.value))
                .map(|entry| entry.value.clone())
                .unwrap_or(Value::Null),
            LogicalPlanAggregateFunctionV1::Max => self
                .scalar_state
                .values
                .values()
                .filter(|e| e.weight > 0)
                .max_by(|a, b| compare_values(&a.value, &b.value))
                .map(|entry| entry.value.clone())
                .unwrap_or(Value::Null),
            LogicalPlanAggregateFunctionV1::CountDistinct => {
                Value::Number(JsonNumber::from(self.scalar_state.values.len() as i64))
            }
            LogicalPlanAggregateFunctionV1::PercentileDisc { .. }
            | LogicalPlanAggregateFunctionV1::PercentileCont { .. } => {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "scalar_aggregate_filter_percentile_not_supported",
                })
            }
        };
        Ok(Some(scalar))
    }

    fn evaluate_comparison(&self, outer_value: &Value, scalar: &Value) -> bool {
        if outer_value.is_null() || scalar.is_null() {
            // UNKNOWN in SQL; WHERE removes the row.
            return false;
        }
        let ordering = compare_values(outer_value, scalar);
        match self.plan.comparison_op {
            ScalarSubqueryComparisonOp::Eq => ordering == std::cmp::Ordering::Equal,
            ScalarSubqueryComparisonOp::NotEq => ordering != std::cmp::Ordering::Equal,
            ScalarSubqueryComparisonOp::Gt => ordering == std::cmp::Ordering::Greater,
            ScalarSubqueryComparisonOp::GtEq => ordering != std::cmp::Ordering::Less,
            ScalarSubqueryComparisonOp::Lt => ordering == std::cmp::Ordering::Less,
            ScalarSubqueryComparisonOp::LtEq => ordering != std::cmp::Ordering::Greater,
        }
    }
}

impl StandingProgramRuntime for ScalarAggregateFilterRuntime {
    fn program_identity(&self) -> &StandingProgramIdentity {
        &self.identity
    }

    fn input_schemas(&self) -> Vec<RelationSchema> {
        vec![
            self.outer_input_schema.clone(),
            self.scalar_input_schema.clone(),
        ]
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

        let mut next_frontiers = self.input_frontiers.clone();
        let mut next_event_time_frontiers = self.input_event_time_frontiers.clone();
        // Phase 7.2 contract: both inputs are applied atomically before the
        // end-of-epoch scalar and output are computed.
        // Save state for rollback on resource contract failure.
        let outer_rows_before = self.outer_rows.clone();
        let scalar_state_before = self.scalar_state.clone();
        let scalar_before = self.current_scalar()?;
        for input in &input_changes {
            if input.relation_id == self.plan.outer_input_relation_id {
                self.apply_outer_input(input)?;
            } else if input.relation_id == self.plan.scalar_input_relation_id {
                self.apply_scalar_input(input)?;
            } else {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "scalar_aggregate_filter_input_relation",
                });
            }
            advance_input_frontier(&mut next_frontiers, input)?;
            advance_input_event_time_frontier(&mut next_event_time_frontiers, input)?;
        }
        if u64::try_from(self.outer_rows.len()).unwrap_or(u64::MAX)
            > self.plan.resource_contract.max_outer_rows
        {
            // Rollback state mutation on resource contract failure.
            self.outer_rows = outer_rows_before;
            self.scalar_state = scalar_state_before;
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "scalar_aggregate_filter_resource_contract",
            });
        }
        let scalar_after = self.current_scalar()?;
        let scalar_changed = scalar_before != scalar_after;
        let next_output = if scalar_changed {
            self.recompute_all_outputs()?
        } else {
            self.apply_outer_deltas_only()?
        };
        let output_delta = self
            .published_output
            .inverse()
            .map_err(|_| invalid_runtime_state())?
            .combine(&next_output);
        if u64::try_from(
            output_delta
                .net_rows()
                .map_err(|_| invalid_runtime_state())?
                .len(),
        )
        .unwrap_or(u64::MAX)
            > self.plan.resource_contract.max_output_delta_rows
        {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "scalar_aggregate_filter_resource_contract",
            });
        }
        self.published_output = next_output;
        self.input_frontiers = next_frontiers.clone();
        self.input_event_time_frontiers = next_event_time_frontiers.clone();
        self.applied_epochs
            .insert(idempotency_key_text, logical_epoch);
        retain_recent_applied_epochs(&mut self.applied_epochs);
        self.logical_epoch = logical_epoch;

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
            logical_epoch: self.logical_epoch,
            schema_fingerprint: self.output_schema_fingerprint(),
            batches: vec![batch],
            next_page_token,
        })
    }

    fn checkpoint(&self) -> Result<RuntimeCheckpoint, StandingProgramRuntimeError> {
        let payload = ScalarAggregateFilterCheckpointPayload {
            schema_version: CHECKPOINT_PAYLOAD_SCHEMA_VERSION,
            runtime_kind: SCALAR_AGGREGATE_FILTER_RUNTIME_KIND.to_string(),
            outer_catalog: self.outer_catalog.clone(),
            scalar_catalog: self.scalar_catalog.clone(),
            outer_input_schema: self.outer_input_schema.clone(),
            scalar_input_schema: self.scalar_input_schema.clone(),
            output_schema: self.output_schema.clone(),
            view_sql: self.view_sql.clone(),
            plan: self.plan.clone(),
            logical_plan: self.logical_plan.clone(),
            input_frontiers: self.input_frontiers.clone(),
            input_event_time_frontiers: self.input_event_time_frontiers.clone(),
            outer_rows: self.outer_rows.clone(),
            scalar_state: self.scalar_state.clone(),
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
        let payload = serde_json::to_string(&payload).map_err(|_| invalid_checkpoint())?;
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
        let Some(state_payload) = &checkpoint.state_payload else {
            return Err(invalid_checkpoint());
        };
        if state_payload.codec_identity != checkpoint.checkpoint_codec_identity {
            return Err(StandingProgramRuntimeError::CheckpointCodecMismatch {
                expected: checkpoint.checkpoint_codec_identity.clone(),
                actual: state_payload.codec_identity.clone(),
            });
        }
        let payload: ScalarAggregateFilterCheckpointPayload =
            serde_json::from_str(&state_payload.payload).map_err(|_| invalid_checkpoint())?;
        if payload.schema_version != CHECKPOINT_PAYLOAD_SCHEMA_VERSION
            || payload.runtime_kind != SCALAR_AGGREGATE_FILTER_RUNTIME_KIND
        {
            return Err(invalid_checkpoint());
        }
        validate_scalar_aggregate_filter_contract(
            &[
                payload.outer_catalog.clone(),
                payload.scalar_catalog.clone(),
            ],
            &[
                payload.outer_input_schema.clone(),
                payload.scalar_input_schema.clone(),
            ],
            &payload.output_schema,
            &payload.plan,
        )?;
        if payload.logical_epoch != checkpoint.logical_epoch
            || payload.input_frontiers != checkpoint.input_frontiers
            || payload.input_event_time_frontiers != checkpoint.input_event_time_frontiers
        {
            return Err(invalid_checkpoint());
        }
        validate_view_sql_hash(&checkpoint.identity, payload.view_sql.as_str())?;
        let compiled = validate_supported_scalar_aggregate_filter_sql(
            payload.view_sql.as_str(),
            &[
                payload.outer_catalog.clone(),
                payload.scalar_catalog.clone(),
            ],
        )
        .map_err(|_| invalid_checkpoint())?;
        if compiled != payload.plan {
            return Err(invalid_checkpoint());
        }
        validate_published_output(&payload.published_output)?;
        let mut applied_epochs = payload
            .applied_epochs
            .into_iter()
            .map(|entry| (entry.idempotency_key, entry.logical_epoch))
            .collect();
        retain_recent_applied_epochs(&mut applied_epochs);
        Ok(Self {
            identity: checkpoint.identity,
            outer_catalog: payload.outer_catalog,
            scalar_catalog: payload.scalar_catalog,
            outer_input_schema: payload.outer_input_schema,
            scalar_input_schema: payload.scalar_input_schema,
            output_schema: payload.output_schema,
            view_sql: payload.view_sql,
            plan: payload.plan,
            logical_plan: payload.logical_plan,
            outer_rows: payload.outer_rows,
            scalar_state: payload.scalar_state,
            published_output: payload.published_output,
            input_frontiers: checkpoint.input_frontiers,
            input_event_time_frontiers: checkpoint.input_event_time_frontiers,
            applied_epochs,
            logical_epoch: checkpoint.logical_epoch,
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ScalarAggregateFilterCheckpointPayload {
    schema_version: u32,
    runtime_kind: String,
    outer_catalog: VelorixRelationCatalogV1,
    scalar_catalog: VelorixRelationCatalogV1,
    outer_input_schema: RelationSchema,
    scalar_input_schema: RelationSchema,
    output_schema: RelationSchema,
    view_sql: String,
    plan: SupportedScalarAggregateFilterPlanV1,
    logical_plan: VelorixLogicalViewPlanV1,
    input_frontiers: Vec<RelationFrontier>,
    input_event_time_frontiers: Vec<InputEventTimeFrontier>,
    outer_rows: BTreeMap<String, ScalarOuterRow>,
    scalar_state: ScalarAggregateState,
    published_output: DeltaBatch,
    applied_epochs: Vec<GenericAppliedEpoch>,
    logical_epoch: LogicalEpoch,
}

impl ScalarAggregateFilterRuntime {
    fn apply_outer_input(
        &mut self,
        input: &RelationInputBatch,
    ) -> Result<(), StandingProgramRuntimeError> {
        validate_input_matches_schema(input, &self.outer_input_schema, "scalar_aggregate_outer")?;
        let columns = filter_project_input_column_ids(&self.plan.projection);
        let delta = if let Some(empty_delta) =
            published_input_empty_delta(input, &self.outer_catalog)?
        {
            empty_delta
        } else {
            arrow_record_batches_to_key_multi_value_delta_batch(
                &self.outer_catalog,
                &input.relation_id,
                &input.relation_version,
                &input.schema_fingerprint,
                std::slice::from_ref(&self.plan.outer_key_column_id),
                &columns,
                &input.batches,
            )
            .map_err(|_| StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "scalar_aggregate_outer_input_batch",
            })?
        };
        for record in delta.net_rows().map_err(|_| invalid_runtime_state())? {
            let key = canonical_json(record.key.as_json());
            let entry = self
                .outer_rows
                .entry(key)
                .or_insert_with(|| ScalarOuterRow {
                    key: record.key.as_json().clone(),
                    values: BTreeMap::new(),
                    weight: 0,
                });
            entry.weight = entry
                .weight
                .checked_add(record.weight)
                .ok_or_else(invalid_runtime_state)?;
            if entry.weight < 0 {
                return Err(invalid_runtime_state());
            }
            if entry.weight == 0 {
                self.outer_rows
                    .remove(&canonical_json(record.key.as_json()));
                continue;
            }
            let value = record
                .value
                .as_json()
                .as_object()
                .ok_or_else(invalid_runtime_state)?;
            entry.values = value
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect();
        }
        Ok(())
    }

    fn apply_scalar_input(
        &mut self,
        input: &RelationInputBatch,
    ) -> Result<(), StandingProgramRuntimeError> {
        validate_input_matches_schema(input, &self.scalar_input_schema, "scalar_aggregate_scalar")?;
        let Some(column_id) = &self.plan.scalar_aggregate.input_column_id else {
            // count(*): only the weight matters.
            let delta = if let Some(empty_delta) =
                published_input_empty_delta(input, &self.scalar_catalog)?
            {
                empty_delta
            } else {
                arrow_record_batches_to_key_nullable_count_delta_batch(
                    &self.scalar_catalog,
                    &input.relation_id,
                    &input.relation_version,
                    &input.schema_fingerprint,
                    &self.plan.outer_key_column_id,
                    &self
                        .plan
                        .scalar_aggregate
                        .input_column_id
                        .clone()
                        .unwrap_or_default(),
                    &input.batches,
                )
                .map_err(|_| {
                    StandingProgramRuntimeError::InvalidProgramIdentity {
                        field: "scalar_aggregate_scalar_input_batch",
                    }
                })?
            };
            for record in delta.net_rows().map_err(|_| invalid_runtime_state())? {
                self.scalar_state.count = self
                    .scalar_state
                    .count
                    .checked_add(record.weight)
                    .ok_or_else(invalid_runtime_state)?;
                if self.scalar_state.count < 0 {
                    return Err(invalid_runtime_state());
                }
            }
            return Ok(());
        };
        let delta = if let Some(empty_delta) =
            published_input_empty_delta(input, &self.scalar_catalog)?
        {
            empty_delta
        } else {
            arrow_record_batches_to_key_value_delta_batch(
                &self.scalar_catalog,
                &input.relation_id,
                &input.relation_version,
                &input.schema_fingerprint,
                std::slice::from_ref(column_id),
                column_id,
                &input.batches,
            )
            .map_err(|_| StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "scalar_aggregate_scalar_input_batch",
            })?
        };
        for record in delta.net_rows().map_err(|_| invalid_runtime_state())? {
            let value = if record.value.as_json().is_object() {
                record
                    .value
                    .as_json()
                    .get(column_id)
                    .cloned()
                    .ok_or(StandingProgramRuntimeError::InvalidProgramIdentity {
                        field: "scalar_missing_scalar_value",
                    })?
            } else {
                record.value.as_json().clone()
            };
            if value.is_null() {
                continue;
            }
            let key = canonical_json(&value);
            match self.plan.scalar_aggregate.function {
                LogicalPlanAggregateFunctionV1::Count
                | LogicalPlanAggregateFunctionV1::CountDistinct => {
                    let entry = self
                        .scalar_state
                        .values
                        .entry(key.clone())
                        .or_insert_with(|| ScalarAggregateEntry {
                            value: value.clone(),
                            weight: 0,
                        });
                    entry.weight = entry
                        .weight
                        .checked_add(record.weight)
                        .ok_or_else(invalid_runtime_state)?;
                    if entry.weight <= 0 {
                        self.scalar_state.values.remove(&key);
                    }
                    if self.plan.scalar_aggregate.function == LogicalPlanAggregateFunctionV1::Count
                    {
                        self.scalar_state.count = self
                            .scalar_state
                            .count
                            .checked_add(record.weight)
                            .ok_or_else(invalid_runtime_state)?;
                        if self.scalar_state.count < 0 {
                            return Err(invalid_runtime_state());
                        }
                    }
                }
                LogicalPlanAggregateFunctionV1::Sum => {
                    let amount = value.as_i64().ok_or_else(invalid_runtime_state)?;
                    self.scalar_state.sum = self
                        .scalar_state
                        .sum
                        .checked_add(
                            amount
                                .checked_mul(record.weight)
                                .ok_or_else(invalid_runtime_state)?,
                        )
                        .ok_or_else(invalid_runtime_state)?;
                    // SUM also needs to track count for non-NULL row detection
                    self.scalar_state.count = self
                        .scalar_state
                        .count
                        .checked_add(record.weight)
                        .ok_or_else(invalid_runtime_state)?;
                    if self.scalar_state.count < 0 {
                        return Err(invalid_runtime_state());
                    }
                }
                LogicalPlanAggregateFunctionV1::Avg => {
                    let amount = value.as_i64().ok_or_else(invalid_runtime_state)?;
                    self.scalar_state.avg_sums = self
                        .scalar_state
                        .avg_sums
                        .checked_add(
                            amount
                                .checked_mul(record.weight)
                                .ok_or_else(invalid_runtime_state)?,
                        )
                        .ok_or_else(invalid_runtime_state)?;
                    self.scalar_state.avg_counts = self
                        .scalar_state
                        .avg_counts
                        .checked_add(record.weight)
                        .ok_or_else(invalid_runtime_state)?;
                }
                LogicalPlanAggregateFunctionV1::Min | LogicalPlanAggregateFunctionV1::Max => {
                    let entry = self
                        .scalar_state
                        .values
                        .entry(key.clone())
                        .or_insert_with(|| ScalarAggregateEntry {
                            value: value.clone(),
                            weight: 0,
                        });
                    entry.weight = entry
                        .weight
                        .checked_add(record.weight)
                        .ok_or_else(invalid_runtime_state)?;
                    if entry.weight <= 0 {
                        self.scalar_state.values.remove(&key);
                    }
                }
                LogicalPlanAggregateFunctionV1::PercentileDisc { .. }
                | LogicalPlanAggregateFunctionV1::PercentileCont { .. } => {
                    return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                        field: "scalar_aggregate_filter_percentile_not_supported",
                    })
                }
            }
        }
        // MIN/MAX: canonical key order over the multiset keys gives the
        // extrema (JSON canonical ordering is a total order for scalars).
        let _ = column_id;
        Ok(())
    }

    fn recompute_all_outputs(&self) -> Result<DeltaBatch, StandingProgramRuntimeError> {
        let scalar = self.current_scalar()?;
        let mut records = Vec::new();
        let comparison_column = &self.plan.outer_comparison_column_id;
        for row in self.outer_rows.values() {
            let outer_value = row.values.get(comparison_column).cloned();
            let Some(outer_value) = outer_value else {
                continue;
            };
            let matches = match &scalar {
                Some(scalar) => self.evaluate_comparison(&outer_value, scalar),
                None => false,
            };
            if matches {
                records.push(self.project_row(row)?);
            }
        }
        if u64::try_from(records.len()).unwrap_or(u64::MAX)
            > self.plan.resource_contract.max_recomputed_rows_per_epoch
        {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "scalar_aggregate_filter_resource_contract",
            });
        }
        Ok(DeltaBatch::from_records(records))
    }

    fn apply_outer_deltas_only(&self) -> Result<DeltaBatch, StandingProgramRuntimeError> {
        // With an unchanged scalar the output is recomputed from the full
        // outer bag as well; the diff against the published output yields
        // exactly the changed rows, and the resource contract bounds the
        // work. This keeps the implementation simple and exact.
        self.recompute_all_outputs()
    }

    fn project_row(
        &self,
        row: &ScalarOuterRow,
    ) -> Result<DeltaRecord, StandingProgramRuntimeError> {
        let mut output = serde_json::Map::new();
        let row_values: serde_json::Map<String, Value> = row
            .values
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        for column in &self.plan.projection.value_columns {
            let value = if let Some(expression) = &column.expression {
                Value::Number(JsonNumber::from(evaluate_projection_expr(
                    expression,
                    &row_values,
                    &self.outer_catalog,
                )?))
            } else {
                row.values
                    .get(column.input_column_id.as_str())
                    .cloned()
                    .ok_or(StandingProgramRuntimeError::InvalidProgramIdentity {
                        field: "scalar_missing_projection_value",
                    })?
            };
            output.insert(column.output_column_id.clone(), value);
        }
        let output_key = if let Some(output_key_input_column_id) =
            &self.plan.projection.output_key_input_column_id
        {
            row.values
                .get(output_key_input_column_id.as_str())
                .cloned()
                .ok_or_else(invalid_runtime_state)?
        } else {
            row.key.clone()
        };
        Ok(DeltaRecord::new(
            DeltaKey::from_json(output_key),
            DeltaValue::from_json(Value::Object(output)),
            row.weight,
        ))
    }
}

pub(super) const SCALAR_AGGREGATE_FILTER_RUNTIME_KIND: &str = "scalar_aggregate_filter";

pub(super) fn catalog_for_relation_id<'a>(
    catalogs: &'a [VelorixRelationCatalogV1],
    relation_id: &str,
) -> Result<&'a VelorixRelationCatalogV1, StandingProgramRuntimeError> {
    catalogs
        .iter()
        .find(|catalog| catalog.relation_schema.relation_id == relation_id)
        .ok_or(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "scalar_aggregate_filter_catalog",
        })
}

fn validate_scalar_aggregate_filter_contract(
    catalogs: &[VelorixRelationCatalogV1],
    input_schemas: &[RelationSchema],
    output_schema: &RelationSchema,
    plan: &SupportedScalarAggregateFilterPlanV1,
) -> Result<(), StandingProgramRuntimeError> {
    if plan.schema_version != 1 || catalogs.len() != 2 || input_schemas.len() != 2 {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "scalar_aggregate_filter_contract",
        });
    }
    let outer_catalog = catalog_for_relation_id(catalogs, &plan.outer_input_relation_id)?;
    let scalar_catalog = catalog_for_relation_id(catalogs, &plan.scalar_input_relation_id)?;
    if outer_catalog.relation_schema.relation_id == scalar_catalog.relation_schema.relation_id {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "scalar_aggregate_filter_contract",
        });
    }
    catalog_column(outer_catalog, &plan.outer_key_column_id)?;
    catalog_column(outer_catalog, &plan.outer_comparison_column_id)?;
    if let Some(input_column_id) = &plan.scalar_aggregate.input_column_id {
        catalog_column(scalar_catalog, input_column_id)?;
    }
    validate_filter_project_supported_schemas(
        outer_catalog,
        &input_schemas[0],
        output_schema,
        &plan.projection,
    )?;
    let expected_scalar = catalog_input_relation_schema(scalar_catalog).map_err(|_| {
        StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "scalar_aggregate_filter_scalar_input_schema",
        }
    })?;
    if expected_scalar != input_schemas[1] {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "scalar_aggregate_filter_scalar_input_schema",
        });
    }
    Ok(())
}
