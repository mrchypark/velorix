//! Native approximate quantiles retain signed value multiplicities and rebuild only changed groups.
//! The pinned DataFusion 55.1.0 weighted t-digest is math only: no query engine is invoked.
//! Numeric value order, 512-value chunks and max_size 100 define the persisted algorithm identity.
//! Rebuild cost is O(D log D) for D distinct retained values, independent of expanded multiplicity;
//! retained state is O(D), with a bounded digest and chunk. No timestamp-age cutoff is applied.
//! This approximation is not DuckDB bitwise equivalence; cross-engine validation must compare
//! both quantile estimates and any downstream threshold decisions.

use super::*;

pub(super) fn bag_input_delta(
    catalog: &VelorixRelationCatalogV1,
    plan: &SupportedViewPlan,
    input: &RelationInputBatch,
    value_column_ids: &BTreeSet<String>,
) -> Result<DeltaBatch, StandingProgramRuntimeError> {
    if input.batches.is_empty() {
        return Err(invalid_runtime_state());
    }
    let anchor = catalog_column(catalog, &plan.group_key_column_id)?;
    let weight = catalog_column(catalog, &catalog.relation_schema.weight_column_id)?;
    let columns = value_column_ids
        .iter()
        .map(|id| catalog_column(catalog, id))
        .collect::<Result<Vec<_>, _>>()?;
    let mut records = Vec::new();
    for batch in &input.batches {
        validate_record_batch_matches_catalog(catalog, batch)
            .map_err(|_| invalid_runtime_state())?;
        let weights = batch
            .column_by_name(&weight.name)
            .and_then(|array| array.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(invalid_runtime_state)?;
        for row in 0..batch.num_rows() {
            if weights.is_null(row) {
                return Err(invalid_runtime_state());
            }
            let values = columns
                .iter()
                .map(|column| {
                    Ok((
                        column.column_id.clone(),
                        two_input_join::typed_join_arrow_value(batch, column, row)?,
                    ))
                })
                .collect::<Result<Map<_, _>, StandingProgramRuntimeError>>()?;
            records.push(DeltaRecord::new(
                DeltaKey::from_json(two_input_join::typed_join_arrow_value(batch, anchor, row)?),
                DeltaValue::from_json(Value::Object(values)),
                weights.value(row),
            ));
        }
    }
    Ok(DeltaBatch::from_records(records))
}

pub(crate) const APPROX_QUANTILE_ALGORITHM: &str =
    "datafusion-55.1.0-tdigest-weighted-sorted-512-max100-v1";
const APPROX_QUANTILE_MAX_SIZE: usize = 100;
const APPROX_QUANTILE_CHUNK_SIZE: usize = 512;

pub(crate) fn zeroed_approx_quantile(percentile: f64) -> Value {
    serde_json::json!({
        "algorithm": APPROX_QUANTILE_ALGORITHM,
        "percentile": percentile,
        "values": [],
        "estimate": null,
    })
}

pub(crate) fn update_approx_quantile_values(
    row: &mut Map<String, Value>,
    output_column_id: &str,
    value: &Value,
    weight: i64,
) -> Result<(), StandingProgramRuntimeError> {
    if !value.is_number() {
        return Err(invalid_runtime_state());
    }
    aggregate_sum_as_f64(value)?;
    let state = row
        .get_mut(output_column_id)
        .and_then(Value::as_object_mut)
        .ok_or_else(invalid_runtime_state)?;
    let mut bag = Map::new();
    bag.insert(
        output_column_id.to_string(),
        state
            .get("values")
            .cloned()
            .ok_or_else(invalid_runtime_state)?,
    );
    update_filtered_multiset_value(&mut bag, output_column_id, value, weight)?;
    state.insert(
        "values".into(),
        bag.remove(output_column_id)
            .ok_or_else(invalid_runtime_state)?,
    );
    Ok(())
}

pub(crate) fn approx_quantile_estimate(
    values: &[Value],
    percentile: f64,
) -> Result<Value, StandingProgramRuntimeError> {
    use arrow::array::Float64Array;
    use arrow::datatypes::DataType;
    use datafusion::{
        common::ScalarValue,
        functions_aggregate::{
            approx_percentile_cont::ApproxPercentileAccumulator,
            approx_percentile_cont_with_weight::ApproxPercentileWithWeightAccumulator,
        },
        logical_expr::Accumulator,
    };
    use std::sync::Arc;
    if !percentile.is_finite() || !(0.0..=1.0).contains(&percentile) {
        return Err(invalid_runtime_state());
    }
    let mut weighted_values = Vec::with_capacity(values.len());
    let mut total_weight = 0_i64;
    let mut seen = BTreeSet::new();
    for entry in values {
        let input = entry.get("value").ok_or_else(invalid_runtime_state)?;
        if !input.is_number() || !seen.insert(canonical_json(input)) {
            return Err(invalid_runtime_state());
        }
        let value = aggregate_sum_as_f64(input)?;
        let weight = entry
            .get("weight")
            .and_then(Value::as_i64)
            .ok_or_else(invalid_runtime_state)?;
        if weight <= 0 {
            return Err(invalid_runtime_state());
        }
        total_weight = total_weight
            .checked_add(weight)
            .ok_or_else(invalid_runtime_state)?;
        // The sketch uses f64 weights; preserve exact source multiplicity at its boundary.
        if total_weight > (1_i64 << 53) {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "approx_quantile.weight_precision",
            });
        }
        weighted_values.push((value, weight));
    }
    weighted_values.sort_by(|left, right| left.0.total_cmp(&right.0).then(left.1.cmp(&right.1)));
    let mut digest =
        ApproxPercentileWithWeightAccumulator::new(ApproxPercentileAccumulator::new_with_max_size(
            percentile,
            DataType::Float64,
            APPROX_QUANTILE_MAX_SIZE,
        ));
    for chunk in weighted_values.chunks(APPROX_QUANTILE_CHUNK_SIZE) {
        let means = Float64Array::from(chunk.iter().map(|(value, _)| *value).collect::<Vec<_>>());
        let weights = Float64Array::from(
            chunk
                .iter()
                .map(|(_, weight)| *weight as f64)
                .collect::<Vec<_>>(),
        );
        digest
            .update_batch(&[Arc::new(means), Arc::new(weights)])
            .map_err(|_| invalid_runtime_state())?;
    }
    match digest.evaluate().map_err(|_| invalid_runtime_state())? {
        ScalarValue::Float64(None) => Ok(Value::Null),
        ScalarValue::Float64(Some(value)) => JsonNumber::from_f64(value)
            .map(Value::Number)
            .ok_or_else(invalid_runtime_state),
        _ => Err(invalid_runtime_state()),
    }
}

pub(crate) fn rebuild_approx_quantile(
    state: &mut Value,
    percentile: f64,
) -> Result<(), StandingProgramRuntimeError> {
    project_approx_quantile(state, percentile)?;
    let state = state.as_object_mut().ok_or_else(invalid_runtime_state)?;
    let values = state
        .get("values")
        .and_then(Value::as_array)
        .ok_or_else(invalid_runtime_state)?;
    let estimate = approx_quantile_estimate(values, percentile)?;
    state.insert("estimate".into(), estimate);
    Ok(())
}

pub(crate) fn project_approx_quantile(
    state: &Value,
    percentile: f64,
) -> Result<Value, StandingProgramRuntimeError> {
    // Projection is idempotent when HAVING reads an already-published scalar.
    if !state.is_object() {
        if state.is_null() {
            return Ok(Value::Null);
        }
        aggregate_sum_as_f64(state)?;
        return Ok(state.clone());
    }
    if state.get("algorithm").and_then(Value::as_str) != Some(APPROX_QUANTILE_ALGORITHM)
        || state.get("percentile").and_then(Value::as_f64) != Some(percentile)
        || state.get("values").and_then(Value::as_array).is_none()
    {
        return Err(invalid_runtime_state());
    }
    let estimate = state.get("estimate").ok_or_else(invalid_runtime_state)?;
    if !estimate.is_null() {
        aggregate_sum_as_f64(estimate)?;
    }
    Ok(estimate.clone())
}

pub(crate) fn validate_approx_quantile_checkpoint(
    state: &Value,
    percentile: f64,
) -> Result<(), StandingProgramRuntimeError> {
    if !state.is_object() {
        return Err(invalid_checkpoint());
    }
    let published = project_approx_quantile(state, percentile)?;
    let values = state
        .get("values")
        .and_then(Value::as_array)
        .ok_or_else(invalid_checkpoint)?;
    if approx_quantile_estimate(values, percentile)? != published {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "approx_quantile.checkpoint_estimate",
        });
    }
    Ok(())
}

pub(super) fn arg_max_order_column(
    catalog: &VelorixRelationCatalogV1,
    ordinal: usize,
) -> Result<&velorix_core::relation::RelationColumnV1, StandingProgramRuntimeError> {
    let column = catalog
        .relation_schema
        .columns
        .iter()
        .find(|column| column.ordinal as usize == ordinal)
        .ok_or_else(invalid_runtime_state)?;
    if column.column_id == catalog.relation_schema.weight_column_id
        || !matches!(
            column.physical_arrow_type,
            ArrowPhysicalTypeV1::Int64
                | ArrowPhysicalTypeV1::TimestampNanosecond { timezone: None }
        )
    {
        return Err(invalid_runtime_state());
    }
    Ok(column)
}

pub(super) fn arg_max_input(
    aggregate: &SupportedAggregateOutput,
    ordinal: usize,
    plan: &SupportedViewPlan,
    catalog: &VelorixRelationCatalogV1,
    record: &DeltaRecord,
) -> Result<Option<Value>, StandingProgramRuntimeError> {
    let value = aggregate_input_record_value(aggregate, &plan.sum_value_column_id, record);
    let column = arg_max_order_column(catalog, ordinal)?;
    let ordering = if column.column_id == plan.group_key_column_id {
        record
            .value
            .as_json()
            .get(&column.column_id)
            .unwrap_or(record.key.as_json())
    } else {
        record
            .value
            .as_json()
            .get(&column.column_id)
            .ok_or_else(invalid_runtime_state)?
    };
    if value.is_null() || ordering.is_null() {
        return Ok(None);
    }
    aggregate_sum_as_f64(value)?;
    let ordering = ordering.as_i64().ok_or_else(invalid_runtime_state)?;
    Ok(Some(
        serde_json::json!({"ordering": ordering, "value": value}),
    ))
}

pub(super) fn project_arg_max(values: &[Value]) -> Result<Value, StandingProgramRuntimeError> {
    let mut selected: Option<(i64, String, Value)> = None;
    for entry in values {
        let weight = entry
            .get("weight")
            .and_then(Value::as_i64)
            .ok_or_else(invalid_runtime_state)?;
        if weight <= 0 {
            return Err(invalid_runtime_state());
        }
        let candidate = entry.get("value").ok_or_else(invalid_runtime_state)?;
        let ordering = candidate
            .get("ordering")
            .and_then(Value::as_i64)
            .ok_or_else(invalid_runtime_state)?;
        let value = candidate.get("value").ok_or_else(invalid_runtime_state)?;
        aggregate_sum_as_f64(value)?;
        let tie_key = canonical_json(value);
        // Equal timestamps use a persisted value ordering, independent of arrival order.
        if selected
            .as_ref()
            .is_none_or(|(current_order, current_tie, _)| {
                (ordering, &tie_key) > (*current_order, current_tie)
            })
        {
            selected = Some((ordering, tie_key, value.clone()));
        }
    }
    Ok(selected.map(|(_, _, value)| value).unwrap_or(Value::Null))
}

#[derive(Clone, Debug)]
pub struct SingleKeySumCountRuntime {
    limits: RuntimeResourceLimits,
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
    group_input_counts: DeltaBatch,
    input_frontiers: Vec<RelationFrontier>,
    input_event_time_frontiers: Vec<InputEventTimeFrontier>,
    applied_epochs: BTreeMap<String, LogicalEpoch>,
    #[cfg(test)]
    fail_next_output_encoding: bool,
}

impl SingleKeySumCountRuntime {
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_logical_plan_and_limits(
        identity: StandingProgramIdentity,
        catalog: VelorixRelationCatalogV1,
        input_schema: RelationSchema,
        output_schema: RelationSchema,
        view_sql: String,
        plan: SupportedViewPlan,
        logical_plan: VelorixLogicalViewPlanV1,
        limits: RuntimeResourceLimits,
    ) -> Result<Self, StandingProgramRuntimeError> {
        limits.validate()?;
        let mut runtime = Self::new_with_logical_plan(
            identity,
            catalog,
            input_schema,
            output_schema,
            view_sql,
            plan,
            logical_plan,
        )?;
        runtime.limits = limits;
        runtime.checkpoint_payload()?;
        Ok(runtime)
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
            limits: RuntimeResourceLimits::default(),
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
            group_input_counts: DeltaBatch::default(),
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

    fn checkpoint_payload(
        &self,
    ) -> Result<velorix_core::arrow_state::ArrowState, StandingProgramRuntimeError> {
        self.limits.checkpoint_arrow(&self.checkpoint_data())
    }

    fn checkpoint_data(&self) -> GenericCheckpointPayload {
        GenericCheckpointPayload {
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
            group_input_counts: single_key_plan_uses_runtime_aggregate_state(
                &self.plan,
                &self.catalog,
            )
            .then(|| self.group_input_counts.clone()),
            applied_epochs: self
                .applied_epochs
                .iter()
                .map(|(idempotency_key, logical_epoch)| GenericAppliedEpoch {
                    idempotency_key: idempotency_key.clone(),
                    logical_epoch: *logical_epoch,
                })
                .collect(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn validate_candidate_checkpoint(
        &self,
        prepared: &PreparedKeyedAggregateEpoch,
        epoch: LogicalEpoch,
        output: &DeltaBatch,
        filtered: &DeltaBatch,
        groups: &DeltaBatch,
        frontiers: &[RelationFrontier],
        event_frontiers: &[InputEventTimeFrontier],
        key: &str,
    ) -> Result<(), StandingProgramRuntimeError> {
        let mut payload = self.checkpoint_data();
        let state = apply_published_output_delta(
            &self.engine.materialized_state(),
            prepared.output_changes(),
        )?;
        payload.engine = velorix_core::engine::EngineCheckpoint::new(epoch, state).to_payload();
        payload.published_output = output.clone();
        payload.filtered_aggregate_state = filtered.clone();
        if payload.group_input_counts.is_some() {
            payload.group_input_counts = Some(groups.clone());
        }
        payload.input_frontiers = frontiers.to_vec();
        payload.input_event_time_frontiers = event_frontiers.to_vec();
        let mut applied = self.applied_epochs.clone();
        applied.insert(key.to_owned(), epoch);
        retain_recent_applied_epochs(&mut applied);
        payload.applied_epochs = applied
            .into_iter()
            .map(|(idempotency_key, logical_epoch)| GenericAppliedEpoch {
                idempotency_key,
                logical_epoch,
            })
            .collect();
        self.limits.validate_checkpoint_value(&payload)?;
        Ok(())
    }

    fn restore_payload(
        checkpoint: &RuntimeCheckpoint,
        limits: RuntimeResourceLimits,
    ) -> Result<GenericCheckpointPayload, StandingProgramRuntimeError> {
        let Some(state_payload) = &checkpoint.state_payload else {
            return Err(invalid_checkpoint());
        };
        limits.validate_checkpoint(&state_payload.payload)?;
        if state_payload.codec_identity != checkpoint.checkpoint_codec_identity {
            return Err(StandingProgramRuntimeError::CheckpointCodecMismatch {
                expected: checkpoint.checkpoint_codec_identity.clone(),
                actual: state_payload.codec_identity.clone(),
            });
        }
        let payload: GenericCheckpointPayload =
            velorix_core::arrow_state::from_state(&state_payload.payload)
                .map_err(|_| invalid_checkpoint())?;
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
        if single_key_plan_uses_runtime_aggregate_state(&self.plan, &self.catalog) {
            let mut combined = DeltaBatch::default();
            let mut combined_group_inputs = DeltaBatch::default();
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
                // Count rows surviving the view-level WHERE, but before each
                // aggregate's FILTER clause, so group liveness matches SQL
                // grouping semantics rather than one aggregate's inputs.
                let group_delta =
                    rekey_delta_batch_for_aggregate_group(&delta, &self.catalog, &self.plan)?;
                combined_group_inputs = combined_group_inputs.combine(&group_delta);
                let delta = group_delta;
                combined = combined.combine(&delta);
                advance_input_frontier(&mut input_frontiers, &input)?;
                advance_input_event_time_frontier(&mut input_event_time_frontiers, &input)?;
            }
            let next_group_input_counts =
                apply_group_input_count_delta(&self.group_input_counts, &combined_group_inputs)?;
            let (next_state, _) = apply_filtered_single_key_aggregate_delta(
                &self.filtered_aggregate_state,
                &combined,
                &self.plan,
                &self.catalog,
            )?;
            let next_state = synchronize_filtered_aggregate_groups(
                &next_state,
                &next_group_input_counts,
                &self.plan,
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
            self.validate_candidate_checkpoint(
                &prepared_epoch,
                logical_epoch,
                &visible_output,
                &next_state,
                &next_group_input_counts,
                &input_frontiers,
                &input_event_time_frontiers,
                &idempotency_key_text,
            )?;
            self.engine
                .commit_prepared_epoch(prepared_epoch)
                .map_err(|_| invalid_runtime_state())?;
            self.filtered_aggregate_state = next_state;
            self.group_input_counts = next_group_input_counts;
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
            self.validate_candidate_checkpoint(
                &prepared_epoch,
                logical_epoch,
                &staged_output,
                &self.filtered_aggregate_state,
                &self.group_input_counts,
                &input_frontiers,
                &input_event_time_frontiers,
                &idempotency_key_text,
            )?;
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
            self.validate_candidate_checkpoint(
                &prepared_epoch,
                logical_epoch,
                &staged_output,
                &self.filtered_aggregate_state,
                &self.group_input_counts,
                &input_frontiers,
                &input_event_time_frontiers,
                &idempotency_key_text,
            )?;
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
        Self::restore_with_limits(checkpoint, RuntimeResourceLimits::default())
    }
}

impl SingleKeySumCountRuntime {
    pub fn restore_with_limits(
        checkpoint: RuntimeCheckpoint,
        limits: RuntimeResourceLimits,
    ) -> Result<Self, StandingProgramRuntimeError> {
        limits.validate()?;
        checkpoint.validate_identity(&checkpoint.identity)?;
        let payload = Self::restore_payload(&checkpoint, limits)?;
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
        let group_input_counts =
            if single_key_plan_uses_runtime_aggregate_state(&plan, &payload.catalog) {
                payload
                    .group_input_counts
                    .ok_or_else(legacy_single_key_checkpoint_requires_replay)?
            } else {
                DeltaBatch::default()
            };
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
        let filtered_aggregate_state =
            if single_key_plan_uses_runtime_aggregate_state(&plan, &payload.catalog)
                && !supported_view_plan_is_singleton(&plan)
                && payload.filtered_aggregate_state.records().is_empty()
            {
                published_output.clone()
            } else {
                payload.filtered_aggregate_state
            };
        validate_published_output(&published_output)?;
        validate_published_output(&filtered_aggregate_state)?;
        if single_key_plan_uses_runtime_aggregate_state(&plan, &payload.catalog) {
            validate_group_input_count_state(
                &group_input_counts,
                &filtered_aggregate_state,
                &plan,
                &payload.catalog,
            )?;
        }
        let mut applied_epochs = payload
            .applied_epochs
            .into_iter()
            .map(|entry| (entry.idempotency_key, entry.logical_epoch))
            .collect();
        retain_recent_applied_epochs(&mut applied_epochs);
        Ok(Self {
            limits,
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
            group_input_counts,
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
    fn approximate_quantile_retains_duplicate_weights_and_validates_restore() {
        let mut row = Map::new();
        row.insert("rate".into(), zeroed_approx_quantile(0.5));
        assert_eq!(
            project_approx_quantile(&row["rate"], 0.5).unwrap(),
            Value::Null
        );
        update_approx_quantile_values(&mut row, "rate", &serde_json::json!(10.0), 2).unwrap();
        update_approx_quantile_values(&mut row, "rate", &serde_json::json!(30.0), 1).unwrap();
        rebuild_approx_quantile(row.get_mut("rate").unwrap(), 0.5).unwrap();
        let serialized = serde_json::to_string(&row["rate"]).unwrap();
        let restored: Value = serde_json::from_str(&serialized).unwrap();
        validate_approx_quantile_checkpoint(&restored, 0.5).unwrap();
        let before = restored["estimate"].as_f64().unwrap();
        assert!((10.0..=30.0).contains(&before));

        update_approx_quantile_values(&mut row, "rate", &serde_json::json!(10.0), -1).unwrap();
        assert_eq!(row["rate"]["values"].as_array().unwrap().len(), 2);
        update_approx_quantile_values(&mut row, "rate", &serde_json::json!(30.0), -1).unwrap();
        rebuild_approx_quantile(row.get_mut("rate").unwrap(), 0.5).unwrap();
        assert_eq!(row["rate"]["estimate"].as_f64(), Some(10.0));
        update_approx_quantile_values(&mut row, "rate", &serde_json::json!(10.0), -1).unwrap();
        rebuild_approx_quantile(row.get_mut("rate").unwrap(), 0.5).unwrap();
        assert_eq!(row["rate"]["estimate"], Value::Null);

        let mut corrupt = restored.clone();
        corrupt["estimate"] = serde_json::json!(999.0);
        assert!(validate_approx_quantile_checkpoint(&corrupt, 0.5).is_err());
        corrupt = restored;
        corrupt["algorithm"] = serde_json::json!("exact-median");
        assert!(validate_approx_quantile_checkpoint(&corrupt, 0.5).is_err());
    }

    #[test]
    fn approximate_quantile_fractional_checkpoint_preserves_retraction_identity() {
        let mut row = Map::new();
        row.insert("rate".into(), zeroed_approx_quantile(0.5));
        // Real sequence rates contain non-dyadic fractions. JSON restoration must preserve
        // the exact retained f64, otherwise both the digest and later delete lookup drift.
        let rates = [
            1.2345678901234567,
            60.0 / 7.0,
            59.0 / 13.0,
            1.0000000000000002,
        ];
        for rate in rates {
            update_approx_quantile_values(&mut row, "rate", &serde_json::json!(rate), 2).unwrap();
        }
        rebuild_approx_quantile(row.get_mut("rate").unwrap(), 0.5).unwrap();
        let encoded = serde_json::to_string(&row).unwrap();
        let mut restored: Map<String, Value> = serde_json::from_str(&encoded).unwrap();
        assert_eq!(restored, row);
        validate_approx_quantile_checkpoint(&restored["rate"], 0.5).unwrap();
        for rate in rates {
            update_approx_quantile_values(&mut restored, "rate", &serde_json::json!(rate), -2)
                .unwrap();
        }
        rebuild_approx_quantile(restored.get_mut("rate").unwrap(), 0.5).unwrap();
        assert_eq!(
            project_approx_quantile(&restored["rate"], 0.5).unwrap(),
            Value::Null
        );
    }

    #[test]
    fn approximate_quantile_large_multiplicity_does_not_expand_rows() {
        let bag = vec![serde_json::json!({"value": 12.5, "weight": 1_000_000_000_000_i64})];
        assert_eq!(
            approx_quantile_estimate(&bag, 0.5).unwrap().as_f64(),
            Some(12.5)
        );
        assert!(approx_quantile_estimate(&bag, f64::NAN).is_err());
        assert!(approx_quantile_estimate(&bag, 1.1).is_err());
        assert!(
            approx_quantile_estimate(&[serde_json::json!({"value": 1.0, "weight": -1})], 0.5)
                .is_err()
        );
    }

    #[test]
    fn nullable_float_aggregates_retract_and_restore() {
        let mut catalog = purchases_catalog();
        let amount = &mut catalog.relation_schema.columns[1];
        amount.logical_type = VelorixLogicalTypeV1::Float64;
        amount.physical_arrow_type = ArrowPhysicalTypeV1::Float64;
        amount.nullable = true;
        catalog.schema_fingerprint =
            SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema).unwrap();
        catalog.incremental_relation.schema_fingerprint = catalog.schema_fingerprint.clone();
        let sql = "select user_id, sum(amount) as total, avg(amount) as mean, min(amount) as low, max(amount) as high, count(amount) as n, count(*) as rows from purchases group by user_id";
        let mut output = aggregate_output_schema("total", "n");
        output.columns.truncate(1);
        for name in ["total", "mean", "low", "high", "n", "rows"] {
            output.columns.push(ColumnSchema {
                name: name.to_string(),
                data_type: if matches!(name, "n" | "rows") {
                    SqlDataType::Int64
                } else {
                    SqlDataType::Float64
                },
                nullable: !matches!(name, "n" | "rows"),
            });
        }
        let input_schema = catalog_input_relation_schema(&catalog).unwrap();
        let plan = validate_supported_view_sql(sql, &catalog).unwrap();
        let logical = lower_supported_view_sql_to_logical_plan(sql, &catalog, &output).unwrap();
        let mut runtime = SingleKeySumCountRuntime::new_with_plan(
            test_identity(sql),
            catalog.clone(),
            input_schema,
            output,
            sql.to_string(),
            plan,
            logical,
        )
        .unwrap();
        for (epoch, amounts, weights, expected) in [
            (
                1,
                vec![Some(1.25), Some(1.25), Some(3.5), None],
                vec![1, 1, 1, 1],
                serde_json::json!([6.0, 2.0, 1.25, 3.5, 3, 4]),
            ),
            (
                2,
                vec![Some(1.25), Some(2.75)],
                vec![-1, 1],
                serde_json::json!([7.5, 2.5, 1.25, 3.5, 3, 4]),
            ),
            (
                3,
                vec![Some(1.25), Some(2.75), Some(3.5)],
                vec![-1, -1, -1],
                serde_json::json!([null, null, null, null, 0, 1]),
            ),
        ] {
            let mut input = purchases_input(&catalog, epoch - 1, epoch, "alice", 0);
            input.batches = vec![RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("user_id", DataType::Utf8, false),
                    Field::new("amount", DataType::Float64, true),
                    Field::new("delta", DataType::Int64, false),
                ])),
                vec![
                    Arc::new(StringArray::from(vec!["alice"; amounts.len()])),
                    Arc::new(arrow::array::Float64Array::from(amounts)),
                    Arc::new(Int64Array::from(weights)),
                ],
            )
            .unwrap()];
            runtime
                .apply_changes(
                    epoch,
                    EpochIdempotencyKey::new(format!("float-{epoch}")).unwrap(),
                    vec![input],
                )
                .unwrap();
            let rows = runtime.published_output.net_rows().unwrap();
            assert_eq!(rows.len(), 1);
            let actual = rows[0].value.as_json();
            for (index, name) in ["total", "mean", "low", "high", "n", "rows"]
                .iter()
                .enumerate()
            {
                assert_eq!(&actual[*name], &expected[index], "{name} at epoch {epoch}");
            }
            let checkpoint = runtime.checkpoint().unwrap();
            runtime = SingleKeySumCountRuntime::restore(checkpoint.clone()).unwrap();
            assert_eq!(runtime.checkpoint().unwrap(), checkpoint);
        }
        let before = runtime.checkpoint().unwrap();
        for amount in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let mut input = purchases_input(&catalog, 3, 4, "alice", 0);
            input.batches = vec![RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("user_id", DataType::Utf8, false),
                    Field::new("amount", DataType::Float64, true),
                    Field::new("delta", DataType::Int64, false),
                ])),
                vec![
                    Arc::new(StringArray::from(vec!["alice"])),
                    Arc::new(arrow::array::Float64Array::from(vec![Some(amount)])),
                    Arc::new(Int64Array::from(vec![1])),
                ],
            )
            .unwrap()];
            assert!(runtime
                .apply_changes(
                    4,
                    EpochIdempotencyKey::new("invalid-float").unwrap(),
                    vec![input]
                )
                .is_err());
            assert_eq!(runtime.checkpoint().unwrap(), before);
        }
    }

    #[test]
    fn float_accumulator_rejects_nonfinite_without_mutating_state() {
        assert!(aggregate_sum_as_f64(&serde_json::json!("NaN")).is_err());
        assert!(aggregate_sum_as_f64(&serde_json::json!("inf")).is_err());
        let mut row = Map::new();
        row.insert("sum".into(), zeroed_filtered_avg_value());
        update_filtered_avg_value(&mut row, "sum", 1.25, 2).unwrap();
        let before = row.clone();
        assert!(update_filtered_avg_value(&mut row, "sum", f64::INFINITY, 1).is_err());
        assert_eq!(row, before);
        update_filtered_avg_value(&mut row, "sum", 1.25, -2).unwrap();
        assert_eq!(row["sum"]["sum"].as_f64(), Some(0.0));
        assert_eq!(row["sum"]["count"].as_i64(), Some(0));
    }

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

    #[test]
    fn checkpoint_budget_rolls_back_and_lower_restore_limit_rejects() {
        for sql in [
            "select user_id, sum(amount) as total_amount, count(*) as event_count from purchases group by user_id",
            "select user_id, sum(amount + 1) as total_amount, count(*) as event_count from purchases group by user_id",
        ] {
            let catalog = purchases_catalog();
            let output = aggregate_output_schema("total_amount", "event_count");
            let plan = validate_supported_view_sql(sql, &catalog).unwrap();
            let logical = lower_supported_view_sql_to_logical_plan(sql, &catalog, &output).unwrap();
            let mut runtime = SingleKeySumCountRuntime::new_with_logical_plan(
                test_identity(sql), catalog.clone(), catalog_input_relation_schema(&catalog).unwrap(),
                output, sql.into(), plan, logical,
            ).unwrap();
            let empty = runtime.checkpoint().unwrap();
            let limits = RuntimeResourceLimits {
                max_checkpoint_bytes: empty.state_payload.as_ref().unwrap().payload.len(),
                ..RuntimeResourceLimits::default()
            };
            let mut bounded = SingleKeySumCountRuntime::restore_with_limits(empty.clone(), limits).unwrap();
            let key = EpochIdempotencyKey::new("budget-1").unwrap();
            let input = purchases_input(&catalog, 0, 1, "alice", 10);
            assert!(bounded.apply_changes(1, key.clone(), vec![input.clone()]).is_err());
            assert_eq!(bounded.checkpoint().unwrap(), empty);
            runtime.apply_changes(1, key, vec![input]).unwrap();
            assert!(SingleKeySumCountRuntime::restore_with_limits(runtime.checkpoint().unwrap(), limits).is_err());
        }
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

    #[test]
    fn keyless_global_sum_and_nonnumeric_count_empty_retract_restart() {
        use arrow::array::Float64Array;
        for (expression, grouped, numeric) in [
            ("sum(amount)", false, true),
            ("min(amount)", false, true),
            ("max(amount)", false, true),
            ("avg(amount)", false, true),
            ("count(*)", false, false),
            ("count(*)", true, false),
        ] {
            let mut catalog = purchases_catalog();
            catalog.relation_schema.primary_key_column_ids.clear();
            catalog.incremental_adapter.adapter_id =
                velorix_core::relation::CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.into();
            let physical = if numeric {
                ArrowPhysicalTypeV1::Float64
            } else {
                ArrowPhysicalTypeV1::TimestampNanosecond { timezone: None }
            };
            catalog.relation_schema.columns[1].physical_arrow_type = physical;
            catalog.relation_schema.columns[1].logical_type = if numeric {
                VelorixLogicalTypeV1::Float64
            } else {
                VelorixLogicalTypeV1::Timestamp { timezone: None }
            };
            catalog.schema_fingerprint =
                SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema).unwrap();
            catalog.incremental_relation.schema_fingerprint = catalog.schema_fingerprint.clone();
            let sql = if grouped {
                format!("select amount, user_id, {expression} as result from purchases group by amount, user_id")
            } else {
                format!("select {expression} as result from purchases")
            };
            let mut output = aggregate_output_schema("result", "unused");
            output.primary_key.clear();
            output.columns.clear();
            if grouped {
                output.primary_key = vec!["amount".into(), "user_id".into()];
                output.columns.extend([
                    ColumnSchema {
                        name: "amount".into(),
                        data_type: SqlDataType::Timestamp { timezone: None },
                        nullable: false,
                    },
                    ColumnSchema {
                        name: "user_id".into(),
                        data_type: SqlDataType::Utf8,
                        nullable: false,
                    },
                ]);
            }
            output.columns.push(ColumnSchema {
                name: "result".into(),
                data_type: if numeric {
                    SqlDataType::Float64
                } else {
                    SqlDataType::Int64
                },
                nullable: numeric,
            });
            let logical =
                lower_supported_view_sql_to_logical_plan(&sql, &catalog, &output).unwrap();
            let mut runtime = create_standing_runtime_with_logical_plan_and_catalogs(
                &test_identity(&sql),
                std::slice::from_ref(&catalog),
                logical,
                &[catalog_input_relation_schema(&catalog).unwrap()],
                &[output],
            )
            .unwrap();
            let view = ScopedViewId {
                tenant_id: "tenant-a".into(),
                program_id: "program-purchases".into(),
                view_id: "purchases_by_user".into(),
            };
            let check_empty = |runtime: &dyn StandingProgramRuntime| {
                let page = runtime
                    .materialized_view_page(view.clone(), SnapshotPageRequest::default())
                    .unwrap();
                let batch = &page.batches[0];
                assert_eq!(batch.num_rows(), usize::from(!grouped));
                if !grouped {
                    if numeric {
                        assert!(batch.column(0).is_null(0));
                    } else {
                        assert_eq!(
                            batch
                                .column(0)
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap()
                                .value(0),
                            0
                        );
                    }
                }
            };
            check_empty(runtime.as_ref());
            let empty = runtime.checkpoint().unwrap();
            runtime = restore_standing_runtime(empty.clone()).unwrap();
            assert_eq!(runtime.checkpoint().unwrap(), empty);
            check_empty(runtime.as_ref());
            let input = |offset, weight| {
                let mut input = purchases_input(&catalog, offset, offset + 1, "same", 0);
                let (data_type, values): (DataType, arrow::array::ArrayRef) = if numeric {
                    (DataType::Float64, Arc::new(Float64Array::from(vec![2.5])))
                } else {
                    (
                        DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
                        Arc::new(TimestampNanosecondArray::from(vec![17])),
                    )
                };
                input.batches = vec![RecordBatch::try_new(
                    Arc::new(Schema::new(vec![
                        Field::new("user_id", DataType::Utf8, false),
                        Field::new("amount", data_type, false),
                        Field::new("delta", DataType::Int64, false),
                    ])),
                    vec![
                        Arc::new(StringArray::from(vec!["same"])),
                        values,
                        Arc::new(Int64Array::from(vec![weight])),
                    ],
                )
                .unwrap()];
                input
            };
            let inserted = runtime
                .apply_changes(
                    1,
                    EpochIdempotencyKey::new("global-1").unwrap(),
                    vec![input(0, 2)],
                )
                .unwrap();
            let values = inserted.output_batches[0].batches[0].column(if grouped { 2 } else { 0 });
            if numeric {
                assert_eq!(
                    values
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap()
                        .value(0),
                    if expression == "sum(amount)" {
                        5.0
                    } else {
                        2.5
                    }
                );
            } else {
                assert_eq!(
                    values
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(0),
                    2
                );
            }
            runtime = restore_standing_runtime(runtime.checkpoint().unwrap()).unwrap();
            runtime
                .apply_changes(
                    2,
                    EpochIdempotencyKey::new("global-2").unwrap(),
                    vec![input(1, -2)],
                )
                .unwrap();
            check_empty(runtime.as_ref());
            let empty = runtime.checkpoint().unwrap();
            runtime = restore_standing_runtime(empty.clone()).unwrap();
            check_empty(runtime.as_ref());
            assert!(runtime
                .apply_changes(2, EpochIdempotencyKey::new("global-2").unwrap(), vec![])
                .unwrap()
                .output_deltas
                .is_empty());
            assert_eq!(runtime.checkpoint().unwrap(), empty);
        }
    }

    #[test]
    fn keyless_nullable_compound_aggregates_retract_replay_and_restart() {
        use arrow::array::{Date32Array, Float64Array};
        use serde_json::json;
        type Row = (
            Option<&'static str>,
            Option<i32>,
            Option<i64>,
            Option<f64>,
            i64,
        );
        let mut catalog = purchases_catalog();
        catalog.relation_schema.primary_key_column_ids.clear();
        catalog.incremental_adapter.adapter_id =
            velorix_core::relation::CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.into();
        catalog.relation_schema.columns[0].nullable = true;
        catalog.relation_schema.columns[1].nullable = true;
        catalog.relation_schema.columns.extend([
            RelationColumnV1 {
                column_id: "observed_day".into(),
                name: "observed_day".into(),
                logical_type: VelorixLogicalTypeV1::Date,
                physical_arrow_type: ArrowPhysicalTypeV1::Date32,
                nullable: true,
                ordinal: 3,
                semantic_role: RelationSemanticRoleV1::Metadata,
            },
            RelationColumnV1 {
                column_id: "metric".into(),
                name: "metric".into(),
                logical_type: VelorixLogicalTypeV1::Float64,
                physical_arrow_type: ArrowPhysicalTypeV1::Float64,
                nullable: true,
                ordinal: 4,
                semantic_role: RelationSemanticRoleV1::Value,
            },
        ]);
        catalog.schema_fingerprint =
            SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema).unwrap();
        catalog.incremental_relation.schema_fingerprint = catalog.schema_fingerprint.clone();
        let sql = "select observed_day, user_id, count(*) as samples, count(amount) as measured, sum(amount) as total, avg(metric) as mean, min(amount) as smallest, max(amount) as largest from purchases group by observed_day, user_id";
        let mut output = aggregate_output_schema("total", "samples");
        output.primary_key = vec!["observed_day".into(), "user_id".into()];
        output.columns = [
            ("observed_day", SqlDataType::Date, true),
            ("user_id", SqlDataType::Utf8, true),
            ("samples", SqlDataType::Int64, false),
            ("measured", SqlDataType::Int64, false),
            ("total", SqlDataType::Int64, true),
            ("mean", SqlDataType::Float64, true),
            ("smallest", SqlDataType::Int64, true),
            ("largest", SqlDataType::Int64, true),
        ]
        .into_iter()
        .map(|(name, data_type, nullable)| ColumnSchema {
            name: name.into(),
            data_type,
            nullable,
        })
        .collect();
        let logical = lower_supported_view_sql_to_logical_plan(sql, &catalog, &output).unwrap();
        let mut runtime = create_standing_runtime_with_logical_plan_and_catalogs(
            &test_identity(sql),
            std::slice::from_ref(&catalog),
            logical,
            &[catalog_input_relation_schema(&catalog).unwrap()],
            &[output],
        )
        .unwrap();
        let input = |offset: u64, rows: &[Row]| {
            let mut input =
                purchases_input(&catalog, offset, offset + rows.len() as u64, "unused", 0);
            input.batches = vec![RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("user_id", DataType::Utf8, true),
                    Field::new("amount", DataType::Int64, true),
                    Field::new("delta", DataType::Int64, false),
                    Field::new("observed_day", DataType::Date32, true),
                    Field::new("metric", DataType::Float64, true),
                ])),
                vec![
                    Arc::new(StringArray::from(
                        rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        rows.iter().map(|r| r.4).collect::<Vec<_>>(),
                    )),
                    Arc::new(Date32Array::from(
                        rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                    )),
                    Arc::new(Float64Array::from(
                        rows.iter().map(|r| r.3).collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap()];
            input
        };
        let nullable_group = |commit: &EpochCommit| {
            let batch = &commit.output_batches[0].batches[0];
            let row = (0..batch.num_rows())
                .find(|row| batch.column(0).is_null(*row))
                .unwrap();
            (2..8)
                .map(|column| {
                    let array = batch.column(column);
                    if array.is_null(row) {
                        Value::Null
                    } else if column == 5 {
                        json!(array
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .unwrap()
                            .value(row))
                    } else {
                        json!(array
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .value(row))
                    }
                })
                .collect::<Vec<_>>()
        };
        let key = |epoch| EpochIdempotencyKey::new(format!("keyless-{epoch}")).unwrap();
        let commit = runtime
            .apply_changes(
                1,
                key(1),
                vec![input(
                    0,
                    &[
                        (None, None, Some(2), Some(1.5), 2),
                        (None, None, Some(6), Some(4.5), 1),
                        (None, None, None, None, 1),
                        (Some("z"), Some(1), Some(9), Some(10.0), 1),
                    ],
                )],
            )
            .unwrap();
        assert_eq!(
            nullable_group(&commit),
            vec![
                json!(4),
                json!(3),
                json!(10),
                json!(2.5),
                json!(2),
                json!(6)
            ]
        );
        let checkpoint = runtime.checkpoint().unwrap();
        let mut restored = restore_standing_runtime(checkpoint.clone()).unwrap();
        assert_eq!(restored.checkpoint().unwrap(), checkpoint);
        let replay = restored.apply_changes(1, key(1), vec![]).unwrap();
        assert!(replay.output_deltas.is_empty());
        assert_eq!(restored.checkpoint().unwrap(), checkpoint);
        let commit = restored
            .apply_changes(
                2,
                key(2),
                vec![input(
                    4,
                    &[
                        (None, None, Some(2), Some(1.5), -1),
                        (Some("z"), Some(0), Some(7), Some(8.0), 1),
                    ],
                )],
            )
            .unwrap();
        assert_eq!(
            nullable_group(&commit),
            vec![json!(3), json!(2), json!(8), json!(3.0), json!(2), json!(6)]
        );
        let checkpoint = restored.checkpoint().unwrap();
        assert!(restored
            .apply_changes(
                3,
                key(3),
                vec![input(6, &[(None, None, Some(2), Some(1.5), -4)])]
            )
            .is_err());
        assert_eq!(restored.checkpoint().unwrap(), checkpoint);
        let commit = restored
            .apply_changes(
                3,
                key(3),
                vec![input(
                    6,
                    &[
                        (None, None, Some(2), Some(1.5), -1),
                        (None, None, Some(6), Some(4.5), -1),
                    ],
                )],
            )
            .unwrap();
        assert_eq!(
            nullable_group(&commit),
            vec![
                json!(1),
                json!(0),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null
            ]
        );
        let checkpoint = restored.checkpoint().unwrap();
        assert_eq!(
            restore_standing_runtime(checkpoint.clone())
                .unwrap()
                .checkpoint()
                .unwrap(),
            checkpoint
        );
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
