//! Analytic window frame runtime (Phase 8.1).
//!
//! Materializes bounded ROWS window frames with navigation functions
//! (LAG/LEAD/FIRST_VALUE/LAST_VALUE/NTH_VALUE) over one partition column
//! and one non-null sortable order column with the primary key as the
//! deterministic tie-breaker. The frame is `ROWS BETWEEN k PRECEDING AND
//! k FOLLOWING` with constant k; outputs are computed from the per-partition
//! sorted row order each epoch (bounded affected rows per insert/retract
//! because the frame is bounded).

use super::*;

pub struct AnalyticWindowFrameRuntime {
    identity: StandingProgramIdentity,
    catalog: VelorixRelationCatalogV1,
    input_schema: RelationSchema,
    output_schema: RelationSchema,
    view_sql: String,
    plan: SupportedAnalyticWindowFramePlanV1,
    logical_plan: VelorixLogicalViewPlanV1,
    rows: BTreeMap<String, AnalyticFrameRow>,
    published_output: DeltaBatch,
    input_frontiers: Vec<RelationFrontier>,
    input_event_time_frontiers: Vec<InputEventTimeFrontier>,
    applied_epochs: BTreeMap<String, LogicalEpoch>,
    logical_epoch: LogicalEpoch,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AnalyticFrameRow {
    key: Value,
    partition_value: Value,
    order_value: Value,
    values: BTreeMap<String, Value>,
    weight: i64,
}

impl AnalyticWindowFrameRuntime {
    pub fn new_with_logical_plan(
        identity: StandingProgramIdentity,
        catalog: VelorixRelationCatalogV1,
        input_schema: RelationSchema,
        output_schema: RelationSchema,
        view_sql: String,
        plan: SupportedAnalyticWindowFramePlanV1,
        logical_plan: VelorixLogicalViewPlanV1,
    ) -> Result<Self, StandingProgramRuntimeError> {
        identity.validate()?;
        validate_builtin_runtime_identity(&identity)?;
        validate_view_sql_hash(&identity, view_sql.as_str())?;
        validate_logical_view_plan(&logical_plan).map_err(|_| {
            StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "logical_analytic_window_frame_plan",
            }
        })?;
        validate_analytic_frame_contract(&catalog, &input_schema, &output_schema, &plan)?;
        let compiled = validate_supported_analytic_window_frame_sql(view_sql.as_str(), &catalog)
            .map_err(|_| StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "analytic_window_frame_plan",
            })?;
        if compiled != plan {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "analytic_window_frame_plan",
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
            rows: BTreeMap::new(),
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

    fn key_column(&self) -> Result<&RelationColumnV1, StandingProgramRuntimeError> {
        catalog_primary_key_column(&self.catalog)
    }

    fn order_column(&self) -> Result<&RelationColumnV1, StandingProgramRuntimeError> {
        catalog_column_by_id(&self.catalog, &self.plan.order_column_id)
    }

    fn recompute_all_outputs(&self) -> Result<DeltaBatch, StandingProgramRuntimeError> {
        let key_column = self.key_column()?;
        let order_column = self.order_column()?;
        let mut partitions: BTreeMap<String, Vec<&AnalyticFrameRow>> = BTreeMap::new();
        for row in self.rows.values() {
            partitions
                .entry(canonical_json(&row.partition_value))
                .or_default()
                .push(row);
        }
        let mut records = Vec::new();
        for rows in partitions.into_values() {
            let mut rows = rows;
            rows.sort_by(|left, right| {
                let ordering =
                    compare_row_number_values(order_column, &left.order_value, &right.order_value);
                let ordering = if self.plan.order_descending {
                    ordering.reverse()
                } else {
                    ordering
                };
                ordering.then_with(|| compare_row_number_values(key_column, &left.key, &right.key))
            });
            for (index, row) in rows.iter().enumerate() {
                let frame_start = index.saturating_sub(self.plan.frame_preceding as usize);
                let frame_end = (index + self.plan.frame_following as usize + 1).min(rows.len());
                let frame_slice = &rows[frame_start..frame_end];
                let Some(value) =
                    self.navigation_value_with_sorted_partition(row, &rows, index, frame_slice)
                else {
                    continue;
                };
                let mut output = Map::new();
                output.insert(self.plan.output_column_id.clone(), value);
                let output_key = if let Some(output_key_input_column_id) =
                    &self.plan.output_key_input_column_id
                {
                    row.values
                        .get(output_key_input_column_id)
                        .cloned()
                        .ok_or_else(invalid_runtime_state)?
                } else {
                    row.key.clone()
                };
                records.push(DeltaRecord::new(
                    DeltaKey::from_json(output_key),
                    DeltaValue::from_json(Value::Object(output)),
                    1,
                ));
            }
        }
        Ok(DeltaBatch::from_records(records))
    }

    /// Computes the navigation function result for the current row using the
    /// already-sorted partition. LAG/LEAD use the full partition for global
    /// position lookup. FIRST_VALUE/LAST_VALUE/NTH_VALUE use the bounded
    /// frame slice to respect ROWS BETWEEN semantics.
    fn navigation_value_with_sorted_partition(
        &self,
        _current: &AnalyticFrameRow,
        sorted_partition: &[&AnalyticFrameRow],
        index: usize,
        frame: &[&AnalyticFrameRow],
    ) -> Option<Value> {
        let value_of =
            |row: &AnalyticFrameRow| row.values.get(frame_value_column_id(&self.plan)).cloned();
        match &self.plan.function {
            WindowNavigationFunctionV1::Lag { offset, .. } => {
                if index < *offset as usize {
                    return None;
                }
                let target = index - *offset as usize;
                value_of(sorted_partition.get(target)?)
            }
            WindowNavigationFunctionV1::Lead { offset, .. } => {
                let target = index + *offset as usize;
                value_of(sorted_partition.get(target)?)
            }
            // FIRST_VALUE/LAST_VALUE/NTH_VALUE operate within the bounded frame
            WindowNavigationFunctionV1::FirstValue { .. } => value_of(frame.first()?),
            WindowNavigationFunctionV1::LastValue { .. } => value_of(frame.last()?),
            WindowNavigationFunctionV1::NthValue { n, .. } => {
                if *n == 0 || *n > frame.len() as u64 {
                    return None;
                }
                value_of(frame.get(*n as usize - 1)?)
            }
        }
    }
}

impl StandingProgramRuntime for AnalyticWindowFrameRuntime {
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

        let mut next_frontiers = self.input_frontiers.clone();
        let mut next_event_time_frontiers = self.input_event_time_frontiers.clone();
        // Clone rows for staged mutation — if any step fails, self.rows is restored.
        let original_rows = self.rows.clone();
        for input in &input_changes {
            validate_input_matches_schema(input, &self.input_schema, "analytic_frame_input")?;
            let delta =
                if let Some(empty_delta) = published_input_empty_delta(input, &self.catalog)? {
                    empty_delta
                } else {
                    let columns = analytic_frame_input_column_ids(&self.plan, &self.catalog);
                    arrow_record_batches_to_key_multi_value_delta_batch(
                        &self.catalog,
                        &input.relation_id,
                        &input.relation_version,
                        &input.schema_fingerprint,
                        std::slice::from_ref(&self.plan.key_column_id),
                        &columns,
                        &input.batches,
                    )
                    .map_err(|_| {
                        StandingProgramRuntimeError::InvalidProgramIdentity {
                            field: "analytic_frame_input_batch",
                        }
                    })?
                };
            for record in delta.net_rows().map_err(|_| invalid_runtime_state())? {
                let key = canonical_json(record.key.as_json());
                let entry = self
                    .rows
                    .entry(key.clone())
                    .or_insert_with(|| AnalyticFrameRow {
                        key: record.key.as_json().clone(),
                        partition_value: Value::Null,
                        order_value: Value::Null,
                        values: BTreeMap::new(),
                        weight: 0,
                    });
                entry.weight = entry
                    .weight
                    .checked_add(record.weight)
                    .ok_or_else(invalid_runtime_state)?;
                if entry.weight < 0 {
                    self.rows = original_rows;
                    return Err(invalid_runtime_state());
                }
                if entry.weight == 0 {
                    self.rows.remove(&key);
                    continue;
                }
                let value = record
                    .value
                    .as_json()
                    .as_object()
                    .ok_or_else(invalid_runtime_state)?;
                entry.partition_value = value
                    .get(&self.plan.partition_column_id)
                    .cloned()
                    .ok_or_else(invalid_runtime_state)?;
                entry.order_value = value
                    .get(&self.plan.order_column_id)
                    .cloned()
                    .ok_or_else(invalid_runtime_state)?;
                entry.values = value
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect();
            }
            advance_input_frontier(&mut next_frontiers, input)?;
            advance_input_event_time_frontier(&mut next_event_time_frontiers, input)?;
        }
        // All subsequent operations may fail. Rollback rows on any error.
        let next_output = match self.recompute_all_outputs() {
            Ok(output) => output,
            Err(e) => {
                self.rows = original_rows;
                return Err(e);
            }
        };
        let output_delta = match self.published_output.diff(&next_output) {
            Ok(delta) => delta,
            Err(_) => {
                self.rows = original_rows;
                return Err(invalid_runtime_state());
            }
        };
        // Validate output before commit — use next_output (local), not self.published_output.
        // Rollback rows if serialization fails.
        let output_batches =
            match materialized_generic_delta_to_record_batch(&self.output_schema, &next_output) {
                Ok(batch) => vec![ViewOutputBatch {
                    view_id: self.identity.view_ids[0].clone(),
                    schema_fingerprint: self.output_schema_fingerprint(),
                    batches: vec![batch],
                }],
                Err(e) => {
                    self.rows = original_rows;
                    return Err(e);
                }
            };
        // Commit staged state — only after all validations pass
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
        let payload = AnalyticFrameCheckpointPayload {
            schema_version: CHECKPOINT_PAYLOAD_SCHEMA_VERSION,
            runtime_kind: ANALYTIC_FRAME_RUNTIME_KIND.to_string(),
            catalog: self.catalog.clone(),
            input_schema: self.input_schema.clone(),
            output_schema: self.output_schema.clone(),
            view_sql: self.view_sql.clone(),
            plan: self.plan.clone(),
            logical_plan: self.logical_plan.clone(),
            input_frontiers: self.input_frontiers.clone(),
            input_event_time_frontiers: self.input_event_time_frontiers.clone(),
            rows: self.rows.clone(),
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
        let payload =
            velorix_core::arrow_state::to_state(&payload).map_err(|_| invalid_checkpoint())?;
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
        let payload: AnalyticFrameCheckpointPayload =
            velorix_core::arrow_state::from_state(&state_payload.payload)
                .map_err(|_| invalid_checkpoint())?;
        if payload.schema_version != CHECKPOINT_PAYLOAD_SCHEMA_VERSION
            || payload.runtime_kind != ANALYTIC_FRAME_RUNTIME_KIND
        {
            return Err(invalid_checkpoint());
        }
        validate_analytic_frame_contract(
            &payload.catalog,
            &payload.input_schema,
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
        let compiled = validate_supported_analytic_window_frame_sql(
            payload.view_sql.as_str(),
            &payload.catalog,
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
            catalog: payload.catalog,
            input_schema: payload.input_schema,
            output_schema: payload.output_schema,
            view_sql: payload.view_sql,
            plan: payload.plan,
            logical_plan: payload.logical_plan,
            rows: payload.rows,
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
struct AnalyticFrameCheckpointPayload {
    schema_version: u32,
    runtime_kind: String,
    catalog: VelorixRelationCatalogV1,
    input_schema: RelationSchema,
    output_schema: RelationSchema,
    view_sql: String,
    plan: SupportedAnalyticWindowFramePlanV1,
    logical_plan: VelorixLogicalViewPlanV1,
    input_frontiers: Vec<RelationFrontier>,
    input_event_time_frontiers: Vec<InputEventTimeFrontier>,
    rows: BTreeMap<String, AnalyticFrameRow>,
    published_output: DeltaBatch,
    applied_epochs: Vec<GenericAppliedEpoch>,
    logical_epoch: LogicalEpoch,
}

pub(super) const ANALYTIC_FRAME_RUNTIME_KIND: &str = "analytic_window_frame";

fn analytic_frame_input_column_ids(
    plan: &SupportedAnalyticWindowFramePlanV1,
    catalog: &VelorixRelationCatalogV1,
) -> Vec<String> {
    let mut columns = BTreeSet::new();
    columns.insert(plan.partition_column_id.clone());
    columns.insert(plan.order_column_id.clone());
    columns.insert(frame_value_column_id(plan).to_string());
    if let Some(output_key_input_column_id) = &plan.output_key_input_column_id {
        columns.insert(output_key_input_column_id.clone());
    }
    let _ = catalog;
    columns.into_iter().collect()
}

fn frame_value_column_id(plan: &SupportedAnalyticWindowFramePlanV1) -> &str {
    match &plan.function {
        WindowNavigationFunctionV1::Lag {
            value_column_id, ..
        }
        | WindowNavigationFunctionV1::Lead {
            value_column_id, ..
        }
        | WindowNavigationFunctionV1::FirstValue { value_column_id }
        | WindowNavigationFunctionV1::LastValue { value_column_id }
        | WindowNavigationFunctionV1::NthValue {
            value_column_id, ..
        } => value_column_id,
    }
}

fn validate_analytic_frame_contract(
    catalog: &VelorixRelationCatalogV1,
    input_schema: &RelationSchema,
    output_schema: &RelationSchema,
    plan: &SupportedAnalyticWindowFramePlanV1,
) -> Result<(), StandingProgramRuntimeError> {
    if plan.schema_version != 1 {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "analytic_window_frame_contract",
        });
    }
    if plan.input_relation_id != catalog.relation_schema.relation_id {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "analytic_window_frame_contract",
        });
    }
    catalog_column(catalog, &plan.key_column_id)?;
    catalog_column(catalog, &plan.partition_column_id)?;
    catalog_column(catalog, &plan.order_column_id)?;
    let key = catalog_primary_key_column(catalog)?;
    let expected_input = catalog_input_relation_schema(catalog).map_err(|_| {
        StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "analytic_window_frame_input_schema",
        }
    })?;
    if expected_input != *input_schema {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "analytic_window_frame_input_schema",
        });
    }
    let output_key_name = if plan.output_key_column_id.is_empty() {
        key.name.clone()
    } else {
        plan.output_key_column_id.clone()
    };
    let [output_key, value_column] = output_schema.columns.as_slice() else {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "analytic_window_frame_output_schema",
        });
    };
    if output_key.name != output_key_name
        || output_key.nullable
        || output_key.data_type != sql_type_from_catalog_column(key)?
    {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "analytic_window_frame_output_schema",
        });
    }
    if value_column.name != plan.output_column_id {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "analytic_window_frame_output_schema",
        });
    }
    let value_type =
        sql_type_from_catalog_column(catalog_column(catalog, frame_value_column_id(plan))?)?;
    if value_column.data_type != value_type {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "analytic_window_frame_output_schema",
        });
    }
    Ok(())
}

use velorix_core::view_plan::{
    validate_supported_ordered_window_sql, OrderedWindowFunctionV1, SupportedOrderedWindowPlanV1,
};

pub(super) const ORDERED_WINDOW_RUNTIME_KIND: &str = "ordered_window";

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct OrderedWindowRow {
    key: Value,
    values: Map<String, Value>,
    #[serde(
        default = "ordered_single_occurrence",
        skip_serializing_if = "ordered_is_single_occurrence"
    )]
    multiplicity: i64,
}

fn ordered_single_occurrence() -> i64 {
    1
}
fn ordered_is_single_occurrence(count: &i64) -> bool {
    *count == 1
}

type OrderedPartition = BTreeMap<String, OrderedWindowRow>;

/// Native ordered state: all ages remain mutable. Only partitions touched by an
/// epoch are sorted/evaluated; queries read the committed materialized output.
/// ponytail: partition updates cost O(n log n + n * outputs), with O(n) retained
/// state; publishing snapshots/delta consolidation additionally scans the output.
/// Add an ordered prefix index/spill when large partitions require it.
#[derive(Clone)]
pub struct OrderedWindowRuntime {
    limits: RuntimeResourceLimits,
    identity: StandingProgramIdentity,
    catalog: VelorixRelationCatalogV1,
    input_schema: RelationSchema,
    output_schema: RelationSchema,
    view_sql: String,
    plan: SupportedOrderedWindowPlanV1,
    logical_plan: VelorixLogicalViewPlanV1,
    partitions: BTreeMap<String, OrderedPartition>,
    key_partitions: BTreeMap<String, String>,
    partition_outputs: BTreeMap<String, DeltaBatch>,
    published_output: DeltaBatch,
    input_frontiers: Vec<RelationFrontier>,
    input_event_time_frontiers: Vec<InputEventTimeFrontier>,
    applied_epochs: BTreeMap<String, LogicalEpoch>,
    logical_epoch: LogicalEpoch,
}

impl OrderedWindowRuntime {
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_logical_plan_and_limits(
        identity: StandingProgramIdentity,
        catalog: VelorixRelationCatalogV1,
        input_schema: RelationSchema,
        output_schema: RelationSchema,
        view_sql: String,
        plan: SupportedOrderedWindowPlanV1,
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
        runtime.checkpoint()?;
        Ok(runtime)
    }

    fn row_key(&self, values: &Map<String, Value>) -> Result<Value, StandingProgramRuntimeError> {
        if self.plan.schema_version == 3 {
            return Ok(Value::Object(values.clone()));
        }
        let keys = self
            .catalog
            .relation_schema
            .primary_key_column_ids
            .iter()
            .map(|id| values.get(id).cloned().ok_or_else(invalid_runtime_state))
            .collect::<Result<Vec<_>, _>>()?;
        if self.plan.schema_version == 1 {
            keys.into_iter().next().ok_or_else(invalid_runtime_state)
        } else {
            Ok(Value::Array(keys))
        }
    }

    fn row_identity(&self, key: &Value) -> String {
        if self.plan.schema_version == 1 {
            canonical_json(key)
        } else {
            velorix_core::delta::encode_key_ordered(key)
                .as_bytes()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect()
        }
    }

    pub fn new_with_logical_plan(
        identity: StandingProgramIdentity,
        catalog: VelorixRelationCatalogV1,
        input_schema: RelationSchema,
        output_schema: RelationSchema,
        view_sql: String,
        plan: SupportedOrderedWindowPlanV1,
        logical_plan: VelorixLogicalViewPlanV1,
    ) -> Result<Self, StandingProgramRuntimeError> {
        identity.validate()?;
        validate_builtin_runtime_identity(&identity)?;
        validate_view_sql_hash(&identity, &view_sql)?;
        validate_logical_view_plan(&logical_plan).map_err(|_| invalid_runtime_state())?;
        if validate_supported_ordered_window_sql(&view_sql, &catalog)
            .map_err(|_| invalid_runtime_state())?
            != plan
            || !matches!(&logical_plan.execution,
                VelorixLogicalViewExecutionV1::OrderedWindows { plan: logical } if **logical == plan)
        {
            return Err(invalid_runtime_state());
        }
        validate_ordered_window_contract(&catalog, &input_schema, &output_schema, &plan)?;
        let expected = velorix_core::view_plan::lower_supported_ordered_window_sql_to_logical_plan(
            &view_sql,
            &catalog,
            &output_schema,
        )
        .map_err(|_| invalid_runtime_state())?;
        if logical_plan != expected {
            return Err(invalid_runtime_state());
        }
        Ok(Self {
            identity,
            catalog,
            input_schema,
            output_schema,
            view_sql,
            plan,
            logical_plan,
            partitions: BTreeMap::new(),
            limits: RuntimeResourceLimits::default(),
            key_partitions: BTreeMap::new(),
            partition_outputs: BTreeMap::new(),
            published_output: DeltaBatch::default(),
            input_frontiers: Vec::new(),
            input_event_time_frontiers: Vec::new(),
            applied_epochs: BTreeMap::new(),
            logical_epoch: 0,
        })
    }

    fn partition_id(
        &self,
        values: &Map<String, Value>,
    ) -> Result<String, StandingProgramRuntimeError> {
        let values = self
            .plan
            .partition_column_ids
            .iter()
            .map(|id| {
                let value = values.get(id).cloned().ok_or_else(invalid_runtime_state)?;
                // SQL equality groups both signs of floating-point zero.
                if catalog_column_by_id(&self.catalog, id)?.physical_arrow_type
                    == ArrowPhysicalTypeV1::Float64
                    && value.as_f64() == Some(0.0)
                {
                    Ok(Value::from(0.0))
                } else {
                    Ok(value)
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(canonical_json(&Value::Array(values)))
    }

    fn compare_order_values(
        &self,
        column: &RelationColumnV1,
        left: &Value,
        right: &Value,
    ) -> Ordering {
        let nulls_first = self
            .plan
            .order_nulls_first
            .unwrap_or(self.plan.order_descending);
        match (left.is_null(), right.is_null()) {
            (true, true) => Ordering::Equal,
            (true, false) => {
                if nulls_first {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            (false, true) => {
                if nulls_first {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (false, false) => {
                let order = compare_row_number_values(column, left, right);
                if self.plan.order_descending {
                    order.reverse()
                } else {
                    order
                }
            }
        }
    }

    fn clone_output_value(
        &self,
        name: &str,
        value: &Value,
        expansion: &mut (usize, usize),
    ) -> Result<Value, StandingProgramRuntimeError> {
        // Ordered scalar outputs never contain JSON containers; charge before cloning strings.
        let payload_bytes = match value {
            Value::String(value) => value.len(),
            Value::Null | Value::Bool(_) | Value::Number(_) => 0,
            _ => return Err(invalid_runtime_state()),
        };
        let width = payload_bytes
            .checked_add(name.len())
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<(String, Value)>()))
            .ok_or_else(invalid_runtime_state)?;
        let mut alias_count = 0;
        self.limits
            .charge_expansion(&mut expansion.0, &mut alias_count, width, 1)?;
        Ok(value.clone())
    }

    fn evaluate_partition(
        &self,
        partition: &OrderedPartition,
        expansion: &mut (usize, usize),
    ) -> Result<DeltaBatch, StandingProgramRuntimeError> {
        let order_column = catalog_column_by_id(&self.catalog, &self.plan.order_column_id)?;
        let key_columns = self
            .catalog
            .relation_schema
            .primary_key_column_ids
            .iter()
            .map(|id| catalog_column_by_id(&self.catalog, id))
            .collect::<Result<Vec<_>, _>>()?;
        let count = partition.values().try_fold(0usize, |count, row| {
            let copies = usize::try_from(row.multiplicity)
                .ok()
                .filter(|copies| *copies > 0)
                .ok_or_else(invalid_runtime_state)?;
            let width = serde_json::to_vec(&row.values)
                .map_err(|_| invalid_runtime_state())?
                .len()
                .checked_add(std::mem::size_of::<OrderedWindowRow>())
                .ok_or_else(invalid_runtime_state)?;
            self.limits
                .charge_expansion(&mut expansion.0, &mut expansion.1, width, copies)?;
            count.checked_add(copies).ok_or_else(invalid_runtime_state)
        })?;
        let mut rows = Vec::with_capacity(count);
        for row in partition.values() {
            // Occurrences occupy ordinals 0..multiplicity; retractions remove the suffix.
            rows.extend(std::iter::repeat_n(row, row.multiplicity as usize));
        }
        rows.sort_by(|left, right| {
            let order = self.compare_order_values(
                order_column,
                &left.values[&self.plan.order_column_id],
                &right.values[&self.plan.order_column_id],
            );
            order.then_with(|| {
                key_columns
                    .iter()
                    .map(|column| {
                        let left = &left.values[&column.column_id];
                        let right = &right.values[&column.column_id];
                        left.is_null()
                            .cmp(&right.is_null())
                            .then_with(|| compare_row_number_values(column, left, right))
                    })
                    .find(|order| !order.is_eq())
                    .unwrap_or(Ordering::Equal)
            })
        });
        let mut values = vec![Map::new(); rows.len()];
        for projection in &self.plan.projections {
            for (index, row) in rows.iter().enumerate() {
                let value = row
                    .values
                    .get(&projection.input_column_id)
                    .ok_or_else(invalid_runtime_state)?;
                let value =
                    self.clone_output_value(&projection.output_column_id, value, expansion)?;
                values[index].insert(projection.output_column_id.clone(), value);
            }
        }
        for output in &self.plan.outputs {
            match &output.function {
                OrderedWindowFunctionV1::Lag {
                    value_column_id,
                    offset,
                } => {
                    for (index, value_row) in values.iter_mut().enumerate() {
                        let value = usize::try_from(*offset)
                            .ok()
                            .and_then(|offset| index.checked_sub(offset))
                            .and_then(|target| rows.get(target))
                            .and_then(|row| row.values.get(value_column_id))
                            .unwrap_or(&Value::Null);
                        let value =
                            self.clone_output_value(&output.output_column_id, value, expansion)?;
                        value_row.insert(output.output_column_id.clone(), value);
                    }
                }
                OrderedWindowFunctionV1::SumFloat64 {
                    value_column_id,
                    rows: row_frame,
                    preceding,
                } => {
                    let mut prefixes = Vec::with_capacity(rows.len() + 1);
                    let mut sum = 0.0;
                    let mut count = 0u64;
                    prefixes.push(Value::Null);
                    for row in &rows {
                        let value = row
                            .values
                            .get(value_column_id)
                            .ok_or_else(invalid_runtime_state)?;
                        if !value.is_null() {
                            sum += value.as_f64().ok_or_else(invalid_runtime_state)?;
                            if !sum.is_finite() {
                                return Err(invalid_runtime_state());
                            }
                            count = count.checked_add(1).ok_or_else(invalid_runtime_state)?;
                        }
                        prefixes.push(if count == 0 {
                            Value::Null
                        } else {
                            Value::from(sum)
                        });
                    }
                    let mut peer_end = 0;
                    for (index, value_row) in values.iter_mut().enumerate() {
                        let end = if *row_frame {
                            usize::try_from(*preceding)
                                .ok()
                                .and_then(|preceding| (index + 1).checked_sub(preceding))
                                .unwrap_or(0)
                        } else {
                            if peer_end <= index {
                                peer_end = index + 1;
                                while peer_end < rows.len()
                                    && self.compare_order_values(
                                        order_column,
                                        &rows[index].values[&self.plan.order_column_id],
                                        &rows[peer_end].values[&self.plan.order_column_id],
                                    ) == Ordering::Equal
                                {
                                    peer_end += 1;
                                }
                            }
                            peer_end
                        };
                        let value = self.clone_output_value(
                            &output.output_column_id,
                            &prefixes[end],
                            expansion,
                        )?;
                        value_row.insert(output.output_column_id.clone(), value);
                    }
                }
            }
        }
        let mut output_keys = BTreeSet::new();
        let records = rows
            .iter()
            .zip(values)
            .map(|(row, mut values)| {
                if self.plan.schema_version == 3 {
                    return Ok(DeltaRecord::new(
                        DeltaKey::from_json(Value::Object(values)),
                        DeltaValue::from_json(Value::Null),
                        1,
                    ));
                }
                let key = if self.plan.schema_version == 2 {
                    let aliases =
                        velorix_core::view_plan::supported_ordered_window_output_primary_key(
                            &self.catalog,
                            &self.plan,
                        )
                        .map_err(|_| invalid_runtime_state())?;
                    Value::Object(
                        self.catalog
                            .relation_schema
                            .primary_key_column_ids
                            .iter()
                            .zip(aliases)
                            .map(|(id, alias)| (alias, row.values[id].clone()))
                            .collect(),
                    )
                } else if let Some(id) = &self.plan.output_key_input_column_id {
                    row.values
                        .get(id)
                        .cloned()
                        .ok_or_else(invalid_runtime_state)?
                } else {
                    row.key.clone()
                };
                if key.is_null() || !output_keys.insert(canonical_json(&key)) {
                    return Err(invalid_runtime_state());
                }
                values.remove(&self.plan.output_key_column_id);
                Ok(DeltaRecord::new(
                    DeltaKey::from_json(key),
                    DeltaValue::from_json(Value::Object(values)),
                    1,
                ))
            })
            .collect::<Result<Vec<_>, StandingProgramRuntimeError>>()?;
        Ok(DeltaBatch::from_records(records))
    }

    fn input_delta(
        &self,
        input: &RelationInputBatch,
    ) -> Result<DeltaBatch, StandingProgramRuntimeError> {
        if let Some(delta) = published_input_empty_delta(input, &self.catalog)? {
            return Ok(delta);
        }
        if input.batches.is_empty() {
            return Err(invalid_runtime_state());
        }
        let columns = self
            .catalog
            .relation_schema
            .columns
            .iter()
            .filter(|column| column.column_id != self.catalog.relation_schema.weight_column_id)
            .collect::<Vec<_>>();
        let weight_column = catalog_column_by_id(
            &self.catalog,
            &self.catalog.relation_schema.weight_column_id,
        )?;
        let mut records = Vec::new();
        for batch in &input.batches {
            validate_record_batch_matches_catalog(&self.catalog, batch)
                .map_err(|_| invalid_runtime_state())?;
            for row in 0..batch.num_rows() {
                let values = columns
                    .iter()
                    .map(|column| {
                        Ok((
                            column.column_id.clone(),
                            super::two_input_join::typed_join_arrow_value(batch, column, row)?,
                        ))
                    })
                    .collect::<Result<Map<_, _>, StandingProgramRuntimeError>>()?;
                let key = self.row_key(&values)?;
                if key.is_null() {
                    return Err(invalid_runtime_state());
                }
                let weight =
                    super::two_input_join::typed_join_arrow_value(batch, weight_column, row)?
                        .as_i64()
                        .ok_or_else(invalid_runtime_state)?;
                records.push(DeltaRecord::new(
                    DeltaKey::from_json(key),
                    DeltaValue::from_json(Value::Object(values)),
                    weight,
                ));
            }
        }
        Ok(DeltaBatch::from_records(records))
    }

    // Generic output serialization expects the key first; project back to the
    // admitted SELECT order without changing the materialized delta contract.
    fn key_first_schema(&self) -> Result<RelationSchema, StandingProgramRuntimeError> {
        let mut schema = self.output_schema.clone();
        let position = schema
            .columns
            .iter()
            .position(|column| column.name == self.plan.output_key_column_id)
            .ok_or_else(invalid_runtime_state)?;
        let key = schema.columns.remove(position);
        schema.columns.insert(0, key);
        Ok(schema)
    }

    fn ordered_batch(
        &self,
        batch: RecordBatch,
    ) -> Result<RecordBatch, StandingProgramRuntimeError> {
        let indices = self
            .output_schema
            .columns
            .iter()
            .map(|column| {
                batch
                    .schema()
                    .index_of(&column.name)
                    .map_err(|_| invalid_runtime_state())
            })
            .collect::<Result<Vec<_>, _>>()?;
        batch.project(&indices).map_err(|_| invalid_runtime_state())
    }

    fn output_batch(
        &self,
        output: &DeltaBatch,
    ) -> Result<RecordBatch, StandingProgramRuntimeError> {
        self.limits
            .validate_page(output, self.logical_epoch, &SnapshotPageRequest::default())?;
        if self.plan.schema_version >= 2 {
            return super::output::materialized_bag_delta_page_batch(
                &self.output_schema,
                output,
                self.logical_epoch,
                SnapshotPageRequest {
                    max_rows: None,
                    page_token: None,
                    committed_epoch: None,
                },
            )
            .map(|(batch, _)| batch);
        }
        self.ordered_batch(materialized_generic_delta_to_record_batch(
            &self.key_first_schema()?,
            output,
        )?)
    }
}

impl StandingProgramRuntime for OrderedWindowRuntime {
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
        let idempotency = idempotency_key.as_str().to_string();
        if let Some(applied) = self.applied_epochs.get(&idempotency) {
            if *applied != logical_epoch {
                return Err(StandingProgramRuntimeError::IdempotencyKeyConflict {
                    idempotency_key: idempotency,
                    first_epoch: *applied,
                    attempted_epoch: logical_epoch,
                });
            }
            return Ok(EpochCommit {
                logical_epoch,
                idempotency_key,
                input_frontiers: self.input_frontiers.clone(),
                input_event_time_frontiers: self.input_event_time_frontiers.clone(),
                output_deltas: Vec::new(),
                output_batches: vec![ViewOutputBatch {
                    view_id: self.identity.view_ids[0].clone(),
                    schema_fingerprint: self.output_schema.schema_fingerprint.clone(),
                    batches: vec![self.output_batch(&self.published_output)?],
                }],
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
        let mut changes = BTreeMap::<String, Vec<DeltaRecord>>::new();
        for input in &input_changes {
            validate_input_matches_schema(input, &self.input_schema, "ordered_window_input")?;
            let delta = self.input_delta(input)?;
            for record in delta.net_rows().map_err(|_| invalid_runtime_state())? {
                changes
                    .entry(self.row_identity(record.key.as_json()))
                    .or_default()
                    .push(record);
            }
            advance_input_frontier(&mut frontiers, input)?;
            advance_input_event_time_frontier(&mut event_frontiers, input)?;
        }
        let mut staged = BTreeMap::<String, OrderedPartition>::new();
        let mut key_updates = Vec::new();
        for (key, mut records) in changes {
            let old_partition = self.key_partitions.get(&key);
            if let Some(partition) = old_partition {
                let row = self
                    .partitions
                    .get(partition)
                    .and_then(|rows| rows.get(&key))
                    .ok_or_else(invalid_runtime_state)?;
                records.push(DeltaRecord::new(
                    DeltaKey::from_json(row.key.clone()),
                    DeltaValue::from_json(Value::Object(row.values.clone())),
                    row.multiplicity,
                ));
            }
            let net = DeltaBatch::from_records(records)
                .net_rows()
                .map_err(|_| invalid_runtime_state())?;
            if net.len() > 1
                || net.first().is_some_and(|row| {
                    row.weight < 1 || (self.plan.schema_version != 3 && row.weight != 1)
                })
            {
                return Err(invalid_runtime_state());
            }
            if let Some(partition) = old_partition {
                staged
                    .entry(partition.clone())
                    .or_insert_with(|| self.partitions[partition].clone())
                    .remove(&key);
            }
            let new_partition = if let Some(record) = net.first() {
                let values = record
                    .value
                    .as_json()
                    .as_object()
                    .cloned()
                    .ok_or_else(invalid_runtime_state)?;
                if values.get(&self.plan.order_column_id).is_none() {
                    return Err(invalid_runtime_state());
                }
                let partition = self.partition_id(&values)?;
                staged
                    .entry(partition.clone())
                    .or_insert_with(|| self.partitions.get(&partition).cloned().unwrap_or_default())
                    .insert(
                        key.clone(),
                        OrderedWindowRow {
                            key: record.key.as_json().clone(),
                            values,
                            multiplicity: record.weight,
                        },
                    );
                Some(partition)
            } else {
                None
            };
            key_updates.push((key, new_partition));
        }
        let mut updated_outputs = BTreeMap::new();
        let mut delta_records = Vec::new();
        let empty_output = DeltaBatch::default();
        let mut expansion = (0, 0);
        for (partition, rows) in &staged {
            let output = self.evaluate_partition(rows, &mut expansion)?;
            let previous = self
                .partition_outputs
                .get(partition)
                .unwrap_or(&empty_output);
            delta_records.extend(
                previous
                    .diff(&output)
                    .map_err(|_| invalid_runtime_state())?
                    .records()
                    .iter()
                    .cloned(),
            );
            updated_outputs.insert(partition.clone(), output);
        }
        let delta = DeltaBatch::from_records(
            DeltaBatch::from_records(delta_records)
                .net_rows()
                .map_err(|_| invalid_runtime_state())?,
        );
        let next_output = DeltaBatch::from_records(
            self.published_output
                .combine(&delta)
                .net_rows()
                .map_err(|_| invalid_runtime_state())?,
        );
        let output_batch = self.output_batch(&next_output)?;
        let mut output_keys = BTreeSet::new();
        for row in next_output.records() {
            if self.plan.schema_version != 3
                && !output_keys.insert(canonical_json(row.key.as_json()))
            {
                return Err(invalid_runtime_state());
            }
        }
        let mut candidate = self.clone();
        for (partition, rows) in staged {
            if rows.is_empty() {
                candidate.partitions.remove(&partition);
                candidate.partition_outputs.remove(&partition);
            } else {
                candidate.partitions.insert(partition, rows);
            }
        }
        for (partition, output) in updated_outputs {
            if candidate.partitions.contains_key(&partition) {
                candidate.partition_outputs.insert(partition, output);
            }
        }
        for (key, partition) in key_updates {
            if let Some(partition) = partition {
                candidate.key_partitions.insert(key, partition);
            } else {
                candidate.key_partitions.remove(&key);
            }
        }
        candidate.published_output = next_output;
        candidate.input_frontiers = frontiers.clone();
        candidate.input_event_time_frontiers = event_frontiers.clone();
        candidate.applied_epochs.insert(idempotency, logical_epoch);
        retain_recent_applied_epochs(&mut candidate.applied_epochs);
        candidate.logical_epoch = logical_epoch;
        candidate.checkpoint()?;
        *self = candidate;
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
            output_batches: vec![ViewOutputBatch {
                view_id: self.identity.view_ids[0].clone(),
                schema_fingerprint: self.output_schema.schema_fingerprint.clone(),
                batches: vec![output_batch],
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
            || !self.identity.view_ids.contains(&view.view_id)
        {
            return Err(StandingProgramRuntimeError::UnknownView {
                view_id: view.view_id,
            });
        }
        let (batch, next_page_token) = if self.plan.schema_version >= 2 {
            self.limits
                .validate_page(&self.published_output, self.logical_epoch, &page)?;
            super::output::materialized_bag_delta_page_batch(
                &self.output_schema,
                &self.published_output,
                self.logical_epoch,
                page,
            )?
        } else {
            self.limits.validate_page(
                &self.published_output,
                self.logical_epoch,
                &SnapshotPageRequest::default(),
            )?;
            materialized_generic_delta_page_batch(
                &self.key_first_schema()?,
                &self.published_output,
                self.logical_epoch,
                page,
            )?
        };
        Ok(MaterializedViewPage {
            view,
            logical_epoch: self.logical_epoch,
            schema_fingerprint: self.output_schema.schema_fingerprint.clone(),
            batches: vec![self.ordered_batch(batch)?],
            next_page_token,
        })
    }

    fn checkpoint(&self) -> Result<RuntimeCheckpoint, StandingProgramRuntimeError> {
        let payload = OrderedWindowCheckpoint {
            schema_version: CHECKPOINT_PAYLOAD_SCHEMA_VERSION,
            runtime_kind: ORDERED_WINDOW_RUNTIME_KIND.to_string(),
            catalog: self.catalog.clone(),
            input_schema: self.input_schema.clone(),
            output_schema: self.output_schema.clone(),
            view_sql: self.view_sql.clone(),
            plan: self.plan.clone(),
            logical_plan: self.logical_plan.clone(),
            partitions: self.partitions.clone(),
            published_output: self.published_output.clone(),
            input_frontiers: self.input_frontiers.clone(),
            input_event_time_frontiers: self.input_event_time_frontiers.clone(),
            applied_epochs: self.applied_epochs.clone(),
            logical_epoch: self.logical_epoch,
        };
        let payload = self.limits.checkpoint_arrow(&payload)?;
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

impl OrderedWindowRuntime {
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
        let payload: OrderedWindowCheckpoint =
            velorix_core::arrow_state::from_state(&state.payload)
                .map_err(|_| invalid_checkpoint())?;
        if payload.schema_version != CHECKPOINT_PAYLOAD_SCHEMA_VERSION
            || payload.runtime_kind != ORDERED_WINDOW_RUNTIME_KIND
            || payload.logical_epoch != checkpoint.logical_epoch
            || payload.input_frontiers != checkpoint.input_frontiers
            || payload.input_event_time_frontiers != checkpoint.input_event_time_frontiers
        {
            return Err(invalid_checkpoint());
        }
        let mut runtime = Self::new_with_logical_plan_and_limits(
            checkpoint.identity,
            payload.catalog,
            payload.input_schema,
            payload.output_schema,
            payload.view_sql,
            payload.plan,
            payload.logical_plan,
            limits,
        )?;
        runtime.partitions = payload.partitions;
        let mut outputs = Vec::new();
        for (partition, rows) in &runtime.partitions {
            if rows.is_empty() {
                return Err(invalid_checkpoint());
            }
            for (key, row) in rows {
                if runtime.row_identity(&row.key) != *key
                    || runtime.row_key(&row.values)? != row.key
                    || row.multiplicity < 1
                    || (runtime.plan.schema_version != 3 && row.multiplicity != 1)
                    || row.values.len() + 1 != runtime.catalog.relation_schema.columns.len()
                    || runtime
                        .catalog
                        .relation_schema
                        .columns
                        .iter()
                        .filter(|column| {
                            column.column_id != runtime.catalog.relation_schema.weight_column_id
                        })
                        .any(|column| {
                            row.values
                                .get(&column.column_id)
                                .is_none_or(|value| !column.nullable && value.is_null())
                        })
                    || runtime.partition_id(&row.values)? != *partition
                    || row.values.get(&runtime.plan.order_column_id).is_none()
                    || runtime
                        .key_partitions
                        .insert(key.clone(), partition.clone())
                        .is_some()
                {
                    return Err(invalid_checkpoint());
                }
            }
            let output = runtime.evaluate_partition(rows, &mut (0, 0))?;
            outputs.extend_from_slice(output.records());
            runtime.partition_outputs.insert(partition.clone(), output);
        }
        // Apply retains canonical net-row order for every identity mode. Window
        // evaluation order can differ (especially for NULL peers), so restore
        // must normalize keyed outputs too before serializing the next checkpoint.
        let expected = DeltaBatch::from_records(
            DeltaBatch::from_records(outputs)
                .net_rows()
                .map_err(|_| invalid_checkpoint())?,
        );
        if expected.net_rows().map_err(|_| invalid_checkpoint())?
            != payload
                .published_output
                .net_rows()
                .map_err(|_| invalid_checkpoint())?
        {
            return Err(invalid_checkpoint());
        }
        runtime.output_batch(&expected)?;
        let mut keys = BTreeSet::new();
        for row in expected.records() {
            if runtime.plan.schema_version != 3 && !keys.insert(canonical_json(row.key.as_json())) {
                return Err(invalid_checkpoint());
            }
        }
        if payload
            .applied_epochs
            .values()
            .any(|epoch| *epoch > payload.logical_epoch)
        {
            return Err(invalid_checkpoint());
        }
        runtime.published_output = expected;
        runtime.input_frontiers = payload.input_frontiers;
        runtime.input_event_time_frontiers = payload.input_event_time_frontiers;
        runtime.applied_epochs = payload.applied_epochs;
        retain_recent_applied_epochs(&mut runtime.applied_epochs);
        runtime.logical_epoch = payload.logical_epoch;
        Ok(runtime)
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct OrderedWindowCheckpoint {
    schema_version: u32,
    runtime_kind: String,
    catalog: VelorixRelationCatalogV1,
    input_schema: RelationSchema,
    output_schema: RelationSchema,
    view_sql: String,
    plan: SupportedOrderedWindowPlanV1,
    logical_plan: VelorixLogicalViewPlanV1,
    partitions: BTreeMap<String, OrderedPartition>,
    published_output: DeltaBatch,
    input_frontiers: Vec<RelationFrontier>,
    input_event_time_frontiers: Vec<InputEventTimeFrontier>,
    applied_epochs: BTreeMap<String, LogicalEpoch>,
    logical_epoch: LogicalEpoch,
}

fn validate_ordered_window_contract(
    catalog: &VelorixRelationCatalogV1,
    input: &RelationSchema,
    output: &RelationSchema,
    plan: &SupportedOrderedWindowPlanV1,
) -> Result<(), StandingProgramRuntimeError> {
    if !matches!(plan.schema_version, 1..=3)
        || plan.input_relation_id != catalog.relation_schema.relation_id
        || (plan.schema_version == 3) != catalog.relation_schema.primary_key_column_ids.is_empty()
        || (plan.schema_version != 3
            && catalog.relation_schema.primary_key_column_ids.first() != Some(&plan.key_column_id))
        || *input != catalog_input_relation_schema(catalog).map_err(|_| invalid_runtime_state())?
        || output.primary_key
            != velorix_core::view_plan::supported_ordered_window_output_primary_key(catalog, plan)
                .map_err(|_| invalid_runtime_state())?
        || plan.outputs.is_empty()
    {
        return Err(invalid_runtime_state());
    }
    catalog_column_by_id(catalog, &plan.order_column_id)?;
    for id in &plan.partition_column_ids {
        catalog_column_by_id(catalog, id)?;
    }
    let key = if plan.schema_version == 3 {
        None
    } else {
        Some(catalog_column_by_id(
            catalog,
            plan.output_key_input_column_id
                .as_ref()
                .unwrap_or(&plan.key_column_id),
        )?)
    };
    let mut expected = BTreeMap::new();
    if let Some(key) = key {
        expected.insert(
            plan.output_key_column_id.clone(),
            (sql_type_from_catalog_column(key)?, key.nullable),
        );
    }
    for projection in &plan.projections {
        let column = catalog_column_by_id(catalog, &projection.input_column_id)?;
        if projection.output_column_id == plan.output_key_column_id {
            if key.is_none_or(|key| projection.input_column_id != key.column_id) {
                return Err(invalid_runtime_state());
            }
        } else if expected
            .insert(
                projection.output_column_id.clone(),
                (sql_type_from_catalog_column(column)?, column.nullable),
            )
            .is_some()
        {
            return Err(invalid_runtime_state());
        }
    }
    for window in &plan.outputs {
        let data_type = match &window.function {
            OrderedWindowFunctionV1::SumFloat64 {
                value_column_id,
                rows,
                preceding,
            } => {
                let column = catalog_column_by_id(catalog, value_column_id)?;
                if column.physical_arrow_type != ArrowPhysicalTypeV1::Float64
                    || (!rows && *preceding != 0)
                {
                    return Err(invalid_runtime_state());
                }
                SqlDataType::Float64
            }
            OrderedWindowFunctionV1::Lag {
                value_column_id, ..
            } => sql_type_from_catalog_column(catalog_column_by_id(catalog, value_column_id)?)?,
        };
        if expected
            .insert(window.output_column_id.clone(), (data_type, true))
            .is_some()
        {
            return Err(invalid_runtime_state());
        }
    }
    if expected.len() != output.columns.len() {
        return Err(invalid_runtime_state());
    }
    let mut names = BTreeSet::new();
    for column in &output.columns {
        if !names.insert(&column.name)
            || expected.get(&column.name) != Some(&(column.data_type.clone(), column.nullable))
        {
            return Err(invalid_runtime_state());
        }
    }
    Ok(())
}
