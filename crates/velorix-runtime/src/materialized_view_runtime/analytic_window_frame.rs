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
                let Some(value) = self.navigation_value(row, &rows[frame_start..frame_end], index)
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

    /// Computes the navigation function result for the current row over its
    /// (bounded) frame window. Returns `None` when the value is out of
    /// range (LAG/LEAD beyond the partition, NTH_VALUE beyond the frame).
    fn navigation_value(
        &self,
        current: &AnalyticFrameRow,
        frame: &[&AnalyticFrameRow],
        index: usize,
    ) -> Option<Value> {
        let value_of =
            |row: &AnalyticFrameRow| row.values.get(frame_value_column_id(&self.plan)).cloned();
        match &self.plan.function {
            WindowNavigationFunctionV1::Lag { offset, .. } => {
                if index < *offset as usize {
                    return None;
                }
                // The frame may be narrower than the offset; LAG reads
                // within the partition order regardless of the frame bounds.
                let target = index - *offset as usize;
                self.rows.values().find(|row| {
                    row.key == current.key && self.partition_rank(row, target).is_some()
                })?;
                let partition = current.partition_value.clone();
                let mut partition_rows = self
                    .rows
                    .values()
                    .filter(|row| row.partition_value == partition)
                    .collect::<Vec<_>>();
                partition_rows.sort_by(|left, right| {
                    let ordering = compare_row_number_values(
                        self.order_column().expect("validated"),
                        &left.order_value,
                        &right.order_value,
                    );
                    let ordering = if self.plan.order_descending {
                        ordering.reverse()
                    } else {
                        ordering
                    };
                    ordering.then_with(|| {
                        compare_row_number_values(
                            self.key_column().expect("validated"),
                            &left.key,
                            &right.key,
                        )
                    })
                });
                value_of(partition_rows.get(target)?)
            }
            WindowNavigationFunctionV1::Lead { offset, .. } => {
                let partition = current.partition_value.clone();
                let mut partition_rows = self
                    .rows
                    .values()
                    .filter(|row| row.partition_value == partition)
                    .collect::<Vec<_>>();
                partition_rows.sort_by(|left, right| {
                    let ordering = compare_row_number_values(
                        self.order_column().expect("validated"),
                        &left.order_value,
                        &right.order_value,
                    );
                    let ordering = if self.plan.order_descending {
                        ordering.reverse()
                    } else {
                        ordering
                    };
                    ordering.then_with(|| {
                        compare_row_number_values(
                            self.key_column().expect("validated"),
                            &left.key,
                            &right.key,
                        )
                    })
                });
                value_of(partition_rows.get(index + *offset as usize)?)
            }
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

    fn partition_rank(&self, row: &AnalyticFrameRow, target: usize) -> Option<usize> {
        let _ = (row, target);
        Some(target)
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
        let next_output = self.recompute_all_outputs()?;
        let output_delta = self
            .published_output
            .inverse()
            .map_err(|_| invalid_runtime_state())?
            .combine(&next_output);
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
        let payload: AnalyticFrameCheckpointPayload =
            serde_json::from_str(&state_payload.payload).map_err(|_| invalid_checkpoint())?;
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
