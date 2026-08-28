//! Phase 8.3: Temporal (as-of) join runtime.
//!
//! Matches each left row with the most recent right row by event time
//! before the left row's timestamp: `SELECT ... FROM left l JOIN right r
//! ON r.event_time <= l.event_time`. This is common for order enrichment
//! (match each order with the latest price snapshot).
//!
//! State: BTreeMap of right-side rows keyed by join_key, then by
//! event_time, with bag semantics (multiple rows per key+time).
//! Left-side rows are matched on insert; late left rows that arrive
//! after a right row with later timestamp match the correct predecessor.
//! Output is the full projected row.
//!
//! Checkpoint: JSON-serialized state includes full catalogs, schemas, and
//! all staged rows. For very large state (>100K rows), consider switching
//! to a binary checkpoint format or checkpoint-only-delta approach.

use super::*;
use crate::materialized_view_runtime::semi_anti_join::catalog_for_relation_id;
use velorix_core::view_plan::{
    validate_supported_temporal_join_sql, SupportedTemporalJoinPlanV1, TemporalJoinSideV1,
};

pub struct TemporalJoinRuntime {
    identity: StandingProgramIdentity,
    left_catalog: VelorixRelationCatalogV1,
    right_catalog: VelorixRelationCatalogV1,
    left_input_schema: RelationSchema,
    right_input_schema: RelationSchema,
    output_schema: RelationSchema,
    view_sql: String,
    plan: SupportedTemporalJoinPlanV1,
    logical_plan: VelorixLogicalViewPlanV1,
    /// Right-side rows indexed by join_key -> event_time -> rows.
    /// Bag semantics: multiple rows with the same join_key and event_time
    /// are stored independently. Eviction keeps at least the most recent
    /// event_time per join key.
    right_index: BTreeMap<String, BTreeMap<i64, Vec<TemporalRow>>>,
    /// Left-side rows indexed by join_key -> event_time -> rows.
    /// Bag semantics: multiple rows with the same join_key and event_time
    /// are stored independently, matching the right-side structure.
    left_rows: BTreeMap<String, BTreeMap<i64, Vec<TemporalRow>>>,
    published_output: DeltaBatch,
    input_frontiers: Vec<RelationFrontier>,
    input_event_time_frontiers: Vec<InputEventTimeFrontier>,
    applied_epochs: BTreeMap<String, LogicalEpoch>,
    logical_epoch: LogicalEpoch,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct TemporalRow {
    values: BTreeMap<String, Value>,
    event_time_ns: i64,
    weight: i64,
}

impl TemporalJoinRuntime {
    pub fn new_with_logical_plan(
        identity: StandingProgramIdentity,
        catalogs: Vec<VelorixRelationCatalogV1>,
        input_schemas: Vec<RelationSchema>,
        output_schema: RelationSchema,
        view_sql: String,
        plan: SupportedTemporalJoinPlanV1,
        logical_plan: VelorixLogicalViewPlanV1,
    ) -> Result<Self, StandingProgramRuntimeError> {
        identity.validate()?;
        validate_builtin_runtime_identity(&identity)?;
        validate_view_sql_hash(&identity, view_sql.as_str())?;
        validate_logical_view_plan(&logical_plan).map_err(|_| {
            StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "logical_temporal_join_plan",
            }
        })?;
        validate_temporal_join_contract(&catalogs, &input_schemas, &plan)?;
        let compiled =
            validate_supported_temporal_join_sql(view_sql.as_str(), &catalogs).map_err(|_| {
                StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "temporal_join_plan",
                }
            })?;
        if compiled != plan {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "temporal_join_plan",
            });
        }
        let (left_catalog, right_catalog) = match catalogs.as_slice() {
            [left, right] => (left.clone(), right.clone()),
            _ => {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "temporal_join_catalogs",
                })
            }
        };
        let (left_input_schema, right_input_schema) = match input_schemas.as_slice() {
            [left, right] => (left.clone(), right.clone()),
            _ => {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "temporal_join_input_schemas",
                })
            }
        };
        Ok(Self {
            identity,
            left_catalog,
            right_catalog,
            left_input_schema,
            right_input_schema,
            output_schema,
            view_sql,
            plan,
            logical_plan,
            right_index: BTreeMap::new(),
            left_rows: BTreeMap::new(),
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
        materialized_delta_to_record_batch(&self.output_schema, &self.published_output, Some(&[]))
    }

    fn materialized_page_batch(
        &self,
        page: SnapshotPageRequest,
    ) -> Result<(RecordBatch, Option<String>), StandingProgramRuntimeError> {
        materialized_delta_page_batch(
            &self.output_schema,
            &self.published_output,
            self.logical_epoch,
            page,
            Some(&[]),
        )
    }

    fn apply_right_side(
        right_index: &mut BTreeMap<String, BTreeMap<i64, Vec<TemporalRow>>>,
        record: &DeltaRecord,
        key_column: &str,
        time_column: &str,
    ) -> Result<(), StandingProgramRuntimeError> {
        let key = record
            .value
            .as_json()
            .as_object()
            .and_then(|obj| obj.get(key_column))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "temporal_join_right_key",
            })?;
        let event_time = record
            .value
            .as_json()
            .as_object()
            .and_then(|obj| obj.get(time_column))
            .and_then(|v| v.as_i64())
            .ok_or(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "temporal_join_right_event_time",
            })?;
        if event_time == i64::MIN {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "temporal_join_event_time_overflow",
            });
        }
        let values: BTreeMap<String, Value> = record
            .value
            .as_json()
            .as_object()
            .ok_or_else(invalid_runtime_state)?
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let time_map = right_index.entry(key.clone()).or_default();
        let rows = time_map.entry(event_time).or_default();
        if record.weight > 0 {
            // Insert: check if a row with identical values already exists (bag dedup)
            if let Some(existing) = rows.iter_mut().find(|r| r.values == values) {
                existing.weight = existing
                    .weight
                    .checked_add(record.weight)
                    .ok_or_else(invalid_runtime_state)?;
            } else {
                rows.push(TemporalRow {
                    values,
                    event_time_ns: event_time,
                    weight: record.weight,
                });
            }
        } else if record.weight < 0 {
            // Retract: find a row with matching values and decrement weight
            if let Some(existing) = rows.iter_mut().find(|r| r.values == values) {
                existing.weight = existing
                    .weight
                    .checked_add(record.weight)
                    .ok_or_else(invalid_runtime_state)?;
                if existing.weight < 0 {
                    return Err(invalid_runtime_state());
                }
            } else {
                return Err(invalid_runtime_state());
            }
            // Remove zero-weight rows
            rows.retain(|r| r.weight != 0);
            if rows.is_empty() {
                time_map.remove(&event_time);
            }
            if time_map.is_empty() {
                right_index.remove(&key);
            }
        }
        Ok(())
    }
}

impl StandingProgramRuntime for TemporalJoinRuntime {
    fn program_identity(&self) -> &StandingProgramIdentity {
        &self.identity
    }

    fn input_schemas(&self) -> Vec<RelationSchema> {
        vec![
            self.left_input_schema.clone(),
            self.right_input_schema.clone(),
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
        let mut next_left = self.left_rows.clone();
        let mut next_right = self.right_index.clone();

        for input in &input_changes {
            if input.relation_id == self.plan.left_input_relation_id {
                validate_input_matches_schema(
                    input,
                    &self.left_input_schema,
                    "temporal_join_input",
                )?;
                let delta = if let Some(empty_delta) =
                    published_input_empty_delta(input, &self.left_catalog)?
                {
                    empty_delta
                } else {
                    let mut columns = BTreeSet::new();
                    // Include all columns needed by the output projection
                    for item in &self.plan.output_columns {
                        match item.side {
                            TemporalJoinSideV1::Left => {
                                columns.insert(item.column_id.clone());
                            }
                            TemporalJoinSideV1::Right => {}
                        }
                    }
                    columns.insert(self.plan.left_join_column_id.to_string());
                    columns.insert(self.plan.left_event_time_column_id.to_string());
                    arrow_record_batches_to_key_multi_value_delta_batch(
                        &self.left_catalog,
                        &input.relation_id,
                        &input.relation_version,
                        &input.schema_fingerprint,
                        std::slice::from_ref(&self.plan.left_join_column_id),
                        &columns.into_iter().collect::<Vec<_>>(),
                        &input.batches,
                    )
                    .map_err(|_| {
                        StandingProgramRuntimeError::InvalidProgramIdentity {
                            field: "temporal_join_input_batch",
                        }
                    })?
                };
                for record in delta.net_rows().map_err(|_| invalid_runtime_state())? {
                    let key = record
                        .value
                        .as_json()
                        .as_object()
                        .and_then(|obj| obj.get(&self.plan.left_join_column_id))
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                        .ok_or(StandingProgramRuntimeError::InvalidProgramIdentity {
                            field: "temporal_join_left_key",
                        })?;
                    let event_time = record
                        .value
                        .as_json()
                        .as_object()
                        .and_then(|obj| obj.get(&self.plan.left_event_time_column_id))
                        .and_then(|v| v.as_i64())
                        .ok_or(StandingProgramRuntimeError::InvalidProgramIdentity {
                            field: "temporal_join_left_event_time",
                        })?;
                    if event_time == i64::MIN {
                        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                            field: "temporal_join_event_time_overflow",
                        });
                    }
                    let values: BTreeMap<String, Value> = record
                        .value
                        .as_json()
                        .as_object()
                        .ok_or_else(invalid_runtime_state)?
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect();
                    let by_time = next_left.entry(key.clone()).or_default();
                    let rows = by_time.entry(event_time).or_default();
                    if record.weight > 0 {
                        // Insert: check if a row with identical values already exists (bag dedup)
                        if let Some(existing) = rows.iter_mut().find(|r| r.values == values) {
                            existing.weight = existing
                                .weight
                                .checked_add(record.weight)
                                .ok_or_else(invalid_runtime_state)?;
                        } else {
                            rows.push(TemporalRow {
                                values,
                                event_time_ns: event_time,
                                weight: record.weight,
                            });
                        }
                    } else if record.weight < 0 {
                        // Retract: find a row with matching values and decrement weight
                        if let Some(existing) = rows.iter_mut().find(|r| r.values == values) {
                            existing.weight = existing
                                .weight
                                .checked_add(record.weight)
                                .ok_or_else(invalid_runtime_state)?;
                            if existing.weight < 0 {
                                return Err(invalid_runtime_state());
                            }
                        } else {
                            return Err(invalid_runtime_state());
                        }
                        // Remove zero-weight rows
                        rows.retain(|r| r.weight != 0);
                        if rows.is_empty() {
                            by_time.remove(&event_time);
                        }
                        if by_time.is_empty() {
                            next_left.remove(&key);
                        }
                        continue;
                    }
                }
            } else if input.relation_id == self.plan.right_input_relation_id {
                validate_input_matches_schema(
                    input,
                    &self.right_input_schema,
                    "temporal_join_input",
                )?;
                let delta = if let Some(empty_delta) =
                    published_input_empty_delta(input, &self.right_catalog)?
                {
                    empty_delta
                } else {
                    let mut columns = BTreeSet::new();
                    // Include all columns needed by the output projection
                    for item in &self.plan.output_columns {
                        match item.side {
                            TemporalJoinSideV1::Left => {}
                            TemporalJoinSideV1::Right => {
                                columns.insert(item.column_id.clone());
                            }
                        }
                    }
                    columns.insert(self.plan.right_join_column_id.to_string());
                    columns.insert(self.plan.right_event_time_column_id.to_string());
                    arrow_record_batches_to_key_multi_value_delta_batch(
                        &self.right_catalog,
                        &input.relation_id,
                        &input.relation_version,
                        &input.schema_fingerprint,
                        std::slice::from_ref(&self.plan.right_join_column_id),
                        &columns.into_iter().collect::<Vec<_>>(),
                        &input.batches,
                    )
                    .map_err(|_| {
                        StandingProgramRuntimeError::InvalidProgramIdentity {
                            field: "temporal_join_input_batch",
                        }
                    })?
                };
                for record in delta.net_rows().map_err(|_| invalid_runtime_state())? {
                    Self::apply_right_side(
                        &mut next_right,
                        &record,
                        &self.plan.right_join_column_id,
                        &self.plan.right_event_time_column_id,
                    )?;
                }
            } else {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "temporal_join_input_relation",
                });
            }
            advance_input_frontier(&mut next_frontiers, input)?;
            advance_input_event_time_frontier(&mut next_event_time_frontiers, input)?;
        }

        evict_right_index(&mut next_right, &next_left, &next_event_time_frontiers);

        if next_left
            .values()
            .map(|m| m.values().map(|rows| rows.len() as u64).sum::<u64>())
            .sum::<u64>()
            > self.plan.resource_contract.max_rows_per_side
        {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "temporal_join_resource_contract",
            });
        }
        if next_right.len() as u64 > self.plan.resource_contract.max_rows_per_side {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "temporal_join_resource_contract",
            });
        }

        let next_output = recompute_from_staged(&next_left, &next_right, &self.plan)?;
        if next_output
            .net_rows()
            .map_err(|_| invalid_runtime_state())?
            .len() as u64
            > self.plan.resource_contract.max_output_rows_per_epoch
        {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "temporal_join_resource_contract",
            });
        }
        let output_delta = self
            .published_output
            .diff(&next_output)
            .map_err(|_| invalid_runtime_state())?;
        // Validate output before commit
        let output_batches = vec![ViewOutputBatch {
            view_id: self.identity.view_ids[0].clone(),
            schema_fingerprint: self.output_schema_fingerprint(),
            batches: vec![materialized_delta_to_record_batch(
                &self.output_schema,
                &next_output,
                Some(&[]),
            )?],
        }];
        // Commit staged state
        self.left_rows = next_left;
        self.right_index = next_right;
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
            || !self.identity.view_ids.iter().any(|id| id == &view.view_id)
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
        let payload = TemporalJoinCheckpointPayloadV2 {
            schema_version: CHECKPOINT_PAYLOAD_SCHEMA_VERSION,
            runtime_kind: TEMPORAL_JOIN_RUNTIME_KIND.to_string(),
            left_catalog: self.left_catalog.clone(),
            right_catalog: self.right_catalog.clone(),
            left_input_schema: self.left_input_schema.clone(),
            right_input_schema: self.right_input_schema.clone(),
            output_schema: self.output_schema.clone(),
            view_sql: self.view_sql.clone(),
            plan: self.plan.clone(),
            logical_plan: self.logical_plan.clone(),
            input_frontiers: self.input_frontiers.clone(),
            input_event_time_frontiers: self.input_event_time_frontiers.clone(),
            left_rows: self.left_rows.clone(),
            right_index: self.right_index.clone(),
            published_output: self.published_output.clone(),
            applied_epochs: self
                .applied_epochs
                .iter()
                .map(|(k, v)| GenericAppliedEpoch {
                    idempotency_key: k.clone(),
                    logical_epoch: *v,
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
        let payload: TemporalJoinCheckpointPayloadV2 =
            serde_json::from_str(&state_payload.payload).map_err(|_| invalid_checkpoint())?;
        if payload.schema_version != CHECKPOINT_PAYLOAD_SCHEMA_VERSION
            || payload.runtime_kind != TEMPORAL_JOIN_RUNTIME_KIND
        {
            return Err(invalid_checkpoint());
        }
        validate_temporal_join_contract(
            &[payload.left_catalog.clone(), payload.right_catalog.clone()],
            &[
                payload.left_input_schema.clone(),
                payload.right_input_schema.clone(),
            ],
            &payload.plan,
        )?;
        if payload.logical_epoch != checkpoint.logical_epoch
            || payload.input_frontiers != checkpoint.input_frontiers
            || payload.input_event_time_frontiers != checkpoint.input_event_time_frontiers
        {
            return Err(invalid_checkpoint());
        }
        validate_view_sql_hash(&checkpoint.identity, payload.view_sql.as_str())?;
        validate_published_output(&payload.published_output)?;
        let mut applied_epochs = payload
            .applied_epochs
            .into_iter()
            .map(|e| (e.idempotency_key, e.logical_epoch))
            .collect();
        retain_recent_applied_epochs(&mut applied_epochs);
        Ok(Self {
            identity: checkpoint.identity,
            left_catalog: payload.left_catalog,
            right_catalog: payload.right_catalog,
            left_input_schema: payload.left_input_schema,
            right_input_schema: payload.right_input_schema,
            output_schema: payload.output_schema,
            view_sql: payload.view_sql,
            plan: payload.plan,
            logical_plan: payload.logical_plan,
            right_index: payload.right_index,
            left_rows: payload.left_rows,
            published_output: payload.published_output,
            input_frontiers: checkpoint.input_frontiers,
            input_event_time_frontiers: checkpoint.input_event_time_frontiers,
            applied_epochs,
            logical_epoch: checkpoint.logical_epoch,
        })
    }
}

fn recompute_from_staged(
    left: &BTreeMap<String, BTreeMap<i64, Vec<TemporalRow>>>,
    right: &BTreeMap<String, BTreeMap<i64, Vec<TemporalRow>>>,
    plan: &SupportedTemporalJoinPlanV1,
) -> Result<DeltaBatch, StandingProgramRuntimeError> {
    let mut records = Vec::new();
    for (join_key, by_time) in left {
        for left_rows in by_time.values() {
            for left_row in left_rows {
                if left_row.weight <= 0 {
                    continue;
                }
                let left_event_time = left_row.event_time_ns;
                // Find the most recent right event_time <= left_event_time for this key
                if let Some(time_map) = right.get(join_key) {
                    // BTreeMap iter in ascending order; find floor (largest <= left_event_time)
                    let floor_time = time_map
                        .range(..=left_event_time)
                        .next_back()
                        .map(|(t, _)| *t);
                    if let Some(rt) = floor_time {
                        if let Some(rows) = time_map.get(&rt) {
                            for right_row in rows {
                                if right_row.weight <= 0 {
                                    continue;
                                }
                                let mut output = serde_json::Map::new();
                                for item in &plan.output_columns {
                                    let value = match item.side {
                                        TemporalJoinSideV1::Left => {
                                            left_row.values.get(&item.column_id)
                                        }
                                        TemporalJoinSideV1::Right => {
                                            right_row.values.get(&item.column_id)
                                        }
                                    };
                                    output.insert(
                                        item.output_name.clone(),
                                        value.cloned().unwrap_or(Value::Null),
                                    );
                                }
                                let weight = left_row
                                    .weight
                                    .checked_mul(right_row.weight)
                                    .ok_or_else(invalid_runtime_state)?;
                                records.push(DeltaRecord::new(
                                    DeltaKey::from_json(Value::Object(output)),
                                    DeltaValue::from_json(Value::Object(serde_json::Map::new())),
                                    weight,
                                ));
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(DeltaBatch::from_records(records))
}

/// Evict superseded right-side rows using the left input event time watermark.
///
/// For each join key, a right row at event_time R is superseded when:
/// 1. There exists a newer right row at R' > R for the same key
/// 2. R < L_min (below the minimum left event time for this key)
/// 3. R' <= L_min (the newer row is also below L_min, so no future left
///    row will ever match R instead of R')
///
/// This means we only evict old rows that are fully shadowed by a newer
/// row that is also below all current/future left event times.
fn evict_right_index(
    right_index: &mut BTreeMap<String, BTreeMap<i64, Vec<TemporalRow>>>,
    left_rows: &BTreeMap<String, BTreeMap<i64, Vec<TemporalRow>>>,
    left_event_time_frontiers: &[InputEventTimeFrontier],
) {
    let watermark = match left_event_time_frontiers
        .iter()
        .map(|f| f.watermark_ns)
        .min()
    {
        Some(w) if w > 0 => w,
        _ => return,
    };
    let join_keys: Vec<String> = right_index.keys().cloned().collect();
    for join_key in join_keys {
        if let Some(time_map) = right_index.get_mut(&join_key) {
            let l_min = left_rows
                .get(&join_key)
                .and_then(|by_time| by_time.keys().next())
                .copied();
            if let Some(lm) = l_min {
                // Collect event times below watermark
                let below_wm: Vec<i64> = time_map
                    .keys()
                    .copied()
                    .filter(|&et| et <= watermark)
                    .collect();
                if below_wm.is_empty() {
                    continue;
                }
                // Find the maximum event time overall for this key
                let max_et = time_map.keys().copied().max().unwrap();
                // Only evict times below l_min if the max time is also <= l_min
                // (fully shadowed). If max_et > l_min, some of those old times
                // may still be floor predecessors for left rows with et between
                // the old time and l_min.
                if max_et <= lm {
                    let mut to_remove = Vec::new();
                    for &et in &below_wm {
                        if et < lm {
                            to_remove.push(et);
                        }
                    }
                    for et in to_remove {
                        time_map.remove(&et);
                    }
                }
            }
            // Remove empty time buckets
            time_map.retain(|_, rows| !rows.is_empty());
        }
        // Remove empty join keys
        if right_index.get(&join_key).is_some_and(|m| m.is_empty()) {
            right_index.remove(&join_key);
        }
    }
    // Remove orphaned keys (no current left rows) where the most recent
    // right time is below the watermark. Preserve the floor predecessor
    // (most recent right row) for future left rows; only evict strictly
    // shadowed older rows.
    let orphaned_keys: Vec<String> = right_index
        .keys()
        .filter(|k| !left_rows.contains_key(*k))
        .cloned()
        .collect();
    for join_key in orphaned_keys {
        if let Some(time_map) = right_index.get_mut(&join_key) {
            let most_recent = time_map.keys().copied().max();
            if let Some(et) = most_recent {
                if et <= watermark {
                    // Keep only the floor predecessor (most recent row).
                    // All older rows are fully shadowed because any future
                    // left row with event_time >= watermark will match the
                    // most recent right row instead.
                    let keys_to_remove: Vec<i64> =
                        time_map.keys().copied().filter(|&k| k < et).collect();
                    for key in keys_to_remove {
                        time_map.remove(&key);
                    }
                }
            }
            // Remove empty time buckets
            time_map.retain(|_, rows| !rows.is_empty());
        }
        // Remove empty join keys
        if right_index.get(&join_key).is_some_and(|m| m.is_empty()) {
            right_index.remove(&join_key);
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct TemporalJoinCheckpointPayloadV2 {
    schema_version: u32,
    runtime_kind: String,
    left_catalog: VelorixRelationCatalogV1,
    right_catalog: VelorixRelationCatalogV1,
    left_input_schema: RelationSchema,
    right_input_schema: RelationSchema,
    output_schema: RelationSchema,
    view_sql: String,
    plan: SupportedTemporalJoinPlanV1,
    logical_plan: VelorixLogicalViewPlanV1,
    input_frontiers: Vec<RelationFrontier>,
    input_event_time_frontiers: Vec<InputEventTimeFrontier>,
    left_rows: BTreeMap<String, BTreeMap<i64, Vec<TemporalRow>>>,
    right_index: BTreeMap<String, BTreeMap<i64, Vec<TemporalRow>>>,
    published_output: DeltaBatch,
    applied_epochs: Vec<GenericAppliedEpoch>,
    logical_epoch: LogicalEpoch,
}

pub(super) const TEMPORAL_JOIN_RUNTIME_KIND: &str = "temporal_join_v1";

fn validate_temporal_join_contract(
    catalogs: &[VelorixRelationCatalogV1],
    input_schemas: &[RelationSchema],
    plan: &SupportedTemporalJoinPlanV1,
) -> Result<(), StandingProgramRuntimeError> {
    if plan.schema_version != 1 || catalogs.len() != 2 || input_schemas.len() != 2 {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "temporal_join_contract",
        });
    }
    let left_catalog = catalog_for_relation_id(catalogs, &plan.left_input_relation_id)?;
    let right_catalog = catalog_for_relation_id(catalogs, &plan.right_input_relation_id)?;
    if left_catalog.relation_schema.relation_id == right_catalog.relation_schema.relation_id {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "temporal_join_contract",
        });
    }
    let expected_left = catalog_input_relation_schema(left_catalog).map_err(|_| {
        StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "temporal_join_left_input_schema",
        }
    })?;
    let expected_right = catalog_input_relation_schema(right_catalog).map_err(|_| {
        StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "temporal_join_right_input_schema",
        }
    })?;
    if expected_left != input_schemas[0] || expected_right != input_schemas[1] {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "temporal_join_input_schemas",
        });
    }
    for output in &plan.output_columns {
        let catalog = match output.side {
            TemporalJoinSideV1::Left => left_catalog,
            TemporalJoinSideV1::Right => right_catalog,
        };
        catalog_column(catalog, &output.column_id)?;
    }
    Ok(())
}
