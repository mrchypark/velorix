//! Phase 8.3: Temporal (as-of) join runtime.
//!
//! Matches each left row with the most recent right row by event time
//! before the left row's timestamp: `SELECT ... FROM left l JOIN right r
//! ON r.event_time <= l.event_time`. This is common for order enrichment
//! (match each order with the latest price snapshot).
//!
//! State: BTreeMap of right-side rows keyed by (join_key, -event_time_ns)
//! for O(log K) per-event lookup. Left-side rows are matched on insert;
//! late left rows that arrive after a right row with later timestamp match
//! the correct predecessor. Output is the full projected row.

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
    /// Right-side rows indexed by (join_key, -event_time_ns) for
    /// reverse-chronological iteration per join key.
    ///
    /// NOTE: no automatic eviction — ASOF join semantics require retaining
    /// all historical right rows per join key until resource contract limits
    /// are reached. A future watermark-per-join-key design could enable safe
    /// eviction when all left rows for a key have been processed.
    right_index: BTreeMap<(String, i64), TemporalRow>,
    /// Left-side rows by join key, then by event_time for bag semantics.
    /// Multiple left rows with the same join key but different event times
    /// are independently matched against right-side rows.
    left_rows: BTreeMap<String, BTreeMap<i64, TemporalRow>>,
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
        right_index: &mut BTreeMap<(String, i64), TemporalRow>,
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
        let map_key = (key.clone(), -event_time);
        right_index
            .entry(map_key.clone())
            .or_insert_with(|| TemporalRow {
                values: BTreeMap::new(),
                event_time_ns: event_time,
                weight: 0,
            });
        let entry = right_index.get_mut(&map_key).unwrap();
        entry.weight = entry
            .weight
            .checked_add(record.weight)
            .ok_or_else(invalid_runtime_state)?;
        if entry.weight < 0 {
            return Err(invalid_runtime_state());
        }
        if entry.weight == 0 {
            right_index.remove(&map_key);
            return Ok(());
        }
        entry.values = record
            .value
            .as_json()
            .as_object()
            .ok_or_else(invalid_runtime_state)?
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        entry.event_time_ns = event_time;
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
                    let by_time = next_left.entry(key.clone()).or_default();
                    let entry = by_time.entry(event_time).or_insert_with(|| TemporalRow {
                        values: BTreeMap::new(),
                        event_time_ns: event_time,
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
                        by_time.remove(&event_time);
                        if by_time.is_empty() {
                            next_left.remove(&key);
                        }
                        continue;
                    }
                    entry.values = record
                        .value
                        .as_json()
                        .as_object()
                        .ok_or_else(invalid_runtime_state)?
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect();
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

        if next_left.values().map(|m| m.len() as u64).sum::<u64>()
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
        if next_output.net_rows().map_err(|_| invalid_runtime_state())?.len() as u64
            > self.plan.resource_contract.max_output_rows_per_epoch
        {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "temporal_join_resource_contract",
            });
        }
        let prev_inv = self
            .published_output
            .inverse()
            .map_err(|_| invalid_runtime_state())?;
        let output_delta = prev_inv.combine(&next_output);
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
            right_index: self
                .right_index
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
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
            right_index: payload.right_index.into_iter().collect(),
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
    left: &BTreeMap<String, BTreeMap<i64, TemporalRow>>,
    right: &BTreeMap<(String, i64), TemporalRow>,
    plan: &SupportedTemporalJoinPlanV1,
) -> Result<DeltaBatch, StandingProgramRuntimeError> {
    let mut records = Vec::new();
    for (join_key, by_time) in left {
        for left_row in by_time.values() {
            if left_row.weight <= 0 {
                continue;
            }
            let left_event_time = left_row.event_time_ns;
            // Filter right rows by join key AND find most recent by event_time
            let probe = (join_key.clone(), -left_event_time);
            let best_right: Option<(&TemporalRow, i64)> = right
                .range(probe..)
                .next()
                .filter(|((key, _), _)| key == join_key)
                .map(|((_, _), row)| (row, row.weight));
            if let Some((right_row, right_weight)) = best_right {
                let mut output = serde_json::Map::new();
                for item in &plan.output_columns {
                    let value = match item.side {
                        TemporalJoinSideV1::Left => left_row.values.get(&item.column_id),
                        TemporalJoinSideV1::Right => right_row.values.get(&item.column_id),
                    };
                    output.insert(
                        item.output_name.clone(),
                        value.cloned().unwrap_or(Value::Null),
                    );
                }
                let weight = left_row
                    .weight
                    .checked_mul(right_weight)
                    .ok_or_else(invalid_runtime_state)?;
                records.push(DeltaRecord::new(
                    DeltaKey::from_json(Value::Object(output.clone())),
                    DeltaValue::from_json(Value::Object(serde_json::Map::new())),
                    weight,
                ));
            }
        }
    }
    Ok(DeltaBatch::from_records(records))
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
    left_rows: BTreeMap<String, BTreeMap<i64, TemporalRow>>,
    right_index: Vec<((String, i64), TemporalRow)>,
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
