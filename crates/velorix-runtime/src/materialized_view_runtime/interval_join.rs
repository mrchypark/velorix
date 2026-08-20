//! Interval overlap join runtime (Phase 8.3).
//!
//! Materializes `SELECT ... FROM left INNER JOIN right ON
//! left.start < right.end AND right.start < left.end` for non-null
//! timestamp endpoints. Every epoch applies both sides atomically and
//! recomputes the full overlap match set from the interval states, then
//! diffs against the previous output (exact, same class as the window
//! full-output diff). Retractions are exact because matches are recomputed
//! from the post-epoch states; the resource contract bounds state and
//! per-epoch work and fails the epoch closed on overflow.

use super::*;
use crate::materialized_view_runtime::{
    catalog_column, semi_anti_join::catalog_for_relation_id, validate_supported_interval_join_sql,
    SupportedIntervalJoinPlanV1,
};

pub struct IntervalJoinRuntime {
    identity: StandingProgramIdentity,
    left_catalog: VelorixRelationCatalogV1,
    right_catalog: VelorixRelationCatalogV1,
    left_input_schema: RelationSchema,
    right_input_schema: RelationSchema,
    output_schema: RelationSchema,
    view_sql: String,
    plan: SupportedIntervalJoinPlanV1,
    logical_plan: VelorixLogicalViewPlanV1,
    left_intervals: BTreeMap<String, IntervalRow>,
    right_intervals: BTreeMap<String, IntervalRow>,
    published_output: DeltaBatch,
    input_frontiers: Vec<RelationFrontier>,
    input_event_time_frontiers: Vec<InputEventTimeFrontier>,
    applied_epochs: BTreeMap<String, LogicalEpoch>,
    logical_epoch: LogicalEpoch,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct IntervalRow {
    key: Value,
    start_ns: i64,
    end_ns: i64,
    values: BTreeMap<String, Value>,
    weight: i64,
}

impl IntervalJoinRuntime {
    pub fn new_with_logical_plan(
        identity: StandingProgramIdentity,
        catalogs: Vec<VelorixRelationCatalogV1>,
        input_schemas: Vec<RelationSchema>,
        output_schema: RelationSchema,
        view_sql: String,
        plan: SupportedIntervalJoinPlanV1,
        logical_plan: VelorixLogicalViewPlanV1,
    ) -> Result<Self, StandingProgramRuntimeError> {
        identity.validate()?;
        validate_builtin_runtime_identity(&identity)?;
        validate_view_sql_hash(&identity, view_sql.as_str())?;
        validate_logical_view_plan(&logical_plan).map_err(|_| {
            StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "logical_interval_join_plan",
            }
        })?;
        validate_interval_join_contract(&catalogs, &input_schemas, &output_schema, &plan)?;
        let compiled =
            validate_supported_interval_join_sql(view_sql.as_str(), &catalogs).map_err(|_| {
                StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "interval_join_plan",
                }
            })?;
        if compiled != plan {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "interval_join_plan",
            });
        }
        let (left_catalog, right_catalog) = match catalogs.as_slice() {
            [left, right] => (left.clone(), right.clone()),
            _ => {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "interval_join_catalogs",
                })
            }
        };
        let (left_input_schema, right_input_schema) = match input_schemas.as_slice() {
            [left, right] => (left.clone(), right.clone()),
            _ => {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "interval_join_input_schemas",
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
            left_intervals: BTreeMap::new(),
            right_intervals: BTreeMap::new(),
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

    fn apply_side(
        intervals: &mut BTreeMap<String, IntervalRow>,
        start_column: &str,
        end_column: &str,
        record: &DeltaRecord,
    ) -> Result<(), StandingProgramRuntimeError> {
        let key = canonical_json(record.key.as_json());
        let entry = intervals.entry(key.clone()).or_insert_with(|| IntervalRow {
            key: record.key.as_json().clone(),
            start_ns: 0,
            end_ns: 0,
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
            intervals.remove(&key);
            return Ok(());
        }
        let value = record
            .value
            .as_json()
            .as_object()
            .ok_or_else(invalid_runtime_state)?;
        let start = value
            .get(start_column)
            .and_then(Value::as_i64)
            .ok_or_else(invalid_runtime_state)?;
        let end = value
            .get(end_column)
            .and_then(Value::as_i64)
            .ok_or_else(invalid_runtime_state)?;
        if start >= end {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "interval_join_endpoint_order",
            });
        }
        entry.start_ns = start;
        entry.end_ns = end;
        entry.values = value
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        Ok(())
    }

    fn recompute_all_matches(
        &self,
        left: &BTreeMap<String, IntervalRow>,
        right: &BTreeMap<String, IntervalRow>,
    ) -> Result<DeltaBatch, StandingProgramRuntimeError> {
        if u64::try_from(left.len()).unwrap_or(u64::MAX)
            > self.plan.resource_contract.max_intervals_per_side
            || u64::try_from(right.len()).unwrap_or(u64::MAX)
                > self.plan.resource_contract.max_intervals_per_side
        {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "interval_join_resource_contract",
            });
        }
        let mut records = Vec::new();
        for left_row in left.values() {
            for right_row in right.values() {
                let overlaps =
                    left_row.start_ns < right_row.end_ns && right_row.start_ns < left_row.end_ns;
                if !overlaps {
                    continue;
                }
                // Every match is keyed by its full projected row, so the
                // output schema primary key must cover all output columns
                // (uniqueness per (left row, right row) pair).
                let mut output = serde_json::Map::new();
                for column in &self.plan.output_columns {
                    let value = left_row
                        .values
                        .get(&column.left_column_id)
                        .ok_or_else(invalid_runtime_state)?;
                    output.insert(column.output_name.clone(), value.clone());
                }
                let right_key = right_row
                    .values
                    .get(&self.plan.right_key_column_id)
                    .ok_or_else(invalid_runtime_state)?;
                output.insert(self.plan.right_key_output_name.clone(), right_key.clone());
                records.push(DeltaRecord::new(
                    DeltaKey::from_json(Value::Object(output)),
                    DeltaValue::from_json(Value::Object(serde_json::Map::new())),
                    left_row
                        .weight
                        .checked_mul(right_row.weight)
                        .ok_or_else(invalid_runtime_state)?,
                ));
                if records.len() as u64 > self.plan.resource_contract.max_matches_per_epoch {
                    return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                        field: "interval_join_resource_contract",
                    });
                }
            }
        }
        Ok(DeltaBatch::from_records(records))
    }

    /// Left columns the runtime must carry in state: the output projection
    /// plus the left endpoints.
    fn left_state_columns(&self) -> Vec<String> {
        let mut columns = BTreeSet::new();
        for column in &self.plan.output_columns {
            columns.insert(column.left_column_id.clone());
        }
        columns.insert(self.plan.left_start_column_id.clone());
        columns.insert(self.plan.left_end_column_id.clone());
        columns.into_iter().collect()
    }

    /// Right columns the runtime must carry in state: the right endpoints
    /// and the right key.
    fn right_state_columns(&self) -> Vec<String> {
        let mut columns = BTreeSet::new();
        columns.insert(self.plan.right_start_column_id.clone());
        columns.insert(self.plan.right_end_column_id.clone());
        columns.insert(self.plan.right_key_column_id.clone());
        columns.into_iter().collect()
    }
}

impl StandingProgramRuntime for IntervalJoinRuntime {
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
        let left_value_columns = self.left_state_columns();
        let right_value_columns = self.right_state_columns();
        // Stage both sides in clones so a failed epoch leaves the runtime
        // state untouched and the same epoch can be retried exactly once.
        let mut next_left = self.left_intervals.clone();
        let mut next_right = self.right_intervals.clone();
        for input in &input_changes {
            let (side, schema, catalog, start_column, end_column, intervals) =
                if input.relation_id == self.plan.left_input_relation_id {
                    (
                        "left",
                        &self.left_input_schema,
                        &self.left_catalog,
                        self.plan.left_start_column_id.as_str(),
                        self.plan.left_end_column_id.as_str(),
                        &mut next_left,
                    )
                } else if input.relation_id == self.plan.right_input_relation_id {
                    (
                        "right",
                        &self.right_input_schema,
                        &self.right_catalog,
                        self.plan.right_start_column_id.as_str(),
                        self.plan.right_end_column_id.as_str(),
                        &mut next_right,
                    )
                } else {
                    return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                        field: "interval_join_input_relation",
                    });
                };
            validate_input_matches_schema(input, schema, "interval_join_input")?;
            let delta = if let Some(empty_delta) = published_input_empty_delta(input, catalog)? {
                empty_delta
            } else {
                let mut columns = BTreeSet::new();
                columns.insert(start_column.to_string());
                columns.insert(end_column.to_string());
                columns.extend(
                    (if side == "left" {
                        &left_value_columns
                    } else {
                        &right_value_columns
                    })
                    .iter()
                    .cloned(),
                );
                arrow_record_batches_to_key_multi_value_delta_batch(
                    catalog,
                    &input.relation_id,
                    &input.relation_version,
                    &input.schema_fingerprint,
                    if side == "left" {
                        std::slice::from_ref(&self.plan.left_key_column_id)
                    } else {
                        std::slice::from_ref(&self.plan.right_key_column_id)
                    },
                    &columns.into_iter().collect::<Vec<_>>(),
                    &input.batches,
                )
                .map_err(|_| {
                    StandingProgramRuntimeError::InvalidProgramIdentity {
                        field: "interval_join_input_batch",
                    }
                })?
            };
            for record in delta.net_rows().map_err(|_| invalid_runtime_state())? {
                Self::apply_side(intervals, start_column, end_column, &record)?;
            }
            advance_input_frontier(&mut next_frontiers, input)?;
            advance_input_event_time_frontier(&mut next_event_time_frontiers, input)?;
        }
        let next_output = self.recompute_all_matches(&next_left, &next_right)?;
        let output_delta = self
            .published_output
            .inverse()
            .map_err(|_| invalid_runtime_state())?
            .combine(&next_output);
        self.left_intervals = next_left;
        self.right_intervals = next_right;
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
        let payload = IntervalJoinCheckpointPayload {
            schema_version: CHECKPOINT_PAYLOAD_SCHEMA_VERSION,
            runtime_kind: INTERVAL_JOIN_RUNTIME_KIND.to_string(),
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
            left_intervals: self.left_intervals.clone(),
            right_intervals: self.right_intervals.clone(),
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
        let payload: IntervalJoinCheckpointPayload =
            serde_json::from_str(&state_payload.payload).map_err(|_| invalid_checkpoint())?;
        if payload.schema_version != CHECKPOINT_PAYLOAD_SCHEMA_VERSION
            || payload.runtime_kind != INTERVAL_JOIN_RUNTIME_KIND
        {
            return Err(invalid_checkpoint());
        }
        validate_interval_join_contract(
            &[payload.left_catalog.clone(), payload.right_catalog.clone()],
            &[
                payload.left_input_schema.clone(),
                payload.right_input_schema.clone(),
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
        let compiled = validate_supported_interval_join_sql(
            payload.view_sql.as_str(),
            &[payload.left_catalog.clone(), payload.right_catalog.clone()],
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
            left_catalog: payload.left_catalog,
            right_catalog: payload.right_catalog,
            left_input_schema: payload.left_input_schema,
            right_input_schema: payload.right_input_schema,
            output_schema: payload.output_schema,
            view_sql: payload.view_sql,
            plan: payload.plan,
            logical_plan: payload.logical_plan,
            left_intervals: payload.left_intervals,
            right_intervals: payload.right_intervals,
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
struct IntervalJoinCheckpointPayload {
    schema_version: u32,
    runtime_kind: String,
    left_catalog: VelorixRelationCatalogV1,
    right_catalog: VelorixRelationCatalogV1,
    left_input_schema: RelationSchema,
    right_input_schema: RelationSchema,
    output_schema: RelationSchema,
    view_sql: String,
    plan: SupportedIntervalJoinPlanV1,
    logical_plan: VelorixLogicalViewPlanV1,
    input_frontiers: Vec<RelationFrontier>,
    input_event_time_frontiers: Vec<InputEventTimeFrontier>,
    left_intervals: BTreeMap<String, IntervalRow>,
    right_intervals: BTreeMap<String, IntervalRow>,
    published_output: DeltaBatch,
    applied_epochs: Vec<GenericAppliedEpoch>,
    logical_epoch: LogicalEpoch,
}

pub(super) const INTERVAL_JOIN_RUNTIME_KIND: &str = "interval_join";

fn validate_interval_join_contract(
    catalogs: &[VelorixRelationCatalogV1],
    input_schemas: &[RelationSchema],
    output_schema: &RelationSchema,
    plan: &SupportedIntervalJoinPlanV1,
) -> Result<(), StandingProgramRuntimeError> {
    if plan.schema_version != 1 || catalogs.len() != 2 || input_schemas.len() != 2 {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "interval_join_contract",
        });
    }
    let left_catalog = catalog_for_relation_id(catalogs, &plan.left_input_relation_id)?;
    let right_catalog = catalog_for_relation_id(catalogs, &plan.right_input_relation_id)?;
    if left_catalog.relation_schema.relation_id == right_catalog.relation_schema.relation_id {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "interval_join_contract",
        });
    }
    catalog_column(left_catalog, &plan.left_key_column_id)?;
    catalog_column(left_catalog, &plan.left_start_column_id)?;
    catalog_column(left_catalog, &plan.left_end_column_id)?;
    catalog_column(right_catalog, &plan.right_key_column_id)?;
    catalog_column(right_catalog, &plan.right_start_column_id)?;
    catalog_column(right_catalog, &plan.right_end_column_id)?;
    if plan.output_columns.is_empty() {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "interval_join_contract",
        });
    }
    let mut output_names = BTreeSet::new();
    for column in &plan.output_columns {
        catalog_column(left_catalog, &column.left_column_id)?;
        if !output_names.insert(column.output_name.clone())
            || column.output_name == plan.right_key_output_name
        {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "interval_join_contract",
            });
        }
    }
    let expected_left = catalog_input_relation_schema(left_catalog).map_err(|_| {
        StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "interval_join_left_input_schema",
        }
    })?;
    let expected_right = catalog_input_relation_schema(right_catalog).map_err(|_| {
        StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "interval_join_right_input_schema",
        }
    })?;
    if expected_left != input_schemas[0] || expected_right != input_schemas[1] {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "interval_join_input_schemas",
        });
    }
    let _ = output_schema;
    Ok(())
}
