//! Recursive CTE fixpoint runtime (Phase 8.5).
//!
//! Materializes `WITH RECURSIVE r AS (anchor UNION DISTINCT term)
//! SELECT ... FROM r ...` for a positive anchor/recursive term over one
//! registered base relation. Every epoch applies the signed base deltas
//! atomically (clone-and-swap), recomputes the anchor and the closure
//! (set semantics, canonical iteration order, bounded work units), then
//! diffs the new derived set against the previous one. Retractions are
//! exact because the closure is a pure function of the base multiset and
//! the derived set is recomputed, not incrementally maintained.

use super::*;

pub struct RecursiveFixpointRuntime {
    identity: StandingProgramIdentity,
    catalog: VelorixRelationCatalogV1,
    input_schema: RelationSchema,
    output_schema: RelationSchema,
    view_sql: String,
    plan: SupportedRecursiveFixpointPlanV1,
    logical_plan: VelorixLogicalViewPlanV1,
    base_multiset: BTreeMap<String, RecursiveBaseRow>,
    derived_set: BTreeMap<String, Value>,
    published_output: DeltaBatch,
    input_frontiers: Vec<RelationFrontier>,
    input_event_time_frontiers: Vec<InputEventTimeFrontier>,
    applied_epochs: BTreeMap<String, LogicalEpoch>,
    logical_epoch: LogicalEpoch,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RecursiveBaseRow {
    values: BTreeMap<String, Value>,
    weight: i64,
}

impl RecursiveFixpointRuntime {
    pub fn new_with_logical_plan(
        identity: StandingProgramIdentity,
        catalog: VelorixRelationCatalogV1,
        input_schema: RelationSchema,
        output_schema: RelationSchema,
        view_sql: String,
        plan: SupportedRecursiveFixpointPlanV1,
        logical_plan: VelorixLogicalViewPlanV1,
    ) -> Result<Self, StandingProgramRuntimeError> {
        identity.validate()?;
        validate_builtin_runtime_identity(&identity)?;
        validate_view_sql_hash(&identity, view_sql.as_str())?;
        validate_logical_view_plan(&logical_plan).map_err(|_| {
            StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "logical_recursive_fixpoint_plan",
            }
        })?;
        validate_recursive_fixpoint_contract(&catalog, &input_schema, &plan)?;
        let compiled =
            validate_supported_recursive_cte_sql(view_sql.as_str(), &catalog).map_err(|_| {
                StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "recursive_fixpoint_plan",
                }
            })?;
        if compiled != plan {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "recursive_fixpoint_plan",
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
            base_multiset: BTreeMap::new(),
            derived_set: BTreeMap::new(),
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

    fn apply_base_delta(&mut self, delta: &DeltaBatch) -> Result<(), StandingProgramRuntimeError> {
        let mut next = self.base_multiset.clone();
        for record in delta.net_rows().map_err(|_| invalid_runtime_state())? {
            let key = canonical_json(record.key.as_json());
            let entry = next.entry(key.clone()).or_insert_with(|| RecursiveBaseRow {
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
                next.remove(&key);
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
        self.base_multiset = next;
        Ok(())
    }

    fn anchor_row(
        &self,
        base: &RecursiveBaseRow,
        predicate: &[RecursiveBasePredicateV1],
    ) -> Result<Option<Value>, StandingProgramRuntimeError> {
        if !recursive_base_predicates_match(base, predicate)? {
            return Ok(None);
        }
        let mut row = serde_json::Map::new();
        for (index, column_id) in self.plan.anchor_projection.iter().enumerate() {
            let value = base
                .values
                .get(column_id)
                .ok_or_else(invalid_runtime_state)?;
            row.insert(
                self.plan.recursion_column_names[index].clone(),
                value.clone(),
            );
        }
        Ok(Some(Value::Object(row)))
    }

    fn recursive_candidate(
        &self,
        derived: &Value,
        base: &RecursiveBaseRow,
    ) -> Result<Option<Value>, StandingProgramRuntimeError> {
        if !recursive_base_predicates_match(base, &self.plan.recursive_base_predicate)? {
            return Ok(None);
        }
        let derived_object = derived.as_object().ok_or_else(invalid_runtime_state)?;
        let base_join = base
            .values
            .get(&self.plan.recursive_join.base_column_id)
            .ok_or_else(invalid_runtime_state)?;
        let derived_join = derived_object
            .get(&self.plan.recursive_join.recursive_column_id)
            .ok_or_else(invalid_runtime_state)?;
        if derived_join != base_join {
            return Ok(None);
        }
        let mut row = serde_json::Map::new();
        for (index, item) in self.plan.recursive_projection.iter().enumerate() {
            let value = match item {
                RecursiveProjectionItemV1::Recursive { column_id } => derived_object
                    .get(column_id)
                    .ok_or_else(invalid_runtime_state)?,
                RecursiveProjectionItemV1::Base { column_id } => base
                    .values
                    .get(column_id)
                    .ok_or_else(invalid_runtime_state)?,
            };
            row.insert(
                self.plan.recursion_column_names[index].clone(),
                value.clone(),
            );
        }
        Ok(Some(Value::Object(row)))
    }

    fn recompute_closure(&self) -> Result<BTreeMap<String, Value>, StandingProgramRuntimeError> {
        let mut derived: BTreeMap<String, Value> = BTreeMap::new();
        for base in self.base_multiset.values() {
            if let Some(row) = self.anchor_row(base, &self.plan.anchor_base_predicate)? {
                derived.insert(canonical_json(&row), row);
            }
        }
        if derived.len() as u64 > self.plan.resource_contract.max_derived_rows {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "recursive_fixpoint_resource_contract",
            });
        }
        let mut work_units: u64 = 0;
        for iteration in 0..self.plan.resource_contract.max_iterations {
            let mut frontier = Vec::new();
            for row in derived.values() {
                for base in self.base_multiset.values() {
                    if base.weight <= 0 {
                        continue;
                    }
                    work_units = work_units
                        .checked_add(1)
                        .ok_or_else(invalid_runtime_state)?;
                    if work_units > self.plan.resource_contract.max_work_units_per_epoch {
                        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                            field: "recursive_fixpoint_resource_contract",
                        });
                    }
                    let Some(candidate) = self.recursive_candidate(row, base)? else {
                        continue;
                    };
                    let key = canonical_json(&candidate);
                    if !derived.contains_key(&key) {
                        frontier.push((key, candidate));
                    }
                }
            }
            if frontier.is_empty() {
                break;
            }
            if iteration + 1 >= self.plan.resource_contract.max_iterations {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "recursive_fixpoint_resource_contract",
                });
            }
            for (key, row) in frontier {
                derived.insert(key, row);
                if derived.len() as u64 > self.plan.resource_contract.max_derived_rows {
                    return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                        field: "recursive_fixpoint_resource_contract",
                    });
                }
            }
        }
        Ok(derived)
    }

    fn output_delta_for_derived(
        &self,
        derived: &BTreeMap<String, Value>,
    ) -> Result<DeltaBatch, StandingProgramRuntimeError> {
        let previous = self
            .published_output
            .net_rows()
            .map_err(|_| invalid_runtime_state())?;
        let mut previous_keys = BTreeSet::new();
        for record in &previous {
            previous_keys.insert(canonical_json(record.key.as_json()));
        }
        let mut records = Vec::new();
        for (key, row) in derived {
            if previous_keys.contains(key) {
                continue;
            }
            records.push(DeltaRecord::new(
                DeltaKey::from_json(row.clone()),
                DeltaValue::from_json(Value::Object(serde_json::Map::new())),
                1,
            ));
        }
        for key in &previous_keys {
            if derived.contains_key(key) {
                continue;
            }
            let row = previous
                .iter()
                .find(|record| canonical_json(record.key.as_json()) == *key)
                .ok_or_else(invalid_runtime_state)?;
            records.push(DeltaRecord::new(
                DeltaKey::from_json(row.key.as_json().clone()),
                DeltaValue::from_json(Value::Object(serde_json::Map::new())),
                -1,
            ));
        }
        Ok(DeltaBatch::from_records(records))
    }
}

impl StandingProgramRuntime for RecursiveFixpointRuntime {
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
            if input.relation_id != self.plan.input_relation_id {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "recursive_fixpoint_input_relation",
                });
            }
            validate_input_matches_schema(input, &self.input_schema, "recursive_fixpoint_input")?;
            let delta =
                if let Some(empty_delta) = published_input_empty_delta(input, &self.catalog)? {
                    empty_delta
                } else {
                    let columns = self.base_multiset_columns();
                    arrow_record_batches_to_key_multi_value_delta_batch(
                        &self.catalog,
                        &input.relation_id,
                        &input.relation_version,
                        &input.schema_fingerprint,
                        std::slice::from_ref(&self.plan_primary_key_column_id()),
                        &columns,
                        &input.batches,
                    )
                    .map_err(|_| {
                        StandingProgramRuntimeError::InvalidProgramIdentity {
                            field: "recursive_fixpoint_input_batch",
                        }
                    })?
                };
            self.apply_base_delta(&delta)?;
            advance_input_frontier(&mut next_frontiers, input)?;
            advance_input_event_time_frontier(&mut next_event_time_frontiers, input)?;
        }
        let next_derived = self.recompute_closure()?;
        let next_output = DeltaBatch::from_records(
            next_derived
                .values()
                .map(|row| {
                    DeltaRecord::new(
                        DeltaKey::from_json(row.clone()),
                        DeltaValue::from_json(Value::Object(serde_json::Map::new())),
                        1,
                    )
                })
                .collect::<Vec<_>>(),
        );
        let output_delta = self.output_delta_for_derived(&next_derived)?;
        self.derived_set = next_derived;
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
        let payload = RecursiveFixpointCheckpointPayloadV2 {
            schema_version: CHECKPOINT_PAYLOAD_SCHEMA_VERSION,
            runtime_kind: RECURSIVE_FIXPOINT_RUNTIME_KIND.to_string(),
            catalog: self.catalog.clone(),
            input_schema: self.input_schema.clone(),
            output_schema: self.output_schema.clone(),
            view_sql: self.view_sql.clone(),
            plan: self.plan.clone(),
            logical_plan: self.logical_plan.clone(),
            input_frontiers: self.input_frontiers.clone(),
            input_event_time_frontiers: self.input_event_time_frontiers.clone(),
            base_multiset: self.base_multiset.clone(),
            derived_set: self
                .derived_set
                .iter()
                .map(|(key, row)| (key.clone(), row.clone()))
                .collect(),
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
        let payload: RecursiveFixpointCheckpointPayloadV2 =
            serde_json::from_str(&state_payload.payload).map_err(|_| invalid_checkpoint())?;
        if payload.schema_version != CHECKPOINT_PAYLOAD_SCHEMA_VERSION
            || payload.runtime_kind != RECURSIVE_FIXPOINT_RUNTIME_KIND
        {
            return Err(invalid_checkpoint());
        }
        validate_recursive_fixpoint_contract(
            &payload.catalog,
            &payload.input_schema,
            &payload.plan,
        )?;
        if payload.logical_epoch != checkpoint.logical_epoch
            || payload.input_frontiers != checkpoint.input_frontiers
            || payload.input_event_time_frontiers != checkpoint.input_event_time_frontiers
        {
            return Err(invalid_checkpoint());
        }
        validate_view_sql_hash(&checkpoint.identity, payload.view_sql.as_str())?;
        let compiled =
            validate_supported_recursive_cte_sql(payload.view_sql.as_str(), &payload.catalog)
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
            base_multiset: payload.base_multiset,
            derived_set: payload.derived_set,
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
struct RecursiveFixpointCheckpointPayloadV2 {
    schema_version: u32,
    runtime_kind: String,
    catalog: VelorixRelationCatalogV1,
    input_schema: RelationSchema,
    output_schema: RelationSchema,
    view_sql: String,
    plan: SupportedRecursiveFixpointPlanV1,
    logical_plan: VelorixLogicalViewPlanV1,
    input_frontiers: Vec<RelationFrontier>,
    input_event_time_frontiers: Vec<InputEventTimeFrontier>,
    base_multiset: BTreeMap<String, RecursiveBaseRow>,
    derived_set: BTreeMap<String, Value>,
    published_output: DeltaBatch,
    applied_epochs: Vec<GenericAppliedEpoch>,
    logical_epoch: LogicalEpoch,
}

pub(super) const RECURSIVE_FIXPOINT_RUNTIME_KIND: &str = "recursive_fixpoint_v2";

fn validate_recursive_fixpoint_contract(
    catalog: &VelorixRelationCatalogV1,
    input_schema: &RelationSchema,
    plan: &SupportedRecursiveFixpointPlanV1,
) -> Result<(), StandingProgramRuntimeError> {
    if plan.schema_version != 1 || plan.input_relation_id != catalog.relation_schema.relation_id {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "recursive_fixpoint_contract",
        });
    }
    let expected_schema = catalog_input_relation_schema(catalog).map_err(|_| {
        StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "recursive_fixpoint_input_schema",
        }
    })?;
    if expected_schema != *input_schema {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "recursive_fixpoint_input_schema",
        });
    }
    if plan.recursion_column_names.is_empty()
        || plan.anchor_projection.len() != plan.recursion_column_names.len()
        || plan.recursive_projection.len() != plan.recursion_column_names.len()
    {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "recursive_fixpoint_contract",
        });
    }
    for column_id in &plan.anchor_projection {
        catalog_column(catalog, column_id)?;
    }
    for item in &plan.recursive_projection {
        match item {
            RecursiveProjectionItemV1::Base { column_id } => {
                catalog_column(catalog, column_id)?;
            }
            RecursiveProjectionItemV1::Recursive { .. } => {}
        }
    }
    catalog_column(catalog, &plan.recursive_join.base_column_id)?;
    for predicate in plan
        .anchor_base_predicate
        .iter()
        .chain(plan.recursive_base_predicate.iter())
    {
        catalog_column(catalog, &predicate.base_column_id)?;
    }
    Ok(())
}

fn compare_predicate<T: PartialOrd>(
    op: PredicateOp,
    actual: T,
    expected: T,
) -> Result<bool, StandingProgramRuntimeError> {
    match op {
        PredicateOp::Eq => Ok(actual == expected),
        PredicateOp::NotEq => Ok(actual != expected),
        PredicateOp::Lt => Ok(actual < expected),
        PredicateOp::LtEq => Ok(actual <= expected),
        PredicateOp::Gt => Ok(actual > expected),
        PredicateOp::GtEq => Ok(actual >= expected),
        _ => Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "recursive_fixpoint_predicate",
        }),
    }
}

fn recursive_base_predicates_match(
    base: &RecursiveBaseRow,
    predicates: &[RecursiveBasePredicateV1],
) -> Result<bool, StandingProgramRuntimeError> {
    for predicate in predicates {
        let value = base
            .values
            .get(&predicate.base_column_id)
            .ok_or_else(invalid_runtime_state)?;
        let matched = match (value, &predicate.literal) {
            (Value::Number(actual), Value::Number(expected)) => {
                let actual = actual.as_i64().ok_or_else(invalid_runtime_state)?;
                let expected = expected.as_i64().ok_or_else(invalid_runtime_state)?;
                compare_predicate(predicate.op, actual, expected)?
            }
            (Value::String(actual), Value::String(expected)) => {
                compare_predicate(predicate.op, actual, expected)?
            }
            _ => {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "recursive_fixpoint_predicate",
                })
            }
        };
        if !matched {
            return Ok(false);
        }
    }
    Ok(true)
}

impl RecursiveFixpointRuntime {
    fn base_multiset_columns(&self) -> Vec<String> {
        let mut columns = self
            .catalog
            .relation_schema
            .columns
            .iter()
            .map(|column| column.column_id.clone())
            .filter(|column_id| *column_id != self.catalog.relation_schema.weight_column_id)
            .collect::<Vec<_>>();
        columns.sort();
        columns
    }

    fn plan_primary_key_column_id(&self) -> String {
        self.catalog.relation_schema.primary_key_column_ids[0].clone()
    }
}
