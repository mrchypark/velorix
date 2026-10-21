use super::*;
use velorix_core::delta::{encode_kv_ordered, TypedBinaryKey};

type Bag = BTreeMap<TypedBinaryKey, DeltaRecord>;

pub(super) const RUNTIME_KIND: &str = "generic_set_ops_v1";

#[derive(Serialize, Deserialize)]
struct Checkpoint {
    schema_version: u32,
    runtime_kind: String,
    catalogs: Vec<VelorixRelationCatalogV1>,
    input_schemas: Vec<RelationSchema>,
    output_schema: RelationSchema,
    logical_plan: VelorixLogicalViewPlanV1,
    epochs: EpochState,
    sources: Vec<DeltaBatch>,
    output: DeltaBatch,
}

// Keep field order and the DeltaBatch { records: [...] } wire shape identical to
// Checkpoint. Stream retained rows in bag order without cloning their payloads.
#[derive(Serialize)]
struct CheckpointRef<'a> {
    schema_version: u32,
    runtime_kind: &'a str,
    catalogs: &'a [VelorixRelationCatalogV1],
    input_schemas: &'a [RelationSchema],
    output_schema: &'a RelationSchema,
    logical_plan: &'a VelorixLogicalViewPlanV1,
    epochs: &'a EpochState,
    #[serde(serialize_with = "serialize_source_bags")]
    sources: &'a [Bag],
    output: BagRef<'a>,
}

#[derive(Serialize)]
struct BagRef<'a> {
    #[serde(serialize_with = "serialize_bag_records")]
    records: &'a Bag,
}

fn serialize_bag_records<S: serde::Serializer>(
    bag: &Bag,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.collect_seq(bag.values())
}

fn serialize_source_bags<S: serde::Serializer>(
    sources: &[Bag],
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.collect_seq(sources.iter().map(|bag| BagRef { records: bag }))
}

#[derive(Clone)]
pub(super) struct SetOpsRuntime {
    limits: RuntimeResourceLimits,
    identity: StandingProgramIdentity,
    catalogs: Vec<VelorixRelationCatalogV1>,
    input_schemas: Vec<RelationSchema>,
    output_schema: RelationSchema,
    logical_plan: VelorixLogicalViewPlanV1,
    epochs: EpochState,
    sources: Vec<Bag>,
    output: Bag,
}

impl SetOpsRuntime {
    #[cfg(test)]
    pub(super) fn new(
        identity: StandingProgramIdentity,
        catalogs: Vec<VelorixRelationCatalogV1>,
        input_schemas: Vec<RelationSchema>,
        output_schema: RelationSchema,
        logical_plan: VelorixLogicalViewPlanV1,
    ) -> Result<Self, StandingProgramRuntimeError> {
        Self::new_with_limits(
            identity,
            catalogs,
            input_schemas,
            output_schema,
            logical_plan,
            RuntimeResourceLimits::default(),
        )
    }

    pub(super) fn new_with_limits(
        identity: StandingProgramIdentity,
        catalogs: Vec<VelorixRelationCatalogV1>,
        input_schemas: Vec<RelationSchema>,
        output_schema: RelationSchema,
        logical_plan: VelorixLogicalViewPlanV1,
        limits: RuntimeResourceLimits,
    ) -> Result<Self, StandingProgramRuntimeError> {
        limits.validate()?;
        identity.validate()?;
        validate_builtin_runtime_identity(&identity)?;
        validate_view_sql_hash(&identity, &logical_plan.view_sql)?;
        validate_logical_view_plan(&logical_plan).map_err(|_| invalid_runtime_state())?;
        let bound_outputs = logical_plan
            .nodes
            .iter()
            .filter_map(|node| match node {
                VelorixLogicalViewPlanNodeV1::TypedMap { output_schema, .. }
                | VelorixLogicalViewPlanNodeV1::Series { output_schema, .. }
                | VelorixLogicalViewPlanNodeV1::UnionAll { output_schema, .. } => {
                    Some(output_schema)
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        if bound_outputs.as_slice() != [&output_schema]
            || identity.view_ids.as_slice() != [output_schema.relation_id.clone()]
        {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "set_ops_output_schema",
            });
        }
        let (schemas, columns) = match &logical_plan.execution {
            VelorixLogicalViewExecutionV1::TypedMap { plan } => {
                let [catalog] = catalogs.as_slice() else {
                    return Err(invalid_runtime_state());
                };
                let compiled =
                    velorix_core::view_plan::lower_supported_typed_map_sql_to_logical_plan(
                        &logical_plan.view_sql,
                        catalog,
                        &output_schema,
                    )
                    .map_err(|_| invalid_runtime_state())?;
                if compiled != logical_plan {
                    return Err(invalid_runtime_state());
                }
                (vec![&plan.input_schema], &plan.output_columns)
            }
            VelorixLogicalViewExecutionV1::Series { plan } => {
                if plan.schema_version != 1 || plan.max_emitted_rows_per_input == 0 {
                    return Err(invalid_runtime_state());
                }
                let [catalog] = catalogs.as_slice() else {
                    return Err(invalid_runtime_state());
                };
                if velorix_core::view_plan::validate_supported_series_sql_with_budget(
                    &logical_plan.view_sql,
                    catalog,
                    plan.max_emitted_rows_per_input,
                )
                .map_err(|_| invalid_runtime_state())?
                    != **plan
                {
                    return Err(invalid_runtime_state());
                }
                (vec![&plan.input_schema], &plan.output_columns)
            }
            VelorixLogicalViewExecutionV1::UnionAll { plan } => {
                if plan.schema_version != 1 || plan.branches.len() < 2 {
                    return Err(invalid_runtime_state());
                }
                if velorix_core::view_plan::validate_supported_union_all_sql(
                    &logical_plan.view_sql,
                    &catalogs,
                )
                .map_err(|_| invalid_runtime_state())?
                    != **plan
                {
                    return Err(invalid_runtime_state());
                }
                (
                    plan.branches
                        .iter()
                        .map(|branch| &branch.input_schema)
                        .collect(),
                    &plan.output_columns,
                )
            }
            _ => return Err(invalid_runtime_state()),
        };
        if columns != &output_schema.columns
            || columns.is_empty()
            || catalogs.len() != input_schemas.len()
            || schemas.iter().any(|schema| !input_schemas.contains(schema))
            || input_schemas
                .iter()
                .any(|schema| !schemas.contains(&schema))
        {
            return Err(invalid_runtime_state());
        }
        for (index, schema) in input_schemas.iter().enumerate() {
            if catalog_input_relation_schema(&catalogs[index])
                .map_err(|_| invalid_runtime_state())?
                != *schema
                || input_schemas[..index]
                    .iter()
                    .any(|previous| previous.relation_id == schema.relation_id)
            {
                return Err(invalid_runtime_state());
            }
        }
        let sources = vec![Bag::new(); input_schemas.len()];
        Ok(Self {
            limits,
            identity,
            catalogs,
            input_schemas,
            output_schema,
            logical_plan,
            epochs: EpochState::default(),
            sources,
            output: Bag::new(),
        })
    }

    pub(super) fn materialized_delta(&self) -> DeltaBatch {
        DeltaBatch::from_records(self.output.values().cloned())
    }

    pub(super) fn into_materialized_delta(self) -> DeltaBatch {
        DeltaBatch::from_records(self.output.into_values())
    }

    fn decode(
        &self,
        index: usize,
        input: &RelationInputBatch,
    ) -> Result<DeltaBatch, StandingProgramRuntimeError> {
        let catalog = &self.catalogs[index];
        if let Some(empty) = published_input_empty_delta(input, catalog)? {
            return Ok(empty);
        }
        if input.batches.is_empty() {
            return Err(invalid_runtime_state());
        }
        let mut rows = Vec::new();
        for batch in &input.batches {
            validate_record_batch_matches_catalog(catalog, batch)
                .map_err(|_| invalid_runtime_state())?;
            let weight = catalog_column(catalog, &catalog.relation_schema.weight_column_id)?;
            let weights = batch
                .column_by_name(&weight.name)
                .and_then(|array| array.as_any().downcast_ref::<Int64Array>())
                .ok_or_else(invalid_runtime_state)?;
            for index in 0..batch.num_rows() {
                if weights.is_null(index) {
                    return Err(invalid_runtime_state());
                }
                let values = catalog
                    .relation_schema
                    .columns
                    .iter()
                    .filter(|column| column.column_id != catalog.relation_schema.weight_column_id)
                    .map(|column| {
                        Ok((
                            column.column_id.clone(),
                            two_input_join::typed_join_arrow_value(batch, column, index)?,
                        ))
                    })
                    .collect::<Result<Map<_, _>, StandingProgramRuntimeError>>()?;
                rows.push(DeltaRecord::new(
                    DeltaKey::from_json(Value::Null),
                    DeltaValue::from_json(Value::Object(values)),
                    weights.value(index),
                ));
            }
        }
        Ok(DeltaBatch::from_records(rows))
    }

    fn project(
        &self,
        index: usize,
        input: &DeltaBatch,
        emitted_bytes: &mut usize,
        emitted_rows: &mut usize,
    ) -> Result<DeltaBatch, StandingProgramRuntimeError> {
        self.project_bag(index, input.records(), emitted_bytes, emitted_rows)
            .map(|rows| DeltaBatch::from_records(rows.into_values()))
    }

    fn project_bag(
        &self,
        index: usize,
        input: &[DeltaRecord],
        emitted_bytes: &mut usize,
        emitted_rows: &mut usize,
    ) -> Result<Bag, StandingProgramRuntimeError> {
        let catalog = &self.catalogs[index];
        let mut rows: Bag = BTreeMap::new();
        for row in input {
            let get = |id: &str| {
                let column = catalog_column(catalog, id).ok()?;
                let value = row.value.as_json().get(id)?;
                if let ArrowPhysicalTypeV1::Decimal128 { precision, scale } =
                    column.physical_arrow_type
                {
                    return if value.is_null() {
                        Some(RuntimeScalarValue::Null(RuntimeScalarTypeV1::Decimal128 {
                            precision,
                            scale: i8::try_from(scale).ok()?,
                        }))
                    } else {
                        Some(RuntimeScalarValue::Decimal128 {
                            unscaled: parse_decimal128(value.as_str()?, precision, scale)?,
                            scale: i8::try_from(scale).ok()?,
                        })
                    };
                }
                typed_json_value_to_scalar(value.clone(), Some(&column.physical_arrow_type))
            };
            let eval = |expr: &TypedExprProgramV1| {
                evaluate_typed_expr(expr, &get).map_err(|_| invalid_runtime_state())
            };
            let mut emit = |values: Map<String, Value>| -> Result<(), StandingProgramRuntimeError> {
                let mut record = DeltaRecord::new(
                    DeltaKey::from_json(Value::Object(values)),
                    DeltaValue::from_json(Value::Null),
                    row.weight,
                );
                let key = encode_kv_ordered(record.key.as_json(), record.value.as_json());
                self.limits.charge_expansion(
                    emitted_bytes,
                    emitted_rows,
                    resource_limits::row_width(&record)?,
                    1,
                )?;
                record.weight = rows
                    .get(&key)
                    .map_or(0, |row| row.weight)
                    .checked_add(record.weight)
                    .ok_or_else(invalid_runtime_state)?;
                if record.weight == 0 {
                    rows.remove(&key);
                } else {
                    rows.insert(key, record);
                }
                Ok(())
            };
            match &self.logical_plan.execution {
                VelorixLogicalViewExecutionV1::TypedMap { plan } => {
                    if let Some(projected) = typed_projection(
                        &plan.projection,
                        &plan.output_columns,
                        plan.predicate.as_ref(),
                        &get,
                    )? {
                        emit(projected)?;
                    }
                }
                VelorixLogicalViewExecutionV1::Series { plan } => {
                    let values = series_iter(
                        eval(&plan.start)?,
                        eval(&plan.end)?,
                        eval(&plan.step)?,
                        usize::try_from(plan.max_emitted_rows_per_input)
                            .map_err(|_| invalid_runtime_state())?,
                    )?;
                    for generated in values {
                        let projected = plan
                            .projection
                            .iter()
                            .map(|projection| {
                                let value = match &projection.input_column_id {
                                    Some(id) => row
                                        .value
                                        .as_json()
                                        .get(id)
                                        .cloned()
                                        .ok_or_else(invalid_runtime_state)?,
                                    None => Value::from(generated),
                                };
                                Ok((projection.output_name.clone(), value))
                            })
                            .collect::<Result<Map<_, _>, StandingProgramRuntimeError>>()?;
                        emit(projected)?;
                    }
                }
                VelorixLogicalViewExecutionV1::UnionAll { plan } => {
                    for branch in &plan.branches {
                        if branch.input_schema != self.input_schemas[index] {
                            continue;
                        }
                        if let Some(projected) =
                            typed_projection(&branch.projection, &plan.output_columns, None, &get)?
                        {
                            emit(projected)?;
                        }
                    }
                }
                _ => return Err(invalid_runtime_state()),
            }
        }
        Ok(rows)
    }

    fn batch(&self, output: &DeltaBatch) -> Result<RecordBatch, StandingProgramRuntimeError> {
        self.limits.validate_page(
            output,
            self.epochs.logical_epoch,
            &SnapshotPageRequest::default(),
        )?;
        output::materialized_bag_delta_page_batch(
            &self.output_schema,
            output,
            self.epochs.logical_epoch,
            SnapshotPageRequest {
                max_rows: None,
                page_token: None,
                committed_epoch: None,
            },
        )
        .map(|(batch, _)| batch)
    }

    fn apply(
        &mut self,
        epoch: LogicalEpoch,
        key: EpochIdempotencyKey,
        inputs: Vec<RelationInputBatch>,
        snapshot: bool,
    ) -> Result<EpochCommit, StandingProgramRuntimeError> {
        let Some(next) = self
            .epochs
            .prepare(epoch, &key, &inputs, &self.input_schemas)?
        else {
            return Ok(EpochCommit {
                logical_epoch: epoch,
                idempotency_key: key,
                input_frontiers: self.epochs.input_frontiers.clone(),
                input_event_time_frontiers: self.epochs.input_event_time_frontiers.clone(),
                output_deltas: Vec::new(),
                output_batches: if snapshot {
                    vec![ViewOutputBatch {
                        view_id: self.identity.view_ids[0].clone(),
                        schema_fingerprint: self.output_schema.schema_fingerprint.clone(),
                        batches: vec![self.batch(&self.materialized_delta())?],
                    }]
                } else {
                    Vec::new()
                },
            });
        };
        let mut deltas = vec![DeltaBatch::default(); self.sources.len()];
        for input in &inputs {
            let index = self
                .input_schemas
                .iter()
                .position(|schema| schema.relation_id == input.relation_id)
                .ok_or_else(invalid_runtime_state)?;
            if let Some(watermark) = &input.event_time_watermark {
                let catalog = &self.catalogs[index];
                if catalog.relation_schema.event_time_column_id.as_deref()
                    != Some(watermark.event_time_column_id.as_str())
                    || watermark.stream_id != input.stream_id
                    || watermark.partition_id != input.partition_id
                    || !matches!(
                        catalog_column(catalog, &watermark.event_time_column_id)?
                            .physical_arrow_type,
                        ArrowPhysicalTypeV1::Int64
                            | ArrowPhysicalTypeV1::Date32
                            | ArrowPhysicalTypeV1::TimestampNanosecond { .. }
                    )
                {
                    return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                        field: "set_ops_input_watermark_binding",
                    });
                }
            }
            deltas[index] = deltas[index].combine(&self.decode(index, input)?);
        }
        let mut updates = Vec::new();
        let mut output_delta = DeltaBatch::default();
        let mut emitted_bytes = 0;
        let mut emitted_rows = 0;
        for (index, delta) in deltas.iter().enumerate() {
            let delta =
                DeltaBatch::from_records(delta.net_rows().map_err(|_| invalid_runtime_state())?);
            updates.push(prepare_bag(&self.sources[index], &delta)?);
            output_delta = output_delta.combine(&self.project(
                index,
                &delta,
                &mut emitted_bytes,
                &mut emitted_rows,
            )?);
        }
        let output_delta = DeltaBatch::from_records(
            output_delta
                .net_rows()
                .map_err(|_| invalid_runtime_state())?,
        );
        let output_updates = prepare_bag(&self.output, &output_delta)?;
        let units =
            DeltaBatch::from_records(output_delta.records().iter().map(|row| DeltaRecord {
                weight: 1,
                ..row.clone()
            }));
        output::materialized_bag_delta_page_batch(
            &self.output_schema,
            &units,
            self.epochs.logical_epoch,
            SnapshotPageRequest::default(),
        )?;
        let output_batches = if snapshot {
            let mut rows = self
                .output
                .iter()
                .filter(|(key, _)| !output_updates.contains_key(*key))
                .map(|(_, row)| row.clone())
                .collect::<Vec<_>>();
            rows.extend(
                output_updates
                    .values()
                    .filter(|row| row.weight > 0)
                    .cloned(),
            );
            vec![ViewOutputBatch {
                view_id: self.identity.view_ids[0].clone(),
                schema_fingerprint: self.output_schema.schema_fingerprint.clone(),
                batches: vec![self.batch(&DeltaBatch::from_records(rows))?],
            }]
        } else {
            Vec::new()
        };
        let mut candidate = self.clone();
        for (source, update) in candidate.sources.iter_mut().zip(updates) {
            commit_bag(source, update);
        }
        commit_bag(&mut candidate.output, output_updates);
        candidate.epochs = next;
        candidate.checkpoint()?;
        *self = candidate;
        Ok(EpochCommit {
            logical_epoch: epoch,
            idempotency_key: key,
            input_frontiers: self.epochs.input_frontiers.clone(),
            input_event_time_frontiers: self.epochs.input_event_time_frontiers.clone(),
            output_deltas: vec![ViewOutputDelta {
                view_id: self.identity.view_ids[0].clone(),
                schema_fingerprint: self.output_schema.schema_fingerprint.clone(),
                delta: output_delta,
            }],
            output_batches,
        })
    }
}

fn typed_projection(
    projection: &[TypedExprProgramV1],
    columns: &[velorix_core::view_contract::ColumnSchema],
    predicate: Option<&TypedExprProgramV1>,
    get: &dyn Fn(&str) -> Option<RuntimeScalarValue>,
) -> Result<Option<Map<String, Value>>, StandingProgramRuntimeError> {
    if projection.len() != columns.len() {
        return Err(invalid_runtime_state());
    }
    if let Some(predicate) = predicate {
        match evaluate_typed_expr(predicate, get).map_err(|_| invalid_runtime_state())? {
            RuntimeScalarValue::Boolean(true) => {}
            RuntimeScalarValue::Boolean(false)
            | RuntimeScalarValue::Null(RuntimeScalarTypeV1::Boolean) => return Ok(None),
            _ => return Err(invalid_runtime_state()),
        }
    }
    projection
        .iter()
        .zip(columns)
        .map(|(expr, column)| {
            let value = evaluate_typed_expr(expr, get).map_err(|_| invalid_runtime_state())?;
            Ok((column.name.clone(), scalar_json(value)?))
        })
        .collect::<Result<Map<_, _>, StandingProgramRuntimeError>>()
        .map(Some)
}

fn scalar_json(value: RuntimeScalarValue) -> Result<Value, StandingProgramRuntimeError> {
    if let RuntimeScalarValue::Decimal128 { unscaled, scale } = value {
        let scale = u32::try_from(scale).map_err(|_| invalid_runtime_state())?;
        let divisor = 10u128
            .checked_pow(scale)
            .ok_or_else(invalid_runtime_state)?;
        let magnitude = unscaled.unsigned_abs();
        let sign = if unscaled < 0 { "-" } else { "" };
        return Ok(Value::String(if scale == 0 {
            unscaled.to_string()
        } else {
            format!(
                "{sign}{}.{:0width$}",
                magnitude / divisor,
                magnitude % divisor,
                width = scale as usize
            )
        }));
    }
    typed_scalar_value_to_json(value)
}

impl StandingProgramRuntime for SetOpsRuntime {
    fn program_identity(&self) -> &StandingProgramIdentity {
        &self.identity
    }
    fn input_schemas(&self) -> Vec<RelationSchema> {
        self.input_schemas.clone()
    }
    fn output_schemas(&self) -> Vec<RelationSchema> {
        vec![self.output_schema.clone()]
    }
    fn logical_epoch(&self) -> LogicalEpoch {
        self.epochs.logical_epoch
    }
    fn apply_changes(
        &mut self,
        epoch: LogicalEpoch,
        key: EpochIdempotencyKey,
        inputs: Vec<RelationInputBatch>,
    ) -> Result<EpochCommit, StandingProgramRuntimeError> {
        self.apply(epoch, key, inputs, true)
    }
    fn apply_changes_delta_only(
        &mut self,
        epoch: LogicalEpoch,
        key: EpochIdempotencyKey,
        inputs: Vec<RelationInputBatch>,
    ) -> Result<EpochCommit, StandingProgramRuntimeError> {
        self.apply(epoch, key, inputs, false)
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
        self.limits
            .validate_page(&self.materialized_delta(), self.epochs.logical_epoch, &page)?;
        let (batch, next_page_token) = output::materialized_bag_delta_page_batch(
            &self.output_schema,
            &self.materialized_delta(),
            self.epochs.logical_epoch,
            page,
        )?;
        Ok(MaterializedViewPage {
            view,
            logical_epoch: self.epochs.logical_epoch,
            schema_fingerprint: self.output_schema.schema_fingerprint.clone(),
            batches: vec![batch],
            next_page_token,
        })
    }
    fn checkpoint(&self) -> Result<RuntimeCheckpoint, StandingProgramRuntimeError> {
        let payload = self.limits.checkpoint_arrow(&CheckpointRef {
            schema_version: 1,
            runtime_kind: RUNTIME_KIND,
            catalogs: &self.catalogs,
            input_schemas: &self.input_schemas,
            output_schema: &self.output_schema,
            logical_plan: &self.logical_plan,
            epochs: &self.epochs,
            sources: &self.sources,
            output: BagRef {
                records: &self.output,
            },
        })?;
        Ok(RuntimeCheckpoint {
            identity: self.identity.clone(),
            logical_epoch: self.epochs.logical_epoch,
            input_frontiers: self.epochs.input_frontiers.clone(),
            input_event_time_frontiers: self.epochs.input_event_time_frontiers.clone(),
            output_frontiers: self
                .identity
                .view_ids
                .iter()
                .map(|view_id| ViewFrontier {
                    view_id: view_id.clone(),
                    committed_epoch: self.epochs.logical_epoch,
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

impl SetOpsRuntime {
    pub(super) fn restore_with_limits(
        checkpoint: RuntimeCheckpoint,
        limits: RuntimeResourceLimits,
    ) -> Result<Self, StandingProgramRuntimeError> {
        limits.validate()?;
        let state = checkpoint
            .state_payload
            .as_ref()
            .ok_or_else(invalid_checkpoint)?;
        limits.validate_checkpoint(&state.payload)?;
        let payload: Checkpoint = velorix_core::arrow_state::from_state(&state.payload)
            .map_err(|_| invalid_checkpoint())?;
        if payload.schema_version != 1 || payload.runtime_kind != RUNTIME_KIND {
            return Err(invalid_checkpoint());
        }
        payload.epochs.validate_checkpoint(
            &checkpoint,
            &payload.input_schemas,
            &payload.catalogs,
            limits,
        )?;
        let mut runtime = Self::new_with_limits(
            checkpoint.identity,
            payload.catalogs,
            payload.input_schemas,
            payload.output_schema,
            payload.logical_plan,
            limits,
        )?;
        if payload.sources.len() != runtime.sources.len() {
            return Err(invalid_checkpoint());
        }
        runtime.sources = payload
            .sources
            .iter()
            .map(restore_bag)
            .collect::<Result<_, _>>()?;
        runtime.output = restore_bag(&payload.output)?;
        let mut expected: Bag = BTreeMap::new();
        for (index, source) in payload.sources.iter().enumerate() {
            // Restore may cover many committed epochs; bound each retained input separately.
            for row in source.records() {
                let projected =
                    runtime.project_bag(index, std::slice::from_ref(row), &mut 0, &mut 0)?;
                // Projection already encoded the exact canonical bag key.
                // Move it and the row instead of re-encoding and cloning them.
                for (key, row) in projected {
                    let weight = expected
                        .get(&key)
                        .map_or(0, |row| row.weight)
                        .checked_add(row.weight)
                        .ok_or_else(invalid_checkpoint)?;
                    if weight
                        > runtime
                            .output
                            .get(&key)
                            .ok_or_else(invalid_checkpoint)?
                            .weight
                    {
                        return Err(invalid_checkpoint());
                    }
                    expected.insert(key, DeltaRecord { weight, ..row });
                }
            }
        }
        if expected != runtime.output {
            return Err(invalid_checkpoint());
        }
        let units = DeltaBatch::from_records(runtime.output.values().map(|row| DeltaRecord {
            weight: 1,
            ..row.clone()
        }));
        output::materialized_bag_delta_page_batch(
            &runtime.output_schema,
            &units,
            runtime.epochs.logical_epoch,
            SnapshotPageRequest::default(),
        )?;
        runtime.epochs = payload.epochs;
        retain_recent_applied_epochs(&mut runtime.epochs.applied_epochs);
        Ok(runtime)
    }
}

/// Only touched entries are staged; validation never mutates the retained bag.
fn prepare_bag(bag: &Bag, delta: &DeltaBatch) -> Result<Bag, StandingProgramRuntimeError> {
    let mut updates = BTreeMap::new();
    for row in delta.net_rows().map_err(|_| invalid_runtime_state())? {
        let key = encode_kv_ordered(row.key.as_json(), row.value.as_json());
        let weight = bag
            .get(&key)
            .map_or(0, |previous| previous.weight)
            .checked_add(row.weight)
            .filter(|weight| *weight >= 0)
            .ok_or_else(invalid_runtime_state)?;
        updates.insert(key, DeltaRecord { weight, ..row });
    }
    Ok(updates)
}

fn commit_bag(bag: &mut Bag, updates: Bag) {
    for (key, row) in updates {
        if row.weight == 0 {
            bag.remove(&key);
        } else {
            bag.insert(key, row);
        }
    }
}

fn restore_bag(delta: &DeltaBatch) -> Result<Bag, StandingProgramRuntimeError> {
    let mut bag = BTreeMap::new();
    for row in delta.records() {
        if row.weight <= 0
            || bag
                .insert(
                    encode_kv_ordered(row.key.as_json(), row.value.as_json()),
                    row.clone(),
                )
                .is_some()
        {
            return Err(invalid_checkpoint());
        }
    }
    Ok(bag)
}

#[derive(Clone, Default, Serialize, Deserialize)]
struct EpochState {
    logical_epoch: LogicalEpoch,
    applied_epochs: BTreeMap<String, LogicalEpoch>,
    input_frontiers: Vec<RelationFrontier>,
    input_event_time_frontiers: Vec<InputEventTimeFrontier>,
}

impl EpochState {
    fn is_replay(
        &self,
        epoch: LogicalEpoch,
        key: &EpochIdempotencyKey,
    ) -> Result<bool, StandingProgramRuntimeError> {
        if let Some(first) = self.applied_epochs.get(key.as_str()) {
            if *first != epoch {
                return Err(StandingProgramRuntimeError::IdempotencyKeyConflict {
                    idempotency_key: key.as_str().to_owned(),
                    first_epoch: *first,
                    attempted_epoch: epoch,
                });
            }
            return Ok(true);
        }
        // Evicted retry keys must not turn old epochs into fresh mutations.
        if epoch <= self.logical_epoch {
            return Err(StandingProgramRuntimeError::NonMonotonicLogicalEpoch {
                current: self.logical_epoch,
                attempted: epoch,
            });
        }
        Ok(false)
    }

    fn prepare(
        &self,
        epoch: LogicalEpoch,
        key: &EpochIdempotencyKey,
        inputs: &[RelationInputBatch],
        schemas: &[RelationSchema],
    ) -> Result<Option<Self>, StandingProgramRuntimeError> {
        if self.is_replay(epoch, key)? {
            return Ok(None);
        }
        let mut next = self.clone();
        for input in inputs {
            validate_input_matches_one_schema(input, schemas, "set_ops_input")?;
            advance_input_frontier(&mut next.input_frontiers, input)?;
            advance_input_event_time_frontier(&mut next.input_event_time_frontiers, input)?;
        }
        next.logical_epoch = epoch;
        next.applied_epochs.insert(key.as_str().to_owned(), epoch);
        retain_recent_applied_epochs(&mut next.applied_epochs);
        Ok(Some(next))
    }

    fn validate_checkpoint(
        &self,
        checkpoint: &RuntimeCheckpoint,
        schemas: &[RelationSchema],
        catalogs: &[VelorixRelationCatalogV1],
        limits: RuntimeResourceLimits,
    ) -> Result<(), StandingProgramRuntimeError> {
        checkpoint.validate_identity(&checkpoint.identity)?;
        let state = checkpoint
            .state_payload
            .as_ref()
            .ok_or_else(invalid_checkpoint)?;
        limits.validate_checkpoint(&state.payload)?;
        if state.codec_identity != checkpoint.checkpoint_codec_identity
            || stable_bytes_hash(state.payload.as_bytes()) != checkpoint.state_root.content_hash
            || self.logical_epoch != checkpoint.logical_epoch
            || self.input_frontiers != checkpoint.input_frontiers
            || self.input_event_time_frontiers != checkpoint.input_event_time_frontiers
        {
            return Err(invalid_checkpoint());
        }
        validate_checkpoint_frontiers_for_schemas(checkpoint, schemas)?;
        validate_input_event_time_frontiers_for_catalogs(checkpoint, catalogs)?;
        for (key, epoch) in &self.applied_epochs {
            EpochIdempotencyKey::new(key.clone()).map_err(|_| invalid_checkpoint())?;
            if *epoch == 0 || *epoch > self.logical_epoch {
                return Err(invalid_checkpoint());
            }
        }
        Ok(())
    }
}

/// Count in i128 before allocation, including endpoints without stepping past i64.
fn series_iter(
    start: RuntimeScalarValue,
    end: RuntimeScalarValue,
    step: RuntimeScalarValue,
    budget: usize,
) -> Result<impl Iterator<Item = i64>, StandingProgramRuntimeError> {
    let (start, step, count) = match (start, end, step) {
        // SQL NULL arguments produce an empty series, even alongside a zero step.
        (RuntimeScalarValue::Null(_), _, _)
        | (_, RuntimeScalarValue::Null(_), _)
        | (_, _, RuntimeScalarValue::Null(_)) => (0, 1, 0),
        (
            RuntimeScalarValue::Int64(start),
            RuntimeScalarValue::Int64(end),
            RuntimeScalarValue::Int64(step),
        )
        | (
            RuntimeScalarValue::TimestampNanosecond(start),
            RuntimeScalarValue::TimestampNanosecond(end),
            RuntimeScalarValue::Int64(step),
        ) => {
            if step == 0 {
                return Err(invalid_runtime_state());
            }
            let (start, end, step) = (i128::from(start), i128::from(end), i128::from(step));
            let count = if (step > 0 && start > end) || (step < 0 && start < end) {
                0
            } else {
                (end - start) / step + 1
            };
            (start, step, count)
        }
        _ => return Err(invalid_runtime_state()),
    };
    let count = usize::try_from(count)
        .ok()
        .filter(|count| *count <= budget)
        .ok_or_else(invalid_runtime_state)?;
    // Count guarantees each i128 intermediate lies between the original i64 bounds.
    Ok((0..count).map(move |index| (start + step * index as i128) as i64))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use velorix_core::relation::*;
    use velorix_core::standing_program::{BuiltinRuntimeIdentity, NativeCodePolicy};

    fn series_values(
        start: RuntimeScalarValue,
        end: RuntimeScalarValue,
        step: RuntimeScalarValue,
        budget: usize,
    ) -> Result<Vec<i64>, StandingProgramRuntimeError> {
        series_iter(start, end, step, budget).map(Iterator::collect)
    }

    fn catalog(id: &str, names: [&str; 2]) -> VelorixRelationCatalogV1 {
        let relation_schema = VelorixRelationSchemaV1 {
            relation_id: id.into(),
            relation_name: id.into(),
            relation_version: "v1".into(),
            columns: [names[0], names[1], "weight"]
                .iter()
                .enumerate()
                .map(|(index, name)| RelationColumnV1 {
                    column_id: (*name).into(),
                    name: (*name).into(),
                    logical_type: VelorixLogicalTypeV1::Int64,
                    physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                    nullable: false,
                    ordinal: index as u32,
                    semantic_role: match index {
                        0 => RelationSemanticRoleV1::PrimaryKey,
                        1 => RelationSemanticRoleV1::Value,
                        _ => RelationSemanticRoleV1::Weight,
                    },
                })
                .collect(),
            primary_key_column_ids: vec![names[0].into()],
            weight_column_id: "weight".into(),
            allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
            event_time_column_id: None,
        };
        let fingerprint = SchemaFingerprintV1::for_relation_schema(&relation_schema).unwrap();
        VelorixRelationCatalogV1 {
            schema_version: RELATION_SCHEMA_VERSION_V1,
            relation_schema,
            schema_fingerprint: fingerprint.clone(),
            relation_source: VelorixRelationSourceV1::SourceRelation,
            datafusion_registration: DataFusionRegistrationV1 {
                name: id.into(),
                mode: DataFusionRegistrationModeV1::Table,
            },
            incremental_relation: IncrementalRelationBindingV1 {
                relation_id: id.into(),
                schema_fingerprint: fingerprint,
            },
            incremental_adapter: IncrementalAdapterBindingV1 {
                adapter_id: CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.into(),
            },
        }
    }

    fn input(
        catalog: &VelorixRelationCatalogV1,
        offset: u64,
        rows: &[(i64, i64, i64)],
    ) -> RelationInputBatch {
        let fields = catalog
            .relation_schema
            .columns
            .iter()
            .map(|column| {
                arrow::datatypes::Field::new(&column.name, arrow::datatypes::DataType::Int64, false)
            })
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(
            Arc::new(arrow::datatypes::Schema::new(fields)),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| row.0).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| row.1).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| row.2).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        RelationInputBatch {
            relation_id: catalog.relation_schema.relation_id.clone(),
            relation_version: "v1".into(),
            schema_fingerprint: catalog.schema_fingerprint.to_string(),
            stream_id: "rows".into(),
            partition_id: 0,
            start_offset_inclusive: offset,
            end_offset_exclusive: offset + rows.len() as u64,
            event_time_watermark: None,
            encoding: RelationInputEncodingV1::SourceRelationV1,
            batches: vec![batch],
        }
    }

    fn runtime(sql: &str, catalogs: Vec<VelorixRelationCatalogV1>) -> SetOpsRuntime {
        let columns = if sql.contains("UNION ALL") {
            velorix_core::view_plan::supported_union_all_output_columns(sql, &catalogs).unwrap()
        } else {
            velorix_core::view_plan::supported_series_output_columns(sql, &catalogs[0]).unwrap()
        };
        runtime_with_columns(sql, catalogs, columns)
    }

    fn runtime_with_columns(
        sql: &str,
        catalogs: Vec<VelorixRelationCatalogV1>,
        columns: Vec<velorix_core::view_contract::ColumnSchema>,
    ) -> SetOpsRuntime {
        let output = RelationSchema {
            relation_id: "result".into(),
            relation_name: "result".into(),
            relation_version: "v1".into(),
            schema_fingerprint: format!("sha256:{}", "3".repeat(64)),
            primary_key: Vec::new(),
            columns,
        };
        let logical = lower_supported_sql_to_logical_plan(sql, &catalogs, &output).unwrap();
        let identity = StandingProgramIdentity {
            tenant_id: "tenant".into(),
            program_id: "set-ops".into(),
            view_ids: vec!["result".into()],
            sql_hash: stable_bytes_hash(sql.as_bytes()),
            input_catalog_hash: format!("sha256:{}", "1".repeat(64)),
            output_schema_hash: format!("sha256:{}", "2".repeat(64)),
            planner_identity: "velorix-logical-view-planner@1".into(),
            builtin_runtime_identities: vec![BuiltinRuntimeIdentity {
                name: CRATE_NAME.into(),
                version: "0.1.0".into(),
            }],
            runtime_capabilities: vec!["materialized-view-runtime-v1".into()],
            runtime_compatibility: "velorix-materialized-view-runtime-v1".into(),
            checkpoint_codec_identity: "velorix-standing-program-checkpoint-v1".into(),
            native_code_policy: NativeCodePolicy::DisabledNoExternalDependencies,
        };
        let schemas = catalogs
            .iter()
            .map(|catalog| catalog_input_relation_schema(catalog).unwrap())
            .collect();
        SetOpsRuntime::new(identity, catalogs, schemas, output, logical).unwrap()
    }

    #[test]
    fn borrowed_checkpoint_matches_owned_bytes_hash_and_restore() {
        let left = catalog("alpha", ["id", "amount"]);
        let right = catalog("beta", ["code", "reading"]);
        let mut runtime = runtime(
            "SELECT a.amount AS value FROM alpha a UNION ALL SELECT b.reading AS other FROM beta b",
            vec![left.clone(), right.clone()],
        );
        for epoch in 0..=2 {
            if epoch > 0 {
                let weight = if epoch == 1 { 2 } else { -2 };
                runtime
                    .apply_changes_delta_only(
                        epoch,
                        EpochIdempotencyKey::new(format!("borrowed-{epoch}")).unwrap(),
                        vec![
                            input(&left, (epoch - 1) * 2, &[(9, 7, weight), (-2, 3, weight)]),
                            input(&right, epoch - 1, &[(4, 7, weight)]),
                        ],
                    )
                    .unwrap();
            }
            let owned = Checkpoint {
                schema_version: 1,
                runtime_kind: RUNTIME_KIND.into(),
                catalogs: runtime.catalogs.clone(),
                input_schemas: runtime.input_schemas.clone(),
                output_schema: runtime.output_schema.clone(),
                logical_plan: runtime.logical_plan.clone(),
                epochs: runtime.epochs.clone(),
                sources: runtime
                    .sources
                    .iter()
                    .map(|bag| DeltaBatch::from_records(bag.values().cloned()))
                    .collect(),
                output: runtime.materialized_delta(),
            };
            let expected = runtime.limits.checkpoint_arrow(&owned).unwrap();
            // Restore's moved keys must match the old re-encoding path,
            // including duplicate UNION output and canonical row order.
            for (index, source) in owned.sources.iter().enumerate() {
                let projected = runtime
                    .project_bag(index, source.records(), &mut 0, &mut 0)
                    .unwrap();
                let reencoded: Bag = projected
                    .values()
                    .map(|row| {
                        (
                            encode_kv_ordered(row.key.as_json(), row.value.as_json()),
                            row.clone(),
                        )
                    })
                    .collect();
                assert_eq!(projected, reencoded);
                assert_eq!(
                    serde_json::to_vec(&DeltaBatch::from_records(projected.into_values())).unwrap(),
                    serde_json::to_vec(&DeltaBatch::from_records(reencoded.into_values())).unwrap(),
                );
            }
            let checkpoint = runtime.checkpoint().unwrap();
            assert_eq!(checkpoint.state_payload.as_ref().unwrap().payload, expected);
            assert_eq!(
                checkpoint.state_root.content_hash,
                stable_bytes_hash(expected.as_bytes())
            );
            let restored = SetOpsRuntime::restore(checkpoint.clone()).unwrap();
            assert_eq!(restored.sources, runtime.sources);
            assert_eq!(restored.output, runtime.output);
            assert_eq!(restored.checkpoint().unwrap(), checkpoint);
            let cloned_output = restored.materialized_delta();
            let consumed_output = restored.into_materialized_delta();
            assert_eq!(consumed_output, cloned_output);
            let cloned_bytes = serde_json::to_vec(&cloned_output).unwrap();
            let consumed_bytes = serde_json::to_vec(&consumed_output).unwrap();
            assert_eq!(consumed_bytes, cloned_bytes);
            assert_eq!(
                stable_bytes_hash(&consumed_bytes),
                stable_bytes_hash(&cloned_bytes)
            );
            assert_eq!(
                bag_checkpoint_published_output(&checkpoint).unwrap(),
                Some(consumed_output),
            );
            let original_limits = runtime.limits;
            runtime.limits.max_checkpoint_bytes = expected.len();
            assert_eq!(runtime.checkpoint().unwrap(), checkpoint);
            runtime.limits.max_checkpoint_bytes -= 1;
            assert!(runtime.checkpoint().is_err());
            runtime.limits = original_limits;
        }
    }

    #[test]
    fn independent_limits_reject_before_commit_and_on_restore() {
        let source = catalog("ranges", ["lo", "hi"]);
        let baseline = runtime(
            "SELECT g.value AS value FROM ranges r CROSS JOIN LATERAL generate_series(r.lo, r.hi, 1) AS g(value)",
            vec![source.clone()],
        );
        let initial = baseline.checkpoint().unwrap();
        let defaults = RuntimeResourceLimits::default();
        for limits in [
            RuntimeResourceLimits {
                max_checkpoint_bytes: initial.state_payload.as_ref().unwrap().payload.len(),
                ..defaults
            },
            RuntimeResourceLimits {
                max_expansion_bytes: 1,
                ..defaults
            },
            RuntimeResourceLimits {
                max_expansion_rows: 1,
                ..defaults
            },
            RuntimeResourceLimits {
                max_snapshot_bytes: 1,
                ..defaults
            },
        ] {
            let mut runtime = SetOpsRuntime::restore_with_limits(initial.clone(), limits).unwrap();
            assert!(runtime
                .apply_changes(
                    1,
                    EpochIdempotencyKey::new("retry").unwrap(),
                    vec![input(&source, 0, &[(1, 2, 1)])]
                )
                .is_err());
            assert_eq!(
                runtime.checkpoint().unwrap().state_root.content_hash,
                initial.state_root.content_hash
            );
        }
        let mut populated = baseline;
        populated
            .apply_changes_delta_only(
                1,
                EpochIdempotencyKey::new("first").unwrap(),
                vec![input(&source, 0, &[(1, 2, 1)])],
            )
            .unwrap();
        let checkpoint = populated.checkpoint().unwrap();
        let lower = RuntimeResourceLimits {
            max_checkpoint_bytes: checkpoint.state_payload.as_ref().unwrap().payload.len() - 1,
            ..defaults
        };
        assert!(restore_standing_runtime_with_limits(checkpoint.clone(), lower).is_err());
        let lower = RuntimeResourceLimits {
            max_expansion_rows: 1,
            ..defaults
        };
        assert!(SetOpsRuntime::restore_with_limits(checkpoint, lower).is_err());
        let view = ScopedViewId {
            tenant_id: populated.identity.tenant_id.clone(),
            program_id: populated.identity.program_id.clone(),
            view_id: populated.identity.view_ids[0].clone(),
        };
        populated.limits.max_snapshot_bytes =
            resource_limits::row_width(&populated.materialized_delta().records()[0]).unwrap();
        assert!(populated
            .materialized_view_page(view.clone(), SnapshotPageRequest::default())
            .is_err());
        assert!(populated
            .materialized_view_page(
                view,
                SnapshotPageRequest {
                    max_rows: Some(1),
                    ..SnapshotPageRequest::default()
                }
            )
            .is_ok());
    }

    #[test]
    fn removed_specialized_checkpoints_fail_without_generic_fallback() {
        let runtime = runtime(
            "SELECT g.value AS value FROM ranges r CROSS JOIN LATERAL generate_series(r.lo, r.hi, 1) AS g(value)",
            vec![catalog("ranges", ["lo", "hi"])],
        );
        for kind in [
            "native_projection_v1",
            "native_windowed_fact_v1",
            "native_rollup_v1",
            "native_native_rollup_v1",
        ] {
            let mut checkpoint = runtime.checkpoint().unwrap();
            let state = checkpoint.state_payload.as_mut().unwrap();
            let mut payload: Value = velorix_core::arrow_state::from_state(&state.payload).unwrap();
            payload["runtime_kind"] = Value::from(kind);
            state.payload = velorix_core::arrow_state::to_state(&payload).unwrap();
            checkpoint.state_root.content_hash = stable_bytes_hash(state.payload.as_bytes());
            let error = restore_standing_runtime(checkpoint.clone()).err().unwrap();
            assert!(error.contains("unsupported legacy specialized runtime checkpoint"));
            assert!(bag_checkpoint_published_output(&checkpoint)
                .unwrap_err()
                .to_string()
                .contains("rebuild derived data"));
            let view = ScopedViewId {
                tenant_id: runtime.identity.tenant_id.clone(),
                program_id: runtime.identity.program_id.clone(),
                view_id: runtime.identity.view_ids[0].clone(),
            };
            assert!(materialized_delta_to_page_with_checkpoint(
                &runtime.output_schema,
                &runtime.materialized_delta(),
                view,
                0,
                SnapshotPageRequest::default(),
                None,
                &checkpoint
            )
            .unwrap_err()
            .to_string()
            .contains("rebuild derived data"));
        }
    }

    #[test]
    fn typed_map_keyless_projection_preserves_bags_and_filtered_source_state() {
        let source = catalog("measurements", ["id", "amount"]);
        let mut runtime = runtime_with_columns(
            "SELECT m.amount AS value FROM measurements m WHERE m.amount > 10",
            vec![source.clone()],
            vec![velorix_core::view_contract::ColumnSchema {
                name: "value".into(),
                data_type: SqlDataType::Int64,
                nullable: false,
            }],
        );
        assert!(matches!(
            runtime.logical_plan.execution,
            VelorixLogicalViewExecutionV1::TypedMap { .. }
        ));
        runtime
            .apply_changes_delta_only(
                1,
                EpochIdempotencyKey::new("first").unwrap(),
                vec![input(&source, 0, &[(1, 20, 2), (2, 20, 1), (3, 5, 1)])],
            )
            .unwrap();
        assert_eq!(runtime.materialized_delta().records().len(), 1);
        assert_eq!(runtime.materialized_delta().records()[0].weight, 3);
        let mut runtime = SetOpsRuntime::restore(runtime.checkpoint().unwrap()).unwrap();
        assert!(runtime
            .apply_changes_delta_only(1, EpochIdempotencyKey::new("first").unwrap(), Vec::new())
            .unwrap()
            .output_deltas
            .is_empty());
        let before = runtime.checkpoint().unwrap().state_root.content_hash;
        assert!(runtime
            .apply_changes_delta_only(
                2,
                EpochIdempotencyKey::new("filtered-underflow").unwrap(),
                vec![input(&source, 3, &[(3, 5, -2)])]
            )
            .is_err());
        assert_eq!(
            runtime.checkpoint().unwrap().state_root.content_hash,
            before
        );
        runtime
            .apply_changes_delta_only(
                2,
                EpochIdempotencyKey::new("retract").unwrap(),
                vec![input(&source, 3, &[(1, 20, -1), (3, 5, -1)])],
            )
            .unwrap();
        assert_eq!(runtime.materialized_delta().records()[0].weight, 2);
        let restored = SetOpsRuntime::restore(runtime.checkpoint().unwrap()).unwrap();
        assert_eq!(restored.materialized_delta(), runtime.materialized_delta());
    }

    #[test]
    fn typed_projection_accepts_only_true_and_skips_filtered_expressions() {
        let column = |id: &str, result_type| TypedExprProgramV1 {
            encoding_version: velorix_core::view_plan::TYPED_EXPR_PROGRAM_SCHEMA_VERSION_V1,
            root: TypedExprNodeV1 {
                result_type,
                nullable: true,
                kind: TypedExprKindV1::Column {
                    column_id: id.into(),
                },
            },
        };
        let projection = vec![column("value", RuntimeScalarTypeV1::Int64)];
        let predicate = column("keep", RuntimeScalarTypeV1::Boolean);
        let columns = vec![velorix_core::view_contract::ColumnSchema {
            name: "mapped".into(),
            data_type: SqlDataType::Int64,
            nullable: true,
        }];
        for value in [
            RuntimeScalarValue::Boolean(false),
            RuntimeScalarValue::Null(RuntimeScalarTypeV1::Boolean),
        ] {
            assert!(
                typed_projection(&projection, &columns, Some(&predicate), &|id| {
                    assert_eq!(id, "keep");
                    Some(value.clone())
                })
                .unwrap()
                .is_none()
            );
        }
        let projected = typed_projection(&projection, &columns, Some(&predicate), &|id| match id {
            "keep" => Some(RuntimeScalarValue::Boolean(true)),
            "value" => Some(RuntimeScalarValue::Int64(42)),
            _ => None,
        })
        .unwrap()
        .unwrap();
        assert_eq!(projected.get("mapped"), Some(&Value::from(42)));
        assert!(
            typed_projection(&projection, &columns, Some(&predicate), &|_| Some(
                RuntimeScalarValue::Int64(1)
            ))
            .is_err()
        );
    }

    #[test]
    fn output_binding_and_invalid_watermark_fail_before_mutation() {
        let source = catalog("ranges", ["lo", "hi"]);
        let mut runtime = runtime(
            "SELECT g.value AS value FROM ranges r CROSS JOIN LATERAL generate_series(r.lo, r.hi, 1) AS g(value)",
            vec![source.clone()],
        );
        let mut wrong_output = runtime.output_schema.clone();
        wrong_output.relation_version = "v2".into();
        assert!(SetOpsRuntime::new(
            runtime.identity.clone(),
            runtime.catalogs.clone(),
            runtime.input_schemas.clone(),
            wrong_output,
            runtime.logical_plan.clone()
        )
        .is_err());
        let mut wrong_identity = runtime.identity.clone();
        wrong_identity.view_ids = vec!["other".into()];
        assert!(SetOpsRuntime::new(
            wrong_identity,
            runtime.catalogs.clone(),
            runtime.input_schemas.clone(),
            runtime.output_schema.clone(),
            runtime.logical_plan.clone()
        )
        .is_err());
        let before = runtime.checkpoint().unwrap().state_root.content_hash;
        let mut invalid = input(&source, 0, &[(1, 2, 1)]);
        invalid.event_time_watermark =
            Some(velorix_core::standing_program::InputEventTimeWatermark {
                stream_id: "rows".into(),
                partition_id: 0,
                event_time_column_id: "lo".into(),
                max_observed_event_time_ns: 10,
                watermark_ns: 5,
            });
        assert!(runtime
            .apply_changes_delta_only(1, EpochIdempotencyKey::new("retry").unwrap(), vec![invalid])
            .is_err());
        assert_eq!(
            runtime.checkpoint().unwrap().state_root.content_hash,
            before
        );
        runtime
            .apply_changes_delta_only(
                1,
                EpochIdempotencyKey::new("retry").unwrap(),
                vec![input(&source, 0, &[(1, 2, 1)])],
            )
            .unwrap();
        SetOpsRuntime::restore(runtime.checkpoint().unwrap()).unwrap();
    }

    #[test]
    fn union_two_schemas_duplicates_late_retractions_restart_and_rollback() {
        let left = catalog("alpha", ["id", "amount"]);
        let right = catalog("beta", ["code", "reading"]);
        let mut runtime = runtime(
            "SELECT a.amount AS value FROM alpha a UNION ALL SELECT b.reading AS other FROM beta b UNION ALL SELECT a.amount AS again FROM alpha a",
            vec![left.clone(), right.clone()],
        );
        runtime
            .apply_changes_delta_only(
                1,
                EpochIdempotencyKey::new("first").unwrap(),
                vec![
                    input(&left, 0, &[(10, 7, 2)]),
                    input(&right, 0, &[(20, 7, 1)]),
                ],
            )
            .unwrap();
        assert_eq!(runtime.materialized_delta().records()[0].weight, 5);
        let checkpoint = runtime.checkpoint().unwrap();
        let mut restored = SetOpsRuntime::restore(checkpoint.clone()).unwrap();
        assert!(restored
            .apply_changes_delta_only(1, EpochIdempotencyKey::new("first").unwrap(), Vec::new())
            .unwrap()
            .output_deltas
            .is_empty());
        assert!(restored
            .apply_changes_delta_only(
                2,
                EpochIdempotencyKey::new("bad").unwrap(),
                vec![input(&right, 1, &[(20, 7, -2)])]
            )
            .is_err());
        assert_eq!(
            restored.checkpoint().unwrap().state_root.content_hash,
            checkpoint.state_root.content_hash
        );
        restored
            .apply_changes_delta_only(
                2,
                EpochIdempotencyKey::new("late").unwrap(),
                vec![input(&left, 1, &[(10, 7, -1), (1, 3, 1)])],
            )
            .unwrap();
        assert_eq!(
            restored
                .materialized_delta()
                .records()
                .iter()
                .map(|row| row.weight)
                .sum::<i64>(),
            5
        );
        let restored = SetOpsRuntime::restore(restored.checkpoint().unwrap()).unwrap();
        assert_eq!(
            restored
                .batch(&restored.materialized_delta())
                .unwrap()
                .num_rows(),
            5
        );
    }

    #[test]
    fn series_overlap_retract_restart_and_invalid_epoch_are_atomic() {
        let source = catalog("ranges", ["lo", "hi"]);
        let mut runtime = runtime(
            "SELECT g.value AS value FROM ranges r CROSS JOIN LATERAL generate_series(r.lo, r.hi, 1) AS g(value)",
            vec![source.clone()],
        );
        runtime
            .apply_changes_delta_only(
                1,
                EpochIdempotencyKey::new("first").unwrap(),
                vec![input(&source, 0, &[(1, 3, 1), (2, 3, 1)])],
            )
            .unwrap();
        assert_eq!(
            runtime
                .batch(&runtime.materialized_delta())
                .unwrap()
                .num_rows(),
            5
        );
        let mut runtime = SetOpsRuntime::restore(runtime.checkpoint().unwrap()).unwrap();
        let before = runtime.checkpoint().unwrap().state_root.content_hash;
        assert!(runtime
            .apply_changes_delta_only(
                1,
                EpochIdempotencyKey::new("old").unwrap(),
                vec![input(&source, 2, &[(1, 3, -1)])]
            )
            .is_err());
        assert_eq!(
            runtime.checkpoint().unwrap().state_root.content_hash,
            before
        );
        assert!(runtime
            .apply_changes_delta_only(
                2,
                EpochIdempotencyKey::new("large").unwrap(),
                vec![input(&source, 2, &[(0, i64::MAX, 1)])]
            )
            .is_err());
        assert_eq!(
            runtime.checkpoint().unwrap().state_root.content_hash,
            before
        );
        let many_ranges = (0..10)
            .map(|index| (index * 100_000, index * 100_000 + 99_999, 1))
            .collect::<Vec<_>>();
        assert!(runtime
            .apply_changes_delta_only(
                2,
                EpochIdempotencyKey::new("epoch-budget").unwrap(),
                vec![input(&source, 2, &many_ranges)]
            )
            .is_err());
        assert_eq!(
            runtime.checkpoint().unwrap().state_root.content_hash,
            before
        );
        runtime
            .apply_changes_delta_only(
                2,
                EpochIdempotencyKey::new("retract").unwrap(),
                vec![input(&source, 2, &[(1, 3, -1)])],
            )
            .unwrap();
        assert_eq!(
            runtime
                .batch(&runtime.materialized_delta())
                .unwrap()
                .num_rows(),
            2
        );
        assert_eq!(
            SetOpsRuntime::restore(runtime.checkpoint().unwrap())
                .unwrap()
                .materialized_delta(),
            runtime.materialized_delta()
        );
    }

    fn row(value: i64, weight: i64) -> DeltaRecord {
        DeltaRecord::new(
            DeltaKey::from_json(Value::from(value)),
            DeltaValue::from_json(Value::Null),
            weight,
        )
    }

    #[test]
    fn signed_bag_consolidates_and_rejects_underflow_without_mutation() {
        let mut bag = Bag::new();
        let initial = DeltaBatch::from_records([row(7, 2)]);
        let updates = prepare_bag(&bag, &initial).unwrap();
        commit_bag(&mut bag, updates);
        assert!(prepare_bag(&bag, &DeltaBatch::from_records([row(7, -3)])).is_err());
        assert_eq!(bag.values().next().unwrap().weight, 2);
        let updates =
            prepare_bag(&bag, &DeltaBatch::from_records([row(7, -3), row(7, 1)])).unwrap();
        commit_bag(&mut bag, updates);
        assert!(bag.is_empty());
        assert!(restore_bag(&DeltaBatch::from_records([row(7, 1), row(7, 1)])).is_err());
        assert!(restore_bag(&DeltaBatch::from_records([row(7, -1)])).is_err());
        let maximum = restore_bag(&DeltaBatch::from_records([row(7, i64::MAX)])).unwrap();
        assert!(prepare_bag(&maximum, &DeltaBatch::from_records([row(7, 1)])).is_err());
    }

    #[test]
    fn replay_survives_serialization_and_evicted_epochs_fail_closed() {
        let mut state = EpochState::default();
        for epoch in 1..=1025 {
            state.logical_epoch = epoch;
            state.applied_epochs.insert(format!("epoch-{epoch}"), epoch);
        }
        retain_recent_applied_epochs(&mut state.applied_epochs);
        let state: EpochState =
            serde_json::from_str(&serde_json::to_string(&state).unwrap()).unwrap();
        let recent = EpochIdempotencyKey::new("epoch-1025").unwrap();
        assert!(state.is_replay(1025, &recent).unwrap());
        assert!(state.is_replay(1026, &recent).is_err());
        let old = EpochIdempotencyKey::new("epoch-1").unwrap();
        assert!(state.is_replay(1, &old).is_err());
        assert!(!state.is_replay(1026, &old).unwrap());
    }

    #[test]
    fn inclusive_series_checks_bounds_steps_and_budget() {
        use RuntimeScalarValue::{Int64 as I, TimestampNanosecond as T};
        assert_eq!(series_values(I(2), I(6), I(2), 3).unwrap(), [2, 4, 6]);
        assert_eq!(series_values(T(6), T(2), I(-2), 3).unwrap(), [6, 4, 2]);
        assert_eq!(
            series_values(I(i64::MAX), I(i64::MAX), I(1), 1).unwrap(),
            [i64::MAX]
        );
        assert!(series_values(I(i64::MIN), I(i64::MAX), I(1), 10).is_err());
        assert!(series_values(I(1), I(2), I(0), 10).is_err());
        assert!(series_values(I(1), I(2), I(1), 1).is_err());
        assert!(series_values(I(1), T(2), I(1), 10).is_err());
        let null_int = RuntimeScalarValue::Null(RuntimeScalarTypeV1::Int64);
        let null_time = RuntimeScalarValue::Null(RuntimeScalarTypeV1::TimestampNanosecond);
        for (start, end, step) in [
            (null_int.clone(), I(2), I(1)),
            (I(1), null_int.clone(), I(1)),
            (I(1), I(2), null_int.clone()),
            (null_int.clone(), I(2), I(0)),
            (null_time.clone(), T(2), I(1)),
            (T(1), null_time, I(1)),
            (T(1), T(2), null_int),
        ] {
            assert!(series_values(start, end, step, 0).unwrap().is_empty());
        }
        assert!(series_values(T(1), T(2), I(0), 10).is_err());
        assert!(series_values(I(2), I(1), I(1), 0).unwrap().is_empty());
    }
}
