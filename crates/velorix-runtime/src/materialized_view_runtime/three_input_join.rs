use super::*;

pub(super) const THREE_INPUT_JOIN_RUNTIME_KIND: &str = "three_input_inner_join_count_dag_v1";

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ThreeInputJoinCheckpointPayloadV1 {
    schema_version: u32,
    runtime_kind: String,
    catalogs: Vec<VelorixRelationCatalogV1>,
    input_schemas: Vec<RelationSchema>,
    output_schema: RelationSchema,
    logical_plan: VelorixLogicalViewPlanV1,
    graph: NativeOperatorGraphCheckpointV1,
    published_output: DeltaBatch,
    input_frontiers: Vec<RelationFrontier>,
    input_event_time_frontiers: Vec<InputEventTimeFrontier>,
    applied_epochs: Vec<GenericAppliedEpoch>,
}

pub struct ThreeInputInnerJoinCountRuntime {
    identity: StandingProgramIdentity,
    catalogs: Vec<VelorixRelationCatalogV1>,
    input_schemas: Vec<RelationSchema>,
    output_schema: RelationSchema,
    logical_plan: VelorixLogicalViewPlanV1,
    plan: SupportedThreeInputInnerJoinCountPlanV1,
    graph: NativeOperatorGraph,
    published_output: DeltaBatch,
    logical_epoch: LogicalEpoch,
    input_frontiers: Vec<RelationFrontier>,
    input_event_time_frontiers: Vec<InputEventTimeFrontier>,
    applied_epochs: BTreeMap<String, LogicalEpoch>,
}

impl ThreeInputInnerJoinCountRuntime {
    pub fn new_with_logical_plan(
        identity: StandingProgramIdentity,
        catalogs: Vec<VelorixRelationCatalogV1>,
        input_schemas: Vec<RelationSchema>,
        output_schema: RelationSchema,
        logical_plan: VelorixLogicalViewPlanV1,
    ) -> Result<Self, StandingProgramRuntimeError> {
        let VelorixLogicalViewExecutionV1::ThreeInputInnerJoinCount { plan } =
            logical_plan.execution.clone()
        else {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "three_input_join_execution",
            });
        };
        identity.validate()?;
        validate_builtin_runtime_identity(&identity)?;
        validate_view_sql_hash(&identity, &logical_plan.view_sql)?;
        validate_logical_view_plan(&logical_plan).map_err(|_| invalid_runtime_state())?;
        validate_three_input_runtime_contract(
            &catalogs,
            &input_schemas,
            &output_schema,
            &logical_plan,
            &plan,
        )?;
        let graph = build_three_input_join_graph(&plan)?;
        Ok(Self {
            identity,
            catalogs,
            input_schemas,
            output_schema,
            logical_plan,
            plan,
            graph,
            published_output: DeltaBatch::default(),
            logical_epoch: 0,
            input_frontiers: Vec::new(),
            input_event_time_frontiers: Vec::new(),
            applied_epochs: BTreeMap::new(),
        })
    }

    fn aggregate_outputs(&self) -> [SupportedAggregateOutput; 1] {
        [SupportedAggregateOutput {
            function: LogicalPlanAggregateFunctionV1::Count,
            input_column_id: None,
            input_relation_side: None,
            input_expression: None,
            output_column_id: self.plan.count_output_column_id.clone(),
        }]
    }

    fn materialized_batch(&self) -> Result<RecordBatch, StandingProgramRuntimeError> {
        materialized_delta_to_record_batch(
            &self.output_schema,
            &self.published_output,
            Some(&self.aggregate_outputs()),
        )
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
            Some(&self.aggregate_outputs()),
        )
    }

    fn checkpoint_payload(&self) -> Result<String, StandingProgramRuntimeError> {
        let payload = ThreeInputJoinCheckpointPayloadV1 {
            schema_version: 1,
            runtime_kind: THREE_INPUT_JOIN_RUNTIME_KIND.to_string(),
            catalogs: self.catalogs.clone(),
            input_schemas: self.input_schemas.clone(),
            output_schema: self.output_schema.clone(),
            logical_plan: self.logical_plan.clone(),
            graph: self.graph.checkpoint().map_err(|_| invalid_checkpoint())?,
            published_output: self.published_output.clone(),
            input_frontiers: self.input_frontiers.clone(),
            input_event_time_frontiers: self.input_event_time_frontiers.clone(),
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

    pub fn restore(checkpoint: RuntimeCheckpoint) -> Result<Self, StandingProgramRuntimeError> {
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
        let payload: ThreeInputJoinCheckpointPayloadV1 =
            serde_json::from_str(&state_payload.payload).map_err(|_| invalid_checkpoint())?;
        if payload.schema_version != 1
            || payload.runtime_kind != THREE_INPUT_JOIN_RUNTIME_KIND
            || payload.graph.logical_epoch != checkpoint.logical_epoch
            || payload.input_frontiers != checkpoint.input_frontiers
            || payload.input_event_time_frontiers != checkpoint.input_event_time_frontiers
            || checkpoint.output_frontiers.len() != checkpoint.identity.view_ids.len()
            || checkpoint.output_frontiers.iter().any(|frontier| {
                frontier.committed_epoch != checkpoint.logical_epoch
                    || !checkpoint.identity.view_ids.contains(&frontier.view_id)
            })
        {
            return Err(invalid_checkpoint());
        }
        validate_view_sql_hash(&checkpoint.identity, &payload.logical_plan.view_sql)?;
        validate_logical_view_plan(&payload.logical_plan).map_err(|_| invalid_checkpoint())?;
        let VelorixLogicalViewExecutionV1::ThreeInputInnerJoinCount { plan } =
            payload.logical_plan.execution.clone()
        else {
            return Err(invalid_checkpoint());
        };
        validate_three_input_runtime_contract(
            &payload.catalogs,
            &payload.input_schemas,
            &payload.output_schema,
            &payload.logical_plan,
            &plan,
        )
        .map_err(|_| invalid_checkpoint())?;
        validate_input_event_time_frontiers_for_catalogs(&checkpoint, &payload.catalogs)?;
        validate_published_output(&payload.published_output)?;
        let aggregate_state = payload
            .graph
            .operators
            .iter()
            .find(|operator| operator.node_id == "aggregate_three_input_count")
            .and_then(|operator| match &operator.state {
                NativeOperatorStateV1::Unary { state } => Some(state),
                _ => None,
            })
            .ok_or_else(invalid_checkpoint)?;
        if aggregate_state != &payload.published_output {
            return Err(invalid_checkpoint());
        }
        let mut graph = build_three_input_join_graph(&plan)?;
        graph
            .restore(&payload.graph)
            .map_err(|_| invalid_checkpoint())?;
        let mut applied_epochs = payload
            .applied_epochs
            .into_iter()
            .map(|entry| (entry.idempotency_key, entry.logical_epoch))
            .collect();
        retain_recent_applied_epochs(&mut applied_epochs);
        if applied_epochs
            .values()
            .any(|epoch| *epoch > checkpoint.logical_epoch)
            || (checkpoint.logical_epoch == 0 && !applied_epochs.is_empty())
            || (checkpoint.logical_epoch > 0
                && applied_epochs.values().max().copied() != Some(checkpoint.logical_epoch))
        {
            return Err(invalid_checkpoint());
        }
        Ok(Self {
            identity: checkpoint.identity,
            catalogs: payload.catalogs,
            input_schemas: payload.input_schemas,
            output_schema: payload.output_schema,
            logical_plan: payload.logical_plan,
            plan,
            graph,
            published_output: payload.published_output,
            logical_epoch: checkpoint.logical_epoch,
            input_frontiers: checkpoint.input_frontiers,
            input_event_time_frontiers: checkpoint.input_event_time_frontiers,
            applied_epochs,
        })
    }
}

impl StandingProgramRuntime for ThreeInputInnerJoinCountRuntime {
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
                        schema_fingerprint: self.output_schema.schema_fingerprint.clone(),
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
        let graph_before = self
            .graph
            .checkpoint()
            .map_err(|_| invalid_runtime_state())?;
        let mut next_frontiers = self.input_frontiers.clone();
        let mut next_event_time_frontiers = self.input_event_time_frontiers.clone();
        let result = (|| {
            let mut graph_inputs = Vec::new();
            for input in &input_changes {
                validate_input_matches_one_schema(
                    input,
                    &self.input_schemas,
                    "three_input_join_input_relation",
                )?;
                advance_input_frontier(&mut next_frontiers, input)?;
                advance_input_event_time_frontier(&mut next_event_time_frontiers, input)?;
                let input_index = self
                    .plan
                    .ordered_input_relation_ids
                    .iter()
                    .position(|relation_id| relation_id == &input.relation_id)
                    .ok_or(StandingProgramRuntimeError::InvalidProgramIdentity {
                        field: "three_input_join_input_relation",
                    })?;
                let catalog = &self.catalogs[input_index];
                let delta = three_input_join_delta(catalog, &self.plan, input_index, input)?;
                let (node_id, port_id) = match input_index {
                    0 => ("join_1", "left"),
                    1 => ("join_1", "right"),
                    2 => ("join_2", "right"),
                    _ => return Err(invalid_runtime_state()),
                };
                graph_inputs.push(NativeOperatorInputV1 {
                    node_id: node_id.to_string(),
                    port_id: port_id.to_string(),
                    batch: delta,
                });
            }
            let output_delta = self
                .graph
                .apply_epoch(logical_epoch, graph_inputs)
                .map_err(|_| invalid_runtime_state())?
                .remove("aggregate_three_input_count")
                .unwrap_or_default();
            let next_published =
                apply_published_output_delta(&self.published_output, &output_delta)?;
            let batch = materialized_delta_to_record_batch(
                &self.output_schema,
                &next_published,
                Some(&self.aggregate_outputs()),
            )?;
            Ok::<_, StandingProgramRuntimeError>((output_delta, next_published, batch))
        })();
        let (output_delta, next_published, batch) = match result {
            Ok(result) => result,
            Err(error) => {
                self.graph
                    .restore(&graph_before)
                    .map_err(|_| invalid_runtime_state())?;
                return Err(error);
            }
        };
        self.published_output = next_published;
        self.logical_epoch = logical_epoch;
        self.input_frontiers = next_frontiers.clone();
        self.input_event_time_frontiers = next_event_time_frontiers.clone();
        self.applied_epochs
            .insert(idempotency_key_text, logical_epoch);
        retain_recent_applied_epochs(&mut self.applied_epochs);
        Ok(EpochCommit {
            logical_epoch,
            idempotency_key,
            input_frontiers: next_frontiers,
            input_event_time_frontiers: next_event_time_frontiers,
            output_deltas: vec![ViewOutputDelta {
                view_id: self.identity.view_ids[0].clone(),
                schema_fingerprint: self.output_schema.schema_fingerprint.clone(),
                delta: output_delta,
            }],
            output_batches: vec![ViewOutputBatch {
                view_id: self.identity.view_ids[0].clone(),
                schema_fingerprint: self.output_schema.schema_fingerprint.clone(),
                batches: vec![batch],
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
            schema_fingerprint: self.output_schema.schema_fingerprint.clone(),
            batches: vec![batch],
            next_page_token,
        })
    }

    fn checkpoint(&self) -> Result<RuntimeCheckpoint, StandingProgramRuntimeError> {
        let payload = self.checkpoint_payload()?;
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
        Self::restore(checkpoint)
    }
}

fn build_three_input_join_graph(
    plan: &SupportedThreeInputInnerJoinCountPlanV1,
) -> Result<NativeOperatorGraph, StandingProgramRuntimeError> {
    let mut graph = NativeOperatorGraph::new();
    for node_id in ["join_1", "join_2"] {
        graph
            .add_operator(NativeBinaryJoinOperator::new(node_id, join_output_value))
            .map_err(|_| invalid_runtime_state())?;
    }
    graph.add_edge(NativeOperatorEdgeV1 {
        from_node_id: "join_1".to_string(),
        to_node_id: "join_2".to_string(),
        to_port_id: "left".to_string(),
    });
    let output_key_column_ids = plan.output_key_column_ids.clone();
    graph
        .add_operator(NativeProjectOperator::new(
            "project_three_input_count",
            move |record| {
                let values = record
                    .key
                    .as_json()
                    .as_array()
                    .ok_or(OperatorError::InvalidAggregateStateValue)?;
                if values.len() != output_key_column_ids.len() {
                    return Err(OperatorError::InvalidAggregateStateValue);
                }
                let key = output_key_column_ids
                    .iter()
                    .cloned()
                    .zip(values.iter().cloned())
                    .collect::<Map<_, _>>();
                Ok((
                    DeltaKey::from_json(Value::Object(key)),
                    DeltaValue::from_json(Value::Number(JsonNumber::from(1))),
                ))
            },
        ))
        .map_err(|_| invalid_runtime_state())?;
    graph
        .add_operator(NativeAggregateOperator::new(
            "aggregate_three_input_count",
            AggregateValueMode::Integer,
            false,
        ))
        .map_err(|_| invalid_runtime_state())?;
    graph.add_edge(NativeOperatorEdgeV1 {
        from_node_id: "join_2".to_string(),
        to_node_id: "project_three_input_count".to_string(),
        to_port_id: "input".to_string(),
    });
    graph.add_edge(NativeOperatorEdgeV1 {
        from_node_id: "project_three_input_count".to_string(),
        to_node_id: "aggregate_three_input_count".to_string(),
        to_port_id: "input".to_string(),
    });
    graph.validate().map_err(|_| invalid_runtime_state())?;
    Ok(graph)
}

fn three_input_join_delta(
    catalog: &VelorixRelationCatalogV1,
    plan: &SupportedThreeInputInnerJoinCountPlanV1,
    input_index: usize,
    input: &RelationInputBatch,
) -> Result<DeltaBatch, StandingProgramRuntimeError> {
    let permutation = plan
        .root_to_input_pk_permutations
        .get(input_index)
        .ok_or_else(invalid_runtime_state)?;
    let key_column_ids = permutation
        .iter()
        .map(|position| {
            catalog
                .relation_schema
                .primary_key_column_ids
                .get(*position)
                .cloned()
                .ok_or_else(invalid_runtime_state)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let value_column_id = key_column_ids.first().ok_or_else(invalid_runtime_state)?;
    let delta = arrow_record_batches_to_key_value_delta_batch(
        catalog,
        &input.relation_id,
        &input.relation_version,
        &input.schema_fingerprint,
        &key_column_ids,
        value_column_id,
        &input.batches,
    )
    .map_err(|_| StandingProgramRuntimeError::InvalidProgramIdentity {
        field: "three_input_join_input_batch",
    })?;
    normalize_composite_join_keys(delta, &key_column_ids)
}

fn validate_three_input_runtime_contract(
    catalogs: &[VelorixRelationCatalogV1],
    input_schemas: &[RelationSchema],
    output_schema: &RelationSchema,
    logical_plan: &VelorixLogicalViewPlanV1,
    plan: &SupportedThreeInputInnerJoinCountPlanV1,
) -> Result<(), StandingProgramRuntimeError> {
    let join_order_policy_id = match (plan.schema_version, plan.join_order_policy_id.as_str()) {
        (1, "") => THREE_INPUT_LEGACY_SQL_ENCOUNTER_JOIN_ORDER_V1,
        (2, THREE_INPUT_ROOT_FIXED_RIGHT_RELATION_ID_JOIN_ORDER_V1) => {
            THREE_INPUT_ROOT_FIXED_RIGHT_RELATION_ID_JOIN_ORDER_V1
        }
        _ => {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "three_input_join_order_policy",
            })
        }
    };
    let expected_plan =
        lower_supported_three_input_inner_join_count_sql_to_logical_plan_with_policy(
            &logical_plan.view_sql,
            catalogs,
            output_schema,
            join_order_policy_id,
        )
        .map_err(|_| StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "three_input_join_logical_plan",
        })?;
    if &expected_plan != logical_plan {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "three_input_join_logical_plan",
        });
    }
    if catalogs.len() != 3
        || input_schemas.len() != 3
        || plan.ordered_input_relation_ids.len() != 3
        || plan.root_to_input_pk_permutations.len() != 3
        || plan.join_key_codec_id != COMPOSITE_PK_POSITIONAL_JSON_ARRAY_JOIN_KEY_CODEC_V1
        || logical_plan
            .execution_implementation
            .as_ref()
            .and_then(|implementation| implementation.join_key_codec_id.as_deref())
            != Some(COMPOSITE_PK_POSITIONAL_JSON_ARRAY_JOIN_KEY_CODEC_V1)
    {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "three_input_join_plan",
        });
    }
    let expected_inputs = catalogs
        .iter()
        .map(|catalog| catalog_input_relation_schema(catalog).map_err(|_| invalid_runtime_state()))
        .collect::<Result<Vec<_>, _>>()?;
    if expected_inputs != input_schemas
        || catalogs
            .iter()
            .map(|catalog| &catalog.relation_schema.relation_id)
            .ne(plan.ordered_input_relation_ids.iter())
    {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "three_input_join_input_schemas",
        });
    }
    let arity = plan.root_primary_key_column_ids.len();
    let root_catalog = &catalogs[0];
    for (index, catalog) in catalogs.iter().enumerate() {
        let permutation = &plan.root_to_input_pk_permutations[index];
        if permutation.len() != arity
            || permutation.iter().copied().collect::<BTreeSet<_>>().len() != arity
            || catalog.relation_schema.primary_key_column_ids.len() != arity
        {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "three_input_join_pk_permutation",
            });
        }
        for (root_position, right_position) in permutation.iter().enumerate() {
            let root_column = catalog_column_by_id(
                root_catalog,
                &plan.root_primary_key_column_ids[root_position],
            )?;
            let right_column_id = catalog
                .relation_schema
                .primary_key_column_ids
                .get(*right_position)
                .ok_or(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "three_input_join_pk_permutation",
                })?;
            let right_column = catalog_column_by_id(catalog, right_column_id)?;
            if root_column.nullable
                || right_column.nullable
                || root_column.physical_arrow_type != right_column.physical_arrow_type
            {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "three_input_join_key_domain",
                });
            }
        }
    }
    if output_schema.primary_key != plan.output_key_column_ids
        || output_schema.columns.len() != arity + 1
    {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "three_input_join_output_schema",
        });
    }
    for (index, column) in output_schema.columns.iter().take(arity).enumerate() {
        let root_column =
            catalog_column_by_id(root_catalog, &plan.root_primary_key_column_ids[index])?;
        if column.name != plan.output_key_column_ids[index]
            || column.data_type != sql_type_from_catalog_column(root_column)?
            || column.nullable
        {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "three_input_join_output_schema",
            });
        }
    }
    let count = &output_schema.columns[arity];
    if count.name != plan.count_output_column_id
        || count.data_type != SqlDataType::Int64
        || count.nullable
    {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "three_input_join_output_schema",
        });
    }
    Ok(())
}
