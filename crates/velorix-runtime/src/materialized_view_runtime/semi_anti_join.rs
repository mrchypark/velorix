use super::*;

pub(super) const SEMI_ANTI_JOIN_RUNTIME_KIND: &str = "two_input_semi_anti_join_project_v1";
const SEMI_ANTI_JOIN_NODE_ID: &str = "semi_anti_join";

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SemiAntiJoinCheckpointPayloadV1 {
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

pub struct TwoInputSemiAntiJoinRuntime {
    identity: StandingProgramIdentity,
    catalogs: Vec<VelorixRelationCatalogV1>,
    input_schemas: Vec<RelationSchema>,
    output_schema: RelationSchema,
    logical_plan: VelorixLogicalViewPlanV1,
    plan: SupportedSemiAntiJoinProjectPlanV1,
    graph: NativeOperatorGraph,
    published_output: DeltaBatch,
    logical_epoch: LogicalEpoch,
    input_frontiers: Vec<RelationFrontier>,
    input_event_time_frontiers: Vec<InputEventTimeFrontier>,
    applied_epochs: BTreeMap<String, LogicalEpoch>,
}

impl TwoInputSemiAntiJoinRuntime {
    pub fn new_with_logical_plan(
        identity: StandingProgramIdentity,
        catalogs: Vec<VelorixRelationCatalogV1>,
        input_schemas: Vec<RelationSchema>,
        output_schema: RelationSchema,
        logical_plan: VelorixLogicalViewPlanV1,
    ) -> Result<Self, StandingProgramRuntimeError> {
        let VelorixLogicalViewExecutionV1::TwoInputSemiAntiJoinProject { plan } =
            logical_plan.execution.clone()
        else {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "semi_anti_join_execution",
            });
        };
        identity.validate()?;
        validate_builtin_runtime_identity(&identity)?;
        validate_view_sql_hash(&identity, &logical_plan.view_sql)?;
        validate_logical_view_plan(&logical_plan).map_err(|_| invalid_runtime_state())?;
        validate_semi_anti_join_runtime_contract(
            &catalogs,
            &input_schemas,
            &output_schema,
            &logical_plan,
            &plan,
        )?;
        let compiled_plan =
            validate_supported_semi_anti_join_sql(&logical_plan.view_sql, &catalogs).map_err(
                |_| StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "semi_anti_join_plan",
                },
            )?;
        if compiled_plan != plan {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "semi_anti_join_plan",
            });
        }
        let compiled_logical = lower_supported_semi_anti_join_sql_to_logical_plan(
            &logical_plan.view_sql,
            &catalogs,
            &output_schema,
        )
        .map_err(|_| StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "logical_semi_anti_join_plan",
        })?;
        if compiled_logical != logical_plan {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "logical_semi_anti_join_plan",
            });
        }
        let graph = build_semi_anti_join_graph(plan.join_kind)?;
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

    fn left_catalog(&self) -> Result<&VelorixRelationCatalogV1, StandingProgramRuntimeError> {
        catalog_for_relation_id(&self.catalogs, &self.plan.left_input_relation_id)
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

    fn checkpoint_payload(&self) -> Result<String, StandingProgramRuntimeError> {
        let payload = SemiAntiJoinCheckpointPayloadV1 {
            schema_version: 1,
            runtime_kind: SEMI_ANTI_JOIN_RUNTIME_KIND.to_string(),
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
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "semi_anti_join_checkpoint",
            });
        };
        if state_payload.codec_identity != checkpoint.checkpoint_codec_identity {
            return Err(StandingProgramRuntimeError::CheckpointCodecMismatch {
                expected: checkpoint.checkpoint_codec_identity.clone(),
                actual: state_payload.codec_identity.clone(),
            });
        }
        let payload: SemiAntiJoinCheckpointPayloadV1 =
            serde_json::from_str(&state_payload.payload).map_err(|_| invalid_checkpoint())?;
        if payload.schema_version != 1
            || payload.runtime_kind != SEMI_ANTI_JOIN_RUNTIME_KIND
            || payload.graph.logical_epoch != checkpoint.logical_epoch
            || payload.input_frontiers != checkpoint.input_frontiers
            || payload.input_event_time_frontiers != checkpoint.input_event_time_frontiers
            || checkpoint.output_frontiers.len() != checkpoint.identity.view_ids.len()
            || checkpoint.output_frontiers.iter().any(|frontier| {
                frontier.committed_epoch != checkpoint.logical_epoch
                    || !checkpoint.identity.view_ids.contains(&frontier.view_id)
            })
        {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "semi_anti_join_checkpoint",
            });
        }
        validate_view_sql_hash(&checkpoint.identity, &payload.logical_plan.view_sql)?;
        validate_logical_view_plan(&payload.logical_plan).map_err(|_| invalid_checkpoint())?;
        let VelorixLogicalViewExecutionV1::TwoInputSemiAntiJoinProject { plan } =
            payload.logical_plan.execution.clone()
        else {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "semi_anti_join_checkpoint",
            });
        };
        validate_semi_anti_join_runtime_contract(
            &payload.catalogs,
            &payload.input_schemas,
            &payload.output_schema,
            &payload.logical_plan,
            &plan,
        )
        .map_err(|_| invalid_checkpoint())?;
        let compiled = lower_supported_semi_anti_join_sql_to_logical_plan(
            &payload.logical_plan.view_sql,
            &payload.catalogs,
            &payload.output_schema,
        )
        .map_err(|_| invalid_checkpoint())?;
        if compiled != payload.logical_plan {
            return Err(invalid_checkpoint());
        }
        validate_input_event_time_frontiers_for_catalogs(&checkpoint, &payload.catalogs)?;
        validate_published_output(&payload.published_output)?;
        let expected_output = expected_semi_anti_output(&payload.graph, plan.join_kind)?;
        let expected_output = project_filter_project_delta_batch(
            &expected_output,
            &plan.projection,
            catalog_for_relation_id(&payload.catalogs, &plan.left_input_relation_id)?,
        )?;
        if expected_output != payload.published_output {
            return Err(invalid_checkpoint());
        }
        let mut graph = build_semi_anti_join_graph(plan.join_kind)?;
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
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "semi_anti_join_checkpoint",
            });
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

impl StandingProgramRuntime for TwoInputSemiAntiJoinRuntime {
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
                    "semi_anti_join_input_relation",
                )?;
                advance_input_frontier(&mut next_frontiers, input)?;
                advance_input_event_time_frontier(&mut next_event_time_frontiers, input)?;
                let catalog = catalog_for_relation_id(&self.catalogs, &input.relation_id)?;
                let (port_id, delta) = if input.relation_id == self.plan.left_input_relation_id {
                    let mut columns = filter_project_input_column_ids(&self.plan.projection);
                    // The projected output key is carried as a value column
                    // when it differs from the join key (non-PK
                    // correlations).
                    let projection_key = self.plan.projection.key_column_id.clone();
                    if projection_key != self.plan.left_join_key_column_id
                        && !columns.iter().any(|column| column == &projection_key)
                    {
                        columns.push(projection_key);
                    }
                    let delta =
                        if let Some(empty_delta) = published_input_empty_delta(input, catalog)? {
                            empty_delta
                        } else {
                            arrow_record_batches_to_key_multi_value_delta_batch(
                                catalog,
                                &input.relation_id,
                                &input.relation_version,
                                &input.schema_fingerprint,
                                std::slice::from_ref(&self.plan.left_join_key_column_id),
                                &columns,
                                &input.batches,
                            )
                            .map_err(|_| {
                                StandingProgramRuntimeError::InvalidProgramIdentity {
                                    field: "semi_anti_join_left_input_batch",
                                }
                            })?
                        };
                    ("left", delta)
                } else if input.relation_id == self.plan.right_input_relation_id {
                    let value_columns = catalog
                        .relation_schema
                        .columns
                        .iter()
                        .filter(|column| {
                            column.column_id != self.plan.right_join_key_column_id
                                && column.column_id != catalog.relation_schema.weight_column_id
                        })
                        .map(|column| column.column_id.clone())
                        .collect::<Vec<_>>();
                    let delta =
                        if let Some(empty_delta) = published_input_empty_delta(input, catalog)? {
                            empty_delta
                        } else {
                            arrow_record_batches_to_key_multi_value_delta_batch(
                                catalog,
                                &input.relation_id,
                                &input.relation_version,
                                &input.schema_fingerprint,
                                std::slice::from_ref(&self.plan.right_join_key_column_id),
                                &value_columns,
                                &input.batches,
                            )
                            .map_err(|_| {
                                StandingProgramRuntimeError::InvalidProgramIdentity {
                                    field: "semi_anti_join_right_input_batch",
                                }
                            })?
                        };
                    ("right", delta)
                } else {
                    return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                        field: "semi_anti_join_input_relation",
                    });
                };
                graph_inputs.push(NativeOperatorInputV1 {
                    node_id: SEMI_ANTI_JOIN_NODE_ID.to_string(),
                    port_id: port_id.to_string(),
                    batch: delta,
                });
            }
            let output_delta = self
                .graph
                .apply_epoch(logical_epoch, graph_inputs)
                .map_err(|_| invalid_runtime_state())?
                .remove(SEMI_ANTI_JOIN_NODE_ID)
                .unwrap_or_default();
            // The graph emits matched left rows keyed by the join key; the
            // projection derives the public output key and values. For
            // primary-key correlations the join key equals the output key.
            let output_delta = project_filter_project_delta_batch(
                &output_delta,
                &self.plan.projection,
                self.left_catalog()?,
            )?;
            let next_published =
                apply_published_output_delta(&self.published_output, &output_delta)?;
            let batch =
                materialized_generic_delta_to_record_batch(&self.output_schema, &next_published)?;
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

fn validate_semi_anti_join_runtime_contract(
    catalogs: &[VelorixRelationCatalogV1],
    input_schemas: &[RelationSchema],
    output_schema: &RelationSchema,
    logical_plan: &VelorixLogicalViewPlanV1,
    plan: &SupportedSemiAntiJoinProjectPlanV1,
) -> Result<(), StandingProgramRuntimeError> {
    if plan.schema_version != 1 || catalogs.len() != 2 || input_schemas.len() != 2 {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "semi_anti_join_contract",
        });
    }
    let left_catalog = catalog_for_relation_id(catalogs, &plan.left_input_relation_id)?;
    let right_catalog = catalog_for_relation_id(catalogs, &plan.right_input_relation_id)?;
    if left_catalog.relation_schema.relation_id == right_catalog.relation_schema.relation_id
        || plan.projection.input_relation_id != plan.left_input_relation_id
        || catalog_column(left_catalog, &plan.projection.key_column_id).is_err()
        || catalog_column(left_catalog, &plan.left_join_key_column_id).is_err()
    {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "semi_anti_join_contract",
        });
    }
    let left_input = schema_for_relation_id(input_schemas, &plan.left_input_relation_id)?;
    let right_input = schema_for_relation_id(input_schemas, &plan.right_input_relation_id)?;
    validate_filter_project_supported_schemas(
        left_catalog,
        left_input,
        output_schema,
        &plan.projection,
    )?;
    let expected_right = catalog_input_relation_schema(right_catalog).map_err(|_| {
        StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "semi_anti_join_right_input_schema",
        }
    })?;
    if &expected_right != right_input
        || logical_plan.input_relations.len() != 2
        || !logical_plan.nodes.iter().any(|node| {
            matches!(
                (plan.join_kind, node),
                (
                    SupportedSemiAntiJoinKindV1::Semi,
                    velorix_core::view_plan::VelorixLogicalViewPlanNodeV1::SemiEquiJoin { .. },
                ) | (
                    SupportedSemiAntiJoinKindV1::Anti,
                    velorix_core::view_plan::VelorixLogicalViewPlanNodeV1::AntiEquiJoin { .. },
                )
            )
        })
    {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "semi_anti_join_contract",
        });
    }
    Ok(())
}

fn catalog_for_relation_id<'a>(
    catalogs: &'a [VelorixRelationCatalogV1],
    relation_id: &str,
) -> Result<&'a VelorixRelationCatalogV1, StandingProgramRuntimeError> {
    catalogs
        .iter()
        .find(|catalog| catalog.relation_schema.relation_id == relation_id)
        .ok_or(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "semi_anti_join_catalog",
        })
}

fn schema_for_relation_id<'a>(
    schemas: &'a [RelationSchema],
    relation_id: &str,
) -> Result<&'a RelationSchema, StandingProgramRuntimeError> {
    schemas
        .iter()
        .find(|schema| schema.relation_id == relation_id)
        .ok_or(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "semi_anti_join_input_schema",
        })
}

fn build_semi_anti_join_graph(
    join_kind: SupportedSemiAntiJoinKindV1,
) -> Result<NativeOperatorGraph, StandingProgramRuntimeError> {
    let mut graph = NativeOperatorGraph::new();
    match join_kind {
        SupportedSemiAntiJoinKindV1::Semi => graph
            .add_operator(NativeSemiJoinOperator::new(SEMI_ANTI_JOIN_NODE_ID))
            .map_err(|_| invalid_runtime_state())?,
        SupportedSemiAntiJoinKindV1::Anti => graph
            .add_operator(NativeAntiJoinOperator::new(SEMI_ANTI_JOIN_NODE_ID))
            .map_err(|_| invalid_runtime_state())?,
    }
    Ok(graph)
}

fn expected_semi_anti_output(
    graph: &NativeOperatorGraphCheckpointV1,
    join_kind: SupportedSemiAntiJoinKindV1,
) -> Result<DeltaBatch, StandingProgramRuntimeError> {
    let operator = graph
        .operators
        .iter()
        .find(|operator| operator.node_id == SEMI_ANTI_JOIN_NODE_ID)
        .ok_or_else(invalid_checkpoint)?;
    let NativeOperatorStateV1::Binary {
        left_state,
        right_state,
    } = &operator.state
    else {
        return Err(invalid_checkpoint());
    };
    let right_rows = right_state.net_rows().map_err(|_| invalid_checkpoint())?;
    let mut output = Vec::new();
    for row in left_state.net_rows().map_err(|_| invalid_checkpoint())? {
        if row.weight < 0 {
            return Err(invalid_checkpoint());
        }
        let mut right_count = 0_i64;
        for right in right_rows.iter().filter(|right| right.key == row.key) {
            if right.weight < 0 {
                return Err(invalid_checkpoint());
            }
            right_count = right_count
                .checked_add(right.weight)
                .ok_or_else(invalid_checkpoint)?;
        }
        let retained = match join_kind {
            SupportedSemiAntiJoinKindV1::Semi => right_count > 0,
            SupportedSemiAntiJoinKindV1::Anti => right_count == 0,
        };
        if retained {
            output.push(row);
        }
    }
    Ok(DeltaBatch::from_records(output))
}
