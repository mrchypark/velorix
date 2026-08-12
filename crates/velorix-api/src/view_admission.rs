use super::*;

pub(super) async fn create_view(
    State(state): State<ApiState>,
    Json(request): Json<CreateViewRequest>,
) -> Result<(StatusCode, Json<ViewResponse>), ApiError> {
    validate_public_view_feature_admission(&state, &request)?;
    let resolved_inputs = resolve_standing_inputs_for_view_request(&state, &request).await?;
    validate_resolved_input_scope(&resolved_inputs)?;
    let catalogs = resolved_inputs
        .iter()
        .map(|input| input.catalog())
        .collect::<Result<Vec<_>, ApiError>>()?;
    let spec = view_spec_from_request(&state, &request, &catalogs)?;
    validate_materialized_runtime_spec_admission(&spec)?;
    state.validate_standing_runtime_fencing_or_evict().await?;
    let input_bindings =
        input_bindings_for_resolved_inputs(&state, "default", &resolved_inputs).await?;
    let mut runtime_binding =
        materialized_view_runtime_binding_for_spec(&catalogs, &spec, &input_bindings)?;
    validate_public_runtime_plan_admission(
        &state,
        runtime_binding.logical_plan.as_ref().ok_or_else(|| {
            ApiError::bad_request("materialized view runtime binding is missing a logical plan")
        })?,
    )?;
    let spec_hash = view_spec_hash(&spec).map_err(ApiError::bad_request)?;
    let api_metadata = api_metadata_from_create_view_request(&request);
    validate_view_api_metadata(&api_metadata)?;
    validate_query_policy_reference(&state, &api_metadata).await?;
    validate_view_api_output_binding(&spec.view_id, &api_metadata, &spec.output_relations)?;
    validate_standing_runtime_create_api_metadata(
        &spec.view_id,
        &api_metadata,
        &spec.output_relations,
    )
    .await?;
    // Serialize view-on-view admissions in-process so concurrent creates
    // cannot both pass the acyclic check against the same graph snapshot; the
    // authoritative meta-store graph revision CAS extends the fence across
    // processes.
    let _graph_guard = state.view_dependency_graph_mutex.lock().await;
    let candidate_edges = dependency_edges_from_input_bindings(
        &runtime_binding.standing_program_identity.tenant_id,
        &runtime_binding.standing_program_identity.program_id,
        &spec.view_id,
        &input_bindings,
    )?;
    let existing_edges = view_dependency_edges_from_active_views(&state).await?;
    for candidate in &candidate_edges {
        validate_view_dependency_graph_with_candidate(&existing_edges, candidate)?;
    }
    let pending_runtime = build_standing_runtime_for_runtime_binding(
        &state,
        &spec,
        &runtime_binding,
        &catalogs,
        &spec.input_relations,
        &spec.output_relations,
    )?;
    let execution_mode = MaterializedViewExecutionMode::StandingRuntime;
    let bootstrap =
        begin_authoritative_view_bootstrap(&state, &spec, &runtime_binding, &input_bindings)
            .await?;
    runtime_binding.published_relations = published_relation_bindings_for_spec(
        &spec,
        &runtime_binding,
        bootstrap.bootstrap_generation,
    )?;
    runtime_binding.input_bindings = input_bindings;
    let requires_backfill = standing_runtime_create_requires_backfill(&state, &spec).await?;
    let lifecycle = lifecycle_for_create_view_execution(&execution_mode, requires_backfill);
    let outcome = if let Some(runtime) = pending_runtime {
        let operation_lock =
            state.standing_runtime_operation_lock(runtime.program_identity(), &spec.view_id)?;
        let _operation_guard = operation_lock.lock().await;
        let outcome = register_materialized_view_execution(
            &state,
            &spec,
            Some(api_metadata.clone()),
            runtime_binding.clone(),
            Some(lifecycle.clone()),
        )
        .await?;
        if view_query_availability(&lifecycle) {
            insert_standing_runtime(&state, &spec.view_id, runtime)?;
        }
        outcome
    } else {
        register_materialized_view_execution(
            &state,
            &spec,
            Some(api_metadata.clone()),
            runtime_binding.clone(),
            Some(lifecycle.clone()),
        )
        .await?
    };
    if view_has_published_view_inputs(&runtime_binding) {
        bootstrap_consumer_view_after_registration(
            &state,
            &spec,
            &runtime_binding,
            &resolved_inputs,
        )
        .await?;
    }
    let (status, outcome_text) = match outcome {
        RegisterMaterializedViewOutcome::Created => (StatusCode::CREATED, "created"),
        RegisterMaterializedViewOutcome::Duplicate => (StatusCode::OK, "duplicate"),
    };

    Ok((
        status,
        Json(view_response(
            &spec,
            spec_hash,
            execution_mode,
            lifecycle,
            Some(api_metadata),
            Some(outcome_text),
            state.experimental_advanced_view_features,
        )?),
    ))
}

fn view_has_published_view_inputs(runtime: &MaterializedViewRuntimeBinding) -> bool {
    runtime
        .input_bindings
        .iter()
        .any(|binding| matches!(binding, StandingInputBindingV1::PublishedView { .. }))
}

async fn bootstrap_consumer_view_after_registration(
    state: &ApiState,
    spec: &StandingViewSpec,
    runtime: &MaterializedViewRuntimeBinding,
    resolved_inputs: &[ResolvedAdmissionInputV1],
) -> Result<(), ApiError> {
    let active = state
        .view_registry()?
        .read_active(&spec.view_id)
        .await
        .map_err(materialized_view_registry_error_to_api)?;
    let Some(identity) = active_standing_runtime_identity(&active) else {
        return Err(ApiError::bad_request(format!(
            "consumer view `{}` has no standing runtime identity",
            spec.view_id
        )));
    };
    for resolved in resolved_inputs {
        let ResolvedAdmissionInputV1::PublishedView { binding, .. } = resolved else {
            continue;
        };
        let binding = binding.clone();
        let input_binding = runtime
            .input_bindings
            .iter()
            .find_map(|candidate| match candidate {
                StandingInputBindingV1::PublishedView {
                    edge_id,
                    producer_tenant_id,
                    producer_program_id,
                    published_relation,
                    bootstrap_cursor,
                    ..
                } if published_relation == &binding => Some((
                    edge_id,
                    producer_tenant_id,
                    producer_program_id,
                    published_relation,
                    bootstrap_cursor,
                )),
                _ => None,
            })
            .ok_or_else(|| {
                ApiError::bad_request(format!(
                    "consumer view `{}` is missing its persisted published-view input binding",
                    spec.view_id
                ))
            })?;
        let (
            edge_id,
            producer_tenant_id,
            producer_program_id,
            published_relation,
            bootstrap_cursor,
        ) = input_binding;
        let edge = view_dependency_edge_from_binding(
            producer_tenant_id,
            &identity.program_id,
            &spec.view_id,
            1,
            &published_relation.relation.relation_id,
            &published_relation.relation.relation_version,
            producer_program_id,
            published_relation,
        )
        .map_err(ApiError::bad_request)?;
        if &edge.edge_id != edge_id {
            return Err(ApiError::bad_request(format!(
                "consumer view `{}` dependency edge mismatch",
                spec.view_id
            )));
        }
        // Duplicate CREATE requests must resume, not reapply: if the consumer
        // already has a checkpoint with this edge's cursor, the baseline was
        // applied and only catch-up is needed. A checkpoint without the edge
        // cursor is corrupt and fails closed.
        let latest =
            read_latest_standing_runtime_checkpoint(state, identity, &spec.view_id).await?;
        let baseline_pending = match &latest {
            Some(record) => match consumer_edge_cursor(record, edge_id)? {
                Some(_) => false,
                None => {
                    return Err(ApiError::bad_request(format!(
                        "consumer view `{}` has a checkpoint without the dependency cursor for edge `{edge_id}`",
                        spec.view_id
                    )));
                }
            },
            None => true,
        };
        if baseline_pending {
            bootstrap_consumer_from_published_snapshot(
                state,
                &active,
                &ConsumerViewDependencyInputV1 {
                    edge,
                    binding: published_relation.clone(),
                    bootstrap_cursor: bootstrap_cursor.clone(),
                },
            )
            .await?;
        }
    }
    drain_published_view_dependencies(state).await?;
    activate_authoritative_view_bootstrap(state, identity, &spec.view_id).await?;
    state
        .view_registry()?
        .update_standing_runtime_lifecycle(
            &spec.view_id,
            &active.spec_hash,
            MaterializedViewLifecycleStatus::standing_runtime(),
        )
        .await
        .map_err(materialized_view_registry_error_to_api)?;
    Ok(())
}

async fn begin_authoritative_view_bootstrap(
    state: &ApiState,
    spec: &StandingViewSpec,
    runtime: &MaterializedViewRuntimeBinding,
    input_bindings: &[StandingInputBindingV1],
) -> Result<ViewBootstrapControlV1, ApiError> {
    let meta_store = state.view_bootstrap_meta_store.as_ref().ok_or_else(|| {
        ApiError::service_unavailable(
            "materialized view admission requires authoritative bootstrap metadata",
        )
    })?;
    let plan_hash = runtime
        .logical_plan
        .as_ref()
        .and_then(|plan| plan.plan_hash.clone())
        .ok_or_else(|| {
            ApiError::bad_request("materialized view logical plan is missing plan_hash")
        })?;
    let view_spec_json =
        serde_json::to_vec(spec).map_err(|error| ApiError::internal(error.to_string()))?;
    let view_inputs = input_bindings
        .iter()
        .filter_map(|binding| match binding {
            StandingInputBindingV1::PublishedView {
                edge_id,
                producer_program_id,
                published_relation,
                bootstrap_cursor,
                ..
            } => Some(BeginViewDependencyEdgeV1 {
                edge_id: edge_id.clone(),
                producer_program_id: producer_program_id.clone(),
                producer_view_id: published_relation.producer_view_id.clone(),
                producer_generation: published_relation.producer_view_generation,
                producer_plan_hash: published_relation.producer_plan_hash.clone(),
                input_relation_id: published_relation.relation.relation_id.clone(),
                input_relation_version: published_relation.relation.relation_version.clone(),
                output_stream_id: published_relation.output_stream_id.clone(),
                output_schema_hash: published_relation.output_schema_hash.clone(),
                key_descriptor_hash: published_relation.key_descriptor_hash.clone(),
                delta_codec_identity: published_relation.delta_codec_identity.clone(),
                frontier_kind: published_relation.frontier_kind.clone(),
                bootstrap_cursor: bootstrap_cursor.clone(),
            }),
            StandingInputBindingV1::Source { .. } => None,
        })
        .collect::<Vec<_>>();
    let expected_graph_revision = if view_inputs.is_empty() {
        0
    } else {
        meta_store
            .read_view_dependency_graph_revision(&runtime.standing_program_identity.tenant_id)
            .await
            .map_err(meta_error_to_api)?
    };
    let request = BeginViewBootstrapRequest {
        tenant_id: runtime.standing_program_identity.tenant_id.clone(),
        program_id: runtime.standing_program_identity.program_id.clone(),
        view_id: spec.view_id.clone(),
        plan_hash,
        view_spec_json,
        relations: spec
            .input_relations
            .iter()
            .filter(|relation| {
                !view_inputs
                    .iter()
                    .any(|edge| edge.input_relation_id == relation.relation_id)
            })
            .map(|relation| IngestSourceRelationIdentityV1 {
                relation_id: relation.relation_id.clone(),
                relation_version: relation.relation_version.clone(),
                relation_generation: INGEST_SOURCE_IDENTITY_GENERATION_V1,
                schema_fingerprint: relation.schema_fingerprint.clone(),
            })
            .collect(),
        view_inputs,
        expected_graph_revision,
    };
    match meta_store
        .begin_view_bootstrap(request)
        .await
        .map_err(meta_error_to_api)?
    {
        BeginViewBootstrapOutcome::Created(control)
        | BeginViewBootstrapOutcome::Duplicate(control) => Ok(control),
        BeginViewBootstrapOutcome::Conflict => Err(ApiError::conflict(format!(
            "authoritative view bootstrap conflict for `{}` (expected graph revision {expected_graph_revision})",
            spec.view_id
        ))),
    }
}

/// Per-capability SQL feature admission mode (Phase 5 gate split). Capabilities
/// that are a stable public contract use `Enabled`; capabilities that still
/// need design artifacts stay `Experimental` and reject on the public path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum FeatureAdmissionModeV1 {
    Enabled,
    Experimental,
}

/// Public view feature policy. The legacy single boolean maps to
/// `{event_time_windows: Enabled, analytic_windows: Enabled}` when set, and to
/// the Phase 5 public defaults (event-time enabled, analytic experimental)
/// when unset.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct PublicViewFeaturePolicyV1 {
    pub event_time_windows: FeatureAdmissionModeV1,
    pub analytic_windows: FeatureAdmissionModeV1,
}

impl Default for PublicViewFeaturePolicyV1 {
    fn default() -> Self {
        Self {
            event_time_windows: FeatureAdmissionModeV1::Enabled,
            analytic_windows: FeatureAdmissionModeV1::Experimental,
        }
    }
}

impl From<bool> for PublicViewFeaturePolicyV1 {
    fn from(experimental: bool) -> Self {
        if experimental {
            Self {
                event_time_windows: FeatureAdmissionModeV1::Enabled,
                analytic_windows: FeatureAdmissionModeV1::Enabled,
            }
        } else {
            Self::default()
        }
    }
}

pub(super) fn validate_public_view_feature_admission(
    state: &ApiState,
    request: &CreateViewRequest,
) -> Result<(), ApiError> {
    let policy = state.public_view_feature_policy;
    let sql = request.sql.to_ascii_lowercase();
    let event_time_enabled = policy.event_time_windows == FeatureAdmissionModeV1::Enabled;
    let analytic_enabled = policy.analytic_windows == FeatureAdmissionModeV1::Enabled;
    if !event_time_enabled
        && (contains_sql_function_call(&sql, "tumble")
            || contains_sql_function_call(&sql, "hop")
            || contains_sql_function_call(&sql, "session"))
    {
        return Err(ApiError::bad_request(
            "event-time window SQL is experimental and disabled for the public 1.0 API",
        ));
    }
    if !analytic_enabled
        && (contains_sql_function_call(&sql, "row_number")
            || contains_sql_function_call(&sql, "rank")
            || contains_sql_function_call(&sql, "dense_rank")
            || contains_sql_keyword(&sql, "over"))
    {
        return Err(ApiError::bad_request(
            "analytic window SQL is experimental and disabled for the public 1.0 API",
        ));
    }

    // TUMBLE is allowed for single-relation views (Phase 5 public admission)
    // HOP, SESSION remain blocked until their retention/recovery contracts are proven

    Ok(())
}

pub(super) fn validate_public_runtime_plan_admission(
    state: &ApiState,
    plan: &VelorixLogicalViewPlanV1,
) -> Result<(), ApiError> {
    let policy = state.public_view_feature_policy;
    let event_time_enabled = policy.event_time_windows == FeatureAdmissionModeV1::Enabled;
    let analytic_enabled = policy.analytic_windows == FeatureAdmissionModeV1::Enabled;
    if plan.input_relations.len() > PUBLIC_1_0_MAX_JOIN_INPUT_RELATIONS {
        return Err(ApiError::bad_request(format!(
            "materialized view uses {} input relations; public 1.0 supports at most {}",
            plan.input_relations.len(),
            PUBLIC_1_0_MAX_JOIN_INPUT_RELATIONS
        )));
    }
    match &plan.execution {
        VelorixLogicalViewExecutionV1::AnalyticRowNumber { .. } if !analytic_enabled => {
            Err(ApiError::bad_request(
                "analytic window execution is experimental and disabled for the public 1.0 API",
            ))
        }
        VelorixLogicalViewExecutionV1::TumblingEventTimeAggregate { .. } if !event_time_enabled => {
            Err(ApiError::bad_request(
                "event-time window execution is experimental and disabled for the public 1.0 API",
            ))
        }
        VelorixLogicalViewExecutionV1::AnalyticRowNumber { .. }
        | VelorixLogicalViewExecutionV1::TumblingEventTimeAggregate { .. } => Ok(()),
        VelorixLogicalViewExecutionV1::SingleKeySumCount { plan } => {
            validate_public_top_k_limit(plan.top_k.as_ref())
        }
        VelorixLogicalViewExecutionV1::FilterProject { plan } => {
            validate_public_top_k_limit(plan.top_k.as_ref())
        }
        VelorixLogicalViewExecutionV1::LatestByKey { plan } => {
            validate_public_top_k_limit(plan.top_k.as_ref())
        }
        VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { plan } => {
            validate_public_top_k_limit(plan.top_k.as_ref())
        }
        VelorixLogicalViewExecutionV1::ThreeInputInnerJoinCount { .. } => Ok(()),
        VelorixLogicalViewExecutionV1::TwoInputSemiAntiJoinProject { .. } => Ok(()),
    }
}

pub(super) fn validate_public_top_k_limit(
    top_k: Option<&velorix_core::view_plan::SupportedTopKPlan>,
) -> Result<(), ApiError> {
    if let Some(top_k) = top_k {
        if top_k.limit > PUBLIC_1_0_MAX_TOP_K_LIMIT {
            return Err(ApiError::bad_request(format!(
                "top-k limit {} exceeds public 1.0 limit {}",
                top_k.limit, PUBLIC_1_0_MAX_TOP_K_LIMIT
            )));
        }
    }
    Ok(())
}

pub(super) fn contains_sql_function_call(sql: &str, function_name: &str) -> bool {
    sql.match_indices(function_name).any(|(index, _)| {
        let bytes = sql.as_bytes();
        if index > 0 && is_sql_identifier_byte(bytes[index - 1]) {
            return false;
        }
        let mut next = index + function_name.len();
        if next < bytes.len() && is_sql_identifier_byte(bytes[next]) {
            return false;
        }
        while next < bytes.len() && bytes[next].is_ascii_whitespace() {
            next += 1;
        }
        next < bytes.len() && bytes[next] == b'('
    })
}

pub(super) fn contains_sql_keyword(sql: &str, keyword: &str) -> bool {
    sql.match_indices(keyword).any(|(index, _)| {
        let bytes = sql.as_bytes();
        let before_is_identifier = index > 0 && is_sql_identifier_byte(bytes[index - 1]);
        let after = index + keyword.len();
        let after_is_identifier = after < bytes.len() && is_sql_identifier_byte(bytes[after]);
        !before_is_identifier && !after_is_identifier
    })
}

pub(super) fn is_sql_identifier_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

pub(super) async fn register_materialized_view_execution(
    state: &ApiState,
    spec: &StandingViewSpec,
    api: Option<MaterializedViewApiMetadata>,
    runtime: MaterializedViewRuntimeBinding,
    lifecycle: Option<MaterializedViewLifecycleStatus>,
) -> Result<RegisterMaterializedViewOutcome, ApiError> {
    state
        .view_registry()?
        .register_with_api_metadata_runtime_execution(spec, api, runtime, lifecycle)
        .await
        .map_err(materialized_view_registry_error_to_api)
}

pub(super) fn materialized_view_runtime_binding_for_spec(
    catalogs: &[VelorixRelationCatalogV1],
    spec: &StandingViewSpec,
    input_bindings: &[StandingInputBindingV1],
) -> Result<MaterializedViewRuntimeBinding, ApiError> {
    let identity =
        standing_program_identity_from_materialized_view_runtime(catalogs, spec, input_bindings)?;
    let output_schema = only_output_relation_for_runtime_binding(spec)?;
    let logical_plan = lower_materialized_view_runtime_sql_to_logical_plan(
        spec.sql.as_str(),
        catalogs,
        output_schema,
    )?;
    Ok(MaterializedViewRuntimeBinding {
        runtime_kind: MATERIALIZED_VIEW_RUNTIME_NAME.to_string(),
        runtime_version: "builtin-v1".to_string(),
        standing_program_identity: identity,
        logical_plan: Some(logical_plan),
        published_relations: Vec::new(),
        input_bindings: input_bindings.to_vec(),
    })
}

/// Build a runtime binding for a published-view input consumer.
///
/// The input has no physical catalog. The output schema is inferred from the
/// SQL projection, the plan is lowered through the published single-key
/// sum/count lowerer, and the input catalog hash binds the verified producer
/// relation schema fingerprint.
pub(super) fn materialized_view_runtime_binding_for_published_spec(
    view_id: &str,
    sql: &str,
    relation: &RelationSchema,
    binding: &PublishedRelationBindingV1,
    codec: &str,
    frontier: &str,
) -> Result<MaterializedViewRuntimeBinding, ApiError> {
    let planner_input = PlannerRelationInput::from_published_binding(
        relation.clone(),
        codec.to_string(),
        frontier.to_string(),
    );
    let output_schema = infer_single_key_sum_count_output_schema(sql, &planner_input, view_id)
        .map_err(ApiError::bad_request)?;
    let logical_plan = lower_published_single_key_sum_count_sql(
        sql,
        &planner_input,
        &output_schema,
        codec,
        frontier,
    )
    .map_err(ApiError::bad_request)?;
    let spec = StandingViewSpec {
        view_id: view_id.to_string(),
        sql: sql.to_string(),
        dialect: SqlDialect::VelorixSql,
        source_kind: SqlSourceKind::StandingView,
        input_relations: vec![relation.clone()],
        output_relations: vec![output_schema],
        shape: StandingViewShape {
            is_materialized: true,
            multi_input: false,
            multi_output: false,
        },
    };
    let identity = standing_program_identity_from_materialized_view_runtime(&[], &spec)?;
    let mut binding_result = MaterializedViewRuntimeBinding {
        runtime_kind: MATERIALIZED_VIEW_RUNTIME_NAME.to_string(),
        runtime_version: "builtin-v1".to_string(),
        standing_program_identity: identity,
        logical_plan: Some(logical_plan),
        published_relations: Vec::new(),
    };
    let _ = binding;
    binding_result.standing_program_identity.input_catalog_hash =
        relation.schema_fingerprint.clone();
    Ok(binding_result)
}

pub(super) fn published_relation_bindings_for_spec(
    spec: &StandingViewSpec,
    runtime: &MaterializedViewRuntimeBinding,
    producer_view_generation: u64,
) -> Result<Vec<velorix_core::view_contract::PublishedRelationBindingV1>, ApiError> {
    let plan_hash = runtime
        .logical_plan
        .as_ref()
        .and_then(|plan| plan.plan_hash.as_deref())
        .ok_or_else(|| {
            ApiError::bad_request("materialized view logical plan is missing plan_hash")
        })?;
    spec.output_relations
        .iter()
        .map(|relation| {
            published_relation_binding_v1(
                &spec.view_id,
                producer_view_generation,
                plan_hash,
                relation,
            )
            .map_err(ApiError::bad_request)
        })
        .collect()
}

pub(super) fn published_relation_binding_for_active_view(
    active: &ActiveMaterializedView,
) -> Result<Option<PublishedRelationBindingV1>, ApiError> {
    let Some(runtime) = active.runtime.as_ref() else {
        return Ok(None);
    };
    match runtime.published_relations.as_slice() {
        [] => Ok(None),
        [binding] => Ok(Some(binding.clone())),
        bindings => Err(ApiError::internal(format!(
            "active standing view `{}` has {} published relation bindings; exactly one is supported",
            active.spec.view_id,
            bindings.len()
        ))),
    }
}

pub(super) fn only_output_relation_for_runtime_binding(
    spec: &StandingViewSpec,
) -> Result<&RelationSchema, ApiError> {
    let [output_schema] = spec.output_relations.as_slice() else {
        return Err(ApiError::bad_request(
            "materialized view runtime requires exactly one output relation",
        ));
    };
    Ok(output_schema)
}

/// Resolve a create-view request's input references into explicit source/view
/// admission inputs.
///
/// Each `InputRelationRef` carries an explicit `input_kind`. `Source` inputs
/// resolve against the physical relation catalog. `View` inputs resolve against
/// an active view's published output and produce an immutable dependency edge.
///
/// The first slice requires exactly one input and forbids mixing source and
/// view inputs, per the Phase 4 vertical-slice contract.
pub(super) async fn resolve_view_inputs_for_request(
    state: &ApiState,
    request: &CreateViewRequest,
) -> Result<Vec<ResolvedAdmissionInput>, ApiError> {
    let refs = if !request.input_relation_refs.is_empty() {
        &request.input_relation_refs
    } else if !request.input_relation_id.trim().is_empty()
        || !request.input_relation_version.trim().is_empty()
    {
        return read_single_source_input_for_request(state, request).await;
    } else {
        return Err(ApiError::bad_request(
            "view requires either input_relation_refs or input_relation_id/input_relation_version",
        ));
    };

    let mut resolved = Vec::with_capacity(refs.len());
    let mut seen = BTreeSet::new();
    for input in refs {
        if input.relation_id.trim().is_empty() || input.relation_version.trim().is_empty() {
            return Err(ApiError::bad_request(
                "input_relation_refs must include non-empty relation_id and relation_version",
            ));
        }
        if !seen.insert((input.relation_id.as_str(), input.relation_version.as_str())) {
            return Err(ApiError::bad_request(format!(
                "duplicate input_relation_refs entry for relation `{}` version `{}`",
                input.relation_id, input.relation_version
            )));
        }
        match input.input_kind {
            InputRelationKind::Source => {
                let catalog =
                    read_relation_catalog(state, &input.relation_id, &input.relation_version)
                        .await?;
                let relation =
                    catalog_input_relation_schema(&catalog).map_err(ApiError::bad_request)?;
                resolved.push(ResolvedAdmissionInput::Source {
                    binding: SourceInputBindingV1 {
                        relation_id: input.relation_id.clone(),
                        relation_version: input.relation_version.clone(),
                        relation_generation: INGEST_SOURCE_IDENTITY_GENERATION_V1,
                        schema_fingerprint: catalog.schema_fingerprint.to_string(),
                    },
                    relation,
                    catalog,
                });
            }
            InputRelationKind::View => {
                let resolved_view = resolve_single_view_input(state, input).await?;
                resolved.push(resolved_view);
            }
        }
    }
    Ok(resolved)
}

async fn read_single_source_input_for_request(
    state: &ApiState,
    request: &CreateViewRequest,
) -> Result<Vec<ResolvedAdmissionInput>, ApiError> {
    let catalog = read_relation_catalog(
        state,
        &request.input_relation_id,
        &request.input_relation_version,
    )
    .await?;
    let relation = catalog_input_relation_schema(&catalog).map_err(ApiError::bad_request)?;
    Ok(vec![ResolvedAdmissionInput::Source {
        binding: SourceInputBindingV1 {
            relation_id: request.input_relation_id.clone(),
            relation_version: request.input_relation_version.clone(),
            relation_generation: INGEST_SOURCE_IDENTITY_GENERATION_V1,
            schema_fingerprint: catalog.schema_fingerprint.to_string(),
        },
        relation,
        catalog,
    }])
}

/// Resolve a single `View` input reference against an active producer view.
async fn resolve_single_view_input(
    state: &ApiState,
    input: &InputRelationRef,
) -> Result<ResolvedAdmissionInput, ApiError> {
    let active_views = state
        .view_registry()?
        .list_active()
        .await
        .map_err(materialized_view_registry_error_to_api)?;
    let producer = active_views
        .iter()
        .find(|active| {
            active.spec.view_id == input.relation_id
                && active.spec.output_relations.iter().any(|output| {
                    output.relation_id == input.relation_id
                        && output.relation_version == input.relation_version
                })
        })
        .ok_or_else(|| {
            ApiError::bad_request(format!(
                "view input `{}` version `{}` does not match an active view output",
                input.relation_id, input.relation_version
            ))
        })?;
    if !standing_runtime_can_accept_incremental_ingest(producer) {
        return Err(ApiError::bad_request(format!(
            "view input producer `{}` is not in an active lifecycle",
            input.relation_id
        )));
    }
    let published = published_relation_binding_for_active_view(producer)?.ok_or_else(|| {
        ApiError::bad_request(format!(
            "view input producer `{}` has no published output binding",
            input.relation_id
        ))
    })?;
    validate_published_relation_binding_v1(&published).map_err(ApiError::bad_request)?;
    let identity = active_standing_runtime_identity(producer).ok_or_else(|| {
        ApiError::internal(format!(
            "active view `{}` has no standing runtime identity",
            input.relation_id
        ))
    })?;
    let relation = published.relation.clone();
    let edge = ViewDependencyEdgeBindingV1 {
        input_edge_id: format!("{}->{}", input.relation_id, input.relation_version),
        graph_revision: 0,
        producer_tenant_id: identity.tenant_id.clone(),
        producer_program_id: identity.program_id.clone(),
        producer_view_id: published.producer_view_id.clone(),
        producer_generation: published.producer_view_generation,
        producer_plan_hash: published.producer_plan_hash.clone(),
        output_schema_hash: published.output_schema_hash.clone(),
        key_descriptor_hash: published.key_descriptor_hash.clone(),
        output_stream_id: published.output_stream_id.clone(),
        delta_codec_identity: published.delta_codec_identity.clone(),
        frontier_kind: published.frontier_kind.clone(),
    };
    resolve_view_input_relation_v1(&edge, &identity.tenant_id, &identity.program_id, &published)
        .map_err(ApiError::bad_request)?;
    Ok(ResolvedAdmissionInput::View {
        relation,
        published,
        edge,
    })
}

pub(super) fn lower_materialized_view_runtime_sql_to_logical_plan(
    sql: &str,
    catalogs: &[VelorixRelationCatalogV1],
    output_schema: &RelationSchema,
) -> Result<VelorixLogicalViewPlanV1, ApiError> {
    if let [catalog] = catalogs {
        match lower_supported_analytic_row_number_sql_to_logical_plan(sql, catalog, output_schema) {
            Ok(plan) => return Ok(plan),
            Err(ViewPlanError::UnsupportedShape { .. }) => {}
            Err(error) => return Err(ApiError::bad_request(error)),
        }
    }
    lower_supported_sql_to_logical_plan(sql, catalogs, output_schema).map_err(ApiError::bad_request)
}

pub(super) fn standing_program_identity_from_materialized_view_runtime(
    catalogs: &[VelorixRelationCatalogV1],
    spec: &StandingViewSpec,
    input_bindings: &[StandingInputBindingV1],
) -> Result<StandingProgramIdentity, ApiError> {
    let input_schema_bytes = serde_json::to_vec(&spec.input_relations)
        .map_err(|source| ApiError::internal(source.to_string()))?;
    let output_schema_bytes = serde_json::to_vec(&spec.output_relations)
        .map_err(|source| ApiError::internal(source.to_string()))?;
    let input_catalog_hash = if input_bindings
        .iter()
        .any(|binding| matches!(binding, StandingInputBindingV1::PublishedView { .. }))
    {
        // View-on-view inputs are fenced by the full producer binding: a
        // generation, plan, schema, key, codec, or frontier change must
        // produce a different program identity and fail closed on restore.
        let mut binding_hashes = input_bindings
            .iter()
            .map(|binding| binding.input_catalog_hash().map_err(ApiError::bad_request))
            .collect::<Result<Vec<_>, ApiError>>()?;
        binding_hashes.sort();
        stable_bytes_hash(binding_hashes.join("\u{1f}").as_bytes())
    } else if catalogs.len() == 1 {
        catalogs[0].schema_fingerprint.as_str().to_string()
    } else {
        stable_bytes_hash(&input_schema_bytes)
    };
    let identity = StandingProgramIdentity {
        tenant_id: "default".to_string(),
        program_id: spec.view_id.clone(),
        view_ids: standing_program_view_ids_for_spec(spec),
        sql_hash: stable_bytes_hash(spec.sql.as_bytes()),
        input_catalog_hash,
        output_schema_hash: stable_bytes_hash(&output_schema_bytes),
        planner_identity: "velorix-logical-view-planner".to_string(),
        builtin_runtime_identities: vec![BuiltinRuntimeIdentity {
            name: MATERIALIZED_VIEW_RUNTIME_NAME.to_string(),
            version: "builtin-v1".to_string(),
        }],
        runtime_capabilities: vec![
            "materialized_view_runtime".to_string(),
            INCREMENTAL_KEY_SEMANTICS_VERSION_V1.to_string(),
            INCREMENTAL_BAG_SEMANTICS_VERSION_V1.to_string(),
        ],
        runtime_compatibility: "velorix-materialized-view-runtime-v1".to_string(),
        checkpoint_codec_identity: "velorix-materialized-view-state-v1".to_string(),
        native_code_policy: NativeCodePolicy::DisabledNoExternalDependencies,
        dependency_binding_digest: String::new(),
        authenticated_tenant_id: "default".to_string(),
    };
    identity.validate().map_err(ApiError::bad_request)?;
    Ok(identity)
}

pub(super) fn active_standing_runtime_identity(
    active: &ActiveMaterializedView,
) -> Option<&StandingProgramIdentity> {
    active
        .runtime
        .as_ref()
        .map(|runtime| &runtime.standing_program_identity)
}

pub(super) fn standing_program_view_ids_for_spec(spec: &StandingViewSpec) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut view_ids = Vec::new();
    for view_id in std::iter::once(&spec.view_id).chain(
        spec.output_relations
            .iter()
            .map(|schema| &schema.relation_id),
    ) {
        if seen.insert(view_id.clone()) {
            view_ids.push(view_id.clone());
        }
    }
    view_ids
}

pub(super) async fn ensure_standing_runtime_for_active_view(
    state: &ApiState,
    active: &ActiveMaterializedView,
) -> Result<Option<StandingRuntimeReplayPlan>, ApiError> {
    let Some(runtime_binding) = active.runtime.as_ref() else {
        return Ok(None);
    };
    let Some((runtime, replay_plan)) = restore_or_build_standing_runtime_for_runtime_binding(
        state,
        &active.spec,
        runtime_binding,
        &active.spec.input_relations,
        &active.spec.output_relations,
    )
    .await?
    else {
        return Ok(None);
    };
    let committed_checkpoint = read_latest_standing_runtime_checkpoint(
        state,
        runtime.program_identity(),
        &active.spec.view_id,
    )
    .await?
    .as_ref()
    .map(standing_runtime_checkpoint_pointer_from_record);
    state.set_standing_runtime_committed_checkpoint(
        runtime.program_identity(),
        &active.spec.view_id,
        committed_checkpoint,
    )?;
    insert_standing_runtime(state, &active.spec.view_id, runtime)?;
    Ok(Some(replay_plan))
}

pub(super) fn build_standing_runtime_for_runtime_binding(
    state: &ApiState,
    spec: &StandingViewSpec,
    runtime_binding: &MaterializedViewRuntimeBinding,
    catalogs: &[VelorixRelationCatalogV1],
    expected_input_schemas: &[RelationSchema],
    expected_output_schemas: &[RelationSchema],
) -> Result<Option<Box<dyn StandingProgramRuntime + Send>>, ApiError> {
    let identity = &runtime_binding.standing_program_identity;
    if state.standing_runtime(identity, &spec.view_id)?.is_some() {
        return Ok(None);
    }
    let Some(factory) = state.standing_runtime_factory(&runtime_binding.runtime_kind)? else {
        return Err(ApiError::bad_request(format!(
            "standing runtime factory is not registered for runtime `{}`",
            runtime_binding.runtime_kind
        )));
    };
    let logical_plan = runtime_binding
        .logical_plan
        .as_ref()
        .ok_or_else(|| ApiError::bad_request("standing runtime binding is missing logical plan"))?;
    let runtime = factory
        .create_with_catalogs_plan_and_spec(
            identity,
            catalogs,
            logical_plan,
            spec,
            expected_input_schemas,
            expected_output_schemas,
        )
        .map_err(ApiError::internal)?;
    if runtime.program_identity() != identity {
        return Err(ApiError::bad_request(
            StandingProgramRuntimeError::ProgramIdentityMismatch {
                expected_program_id: identity.program_id.clone(),
                actual_program_id: runtime.program_identity().program_id.clone(),
            },
        ));
    }
    validate_runtime_schemas(
        runtime.as_ref(),
        expected_input_schemas,
        expected_output_schemas,
    )?;
    Ok(Some(runtime))
}

/// Build a standing runtime bound to a published view output input.
///
/// Uses the published-binding factory seam. The input schema comes from the
/// persisted producer relation, not a physical catalog.
pub(super) fn build_standing_runtime_for_published_binding(
    state: &ApiState,
    spec: &StandingViewSpec,
    runtime_binding: &MaterializedViewRuntimeBinding,
    binding: &PublishedRelationBindingV1,
    expected_input_schemas: &[RelationSchema],
    expected_output_schemas: &[RelationSchema],
) -> Result<Option<Box<dyn StandingProgramRuntime + Send>>, ApiError> {
    let identity = &runtime_binding.standing_program_identity;
    if state.standing_runtime(identity, &spec.view_id)?.is_some() {
        return Ok(None);
    }
    let Some(factory) = state.standing_runtime_factory(&runtime_binding.runtime_kind)? else {
        return Err(ApiError::bad_request(format!(
            "standing runtime factory is not registered for runtime `{}`",
            runtime_binding.runtime_kind
        )));
    };
    let logical_plan = runtime_binding
        .logical_plan
        .as_ref()
        .ok_or_else(|| ApiError::bad_request("standing runtime binding is missing logical plan"))?;
    let runtime = factory
        .create_with_published_binding_plan_and_spec(
            identity,
            binding,
            logical_plan,
            spec,
            expected_input_schemas,
            expected_output_schemas,
        )
        .map_err(ApiError::internal)?;
    if runtime.program_identity() != identity {
        return Err(ApiError::bad_request(
            StandingProgramRuntimeError::ProgramIdentityMismatch {
                expected_program_id: identity.program_id.clone(),
                actual_program_id: runtime.program_identity().program_id.clone(),
            },
        ));
    }
    validate_runtime_schemas(
        runtime.as_ref(),
        expected_input_schemas,
        expected_output_schemas,
    )?;
    Ok(Some(runtime))
}

pub(super) async fn restore_or_build_standing_runtime_for_runtime_binding(
    state: &ApiState,
    spec: &StandingViewSpec,
    runtime_binding: &MaterializedViewRuntimeBinding,
    expected_input_schemas: &[RelationSchema],
    expected_output_schemas: &[RelationSchema],
) -> Result<
    Option<(
        Box<dyn StandingProgramRuntime + Send>,
        StandingRuntimeReplayPlan,
    )>,
    ApiError,
> {
    let identity = &runtime_binding.standing_program_identity;
    if state.standing_runtime(identity, &spec.view_id)?.is_some() {
        return Ok(None);
    }
    let Some(factory) = state.standing_runtime_factory(&runtime_binding.runtime_kind)? else {
        return Err(ApiError::bad_request(format!(
            "standing runtime factory is not registered for runtime `{}`",
            runtime_binding.runtime_kind
        )));
    };
    let logical_plan = runtime_binding
        .logical_plan
        .as_ref()
        .ok_or_else(|| ApiError::bad_request("standing runtime binding is missing logical plan"))?;
    let catalogs = if runtime_binding.input_bindings.is_empty() {
        read_relation_catalogs_for_input_schemas(state, expected_input_schemas).await?
    } else {
        catalogs_for_input_bindings(state, runtime_binding).await?
    };

    let (runtime, replay_plan) = if let Some(record) =
        read_latest_standing_runtime_checkpoint(state, identity, &spec.view_id).await?
    {
        record
            .checkpoint
            .validate_identity(identity)
            .map_err(ApiError::bad_request)?;
        if record.checkpoint.state_payload.is_some() {
            let replay_plan = standing_runtime_replay_plan_from_record_ref(&record);
            let factory = Arc::clone(&factory);
            let catalogs = catalogs.clone();
            let spec = spec.clone();
            let expected_input_schemas = expected_input_schemas.to_vec();
            let expected_output_schemas = expected_output_schemas.to_vec();
            (
                tokio::task::spawn_blocking(move || {
                    factory.restore_with_catalogs_and_spec(
                        record.checkpoint,
                        &catalogs,
                        &spec,
                        &expected_input_schemas,
                        &expected_output_schemas,
                    )
                })
                .await
                .map_err(ApiError::internal)?
                .map_err(ApiError::internal)?,
                replay_plan,
            )
        } else if runtime_binding
            .input_bindings
            .iter()
            .any(|binding| matches!(binding, StandingInputBindingV1::PublishedView { .. }))
        {
            // A published-input consumer checkpoint without its state payload
            // cannot be rebuilt from an empty runtime: history would silently
            // be skipped while the causal cut says otherwise. Fail closed.
            return Err(ApiError::bad_request(format!(
                "consumer view `{}` checkpoint is missing its state payload; rebuild the view explicitly before serving queries",
                spec.view_id
            )));
        } else {
            let factory = Arc::clone(&factory);
            let identity = identity.clone();
            let catalogs = catalogs.clone();
            let logical_plan = logical_plan.clone();
            let spec = spec.clone();
            let expected_input_schemas = expected_input_schemas.to_vec();
            let expected_output_schemas = expected_output_schemas.to_vec();
            (
                tokio::task::spawn_blocking(move || {
                    factory.create_with_catalogs_plan_and_spec(
                        &identity,
                        &catalogs,
                        &logical_plan,
                        &spec,
                        &expected_input_schemas,
                        &expected_output_schemas,
                    )
                })
                .await
                .map_err(ApiError::internal)?
                .map_err(ApiError::internal)?,
                StandingRuntimeReplayPlan::default(),
            )
        }
    } else {
        let factory = Arc::clone(&factory);
        let identity = identity.clone();
        let catalogs = catalogs.clone();
        let logical_plan = logical_plan.clone();
        let spec = spec.clone();
        let expected_input_schemas = expected_input_schemas.to_vec();
        let expected_output_schemas = expected_output_schemas.to_vec();
        (
            tokio::task::spawn_blocking(move || {
                factory.create_with_catalogs_plan_and_spec(
                    &identity,
                    &catalogs,
                    &logical_plan,
                    &spec,
                    &expected_input_schemas,
                    &expected_output_schemas,
                )
            })
            .await
            .map_err(ApiError::internal)?
            .map_err(ApiError::internal)?,
            StandingRuntimeReplayPlan::default(),
        )
    };
    if runtime.program_identity() != identity {
        return Err(ApiError::bad_request(
            StandingProgramRuntimeError::ProgramIdentityMismatch {
                expected_program_id: identity.program_id.clone(),
                actual_program_id: runtime.program_identity().program_id.clone(),
            },
        ));
    }
    validate_runtime_schemas(
        runtime.as_ref(),
        expected_input_schemas,
        expected_output_schemas,
    )?;
    Ok(Some((runtime, replay_plan)))
}

/// Resolves the runtime catalogs for a view's persisted input bindings:
/// registered source relations or published-view-output descriptors derived
/// from the producer's immutable binding.
pub(super) async fn catalogs_for_input_bindings(
    state: &ApiState,
    runtime_binding: &MaterializedViewRuntimeBinding,
) -> Result<Vec<VelorixRelationCatalogV1>, ApiError> {
    let mut catalogs = Vec::with_capacity(runtime_binding.input_bindings.len());
    for binding in &runtime_binding.input_bindings {
        match binding {
            StandingInputBindingV1::Source { relation, .. } => {
                catalogs.push(
                    read_relation_catalog(state, &relation.relation_id, &relation.relation_version)
                        .await?,
                );
            }
            StandingInputBindingV1::PublishedView {
                published_relation, ..
            } => {
                catalogs.push(
                    catalog_from_published_relation_binding(published_relation)
                        .map_err(ApiError::bad_request)?,
                );
            }
        }
    }
    if catalogs.is_empty() {
        return Err(ApiError::bad_request(
            "standing runtime binding has no resolvable input catalogs",
        ));
    }
    Ok(catalogs)
}

pub(super) fn validate_runtime_schemas(
    runtime: &(dyn StandingProgramRuntime + Send),
    expected_input_schemas: &[RelationSchema],
    expected_output_schemas: &[RelationSchema],
) -> Result<(), ApiError> {
    let actual_input_schemas = runtime.input_schemas();
    if actual_input_schemas != expected_input_schemas {
        return Err(ApiError::bad_request(
            "standing runtime input schemas do not match runtime metadata",
        ));
    }
    let actual_output_schemas = runtime.output_schemas();
    if actual_output_schemas != expected_output_schemas {
        return Err(ApiError::bad_request(
            "standing runtime output schemas do not match runtime metadata",
        ));
    }

    Ok(())
}

pub(super) fn insert_standing_runtime(
    state: &ApiState,
    view_id: &str,
    runtime: Box<dyn StandingProgramRuntime + Send>,
) -> Result<(), ApiError> {
    let key = standing_runtime_key(runtime.program_identity(), view_id);
    let mut runtimes = state
        .standing_runtimes
        .runtimes
        .lock()
        .map_err(|_| ApiError::internal("standing runtime registry lock poisoned"))?;
    runtimes.insert(key, Arc::new(Mutex::new(runtime)));
    Ok(())
}

pub(super) fn remove_standing_runtime(
    state: &ApiState,
    identity: &StandingProgramIdentity,
    view_id: &str,
) -> Result<(), ApiError> {
    remove_standing_runtime_if_present(state, identity, view_id).map(|_| ())
}

pub(super) fn remove_standing_runtime_if_present(
    state: &ApiState,
    identity: &StandingProgramIdentity,
    view_id: &str,
) -> Result<bool, ApiError> {
    let mut runtimes = state
        .standing_runtimes
        .runtimes
        .lock()
        .map_err(|_| ApiError::internal("standing runtime registry lock poisoned"))?;
    let key = standing_runtime_key(identity, view_id);
    let removed_runtime = runtimes.remove(&key).is_some();
    let mut local_state = state
        .standing_runtimes
        .local_state
        .lock()
        .map_err(|_| ApiError::internal("standing runtime local state lock poisoned"))?;
    let removed_local_state = local_state.remove(&key).is_some();
    Ok(removed_runtime || removed_local_state)
}

pub(super) fn validate_materialized_runtime_spec_admission(
    spec: &StandingViewSpec,
) -> Result<(), ApiError> {
    validate_materialized_standing_view_spec(spec).map_err(ApiError::bad_request)?;
    validate_materialized_runtime_relation_schemas_admission(
        "spec.input_relations",
        &spec.input_relations,
    )?;
    validate_materialized_runtime_relation_schemas_admission(
        "spec.output_relations",
        &spec.output_relations,
    )?;
    Ok(())
}

pub(super) fn validate_materialized_runtime_relation_schemas_admission(
    field: &str,
    schemas: &[RelationSchema],
) -> Result<(), ApiError> {
    for schema in schemas {
        for column in &schema.columns {
            validate_materialized_runtime_sql_type_admission(
                &format!("{field}.{}.{}", schema.relation_id, column.name),
                &column.data_type,
            )?;
        }
    }
    Ok(())
}

pub(super) fn validate_materialized_runtime_sql_type_admission(
    field: &str,
    data_type: &SqlDataType,
) -> Result<(), ApiError> {
    match data_type {
        SqlDataType::Timestamp {
            timezone: Some(timezone),
        } => Err(ApiError::bad_request(format!(
            "materialized runtime admission rejected `{field}`: timezone-bearing timestamps are not supported yet; timezone={timezone}"
        ))),
        SqlDataType::Array { element_type } => {
            validate_materialized_runtime_sql_type_admission(field, element_type)
        }
        SqlDataType::Struct { fields } => {
            for struct_field in fields {
                validate_materialized_runtime_sql_type_admission(
                    &format!("{field}.{}", struct_field.name),
                    &struct_field.data_type,
                )?;
            }
            Ok(())
        }
        SqlDataType::Map {
            key_type,
            value_type,
        } => {
            validate_materialized_runtime_sql_type_admission(&format!("{field}.key"), key_type)?;
            validate_materialized_runtime_sql_type_admission(&format!("{field}.value"), value_type)
        }
        other => {
            arrow_data_type_from_sql_data_type(other)?;
            Ok(())
        }
    }
}

pub(super) fn standing_runtime_key(
    identity: &StandingProgramIdentity,
    view_id: &str,
) -> StandingRuntimeKey {
    StandingRuntimeKey {
        tenant_id: identity.tenant_id.clone(),
        program_id: identity.program_id.clone(),
        view_id: view_id.to_string(),
    }
}

pub(super) fn standing_runtime_owner_token_from_claim(
    claim: &StandingRuntimeOwnerClaim,
) -> StandingRuntimeOwnerToken {
    StandingRuntimeOwnerToken {
        tenant_id: claim.tenant_id.clone(),
        program_id: claim.program_id.clone(),
        view_id: claim.view_id.clone(),
        owner_id: claim.owner_id.clone(),
        owner_epoch: claim.owner_epoch,
    }
}

pub(super) fn process_incarnation_owner_id(operator_id: String) -> Result<String, ApiError> {
    let operator_id = operator_id.trim();
    if operator_id.is_empty() {
        return Err(ApiError::bad_request("operator_id must not be empty"));
    }
    let boot_nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| ApiError::internal("system clock is before unix epoch"))?
        .as_nanos();
    Ok(format!(
        "{operator_id}/pid-{}/boot-{boot_nanos}",
        std::process::id()
    ))
}
