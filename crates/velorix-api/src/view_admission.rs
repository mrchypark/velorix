use super::*;

pub(super) async fn create_view(
    State(state): State<ApiState>,
    Json(request): Json<CreateViewRequest>,
) -> Result<(StatusCode, Json<ViewResponse>), ApiError> {
    validate_public_view_feature_admission(&state, &request)?;
    let catalogs = read_relation_catalogs_for_view_request(&state, &request).await?;
    let spec = view_spec_from_request(&state, &request, &catalogs)?;
    validate_materialized_runtime_spec_admission(&spec)?;
    state.validate_standing_runtime_fencing_or_evict().await?;
    let runtime_binding = materialized_view_runtime_binding_for_spec(&catalogs, &spec)?;
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
    let pending_runtime = build_standing_runtime_for_runtime_binding(
        &state,
        &spec,
        &runtime_binding,
        &catalogs,
        &spec.input_relations,
        &spec.output_relations,
    )?;
    let execution_mode = MaterializedViewExecutionMode::StandingRuntime;
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

pub(super) fn validate_public_view_feature_admission(
    state: &ApiState,
    request: &CreateViewRequest,
) -> Result<(), ApiError> {
    if state.experimental_advanced_view_features {
        return Ok(());
    }
    let sql = request.sql.to_ascii_lowercase();
    if contains_sql_function_call(&sql, "tumble")
        || contains_sql_function_call(&sql, "hop")
        || contains_sql_function_call(&sql, "session")
        || contains_sql_function_call(&sql, "row_number")
        || contains_sql_function_call(&sql, "rank")
        || contains_sql_function_call(&sql, "dense_rank")
        || contains_sql_keyword(&sql, "over")
    {
        return Err(ApiError::bad_request(
            "advanced view SQL is experimental and disabled for the public 1.0 API",
        ));
    }
    Ok(())
}

pub(super) fn validate_public_runtime_plan_admission(
    state: &ApiState,
    plan: &VelorixLogicalViewPlanV1,
) -> Result<(), ApiError> {
    if state.experimental_advanced_view_features {
        return Ok(());
    }
    if plan.input_relations.len() > PUBLIC_1_0_MAX_JOIN_INPUT_RELATIONS {
        return Err(ApiError::bad_request(format!(
            "materialized view uses {} input relations; public 1.0 supports at most {}",
            plan.input_relations.len(),
            PUBLIC_1_0_MAX_JOIN_INPUT_RELATIONS
        )));
    }
    match &plan.execution {
        VelorixLogicalViewExecutionV1::AnalyticRowNumber { .. }
        | VelorixLogicalViewExecutionV1::TumblingEventTimeAggregate { .. } => {
            Err(ApiError::bad_request(
                "advanced view execution is experimental and disabled for the public 1.0 API",
            ))
        }
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
) -> Result<MaterializedViewRuntimeBinding, ApiError> {
    let identity = standing_program_identity_from_materialized_view_runtime(catalogs, spec)?;
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
    })
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
) -> Result<StandingProgramIdentity, ApiError> {
    let input_schema_bytes = serde_json::to_vec(&spec.input_relations)
        .map_err(|source| ApiError::internal(source.to_string()))?;
    let output_schema_bytes = serde_json::to_vec(&spec.output_relations)
        .map_err(|source| ApiError::internal(source.to_string()))?;
    let input_catalog_hash = if catalogs.len() == 1 {
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
        runtime_capabilities: vec!["materialized_view_runtime".to_string()],
        runtime_compatibility: "velorix-materialized-view-runtime-v1".to_string(),
        checkpoint_codec_identity: "velorix-materialized-view-state-v1".to_string(),
        native_code_policy: NativeCodePolicy::DisabledNoExternalDependencies,
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
    let catalogs = read_relation_catalogs_for_input_schemas(state, expected_input_schemas).await?;

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
