use super::query_response::{QueryBatchResponse, QueryResponseFormat};
use super::*;
use axum::http::HeaderMap;
use velorix_runtime::runtime_contract::{
    acquire_query_permit, query_record_batches_table_with_bindings_and_policy_and_permit,
    QueryExecutionPermit,
};

pub(super) const DEFAULT_TENANT_ID: &str = "default";

pub(super) async fn create_query_policy(
    State(state): State<ApiState>,
    Json(request): Json<CreateQueryPolicyRequest>,
) -> Result<(StatusCode, Json<QueryPolicyResponse>), ApiError> {
    let record = state
        .query_policy_catalog()?
        .create_for_production_table_scan(
            DEFAULT_TENANT_ID,
            &request.query_policy_id,
            request.policy,
        )
        .await
        .map_err(query_policy_catalog_error_to_api)?;
    Ok((
        StatusCode::CREATED,
        Json(query_policy_response(record, Some("created"))),
    ))
}

pub(super) async fn get_query_policy(
    State(state): State<ApiState>,
    AxumPath(query_policy_id): AxumPath<String>,
) -> Result<Json<QueryPolicyResponse>, ApiError> {
    let record = state
        .query_policy_catalog()?
        .get_for_production_table_scan(DEFAULT_TENANT_ID, &query_policy_id)
        .await
        .map_err(query_policy_catalog_error_to_api)?;
    Ok(Json(query_policy_response(record, None)))
}

pub(super) fn query_policy_response(
    record: QueryPolicyCatalogRecord,
    outcome: Option<&str>,
) -> QueryPolicyResponse {
    QueryPolicyResponse {
        tenant_id: record.tenant_id,
        query_policy_id: record.query_policy_id,
        policy: record.policy,
        outcome: outcome.map(ToString::to_string),
    }
}

pub(super) async fn query_view_rows_get(
    State(state): State<ApiState>,
    AxumPath(view_id): AxumPath<String>,
    Query(mut query): Query<BTreeMap<String, String>>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let format = QueryResponseFormat::negotiate(&headers)?;
    let page_request = extract_snapshot_page_request(&mut query)?;
    let request_sql = query.remove("sql").filter(|value| !value.trim().is_empty());
    let parameters = query
        .into_iter()
        .map(|(name, value)| (name, Value::String(value)))
        .collect();
    let active = state
        .view_registry()?
        .read_active(&view_id)
        .await
        .map_err(materialized_view_registry_error_to_api)?;
    validate_direct_view_query_parameter_sources(&active, &parameters)?;
    query_active_view_output_batches_impl(
        state,
        active,
        None,
        request_sql,
        parameters,
        page_request,
        true,
    )
    .await?
    .into_http(format)
}

pub(super) async fn query_view_output_rows_get(
    State(state): State<ApiState>,
    AxumPath((view_id, output_id)): AxumPath<(String, String)>,
    Query(mut query): Query<BTreeMap<String, String>>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let format = QueryResponseFormat::negotiate(&headers)?;
    let page_request = extract_snapshot_page_request(&mut query)?;
    let request_sql = query.remove("sql").filter(|value| !value.trim().is_empty());
    let parameters = query
        .into_iter()
        .map(|(name, value)| (name, Value::String(value)))
        .collect();
    let active = state
        .view_registry()?
        .read_active(&view_id)
        .await
        .map_err(materialized_view_registry_error_to_api)?;
    query_active_view_output_batches_impl(
        state,
        active,
        Some(output_id),
        request_sql,
        parameters,
        page_request,
        false,
    )
    .await?
    .into_http(format)
}

pub(super) fn extract_snapshot_page_request(
    query: &mut BTreeMap<String, String>,
) -> Result<SnapshotPageRequest, ApiError> {
    let committed_epoch = match query.remove("epoch") {
        Some(value) if value.trim().is_empty() => None,
        Some(value) => Some(value.parse::<u64>().map_err(|_| {
            ApiError::bad_request("pagination parameter `epoch` must be a non-negative integer")
        })?),
        None => None,
    };
    let page_token = query.remove("page_token").filter(|value| !value.is_empty());
    let max_rows = match query.remove("max_rows") {
        Some(value) if value.trim().is_empty() => None,
        Some(value) => {
            let parsed = value.parse::<usize>().map_err(|_| {
                ApiError::bad_request("pagination parameter `max_rows` must be a positive integer")
            })?;
            if parsed == 0 {
                return Err(ApiError::bad_request(
                    "pagination parameter `max_rows` must be a positive integer",
                ));
            }
            Some(parsed)
        }
        None => None,
    };
    Ok(SnapshotPageRequest {
        committed_epoch,
        page_token,
        max_rows,
    })
}

pub(super) async fn query_view_api_get(
    State(state): State<ApiState>,
    AxumPath(api_path): AxumPath<String>,
    Query(mut query): Query<BTreeMap<String, String>>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let format = QueryResponseFormat::negotiate(&headers)?;
    let page_request = extract_snapshot_page_request(&mut query)?;
    let (active, mut parameters) = read_active_view_by_api_path(&state, &api_path).await?;
    let api = active.api.clone().unwrap_or_default();
    for (name, raw_value) in query {
        let value = request_query_value_for_api_field(&api, &name, raw_value.as_str())?;
        if api
            .request
            .iter()
            .any(|field| field.field_name == name && field.field_in == "path")
        {
            return Err(ApiError::bad_request(format!(
                "parameter `{name}` must be supplied by the API path"
            )));
        }
        if let Some(existing) = parameters.insert(name.clone(), value.clone()) {
            if existing != value {
                return Err(ApiError::bad_request(format!(
                    "parameter `{name}` is provided by both path and query with different values"
                )));
            }
        }
    }
    query_active_view_output_batches_impl(
        state,
        active,
        api.output_relation_id.clone(),
        None,
        parameters,
        page_request,
        true,
    )
    .await?
    .into_http(format)
}

pub(super) fn request_query_value_for_api_field(
    api: &MaterializedViewApiMetadata,
    name: &str,
    raw_value: &str,
) -> Result<Value, ApiError> {
    let Some(field) = api
        .request
        .iter()
        .find(|field| field.field_name == name && field.field_in == "query")
    else {
        return Ok(Value::String(raw_value.to_string()));
    };
    if field.r#type != "array" {
        return Ok(Value::String(raw_value.to_string()));
    }
    let value = serde_json::from_str::<Value>(raw_value).map_err(|error| {
        ApiError::bad_request(format!(
            "query parameter `{name}` with type `array` must be a JSON array: {error}"
        ))
    })?;
    if !value.is_array() {
        return Err(ApiError::bad_request(format!(
            "query parameter `{name}` with type `array` must be a JSON array"
        )));
    }
    Ok(value)
}

pub(super) async fn query_view_rows_post(
    State(state): State<ApiState>,
    AxumPath(view_id): AxumPath<String>,
    headers: HeaderMap,
    Json(request): Json<QueryViewRequest>,
) -> Result<Response, ApiError> {
    let format = QueryResponseFormat::negotiate(&headers)?;
    let active = state
        .view_registry()?
        .read_active(&view_id)
        .await
        .map_err(materialized_view_registry_error_to_api)?;
    if request.sql.is_none() {
        validate_direct_view_query_parameter_sources(&active, &request.parameters)?;
    }
    query_active_view_output_batches_impl(
        state,
        active,
        None,
        request.sql,
        request.parameters,
        SnapshotPageRequest {
            committed_epoch: request.epoch,
            page_token: request.page_token,
            max_rows: request.max_rows,
        },
        true,
    )
    .await?
    .into_http(format)
}

pub(super) async fn query_view_output_rows_post(
    State(state): State<ApiState>,
    AxumPath((view_id, output_id)): AxumPath<(String, String)>,
    headers: HeaderMap,
    Json(request): Json<QueryViewRequest>,
) -> Result<Response, ApiError> {
    let format = QueryResponseFormat::negotiate(&headers)?;
    let active = state
        .view_registry()?
        .read_active(&view_id)
        .await
        .map_err(materialized_view_registry_error_to_api)?;
    query_active_view_output_batches_impl(
        state,
        active,
        Some(output_id),
        request.sql,
        request.parameters,
        SnapshotPageRequest {
            committed_epoch: request.epoch,
            page_token: request.page_token,
            max_rows: request.max_rows,
        },
        false,
    )
    .await?
    .into_http(format)
}

pub(super) async fn read_active_view_by_api_path(
    state: &ApiState,
    api_path: &str,
) -> Result<(ActiveMaterializedView, BTreeMap<String, Value>), ApiError> {
    let normalized = normalize_api_path(api_path);
    let registry = state.view_registry()?;
    let indexes = registry
        .list_api_path_indexes()
        .await
        .map_err(materialized_view_registry_error_to_api)?;
    let matched = indexes
        .into_iter()
        .find_map(|index| {
            match_api_path_pattern(&index.normalized_url_path, &normalized)
                .map(|parameters| (index.view_id, parameters))
        })
        .ok_or_else(|| ApiError::bad_request(format!("view API path `/{normalized}` not found")))?;
    let active = registry
        .read_active(&matched.0)
        .await
        .map_err(materialized_view_registry_error_to_api)?;

    Ok((active, matched.1))
}

struct ViewQueryBatches {
    response: QueryBatchResponse,
    policy: QueryPolicy,
    json_view_schema: Option<RelationSchema>,
    response_schema: Option<MaterializedViewResponseSchema>,
    permit: QueryExecutionPermit,
}

impl ViewQueryBatches {
    fn into_http(mut self, format: QueryResponseFormat) -> Result<Response, ApiError> {
        let _permit = self.permit;
        if format == QueryResponseFormat::Arrow {
            if let Some(response_schema) = &self.response_schema {
                self.response =
                    query_response::apply_response_schema(self.response, response_schema)?;
            }
        }
        let view_schema = self.json_view_schema;
        let response_schema = self.response_schema;
        self.response
            .into_http_response(format, self.policy, |batches| {
                view_query_json_rows(batches, view_schema.as_ref(), response_schema.as_ref())
            })
    }
}

fn view_query_json_rows(
    batches: &[RecordBatch],
    view_schema: Option<&RelationSchema>,
    response_schema: Option<&MaterializedViewResponseSchema>,
) -> Result<Vec<Value>, ApiError> {
    let rows = match view_schema {
        Some(schema) => record_batches_to_json_rows_for_view_schema(schema, batches)?,
        None => record_batches_to_json_rows(batches)?,
    };
    match response_schema {
        Some(schema) => materialized_rows_to_api_rows(&rows, schema),
        None => Ok(rows),
    }
}

async fn query_active_view_output_batches_impl(
    state: ApiState,
    active: ActiveMaterializedView,
    requested_output_id: Option<String>,
    request_sql: Option<String>,
    parameters: BTreeMap<String, Value>,
    page_request: SnapshotPageRequest,
    use_view_api_metadata: bool,
) -> Result<ViewQueryBatches, ApiError> {
    let active = ensure_view_query_ready(&state, active).await?;
    ensure_view_execution_allowed(&active)?;
    let output_id = resolve_view_query_output_id(&active, requested_output_id.as_deref())?;
    let active_api = active.api.clone().unwrap_or_default();
    let raw_sql_query = request_sql.is_some();
    let use_view_api_metadata = use_view_api_metadata && !raw_sql_query;
    let api = if use_view_api_metadata {
        active_api.clone()
    } else {
        MaterializedViewApiMetadata::default()
    };
    let parameters = if raw_sql_query {
        parameters
    } else {
        resolve_request_parameters(&api.request, &parameters)?
    };
    let query_policy = query_policy_for_view_api(&state, &active_api).await?;
    let policy = query_policy.policy;
    let permit = acquire_query_permit(policy, query_policy.limiter.as_ref())
        .map_err(ApiError::bad_request)?;
    let json_view_schema = if !raw_sql_query && api.sql_template.is_none() {
        active
            .spec
            .output_relations
            .iter()
            .find(|schema| schema.relation_id == output_id)
            .cloned()
    } else {
        None
    };

    match active.execution_mode {
        MaterializedViewExecutionMode::StandingRuntime => {
            validate_standing_runtime_query_contract(
                &active.spec.view_id,
                request_sql.as_ref(),
                &api,
                &parameters,
                &page_request,
            )?;
            let (batches, logical_epoch, next_page_token) = if let Some(sql) = request_sql {
                let requested_epoch = page_request.committed_epoch;
                let sql = render_caller_sql_as_bound_sql(&sql, &parameters)?;
                let predicate = materialized_sql_predicate(
                    &active,
                    &output_id,
                    &normalize_view_query_sql(&sql, &output_id),
                    &[],
                );
                let page_request =
                    page_request_with_query_policy_limit(page_request, query_policy.policy);
                let page = standing_runtime_sql_page(
                    &state,
                    &active,
                    &output_id,
                    page_request,
                    predicate.as_ref(),
                    policy,
                )
                .await?;
                validate_standing_runtime_full_snapshot_page(
                    &active,
                    &output_id,
                    &page,
                    requested_epoch,
                )?;
                let batches = query_record_batches_table_with_bindings_and_policy_and_permit(
                    &output_id,
                    page.batches,
                    &normalize_view_query_sql(&sql, &output_id),
                    &[],
                    query_policy.policy,
                    &permit,
                )
                .await
                .map_err(ApiError::bad_request)?;
                (batches, page.logical_epoch, None)
            } else if let Some(sql_template) = api.sql_template.as_deref() {
                let bound_sql = render_view_sql_template(
                    &normalize_view_query_sql(sql_template, &output_id),
                    &api.request,
                    &parameters,
                )?;
                query_standing_runtime_batches_with_template(
                    &state,
                    &active,
                    &output_id,
                    bound_sql,
                    page_request,
                    query_policy,
                    &permit,
                )
                .await?
            } else {
                query_standing_runtime_batches(
                    &state,
                    &active,
                    &output_id,
                    page_request,
                    query_policy,
                )
                .await?
            };
            let schema = batches
                .first()
                .map(RecordBatch::schema)
                .ok_or_else(|| ApiError::internal("query returned no typed result schema"))?;
            Ok(ViewQueryBatches {
                response: QueryBatchResponse {
                    schema,
                    batches,
                    logical_epoch,
                    next_page_token,
                },
                policy,
                json_view_schema,
                response_schema: api.response_schema,
                permit,
            })
        }
    }
}

pub(super) fn resolve_view_query_output_id(
    active: &ActiveMaterializedView,
    requested_output_id: Option<&str>,
) -> Result<String, ApiError> {
    if let Some(output_id) = requested_output_id {
        if active
            .spec
            .output_relations
            .iter()
            .any(|schema| schema.relation_id == output_id)
        {
            return Ok(output_id.to_string());
        }
        return Err(ApiError::bad_request(format!(
            "view `{}` has no output relation `{output_id}`",
            active.spec.view_id
        )));
    }
    if active.spec.output_relations.len() == 1 {
        return Ok(active.spec.output_relations[0].relation_id.clone());
    }
    if active
        .spec
        .output_relations
        .iter()
        .any(|schema| schema.relation_id == active.spec.view_id)
    {
        return Ok(active.spec.view_id.clone());
    }
    Err(ApiError::bad_request(format!(
        "view `{}` has multiple output relations; query one explicitly with `/v1/views/{}/outputs/{{output_id}}/query`",
        active.spec.view_id, active.spec.view_id
    )))
}

pub(super) fn ensure_view_execution_allowed(
    active: &ActiveMaterializedView,
) -> Result<(), ApiError> {
    if active.lifecycle.admission_status != MaterializedViewAdmissionStatus::Admitted
        || active.lifecycle.deployment_status != MaterializedViewDeploymentStatus::Running
    {
        return Err(ApiError::service_unavailable(format!(
            "standing_runtime_not_deployed: view `{}` is not running yet",
            active.spec.view_id
        )));
    }
    Ok(())
}

pub(super) async fn ensure_view_query_ready(
    state: &ApiState,
    active: ActiveMaterializedView,
) -> Result<ActiveMaterializedView, ApiError> {
    if view_query_availability(&active.lifecycle) {
        if let Some(meta_store) = state.meta_store.as_ref() {
            let identity = active_standing_runtime_identity(&active).ok_or_else(|| {
                ApiError::service_unavailable(format!(
                    "standing_runtime_not_deployed: view `{}` has no runtime identity",
                    active.spec.view_id
                ))
            })?;
            let control = meta_store
                .read_view_bootstrap(
                    &identity.tenant_id,
                    &identity.program_id,
                    &active.spec.view_id,
                )
                .await
                .map_err(meta_error_to_api)?;
            if !control.is_some_and(|control| {
                control.lifecycle == ViewBootstrapLifecycleV1::Active
                    && control.active_checkpoint.is_some()
            }) {
                return Err(ApiError::service_unavailable(format!(
                    "MATERIALIZATION_LAG: authoritative activation is incomplete for view `{}`",
                    active.spec.view_id
                )));
            }
        }
        return Ok(active);
    }
    if view_has_backfill_required_lag(&active) {
        return Err(materialization_lag_error(&active));
    }

    ensure_view_execution_allowed(&active)?;
    Ok(active)
}

pub(super) fn materialization_lag_error(active: &ActiveMaterializedView) -> ApiError {
    ApiError::service_unavailable_with_details(
        format!(
            "MATERIALIZATION_LAG: view `{}` is not fully materialized; query reads published materialized output only, run `/v1/views/{}/backfill` before querying",
            active.spec.view_id, active.spec.view_id
        ),
        json!({
            "code": "MATERIALIZATION_LAG",
            "view_id": active.spec.view_id,
            "query_authority": "published_materialized_output",
            "coverage_state": materialization_coverage_response(&active.lifecycle, false).state,
            "committed_frontier": {
                "status": "ahead_of_materialized_output",
                "source_read_on_query_path": false
            },
            "materialized_frontier": {
                "status": "not_queryable_until_backfill_checkpoint_published"
            },
            "recovery_action": format!("/v1/views/{}/backfill", active.spec.view_id)
        }),
    )
}

pub(super) struct ActiveViewBackfillStepOutcome {
    active: ActiveMaterializedView,
    replay: StandingRuntimeBackfillReplayOutcome,
}

pub(super) async fn run_view_backfill_step(
    state: &ApiState,
    view_id: &str,
    batch_limit: Option<usize>,
    range: Option<&BackfillRangeRequest>,
    scope: Option<&BackfillScopeRequest>,
) -> Result<BackfillViewResponse, ApiError> {
    let active = state
        .view_registry()?
        .read_active(view_id)
        .await
        .map_err(materialized_view_registry_error_to_api)?;
    let outcome = run_active_view_backfill_step(state, active, batch_limit, range, scope).await?;
    let progress = committed_backfill_progress(state, &outcome.active).await?;
    Ok(backfill_view_response(
        &outcome.active,
        if outcome.replay.remaining_batches == 0 {
            "completed"
        } else {
            "advanced"
        },
        "sync",
        outcome.replay.applied_batches,
        outcome.replay.remaining_batches,
        progress,
        state.experimental_advanced_view_features,
    ))
}

pub(super) async fn run_active_view_backfill_step(
    state: &ApiState,
    active: ActiveMaterializedView,
    batch_limit: Option<usize>,
    range: Option<&BackfillRangeRequest>,
    scope: Option<&BackfillScopeRequest>,
) -> Result<ActiveViewBackfillStepOutcome, ApiError> {
    let relations = active
        .spec
        .input_relations
        .iter()
        .map(|input| (input.relation_id.as_str(), input.relation_version.as_str()))
        .collect::<BTreeSet<_>>();
    let mut _relation_guards = Vec::new();
    for (relation_id, relation_version) in relations {
        _relation_guards.push(
            state
                .relation_operation_lock(relation_id, relation_version)?
                .lock_owned()
                .await,
        );
    }
    if view_query_availability(&active.lifecycle) {
        let progress = committed_backfill_progress(state, &active).await?;
        if progress.remaining_batches == 0 {
            if let Some(identity) = active_standing_runtime_identity(&active) {
                activate_authoritative_view_bootstrap(state, identity, &active.spec.view_id)
                    .await?;
            }
            return Ok(ActiveViewBackfillStepOutcome {
                active,
                replay: StandingRuntimeBackfillReplayOutcome::default(),
            });
        }
    } else if !view_has_backfill_required_lag(&active) {
        ensure_view_execution_allowed(&active)?;
        return Ok(ActiveViewBackfillStepOutcome {
            active,
            replay: StandingRuntimeBackfillReplayOutcome::default(),
        });
    }
    let Some(identity) = active_standing_runtime_identity(&active) else {
        return Err(ApiError::service_unavailable(format!(
            "standing_runtime_not_deployed: view `{}` is backfill pending but has no runtime binding",
            active.spec.view_id
        )));
    };
    let replay_plan = if state
        .standing_runtime(identity, &active.spec.view_id)?
        .is_some()
    {
        read_latest_standing_runtime_checkpoint(state, identity, &active.spec.view_id)
            .await?
            .as_ref()
            .map(standing_runtime_replay_plan_from_record_ref)
            .unwrap_or_default()
    } else {
        ensure_standing_runtime_for_active_view(state, &active)
            .await?
            .unwrap_or_default()
    };
    let replay = replay_committed_ingest_into_standing_runtime_limited(
        state,
        &active,
        &replay_plan,
        batch_limit,
        range,
        scope,
    )
    .await?;
    if range.is_none() && scope.is_none() && replay.remaining_batches == 0 {
        activate_authoritative_view_bootstrap(state, identity, &active.spec.view_id).await?;
        state
            .view_registry()?
            .update_standing_runtime_lifecycle(
                &active.spec.view_id,
                &active.spec_hash,
                MaterializedViewLifecycleStatus::standing_runtime(),
            )
            .await
            .map_err(materialized_view_registry_error_to_api)?;
    }

    let refreshed = state
        .view_registry()?
        .read_active(&active.spec.view_id)
        .await
        .map_err(materialized_view_registry_error_to_api)?;
    Ok(ActiveViewBackfillStepOutcome {
        active: refreshed,
        replay,
    })
}

pub(super) async fn activate_authoritative_view_bootstrap(
    state: &ApiState,
    identity: &StandingProgramIdentity,
    view_id: &str,
) -> Result<(), ApiError> {
    let Some(meta_store) = state.meta_store.as_ref() else {
        return Ok(());
    };
    let control = meta_store
        .read_view_bootstrap(&identity.tenant_id, &identity.program_id, view_id)
        .await
        .map_err(meta_error_to_api)?
        .ok_or_else(|| {
            ApiError::service_unavailable(format!(
                "authoritative view bootstrap control is unavailable for `{view_id}`"
            ))
        })?;
    if control.lifecycle == ViewBootstrapLifecycleV1::Active {
        return Ok(());
    }
    let owner = state
        .acquire_standing_runtime_owner(identity, view_id)
        .await?
        .ok_or_else(|| {
            ApiError::service_unavailable(
                "authoritative view activation requires a metadata owner fence",
            )
        })?;
    let fixed = meta_store
        .fix_view_bootstrap_activation_cut(FixViewBootstrapActivationCutRequest {
            tenant_id: identity.tenant_id.clone(),
            program_id: identity.program_id.clone(),
            view_id: view_id.to_string(),
            bootstrap_generation: control.bootstrap_generation,
            plan_hash: control.plan_hash.clone(),
            owner: owner.clone(),
        })
        .await
        .map_err(meta_error_to_api)?;
    let fixed_control = match fixed {
        FixViewBootstrapActivationCutOutcome::Fixed(control)
        | FixViewBootstrapActivationCutOutcome::Duplicate(control) => control,
        FixViewBootstrapActivationCutOutcome::Conflict => {
            return Err(ApiError::conflict(format!(
                "view `{view_id}` activation cut could not be fixed because the current checkpoint does not cover the bootstrap cut"
            )))
        }
    };
    let checkpoint = meta_store
        .read_standing_runtime_checkpoint(&identity.tenant_id, &identity.program_id, view_id)
        .await
        .map_err(meta_error_to_api)?
        .ok_or_else(|| {
            ApiError::service_unavailable(format!(
                "authoritative standing runtime checkpoint is unavailable for `{view_id}`"
            ))
        })?;
    match meta_store
        .promote_view_bootstrap(PromoteViewBootstrapRequest {
            tenant_id: identity.tenant_id.clone(),
            program_id: identity.program_id.clone(),
            view_id: view_id.to_string(),
            bootstrap_generation: fixed_control.bootstrap_generation,
            plan_hash: fixed_control.plan_hash,
            checkpoint,
            owner,
        })
        .await
        .map_err(meta_error_to_api)?
    {
        PromoteViewBootstrapOutcome::Promoted(_)
        | PromoteViewBootstrapOutcome::Duplicate(_) => Ok(()),
        PromoteViewBootstrapOutcome::Conflict => Err(ApiError::conflict(format!(
            "view `{view_id}` activation was fenced because the current checkpoint does not cover the fixed activation cut"
        ))),
    }
}

async fn query_standing_runtime_batches_with_template(
    state: &ApiState,
    active: &ActiveMaterializedView,
    output_id: &str,
    bound_sql: BoundViewSql,
    page_request: SnapshotPageRequest,
    query_policy: ViewQueryPolicy,
    permit: &QueryExecutionPermit,
) -> Result<(Vec<RecordBatch>, u64, Option<String>), ApiError> {
    let requested_epoch = page_request.committed_epoch;
    let predicate =
        materialized_sql_predicate(active, output_id, &bound_sql.sql, &bound_sql.bind_values);
    let page = standing_runtime_sql_page(
        state,
        active,
        output_id,
        page_request,
        predicate.as_ref(),
        query_policy.policy,
    )
    .await?;
    validate_standing_runtime_full_snapshot_page(active, output_id, &page, requested_epoch)?;
    let batches = query_record_batches_table_with_bindings_and_policy_and_permit(
        output_id,
        page.batches,
        &bound_sql.sql,
        &bound_sql.bind_values,
        query_policy.policy,
        permit,
    )
    .await
    .map_err(ApiError::bad_request)?;

    Ok((batches, page.logical_epoch, None))
}

async fn standing_runtime_sql_page(
    state: &ApiState,
    active: &ActiveMaterializedView,
    output_id: &str,
    request: SnapshotPageRequest,
    predicate: Option<&velorix_core::query::PagePredicate>,
    policy: QueryPolicy,
) -> Result<MaterializedViewPage, ApiError> {
    state.validate_standing_runtime_fencing_or_evict().await?;
    if let Some(identity) = active_standing_runtime_identity(active) {
        // Pruning is optional; SQL still needs complete bounded materialized input.
        let predicate = predicate.unwrap_or(&velorix_core::query::PagePredicate::Unknown);
        if let Some(page) = materialized_output_pages::query_filtered(
            state,
            active,
            identity,
            output_id,
            request.clone(),
            predicate,
            policy,
        )
        .await?
        {
            return Ok(page);
        }
    }
    let mut complete = request;
    complete.max_rows = None;
    standing_runtime_page(state, active, output_id, complete, policy).await
}

fn materialized_sql_predicate(
    active: &ActiveMaterializedView,
    output_id: &str,
    sql: &str,
    binds: &[QueryBindValue],
) -> Option<velorix_core::query::PagePredicate> {
    let schema = active
        .spec
        .output_relations
        .iter()
        .find(|s| s.relation_id == output_id)?;
    velorix_core::query::materialized_page_predicate(sql, schema, binds)
}

pub(super) fn validate_standing_runtime_full_snapshot_page(
    active: &ActiveMaterializedView,
    output_id: &str,
    page: &MaterializedViewPage,
    requested_epoch: Option<u64>,
) -> Result<(), ApiError> {
    let identity = active_standing_runtime_identity(active).ok_or_else(|| {
        ApiError::conflict(format!(
            "standing runtime view `{}` is missing runtime identity",
            active.spec.view_id
        ))
    })?;
    let expected_view = ScopedViewId {
        tenant_id: identity.tenant_id.clone(),
        program_id: identity.program_id.clone(),
        view_id: output_id.to_string(),
    };
    if page.view != expected_view {
        return Err(ApiError::conflict(format!(
            "standing runtime view `{}` output `{output_id}` returned a page for a different scoped view",
            active.spec.view_id
        )));
    }
    if let Some(epoch) = requested_epoch {
        if page.logical_epoch != epoch {
            return Err(ApiError::conflict(format!(
                "standing runtime view `{}` returned epoch {} for requested epoch {epoch}",
                active.spec.view_id, page.logical_epoch
            )));
        }
    }
    if page.next_page_token.is_some() {
        return Err(ApiError::conflict(format!(
            "full materialized snapshot is unavailable for standing runtime view `{}`",
            active.spec.view_id
        )));
    }
    let output_schema = active
        .spec
        .output_relations
        .iter()
        .find(|schema| schema.relation_id == output_id)
        .ok_or_else(|| {
            ApiError::conflict(format!(
                "standing runtime view `{}` has no matching output schema for `{output_id}`",
                active.spec.view_id
            ))
        })?;
    if page.schema_fingerprint != output_schema.schema_fingerprint {
        return Err(ApiError::conflict(format!(
            "standing runtime view `{}` returned schema fingerprint `{}` but active schema fingerprint is `{}`",
            active.spec.view_id, page.schema_fingerprint, output_schema.schema_fingerprint
        )));
    }
    let expected_arrow_schema = arrow_schema_from_incremental_relation_schema(output_schema)?;
    if page.batches.is_empty() {
        return Err(ApiError::conflict(format!(
            "standing runtime view `{}` returned no record batches",
            active.spec.view_id
        )));
    }
    for batch in &page.batches {
        if batch.schema().as_ref() != expected_arrow_schema.as_ref() {
            return Err(ApiError::conflict(format!(
                "standing runtime view `{}` returned batches that do not match the active output schema",
                active.spec.view_id
            )));
        }
    }

    Ok(())
}

async fn query_standing_runtime_batches(
    state: &ApiState,
    active: &ActiveMaterializedView,
    output_id: &str,
    mut page_request: SnapshotPageRequest,
    query_policy: ViewQueryPolicy,
) -> Result<(Vec<RecordBatch>, u64, Option<String>), ApiError> {
    if let Some(max_rows) = query_policy.policy.max_output_rows {
        page_request.max_rows = Some(
            page_request
                .max_rows
                .map_or(max_rows, |requested| requested.min(max_rows)),
        );
    }
    let page =
        standing_runtime_page(state, active, output_id, page_request, query_policy.policy).await?;
    Ok((page.batches, page.logical_epoch, page.next_page_token))
}

pub(super) fn page_request_with_query_policy_limit(
    mut page_request: SnapshotPageRequest,
    policy: QueryPolicy,
) -> SnapshotPageRequest {
    let Some(policy_fetch_rows) = policy
        .max_output_rows
        .and_then(|max_rows| max_rows.checked_add(1))
    else {
        return page_request;
    };
    page_request.max_rows = Some(match page_request.max_rows {
        Some(requested_rows) => requested_rows.min(policy_fetch_rows),
        None => policy_fetch_rows,
    });
    page_request
}

pub(super) async fn standing_runtime_page(
    state: &ApiState,
    active: &ActiveMaterializedView,
    output_id: &str,
    page_request: SnapshotPageRequest,
    policy: QueryPolicy,
) -> Result<MaterializedViewPage, ApiError> {
    state.validate_standing_runtime_fencing_or_evict().await?;
    let identity = active_standing_runtime_identity(active).ok_or_else(|| {
        ApiError::conflict(format!(
            "standing runtime view `{}` is missing runtime identity",
            active.spec.view_id
        ))
    })?;
    if let Some(page) = standing_runtime_page_from_output_manifest(
        state,
        active,
        identity,
        output_id,
        page_request.clone(),
        policy,
    )
    .await?
    {
        return Ok(page);
    }

    standing_runtime_page_from_checkpoint_published_output(
        state,
        active,
        identity,
        output_id,
        page_request,
    )
    .await?
    .ok_or_else(|| materialization_lag_error(active))
}

pub(super) async fn standing_runtime_page_from_output_manifest(
    state: &ApiState,
    active: &ActiveMaterializedView,
    identity: &StandingProgramIdentity,
    output_id: &str,
    page_request: SnapshotPageRequest,
    policy: QueryPolicy,
) -> Result<Option<MaterializedViewPage>, ApiError> {
    if let Some(page) = materialized_output_pages::query_with_policy(
        state,
        active,
        identity,
        output_id,
        page_request.clone(),
        policy,
    )
    .await?
    {
        return Ok(Some(page));
    }
    let schema = active
        .spec
        .output_relations
        .iter()
        .find(|s| s.relation_id == output_id)
        .ok_or_else(|| ApiError::conflict("materialized output schema is unavailable"))?;
    legacy_materialized_page(
        state,
        &active.spec.view_id,
        identity,
        output_id,
        schema,
        page_request,
        false,
        policy,
        0,
        0,
    )
    .await
    .map(Some)
}
pub(super) async fn standing_runtime_checkpoint_output_manifest(
    state: &ApiState,
    record: &StandingRuntimeCheckpointRecord,
    output_id: &str,
) -> Result<Option<StandingRuntimeOutputManifestRecord>, ApiError> {
    if let Some(output_ref) = record
        .checkpoint
        .output_manifest_refs
        .iter()
        .find(|output_ref| {
            output_ref
                .strip_prefix(STANDING_RUNTIME_OUTPUT_MANIFEST_REF_PREFIX)
                .and_then(|key| {
                    ObjectKey::parse_standing_runtime_output_manifest(key.to_string())
                        .ok()
                        .map(|(_, parts)| parts)
                })
                .is_some_and(|parts| parts.view_id == output_id)
        })
    {
        return read_standing_runtime_output_manifest_record(state, output_ref, &record.view_id)
            .await
            .map(|(_key, manifest)| Some(manifest));
    }

    let checkpoint_key =
        ObjectKey::parse_standing_runtime_checkpoint(record.checkpoint_key.clone())
            .map_err(ApiError::bad_request)?
            .0;
    let Some(publication) = standing_runtime_output_manifest_record_for_checkpoint(
        &record.checkpoint,
        output_id,
        &checkpoint_key,
    )?
    else {
        return Ok(None);
    };
    let output_ref = format!(
        "{STANDING_RUNTIME_OUTPUT_MANIFEST_REF_PREFIX}{}",
        publication.manifest_key.as_str()
    );
    maybe_read_standing_runtime_output_manifest_record(state, &output_ref, &record.view_id)
        .await
        .map(|record| record.map(|(_key, manifest)| manifest))
}

pub(super) async fn standing_runtime_page_from_checkpoint_published_output(
    state: &ApiState,
    active: &ActiveMaterializedView,
    identity: &StandingProgramIdentity,
    output_id: &str,
    page_request: SnapshotPageRequest,
) -> Result<Option<MaterializedViewPage>, ApiError> {
    let Some(record) =
        read_latest_standing_runtime_checkpoint(state, identity, &active.spec.view_id).await?
    else {
        return Ok(None);
    };
    let Some(published_output) = standing_runtime_checkpoint_published_output(&record.checkpoint)
    else {
        return Ok(None);
    };
    let output_schema = active
        .spec
        .output_relations
        .iter()
        .find(|schema| schema.relation_id == output_id)
        .ok_or_else(|| {
            ApiError::conflict(format!(
                "standing runtime view `{}` has no matching output schema for `{output_id}`",
                active.spec.view_id
            ))
        })?;
    let published_output: DeltaBatch = serde_json::from_value(published_output)
        .map_err(|source| ApiError::bad_request(source.to_string()))?;
    let aggregate_outputs =
        standing_runtime_output_aggregate_outputs_for_checkpoint(&record.checkpoint)?;
    let scoped_view = ScopedViewId {
        tenant_id: identity.tenant_id.clone(),
        program_id: identity.program_id.clone(),
        view_id: output_id.to_string(),
    };
    let page = velorix_runtime::materialized_view_runtime::materialized_delta_to_page(
        output_schema,
        &published_output,
        scoped_view,
        record.checkpoint.logical_epoch,
        page_request,
        aggregate_outputs.as_deref(),
    )
    .map_err(ApiError::bad_request)?;
    Ok(Some(page))
}

pub(super) async fn standing_runtime_published_output_from_manifest_page(
    state: &ApiState,
    manifest: &StandingRuntimeOutputManifestRecord,
) -> Result<DeltaBatch, ApiError> {
    if materialized_output_pages::is_materialized_codec(&manifest.output_encoding) {
        // IPC pages carry canonical public rows and are bound by the manifest's
        // page hashes/schema/statistics; their decoded root header may be empty.
        let mut output = DeltaBatch::default();
        for page in &manifest.pages {
            let (_, record) =
                read_standing_runtime_output_page_record(state, page, &manifest.view_id).await?;
            let rows = materialized_output_pages::validate_page_binding(manifest, &record)?;
            output = output.combine(&rows);
        }
        return Ok(output);
    }
    let Some(page) = manifest.pages.iter().find(|page| page.page_index == 0) else {
        return Err(ApiError::bad_request(format!(
            "standing runtime output manifest has no first page for `{}/{}/{}`",
            manifest.tenant_id, manifest.program_id, manifest.view_id
        )));
    };
    let (_key, page_record) =
        read_standing_runtime_output_page_record(state, page, &manifest.view_id).await?;
    if page_record.output_content_hash != manifest.output_content_hash
        || page_record.logical_epoch != manifest.logical_epoch
        || page_record.tenant_id != manifest.tenant_id
        || page_record.program_id != manifest.program_id
        || page_record.view_id != manifest.view_id
    {
        return Err(ApiError::bad_request(format!(
            "standing runtime output page is not bound to manifest for `{}/{}/{}`",
            manifest.tenant_id, manifest.program_id, manifest.view_id
        )));
    }
    serde_json::from_value(page_record.published_output)
        .map_err(|source| ApiError::bad_request(source.to_string()))
}

pub(super) fn standing_runtime_output_aggregate_outputs_for_checkpoint(
    checkpoint: &RuntimeCheckpoint,
) -> Result<Option<Vec<SupportedAggregateOutput>>, ApiError> {
    let Some(state_payload) = &checkpoint.state_payload else {
        return Ok(None);
    };
    let payload: Value = serde_json::from_str(&state_payload.payload)
        .map_err(|source| ApiError::bad_request(source.to_string()))?;
    match payload.get("runtime_kind").and_then(Value::as_str) {
        Some(
            "filter_project"
            | "analytic_row_number"
            | "latest_by_key"
            | "scalar_aggregate_filter"
            | "analytic_window_frame"
            | "two_input_semi_anti_join_project_v1",
        ) => return Ok(None),
        Some(
            "interval_join"
            | "interval_join_v2"
            | "cross_join_v2"
            | "recursive_fixpoint_v2"
            | "temporal_join_v1",
        ) => return Ok(Some(Vec::new())),
        Some("two_input_join_sum_count" | "two_input_join_common_dag_reference_v1") => {
            let Some(plan) = payload.get("plan").filter(|plan| !plan.is_null()) else {
                return Ok(None);
            };
            let plan: SupportedJoinViewPlan = serde_json::from_value(plan.clone())
                .map_err(|source| ApiError::bad_request(source.to_string()))?;
            return Ok(Some(supported_join_view_plan_aggregate_outputs(&plan)));
        }
        Some("three_input_inner_join_count_dag_v1") => {
            let Some(logical_plan) = payload
                .get("logical_plan")
                .filter(|logical_plan| !logical_plan.is_null())
            else {
                return Err(ApiError::bad_request(
                    "three-input join checkpoint is missing its admitted plan",
                ));
            };
            let logical_plan: VelorixLogicalViewPlanV1 =
                serde_json::from_value(logical_plan.clone())
                    .map_err(|source| ApiError::bad_request(source.to_string()))?;
            let VelorixLogicalViewExecutionV1::ThreeInputInnerJoinCount { plan } =
                logical_plan.execution
            else {
                return Err(ApiError::bad_request(
                    "three-input join checkpoint execution does not match its runtime kind",
                ));
            };
            return Ok(Some(vec![SupportedAggregateOutput {
                function: LogicalPlanAggregateFunctionV1::Count,
                input_column_id: None,
                input_relation_side: None,
                input_expression: None,
                output_column_id: plan.count_output_column_id,
            }]));
        }
        Some("tumbling_event_time_aggregate") => {
            let Some(plan) = payload.get("plan").filter(|plan| !plan.is_null()) else {
                return Ok(None);
            };
            let plan: SupportedTumblingWindowPlan = serde_json::from_value(plan.clone())
                .map_err(|source| ApiError::bad_request(source.to_string()))?;
            return Ok(Some(plan.aggregate_outputs));
        }
        Some("single_key_sum_count") => {
            let Some(plan) = payload.get("plan").filter(|plan| !plan.is_null()) else {
                return Ok(None);
            };
            let plan: SupportedViewPlan = serde_json::from_value(plan.clone())
                .map_err(|source| ApiError::bad_request(source.to_string()))?;
            return Ok(Some(supported_view_plan_aggregate_outputs(&plan)));
        }
        Some(kind) => {
            return Err(ApiError::bad_request(format!(
                "unsupported standing runtime checkpoint kind `{kind}`"
            )))
        }
        None => {}
    }
    let Some(plan) = payload.get("plan").filter(|plan| !plan.is_null()) else {
        return Ok(None);
    };
    let plan: SupportedViewPlan = serde_json::from_value(plan.clone())
        .map_err(|source| ApiError::bad_request(source.to_string()))?;
    Ok(Some(supported_view_plan_aggregate_outputs(&plan)))
}

// Legacy JSON checkpoints still use the established proof/hydration readers.
// This request-local store bounds their reads before JSON decoding and charges
// the preceding native-format probe, including warm metadata cache hits.
#[derive(Clone, Debug)]
struct LegacyQueryStore {
    inner: Arc<dyn ObjectStore>,
    policy: QueryPolicy,
    usage: Arc<Mutex<LegacyReadUsage>>,
}
#[derive(Debug)]
struct LegacyReadUsage {
    requests: usize,
    bytes: u64,
    files: BTreeSet<String>,
    exceeded: bool,
}
impl std::fmt::Display for LegacyQueryStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LegacyQueryStore({})", self.inner)
    }
}
impl LegacyQueryStore {
    fn failure() -> object_store::Error {
        object_store::Error::Generic {
            store: "legacy materialized query",
            source: Box::new(std::io::Error::other(
                "legacy materialized read exceeds query source budget",
            )),
        }
    }
    fn charge(
        &self,
        path: Option<&object_store::path::Path>,
        bytes: u64,
        request: bool,
    ) -> object_store::Result<()> {
        let mut usage = self.usage.lock().unwrap();
        if request {
            usage.requests = usage.requests.saturating_add(1);
        }
        usage.bytes = usage.bytes.saturating_add(bytes);
        if let Some(path) = path {
            if path
                .as_ref()
                .starts_with("v1/standing-runtime-output-pages/")
                || path
                    .as_ref()
                    .starts_with("v1/standing-runtime-state-payloads/")
            {
                usage.files.insert(path.to_string());
            }
        }
        if self
            .policy
            .max_object_requests
            .is_some_and(|n| usage.requests > n)
            || self
                .policy
                .max_scan_files
                .is_some_and(|n| usage.files.len() > n)
            || self.policy.max_scan_bytes.is_some_and(|n| usage.bytes > n)
            || self
                .policy
                .memory_limit_bytes
                .is_some_and(|n| usage.bytes.saturating_mul(16) > n)
        {
            usage.exceeded = true;
            return Err(Self::failure());
        }
        Ok(())
    }
}
#[async_trait::async_trait]
impl ObjectStore for LegacyQueryStore {
    async fn put_opts(
        &self,
        path: &object_store::path::Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(path, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        path: &object_store::path::Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(path, opts).await
    }
    async fn get_opts(
        &self,
        path: &object_store::path::Path,
        opts: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        use futures::StreamExt;
        self.charge(Some(path), 0, true)?;
        let result = self.inner.get_opts(path, opts).await?;
        let meta = result.meta.clone();
        let range = result.range.clone();
        let attributes = result.attributes.clone();
        let extensions = result.extensions.clone();
        // Object metadata rejects oversized bodies before consuming the stream.
        self.charge(None, range.end.saturating_sub(range.start), false)?;
        let mut stream = result.into_stream();
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            let next = bytes
                .len()
                .checked_add(chunk.len())
                .ok_or_else(Self::failure)?;
            if next as u64 > range.end.saturating_sub(range.start) {
                self.usage.lock().unwrap().exceeded = true;
                return Err(Self::failure());
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(object_store::GetResult {
            payload: object_store::GetResultPayload::Stream(Box::pin(futures::stream::once(
                async move { Ok(bytes::Bytes::from(bytes)) },
            ))),
            meta,
            range,
            attributes,
            extensions,
        })
    }
    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        if let Err(error) = self.charge(None, 0, true) {
            return Box::pin(futures::stream::once(async move { Err(error) }));
        }
        let budget = self.clone();
        let stream = self.inner.list(prefix);
        Box::pin(futures::stream::try_unfold(
            (stream, budget, 0usize),
            |(mut stream, budget, entries)| async move {
                use futures::TryStreamExt;
                // ObjectStore does not expose provider LIST page requests. Charge
                // each poll conservatively, including the terminating poll.
                budget.charge(None, 0, true)?;
                let Some(meta) = stream.try_next().await? else {
                    return Ok(None);
                };
                let entries = entries.saturating_add(1);
                if entries > DEFAULT_MAX_STANDING_RUNTIME_STATE_PAYLOAD_BYTES / 1024
                    || meta.location.as_ref().len()
                        > DEFAULT_MAX_STANDING_RUNTIME_STATE_PAYLOAD_BYTES
                {
                    budget.usage.lock().unwrap().exceeded = true;
                    return Err(Self::failure());
                }
                Ok(Some((meta, (stream, budget, entries))))
            },
        ))
    }
    fn delete_stream(
        &self,
        paths: futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>> {
        self.inner.delete_stream(paths)
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.charge(None, 0, true)?;
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        opts: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, opts).await
    }
}

// The explicit arguments preserve the already charged probe and the admitted
// output schema without reloading admission metadata or changing SQL semantics.
#[allow(clippy::too_many_arguments)]
pub(super) async fn legacy_materialized_page(
    state: &ApiState,
    view_id: &str,
    identity: &StandingProgramIdentity,
    output_id: &str,
    schema: &RelationSchema,
    mut request: SnapshotPageRequest,
    complete_sql: bool,
    policy: QueryPolicy,
    metadata_bytes: u64,
    metadata_requests: usize,
) -> Result<MaterializedViewPage, ApiError> {
    if complete_sql {
        if request.page_token.is_some() {
            return Err(ApiError::bad_request("SQL cannot use a raw page cursor"));
        }
        request.max_rows = None;
    }
    let store = Arc::new(LegacyQueryStore {
        inner: state.store.clone(),
        policy,
        usage: Arc::new(Mutex::new(LegacyReadUsage {
            requests: metadata_requests,
            bytes: metadata_bytes,
            files: BTreeSet::new(),
            exceeded: false,
        })),
    });
    store.charge(None, 0, false).map_err(|_| {
        ApiError::bad_request("legacy materialized read exceeds query source budget")
    })?;
    let mut bounded = state.clone();
    bounded.store = store.clone();
    let result = async {
        let expected_head = match &state.meta_store {
            Some(meta) => meta
                .read_standing_runtime_checkpoint(
                    &identity.tenant_id,
                    &identity.program_id,
                    view_id,
                )
                .await
                .map_err(meta_error_to_api)?,
            None => None,
        };
        let record = read_latest_standing_runtime_checkpoint(&bounded, identity, view_id)
            .await?
            .ok_or_else(|| {
                ApiError::service_unavailable(format!(
                    "MATERIALIZATION_LAG: view `{view_id}` is not fully materialized; materialized checkpoint is unavailable"
                ))
            })?;
        let published = if let Some(manifest) =
            standing_runtime_checkpoint_output_manifest(&bounded, &record, output_id).await?
        {
            if manifest.checkpoint_key != record.checkpoint_key
                || manifest.logical_epoch != record.checkpoint.logical_epoch
                || manifest.checkpoint_content_hash != record.checkpoint.state_root.content_hash
            {
                return Err(ApiError::bad_request(
                    "legacy manifest is not bound to the current checkpoint",
                ));
            }
            standing_runtime_published_output_from_manifest_page(&bounded, &manifest).await?
        } else {
            serde_json::from_value(
                standing_runtime_checkpoint_published_output(&record.checkpoint)
                    .ok_or_else(|| ApiError::conflict("materialized output is unavailable"))?,
            )
            .map_err(ApiError::bad_request)?
        };
        let aggregates =
            standing_runtime_output_aggregate_outputs_for_checkpoint(&record.checkpoint)?;
        // Reject BAG expansion before the runtime allocates repeated Arrow rows.
        let expanded = published.records().iter().try_fold(0u64, |n, row| {
            let bytes = serde_json::to_vec(row)
                .map_err(ApiError::bad_request)?
                .len() as u64;
            let copies = u64::try_from(row.weight.max(0)).map_err(ApiError::bad_request)?;
            n.checked_add(bytes.saturating_mul(copies))
                .ok_or_else(|| ApiError::bad_request("legacy expansion size overflow"))
        })?;
        let retained = store.usage.lock().unwrap().bytes.saturating_mul(16);
        if policy
            .memory_limit_bytes
            .is_some_and(|n| retained.saturating_add(expanded.saturating_mul(16)) > n)
        {
            return Err(ApiError::bad_request(
                "legacy materialized expansion exceeds query memory budget",
            ));
        }
        let page = velorix_runtime::materialized_view_runtime::materialized_delta_to_page(
            schema,
            &published,
            ScopedViewId {
                tenant_id: identity.tenant_id.clone(),
                program_id: identity.program_id.clone(),
                view_id: output_id.into(),
            },
            record.checkpoint.logical_epoch,
            request,
            aggregates.as_deref(),
        )
        .map_err(ApiError::bad_request)?;
        let retained = store.usage.lock().unwrap().bytes.saturating_mul(16);
        let arrays = page
            .batches
            .iter()
            .try_fold(0u64, |n, b| n.checked_add(b.get_array_memory_size() as u64))
            .ok_or_else(|| ApiError::bad_request("legacy query memory size overflow"))?;
        if policy
            .memory_limit_bytes
            .is_some_and(|n| retained.saturating_add(arrays) > n)
        {
            return Err(ApiError::bad_request(
                "legacy materialized expansion exceeds query memory budget",
            ));
        }
        // Preserve a single head across descriptor, state and page reads.
        if let Some(meta) = &state.meta_store {
            let current = meta
                .read_standing_runtime_checkpoint(
                    &identity.tenant_id,
                    &identity.program_id,
                    view_id,
                )
                .await
                .map_err(meta_error_to_api)?;
            if current != expected_head
                || current.as_ref().is_none_or(|p| {
                    p.checkpoint_key != record.checkpoint_key
                        || p.logical_epoch != record.checkpoint.logical_epoch
                        || p.content_hash != record.checkpoint.state_root.content_hash
                })
            {
                return Err(ApiError::conflict(
                    "materialized output head changed during legacy paging",
                ));
            }
        } else {
            let mut requests = 0;
            let current = materialized_output_pages::discover_checkpoint_head(
                &bounded,
                identity,
                view_id,
                QueryPolicy::default(),
                &mut requests,
            )
            .await?;
            if current.is_none_or(|(path, parts)| {
                path != record.checkpoint_key
                    || parts.logical_epoch != record.checkpoint.logical_epoch
                    || parts.content_hash != record.checkpoint.state_root.content_hash
            }) {
                return Err(ApiError::conflict(
                    "materialized output head changed during legacy paging",
                ));
            }
        }
        Ok(page)
    }
    .await;
    if store.usage.lock().unwrap().exceeded {
        return Err(ApiError::bad_request(
            "legacy materialized read exceeds query source budget",
        ));
    }
    result
}
