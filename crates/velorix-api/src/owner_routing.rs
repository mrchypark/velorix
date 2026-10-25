use super::*;
use axum::extract::Path;

const MAX_OWNER_ROUTE_HOPS: u8 = 8;
const EXPLICIT_OWNER_ROUTE_BUDGET_MS: u64 = 30_000;

#[derive(Clone, Debug)]
pub(super) enum ViewOwnerRoute {
    Local {
        claim: Option<StandingRuntimeOwnerClaim>,
    },
    Remote {
        endpoint: String,
        owner_epoch: u64,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DemandStepRpcRequest {
    #[serde(default)]
    pub(super) operation: DemandStepRpcOperation,
    pub(super) scope: Option<demand_materialization::DemandScope>,
    pub(super) budget: demand_materialization::DemandBudget,
    pub(super) order: demand_materialization::DemandOrder,
    pub(super) query_priority: bool,
    pub(super) pinned_spec_hash: String,
    pub(super) expected_owner_epoch: u64,
    pub(super) deadline_unix_ms: u64,
    pub(super) hop_count: u8,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(super) enum DemandStepRpcOperation {
    #[default]
    Materialize,
    PublishedBackfill {
        batch_limit: Option<usize>,
    },
    Control {
        paused: bool,
    },
}

/// Internal RPC shape includes accounting omitted from the public response.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DemandStepRpcOutcome {
    pub(super) checkpoint_epoch: Option<u64>,
    pub(super) work_units: usize,
    pub(super) status: String,
    pub(super) complete: bool,
    pub(super) processed_rows: u64,
    pub(super) read_bytes: usize,
    pub(super) remaining_rows: u64,
    pub(super) dependency_mode: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
enum DemandStepRpcResult {
    Materialization(DemandStepRpcOutcome),
    PublishedBackfill {
        response: serde_json::Value,
        pending: bool,
    },
    Control {
        status: String,
    },
}

impl From<demand_materialization::DemandOutcome> for DemandStepRpcOutcome {
    fn from(outcome: demand_materialization::DemandOutcome) -> Self {
        Self {
            checkpoint_epoch: outcome.checkpoint_epoch,
            work_units: outcome.work_units,
            status: outcome.status,
            complete: outcome.complete,
            processed_rows: outcome.processed_rows,
            read_bytes: outcome.read_bytes,
            remaining_rows: outcome.remaining_rows,
            dependency_mode: outcome.dependency_mode,
        }
    }
}

impl From<DemandStepRpcOutcome> for demand_materialization::DemandOutcome {
    fn from(outcome: DemandStepRpcOutcome) -> Self {
        Self {
            checkpoint_epoch: outcome.checkpoint_epoch,
            work_units: outcome.work_units,
            status: outcome.status,
            complete: outcome.complete,
            processed_rows: outcome.processed_rows,
            read_bytes: outcome.read_bytes,
            remaining_rows: outcome.remaining_rows,
            dependency_mode: outcome.dependency_mode,
        }
    }
}

pub(super) fn validate_owner_endpoint(endpoint: &str) -> Result<String, ApiError> {
    let Some((scheme, authority)) = endpoint.split_once("://") else {
        return Err(ApiError::bad_request("invalid owner endpoint origin"));
    };
    if !matches!(scheme, "http" | "https")
        || authority.is_empty()
        || authority.contains(['/', '\\', '?', '#', '@'])
    {
        return Err(ApiError::bad_request(
            "owner endpoint must be an HTTP(S) origin without credentials, path, query, or fragment",
        ));
    }
    let parsed = reqwest::Url::parse(endpoint).map_err(ApiError::bad_request)?;
    let origin = parsed.origin().ascii_serialization();
    if parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || origin != endpoint
    {
        return Err(ApiError::bad_request(
            "owner endpoint must be a canonical HTTP(S) origin",
        ));
    }
    Ok(origin)
}

pub(super) async fn resolve_view_owner_inner(
    state: &ApiState,
    active: &ActiveMaterializedView,
    pinned_spec_hash: &str,
    expected_owner_epoch: Option<u64>,
    emit_diagnostic: bool,
    deadline_unix_ms: u64,
) -> Result<ViewOwnerRoute, ApiError> {
    let remaining = deadline_unix_ms.saturating_sub(unix_now_ms());
    tokio::time::timeout(
        std::time::Duration::from_millis(remaining),
        resolve_view_owner_before_deadline(
            state,
            active,
            pinned_spec_hash,
            expected_owner_epoch,
            emit_diagnostic,
            deadline_unix_ms,
        ),
    )
    .await
    .map_err(|_| worker_role::pending("worker assignment deadline elapsed"))?
}

async fn resolve_view_owner_before_deadline(
    state: &ApiState,
    active: &ActiveMaterializedView,
    pinned_spec_hash: &str,
    expected_owner_epoch: Option<u64>,
    emit_diagnostic: bool,
    deadline_unix_ms: u64,
) -> Result<ViewOwnerRoute, ApiError> {
    let identity = active_standing_runtime_identity(active)
        .ok_or_else(|| ApiError::bad_request("demand requires a standing runtime"))?;
    let current = if state.meta_store.is_some() {
        view_dependencies::read_active_from_meta(state, &active.spec.view_id).await?
    } else {
        active.clone()
    };
    let current_hash = view_spec_hash(&current.spec).map_err(ApiError::bad_request)?;
    if current_hash != pinned_spec_hash || current.spec_hash != active.spec_hash {
        return Err(ApiError::conflict(
            "demand view spec changed before owner routing; retry against the current view",
        ));
    }
    worker_role::validate_compute_view(state, &current).await?;

    if !state.allows_local_materialization() {
        let owner = worker_role::assign(state, active, pinned_spec_hash, deadline_unix_ms).await?;
        if expected_owner_epoch.is_some_and(|epoch| epoch != owner.owner_epoch) {
            return Err(ApiError::conflict("demand RPC owner epoch is stale"));
        }
        let endpoint = owner
            .owner_endpoint
            .as_deref()
            .ok_or_else(|| worker_role::pending("current owner has no endpoint"))?;
        let endpoint = validate_owner_endpoint(endpoint)?;
        return Ok(ViewOwnerRoute::Remote {
            endpoint,
            owner_epoch: owner.owner_epoch,
        });
    }

    let Some(meta_store) = state.meta_store.as_ref() else {
        return Ok(ViewOwnerRoute::Local { claim: None });
    };
    if let Some(expected_epoch) = expected_owner_epoch {
        let observed = meta_store
            .read_standing_runtime_owner(
                &identity.tenant_id,
                &identity.program_id,
                &active.spec.view_id,
            )
            .await
            .map_err(meta_error_to_api)?;
        // The request can arrive at an intermediary after a claim has moved.
        // Validate the fenced epoch here, then let the acquire result below
        // either execute locally or return the current remote endpoint.
        if !observed.is_some_and(|claim| claim.owner_epoch == expected_epoch) {
            return Err(ApiError::conflict(
                "demand RPC owner epoch is stale or is not local",
            ));
        }
    }
    let request = AcquireStandingRuntimeOwnerRequest {
        tenant_id: identity.tenant_id.clone(),
        program_id: identity.program_id.clone(),
        view_id: active.spec.view_id.clone(),
        owner_id: state.owner_id.clone(),
        ttl_ms: state.standing_runtime_owner_ttl_ms,
    };
    let outcome = meta_store
        .acquire_standing_runtime_owner_with_endpoint(request, state.owner_endpoint.clone())
        .await
        .map_err(meta_error_to_api)?;
    match outcome {
        AcquireStandingRuntimeOwnerOutcome::Acquired(claim)
        | AcquireStandingRuntimeOwnerOutcome::Renewed(claim) => {
            if expected_owner_epoch.is_some_and(|epoch| epoch != claim.owner_epoch) {
                return Err(ApiError::conflict(
                    "demand RPC owner epoch is stale; refresh the owner claim",
                ));
            }
            state.set_standing_runtime_owner(identity, &active.spec.view_id, claim.clone())?;
            if emit_diagnostic {
                route_diagnostic(claim.owner_epoch, true);
            }
            Ok(ViewOwnerRoute::Local { claim: Some(claim) })
        }
        AcquireStandingRuntimeOwnerOutcome::Conflict(claim) => {
            if expected_owner_epoch.is_some_and(|epoch| epoch != claim.owner_epoch) {
                return Err(ApiError::conflict(
                    "demand RPC owner epoch is stale; refresh the owner claim",
                ));
            }
            let endpoint = claim.owner_endpoint.as_deref().ok_or_else(|| {
                ApiError::service_unavailable(
                    "current view owner has no published endpoint; refusing local preparation",
                )
            })?;
            let endpoint = validate_owner_endpoint(endpoint).map_err(|_| {
                ApiError::service_unavailable("current view owner endpoint is invalid")
            })?;
            if claim.owner_id == state.owner_id && endpoint == state.owner_endpoint {
                state.set_standing_runtime_owner(identity, &active.spec.view_id, claim.clone())?;
                if emit_diagnostic {
                    route_diagnostic(claim.owner_epoch, true);
                }
                return Ok(ViewOwnerRoute::Local { claim: Some(claim) });
            }
            if endpoint == state.owner_endpoint {
                // A restarted process must wait for its predecessor's lease, not RPC itself.
                return Err(worker_role::pending(
                    "previous process owner lease is still active",
                ));
            }
            state.set_standing_runtime_owner(identity, &active.spec.view_id, claim.clone())?;
            if emit_diagnostic {
                route_diagnostic(claim.owner_epoch, false);
            }
            Ok(ViewOwnerRoute::Remote {
                endpoint,
                owner_epoch: claim.owner_epoch,
            })
        }
    }
}

/// Resolve and execute exactly one view step. Call once per dependency view.
pub(super) async fn materialize_demand_step(
    state: &ApiState,
    active: &ActiveMaterializedView,
    request: &demand_materialization::DemandRequest,
    order: demand_materialization::DemandOrder,
    query_priority: bool,
) -> Result<demand_materialization::DemandOutcome, ApiError> {
    request.budget.validate()?;
    let wall_deadline = unix_now_ms().saturating_add(request.budget.max_duration_ms);
    let pinned_spec_hash = view_spec_hash(&active.spec).map_err(ApiError::bad_request)?;
    let mut hops: u8 = 0;
    let mut route_diagnostic_emitted = false;
    let mut refreshed_after_local_owner_loss = None;
    loop {
        let remaining_ms = wall_deadline.saturating_sub(unix_now_ms());
        if remaining_ms == 0 {
            return Ok(pending_outcome());
        }
        let route = match resolve_view_owner_inner(
            state,
            active,
            &pinned_spec_hash,
            None,
            !route_diagnostic_emitted,
            wall_deadline,
        )
        .await
        {
            Ok(route) => route,
            Err(error) if error.status == StatusCode::ACCEPTED => return Ok(pending_outcome()),
            Err(error) => return Err(error),
        };
        route_diagnostic_emitted = true;
        match route {
            ViewOwnerRoute::Local { claim, .. } => {
                let local_epoch = claim.as_ref().map(|claim| claim.owner_epoch);
                if local_epoch.is_some() && local_epoch == refreshed_after_local_owner_loss {
                    // We refreshed metadata but the same local claim still owns
                    // the view. Do not repeat preparation under a failing lease.
                    return Ok(pending_outcome());
                }
                let mut bounded = request.clone();
                bounded.budget.max_duration_ms = bounded.budget.max_duration_ms.min(remaining_ms);
                let result = materialize_with_deadline(
                    std::time::Duration::from_millis(remaining_ms),
                    demand_materialization::materialize_demand_step(
                        state,
                        active,
                        &bounded,
                        order,
                        query_priority,
                        claim,
                        None,
                    ),
                )
                .await;
                match result {
                    Ok(outcome) => return Ok(outcome),
                    Err(error) if local_epoch.is_some() && is_owner_lease_refresh_error(&error) => {
                        refreshed_after_local_owner_loss = local_epoch;
                        hops = hops.saturating_add(1);
                        continue;
                    }
                    Err(error) => return Err(error),
                }
            }
            ViewOwnerRoute::Remote {
                endpoint,
                owner_epoch,
                ..
            } => {
                if hops >= MAX_OWNER_ROUTE_HOPS {
                    return Err(ApiError::service_unavailable(
                        "demand owner routing hop limit exceeded",
                    ));
                }
                let mut forwarded_budget = request.budget.clone();
                forwarded_budget.max_duration_ms =
                    forwarded_budget.max_duration_ms.min(remaining_ms);
                let rpc = DemandStepRpcRequest {
                    operation: DemandStepRpcOperation::Materialize,
                    scope: request.scope.clone(),
                    budget: forwarded_budget,
                    order,
                    query_priority,
                    pinned_spec_hash: pinned_spec_hash.clone(),
                    expected_owner_epoch: owner_epoch,
                    deadline_unix_ms: wall_deadline,
                    hop_count: hops + 1,
                };
                let timeout_ms = remaining_ms;
                let response =
                    forward_demand_step(state, &endpoint, &active.spec.view_id, &rpc, timeout_ms)
                        .await;
                match response {
                    Ok(DemandStepRpcResult::Materialization(outcome)) => {
                        return Ok(outcome.into());
                    }
                    Ok(_) => {
                        return Err(ApiError::service_unavailable(
                            "owner returned an RPC result for the wrong demand operation",
                        ));
                    }
                    Err(error) => {
                        if !matches!(
                            error.status,
                            StatusCode::SERVICE_UNAVAILABLE | StatusCode::CONFLICT
                        ) {
                            return Err(error);
                        }
                        let fresh =
                            view_dependencies::read_active_from_meta(state, &active.spec.view_id)
                                .await?;
                        let fresh_hash =
                            view_spec_hash(&fresh.spec).map_err(ApiError::bad_request)?;
                        if fresh_hash != pinned_spec_hash || fresh.spec_hash != active.spec_hash {
                            return Err(ApiError::conflict(
                                "demand view spec changed while routing; retry against the current view",
                            ));
                        }
                        let identity =
                            active_standing_runtime_identity(active).ok_or_else(|| {
                                ApiError::bad_request("demand requires a standing runtime")
                            })?;
                        let Some(meta_store) = state.meta_store.as_ref() else {
                            return Err(error);
                        };
                        let current = meta_store
                            .read_standing_runtime_owner(
                                &identity.tenant_id,
                                &identity.program_id,
                                &active.spec.view_id,
                            )
                            .await
                            .map_err(meta_error_to_api)?;
                        let Some(current) = current else {
                            hops = hops.saturating_add(1);
                            continue;
                        };
                        let current_endpoint = current
                            .owner_endpoint
                            .as_deref()
                            .and_then(|endpoint| validate_owner_endpoint(endpoint).ok());
                        if current.owner_epoch != owner_epoch
                            || current_endpoint.as_deref() != Some(endpoint.as_str())
                        {
                            hops = hops.saturating_add(1);
                            continue;
                        }
                        if error.status == StatusCode::CONFLICT {
                            return Err(ApiError::service_unavailable(
                                "owner rejected the current owner epoch",
                            ));
                        }
                        if current.expires_at_unix_ms > wall_deadline {
                            return Ok(pending_outcome());
                        }
                        let wait_ms = current.expires_at_unix_ms.saturating_sub(unix_now_ms());
                        if wait_ms > 0 {
                            tokio::time::sleep(std::time::Duration::from_millis(
                                wait_ms.min(remaining_ms),
                            ))
                            .await;
                        }
                        hops = 0;
                    }
                }
            }
        }
    }
}

async fn forward_demand_step(
    state: &ApiState,
    endpoint: &str,
    view_id: &str,
    request: &DemandStepRpcRequest,
    timeout_ms: u64,
) -> Result<DemandStepRpcResult, ApiError> {
    let mut url = reqwest::Url::parse(endpoint).map_err(ApiError::bad_request)?;
    url.path_segments_mut()
        .map_err(|_| ApiError::bad_request("owner endpoint cannot accept a route path"))?
        .push("internal")
        .push("v1")
        .push("views")
        .push(view_id)
        .push("demand-step");
    let mut call = reqwest::Client::new()
        .post(url)
        .timeout(std::time::Duration::from_millis(timeout_ms.max(1)))
        .json(request);
    if let Some(token) = &state.api_bearer_token {
        call = call.bearer_auth(token.as_ref());
    }
    let response = call.send().await.map_err(|error| {
        ApiError::service_unavailable(format!("owner demand RPC failed: {error}"))
    })?;
    if !response.status().is_success() {
        let status = response.status();
        return Err(match status {
            StatusCode::CONFLICT => ApiError::conflict("owner rejected stale demand RPC"),
            StatusCode::BAD_REQUEST => ApiError::bad_request("owner rejected malformed route RPC"),
            StatusCode::UNAUTHORIZED => {
                ApiError::unauthorized("owner rejected route authentication")
            }
            StatusCode::FORBIDDEN | StatusCode::UNPROCESSABLE_ENTITY => ApiError {
                status,
                message: "owner rejected route authorization or request schema".into(),
                details: None,
                retryable_materialization: false,
            },
            _ => ApiError {
                status,
                message: format!("owner demand RPC returned {status}"),
                details: None,
                retryable_materialization: false,
            },
        });
    }
    response
        .json::<DemandStepRpcResult>()
        .await
        .map_err(|error| ApiError {
            status: StatusCode::BAD_GATEWAY,
            message: format!("invalid owner demand RPC response: {error}"),
            details: None,
            retryable_materialization: false,
        })
}

pub(super) async fn demand_step_rpc(
    State(state): State<ApiState>,
    Path(view_id): Path<String>,
    Json(request): Json<DemandStepRpcRequest>,
) -> Result<Response, ApiError> {
    state.require_local_materialization()?;
    if request.hop_count == 0 || request.hop_count > MAX_OWNER_ROUTE_HOPS {
        return Err(ApiError::bad_request(
            "invalid demand owner routing hop count",
        ));
    }
    request.budget.validate()?;
    if matches!(
        &request.operation,
        DemandStepRpcOperation::PublishedBackfill {
            batch_limit: Some(0)
        }
    ) {
        return Err(ApiError::bad_request(
            "backfill batch_limit must be a positive integer",
        ));
    }
    let slot = match state.try_compute_slot() {
        Ok(slot) => slot,
        Err(error) if error.status == StatusCode::ACCEPTED => {
            let active = view_dependencies::read_active_from_meta(&state, &view_id).await?;
            if view_spec_hash(&active.spec).map_err(ApiError::bad_request)?
                != request.pinned_spec_hash
            {
                return Err(ApiError::conflict("demand RPC pinned view spec is stale"));
            }
            worker_role::validate_compute_view(&state, &active).await?;
            let identity = active_standing_runtime_identity(&active)
                .ok_or_else(|| ApiError::bad_request("demand RPC requires a standing runtime"))?;
            if let Some(meta) = state.meta_store.as_ref() {
                let owner = meta
                    .read_standing_runtime_owner(
                        &identity.tenant_id,
                        &identity.program_id,
                        &view_id,
                    )
                    .await
                    .map_err(meta_error_to_api)?;
                if !owner.is_some_and(|owner| {
                    owner.owner_epoch == request.expected_owner_epoch
                        && (owner.owner_id == state.owner_id
                            || owner.owner_endpoint.as_deref()
                                == Some(state.owner_endpoint.as_str()))
                }) {
                    return Err(ApiError::conflict("demand RPC owner epoch is stale"));
                }
            }
            return Ok(rpc_result_response(
                pending_operation_result(&state, &active, &request.operation).await?,
            ));
        }
        Err(error) => return Err(error),
    };
    tokio::spawn(async move { demand_step_rpc_inner(state, view_id, request, slot).await })
        .await
        .map_err(ApiError::internal)?
}

async fn demand_step_rpc_inner(
    state: ApiState,
    view_id: String,
    request: DemandStepRpcRequest,
    slot: Arc<tokio::sync::OwnedSemaphorePermit>,
) -> Result<Response, ApiError> {
    if request.hop_count == 0 || request.hop_count > MAX_OWNER_ROUTE_HOPS {
        return Err(ApiError::bad_request(
            "invalid demand owner routing hop count",
        ));
    }
    request.budget.validate()?;
    let active = view_dependencies::read_active_from_meta(&state, &view_id).await?;
    let actual_hash = view_spec_hash(&active.spec).map_err(ApiError::bad_request)?;
    if actual_hash != request.pinned_spec_hash {
        return Err(ApiError::conflict("demand RPC pinned view spec is stale"));
    }
    let now = unix_now_ms();
    let remaining_ms = request.deadline_unix_ms.saturating_sub(now);
    if remaining_ms == 0 {
        let result = pending_operation_result(&state, &active, &request.operation).await?;
        return Ok(rpc_result_response(result));
    }
    let route = match resolve_view_owner_inner(
        &state,
        &active,
        &request.pinned_spec_hash,
        Some(request.expected_owner_epoch),
        false,
        request.deadline_unix_ms,
    )
    .await
    {
        Ok(route) => route,
        Err(error) if error.status == StatusCode::ACCEPTED => {
            return Ok(rpc_result_response(
                pending_operation_result(&state, &active, &request.operation).await?,
            ));
        }
        Err(error) => return Err(error),
    };
    let claim = match route {
        ViewOwnerRoute::Local { claim, .. } => claim,
        ViewOwnerRoute::Remote {
            endpoint,
            owner_epoch,
            ..
        } => {
            if request.hop_count >= MAX_OWNER_ROUTE_HOPS {
                return Err(ApiError::service_unavailable(
                    "demand owner routing hop limit exceeded",
                ));
            }
            let mut forwarded = request;
            forwarded.expected_owner_epoch = owner_epoch;
            forwarded.hop_count += 1;
            forwarded.budget.max_duration_ms = forwarded.budget.max_duration_ms.min(remaining_ms);
            let result =
                forward_demand_step(&state, &endpoint, &view_id, &forwarded, remaining_ms).await?;
            return Ok(rpc_result_response(result));
        }
    };
    let result = match request.operation {
        DemandStepRpcOperation::Materialize => {
            // This is only a priority signal for the received local step.
            // Never take the global query turn here: a caller may already
            // hold its own turn while waiting on this RPC.
            let _foreground = request
                .query_priority
                .then(|| state.demand_scheduler.foreground_guard());
            let mut demand = demand_materialization::DemandRequest {
                scope: request.scope,
                budget: request.budget,
            };
            demand.budget.max_duration_ms = demand.budget.max_duration_ms.min(remaining_ms);
            let outcome = materialize_with_deadline(
                std::time::Duration::from_millis(remaining_ms),
                demand_materialization::materialize_demand_step(
                    &state,
                    &active,
                    &demand,
                    request.order,
                    request.query_priority,
                    claim,
                    Some(slot.clone()),
                ),
            )
            .await?;
            DemandStepRpcResult::Materialization(outcome.into())
        }
        DemandStepRpcOperation::PublishedBackfill { batch_limit } => {
            let outcome = tokio::time::timeout(
                std::time::Duration::from_millis(remaining_ms),
                query_serving::run_view_backfill_step(&state, &view_id, batch_limit, None, None),
            )
            .await
            .map_err(|_| ApiError::service_unavailable("backfill RPC deadline elapsed"))??;
            DemandStepRpcResult::PublishedBackfill {
                response: serde_json::to_value(outcome).map_err(ApiError::internal)?,
                pending: false,
            }
        }
        DemandStepRpcOperation::Control { paused } => {
            tokio::time::timeout(
                std::time::Duration::from_millis(remaining_ms),
                demand_materialization::set_demand_control(
                    &state,
                    &active,
                    paused,
                    request.order,
                    Some(slot.clone()),
                ),
            )
            .await
            .map_err(|_| ApiError::service_unavailable("demand control RPC deadline elapsed"))??;
            DemandStepRpcResult::Control {
                status: if paused { "paused" } else { "resumed" }.into(),
            }
        }
    };
    Ok(rpc_result_response(result))
}

fn rpc_result_response(result: DemandStepRpcResult) -> Response {
    let status = match &result {
        DemandStepRpcResult::Materialization(outcome) if !outcome.complete => StatusCode::ACCEPTED,
        DemandStepRpcResult::PublishedBackfill { pending: true, .. } => StatusCode::ACCEPTED,
        DemandStepRpcResult::Control { status } if status == "owner_unavailable_pending" => {
            StatusCode::ACCEPTED
        }
        _ => StatusCode::OK,
    };
    (status, Json(result)).into_response()
}

pub(super) async fn route_published_backfill_step(
    state: &ApiState,
    active: &ActiveMaterializedView,
    batch_limit: Option<usize>,
) -> Result<Response, ApiError> {
    if batch_limit == Some(0) {
        return Err(ApiError::bad_request(
            "backfill batch_limit must be a positive integer",
        ));
    }
    let budget = demand_materialization::DemandBudget {
        batch_limit: batch_limit.unwrap_or(1).max(1),
        max_duration_ms: EXPLICIT_OWNER_ROUTE_BUDGET_MS,
        ..demand_materialization::DemandBudget::default()
    };
    let operation = DemandStepRpcOperation::PublishedBackfill { batch_limit };
    if let Some(result) = route_explicit_operation(
        state,
        active,
        operation,
        budget.clone(),
        demand_materialization::DemandOrder::Forward,
        false,
    )
    .await?
    {
        return Ok(public_operation_response(result));
    }
    let outcome = tokio::time::timeout(
        std::time::Duration::from_millis(budget.max_duration_ms),
        query_serving::run_view_backfill_step(state, &active.spec.view_id, batch_limit, None, None),
    )
    .await
    .map_err(|_| ApiError::service_unavailable("backfill step deadline elapsed"))??;
    Ok(Json(outcome).into_response())
}

pub(super) async fn route_demand_control(
    state: &ApiState,
    active: &ActiveMaterializedView,
    paused: bool,
    order: demand_materialization::DemandOrder,
) -> Result<Response, ApiError> {
    let budget = demand_materialization::DemandBudget {
        max_duration_ms: EXPLICIT_OWNER_ROUTE_BUDGET_MS,
        ..demand_materialization::DemandBudget::default()
    };
    let operation = DemandStepRpcOperation::Control { paused };
    if let Some(result) =
        route_explicit_operation(state, active, operation, budget.clone(), order, false).await?
    {
        return Ok(public_operation_response(result));
    }
    tokio::time::timeout(
        std::time::Duration::from_millis(budget.max_duration_ms),
        demand_materialization::set_demand_control(state, active, paused, order, None),
    )
    .await
    .map_err(|_| ApiError::service_unavailable("demand control deadline elapsed"))??;
    Ok(control_public_response(
        if paused { "paused" } else { "resumed" },
        false,
    ))
}

async fn route_explicit_operation(
    state: &ApiState,
    active: &ActiveMaterializedView,
    operation: DemandStepRpcOperation,
    budget: demand_materialization::DemandBudget,
    order: demand_materialization::DemandOrder,
    query_priority: bool,
) -> Result<Option<DemandStepRpcResult>, ApiError> {
    budget.validate()?;
    let identity = active_standing_runtime_identity(active)
        .ok_or_else(|| ApiError::bad_request("operation requires a standing runtime"))?;
    let pinned_spec_hash = view_spec_hash(&active.spec).map_err(ApiError::bad_request)?;
    let deadline_unix_ms = unix_now_ms().saturating_add(budget.max_duration_ms);
    let mut hops = 0u8;
    let mut route_diagnostic_emitted = false;
    loop {
        let remaining_ms = deadline_unix_ms.saturating_sub(unix_now_ms());
        if remaining_ms == 0 {
            return Ok(Some(
                pending_operation_result(state, active, &operation).await?,
            ));
        }
        let route = match resolve_view_owner_inner(
            state,
            active,
            &pinned_spec_hash,
            None,
            !route_diagnostic_emitted,
            deadline_unix_ms,
        )
        .await
        {
            Ok(route) => route,
            Err(error) if error.status == StatusCode::ACCEPTED => {
                return Ok(Some(
                    pending_operation_result(state, active, &operation).await?,
                ))
            }
            Err(error) => return Err(error),
        };
        route_diagnostic_emitted = true;
        let ViewOwnerRoute::Remote {
            endpoint,
            owner_epoch,
            ..
        } = route
        else {
            return Ok(None);
        };
        if hops >= MAX_OWNER_ROUTE_HOPS {
            return Err(ApiError::service_unavailable(
                "demand owner routing hop limit exceeded",
            ));
        }
        let mut forwarded_budget = budget.clone();
        forwarded_budget.max_duration_ms = forwarded_budget.max_duration_ms.min(remaining_ms);
        let request = DemandStepRpcRequest {
            operation: operation.clone(),
            scope: None,
            budget: forwarded_budget,
            order,
            query_priority,
            pinned_spec_hash: pinned_spec_hash.clone(),
            expected_owner_epoch: owner_epoch,
            deadline_unix_ms,
            hop_count: hops + 1,
        };
        match forward_demand_step(
            state,
            &endpoint,
            &active.spec.view_id,
            &request,
            remaining_ms,
        )
        .await
        {
            Ok(result) if rpc_result_matches(&operation, &result) => return Ok(Some(result)),
            Ok(_) => {
                return Err(ApiError::service_unavailable(
                    "owner returned an RPC result for the wrong operation",
                ));
            }
            Err(error) => {
                if !matches!(
                    error.status,
                    StatusCode::SERVICE_UNAVAILABLE | StatusCode::CONFLICT
                ) {
                    return Err(error);
                }
                let fresh =
                    view_dependencies::read_active_from_meta(state, &active.spec.view_id).await?;
                let fresh_hash = view_spec_hash(&fresh.spec).map_err(ApiError::bad_request)?;
                if fresh_hash != pinned_spec_hash || fresh.spec_hash != active.spec_hash {
                    return Err(ApiError::conflict(
                        "view spec changed while routing; retry against the current view",
                    ));
                }
                let Some(meta_store) = state.meta_store.as_ref() else {
                    return Err(ApiError::service_unavailable(
                        "owner routing requires metadata authority",
                    ));
                };
                let current = meta_store
                    .read_standing_runtime_owner(
                        &identity.tenant_id,
                        &identity.program_id,
                        &active.spec.view_id,
                    )
                    .await
                    .map_err(meta_error_to_api)?;
                let Some(current) = current else {
                    hops = hops.saturating_add(1);
                    continue;
                };
                let current_endpoint = current
                    .owner_endpoint
                    .as_deref()
                    .and_then(|endpoint| validate_owner_endpoint(endpoint).ok());
                if current.owner_epoch != owner_epoch
                    || current_endpoint.as_deref() != Some(endpoint.as_str())
                {
                    hops = hops.saturating_add(1);
                    continue;
                }
                if error.status == StatusCode::CONFLICT {
                    return Err(ApiError::service_unavailable(
                        "owner rejected the current owner epoch",
                    ));
                }
                if current.expires_at_unix_ms > deadline_unix_ms {
                    return Ok(Some(
                        pending_operation_result(state, active, &operation).await?,
                    ));
                }
                let wait_ms = current.expires_at_unix_ms.saturating_sub(unix_now_ms());
                if wait_ms > 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(wait_ms.min(remaining_ms)))
                        .await;
                }
                hops = 0;
            }
        }
    }
}

fn rpc_result_matches(operation: &DemandStepRpcOperation, result: &DemandStepRpcResult) -> bool {
    matches!(
        (operation, result),
        (
            DemandStepRpcOperation::Materialize,
            DemandStepRpcResult::Materialization(_)
        ) | (
            DemandStepRpcOperation::PublishedBackfill { .. },
            DemandStepRpcResult::PublishedBackfill { .. }
        ) | (
            DemandStepRpcOperation::Control { .. },
            DemandStepRpcResult::Control { .. }
        )
    )
}

fn is_owner_lease_refresh_error(error: &ApiError) -> bool {
    if !matches!(
        error.status,
        StatusCode::UNAUTHORIZED | StatusCode::CONFLICT
    ) {
        return false;
    }
    let message = error.message.to_ascii_lowercase();
    message.contains("owner renewal")
        || message.contains("owner lease")
        || message.contains("owner claim")
        || message.contains("owner disappeared")
        || message.contains("owner changed")
}

async fn pending_operation_result(
    state: &ApiState,
    active: &ActiveMaterializedView,
    operation: &DemandStepRpcOperation,
) -> Result<DemandStepRpcResult, ApiError> {
    match operation {
        DemandStepRpcOperation::PublishedBackfill { .. } => {
            let Json(response) = super::get_view_backfill_status(
                State(state.clone()),
                Path(active.spec.view_id.clone()),
            )
            .await?;
            Ok(DemandStepRpcResult::PublishedBackfill {
                response: serde_json::to_value(response).map_err(ApiError::internal)?,
                pending: true,
            })
        }
        DemandStepRpcOperation::Control { .. } => Ok(DemandStepRpcResult::Control {
            status: "owner_unavailable_pending".into(),
        }),
        DemandStepRpcOperation::Materialize => Ok(DemandStepRpcResult::Materialization(
            DemandStepRpcOutcome::from(pending_outcome()),
        )),
    }
}

fn public_operation_response(result: DemandStepRpcResult) -> Response {
    match result {
        DemandStepRpcResult::Materialization(outcome) => {
            rpc_result_response(DemandStepRpcResult::Materialization(outcome))
        }
        DemandStepRpcResult::PublishedBackfill { response, pending } => {
            let status = if pending {
                StatusCode::ACCEPTED
            } else {
                StatusCode::OK
            };
            (status, Json(response)).into_response()
        }
        DemandStepRpcResult::Control { status } => {
            let pending = status == "owner_unavailable_pending";
            control_public_response(&status, pending)
        }
    }
}

fn control_public_response(status: &str, pending: bool) -> Response {
    let response = serde_json::json!({
        "materialization": {"status": status, "complete": false}
    });
    let status_code = if pending {
        StatusCode::ACCEPTED
    } else {
        StatusCode::OK
    };
    (status_code, Json(response)).into_response()
}

fn pending_outcome() -> demand_materialization::DemandOutcome {
    demand_materialization::DemandOutcome {
        checkpoint_epoch: None,
        work_units: 0,
        status: "owner_unavailable_pending".into(),
        complete: false,
        processed_rows: 0,
        read_bytes: 0,
        remaining_rows: 1,
        dependency_mode: "full_closure".into(),
    }
}

async fn materialize_with_deadline<F>(
    timeout: std::time::Duration,
    future: F,
) -> Result<demand_materialization::DemandOutcome, ApiError>
where
    F: std::future::Future<Output = Result<demand_materialization::DemandOutcome, ApiError>>,
{
    match tokio::time::timeout(timeout, future).await {
        Ok(result) => result,
        Err(_) => Ok(pending_outcome()),
    }
}

pub(super) fn unix_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or_default()
}

fn route_diagnostic(owner_epoch: u64, local_owner: bool) {
    if std::env::var_os("VELORIX_DEMAND_QUERY_DIAGNOSTICS").is_some() {
        eprintln!(
            "demand_owner_route owner_epoch={owner_epoch} local_owner={}",
            u8::from(local_owner)
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owner_endpoint_accepts_only_http_origins() {
        assert_eq!(
            validate_owner_endpoint("https://api.example:8443").unwrap(),
            "https://api.example:8443"
        );
        assert_eq!(
            validate_owner_endpoint("http://api.example").unwrap(),
            "http://api.example"
        );
        for endpoint in [
            "ftp://api.example",
            "http:///missing-host",
            "http://api.example/",
            "http://api.example\\path",
            "http://user@api.example",
            "http://api.example/path",
            "http://api.example?route=elsewhere",
            "http://api.example#fragment",
        ] {
            assert!(validate_owner_endpoint(endpoint).is_err(), "{endpoint}");
        }
    }

    #[test]
    fn demand_step_rpc_round_trip_preserves_internal_accounting() {
        let outcome = demand_materialization::DemandOutcome {
            checkpoint_epoch: Some(41),
            work_units: 3,
            status: "complete".into(),
            complete: true,
            processed_rows: 27,
            read_bytes: 2048,
            remaining_rows: 0,
            dependency_mode: "full_closure".into(),
        };
        let encoded = serde_json::to_vec(&DemandStepRpcOutcome::from(outcome)).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(json["checkpoint_epoch"], 41);
        assert_eq!(json["work_units"], 3);

        let decoded: DemandStepRpcOutcome = serde_json::from_slice(&encoded).unwrap();
        let decoded: demand_materialization::DemandOutcome = decoded.into();
        assert_eq!(decoded.checkpoint_epoch, Some(41));
        assert_eq!(decoded.work_units, 3);
    }

    #[test]
    fn explicit_owner_operations_round_trip_their_parameters() {
        for operation in [
            DemandStepRpcOperation::PublishedBackfill {
                batch_limit: Some(7),
            },
            DemandStepRpcOperation::Control { paused: true },
        ] {
            let request = DemandStepRpcRequest {
                operation: operation.clone(),
                scope: None,
                budget: demand_materialization::DemandBudget::default(),
                order: demand_materialization::DemandOrder::Forward,
                query_priority: false,
                pinned_spec_hash: "spec-hash".into(),
                expected_owner_epoch: 19,
                deadline_unix_ms: 1234,
                hop_count: 1,
            };
            let encoded = serde_json::to_vec(&request).unwrap();
            let decoded: DemandStepRpcRequest = serde_json::from_slice(&encoded).unwrap();
            let result = match &operation {
                DemandStepRpcOperation::PublishedBackfill { .. } => {
                    DemandStepRpcResult::PublishedBackfill {
                        response: serde_json::json!({"status":"backfill"}),
                        pending: false,
                    }
                }
                DemandStepRpcOperation::Control { .. } => DemandStepRpcResult::Control {
                    status: "paused".into(),
                },
                DemandStepRpcOperation::Materialize => unreachable!(),
            };
            assert!(rpc_result_matches(&operation, &result));
            assert_eq!(decoded.operation, request.operation);
            assert_eq!(decoded.expected_owner_epoch, 19);
            assert_eq!(decoded.deadline_unix_ms, 1234);
        }
    }

    #[test]
    fn owner_unavailable_backfill_and_control_remain_pending() {
        let backfill = rpc_result_response(DemandStepRpcResult::PublishedBackfill {
            response: serde_json::json!({"status":"status"}),
            pending: true,
        });
        assert_eq!(backfill.status(), StatusCode::ACCEPTED);

        let control = rpc_result_response(DemandStepRpcResult::Control {
            status: "owner_unavailable_pending".into(),
        });
        assert_eq!(control.status(), StatusCode::ACCEPTED);
    }

    #[tokio::test]
    async fn elapsed_materialization_deadline_returns_incomplete_accepted_outcome() {
        let outcome = materialize_with_deadline(std::time::Duration::from_millis(1), async {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            Ok(pending_outcome())
        })
        .await
        .unwrap();

        assert!(!outcome.complete);
        assert_eq!(outcome.status, "owner_unavailable_pending");
        assert_eq!(
            rpc_result_response(DemandStepRpcResult::Materialization(outcome.into())).status(),
            StatusCode::ACCEPTED
        );
    }

    #[tokio::test]
    async fn materialization_deadline_wrapper_preserves_inner_errors() {
        let error = materialize_with_deadline(std::time::Duration::from_secs(1), async {
            Err(ApiError::bad_request("invalid demand"))
        })
        .await
        .unwrap_err();

        assert_eq!(error.status, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn only_owner_lease_failures_trigger_route_refresh() {
        assert!(is_owner_lease_refresh_error(&ApiError::conflict(
            "backfill owner renewal failed: claim expired",
        )));
        assert!(is_owner_lease_refresh_error(&ApiError::unauthorized(
            "owner claim is no longer valid",
        )));
        assert!(!is_owner_lease_refresh_error(&ApiError::conflict(
            "demand view spec changed before owner routing",
        )));
        assert!(!is_owner_lease_refresh_error(&ApiError::conflict(
            "checkpoint fencing epoch does not match",
        )));
    }
}
