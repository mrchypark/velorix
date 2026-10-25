use super::*;
use std::sync::atomic::Ordering;
use tokio::sync::OwnedSemaphorePermit;

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceRole {
    #[default]
    All,
    Api,
    Worker,
}

impl ServiceRole {
    pub(super) fn parse(value: Option<&str>) -> anyhow::Result<Self> {
        match value.unwrap_or("all") {
            "all" => Ok(Self::All),
            "api" => Ok(Self::Api),
            "worker" => Ok(Self::Worker),
            _ => anyhow::bail!("VELORIX_SERVICE_ROLE must be all, api, or worker"),
        }
    }

    pub(super) fn compute_replica_count(self, api_replicas: u32, workers: usize) -> u32 {
        match self {
            Self::Api => 0,
            Self::All => api_replicas,
            Self::Worker => workers.max(1) as u32,
        }
    }
}

pub(super) fn parse_worker_endpoints(value: Option<&str>) -> anyhow::Result<Vec<String>> {
    let mut endpoints = Vec::new();
    for endpoint in value.into_iter().flat_map(|value| value.split(',')) {
        let endpoint = owner_routing::validate_owner_endpoint(endpoint.trim())?;
        if !endpoints.contains(&endpoint) {
            endpoints.push(endpoint);
        }
    }
    if endpoints.len() > 32 {
        anyhow::bail!("VELORIX_WORKER_ENDPOINTS supports at most 32 workers");
    }
    Ok(endpoints)
}

pub(super) fn validate_split_config(
    role: ServiceRole,
    endpoints: &[String],
    authenticated: bool,
    shared_meta: bool,
) -> anyhow::Result<()> {
    if role != ServiceRole::All && (!authenticated || !shared_meta) {
        anyhow::bail!(
            "split service roles require VELORIX_API_BEARER_TOKEN and VELORIX_META_GRPC_ENDPOINT"
        );
    }
    if role == ServiceRole::Api && endpoints.is_empty() {
        anyhow::bail!("api role requires VELORIX_WORKER_ENDPOINTS");
    }
    Ok(())
}

impl ApiState {
    pub(super) fn allows_local_materialization(&self) -> bool {
        self.service_role != ServiceRole::Api
    }

    pub(super) fn require_local_materialization(&self) -> Result<(), ApiError> {
        if self.allows_local_materialization() {
            Ok(())
        } else {
            Err(pending("api role delegates computation to workers"))
        }
    }

    pub(super) fn with_service_role(
        mut self,
        role: ServiceRole,
        endpoints: Vec<String>,
        max_jobs: usize,
    ) -> Result<Self, ApiError> {
        if max_jobs == 0 || max_jobs > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(ApiError::bad_request(
                "worker concurrency must be a positive supported semaphore capacity",
            ));
        }
        self.service_role = role;
        self.worker_endpoints = endpoints
            .into_iter()
            .map(|endpoint| owner_routing::validate_owner_endpoint(&endpoint))
            .collect::<Result<_, _>>()?;
        self.max_compute_jobs = max_jobs;
        self.compute_slots = Arc::new(tokio::sync::Semaphore::new(max_jobs));
        Ok(self)
    }

    pub(super) fn try_compute_slot(&self) -> Result<Arc<OwnedSemaphorePermit>, ApiError> {
        self.require_local_materialization()?;
        self.compute_slots
            .clone()
            .try_acquire_owned()
            .map(Arc::new)
            .map_err(|_| pending("worker execution slots are full"))
    }
}

pub(super) fn pending(message: &str) -> ApiError {
    ApiError {
        status: StatusCode::ACCEPTED,
        message: message.into(),
        details: Some(
            json!({"materialization": {"status": "owner_unavailable_pending", "complete": false}}),
        ),
        retryable_materialization: true,
    }
}

pub(super) async fn require_worker_auth(
    State(state): State<ApiState>,
    request: Request<Body>,
    next: Next,
) -> Result<Response, ApiError> {
    let expected = state
        .api_bearer_token
        .as_deref()
        .ok_or_else(|| ApiError::unauthorized("worker RPC requires bearer authentication"))?;
    require_bearer_token(&request, expected, "missing or invalid worker bearer token")?;
    state.require_local_materialization()?;
    Ok(next.run(request).await)
}

#[derive(Debug, Deserialize, Serialize)]
pub(super) struct WorkerStatus {
    pub(super) role: ServiceRole,
    pub(super) max_concurrent_jobs: usize,
    pub(super) running_jobs: usize,
    pub(super) available_slots: usize,
    pub(super) resident_runtimes: usize,
    pub(super) owned_views: usize,
    pub(super) resident_limit: usize,
    pub(super) available_runtime_capacity: usize,
}

pub(super) async fn status(State(state): State<ApiState>) -> Result<Json<WorkerStatus>, ApiError> {
    state.require_local_materialization()?;
    let available_slots = state.compute_slots.available_permits();
    let resident_runtimes = state
        .standing_runtimes
        .runtimes
        .lock()
        .map_err(|_| ApiError::internal("standing runtime registry lock poisoned"))?
        .len();
    let owned_views = state
        .standing_runtimes
        .local_state
        .lock()
        .map_err(|_| ApiError::internal("standing runtime local state lock poisoned"))?
        .values()
        .filter(|entry| {
            entry
                .owner
                .as_ref()
                .is_some_and(|owner| owner.owner_id == state.owner_id)
        })
        .count();
    Ok(Json(WorkerStatus {
        role: state.service_role,
        max_concurrent_jobs: state.max_compute_jobs,
        running_jobs: state.max_compute_jobs.saturating_sub(available_slots),
        available_slots,
        resident_runtimes,
        owned_views,
        resident_limit: state.standing_runtime_resident_limit,
        available_runtime_capacity: state
            .standing_runtime_resident_limit
            .saturating_sub(resident_runtimes),
    }))
}

fn valid_status(status: &WorkerStatus) -> bool {
    status.role != ServiceRole::Api
        && status.max_concurrent_jobs > 0
        && status.available_slots > 0
        && status.available_slots <= status.max_concurrent_jobs
        && status.running_jobs == status.max_concurrent_jobs - status.available_slots
        && status.resident_limit > 0
        && status.available_runtime_capacity
            == status
                .resident_limit
                .saturating_sub(status.resident_runtimes)
}

fn order_candidates(candidates: &mut [(String, WorkerStatus)]) {
    candidates.sort_by(|(_, a), (_, b)| {
        b.available_slots
            .cmp(&a.available_slots)
            .then(a.owned_views.cmp(&b.owned_views))
            .then(
                b.available_runtime_capacity
                    .cmp(&a.available_runtime_capacity),
            )
    });
}

pub(super) fn authenticated_request(
    state: &ApiState,
    request: reqwest::RequestBuilder,
) -> Result<reqwest::RequestBuilder, ApiError> {
    let token = state
        .api_bearer_token
        .as_ref()
        .ok_or_else(|| ApiError::unauthorized("worker routing requires bearer authentication"))?;
    Ok(request.bearer_auth(token.as_ref()))
}

pub(super) async fn candidates(state: &ApiState, deadline_unix_ms: u64) -> Vec<String> {
    use futures::StreamExt;
    let client = reqwest::Client::new();
    let endpoints = rotated_endpoints(state)
        .into_iter()
        .cloned()
        .enumerate()
        .collect::<Vec<_>>();
    let mut observed = futures::stream::iter(endpoints)
        .map(|(index, endpoint)| {
            let client = &client;
            async move {
                let remaining = deadline_unix_ms.saturating_sub(owner_routing::unix_now_ms());
                if remaining == 0 {
                    return None;
                }
                let request = client
                    .get(format!("{endpoint}/internal/v1/worker/status"))
                    .timeout(std::time::Duration::from_millis(remaining.min(250)));
                let response = authenticated_request(state, request)
                    .ok()?
                    .send()
                    .await
                    .ok()?;
                if response.status() != StatusCode::OK {
                    return None;
                }
                let status = response.json::<WorkerStatus>().await.ok()?;
                valid_status(&status).then_some((index, endpoint, status))
            }
        })
        .buffer_unordered(4)
        .filter_map(|result| async { result })
        .collect::<Vec<_>>()
        .await;
    observed.sort_by_key(|(index, _, _)| *index);
    let mut candidates = observed
        .into_iter()
        .map(|(_, endpoint, status)| (endpoint, status))
        .collect::<Vec<_>>();
    // Stable sorting retains round-robin order when all reported capacities match.
    order_candidates(&mut candidates);
    candidates
        .into_iter()
        .map(|(endpoint, _)| endpoint)
        .collect()
}

fn rotated_endpoints(state: &ApiState) -> Vec<&String> {
    let offset = state
        .worker_selection_cursor
        .fetch_add(1, Ordering::Relaxed);
    let count = state.worker_endpoints.len().max(1);
    (0..state.worker_endpoints.len())
        .map(|index| &state.worker_endpoints[(index + offset % count) % count])
        .collect()
}

#[derive(Deserialize, Serialize)]
pub(super) struct ClaimRequest {
    pub(super) view_id: String,
    pub(super) pinned_spec_hash: String,
}

pub(super) async fn claim(
    State(state): State<ApiState>,
    Json(request): Json<ClaimRequest>,
) -> Result<Json<StandingRuntimeOwnerClaim>, ApiError> {
    // Admission races must reject before acquiring a lease or touching payloads.
    let _slot = state.try_compute_slot()?;
    let active = view_dependencies::read_active_from_meta(&state, &request.view_id).await?;
    validate_compute_view(&state, &active).await?;
    if view_spec_hash(&active.spec).map_err(ApiError::bad_request)? != request.pinned_spec_hash {
        return Err(ApiError::conflict(
            "worker claim view specification changed",
        ));
    }
    let identity = active_standing_runtime_identity(&active)
        .ok_or_else(|| ApiError::bad_request("worker claim requires a standing runtime"))?;
    state
        .acquire_standing_runtime_owner(identity, &request.view_id)
        .await?;
    let owner = state
        .meta_store
        .as_ref()
        .ok_or_else(|| ApiError::service_unavailable("worker claim requires shared metadata"))?
        .read_standing_runtime_owner(&identity.tenant_id, &identity.program_id, &request.view_id)
        .await
        .map_err(meta_error_to_api)?
        .filter(|owner| owner.owner_id == state.owner_id)
        .ok_or_else(|| ApiError::conflict("worker claim changed before response"))?;
    Ok(Json(owner))
}

pub(super) async fn remote_owner(
    state: &ApiState,
    identity: &StandingProgramIdentity,
    view_id: &str,
) -> Result<Option<StandingRuntimeOwnerClaim>, ApiError> {
    let meta = state
        .meta_store
        .as_ref()
        .ok_or_else(|| ApiError::service_unavailable("worker routing requires shared metadata"))?;
    meta.read_standing_runtime_owner(&identity.tenant_id, &identity.program_id, view_id)
        .await
        .map_err(meta_error_to_api)
}

pub(super) async fn assign(
    state: &ApiState,
    active: &ActiveMaterializedView,
    spec_hash: &str,
    deadline_unix_ms: u64,
) -> Result<StandingRuntimeOwnerClaim, ApiError> {
    let identity = active_standing_runtime_identity(active)
        .ok_or_else(|| ApiError::bad_request("worker assignment requires a standing runtime"))?;
    if let Some(owner) = remote_owner(state, identity, &active.spec.view_id).await? {
        return Ok(owner);
    }
    let endpoints = candidates(state, deadline_unix_ms).await;
    let count = endpoints.len();
    for (index, endpoint) in endpoints.into_iter().enumerate() {
        let remaining = deadline_unix_ms.saturating_sub(owner_routing::unix_now_ms());
        if remaining == 0 {
            break;
        }
        let request = reqwest::Client::new()
            .post(format!("{endpoint}/internal/v1/worker/claim"))
            .timeout(std::time::Duration::from_millis(
                (remaining / (count - index) as u64).clamp(1, 1_000),
            ))
            .json(&ClaimRequest {
                view_id: active.spec.view_id.clone(),
                pinned_spec_hash: spec_hash.into(),
            });
        let Ok(response) = authenticated_request(state, request)?.send().await else {
            continue;
        };
        if response.status() == StatusCode::UNAUTHORIZED {
            return Err(ApiError::unauthorized(
                "worker rejected route authentication",
            ));
        }
        // The response is advisory too: authoritative Meta is the only ownership proof.
        if let Some(owner) = remote_owner(state, identity, &active.spec.view_id).await? {
            return Ok(owner);
        }
    }
    Err(pending("no worker has execution capacity"))
}

async fn forward<T: Serialize>(
    state: &ApiState,
    endpoint: &str,
    path: &str,
    payload: &T,
    deadline_unix_ms: u64,
) -> Result<reqwest::Response, ApiError> {
    let remaining = deadline_unix_ms.saturating_sub(owner_routing::unix_now_ms());
    if remaining == 0 {
        return Err(pending("worker request deadline elapsed"));
    }
    let request = reqwest::Client::new()
        .post(format!("{endpoint}{path}"))
        .timeout(std::time::Duration::from_millis(remaining))
        .json(payload);
    authenticated_request(state, request)?
        .send()
        .await
        .map_err(|_| pending("worker request unavailable; retry with the same input identity"))
}

pub(super) async fn forward_declaration(
    state: &ApiState,
    request: &CreateViewRequest,
    identity: &StandingProgramIdentity,
) -> Result<Response, ApiError> {
    let deadline = owner_routing::unix_now_ms().saturating_add(30_000);
    let existing = remote_owner(state, identity, &request.view_id).await?;
    let endpoints = match existing {
        Some(owner) => vec![owner
            .owner_endpoint
            .ok_or_else(|| pending("view owner has no endpoint"))?],
        None => candidates(state, deadline).await,
    };
    for endpoint in endpoints {
        let response = match forward(
            state,
            &endpoint,
            "/internal/v1/worker/declare",
            request,
            deadline,
        )
        .await
        {
            Ok(response) => response,
            Err(_) => continue,
        };
        if matches!(
            response.status(),
            StatusCode::ACCEPTED | StatusCode::SERVICE_UNAVAILABLE
        ) {
            continue;
        }
        let status = response.status();
        let bytes = response.bytes().await.map_err(ApiError::internal)?;
        return Ok((status, [(header::CONTENT_TYPE, "application/json")], bytes).into_response());
    }
    Err(pending("declaration awaits an available worker"))
}

pub(super) async fn declare(
    State(state): State<ApiState>,
    Json(request): Json<CreateViewRequest>,
) -> Result<Response, ApiError> {
    let slot = state.try_compute_slot()?;
    // HTTP disconnect must not release capacity while the admitted operation still runs.
    tokio::spawn(async move {
        let _slot = slot;
        create_view(State(state), Json(request)).await
    })
    .await
    .map_err(ApiError::internal)?
}
pub(super) async fn validate_split_ingest(
    state: &ApiState,
    snapshot: &ActiveViewReadSnapshot,
) -> Result<(), ApiError> {
    if state.service_role == ServiceRole::All {
        return Ok(());
    }
    if !state.authoritative_relation_ingest_enabled() {
        return Err(ApiError::bad_request("split roles require authoritative demand ingest; legacy eager ingest requires all role"));
    }
    for active in snapshot.views() {
        if !ingest_epoch::is_demand_managed_view(state, active).await? {
            return Err(ApiError::bad_request("split ingest does not support existing eager views; use all role or migrate their checkpoints before ingest"));
        }
    }
    Ok(())
}

pub(super) async fn validate_compute_view(
    state: &ApiState,
    active: &ActiveMaterializedView,
) -> Result<(), ApiError> {
    if state.service_role != ServiceRole::All
        && !ingest_epoch::is_demand_managed_view(state, active).await?
    {
        return Err(ApiError::bad_request("split compute supports authoritative demand views only; legacy eager views require all role"));
    }
    Ok(())
}

pub(super) struct AdmissionLease(Option<Arc<owner_lease::BackfillOwnerLease>>);

impl AdmissionLease {
    pub(super) async fn start(
        state: &ApiState,
        identity: &StandingProgramIdentity,
        view_id: &str,
        metadata_only: bool,
    ) -> Result<Self, ApiError> {
        Ok(Self(if metadata_only {
            None
        } else {
            Some(owner_lease::BackfillOwnerLease::start(state, identity, view_id).await?)
        }))
    }
    pub(super) fn ensure_owned(&self) -> Result<(), ApiError> {
        match &self.0 {
            Some(lease) => lease.ensure_owned(),
            None => Ok(()),
        }
    }
    pub(super) async fn while_owned<T, F>(&self, future: F) -> Result<T, ApiError>
    where
        F: std::future::Future<Output = Result<T, ApiError>>,
    {
        match &self.0 {
            Some(lease) => lease.while_owned(future).await,
            None => future.await,
        }
    }
}

#[cfg(test)]
#[path = "worker_role_tests.rs"]
mod tests;
