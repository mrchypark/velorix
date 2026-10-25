use super::*;

/// Keeps the original fencing epoch alive while a backfill request performs
/// CPU-bound work. The independent executor cannot be starved by that request.
pub(super) struct BackfillOwnerLease {
    pub(super) owner: Option<StandingRuntimeOwnerToken>,
    state: ApiState,
    identity: StandingProgramIdentity,
    view_id: String,
    pub(super) failure: Arc<Mutex<Option<String>>>,
    failure_signal: tokio::sync::watch::Sender<Option<String>>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    worker: Option<std::thread::JoinHandle<()>>,
    stopped: Arc<std::sync::atomic::AtomicBool>,
}

impl BackfillOwnerLease {
    pub(super) async fn start(
        state: &ApiState,
        identity: &StandingProgramIdentity,
        view_id: &str,
    ) -> Result<Arc<Self>, ApiError> {
        let owner = state
            .acquire_standing_runtime_owner(identity, view_id)
            .await?;
        Self::start_with_token(state, identity, view_id, owner).await
    }

    pub(super) async fn start_acquired(
        state: &ApiState,
        identity: &StandingProgramIdentity,
        view_id: &str,
        owner: StandingRuntimeOwnerClaim,
    ) -> Result<Arc<Self>, ApiError> {
        if owner.tenant_id != identity.tenant_id
            || owner.program_id != identity.program_id
            || owner.view_id != view_id
            || owner.owner_id != state.owner_id
        {
            return Err(ApiError::conflict(
                "acquired view owner claim does not match the local view",
            ));
        }
        state.set_standing_runtime_owner(identity, view_id, owner.clone())?;
        Self::start_with_token(
            state,
            identity,
            view_id,
            Some(standing_runtime_owner_token_from_claim(&owner)),
        )
        .await
    }

    async fn start_with_token(
        state: &ApiState,
        identity: &StandingProgramIdentity,
        view_id: &str,
        owner: Option<StandingRuntimeOwnerToken>,
    ) -> Result<Arc<Self>, ApiError> {
        if state.meta_store.is_some() && owner.is_none() {
            return Err(ApiError::service_unavailable(
                "view materialization owner claim is required",
            ));
        }
        let mut lease = Self {
            owner,
            state: state.clone(),
            identity: identity.clone(),
            view_id: view_id.into(),
            failure: Arc::new(Mutex::new(None)),
            failure_signal: tokio::sync::watch::channel(None).0,
            stop: None,
            worker: None,
            stopped: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        let (Some(meta), Some(token)) = (state.meta_store.clone(), lease.owner.clone()) else {
            return Ok(Arc::new(lease));
        };
        let interval =
            std::time::Duration::from_millis((state.standing_runtime_owner_ttl_ms / 3).max(1));
        let (stop, mut stopped) = tokio::sync::oneshot::channel();
        let (ready, initialized) = tokio::sync::oneshot::channel();
        let worker_state = state.clone();
        let worker_identity = identity.clone();
        let worker_view = view_id.to_string();
        let failure = Arc::clone(&lease.failure);
        let failure_signal = lease.failure_signal.clone();
        let ttl = state.standing_runtime_owner_ttl_ms;
        let stopped_flag = Arc::clone(&lease.stopped);
        lease.worker = Some(
            std::thread::Builder::new()
                .name("velorix-backfill-owner".into())
                .spawn(move || {
                    let executor = match tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                    {
                        Ok(executor) => executor,
                        Err(error) => {
                            let _ = ready.send(Err(error.to_string()));
                            return;
                        }
                    };
                    executor.block_on(async move {
                        // Both connection creation and its background I/O driver
                        // belong to this runtime, not the caller's executor.
                        let connecting = std::time::Instant::now();
                        let connected =
                            tokio::time::timeout(interval, meta.isolated_owner_renewal_store())
                                .await;
                        let meta = match connected {
                            Ok(Ok(replacement)) => replacement.unwrap_or(meta),
                            result => {
                                let reason = match result {
                                    Err(_) => "owner renewal transport setup timed out".to_string(),
                                    Ok(Err(error)) => error.to_string(),
                                    Ok(Ok(_)) => unreachable!(),
                                };
                                let diagnostic = renewal_failure_diagnostic(
                                    &reason,
                                    connecting.elapsed(),
                                    interval,
                                    None,
                                );
                                eprintln!("owner_renewal_failure {diagnostic}");
                                let _ = ready.send(Err(diagnostic));
                                return;
                            }
                        };
                        let mut ready = Some(ready);
                        let mut last_confirmed_expiry = None;
                        loop {
                            let started = std::time::Instant::now();
                            let result: Result<u64, String> = async {
                                let claim = tokio::time::timeout(
                                    interval,
                                    meta.renew_standing_runtime_owner(token.clone(), ttl),
                                )
                                .await
                                .map_err(|_| "owner renewal timed out".to_string())?
                                .map_err(|error| format!("owner renewal RPC failed: {error}"))?;
                                if standing_runtime_owner_token_from_claim(&claim) != token {
                                    return Err("owner renewal changed fencing epoch".into());
                                }
                                let expiry = claim.expires_at_unix_ms;
                                if stopped_flag.load(std::sync::atomic::Ordering::Acquire) {
                                    return Ok(expiry);
                                }
                                refresh_resident_owner(
                                    &worker_state,
                                    &worker_identity,
                                    &worker_view,
                                    &token,
                                    claim,
                                )
                                .map_err(|error| {
                                    format!("owner renewal resident state: {error:?}")
                                })?;
                                Ok(expiry)
                            }
                            .await;
                            match result {
                                Ok(expiry) => {
                                    last_confirmed_expiry = Some(expiry);
                                    if let Some(ready) = ready.take() {
                                        let _ = ready.send(Ok(()));
                                    }
                                }
                                Err(error) => {
                                    let diagnostic = renewal_failure_diagnostic(
                                        &error,
                                        started.elapsed(),
                                        interval,
                                        last_confirmed_expiry,
                                    );
                                    eprintln!("owner_renewal_failure {diagnostic}");
                                    if let Ok(mut failure) = failure.lock() {
                                        *failure = Some(diagnostic.clone());
                                    }
                                    failure_signal.send_replace(Some(diagnostic.clone()));
                                    if let Some(ready) = ready.take() {
                                        let _ = ready.send(Err(diagnostic));
                                    }
                                    break;
                                }
                            }
                            // Keep polling the runtime while idle so the isolated
                            // channel's connection and timer tasks remain live.
                            tokio::select! {
                                _ = &mut stopped => break,
                                _ = tokio::time::sleep(interval) => {}
                            }
                        }
                    });
                })
                .map_err(ApiError::internal)?,
        );
        lease.stop = Some(stop);
        // Prove isolated transport and conditional renewal before applying input.
        initialized
            .await
            .map_err(|_| ApiError::internal("owner renewal worker stopped during setup"))?
            .map_err(ApiError::conflict)?;
        Ok(Arc::new(lease))
    }

    pub(super) fn ensure_owned(&self) -> Result<(), ApiError> {
        let failure = self
            .failure
            .lock()
            .map_err(|_| ApiError::internal("owner renewal state poisoned"))?;
        if let Some(error) = failure.as_ref() {
            remove_standing_runtime(&self.state, &self.identity, &self.view_id)?;
            return Err(ApiError::conflict(format!(
                "backfill owner renewal failed: {error}"
            )));
        }
        Ok(())
    }

    pub(super) fn while_owned<'a, T: 'a>(
        &'a self,
        future: impl std::future::Future<Output = Result<T, ApiError>> + 'a,
    ) -> impl std::future::Future<Output = Result<T, ApiError>> + 'a {
        let future = Box::pin(future);
        async move {
            let mut failure = self.failure_signal.subscribe();
            select_while_owned(&mut failure, || self.ensure_owned(), future).await
        }
    }
}

async fn select_while_owned<T>(
    failure: &mut tokio::sync::watch::Receiver<Option<String>>,
    ensure_owned: impl Fn() -> Result<(), ApiError>,
    future: impl std::future::Future<Output = Result<T, ApiError>>,
) -> Result<T, ApiError> {
    ensure_owned()?;
    let result = tokio::select! {
        biased;
        changed = failure.changed() => {
            let _ = changed;
            ensure_owned()?;
            Err(ApiError::conflict("view owner renewal stopped during preparation"))
        }
        result = future => result,
    };
    ensure_owned()?;
    result
}

fn renewal_failure_diagnostic(
    reason: &str,
    elapsed: std::time::Duration,
    deadline: std::time::Duration,
    last_confirmed_expiry: Option<u64>,
) -> String {
    let observed_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default();
    format!(
        "{}; observed_at_unix_ms={observed_at} elapsed_ms={} deadline_ms={} last_confirmed_expiry_unix_ms={last_confirmed_expiry:?}",
        reason.chars().take(512).collect::<String>(), elapsed.as_millis(), deadline.as_millis(),
    )
}

pub(super) fn refresh_resident_owner(
    state: &ApiState,
    identity: &StandingProgramIdentity,
    view_id: &str,
    token: &StandingRuntimeOwnerToken,
    claim: StandingRuntimeOwnerClaim,
) -> Result<(), ApiError> {
    let mut states = state
        .standing_runtimes
        .local_state
        .lock()
        .map_err(|_| ApiError::internal("standing runtime local state lock poisoned"))?;
    let resident = states
        .get_mut(&standing_runtime_key(identity, view_id))
        .and_then(|state| state.owner.as_mut())
        .ok_or_else(|| ApiError::conflict("resident owner disappeared during renewal"))?;
    if standing_runtime_owner_token_from_claim(resident) != *token
        || standing_runtime_owner_token_from_claim(&claim) != *token
    {
        return Err(ApiError::conflict("resident owner changed during renewal"));
    }
    if claim.expires_at_unix_ms >= resident.expires_at_unix_ms {
        *resident = claim;
    }
    Ok(())
}

impl Drop for BackfillOwnerLease {
    fn drop(&mut self) {
        self.stopped
            .store(true, std::sync::atomic::Ordering::Release);
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        // Do not block the request executor waiting for a renewal RPC.
        // The isolated worker owns its runtime until shutdown; an
        // in-flight RPC is bounded by the renewal timeout and cannot reacquire.
        self.worker.take();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    #[tokio::test]
    async fn renewal_failure_cancels_pending_prep_before_next_action() {
        let (signal, mut failure) = tokio::sync::watch::channel(None);
        let renewal_failed = Arc::new(AtomicBool::new(false));
        let future_dropped = Arc::new(AtomicBool::new(false));
        let next_action_count = Arc::new(AtomicUsize::new(0));
        let (started, started_rx) = tokio::sync::oneshot::channel();

        let failed = Arc::clone(&renewal_failed);
        let dropped = Arc::clone(&future_dropped);
        let next_action = Arc::clone(&next_action_count);
        let task = tokio::spawn(async move {
            let prep = async move {
                let _drop_flag = DropFlag(dropped);
                let _ = started.send(());
                std::future::pending::<Result<(), ApiError>>().await
            };
            let result = select_while_owned(
                &mut failure,
                || {
                    if failed.load(Ordering::Acquire) {
                        Err(ApiError::conflict("injected owner renewal failure"))
                    } else {
                        Ok(())
                    }
                },
                prep,
            )
            .await;
            if result.is_ok() {
                next_action.fetch_add(1, Ordering::AcqRel);
            }
            result
        });

        started_rx.await.expect("prep future started");
        renewal_failed.store(true, Ordering::Release);
        signal.send_replace(Some("injected renewal failure".into()));

        assert!(task.await.expect("cancellation task completed").is_err());
        assert!(future_dropped.load(Ordering::Acquire));
        assert_eq!(next_action_count.load(Ordering::Acquire), 0);

        let (signal, mut failure) = tokio::sync::watch::channel(None);
        let renewal_failed = Arc::new(AtomicBool::new(true));
        let prep_start_count = Arc::new(AtomicUsize::new(0));
        signal.send_replace(Some("injected renewal failure".into()));

        let failed = Arc::clone(&renewal_failed);
        let prep_start = Arc::clone(&prep_start_count);
        let result = select_while_owned(
            &mut failure,
            || {
                if failed.load(Ordering::Acquire) {
                    Err(ApiError::conflict("injected owner renewal failure"))
                } else {
                    Ok(())
                }
            },
            async move {
                prep_start.fetch_add(1, Ordering::AcqRel);
                Ok(())
            },
        )
        .await;

        assert!(result.is_err());
        assert_eq!(prep_start_count.load(Ordering::Acquire), 0);
    }
}
