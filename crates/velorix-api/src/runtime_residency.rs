//! Soft count bound for idle, durably checkpointed runtimes. This is not an RSS
//! bound: busy/dirty runtimes stay resident and individual states may be large.
use super::*;

pub(super) const DEFAULT_RESIDENT_LIMIT: usize = 32;

impl ApiState {
    /// Bounds idle resident runtime count, not bytes. Busy or dirty runtimes are
    /// never discarded, so the count may temporarily exceed this positive limit.
    pub fn with_standing_runtime_resident_limit(mut self, limit: usize) -> Self {
        self.standing_runtime_resident_limit = limit.max(1);
        self
    }

    /// Opportunistically prune after durable publication. Never wait for an
    /// operation or runtime lock, and never serialize state to test eligibility.
    pub(super) fn prune_standing_runtime_residents(&self) -> Result<usize, ApiError> {
        let mut keys = {
            let runtimes = self
                .standing_runtimes
                .runtimes
                .lock()
                .map_err(|_| ApiError::internal("standing runtime registry lock poisoned"))?;
            if runtimes.len() <= self.standing_runtime_resident_limit {
                return Ok(0);
            }
            runtimes.keys().cloned().collect::<Vec<_>>()
        };
        // Stable selection avoids HashMap iteration randomness without an LRU
        // framework or per-access bookkeeping.
        keys.sort_by(|a, b| {
            (&a.tenant_id, &a.program_id, &a.view_id).cmp(&(
                &b.tenant_id,
                &b.program_id,
                &b.view_id,
            ))
        });
        let mut removed = 0;
        for key in keys {
            let operation = {
                let locks = self.standing_runtimes.operation_locks.lock().map_err(|_| {
                    ApiError::internal("standing runtime operation lock registry poisoned")
                })?;
                // A runtime that has never had a coordinated operation is not
                // eligible. Keep existing lock identities for future reloads.
                let Some(operation) = locks.get(&key) else {
                    continue;
                };
                Arc::clone(operation)
            };
            let Ok(_operation_guard) = operation.try_lock() else {
                continue;
            };
            let evicted = {
                let mut runtimes =
                    self.standing_runtimes.runtimes.lock().map_err(|_| {
                        ApiError::internal("standing runtime registry lock poisoned")
                    })?;
                if runtimes.len() <= self.standing_runtime_resident_limit {
                    break;
                }
                let Some(runtime) = runtimes.get(&key) else {
                    continue;
                };
                // Lookup clones the Arc under this same registry lock. Holding
                // it across the count check and removal closes the borrow race.
                if Arc::strong_count(runtime) != 1 {
                    continue;
                }
                let Ok(runtime_guard) = runtime.try_lock() else {
                    continue;
                };
                let local = self.standing_runtimes.local_state.lock().map_err(|_| {
                    ApiError::internal("standing runtime local state lock poisoned")
                })?;
                let Some(pointer) = local
                    .get(&key)
                    .and_then(|entry| entry.committed_checkpoint.as_ref())
                else {
                    continue;
                };
                if pointer.tenant_id != key.tenant_id
                    || pointer.program_id != key.program_id
                    || pointer.view_id != key.view_id
                    || pointer.checkpoint_key.is_empty()
                    || pointer.content_hash.is_empty()
                    || pointer.logical_epoch != runtime_guard.logical_epoch()
                {
                    continue;
                }
                drop(local);
                drop(runtime_guard);
                // Preserve local owner/pointer metadata as well as operation
                // locks. Durable metadata and lease workers are untouched.
                runtimes.remove(&key)
            };
            if evicted.is_some() {
                removed += 1;
            }
            // Potentially millions of allocations: never drop under registry locks.
            drop(evicted);
        }
        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower::ServiceExt;
    use velorix_core::relation::*;

    async fn post(router: &Router, path: &str, body: Value, status: StatusCode) -> Value {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(path)
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::ACCEPT, "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let actual = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(actual, status, "{path}: {value}");
        value
    }

    async fn ingest(router: &Router, offset: u64, rows: Value) {
        post(
            router,
            "/v1/relations/records/ingest",
            json!({
                "relation_version":"v1", "stream_id":"rows", "partition_id":0,
                "start_offset_inclusive":offset, "rows":rows,
            }),
            StatusCode::CREATED,
        )
        .await;
    }

    async fn fixture() -> (
        ApiState,
        Router,
        Arc<dyn ObjectStore>,
        Arc<velorix_control::meta_admin::InMemoryMetaStore>,
    ) {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let meta = Arc::new(velorix_control::meta_admin::InMemoryMetaStore::default());
        let state =
            crate::tests::test_api_state_with_store(store.clone(), "residency-owner", false)
                .await
                .with_meta_store(meta.clone())
                .with_standing_runtime_resident_limit(2);
        let router = app(state.clone());
        let catalog = VelorixRelationCatalogV1::from_relation_schema(
            VelorixRelationSchemaV1 {
                relation_id: "records".into(),
                relation_name: "records".into(),
                relation_version: "v1".into(),
                columns: ["item", "reading", "delta"]
                    .into_iter()
                    .enumerate()
                    .map(|(ordinal, name)| RelationColumnV1 {
                        column_id: name.into(),
                        name: name.into(),
                        logical_type: VelorixLogicalTypeV1::Int64,
                        physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                        nullable: false,
                        ordinal: ordinal as u32,
                        semantic_role: if name == "delta" {
                            RelationSemanticRoleV1::Weight
                        } else {
                            RelationSemanticRoleV1::Value
                        },
                    })
                    .collect(),
                primary_key_column_ids: vec![],
                weight_column_id: "delta".into(),
                allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
                event_time_column_id: None,
            },
            CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.into(),
        )
        .unwrap();
        post(
            &router,
            "/v1/relations",
            json!({"catalog":catalog}),
            StatusCode::CREATED,
        )
        .await;
        for (view, source) in [
            ("a_stage", "records"),
            ("b_stage", "a_stage"),
            ("z_stage", "b_stage"),
        ] {
            post(
                &router,
                "/v1/views",
                json!({"view_id":view, "input_relation_id":source,
                "input_relation_version":"v1", "sql":format!("SELECT item, reading FROM {source}"),
                "response_formats":["json"]}),
                StatusCode::CREATED,
            )
            .await;
        }
        ingest(&router, 0, json!([{"item":1,"reading":10,"delta":2}])).await;
        (state, router, store, meta)
    }

    async fn pinned(
        state: &ApiState,
        view: &str,
    ) -> (ActiveMaterializedView, SharedStandingRuntime) {
        let active = state
            .view_registry()
            .unwrap()
            .read_active(view)
            .await
            .unwrap();
        let identity = active_standing_runtime_identity(&active).unwrap();
        let operation = state
            .standing_runtime_operation_lock(identity, view)
            .unwrap();
        let _guard = operation.lock().await;
        let (runtime, _) = restore_standing_runtime_resident_locked(state, &active)
            .await
            .unwrap()
            .unwrap();
        (active, runtime)
    }

    #[tokio::test]
    async fn resident_pruning_skips_busy_referenced_dirty_and_uncheckpointed() {
        let (state, _router, _store, _meta) = fixture().await;
        let mut entries = Vec::new();
        for view in ["a_stage", "b_stage", "z_stage"] {
            entries.push(pinned(&state, view).await);
        }
        assert_eq!(state.prune_standing_runtime_residents().unwrap(), 0); // external Arcs
        let operations = entries
            .iter()
            .map(|(active, _)| {
                state
                    .standing_runtime_operation_lock(
                        active_standing_runtime_identity(active).unwrap(),
                        &active.spec.view_id,
                    )
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let guards = operations
            .iter()
            .map(|operation| operation.try_lock().unwrap())
            .collect::<Vec<_>>();
        let identities = entries
            .iter()
            .map(|(active, _)| {
                (
                    active_standing_runtime_identity(active).unwrap().clone(),
                    active.spec.view_id.clone(),
                )
            })
            .collect::<Vec<_>>();
        drop(entries);
        assert_eq!(state.prune_standing_runtime_residents().unwrap(), 0); // active operations
        drop(guards);
        for (identity, view) in &identities {
            let runtime = state.standing_runtime(identity, view).unwrap().unwrap();
            let mut runtime = runtime.lock().unwrap();
            let epoch = runtime.logical_epoch() + 1;
            runtime
                .apply_changes_delta_only(
                    epoch,
                    EpochIdempotencyKey::new(format!("dirty-{view}")).unwrap(),
                    vec![],
                )
                .unwrap();
        }
        assert_eq!(state.prune_standing_runtime_residents().unwrap(), 0); // unpublished epochs
                                                                          // Reload clean durable epochs before testing the independent no-pointer
                                                                          // guard, so dirty state cannot mask an uncheckpointed eviction bug.
        for (identity, view) in &identities {
            remove_standing_runtime(&state, identity, view).unwrap();
            drop(pinned(&state, view).await);
        }
        state.standing_runtimes.local_state.lock().unwrap().clear();
        assert_eq!(state.prune_standing_runtime_residents().unwrap(), 0); // no durable pointers
        assert_eq!(state.standing_runtimes.runtimes.lock().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn eviction_append_derived_replacement_and_restart_preserve_state() {
        let (state, router, store, meta) = fixture().await;
        let (active, producer) = pinned(&state, "a_stage").await;
        let (_consumer, consumer) = pinned(&state, "b_stage").await;
        let (_last, last) = pinned(&state, "z_stage").await;
        let identity = active_standing_runtime_identity(&active).unwrap();
        let before = read_latest_standing_runtime_checkpoint(&state, identity, "a_stage")
            .await
            .unwrap()
            .unwrap();
        drop(producer);
        assert_eq!(state.prune_standing_runtime_residents().unwrap(), 1);
        assert!(state
            .standing_runtime(identity, "a_stage")
            .unwrap()
            .is_none());
        let after = read_latest_standing_runtime_checkpoint(&state, identity, "a_stage")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(before.checkpoint, after.checkpoint);
        assert!(state
            .standing_runtimes
            .local_state
            .lock()
            .unwrap()
            .get(&standing_runtime_key(identity, "a_stage"))
            .unwrap()
            .committed_checkpoint
            .is_some());
        assert!(state
            .standing_runtimes
            .operation_locks
            .lock()
            .unwrap()
            .contains_key(&standing_runtime_key(identity, "a_stage")));
        drop((consumer, last));
        ingest(&router, 1, json!([{"item":2,"reading":20,"delta":1}])).await;
        // A late replacement retracts one old duplicate, preserving the other.
        ingest(
            &router,
            2,
            json!([{"item":1,"reading":10,"delta":-1},{"item":1,"reading":11,"delta":1}]),
        )
        .await;
        let expected = vec![
            json!({"item":1,"reading":10}),
            json!({"item":1,"reading":11}),
            json!({"item":2,"reading":20}),
        ];
        let result = post(
            &router,
            "/v1/views/z_stage/query",
            json!({}),
            StatusCode::OK,
        )
        .await;
        let mut rows = result["rows"].as_array().unwrap().clone();
        rows.sort_by_key(Value::to_string);
        assert_eq!(rows, expected);
        drop(router);
        drop(state);
        let restarted = crate::tests::test_api_state_with_store(store, "residency-owner", false)
            .await
            .with_meta_store(meta)
            .with_standing_runtime_resident_limit(2);
        restarted
            .restore_standing_program_runtimes_from_active_views()
            .await
            .unwrap();
        // Startup itself must enforce residency even when replay has no work
        // and therefore publishes no checkpoints.
        assert!(restarted.standing_runtimes.runtimes.lock().unwrap().len() <= 2);
        let result = post(
            &app(restarted),
            "/v1/views/z_stage/query",
            json!({}),
            StatusCode::OK,
        )
        .await;
        let mut rows = result["rows"].as_array().unwrap().clone();
        rows.sort_by_key(Value::to_string);
        assert_eq!(rows, expected);
    }
}
