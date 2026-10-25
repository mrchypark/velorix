use super::*;

#[cfg(test)]
#[path = "view_dependency_dag_tests.rs"]
mod dag_tests;

/// One resolved input edge of a view admission: either a direct source
/// relation catalog or the published output of an active producer view.
#[derive(Clone, Debug)]
#[allow(clippy::large_enum_variant)]
pub(super) enum ResolvedAdmissionInputV1 {
    Source {
        catalog: VelorixRelationCatalogV1,
    },
    PublishedView {
        producer: ActiveMaterializedView,
        binding: PublishedRelationBindingV1,
    },
}

impl ResolvedAdmissionInputV1 {
    pub(super) fn catalog(&self) -> Result<VelorixRelationCatalogV1, ApiError> {
        match self {
            ResolvedAdmissionInputV1::Source { catalog } => Ok(catalog.clone()),
            ResolvedAdmissionInputV1::PublishedView { binding, .. } => {
                catalog_from_published_relation_binding(binding).map_err(ApiError::bad_request)
            }
        }
    }

    pub(super) fn is_published_view(&self) -> bool {
        matches!(self, ResolvedAdmissionInputV1::PublishedView { .. })
    }
}

/// Fully resolved view-on-view input for a consumer: the durable edge, the
/// producer binding, the runtime descriptor, and the admission-time baseline
/// cursor over the producer's authoritative head.
#[derive(Clone, Debug)]
pub(super) struct ConsumerViewDependencyInputV1 {
    pub(super) edge: ViewDependencyEdgeV1,
    pub(super) binding: PublishedRelationBindingV1,
    pub(super) bootstrap_cursor: CausalViewCursorV1,
}

/// A validated producer commit ready to be applied to a consumer view.
#[derive(Clone, Debug)]
pub(super) struct ValidatedProducerCommitV1 {
    pub(super) previous_epoch: u64,
    pub(super) logical_epoch: u64,
    pub(super) commit_digest: String,
    pub(super) output_delta: DeltaBatch,
}

/// Validated active-view registry state pinned for one root API operation.
/// Construct a new snapshot for every request; never retain it in ApiState.
#[derive(Clone, Debug)]
pub(super) struct ActiveViewReadSnapshot {
    active_views: Vec<ActiveMaterializedView>,
    records: BTreeMap<String, ActiveViewRecordVersionV1>,
    revision: u64,
}

impl ActiveViewReadSnapshot {
    pub(super) async fn load(state: &ApiState) -> Result<Self, ApiError> {
        let started = std::time::Instant::now();
        let snapshot = read_active_view_registry_snapshot(state).await?;
        let revision = snapshot.snapshot_revision;
        let records = snapshot
            .records
            .into_iter()
            .map(|versioned| (versioned.record.view_id.clone(), versioned))
            .collect::<BTreeMap<_, _>>();
        let registry = state.view_registry()?;
        let mut active_views = Vec::with_capacity(records.len());
        let mut spec_cache_hits = 0;
        let mut spec_cache_misses = 0;
        for versioned in records.values() {
            let (active, cache_hit) = load_active_view_record_with_cache_status(
                state,
                &registry,
                versioned.record.clone(),
            )
            .await?;
            spec_cache_hits += usize::from(cache_hit);
            spec_cache_misses += usize::from(!cache_hit);
            active_views.push(active);
        }
        demand_stage(
            "active_view_registry_snapshot",
            started.elapsed(),
            0,
            spec_cache_hits,
            spec_cache_misses,
        );
        Ok(Self {
            active_views,
            records,
            revision,
        })
    }

    /// Resolve a public API path against the fresh Meta snapshot before
    /// hydrating immutable view specs. Most path queries need exactly one
    /// spec, so they should not fetch/decode every view in the authority.
    pub(super) async fn load_api_path(
        state: &ApiState,
        normalized_path: &str,
    ) -> Result<(ActiveMaterializedView, BTreeMap<String, Value>, Self), ApiError> {
        let started = std::time::Instant::now();
        let snapshot = read_active_view_registry_snapshot(state).await?;
        let revision = snapshot.snapshot_revision;
        let records = snapshot
            .records
            .into_iter()
            .map(|versioned| (versioned.record.view_id.clone(), versioned))
            .collect::<BTreeMap<_, _>>();
        let mut matched = None;
        for versioned in records.values() {
            let Some(path) = versioned
                .record
                .api
                .as_ref()
                .and_then(|api| api.url_path.as_deref())
            else {
                continue;
            };
            let Some(parameters) =
                match_api_path_pattern(&normalize_api_path(path), normalized_path)
            else {
                continue;
            };
            if matched.is_some() {
                return Err(ApiError::conflict(format!(
                    "multiple active views match API path `/{normalized_path}`"
                )));
            }
            matched = Some((versioned, parameters));
        }
        let (versioned, parameters) = matched.ok_or_else(|| {
            ApiError::bad_request(format!("view API path `/{normalized_path}` not found"))
        })?;
        let registry = state.view_registry()?;
        let (active, cache_hit) =
            load_active_view_record_with_cache_status(state, &registry, versioned.record.clone())
                .await?;
        demand_stage(
            "active_view_api_path",
            started.elapsed(),
            0,
            usize::from(cache_hit),
            usize::from(!cache_hit),
        );
        let read_snapshot = Self {
            active_views: vec![active.clone()],
            records,
            revision,
        };
        Ok((active, parameters, read_snapshot))
    }

    pub(super) async fn load_one(
        state: &ApiState,
        view_id: &str,
    ) -> Result<(ActiveMaterializedView, Self), ApiError> {
        let started = std::time::Instant::now();
        let snapshot = read_active_view_registry_snapshot(state).await?;
        let revision = snapshot.snapshot_revision;
        let records = snapshot
            .records
            .into_iter()
            .map(|versioned| (versioned.record.view_id.clone(), versioned))
            .collect::<BTreeMap<_, _>>();
        let versioned = records
            .get(view_id)
            .ok_or_else(|| ApiError::bad_request(format!("active view `{view_id}` not found")))?;
        let registry = state.view_registry()?;
        let (active, cache_hit) =
            load_active_view_record_with_cache_status(state, &registry, versioned.record.clone())
                .await?;
        demand_stage(
            "active_view_read",
            started.elapsed(),
            0,
            usize::from(cache_hit),
            usize::from(!cache_hit),
        );
        let read_snapshot = Self {
            active_views: vec![active.clone()],
            records,
            revision,
        };
        Ok((active, read_snapshot))
    }

    /// Resolve a producer closure from one fresh, authority-scoped metadata
    /// snapshot. Mutable active records are never fetched from S3 on this path.
    pub(super) async fn load_demand_closure(
        state: &ApiState,
        root_view_id: &str,
    ) -> Result<Self, ApiError> {
        let started = std::time::Instant::now();
        let snapshot = read_active_view_registry_snapshot(state).await?;
        Self::resolve_demand_closure(state, snapshot, root_view_id, started.elapsed()).await
    }

    pub(super) async fn resolve_demand_closure(
        state: &ApiState,
        snapshot: ActiveViewRegistrySnapshotV1,
        root_view_id: &str,
        snapshot_elapsed: std::time::Duration,
    ) -> Result<Self, ApiError> {
        let started = std::time::Instant::now();
        let revision = snapshot.snapshot_revision;
        let mut records = snapshot
            .records
            .into_iter()
            .map(|versioned| (versioned.record.view_id.clone(), versioned))
            .collect::<BTreeMap<_, _>>();
        let mut pending = vec![root_view_id.to_string()];
        let mut selected = BTreeSet::new();
        while let Some(view_id) = pending.pop() {
            if !selected.insert(view_id.clone()) {
                continue;
            }
            let versioned = records.get(&view_id).ok_or_else(|| {
                ApiError::conflict(format!(
                    "active-view registry snapshot is missing dependency `{view_id}`"
                ))
            })?;
            if let Some(runtime) = &versioned.record.runtime {
                for binding in &runtime.input_bindings {
                    if let StandingInputBindingV1::PublishedView {
                        published_relation, ..
                    } = binding
                    {
                        pending.push(published_relation.producer_view_id.clone());
                    }
                }
            }
        }
        records.retain(|view_id, _| selected.contains(view_id));
        let registry = state.view_registry()?;
        let mut active_views = Vec::with_capacity(records.len());
        let mut spec_cache_hits = 0;
        let mut spec_cache_misses = 0;
        for versioned in records.values() {
            let (active, cache_hit) = load_active_view_record_with_cache_status(
                state,
                &registry,
                versioned.record.clone(),
            )
            .await?;
            spec_cache_hits += usize::from(cache_hit);
            spec_cache_misses += usize::from(!cache_hit);
            active_views.push(active);
        }
        active_views.sort_by(|left, right| left.spec.view_id.cmp(&right.spec.view_id));
        demand_stage(
            "demand_closure",
            snapshot_elapsed + started.elapsed(),
            0,
            spec_cache_hits,
            spec_cache_misses,
        );
        Ok(Self {
            active_views,
            records,
            revision,
        })
    }

    pub(super) fn views(&self) -> &[ActiveMaterializedView] {
        &self.active_views
    }

    pub(super) fn view(&self, view_id: &str) -> Option<&ActiveMaterializedView> {
        self.active_views
            .iter()
            .find(|active| active.spec.view_id == view_id)
    }

    pub(super) fn registry_snapshot(&self) -> ActiveViewRegistrySnapshotV1 {
        ActiveViewRegistrySnapshotV1 {
            snapshot_revision: self.revision,
            imported: true,
            records: self.records.values().cloned().collect(),
        }
    }
}

/// Leave the Meta gRPC JSON-envelope margin in place: a registry accepted by
/// API validation must also fit the bounded Meta read response.
pub(super) const MAX_ACTIVE_VIEW_REGISTRY_SNAPSHOT_BYTES: usize =
    velorix_control::meta_admin::MAX_META_GRPC_MESSAGE_BYTES - 256;

pub(super) fn active_view_registry_scope(state: &ApiState) -> ActiveViewRegistryScopeV1 {
    ActiveViewRegistryScopeV1 {
        authority_store_id: state.ingest_writer.authority().store_id.clone(),
        namespace: state.materialized_read_cache_namespace().to_string(),
    }
}

pub(super) async fn read_active_view_registry_snapshot(
    state: &ApiState,
) -> Result<ActiveViewRegistrySnapshotV1, ApiError> {
    let meta_store = state.meta_store.as_ref().ok_or_else(|| {
        ApiError::internal("active-view Meta registry is required; S3 fallback is disabled")
    })?;
    let snapshot = meta_store
        .read_active_view_registry_snapshot(ActiveViewRegistryReadRequestV1 {
            scope: active_view_registry_scope(state),
        })
        .await
        .map_err(meta_error_to_api)?;
    validate_active_view_registry_snapshot(&snapshot)?;
    Ok(snapshot)
}

pub(super) fn validate_active_view_registry_snapshot(
    snapshot: &ActiveViewRegistrySnapshotV1,
) -> Result<(), ApiError> {
    if !snapshot.imported {
        return Err(ApiError::conflict(
            "active-view Meta registry has not been imported; explicit migration is required",
        ));
    }
    let encoded = serde_json::to_vec(snapshot).map_err(ApiError::internal)?;
    if encoded.len() > MAX_ACTIVE_VIEW_REGISTRY_SNAPSHOT_BYTES {
        return Err(ApiError::internal(format!(
            "active-view Meta registry snapshot exceeds {} bytes",
            MAX_ACTIVE_VIEW_REGISTRY_SNAPSHOT_BYTES
        )));
    }
    let mut ids = BTreeSet::new();
    if snapshot.records.iter().any(|versioned| {
        versioned.generation == 0 || !ids.insert(versioned.record.view_id.as_str())
    }) {
        return Err(ApiError::internal(
            "active-view Meta registry snapshot contains an invalid or duplicate record",
        ));
    }
    Ok(())
}

pub(super) async fn load_active_view_record_with_cache_status(
    state: &ApiState,
    registry: &MaterializedViewRegistry,
    record: velorix_control::storage_admin::ActiveMaterializedViewRecord,
) -> Result<(ActiveMaterializedView, bool), ApiError> {
    let (bytes, cache_hit) = if let Some(cache) = &state.materialized_read_cache {
        let key = active_view_spec_cache_key(state, registry, &record.view_id, &record.spec_hash)?;
        let view_id = record.view_id.clone();
        let spec_hash = record.spec_hash.clone();
        let fetched = std::sync::atomic::AtomicBool::new(false);
        let validate = |bytes: &[u8]| {
            registry
                .active_view_from_spec_bytes(record.clone(), bytes)
                .map(|_| ())
        };
        let (bytes, cache_hit) = cache
            .read_through(
                key,
                || async {
                    fetched.store(true, std::sync::atomic::Ordering::Relaxed);
                    registry.read_spec_bytes(&view_id, &spec_hash).await
                },
                validate,
            )
            .await
            .map_err(materialized_view_registry_error_to_api)
            .map(|bytes| (bytes, !fetched.load(std::sync::atomic::Ordering::Relaxed)))?;
        (bytes, cache_hit)
    } else {
        let bytes = registry
            .read_spec_bytes(&record.view_id, &record.spec_hash)
            .await
            .map_err(materialized_view_registry_error_to_api)?;
        (bytes, false)
    };
    let active = registry
        .active_view_from_spec_bytes(record, &bytes)
        .map_err(materialized_view_registry_error_to_api)?;
    Ok((active, cache_hit))
}

#[cfg(test)]
pub(super) async fn read_active_cached(
    state: &ApiState,
    view_id: &str,
) -> Result<ActiveMaterializedView, ApiError> {
    ActiveViewReadSnapshot::load_one(state, view_id)
        .await
        .map(|(active, _)| active)
}

pub(super) async fn read_active_from_meta(
    state: &ApiState,
    view_id: &str,
) -> Result<ActiveMaterializedView, ApiError> {
    ActiveViewReadSnapshot::load_one(state, view_id)
        .await
        .map(|(active, _)| active)
}

pub(super) async fn list_active_from_meta(
    state: &ApiState,
) -> Result<Vec<ActiveMaterializedView>, ApiError> {
    Ok(ActiveViewReadSnapshot::load(state).await?.active_views)
}

pub(super) async fn compare_exchange_active_view_record(
    state: &ApiState,
    view_id: &str,
    expected_generation: u64,
    record: velorix_control::storage_admin::ActiveMaterializedViewRecord,
    owner: Option<velorix_control::meta_admin::StandingRuntimeOwnerToken>,
) -> Result<(), ApiError> {
    let meta_store = state.meta_store.as_ref().ok_or_else(|| {
        ApiError::internal("active-view Meta registry is required; S3 fallback is disabled")
    })?;
    if record.view_id != view_id {
        return Err(ApiError::bad_request(
            "active-view compare-and-set record identity mismatch",
        ));
    }
    match meta_store
        .compare_exchange_active_view_record(CompareExchangeActiveViewRecordRequestV1 {
            scope: active_view_registry_scope(state),
            view_id: view_id.to_string(),
            expected_generation,
            record,
            owner,
        })
        .await
        .map_err(meta_error_to_api)?
    {
        CompareExchangeActiveViewRecordOutcomeV1::Updated(_)
        | CompareExchangeActiveViewRecordOutcomeV1::Duplicate(_) => Ok(()),
        CompareExchangeActiveViewRecordOutcomeV1::Conflict => Err(ApiError::conflict(
            "active-view record changed concurrently; retry the operation",
        )),
    }
}

pub(super) async fn update_active_view_lifecycle_admin(
    state: &ApiState,
    view_id: &str,
    expected_spec_hash: &str,
    lifecycle: MaterializedViewLifecycleStatus,
) -> Result<(), ApiError> {
    let snapshot = read_active_view_registry_snapshot(state).await?;
    let versioned = snapshot
        .records
        .iter()
        .find(|versioned| versioned.record.view_id == view_id)
        .ok_or_else(|| ApiError::conflict("active view disappeared from Meta registry"))?;
    if versioned.record.spec_hash != expected_spec_hash {
        return Err(ApiError::conflict(
            "active view changed before lifecycle update; retry the operation",
        ));
    }
    let mut record = versioned.record.clone();
    record.lifecycle = Some(lifecycle);
    let owner = if let Some(runtime) = versioned.record.runtime.as_ref() {
        Some(
            state
                .acquire_standing_runtime_owner(&runtime.standing_program_identity, view_id)
                .await?
                .ok_or_else(|| {
                    ApiError::service_unavailable(
                        "standing-runtime lifecycle update requires a metadata owner fence",
                    )
                })?,
        )
    } else {
        None
    };
    compare_exchange_active_view_record(state, view_id, versioned.generation, record, owner).await
}

pub(super) async fn read_active_view_record_version(
    state: &ApiState,
    view_id: &str,
) -> Result<ActiveViewRecordVersionV1, ApiError> {
    read_active_view_registry_snapshot(state)
        .await?
        .records
        .into_iter()
        .find(|versioned| versioned.record.view_id == view_id)
        .ok_or_else(|| ApiError::conflict("active view disappeared from Meta registry"))
}

pub(super) fn demand_stage(
    stage: &'static str,
    elapsed: std::time::Duration,
    active_pointer_reads: usize,
    spec_cache_hits: usize,
    spec_cache_misses: usize,
) {
    if std::env::var_os("VELORIX_DEMAND_QUERY_DIAGNOSTICS").is_some() {
        eprintln!(
            "demand_query_stage stage={stage} elapsed_us={} active_pointer_reads={active_pointer_reads} spec_cache_hits={spec_cache_hits} spec_cache_misses={spec_cache_misses}",
            elapsed.as_micros()
        );
    }
}

pub(super) fn demand_cache_stage(
    stage: &'static str,
    elapsed: std::time::Duration,
    cache_hits: usize,
    cache_misses: usize,
    canonical_validations: usize,
) {
    if std::env::var_os("VELORIX_DEMAND_QUERY_DIAGNOSTICS").is_some() {
        eprintln!(
            "demand_query_stage stage={stage} elapsed_us={} checkpoint_cache_hits={cache_hits} checkpoint_cache_misses={cache_misses} checkpoint_canonical_validations={canonical_validations}",
            elapsed.as_micros()
        );
    }
}

fn active_view_spec_cache_key(
    state: &ApiState,
    registry: &MaterializedViewRegistry,
    view_id: &str,
    spec_hash: &str,
) -> Result<materialized_read_cache::MaterializedReadCacheKey, ApiError> {
    let object_key = registry
        .object_key(view_id, spec_hash)
        .map_err(materialized_view_registry_error_to_api)?;
    Ok(materialized_read_cache::MaterializedReadCacheKey {
        store_id: state.materialized_read_cache_store_id().to_string(),
        namespace: state.materialized_read_cache_namespace().to_string(),
        tenant_id: String::new(),
        program_id: String::new(),
        output_id: view_id.to_string(),
        object_path: object_key.to_string(),
        content_hash: spec_hash.to_string(),
        codec: "velorix-standing-view-spec-json-v1".to_string(),
        checkpoint_epoch: 0,
        root_hash: String::new(),
    })
}

async fn try_read_source_catalog(
    state: &ApiState,
    relation_id: &str,
    relation_version: &str,
) -> Result<Option<VelorixRelationCatalogV1>, ApiError> {
    if let Some(meta_store) = &state.meta_store {
        // The meta store is the authoritative source-catalog namespace during
        // admission. A NotFound here is a definitive missing dependency: the
        // object-store registry may hold a stale or deleted catalog that must
        // not resurrect a source that the authoritative namespace removed.
        // Recovery from a not-yet-populated meta store is handled by the
        // explicit recovery read path, not by admission.
        match meta_store
            .read_relation_catalog(relation_id, relation_version)
            .await
        {
            Ok(catalog) => Ok(Some(catalog)),
            Err(MetaStoreError::RelationCatalogNotFound { .. }) => Ok(None),
            Err(error) => Err(meta_error_to_api(error)),
        }
    } else {
        match state
            .relation_registry()?
            .read(relation_id, relation_version)
            .await
        {
            Ok(catalog) => Ok(Some(catalog)),
            Err(velorix_control::storage_admin::RelationCatalogRegistryError::ObjectStore(
                object_store::Error::NotFound { .. },
            )) => Ok(None),
            Err(error) => Err(ApiError::bad_request(error)),
        }
    }
}

/// Resolves the view request's input relation references against physical
/// source relations first and then against published view outputs. An input
/// that matches both is ambiguous and rejected; an input that matches neither
/// is a missing dependency and rejected.
pub(super) async fn resolve_standing_inputs_for_view_request(
    state: &ApiState,
    request: &CreateViewRequest,
) -> Result<Vec<ResolvedAdmissionInputV1>, ApiError> {
    let has_single_ref = !request.input_relation_id.trim().is_empty()
        || !request.input_relation_version.trim().is_empty();
    let mut refs = Vec::new();
    if !request.input_relation_refs.is_empty() {
        if !request.input_relations.is_empty() || has_single_ref {
            return Err(ApiError::bad_request(
                "view must use only one input relation selector: input_relation_id/input_relation_version, input_relation_refs, or input_relations",
            ));
        }
        for input in &request.input_relation_refs {
            if input.relation_id.trim().is_empty() || input.relation_version.trim().is_empty() {
                return Err(ApiError::bad_request(
                    "input_relation_refs must include non-empty relation_id and relation_version",
                ));
            }
            refs.push((input.relation_id.clone(), input.relation_version.clone()));
        }
    } else if !request.input_relations.is_empty() {
        if has_single_ref {
            return Err(ApiError::bad_request(
                "view must use only one input relation selector: input_relation_id/input_relation_version, input_relation_refs, or input_relations",
            ));
        }
        for schema in &request.input_relations {
            refs.push((schema.relation_id.clone(), schema.relation_version.clone()));
        }
    } else if request.input_relation_id.trim().is_empty()
        || request.input_relation_version.trim().is_empty()
    {
        return Err(ApiError::bad_request(
            "view requires either input_relation_id/input_relation_version or input_relations",
        ));
    } else {
        refs.push((
            request.input_relation_id.clone(),
            request.input_relation_version.clone(),
        ));
    }
    let mut seen = BTreeSet::new();
    for (relation_id, relation_version) in &refs {
        if !seen.insert((relation_id.as_str(), relation_version.as_str())) {
            return Err(ApiError::bad_request(format!(
                "duplicate input relation reference `{relation_id}` version `{relation_version}`"
            )));
        }
    }

    let active_views = list_active_from_meta(state).await?;
    let mut resolved = Vec::with_capacity(refs.len());
    for (relation_id, relation_version) in refs {
        let source = try_read_source_catalog(state, &relation_id, &relation_version).await?;
        let published = active_views
            .iter()
            // A declared producer exposes its schema before it has checkpoint coverage.
            .filter(|active| {
                standing_runtime_can_accept_incremental_ingest(active)
                    || (state.authoritative_relation_ingest_enabled()
                        && view_has_backfill_required_lag(active))
            })
            .filter_map(|active| {
                let runtime = active.runtime.as_ref()?;
                runtime.published_relations.iter().find_map(|binding| {
                    if binding.relation.relation_id == relation_id
                        && binding.relation.relation_version == relation_version
                    {
                        Some((active.clone(), binding.clone()))
                    } else {
                        None
                    }
                })
            })
            .collect::<Vec<_>>();
        match (source, published.as_slice()) {
            (Some(source), []) => {
                resolved.push(ResolvedAdmissionInputV1::Source { catalog: source });
            }
            (Some(_), _) => {
                return Err(ApiError::bad_request(format!(
                    "input relation `{relation_id}` version `{relation_version}` is ambiguous: it resolves to both a registered source relation and published view outputs"
                )));
            }
            (None, [single]) => {
                let (producer, binding) = single.clone();
                validate_published_relation_binding_v1(&binding).map_err(ApiError::bad_request)?;
                resolved.push(ResolvedAdmissionInputV1::PublishedView { producer, binding });
            }
            (None, matches) if matches.len() > 1 => {
                let producers = matches
                    .iter()
                    .map(|(active, _)| active.spec.view_id.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(ApiError::bad_request(format!(
                    "input relation `{relation_id}` version `{relation_version}` is ambiguous: {} active views publish it (`{producers}`); a view-on-view input must resolve to exactly one producer",
                    matches.len()
                )));
            }
            (None, []) => {
                return Err(ApiError::bad_request(format!(
                    "input relation `{relation_id}` version `{relation_version}` is not a registered relation and is not the published output of any active view"
                )));
            }
            (None, matches) => {
                return Err(ApiError::bad_request(format!(
                    "input relation `{relation_id}` version `{relation_version}` is ambiguous: {} active views publish it; a view-on-view input must resolve to exactly one producer",
                    matches.len()
                )));
            }
        }
    }
    Ok(resolved)
}

/// Published inputs may form a multi-producer DAG. Mixing physical-source
/// replay with published commit replay remains a separate admission boundary.
pub(super) fn validate_resolved_input_scope(
    inputs: &[ResolvedAdmissionInputV1],
) -> Result<(), ApiError> {
    if inputs.is_empty() {
        return Err(ApiError::bad_request("view has no input relation"));
    }
    let published_count = inputs
        .iter()
        .filter(|input| input.is_published_view())
        .count();
    if published_count > 0 && published_count != inputs.len() {
        return Err(ApiError::bad_request(
            "view-on-view dependencies cannot be mixed with direct source inputs in the same view",
        ));
    }
    Ok(())
}

/// Captures the producer's authoritative head as a baseline cursor for a
/// consumer edge. Requires the producer to have at least one published
/// checkpoint with an authoritative output commit.
pub(super) async fn capture_producer_head_cursor(
    state: &ApiState,
    tenant_id: &str,
    producer: &ActiveMaterializedView,
    binding: &PublishedRelationBindingV1,
    edge_id: &str,
) -> Result<CausalViewCursorV1, ApiError> {
    if !view_query_availability(&producer.lifecycle) && !view_has_backfill_required_lag(producer) {
        return Err(ApiError::bad_request(
            "producer lifecycle does not permit capturing a consumer baseline",
        ));
    }
    let _capture_timer = checkpoint_publication::CursorPhaseTimer::new(
        &binding.producer_view_id,
        0,
        "capture_producer_head_total",
    );
    let Some(identity) = active_standing_runtime_identity(producer) else {
        return Err(ApiError::bad_request(format!(
            "producer view `{}` has no standing runtime identity",
            binding.producer_view_id
        )));
    };
    let record = checkpoint_publication::read_latest_standing_runtime_checkpoint_header(state, identity, &binding.producer_view_id).await?
        .ok_or_else(|| {
            ApiError::bad_request(format!(
                "producer view `{}` has no authoritative published checkpoint; a view-on-view consumer requires the producer to have materialized output first",
                binding.producer_view_id
            ))
        })?;
    // Demand completion is checkpoint evidence; declaration lifecycle may still be Deploying.
    if let Some(progress) = &record.demand_materialization {
        progress.validate_checkpoint(&record.checkpoint)?;
        if progress.spec_hash != producer.spec_hash
            || progress.plan_hash != binding.producer_plan_hash
        {
            return Err(ApiError::bad_request(
                "producer demand checkpoint identity mismatch",
            ));
        }
    }
    let demand_complete = record
        .demand_materialization
        .as_ref()
        .map(|progress| progress.full_complete);
    if demand_complete == Some(false)
        || (!view_query_availability(&producer.lifecycle) && demand_complete != Some(true))
        || record
            .published_input_bootstrap
            .as_ref()
            .is_some_and(|progress| !progress.is_complete())
    {
        return Err(ApiError::bad_request(
            "producer baseline is incomplete; complete dependency materialization before capturing a consumer baseline",
        ));
    }
    let commit_digest =
        checkpoint_publication::producer_cursor_digest_from_header(&record, binding)?;
    let cursor = CausalViewCursorV1 {
        input_edge: edge_id.to_string(),
        producer_tenant_id: tenant_id.to_string(),
        producer_program_id: identity.program_id.clone(),
        producer_view_id: binding.producer_view_id.clone(),
        producer_generation: binding.producer_view_generation,
        output_stream: binding.output_stream_id.clone(),
        output_epoch: record.checkpoint.logical_epoch,
        commit_digest,
    };
    cursor
        .validate()
        .map_err(|_| ApiError::bad_request("captured producer head cursor is invalid"))?;
    // The header and commit are bound here; full state/page validation belongs
    // to explicit backfill and must precede applying any snapshot page.
    Ok(cursor)
}

/// Reads and fully validates the single authoritative output commit of a
/// producer checkpoint: identity fields, delta content hash, and the
/// recomputed producer commit digest.
pub(super) async fn authoritative_producer_commit_for_checkpoint(
    state: &ApiState,
    binding: &PublishedRelationBindingV1,
    record: &StandingRuntimeCheckpointRecord,
) -> Result<StandingRuntimeProducerCommitV1, ApiError> {
    record
        .checkpoint
        .validate_identity(&record.checkpoint.identity)
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    authoritative_producer_commit_for_header(state, binding, record).await
}

async fn authoritative_producer_commit_for_header(
    state: &ApiState,
    binding: &PublishedRelationBindingV1,
    record: &StandingRuntimeCheckpointRecord,
) -> Result<StandingRuntimeProducerCommitV1, ApiError> {
    let commit_refs = record
        .checkpoint
        .output_manifest_refs
        .iter()
        .filter(|output_ref| output_ref.starts_with(STANDING_RUNTIME_OUTPUT_COMMIT_REF_PREFIX))
        .collect::<Vec<_>>();
    if commit_refs.len() != 1 {
        return Err(ApiError::bad_request(format!(
            "producer view `{}` epoch {} has {} authoritative output commits; exactly one is required",
            binding.producer_view_id,
            record.checkpoint.logical_epoch,
            commit_refs.len()
        )));
    }
    let (_key, delta_record) =
        read_standing_runtime_output_delta_record(state, commit_refs[0], &binding.producer_view_id)
            .await?;
    let commit = delta_record.producer_commit.as_ref().ok_or_else(|| {
        ApiError::bad_request(format!(
            "producer view `{}` epoch {} has no authoritative producer commit",
            binding.producer_view_id, record.checkpoint.logical_epoch
        ))
    })?;
    if delta_record.logical_epoch != record.checkpoint.logical_epoch
        || commit.checkpoint_key != record.checkpoint_key
        || commit.checkpoint_content_hash != record.checkpoint.state_root.content_hash
        || delta_record.schema_fingerprint != binding.relation.schema_fingerprint
        || commit.producer_view_generation != binding.producer_view_generation
        || commit.producer_plan_hash != binding.producer_plan_hash
        || commit.output_stream_id != binding.output_stream_id
        || commit.output_schema_hash != binding.output_schema_hash
        || commit.key_descriptor_hash != binding.key_descriptor_hash
        || commit.delta_codec_identity != binding.delta_codec_identity
        || commit.frontier_kind != binding.frontier_kind
    {
        return Err(ApiError::bad_request(format!(
            "producer view `{}` epoch {} commit does not match the admitted published binding",
            binding.producer_view_id, record.checkpoint.logical_epoch
        )));
    }
    let expected_delta_bytes = serde_json::to_vec(&delta_record.output_delta)
        .map_err(|source| ApiError::internal(source.to_string()))?;
    let expected_delta_hash = stable_bytes_hash(&expected_delta_bytes);
    if delta_record.delta_content_hash != expected_delta_hash {
        return Err(ApiError::bad_request(format!(
            "producer view `{}` epoch {} output delta content hash mismatch",
            binding.producer_view_id, record.checkpoint.logical_epoch
        )));
    }
    let expected_commit_digest =
        recompute_producer_commit_digest(&record.checkpoint, binding, &delta_record, commit)?;
    if commit.producer_commit_digest != expected_commit_digest {
        return Err(ApiError::bad_request(format!(
            "producer view `{}` epoch {} producer commit digest mismatch",
            binding.producer_view_id, record.checkpoint.logical_epoch
        )));
    }
    Ok(commit.clone())
}

/// Recomputes the producer commit digest over the canonical digest input to
/// detect tampering with any signed field.
fn recompute_producer_commit_digest(
    checkpoint: &RuntimeCheckpoint,
    binding: &PublishedRelationBindingV1,
    delta_record: &StandingRuntimeOutputDeltaRecord,
    commit: &StandingRuntimeProducerCommitV1,
) -> Result<String, ApiError> {
    let causal_cut_digest = checkpoint
        .causal_cut
        .as_ref()
        .ok_or_else(|| ApiError::bad_request("producer commit requires a causal cut"))?
        .stable_digest()
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    #[derive(Serialize)]
    struct DigestInput<'a> {
        schema_version: u32,
        tenant_id: &'a str,
        program_id: &'a str,
        view_id: &'a str,
        logical_epoch: u64,
        producer_view_generation: u64,
        producer_plan_hash: &'a str,
        output_stream_id: &'a str,
        output_schema_hash: &'a str,
        key_descriptor_hash: &'a str,
        delta_codec_identity: &'a str,
        frontier_kind: &'a str,
        schema_fingerprint: &'a str,
        delta_content_hash: &'a str,
        checkpoint_key: &'a str,
        checkpoint_content_hash: &'a str,
        causal_cut_digest: &'a str,
    }
    let digest_input = DigestInput {
        schema_version: 1,
        tenant_id: &checkpoint.identity.tenant_id,
        program_id: &checkpoint.identity.program_id,
        view_id: &binding.producer_view_id,
        logical_epoch: checkpoint.logical_epoch,
        producer_view_generation: binding.producer_view_generation,
        producer_plan_hash: &binding.producer_plan_hash,
        output_stream_id: &binding.output_stream_id,
        output_schema_hash: &binding.output_schema_hash,
        key_descriptor_hash: &binding.key_descriptor_hash,
        delta_codec_identity: &binding.delta_codec_identity,
        frontier_kind: &binding.frontier_kind,
        schema_fingerprint: &delta_record.schema_fingerprint,
        delta_content_hash: &delta_record.delta_content_hash,
        checkpoint_key: &commit.checkpoint_key,
        checkpoint_content_hash: &commit.checkpoint_content_hash,
        causal_cut_digest: &causal_cut_digest,
    };
    let bytes = serde_json::to_vec(&digest_input)
        .map_err(|source| ApiError::internal(source.to_string()))?;
    Ok(stable_bytes_hash(&bytes))
}

/// Validates the candidate edges (existing active views plus the new consumer
/// edge) form an acyclic graph and returns the producer-first topological
/// order of view ids.
pub(super) fn validate_view_dependency_graph_with_candidate(
    existing_edges: &[ViewDependencyEdgeV1],
    candidate: &ViewDependencyEdgeV1,
) -> Result<Vec<String>, ApiError> {
    let mut edges = existing_edges.to_vec();
    if !edges.iter().any(|edge| {
        edge.edge_id == candidate.edge_id && edge.consumer_view_id == candidate.consumer_view_id
    }) {
        edges.push(candidate.clone());
    }
    validate_view_dependency_graph(&edges).map_err(|error| {
        ApiError::bad_request(format!(
            "view-on-view dependency graph rejected at admission: {}",
            error
        ))
    })
}

/// Collects the durable dependency edges of all active views.
pub(super) async fn view_dependency_edges_from_active_views(
    state: &ApiState,
) -> Result<Vec<ViewDependencyEdgeV1>, ApiError> {
    let snapshot = ActiveViewReadSnapshot::load(state).await?;
    view_dependency_edges_from_snapshot(snapshot.views())
}

pub(super) fn view_dependency_edges_from_snapshot(
    active_views: &[ActiveMaterializedView],
) -> Result<Vec<ViewDependencyEdgeV1>, ApiError> {
    let mut edges = Vec::new();
    for active in active_views {
        let Some(runtime) = active.runtime.as_ref() else {
            continue;
        };
        for binding in &runtime.input_bindings {
            if let StandingInputBindingV1::PublishedView {
                edge_id,
                producer_tenant_id,
                producer_program_id,
                published_relation,
                bootstrap_cursor,
                ..
            } = binding
            {
                let edge = view_dependency_edge_from_binding(
                    producer_tenant_id,
                    &runtime.standing_program_identity.program_id,
                    &active.spec.view_id,
                    runtime.standing_program_identity.view_ids.len() as u64,
                    &published_relation.relation.relation_id,
                    &published_relation.relation.relation_version,
                    producer_program_id,
                    published_relation,
                )
                .map_err(ApiError::bad_request)?;
                if edge.edge_id != *edge_id {
                    return Err(ApiError::bad_request(format!(
                        "active view `{}` has a stale view-on-view edge",
                        active.spec.view_id
                    )));
                }
                let _ = bootstrap_cursor;
                edges.push(edge);
            }
        }
    }
    Ok(edges)
}

/// Builds the `StandingInputBindingV1` set for a view with published-view
/// inputs, capturing each producer's authoritative head cursor.
/// Declaration may precede producer materialization. Absence is explicit;
/// corrupt/mismatched published checkpoints still fail closed.
pub(super) async fn capture_optional_producer_bootstrap_cursor(
    state: &ApiState,
    tenant_id: &str,
    producer: &ActiveMaterializedView,
    binding: &PublishedRelationBindingV1,
    edge_id: &str,
) -> Result<Option<CausalViewCursorV1>, ApiError> {
    let identity = active_standing_runtime_identity(producer)
        .ok_or_else(|| ApiError::bad_request("producer runtime identity missing"))?;
    let record = checkpoint_publication::read_latest_standing_runtime_checkpoint_header(
        state,
        identity,
        &binding.producer_view_id,
    )
    .await?;
    let Some(record) = record else {
        return Ok(None);
    };
    if record
        .published_input_bootstrap
        .as_ref()
        .is_some_and(|p| !p.is_complete())
        || record
            .demand_materialization
            .as_ref()
            .is_some_and(|p| !p.full_complete)
    {
        return Ok(None);
    }
    capture_producer_head_cursor(state, tenant_id, producer, binding, edge_id)
        .await
        .map(Some)
}

pub(super) async fn input_bindings_for_resolved_inputs(
    state: &ApiState,
    tenant_id: &str,
    inputs: &[ResolvedAdmissionInputV1],
    capture_bootstrap: bool,
) -> Result<Vec<StandingInputBindingV1>, ApiError> {
    let mut bindings = Vec::with_capacity(inputs.len());
    for input in inputs {
        match input {
            ResolvedAdmissionInputV1::Source { catalog } => {
                bindings.push(StandingInputBindingV1::Source {
                    relation: catalog_input_relation_schema(catalog)
                        .map_err(ApiError::bad_request)?,
                    relation_generation: INGEST_SOURCE_IDENTITY_GENERATION_V1,
                });
            }
            ResolvedAdmissionInputV1::PublishedView {
                producer, binding, ..
            } => {
                let Some(producer_identity) = active_standing_runtime_identity(producer) else {
                    return Err(ApiError::bad_request(format!(
                        "producer view `{}` has no standing runtime identity",
                        binding.producer_view_id
                    )));
                };
                let edge_id =
                    view_dependency_edge_id(tenant_id, &producer_identity.program_id, binding)
                        .map_err(ApiError::bad_request)?;
                let bootstrap_cursor = if capture_bootstrap {
                    capture_optional_producer_bootstrap_cursor(
                        state, tenant_id, producer, binding, &edge_id,
                    )
                    .await?
                } else {
                    None
                };
                bindings.push(StandingInputBindingV1::PublishedView {
                    edge_id,
                    producer_tenant_id: tenant_id.to_string(),
                    producer_program_id: producer_identity.program_id.clone(),
                    published_relation: binding.clone(),
                    graph_revision: 1,
                    bootstrap_cursor,
                });
            }
        }
    }
    for binding in &bindings {
        binding.validate().map_err(ApiError::bad_request)?;
    }
    Ok(bindings)
}

/// Resolve physical history from one durable graph snapshot, never from a
/// published relation ID or from the cardinality of its materialized output.
pub(super) async fn published_input_history_roots(
    state: &ApiState,
    inputs: &[ResolvedAdmissionInputV1],
) -> Result<Vec<RelationSchema>, ApiError> {
    let active = list_active_from_meta(state).await?;
    let mut pending = Vec::new();
    let mut roots = BTreeMap::new();
    for input in inputs {
        match input {
            ResolvedAdmissionInputV1::Source { catalog } => {
                let relation =
                    catalog_input_relation_schema(catalog).map_err(ApiError::bad_request)?;
                roots.insert(
                    (
                        relation.relation_id.clone(),
                        relation.relation_version.clone(),
                    ),
                    (INGEST_SOURCE_IDENTITY_GENERATION_V1, relation),
                );
            }
            ResolvedAdmissionInputV1::PublishedView { binding, .. } => {
                pending.push(binding.clone())
            }
        }
    }
    let mut visited = BTreeMap::new();
    while let Some(binding) = pending.pop() {
        if let Some(previous) = visited.insert(binding.producer_view_id.clone(), binding.clone()) {
            if previous != binding {
                return Err(ApiError::bad_request(
                    "conflicting producer identity in history roots",
                ));
            }
            continue;
        }
        let producer = active
            .iter()
            .find(|view| view.spec.view_id == binding.producer_view_id)
            .ok_or_else(|| ApiError::bad_request("history root producer is missing"))?;
        if published_relation_binding_for_active_view(producer)?.as_ref() != Some(&binding) {
            return Err(ApiError::bad_request(
                "history root producer generation or schema mismatch",
            ));
        }
        let runtime = producer
            .runtime
            .as_ref()
            .ok_or_else(|| ApiError::bad_request("history root runtime binding missing"))?;
        if runtime.input_bindings.is_empty() {
            return Err(ApiError::bad_request("history root input bindings missing"));
        }
        for input in &runtime.input_bindings {
            match input {
                StandingInputBindingV1::Source {
                    relation,
                    relation_generation,
                } => {
                    let key = (
                        relation.relation_id.clone(),
                        relation.relation_version.clone(),
                    );
                    if let Some((generation, previous)) = roots.get(&key) {
                        if generation != relation_generation || previous != relation {
                            return Err(ApiError::bad_request(
                                "conflicting physical root generation or schema",
                            ));
                        }
                    } else {
                        roots.insert(key, (*relation_generation, relation.clone()));
                    }
                    if *relation_generation != INGEST_SOURCE_IDENTITY_GENERATION_V1 {
                        return Err(ApiError::bad_request(
                            "unsupported physical history root generation",
                        ));
                    }
                }
                StandingInputBindingV1::PublishedView {
                    published_relation, ..
                } => pending.push(published_relation.clone()),
            }
        }
    }
    Ok(roots.into_values().map(|(_, relation)| relation).collect())
}

pub(super) fn persisted_consumer_bootstrap_inputs(
    active: &ActiveMaterializedView,
) -> Result<Vec<ConsumerViewDependencyInputV1>, ApiError> {
    let runtime = active
        .runtime
        .as_ref()
        .ok_or_else(|| ApiError::bad_request("consumer runtime binding missing"))?;
    if runtime.input_bindings.is_empty()
        || runtime
            .input_bindings
            .iter()
            .any(|input| matches!(input, StandingInputBindingV1::Source { .. }))
    {
        return Err(ApiError::bad_request(
            "published bootstrap/replay requires exclusively persisted published-view inputs",
        ));
    }
    let mut inputs = Vec::new();
    for input in &runtime.input_bindings {
        if let StandingInputBindingV1::PublishedView {
            edge_id,
            producer_tenant_id,
            producer_program_id,
            published_relation,
            bootstrap_cursor,
            ..
        } = input
        {
            let edge = view_dependency_edge_from_binding(
                producer_tenant_id,
                &runtime.standing_program_identity.program_id,
                &active.spec.view_id,
                1,
                &published_relation.relation.relation_id,
                &published_relation.relation.relation_version,
                producer_program_id,
                published_relation,
            )
            .map_err(ApiError::bad_request)?;
            if edge.edge_id != *edge_id {
                return Err(ApiError::bad_request("persisted bootstrap edge mismatch"));
            }
            inputs.push(ConsumerViewDependencyInputV1 {
                edge,
                binding: published_relation.clone(),
                bootstrap_cursor: bootstrap_cursor.clone().ok_or_else(|| {
                    ApiError::conflict(
                        "published input requires an authenticated persisted execution capture",
                    )
                })?,
            });
        }
    }
    Ok(inputs)
}

/// Resolve a declaration's optional baseline under the consumer operation lock.
/// A checkpoint capture wins over the current producer head on every retry.
/// The temporary clone is used only to construct inputs; it is never registered
/// and neither the declaration nor its logical plan/hash is rewritten.
pub(super) async fn resolve_consumer_bootstrap_inputs_locked(
    state: &ApiState,
    active: &ActiveMaterializedView,
    previous: Option<&StandingRuntimeCheckpointRecord>,
) -> Result<Vec<ConsumerViewDependencyInputV1>, ApiError> {
    let mut resolved = active.clone();
    let runtime = resolved
        .runtime
        .as_mut()
        .ok_or_else(|| ApiError::bad_request("consumer runtime binding missing"))?;
    for input in &mut runtime.input_bindings {
        let StandingInputBindingV1::PublishedView {
            edge_id,
            producer_tenant_id,
            published_relation,
            bootstrap_cursor,
            ..
        } = input
        else {
            continue;
        };
        if let Some(saved) = previous.and_then(|record| record.published_input_bootstrap.as_ref()) {
            saved.validate().map_err(meta_error_to_api)?;
            let capture = saved
                .inputs
                .iter()
                .find(|entry| &entry.input_edge == edge_id)
                .ok_or_else(|| {
                    ApiError::bad_request("persisted execution capture is missing an input edge")
                })?;
            if bootstrap_cursor
                .as_ref()
                .is_some_and(|declared| declared != &capture.captured_cursor)
                || capture.output_schema_hash != published_relation.output_schema_hash
            {
                return Err(ApiError::bad_request(
                    "persisted execution capture conflicts with declaration",
                ));
            }
            *bootstrap_cursor = Some(capture.captured_cursor.clone());
        }
        if bootstrap_cursor.is_none() {
            let producer =
                read_active_from_meta(state, &published_relation.producer_view_id).await?;
            *bootstrap_cursor = capture_optional_producer_bootstrap_cursor(
                state,
                producer_tenant_id,
                &producer,
                published_relation,
                edge_id,
            )
            .await?;
        }
        let cursor = bootstrap_cursor.as_ref().ok_or_else(|| {
            ApiError::service_unavailable(
                "published dependency must complete before capturing its execution baseline",
            )
        })?;
        checkpoint_publication::read_authoritative_cursor_checkpoint_header(
            state,
            producer_tenant_id,
            published_relation,
            cursor,
        )
        .await?;
    }
    persisted_consumer_bootstrap_inputs(&resolved)
}

/// Builds the dependency edges of one view from its durable input bindings.
pub(super) fn dependency_edges_from_input_bindings(
    tenant_id: &str,
    program_id: &str,
    view_id: &str,
    bindings: &[StandingInputBindingV1],
) -> Result<Vec<ViewDependencyEdgeV1>, ApiError> {
    let mut edges = Vec::new();
    for binding in bindings {
        if let StandingInputBindingV1::PublishedView {
            edge_id,
            producer_tenant_id,
            producer_program_id,
            published_relation,
            ..
        } = binding
        {
            let edge = view_dependency_edge_from_binding(
                producer_tenant_id,
                program_id,
                view_id,
                1,
                &published_relation.relation.relation_id,
                &published_relation.relation.relation_version,
                producer_program_id,
                published_relation,
            )
            .map_err(ApiError::bad_request)?;
            if &edge.edge_id != edge_id {
                return Err(ApiError::bad_request(format!(
                    "view `{view_id}` dependency edge mismatch"
                )));
            }
            edges.push(edge);
        }
    }
    let _ = (tenant_id, program_id);
    Ok(edges)
}

/// Converts one signed published delta into a published-delta Arrow batch:
/// public columns in binding order followed by exactly one private Int64
/// weight column. One Arrow row per delta record; weights are preserved
/// verbatim, including negative values and absolute values greater than one.
pub(super) fn published_delta_to_record_batch(
    binding: &PublishedRelationBindingV1,
    delta: &DeltaBatch,
) -> Result<RecordBatch, ApiError> {
    let rows = delta
        .net_rows()
        .map_err(|_| ApiError::bad_request("published output delta is malformed"))?;
    published_rows_to_record_batch(binding, &rows)
}

fn published_rows_to_record_batch(
    binding: &PublishedRelationBindingV1,
    rows: &[velorix_core::delta::DeltaRecord],
) -> Result<RecordBatch, ApiError> {
    let columns = &binding.relation.columns;
    if columns.is_empty() {
        return Err(ApiError::bad_request(
            "published relation delta requires at least one output column",
        ));
    }
    let primary_key = binding.relation.primary_key.iter().collect::<BTreeSet<_>>();
    let mut column_values: Vec<Vec<Value>> = vec![Vec::new(); columns.len()];
    let mut weights = Vec::with_capacity(rows.len());
    for row in rows {
        let key = row.key.as_json();
        let value = row.value.as_json();
        // Bag operators encode the whole row as the key; scalar aggregates use value fields.
        let value = if primary_key.is_empty() && value.is_null() && key.is_object() {
            key
        } else {
            value
        };
        for (index, column) in columns.iter().enumerate() {
            // Published deltas must already be in public-schema shape; missing
            // fields are an encoding violation, never a null default.
            let value_for_column = if primary_key.contains(&column.name) {
                if key.is_object() {
                    key.get(column.name.as_str()).cloned().ok_or_else(|| {
                        ApiError::bad_request(format!(
                            "published delta key for `{}` is missing primary key field `{}`",
                            binding.relation.relation_id, column.name
                        ))
                    })?
                } else if primary_key.len() == 1 {
                    key.clone()
                } else {
                    return Err(ApiError::bad_request(format!(
                        "published delta key for `{}` with {} primary key fields must be an object",
                        binding.relation.relation_id,
                        primary_key.len()
                    )));
                }
            } else {
                value.get(column.name.as_str()).cloned().ok_or_else(|| {
                    ApiError::bad_request(format!(
                        "published delta value for `{}` is missing output column `{}`",
                        binding.relation.relation_id, column.name
                    ))
                })?
            };
            column_values[index].push(value_for_column);
        }
        weights.push(row.weight);
    }
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len() + 1);
    let mut fields = Vec::with_capacity(columns.len() + 1);
    for (index, column) in columns.iter().enumerate() {
        fields.push(Field::new(
            column.name.as_str(),
            arrow_data_type_from_sql_data_type(&column.data_type)?,
            column.nullable,
        ));
        let object_rows = column_values[index]
            .iter()
            .map(|value| json!({ column.name.as_str(): value }))
            .collect::<Vec<_>>();
        arrays.push(
            json_reader_column_to_arrow_array(column, &object_rows).map_err(ApiError::internal)?,
        );
    }
    fields.push(Field::new(
        PUBLISHED_DELTA_WEIGHT_FIELD_V1,
        DataType::Int64,
        false,
    ));
    arrays.push(Arc::new(Int64Array::from(weights)));
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(|error| ApiError::internal(error.to_string()))
}

/// Legacy admission bootstraps all published inputs together; source producers
/// may publish genesis only while admission holds their physical-root locks.
pub(super) async fn bootstrap_legacy_view_if_empty(
    state: &ApiState,
    active: &ActiveMaterializedView,
) -> Result<(), ApiError> {
    if state.authoritative_relation_ingest_enabled() {
        return Err(ApiError::bad_request(
            "legacy bootstrap requires legacy ingest mode",
        ));
    }
    bootstrap_legacy_view_checkpoint(state, active, false).await
}

pub(super) async fn upgrade_existing_published_bootstrap(
    state: &ApiState,
    active: &ActiveMaterializedView,
) -> Result<(), ApiError> {
    if !active
        .runtime
        .as_ref()
        .is_some_and(view_has_published_view_inputs)
    {
        return Ok(());
    }
    bootstrap_legacy_view_checkpoint(state, active, true).await
}

fn bootstrap_legacy_view_checkpoint<'a>(
    state: &'a ApiState,
    active: &'a ActiveMaterializedView,
    require_existing: bool,
) -> impl std::future::Future<Output = Result<(), ApiError>> + Send + 'a {
    Box::pin(bootstrap_legacy_view_checkpoint_inner(
        state,
        active,
        require_existing,
    ))
}

async fn bootstrap_legacy_view_checkpoint_inner(
    state: &ApiState,
    active: &ActiveMaterializedView,
    require_existing: bool,
) -> Result<(), ApiError> {
    state.require_local_materialization()?;
    let identity = active_standing_runtime_identity(active)
        .ok_or_else(|| ApiError::bad_request("legacy bootstrap runtime identity missing"))?;
    let operation = state.standing_runtime_operation_lock(identity, &active.spec.view_id)?;
    let _guard = operation.lock().await;
    let lease =
        owner_lease::BackfillOwnerLease::start(state, identity, &active.spec.view_id).await?;
    lease.ensure_owned()?;
    let published_inputs = active
        .runtime
        .as_ref()
        .is_some_and(view_has_published_view_inputs);
    let previous =
        read_latest_standing_runtime_checkpoint(state, identity, &active.spec.view_id).await?;
    if require_existing && previous.is_none() {
        return Ok(());
    }
    if let Some(previous) = &previous {
        if published_inputs {
            lease
                .while_owned(resolve_consumer_bootstrap_inputs_locked(
                    state,
                    active,
                    Some(previous),
                ))
                .await?;
            consumer_direct_view_inputs(active, previous)?;
        }
        if !published_inputs || previous.published_input_bootstrap.is_some() {
            return Ok(());
        }
    }
    if !published_inputs && standing_runtime_create_requires_backfill(state, &active.spec).await? {
        return Err(ApiError::conflict(
            "legacy producer requires source backfill before bootstrap",
        ));
    }
    let inputs = if published_inputs {
        lease
            .while_owned(resolve_consumer_bootstrap_inputs_locked(
                state,
                active,
                previous.as_ref(),
            ))
            .await?
    } else {
        Vec::new()
    };
    let mut batches = Vec::with_capacity(inputs.len());
    let mut direct_inputs = Vec::with_capacity(inputs.len());
    let mut proofs = Vec::with_capacity(inputs.len());
    let mut bootstrap_inputs = Vec::with_capacity(inputs.len());
    let mut source_prep_traced = false;
    for input in &inputs {
        if !source_prep_traced {
            demand_materialization::source_prep_diagnostic(&lease)?;
            source_prep_traced = true;
        }
        let (record, proof, pages) = lease
            .while_owned(
                checkpoint_publication::read_authoritative_cursor_checkpoint_with_snapshot_pages(
                    state,
                    &input.bootstrap_cursor.producer_tenant_id,
                    &input.binding,
                    &input.bootstrap_cursor,
                    true,
                ),
            )
            .await?;
        let manifests = record
            .checkpoint
            .output_manifest_refs
            .iter()
            .filter(|reference| reference.starts_with(STANDING_RUNTIME_OUTPUT_MANIFEST_REF_PREFIX))
            .collect::<Vec<_>>();
        let [manifest_ref] = manifests.as_slice() else {
            return Err(ApiError::bad_request(
                "legacy bootstrap requires one output manifest",
            ));
        };
        let (_, manifest) = lease
            .while_owned(
                checkpoint_publication::read_standing_runtime_output_manifest_record(
                    state,
                    manifest_ref,
                    &input.binding.producer_view_id,
                ),
            )
            .await?;
        if manifest.checkpoint_key != record.checkpoint_key
            || manifest.checkpoint_content_hash != record.checkpoint.state_root.content_hash
            || manifest.tenant_id != record.checkpoint.identity.tenant_id
            || manifest.program_id != record.checkpoint.identity.program_id
            || manifest.logical_epoch != record.checkpoint.logical_epoch
        {
            return Err(ApiError::bad_request(
                "legacy bootstrap manifest checkpoint mismatch",
            ));
        }
        let total_pages = u64::try_from(manifest.pages.len()).map_err(ApiError::bad_request)?;
        let total_rows = u64::try_from(manifest.output_row_count).map_err(ApiError::bad_request)?;
        bootstrap_inputs.push(
            velorix_control::meta_admin::PublishedInputBootstrapCursorV1 {
                input_edge: input.edge.edge_id.clone(),
                captured_cursor: input.bootstrap_cursor.clone(),
                output_manifest_ref: (*manifest_ref).clone(),
                output_manifest_hash: stable_bytes_hash(
                    &serde_json::to_vec(&manifest).map_err(ApiError::internal)?,
                ),
                output_schema_hash: input.binding.output_schema_hash.clone(),
                total_pages,
                total_rows,
                next_page_index: total_pages,
                applied_rows: total_rows,
            },
        );
        let snapshots = if let Some(pages) = pages {
            if pages.schema != input.binding.relation {
                return Err(ApiError::bad_request(
                    "legacy bootstrap snapshot schema mismatch",
                ));
            }
            pages.pages
        } else {
            let snapshot = standing_runtime_checkpoint_published_output_with_limits(
                &record.checkpoint,
                state.runtime_resource_limits(),
            )
            .ok_or_else(|| ApiError::bad_request("legacy bootstrap snapshot missing"))?;
            vec![serde_json::from_value::<DeltaBatch>(snapshot).map_err(ApiError::bad_request)?]
        };
        let mut relation_batches = Vec::with_capacity(snapshots.len());
        let mut applied_rows = 0u64;
        for snapshot in snapshots {
            lease.ensure_owned()?;
            let rows = snapshot.net_rows().map_err(ApiError::bad_request)?;
            for row in &rows {
                lease.ensure_owned()?;
                let weight = u64::try_from(row.weight).map_err(|_| {
                    ApiError::bad_request("legacy bootstrap snapshot has negative multiplicity")
                })?;
                applied_rows = applied_rows
                    .checked_add(weight)
                    .ok_or_else(|| ApiError::bad_request("legacy bootstrap row count overflow"))?;
            }
            relation_batches.push(published_delta_to_record_batch(&input.binding, &snapshot)?);
        }
        if applied_rows != total_rows {
            return Err(ApiError::bad_request(
                "legacy bootstrap applied rows differ from captured manifest",
            ));
        }
        let binding = &input.binding;
        batches.push(RelationInputBatch {
            relation_id: binding.relation.relation_id.clone(),
            relation_version: binding.relation.relation_version.clone(),
            schema_fingerprint: binding.relation.schema_fingerprint.clone(),
            stream_id: binding.output_stream_id.clone(),
            partition_id: 0,
            start_offset_inclusive: 0,
            end_offset_exclusive: input.bootstrap_cursor.output_epoch,
            event_time_watermark: None,
            encoding: RelationInputEncodingV1::PublishedRelationDeltaV1 {
                delta_codec_identity: binding.delta_codec_identity.clone(),
                output_schema_hash: binding.output_schema_hash.clone(),
                weight_field_name: PUBLISHED_DELTA_WEIGHT_FIELD_V1.to_owned(),
                weight_field_index: binding.relation.columns.len(),
            },
            batches: relation_batches,
        });
        direct_inputs.push(StandingRuntimeDirectViewInputV1 {
            published_relation: binding.clone(),
            cursor: input.bootstrap_cursor.clone(),
        });
        proofs.push(proof);
    }
    let published = published_relation_binding_for_active_view(active)?;
    let bootstrap = if published_inputs {
        let binding = published
            .as_ref()
            .ok_or_else(|| ApiError::bad_request("legacy consumer published binding missing"))?;
        let progress = velorix_control::meta_admin::PublishedInputBootstrapProgressV1 {
            schema_version:
                velorix_control::meta_admin::PUBLISHED_INPUT_BOOTSTRAP_SCHEMA_VERSION_V1,
            bootstrap_generation: binding.producer_view_generation,
            plan_hash: binding.producer_plan_hash.clone(),
            inputs: bootstrap_inputs,
        };
        progress.validate().map_err(meta_error_to_api)?;
        Some(progress)
    } else {
        None
    };
    if let Some(previous) = &previous {
        // Existing complete cursors prove application; only add baseline evidence.
        direct_inputs = consumer_direct_view_inputs(active, previous)?;
        batches.clear();
        proofs.clear();
        for input in &direct_inputs {
            let (_, proof) = checkpoint_publication::read_authoritative_cursor_checkpoint_header(
                state,
                &input.cursor.producer_tenant_id,
                &input.published_relation,
                &input.cursor,
            )
            .await?;
            proofs.push(proof);
        }
    }
    let epoch = previous
        .as_ref()
        .map(|record| {
            record
                .checkpoint
                .logical_epoch
                .checked_add(1)
                .ok_or_else(|| ApiError::bad_request("legacy bootstrap epoch overflow"))
        })
        .transpose()?
        .unwrap_or(if published_inputs { 1 } else { 0 });
    let (runtime, _) = restore_standing_runtime_resident_locked(state, active)
        .await?
        .ok_or_else(|| ApiError::service_unavailable("legacy bootstrap runtime unavailable"))?;
    let key = EpochIdempotencyKey::new(format!("legacy-bootstrap:{}:{epoch}", active.spec_hash))
        .map_err(ApiError::bad_request)?;
    let result = async {
        let applied = apply_standing_runtime_changes_and_checkpoint_many(
            state,
            runtime,
            epoch,
            key,
            batches,
            StandingRuntimeBudgetLimits::from_state(state),
        )
        .await?;
        lease.ensure_owned()?;
        let mut context =
            StandingRuntimeCheckpointPersistContext::new(previous, Vec::new(), lease.owner.clone())
                .with_owner_renewal(Arc::clone(&lease.failure))
                .with_event_time_state(applied.staged_event_time_state)
                .with_published_relation(published)
                .with_direct_view_inputs(direct_inputs);
        context.published_input_bootstrap = bootstrap;
        for proof in proofs {
            context = context.with_validated_cursor_proof(proof);
        }
        persist_standing_runtime_checkpoint(
            state,
            &active.spec.view_id,
            &applied.checkpoint,
            &applied.output_deltas,
            context,
            None,
        )
        .await
    }
    .await;
    if result.is_err() {
        remove_standing_runtime(state, identity, &active.spec.view_id)?;
    }
    result?;
    activate_authoritative_view_bootstrap(state, identity, &active.spec.view_id).await
}

/// Reads and validates all producer output commits strictly after the given
/// cursor, in ascending lineage order. Fails closed on any lineage,
/// identity, or content mismatch.
#[cfg(test)]
pub(super) async fn read_validated_producer_commits_after_cursor(
    state: &ApiState,
    binding: &PublishedRelationBindingV1,
    cursor: &CausalViewCursorV1,
) -> Result<Vec<ValidatedProducerCommitV1>, ApiError> {
    read_producer_commits_after_cursor_inner(state, binding, cursor, false, None).await
}

pub(super) async fn read_validated_producer_commits_after_cursor_with_lease(
    state: &ApiState,
    binding: &PublishedRelationBindingV1,
    cursor: &CausalViewCursorV1,
    lease: &owner_lease::BackfillOwnerLease,
) -> Result<Vec<ValidatedProducerCommitV1>, ApiError> {
    read_producer_commits_after_cursor_inner(state, binding, cursor, false, Some(lease)).await
}

pub(super) async fn read_producer_deltas_after_bootstrap_cursor(
    state: &ApiState,
    binding: &PublishedRelationBindingV1,
    cursor: &CausalViewCursorV1,
) -> Result<Vec<ValidatedProducerCommitV1>, ApiError> {
    read_producer_commits_after_cursor_inner(state, binding, cursor, true, None).await
}

async fn read_producer_commits_after_cursor_inner(
    state: &ApiState,
    binding: &PublishedRelationBindingV1,
    cursor: &CausalViewCursorV1,
    metadata_only: bool,
    lease: Option<&owner_lease::BackfillOwnerLease>,
) -> Result<Vec<ValidatedProducerCommitV1>, ApiError> {
    let latest = latest_checkpoint_record_for_scope_inner(
        state,
        &cursor.producer_tenant_id,
        &cursor.producer_program_id,
        &cursor.producer_view_id,
        metadata_only,
    )
    .await?;
    if let Some(lease) = lease {
        lease.ensure_owned()?;
    }
    let mut latest = latest.ok_or_else(|| {
        ApiError::bad_request(format!(
            "producer view `{}` has no authoritative checkpoint",
            cursor.producer_view_id
        ))
    })?;
    let mut lineage = Vec::new();
    let mut hops = 0usize;
    const MAX_LINEAGE_HOPS: usize = 4_096;
    loop {
        if let Some(lease) = lease {
            lease.ensure_owned()?;
        }
        hops += 1;
        if hops > MAX_LINEAGE_HOPS {
            return Err(ApiError::bad_request(format!(
                "producer view `{}` exceeds the authoritative checkpoint lineage budget",
                cursor.producer_view_id
            )));
        }
        if latest.checkpoint.logical_epoch < cursor.output_epoch {
            return Err(ApiError::bad_request(format!(
                "producer view `{}` cursor is ahead of the authoritative frontier",
                cursor.producer_view_id
            )));
        }
        if latest.checkpoint.logical_epoch == cursor.output_epoch {
            if metadata_only {
                if checkpoint_publication::producer_cursor_digest_from_header(&latest, binding)?
                    != cursor.commit_digest
                {
                    return Err(ApiError::bad_request(
                        "bootstrap catch-up cursor digest mismatch",
                    ));
                }
            } else {
                validate_authoritative_view_cursor_commit(state, binding, cursor, &latest).await?;
                if let Some(lease) = lease {
                    lease.ensure_owned()?;
                }
            }
            break;
        }
        let previous = latest.previous_checkpoint.clone().ok_or_else(|| {
            ApiError::bad_request(format!(
                "producer view `{}` cursor references an epoch missing from the authoritative checkpoint lineage",
                cursor.producer_view_id
            ))
        })?;
        if previous.logical_epoch >= latest.checkpoint.logical_epoch
            || previous.logical_epoch < cursor.output_epoch
        {
            return Err(ApiError::bad_request(format!(
                "producer view `{}` has a non-contiguous authoritative checkpoint lineage",
                cursor.producer_view_id
            )));
        }
        if previous.logical_epoch == cursor.output_epoch {
            // The consumer has already applied this checkpoint prefix. Its
            // authenticated cursor is the proof of that fact, so validate the
            // checkpoint header commitment here without hydrating the old
            // snapshot manifest/pages. Those objects may have been retired
            // after the consumer durably recorded its completed bootstrap.
            let boundary =
                checkpoint_publication::read_standing_runtime_checkpoint_header_from_pointer(
                    state,
                    &cursor.producer_tenant_id,
                    &cursor.producer_program_id,
                    &cursor.producer_view_id,
                    &previous,
                )
                .await?;
            if let Some(lease) = lease {
                lease.ensure_owned()?;
            }
            if boundary
                .published_input_bootstrap
                .as_ref()
                .is_some_and(|progress| !progress.is_complete())
                || checkpoint_publication::producer_cursor_digest_from_header(&boundary, binding)?
                    != cursor.commit_digest
            {
                return Err(ApiError::bad_request(
                    "captured producer cursor header commitment mismatch",
                ));
            }
            // The current record is the first commit after the consumer's
            // captured prefix. Keep it in the replay lineage; only the
            // previous snapshot is already covered by the cursor.
            lineage.push(latest);
            break;
        }
        lineage.push(latest);
        latest = if metadata_only {
            checkpoint_publication::read_standing_runtime_checkpoint_header_from_pointer(
                state,
                &cursor.producer_tenant_id,
                &cursor.producer_program_id,
                &cursor.producer_view_id,
                &previous,
            )
            .await?
        } else {
            read_standing_runtime_checkpoint_record_from_pointer_for_scope(
                state,
                &cursor.producer_tenant_id,
                &cursor.producer_program_id,
                &cursor.producer_view_id,
                &previous,
            )
            .await?
        };
        if let Some(lease) = lease {
            lease.ensure_owned()?;
        }
    }
    lineage.reverse();
    let mut commits = Vec::with_capacity(lineage.len());
    let mut previous_epoch = cursor.output_epoch;
    for record in lineage {
        let partial = record
            .demand_materialization
            .as_ref()
            .is_some_and(|progress| !progress.full_complete)
            || record
                .published_input_bootstrap
                .as_ref()
                .is_some_and(|progress| !progress.is_complete());
        if partial {
            if let Some(progress) = &record.demand_materialization {
                progress.validate_checkpoint(&record.checkpoint)?;
                if progress.plan_hash != binding.producer_plan_hash {
                    return Err(ApiError::bad_request("partial producer plan mismatch"));
                }
            }
            if let Some(progress) = &record.published_input_bootstrap {
                progress.validate().map_err(meta_error_to_api)?;
                if progress.bootstrap_generation != binding.producer_view_generation
                    || progress.plan_hash != binding.producer_plan_hash
                {
                    return Err(ApiError::bad_request(
                        "partial producer generation or plan mismatch",
                    ));
                }
            }
            if record.view_id != binding.producer_view_id
                || record.previous_checkpoint.as_ref().is_some_and(|previous| {
                    previous.bootstrap_generation != 0
                        && (previous.bootstrap_generation != binding.producer_view_generation
                            || previous.plan_hash != binding.producer_plan_hash)
                })
            {
                return Err(ApiError::bad_request("partial producer identity mismatch"));
            }
            if record.checkpoint.input_coverage.is_some()
                || record.checkpoint.causal_cut.is_some()
                || record
                    .checkpoint
                    .output_manifest_refs
                    .iter()
                    .any(|reference| {
                        reference.starts_with(STANDING_RUNTIME_OUTPUT_COMMIT_REF_PREFIX)
                    })
            {
                return Err(ApiError::bad_request(
                    "partial producer checkpoint claims authoritative output or coverage",
                ));
            }
            // Only published commits advance the consumer's predecessor epoch.
            continue;
        }
        let commit = if metadata_only {
            authoritative_producer_commit_for_header(state, binding, &record).await?
        } else {
            authoritative_producer_commit_for_checkpoint(state, binding, &record).await?
        };
        if let Some(lease) = lease {
            lease.ensure_owned()?;
        }
        let (_key, delta_record) = {
            let commit_refs = record
                .checkpoint
                .output_manifest_refs
                .iter()
                .filter(|output_ref| {
                    output_ref.starts_with(STANDING_RUNTIME_OUTPUT_COMMIT_REF_PREFIX)
                })
                .collect::<Vec<_>>();
            read_standing_runtime_output_delta_record(
                state,
                commit_refs[0],
                &cursor.producer_view_id,
            )
            .await?
        };
        if let Some(lease) = lease {
            lease.ensure_owned()?;
        }
        let output_delta = serde_json::from_value::<DeltaBatch>(delta_record.output_delta.clone())
            .map_err(|error| {
                ApiError::bad_request(format!(
                    "producer view `{}` epoch {} output delta is malformed: {error}",
                    cursor.producer_view_id, record.checkpoint.logical_epoch
                ))
            })?;
        commits.push(ValidatedProducerCommitV1 {
            previous_epoch,
            logical_epoch: record.checkpoint.logical_epoch,
            commit_digest: commit.producer_commit_digest.clone(),
            output_delta,
        });
        previous_epoch = record.checkpoint.logical_epoch;
    }
    Ok(commits)
}

/// Reads the latest durable checkpoint record for a standing runtime scope,
/// consulting the authoritative meta-store pointer when present and the
/// object-store LIST scan otherwise.
pub(super) async fn latest_checkpoint_record_for_scope(
    state: &ApiState,
    tenant_id: &str,
    program_id: &str,
    view_id: &str,
) -> Result<Option<StandingRuntimeCheckpointRecord>, ApiError> {
    latest_checkpoint_record_for_scope_inner(state, tenant_id, program_id, view_id, false).await
}

async fn latest_checkpoint_record_for_scope_inner(
    state: &ApiState,
    tenant_id: &str,
    program_id: &str,
    view_id: &str,
    metadata_only: bool,
) -> Result<Option<StandingRuntimeCheckpointRecord>, ApiError> {
    if let Some(meta_store) = &state.meta_store {
        if let Some(pointer) = meta_store
            .read_standing_runtime_checkpoint(tenant_id, program_id, view_id)
            .await
            .map_err(meta_error_to_api)?
        {
            if metadata_only {
                return checkpoint_publication::read_standing_runtime_checkpoint_header_from_pointer(state, tenant_id, program_id, view_id, &pointer).await.map(Some);
            }
            return read_standing_runtime_checkpoint_record_from_pointer_for_scope(
                state, tenant_id, program_id, view_id, &pointer,
            )
            .await
            .map(Some);
        }
        return Ok(None);
    }
    eprintln!(
        "WARNING: No meta_store configured, falling back to S3 LIST scan for checkpoint read \
         (tenant={}, program={}, view={})",
        tenant_id, program_id, view_id
    );
    let prefix = ObjectPath::from(format!(
        "v1/standing-runtime-checkpoints/{}/{}/{}/epochs",
        tenant_id, program_id, view_id
    ));
    let mut stream = state.store.list(Some(&prefix));
    let mut latest_checkpoint: Option<(String, StandingRuntimeCheckpointKeyParts)> = None;
    while let Some(meta) = stream.try_next().await.map_err(ApiError::internal)? {
        let location = meta.location.to_string();
        if location.ends_with(".checkpoint.arrow") {
            let (_, parts) = ObjectKey::parse_standing_runtime_checkpoint(location.clone())
                .map_err(ApiError::bad_request)?;
            latest_checkpoint = Some(match latest_checkpoint {
                Some((current, current_parts))
                    if current_parts.logical_epoch > parts.logical_epoch =>
                {
                    (current, current_parts)
                }
                Some((_current, current_parts))
                    if current_parts.logical_epoch == parts.logical_epoch =>
                {
                    return Err(ApiError::bad_request(format!(
                        "multiple standing runtime checkpoints for `{}/{}/{}` epoch {}",
                        tenant_id, program_id, view_id, parts.logical_epoch
                    )));
                }
                _ => (location, parts),
            });
        }
    }
    let Some((latest_checkpoint_path, _)) = latest_checkpoint else {
        return Ok(None);
    };
    let bytes = state
        .store
        .get(&ObjectPath::from(latest_checkpoint_path.clone()))
        .await
        .map_err(ApiError::internal)?
        .bytes()
        .await
        .map_err(ApiError::internal)?;
    let mut record = standing_runtime_checkpoint_record_from_slice(&bytes)
        .map_err(|source| ApiError::bad_request(source.to_string()))?;
    if metadata_only {
        let pointer = standing_runtime_checkpoint_pointer_from_record(&record);
        return checkpoint_publication::read_standing_runtime_checkpoint_header_from_pointer(
            state, tenant_id, program_id, view_id, &pointer,
        )
        .await
        .map(Some);
    }
    hydrate_standing_runtime_checkpoint_state_payload(state, &mut record).await?;
    Ok(Some(record))
}

/// Converts a validated producer commit into the consumer's published-delta
/// input batch and its cursor.
pub(super) fn relation_input_from_validated_producer_commit(
    binding: &PublishedRelationBindingV1,
    edge_id: &str,
    cursor: &CausalViewCursorV1,
    commit: &ValidatedProducerCommitV1,
) -> Result<(RelationInputBatch, CausalViewCursorV1), ApiError> {
    if cursor.input_edge != edge_id
        || cursor.output_epoch != commit.previous_epoch
        || commit.logical_epoch <= commit.previous_epoch
    {
        return Err(ApiError::bad_request(
            "producer commit does not extend the dependency cursor",
        ));
    }
    let batch = published_delta_to_record_batch(binding, &commit.output_delta)?;
    let relation_input = RelationInputBatch {
        relation_id: binding.relation.relation_id.clone(),
        relation_version: binding.relation.relation_version.clone(),
        stream_id: binding.output_stream_id.clone(),
        partition_id: 0,
        schema_fingerprint: binding.relation.schema_fingerprint.clone(),
        start_offset_inclusive: commit.previous_epoch,
        end_offset_exclusive: commit.logical_epoch,
        event_time_watermark: None,
        encoding: RelationInputEncodingV1::PublishedRelationDeltaV1 {
            delta_codec_identity: binding.delta_codec_identity.clone(),
            output_schema_hash: binding.output_schema_hash.clone(),
            weight_field_name: PUBLISHED_DELTA_WEIGHT_FIELD_V1.to_string(),
            weight_field_index: binding.relation.columns.len(),
        },
        batches: vec![batch],
    };
    let next_cursor = CausalViewCursorV1 {
        input_edge: edge_id.to_string(),
        producer_tenant_id: cursor.producer_tenant_id.clone(),
        producer_program_id: cursor.producer_program_id.clone(),
        producer_view_id: cursor.producer_view_id.clone(),
        producer_generation: cursor.producer_generation,
        output_stream: cursor.output_stream.clone(),
        output_epoch: commit.logical_epoch,
        commit_digest: commit.commit_digest.clone(),
    };
    next_cursor
        .validate()
        .map_err(|_| ApiError::bad_request("producer commit cursor is invalid"))?;
    Ok((relation_input, next_cursor))
}

pub(super) fn published_commit_idempotency_key(
    edge_id: &str,
    previous_epoch: u64,
    logical_epoch: u64,
    commit_digest: &str,
) -> Result<EpochIdempotencyKey, StandingProgramRuntimeError> {
    let hash = stable_bytes_hash(
        format!("{edge_id}:{previous_epoch}:{logical_epoch}:{commit_digest}").as_bytes(),
    );
    EpochIdempotencyKey::new(format!(
        "view-on-view:{edge_id}:{previous_epoch}:{logical_epoch}:{hash}"
    ))
}

/// Applies one validated producer commit to the consumer runtime and publishes
/// the consumer checkpoint with the advanced cursor.
pub(super) async fn apply_validated_producer_commit_to_consumer(
    state: &ApiState,
    active: &ActiveMaterializedView,
    binding: &PublishedRelationBindingV1,
    edge_id: &str,
    cursor: &CausalViewCursorV1,
    commit: &ValidatedProducerCommitV1,
) -> Result<(), ApiError> {
    state.require_local_materialization()?;
    let Some(identity) = active_standing_runtime_identity(active) else {
        return Err(ApiError::bad_request(format!(
            "consumer view `{}` has no standing runtime identity",
            active.spec.view_id
        )));
    };
    let operation_lock = state.standing_runtime_operation_lock(identity, &active.spec.view_id)?;
    let _operation_guard = operation_lock.lock().await;
    let lease =
        owner_lease::BackfillOwnerLease::start(state, identity, &active.spec.view_id).await?;
    apply_validated_producer_commit_to_consumer_locked(
        state, active, binding, edge_id, cursor, commit, lease,
    )
    .await
}

async fn apply_validated_producer_commit_to_consumer_locked(
    state: &ApiState,
    active: &ActiveMaterializedView,
    binding: &PublishedRelationBindingV1,
    edge_id: &str,
    cursor: &CausalViewCursorV1,
    commit: &ValidatedProducerCommitV1,
    lease: Arc<owner_lease::BackfillOwnerLease>,
) -> Result<(), ApiError> {
    state.require_local_materialization()?;
    let identity = active_standing_runtime_identity(active)
        .ok_or_else(|| ApiError::bad_request("consumer view has no standing runtime identity"))?;
    lease.ensure_owned()?;
    // Published consumers resume from their durable baseline/cursors below;
    // their cache-miss replay plan must never trigger physical-source replay.
    let (runtime, _) = lease
        .while_owned(restore_standing_runtime_resident_locked(state, active))
        .await?
        .ok_or_else(|| ApiError::service_unavailable("consumer runtime could not be restored"))?;
    let previous_checkpoint = lease
        .while_owned(read_latest_standing_runtime_checkpoint(
            state,
            identity,
            &active.spec.view_id,
        ))
        .await?;
    let previous = previous_checkpoint.as_ref().ok_or_else(|| {
        ApiError::bad_request("consumer requires a complete baseline checkpoint before replay")
    })?;
    lease
        .while_owned(resolve_consumer_bootstrap_inputs_locked(
            state,
            active,
            Some(previous),
        ))
        .await?;
    lease.ensure_owned()?;
    let mut direct_inputs = consumer_direct_view_inputs(active, previous)?;
    let input = direct_inputs
        .iter_mut()
        .find(|input| input.cursor.input_edge == edge_id)
        .ok_or_else(|| ApiError::bad_request("producer commit references an unconfigured edge"))?;
    if &input.published_relation != binding {
        return Err(ApiError::bad_request(
            "producer commit binding differs from configured edge",
        ));
    }
    let mut expected_cursor_identity = input.cursor.clone();
    expected_cursor_identity.output_epoch = cursor.output_epoch;
    expected_cursor_identity.commit_digest = cursor.commit_digest.clone();
    if cursor != &expected_cursor_identity
        || cursor.output_epoch != commit.previous_epoch
        || commit.logical_epoch <= commit.previous_epoch
    {
        return Err(ApiError::bad_request(
            "producer commit cursor identity or lineage mismatch",
        ));
    }
    // A concurrent drain may already have durably advanced this edge. The
    // durable edge cursor, never another producer's epoch, decides replay.
    if input.cursor.output_epoch >= commit.logical_epoch {
        if input.cursor.output_epoch == commit.logical_epoch
            && input.cursor.commit_digest != commit.commit_digest
        {
            return Err(ApiError::bad_request(
                "producer commit digest conflicts with consumed edge",
            ));
        }
        return Ok(());
    }
    if &input.cursor != cursor {
        return Err(ApiError::bad_request(
            "producer replay cursor differs from durable consumer edge",
        ));
    }
    let (relation_input, next_cursor) =
        relation_input_from_validated_producer_commit(binding, edge_id, &input.cursor, commit)?;
    input.cursor = next_cursor;
    let published_relation = published_relation_binding_for_active_view(active)?;
    let mut cursor_proofs = Vec::with_capacity(direct_inputs.len());
    for input in &direct_inputs {
        let (_, proof) = checkpoint_publication::read_authoritative_cursor_checkpoint_header(
            state,
            &identity.tenant_id,
            &input.published_relation,
            &input.cursor,
        )
        .await?;
        cursor_proofs.push(proof);
    }
    let consumer_epoch = previous
        .checkpoint
        .logical_epoch
        .checked_add(1)
        .ok_or_else(|| ApiError::bad_request("consumer logical epoch exhausted"))?;
    let owner = lease.owner.clone();
    let idempotency_key = published_commit_idempotency_key(
        edge_id,
        commit.previous_epoch,
        commit.logical_epoch,
        &commit.commit_digest,
    )
    .map_err(ApiError::bad_request)?;
    let apply_result = match apply_standing_runtime_changes_and_checkpoint(
        state,
        Arc::clone(&runtime),
        consumer_epoch,
        idempotency_key,
        relation_input,
        StandingRuntimeBudgetLimits::from_state(state),
    )
    .await
    {
        Ok(apply_result) => apply_result,
        Err(error) => {
            remove_standing_runtime(state, identity, &active.spec.view_id)?;
            return Err(error);
        }
    };
    if let Err(error) = lease.ensure_owned() {
        remove_standing_runtime(state, identity, &active.spec.view_id)?;
        return Err(error);
    }
    let mut persist_context =
        StandingRuntimeCheckpointPersistContext::new(previous_checkpoint, Vec::new(), owner)
            .with_owner_renewal(Arc::clone(&lease.failure))
            .with_event_time_state(apply_result.staged_event_time_state.clone())
            .with_published_relation(published_relation)
            .with_direct_view_inputs(direct_inputs);
    for proof in cursor_proofs {
        persist_context = persist_context.with_validated_cursor_proof(proof);
    }
    if let Err(error) = persist_standing_runtime_checkpoint(
        state,
        &active.spec.view_id,
        &apply_result.checkpoint,
        &apply_result.output_deltas,
        persist_context,
        None,
    )
    .await
    {
        remove_standing_runtime(state, identity, &active.spec.view_id)?;
        return Err(error);
    }
    Ok(())
}

/// Returns the consumer's current cursor for the given edge from its latest
/// checkpoint causal cut.
pub(super) fn consumer_edge_cursor(
    record: &StandingRuntimeCheckpointRecord,
    edge_id: &str,
) -> Result<Option<CausalViewCursorV1>, ApiError> {
    let Some(causal_cut) = record.checkpoint.causal_cut.as_ref() else {
        return Ok(None);
    };
    let mut cursors = causal_cut
        .direct_view_cursors
        .iter()
        .filter(|cursor| cursor.input_edge == edge_id)
        .collect::<Vec<_>>();
    if cursors.len() > 1 {
        return Err(ApiError::bad_request(format!(
            "consumer checkpoint contains {} cursors for edge `{edge_id}`",
            cursors.len()
        )));
    }
    Ok(cursors.pop().cloned())
}

/// Reconstruct the complete direct cut from the durable consumer checkpoint.
/// Missing/extra edges fail closed instead of falling back to bootstrap heads.
pub(super) fn consumer_direct_view_inputs(
    active: &ActiveMaterializedView,
    record: &StandingRuntimeCheckpointRecord,
) -> Result<Vec<StandingRuntimeDirectViewInputV1>, ApiError> {
    let runtime = active
        .runtime
        .as_ref()
        .ok_or_else(|| ApiError::bad_request("consumer runtime binding is missing"))?;
    let mut inputs = Vec::new();
    for binding in &runtime.input_bindings {
        if let StandingInputBindingV1::PublishedView {
            edge_id,
            published_relation,
            bootstrap_cursor,
            ..
        } = binding
        {
            let cursor = consumer_edge_cursor(record, edge_id)?.ok_or_else(|| {
                ApiError::bad_request(format!(
                    "consumer checkpoint is missing dependency cursor `{edge_id}`"
                ))
            })?;
            let baseline = bootstrap_cursor
                .as_ref()
                .or_else(|| {
                    record
                        .published_input_bootstrap
                        .as_ref()
                        .and_then(|progress| {
                            progress
                                .inputs
                                .iter()
                                .find(|input| &input.input_edge == edge_id)
                                .map(|input| &input.captured_cursor)
                        })
                })
                .ok_or_else(|| ApiError::bad_request("consumer execution baseline is missing"))?;
            let mut expected_identity = baseline.clone();
            expected_identity.output_epoch = cursor.output_epoch;
            expected_identity.commit_digest = cursor.commit_digest.clone();
            if cursor != expected_identity || cursor.output_epoch < baseline.output_epoch {
                return Err(ApiError::bad_request(
                    "consumer dependency cursor identity or baseline mismatch",
                ));
            }
            inputs.push(StandingRuntimeDirectViewInputV1 {
                published_relation: published_relation.clone(),
                cursor,
            });
        }
    }
    if record
        .checkpoint
        .causal_cut
        .as_ref()
        .map(|cut| cut.direct_view_cursors.len())
        != Some(inputs.len())
    {
        return Err(ApiError::bad_request(
            "consumer checkpoint dependency cursor set mismatch",
        ));
    }
    Ok(inputs)
}

#[derive(Clone, Debug, Default)]
pub(super) struct DependencyDrainSummary {
    pub(super) applied_commits: usize,
    pub(super) updated_views: usize,
}

/// Pulls every pending producer commit into each active consumer view, in
/// producer-first topological order, and continues across dependency layers.
///
/// Durable producer commits are the only propagation source; in-memory deltas
/// are never forwarded. Commits already consumed by a consumer (per its
/// checkpoint cursor) are skipped.
pub(super) async fn drain_published_view_dependencies(
    state: &ApiState,
) -> Result<DependencyDrainSummary, ApiError> {
    drain_published_view_dependencies_skipping(state, &BTreeSet::new()).await
}

pub(super) fn drain_published_view_dependencies_skipping<'a>(
    state: &'a ApiState,
    skipped_views: &'a BTreeSet<String>,
) -> impl std::future::Future<Output = Result<DependencyDrainSummary, ApiError>> + Send + 'a {
    Box::pin(drain_published_view_dependencies_skipping_inner(
        state,
        skipped_views,
    ))
}

async fn drain_published_view_dependencies_skipping_inner(
    state: &ApiState,
    skipped_views: &BTreeSet<String>,
) -> Result<DependencyDrainSummary, ApiError> {
    let snapshot = ActiveViewReadSnapshot::load(state).await?;
    drain_published_view_dependencies_from_snapshot_skipping(state, &snapshot, skipped_views).await
}

pub(super) fn drain_published_view_dependencies_from_snapshot<'a>(
    state: &'a ApiState,
    snapshot: &'a ActiveViewReadSnapshot,
) -> impl std::future::Future<Output = Result<DependencyDrainSummary, ApiError>> + Send + 'a {
    Box::pin(drain_published_view_dependencies_from_snapshot_inner(
        state, snapshot,
    ))
}

async fn drain_published_view_dependencies_from_snapshot_inner(
    state: &ApiState,
    snapshot: &ActiveViewReadSnapshot,
) -> Result<DependencyDrainSummary, ApiError> {
    drain_published_view_dependencies_from_snapshot_skipping(state, snapshot, &BTreeSet::new())
        .await
}

async fn drain_published_view_dependencies_from_snapshot_skipping(
    state: &ApiState,
    snapshot: &ActiveViewReadSnapshot,
    skipped_views: &BTreeSet<String>,
) -> Result<DependencyDrainSummary, ApiError> {
    state.require_local_materialization()?;
    state.validate_standing_runtime_fencing_or_evict().await?;
    let active_views = snapshot.views();
    let edges = view_dependency_edges_from_snapshot(active_views)?;
    let mut summary = DependencyDrainSummary::default();
    if edges.is_empty() {
        return Ok(summary);
    }
    let order = validate_view_dependency_graph(&edges).map_err(|error| {
        ApiError::bad_request(format!(
            "view-on-view dependency graph rejected during drain: {}",
            error
        ))
    })?;
    let mut processed: BTreeSet<String> = BTreeSet::new();
    for view_id in order {
        if !processed.insert(view_id.clone()) {
            continue;
        }
        if skipped_views.contains(&view_id) {
            continue;
        }
        let Some(active) = active_views
            .iter()
            .find(|active| active.spec.view_id == view_id)
        else {
            continue;
        };
        if !standing_runtime_can_accept_incremental_ingest(active) {
            continue;
        }
        let Some(identity) = active_standing_runtime_identity(active) else {
            continue;
        };
        if let Some(meta_store) = state.meta_store.as_ref() {
            let owner = meta_store
                .read_standing_runtime_owner(
                    &identity.tenant_id,
                    &identity.program_id,
                    &active.spec.view_id,
                )
                .await
                .map_err(meta_error_to_api)?;
            if owner
                .as_ref()
                .is_some_and(|owner| owner.owner_id != state.owner_id)
            {
                // Published catchup is startup work too. Leave a view owned
                // by another API untouched; immutable output remains readable.
                continue;
            }
        }
        let Some(runtime_binding) = active.runtime.as_ref() else {
            continue;
        };
        let published_inputs = runtime_binding
            .input_bindings
            .iter()
            .filter_map(|binding| match binding {
                StandingInputBindingV1::PublishedView {
                    edge_id,
                    producer_program_id,
                    published_relation,
                    bootstrap_cursor,
                    ..
                } => Some((
                    edge_id,
                    producer_program_id,
                    published_relation,
                    bootstrap_cursor,
                )),
                StandingInputBindingV1::Source { .. } => None,
            })
            .collect::<Vec<_>>();
        if published_inputs.is_empty() {
            continue;
        }
        let mut latest =
            read_latest_standing_runtime_checkpoint(state, identity, &active.spec.view_id).await?;
        if latest.is_none() && !state.authoritative_relation_ingest_enabled() {
            // Legacy mode eagerly bootstraps a declared consumer when its
            // producer first publishes output. Admission may have happened
            // before that first producer checkpoint existed, so there was
            // no baseline to capture at declaration time.
            match bootstrap_legacy_view_if_empty(state, active).await {
                Ok(()) => {}
                Err(error)
                    if error.status == StatusCode::CONFLICT
                        && error
                            .message
                            .starts_with("standing runtime owner conflict for ") =>
                {
                    continue;
                }
                Err(error) => return Err(error),
            }
            latest = read_latest_standing_runtime_checkpoint(state, identity, &active.spec.view_id)
                .await?;
        }
        let Some(latest) = latest else {
            // The consumer has no checkpoint yet; its admission-time
            // baseline bootstrap is responsible for initializing state.
            continue;
        };
        if latest
            .published_input_bootstrap
            .as_ref()
            .is_some_and(|progress| !progress.is_complete())
        {
            continue;
        }
        let mut pending_edges = Vec::with_capacity(published_inputs.len());
        for (edge_id, _producer_program_id, binding, _baseline_cursor) in published_inputs {
            // Falling back to the admission-time baseline cursor could
            // reapply commits, so a missing cursor fails closed.
            let cursor = consumer_edge_cursor(&latest, edge_id)?.ok_or_else(|| {
                ApiError::bad_request(format!(
                    "consumer view `{}` checkpoint is missing the dependency cursor for edge `{edge_id}`",
                    active.spec.view_id
                ))
            })?;
            pending_edges.push((edge_id, binding, cursor));
        }
        // One view lease covers every dependency edge and is established
        // before any producer commit artifact is read.
        let lease =
            match owner_lease::BackfillOwnerLease::start(state, identity, &active.spec.view_id)
                .await
            {
                Ok(lease) => lease,
                Err(error)
                    if error.status == StatusCode::CONFLICT
                        && error
                            .message
                            .starts_with("standing runtime owner conflict for ") =>
                {
                    continue;
                }
                Err(error) => return Err(error),
            };
        lease.ensure_owned()?;
        demand_materialization::source_prep_diagnostic(&lease)?;
        for (edge_id, binding, mut cursor) in pending_edges {
            let commits = lease
                .while_owned(read_validated_producer_commits_after_cursor_with_lease(
                    state, binding, &cursor, &lease,
                ))
                .await?;
            for commit in &commits {
                let operation_lock =
                    state.standing_runtime_operation_lock(identity, &active.spec.view_id)?;
                let _operation_guard = operation_lock.lock().await;
                apply_validated_producer_commit_to_consumer_locked(
                    state,
                    active,
                    binding,
                    edge_id,
                    &cursor,
                    commit,
                    Arc::clone(&lease),
                )
                .await?;
                cursor.output_epoch = commit.logical_epoch;
                cursor.commit_digest = commit.commit_digest.clone();
                summary.applied_commits += 1;
            }
            if !commits.is_empty() {
                summary.updated_views += 1;
            }
        }
    }
    Ok(summary)
}
