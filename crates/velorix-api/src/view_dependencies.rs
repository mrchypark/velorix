use super::*;

/// One resolved input edge of a view admission: either a direct source
/// relation catalog or the published output of an active producer view.
#[derive(Clone, Debug)]
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

    let active_views = state
        .view_registry()?
        .list_active()
        .await
        .map_err(materialized_view_registry_error_to_api)?;
    let mut resolved = Vec::with_capacity(refs.len());
    for (relation_id, relation_version) in refs {
        let source = try_read_source_catalog(state, &relation_id, &relation_version).await?;
        let published = active_views
            .iter()
            .filter(|active| standing_runtime_can_accept_incremental_ingest(active))
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

/// Phase 4 dependency scope: a consumer view has exactly one input relation,
/// and published-view inputs cannot be mixed with direct source inputs.
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
    if published_count > 0 && inputs.len() > 1 {
        return Err(ApiError::bad_request(format!(
            "view-on-view dependency scope currently supports exactly one input relation; received {}",
            inputs.len()
        )));
    }
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
    let Some(identity) = active_standing_runtime_identity(producer) else {
        return Err(ApiError::bad_request(format!(
            "producer view `{}` has no standing runtime identity",
            binding.producer_view_id
        )));
    };
    let record = read_latest_standing_runtime_checkpoint(state, identity, &binding.producer_view_id)
        .await?
        .ok_or_else(|| {
            ApiError::bad_request(format!(
                "producer view `{}` has no authoritative published checkpoint; a view-on-view consumer requires the producer to have materialized output first",
                binding.producer_view_id
            ))
        })?;
    let commit = authoritative_producer_commit_for_checkpoint(state, binding, &record).await?;
    let cursor = CausalViewCursorV1 {
        input_edge: edge_id.to_string(),
        producer_tenant_id: tenant_id.to_string(),
        producer_program_id: identity.program_id.clone(),
        producer_view_id: binding.producer_view_id.clone(),
        producer_generation: binding.producer_view_generation,
        output_stream: binding.output_stream_id.clone(),
        output_epoch: record.checkpoint.logical_epoch,
        commit_digest: commit.producer_commit_digest.clone(),
    };
    cursor
        .validate()
        .map_err(|_| ApiError::bad_request("captured producer head cursor is invalid"))?;
    // The cursor must satisfy the full authoritative commit validation before
    // it becomes a consumer's baseline.
    validate_authoritative_view_cursor_commit(state, binding, &cursor, &record).await?;
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
    let active_views = state
        .view_registry()?
        .list_active()
        .await
        .map_err(materialized_view_registry_error_to_api)?;
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
pub(super) async fn input_bindings_for_resolved_inputs(
    state: &ApiState,
    tenant_id: &str,
    inputs: &[ResolvedAdmissionInputV1],
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
                let bootstrap_cursor =
                    capture_producer_head_cursor(state, tenant_id, producer, binding, &edge_id)
                        .await?;
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
    let columns = &binding.relation.columns;
    if columns.is_empty() {
        return Err(ApiError::bad_request(
            "published relation delta requires at least one output column",
        ));
    }
    let rows = delta
        .net_rows()
        .map_err(|_| ApiError::bad_request("published output delta is malformed"))?;
    let primary_key = binding.relation.primary_key.iter().collect::<BTreeSet<_>>();
    let mut column_values: Vec<Vec<Value>> = vec![Vec::new(); columns.len()];
    let mut weights = Vec::with_capacity(rows.len());
    for row in &rows {
        let key = row.key.as_json();
        let value = row.value.as_json();
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

/// Reads and validates all producer output commits strictly after the given
/// cursor, in ascending lineage order. Fails closed on any lineage,
/// identity, or content mismatch.
pub(super) async fn read_validated_producer_commits_after_cursor(
    state: &ApiState,
    binding: &PublishedRelationBindingV1,
    cursor: &CausalViewCursorV1,
) -> Result<Vec<ValidatedProducerCommitV1>, ApiError> {
    let mut latest = latest_checkpoint_record_for_scope(
        state,
        &cursor.producer_tenant_id,
        &cursor.producer_program_id,
        &cursor.producer_view_id,
    )
    .await?
    .ok_or_else(|| {
        ApiError::bad_request(format!(
            "producer view `{}` has no authoritative checkpoint",
            cursor.producer_view_id
        ))
    })?;
    let mut lineage = Vec::new();
    let mut hops = 0usize;
    const MAX_LINEAGE_HOPS: usize = 4_096;
    loop {
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
            validate_authoritative_view_cursor_commit(state, binding, cursor, &latest).await?;
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
        lineage.push(latest);
        latest = read_standing_runtime_checkpoint_record_from_pointer_for_scope(
            state,
            &cursor.producer_tenant_id,
            &cursor.producer_program_id,
            &cursor.producer_view_id,
            &previous,
        )
        .await?;
    }
    lineage.reverse();
    let mut commits = Vec::with_capacity(lineage.len());
    let mut previous_epoch = cursor.output_epoch;
    for record in lineage {
        let commit = authoritative_producer_commit_for_checkpoint(state, binding, &record).await?;
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
/// object-store latest cache otherwise.
async fn latest_checkpoint_record_for_scope(
    state: &ApiState,
    tenant_id: &str,
    program_id: &str,
    view_id: &str,
) -> Result<Option<StandingRuntimeCheckpointRecord>, ApiError> {
    if let Some(meta_store) = &state.meta_store {
        if let Some(pointer) = meta_store
            .read_standing_runtime_checkpoint(tenant_id, program_id, view_id)
            .await
            .map_err(meta_error_to_api)?
        {
            return read_standing_runtime_checkpoint_record_from_pointer_for_scope(
                state, tenant_id, program_id, view_id, &pointer,
            )
            .await
            .map(Some);
        }
        return Ok(None);
    }
    let latest_key = ObjectKey::standing_runtime_latest_checkpoint(tenant_id, program_id, view_id)
        .map_err(ApiError::bad_request)?;
    let bytes = match state
        .store
        .get(&ObjectPath::from(latest_key.as_str()))
        .await
    {
        Ok(object) => object.bytes().await.map_err(ApiError::internal)?,
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(error) => return Err(ApiError::internal(error)),
    };
    let mut record = standing_runtime_checkpoint_record_from_slice(&bytes)
        .map_err(|source| ApiError::bad_request(source.to_string()))?;
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
    let Some(identity) = active_standing_runtime_identity(active) else {
        return Err(ApiError::bad_request(format!(
            "consumer view `{}` has no standing runtime identity",
            active.spec.view_id
        )));
    };
    let (relation_input, next_cursor) =
        relation_input_from_validated_producer_commit(binding, edge_id, cursor, commit)?;
    let runtime = state
        .standing_runtime(identity, &active.spec.view_id)?
        .ok_or_else(|| {
            ApiError::service_unavailable(format!(
                "standing runtime disappeared for consumer view `{}`",
                active.spec.view_id
            ))
        })?;
    let operation_lock = state.standing_runtime_operation_lock(identity, &active.spec.view_id)?;
    let _operation_guard = operation_lock.lock().await;
    let previous_checkpoint =
        read_latest_standing_runtime_checkpoint(state, identity, &active.spec.view_id).await?;
    let owner = state
        .acquire_standing_runtime_owner(identity, &active.spec.view_id)
        .await?;
    let idempotency_key = published_commit_idempotency_key(
        edge_id,
        commit.previous_epoch,
        commit.logical_epoch,
        &commit.commit_digest,
    )
    .map_err(ApiError::bad_request)?;
    let apply_result = match apply_standing_runtime_changes_and_checkpoint(
        Arc::clone(&runtime),
        commit.logical_epoch,
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
    if let Err(error) = persist_standing_runtime_checkpoint(
        state,
        &active.spec.view_id,
        &apply_result.checkpoint,
        &apply_result.output_deltas,
        StandingRuntimeCheckpointPersistContext::new(previous_checkpoint, Vec::new(), owner)
            .with_published_relation(published_relation_binding_for_active_view(active)?)
            .with_direct_view_inputs(vec![StandingRuntimeDirectViewInputV1 {
                published_relation: binding.clone(),
                cursor: next_cursor,
            }]),
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
    state.validate_standing_runtime_fencing_or_evict().await?;
    let active_views = state
        .view_registry()?
        .list_active()
        .await
        .map_err(materialized_view_registry_error_to_api)?;
    let edges = view_dependency_edges_from_active_views(state).await?;
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
        for (edge_id, _producer_program_id, binding, _baseline_cursor) in published_inputs {
            let latest =
                read_latest_standing_runtime_checkpoint(state, identity, &active.spec.view_id)
                    .await?;
            let Some(latest) = latest else {
                // The consumer has no checkpoint yet; its admission-time
                // baseline bootstrap is responsible for initializing state.
                continue;
            };
            // A consumer checkpoint must carry the cursor for every
            // configured published-view edge. Falling back to the
            // admission-time baseline cursor could reapply already-consumed
            // commits, so a missing cursor fails closed.
            let cursor = consumer_edge_cursor(&latest, edge_id)?.ok_or_else(|| {
                ApiError::bad_request(format!(
                    "consumer view `{}` checkpoint is missing the dependency cursor for edge `{edge_id}`",
                    active.spec.view_id
                ))
            })?;
            let commits =
                read_validated_producer_commits_after_cursor(state, binding, &cursor).await?;
            for commit in &commits {
                apply_validated_producer_commit_to_consumer(
                    state, active, binding, edge_id, &cursor, commit,
                )
                .await?;
                summary.applied_commits += 1;
            }
            if !commits.is_empty() {
                summary.updated_views += 1;
            }
        }
    }
    Ok(summary)
}

/// Applies the producer's baseline published snapshot to a newly created
/// consumer, publishes its baseline checkpoint with the admission-time cursor,
/// then catches the consumer up to the producer's current head.
pub(super) async fn bootstrap_consumer_from_published_snapshot(
    state: &ApiState,
    active: &ActiveMaterializedView,
    input: &ConsumerViewDependencyInputV1,
) -> Result<(), ApiError> {
    let Some(identity) = active_standing_runtime_identity(active) else {
        return Err(ApiError::bad_request(format!(
            "consumer view `{}` has no standing runtime identity",
            active.spec.view_id
        )));
    };
    let Some(runtime) = state.standing_runtime(identity, &active.spec.view_id)? else {
        return Err(ApiError::service_unavailable(format!(
            "standing runtime is unavailable for consumer view `{}`",
            active.spec.view_id
        )));
    };
    let record =
        producer_checkpoint_record_at_epoch(state, &input.binding, &input.bootstrap_cursor).await?;
    let snapshot =
        standing_runtime_checkpoint_published_output(&record.checkpoint).ok_or_else(|| {
            ApiError::bad_request(format!(
                "producer view `{}` epoch {} has no published output snapshot",
                input.binding.producer_view_id, input.bootstrap_cursor.output_epoch
            ))
        })?;
    let snapshot = serde_json::from_value::<DeltaBatch>(snapshot).map_err(|error| {
        ApiError::bad_request(format!(
            "producer view `{}` epoch {} published output is malformed: {error}",
            input.binding.producer_view_id, input.bootstrap_cursor.output_epoch
        ))
    })?;
    let snapshot_rows = snapshot
        .net_rows()
        .map_err(|_| ApiError::bad_request("producer snapshot is malformed"))?;
    if snapshot_rows.iter().any(|row| row.weight != 1) {
        return Err(ApiError::bad_request(format!(
            "producer view `{}` epoch {} snapshot contains non-unit weights",
            input.binding.producer_view_id, input.bootstrap_cursor.output_epoch
        )));
    }
    let batch = published_delta_to_record_batch(&input.binding, &snapshot)?;
    let relation_input = RelationInputBatch {
        relation_id: input.binding.relation.relation_id.clone(),
        relation_version: input.binding.relation.relation_version.clone(),
        stream_id: input.binding.output_stream_id.clone(),
        partition_id: 0,
        schema_fingerprint: input.binding.relation.schema_fingerprint.clone(),
        start_offset_inclusive: 0,
        end_offset_exclusive: input.bootstrap_cursor.output_epoch,
        event_time_watermark: None,
        encoding: RelationInputEncodingV1::PublishedRelationDeltaV1 {
            delta_codec_identity: input.binding.delta_codec_identity.clone(),
            output_schema_hash: input.binding.output_schema_hash.clone(),
            weight_field_name: PUBLISHED_DELTA_WEIGHT_FIELD_V1.to_string(),
            weight_field_index: input.binding.relation.columns.len(),
        },
        batches: vec![batch],
    };
    let operation_lock = state.standing_runtime_operation_lock(identity, &active.spec.view_id)?;
    let _operation_guard = operation_lock.lock().await;
    let owner = state
        .acquire_standing_runtime_owner(identity, &active.spec.view_id)
        .await?;
    let idempotency_key = published_commit_idempotency_key(
        &input.edge.edge_id,
        0,
        input.bootstrap_cursor.output_epoch,
        &input.bootstrap_cursor.commit_digest,
    )
    .map_err(ApiError::bad_request)?;
    let apply_result = match apply_standing_runtime_changes_and_checkpoint(
        Arc::clone(&runtime),
        input.bootstrap_cursor.output_epoch,
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
    if let Err(error) = persist_standing_runtime_checkpoint(
        state,
        &active.spec.view_id,
        &apply_result.checkpoint,
        &apply_result.output_deltas,
        StandingRuntimeCheckpointPersistContext::new(None, Vec::new(), owner)
            .with_published_relation(published_relation_binding_for_active_view(active)?)
            .with_direct_view_inputs(vec![StandingRuntimeDirectViewInputV1 {
                published_relation: input.binding.clone(),
                cursor: input.bootstrap_cursor.clone(),
            }]),
        None,
    )
    .await
    {
        remove_standing_runtime(state, identity, &active.spec.view_id)?;
        return Err(error);
    }
    Ok(())
}

/// Reads the producer checkpoint record at the cursor epoch by walking the
/// authoritative pointer lineage from the head, and validates the baseline
/// record against the cursor's authoritative commit before returning it.
pub(super) async fn producer_checkpoint_record_at_epoch(
    state: &ApiState,
    binding: &PublishedRelationBindingV1,
    cursor: &CausalViewCursorV1,
) -> Result<StandingRuntimeCheckpointRecord, ApiError> {
    let mut record = latest_checkpoint_record_for_scope(
        state,
        &cursor.producer_tenant_id,
        &cursor.producer_program_id,
        &cursor.producer_view_id,
    )
    .await?
    .ok_or_else(|| {
        ApiError::bad_request(format!(
            "producer view `{}` has no authoritative checkpoint",
            cursor.producer_view_id
        ))
    })?;
    let mut hops = 0usize;
    const MAX_LINEAGE_HOPS: usize = 4_096;
    loop {
        hops += 1;
        if hops > MAX_LINEAGE_HOPS {
            return Err(ApiError::bad_request(format!(
                "producer view `{}` exceeds the authoritative checkpoint lineage budget",
                cursor.producer_view_id
            )));
        }
        if record.checkpoint.logical_epoch < cursor.output_epoch {
            return Err(ApiError::bad_request(format!(
                "producer view `{}` baseline epoch is ahead of the authoritative frontier",
                cursor.producer_view_id
            )));
        }
        if record.checkpoint.logical_epoch == cursor.output_epoch {
            validate_authoritative_view_cursor_commit(state, binding, cursor, &record).await?;
            return Ok(record);
        }
        let previous = record.previous_checkpoint.clone().ok_or_else(|| {
            ApiError::bad_request(format!(
                "producer view `{}` baseline epoch is missing from the authoritative checkpoint lineage",
                cursor.producer_view_id
            ))
        })?;
        if previous.logical_epoch >= record.checkpoint.logical_epoch
            || previous.logical_epoch < cursor.output_epoch
        {
            return Err(ApiError::bad_request(format!(
                "producer view `{}` has a non-contiguous authoritative checkpoint lineage",
                cursor.producer_view_id
            )));
        }
        record = read_standing_runtime_checkpoint_record_from_pointer_for_scope(
            state,
            &cursor.producer_tenant_id,
            &cursor.producer_program_id,
            &cursor.producer_view_id,
            &previous,
        )
        .await?;
    }
}
