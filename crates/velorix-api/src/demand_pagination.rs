//! Continuations over a certified, immutable materialized query result.
//! Mount as a child of query_serving. The caller executes its existing bounded
//! materialized query against PreparedPage::record, never against latest head.
use super::*;
use sha2::{Digest, Sha256};

const PREFIX: &str = "demand-page-v1:";
const MAX_TOKEN_BYTES: usize = 32 * 1024;
const MAX_LINEAGE_HOPS: usize = 1024;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    checkpoint_key: String,
    checkpoint_hash: String,
    epoch: u64,
    spec_hash: String,
    output_id: String,
    output_schema_hash: String,
    scope_hash: String,
    /// Canonical scope predicate certified for this continuation. The signed
    /// cursor carries it so a token-only follow-up request replays the exact
    /// certified scope instead of re-inferring (and losing) it.
    #[serde(default)]
    scope_sql: Option<String>,
    query_hash: String,
    offset: usize,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    cursor: Cursor,
    mac: Vec<u8>,
}

pub(super) struct PreparedPage {
    pub(super) record: StandingRuntimeCheckpointRecord,
    cursor: Cursor,
    limit: usize,
}

impl PreparedPage {
    pub(super) fn row_range(&self) -> (usize, usize) {
        (self.cursor.offset, self.limit)
    }
}

/// Read only output bound to the supplied authenticated checkpoint. Native
/// predicates prune conservatively; the caller must still execute exact scope
/// SQL before serving rows. The legacy codec has one complete bounded page.
pub(super) async fn read_output_record(
    state: &ApiState,
    active: &ActiveMaterializedView,
    record: &StandingRuntimeCheckpointRecord,
    output_id: &str,
    predicate: Option<&velorix_core::query::PagePredicate>,
    row_range: Option<(usize, usize)>,
    policy: QueryPolicy,
) -> Result<(MaterializedViewPage, bool), ApiError> {
    if let Some(page) = materialized_output_pages::query_filtered_record(
        state, active, record, output_id, predicate, row_range, policy,
    )
    .await?
    {
        return Ok((page, true));
    }
    let read = read_legacy_output_record(state, active, record, output_id, policy);
    let page = match policy.execution_timeout_ms {
        Some(ms) => tokio::time::timeout(std::time::Duration::from_millis(ms), read)
            .await
            .map_err(|_| ApiError::bad_request("pinned output read exceeded execution timeout"))?,
        None => read.await,
    }?;
    Ok((page, false))
}

async fn read_legacy_output_record(
    state: &ApiState,
    active: &ActiveMaterializedView,
    record: &StandingRuntimeCheckpointRecord,
    output_id: &str,
    policy: QueryPolicy,
) -> Result<MaterializedViewPage, ApiError> {
    let identity = active_standing_runtime_identity(active).ok_or_else(invalid)?;
    if record.checkpoint.identity != *identity
        || record.view_id != active.spec.view_id
        || record.manifest_hash.is_empty()
    {
        return Err(invalid());
    }
    let schema = active
        .spec
        .output_relations
        .iter()
        .find(|schema| schema.relation_id == output_id)
        .ok_or_else(invalid)?;
    let execution = &active
        .runtime
        .as_ref()
        .and_then(|binding| binding.logical_plan.as_ref())
        .ok_or_else(invalid)?
        .execution;
    let (aggregates, bag) = match execution {
        VelorixLogicalViewExecutionV1::SingleKeySumCount { plan } => {
            (Some(supported_view_plan_aggregate_outputs(plan)), false)
        }
        VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { plan } => (
            Some(supported_join_view_plan_aggregate_outputs(plan)),
            false,
        ),
        VelorixLogicalViewExecutionV1::TumblingEventTimeAggregate { plan } => {
            (Some(plan.aggregate_outputs.clone()), false)
        }
        VelorixLogicalViewExecutionV1::FilterProject { .. }
        | VelorixLogicalViewExecutionV1::LatestByKey { .. }
        | VelorixLogicalViewExecutionV1::AnalyticRowNumber { .. }
        | VelorixLogicalViewExecutionV1::ScalarAggregateFilter { .. }
        | VelorixLogicalViewExecutionV1::AnalyticWindowFrames { .. } => (None, false),
        VelorixLogicalViewExecutionV1::OrderedWindows { plan } if plan.schema_version == 1 => {
            (None, false)
        }
        VelorixLogicalViewExecutionV1::OrderedWindows { plan }
            if matches!(plan.schema_version, 2..=3) =>
        {
            (None, true)
        }
        _ => {
            return Err(ApiError::bad_request(
                "pinned persisted output reader does not support this plan",
            ))
        }
    };
    // One manifest and one output page; never fall back to state hydration.
    if policy.max_object_requests.is_some_and(|max| max < 2)
        || policy.max_scan_files.is_some_and(|max| max < 2)
    {
        return Err(invalid());
    }
    let budget = policy
        .max_scan_bytes
        .unwrap_or(u64::MAX)
        .min(policy.memory_limit_bytes.unwrap_or(u64::MAX) / 16)
        .min(state.runtime_resource_limits().max_snapshot_bytes as u64);
    let manifest_ref = persisted_manifest_ref(&record.checkpoint.output_manifest_refs, output_id)?;
    let path = manifest_ref
        .strip_prefix(STANDING_RUNTIME_OUTPUT_MANIFEST_REF_PREFIX)
        .ok_or_else(invalid)?;
    let bytes = checkpoint_publication::read_object_bytes_bounded(
        state,
        &ObjectPath::from(path),
        Some(budget),
    )
    .await?;
    let used = bytes.len() as u64;
    let manifest: StandingRuntimeOutputManifestRecord =
        serde_json::from_slice(&bytes).map_err(ApiError::bad_request)?;
    let (key, _) = ObjectKey::parse_standing_runtime_output_manifest(path.to_string())
        .map_err(ApiError::bad_request)?;
    checkpoint_publication::validate_standing_runtime_output_manifest_record(&key, &manifest)?;
    if manifest.tenant_id != identity.tenant_id
        || manifest.program_id != identity.program_id
        || manifest.view_id != output_id
        || manifest.checkpoint_key != record.checkpoint_key
        || manifest.logical_epoch != record.checkpoint.logical_epoch
        || manifest.checkpoint_content_hash != record.checkpoint.state_root.content_hash
        || materialized_output_pages::is_bag_codec(&manifest.output_encoding)
        || manifest.pages.len() != 1
        || manifest.pages[0].page_index != 0
    {
        return Err(invalid());
    }
    drop(bytes);
    let (_, page, _) = checkpoint_publication::read_standing_runtime_output_page_record_bounded(
        state,
        &manifest.pages[0],
        output_id,
        Some(budget.saturating_sub(used)),
    )
    .await?;
    if page.tenant_id != manifest.tenant_id
        || page.program_id != manifest.program_id
        || page.view_id != manifest.view_id
        || page.logical_epoch != manifest.logical_epoch
        || page.output_content_hash != manifest.output_content_hash
        || page.published_output != manifest.published_output
    {
        return Err(invalid());
    }
    let delta: DeltaBatch =
        serde_json::from_value(page.published_output).map_err(ApiError::bad_request)?;
    let rows = delta.net_rows().map_err(ApiError::bad_request)?;
    let total = rows.iter().try_fold(0usize, |total, row| {
        let count = usize::try_from(row.weight)
            .ok()
            .filter(|count| *count > 0)
            .ok_or_else(invalid)?;
        total.checked_add(count).ok_or_else(invalid)
    })?;
    if policy.max_output_rows.is_some_and(|limit| total > limit) {
        return Err(ApiError::bad_request(
            "pinned materialized snapshot exceeds bounded query row limit",
        ));
    }
    let view = ScopedViewId {
        tenant_id: identity.tenant_id.clone(),
        program_id: identity.program_id.clone(),
        view_id: output_id.into(),
    };
    let result = if bag {
        let mut batches = Vec::new();
        for row in rows {
            let mut remaining = row.weight;
            while remaining > 0 {
                let mut piece = row.clone();
                piece.weight = remaining.min(512);
                remaining -= piece.weight;
                batches.push(
                    velorix_runtime::materialized_view_runtime::materialized_bag_canonical_batch(
                        schema,
                        &DeltaBatch::from_records(vec![piece]),
                    )
                    .map_err(ApiError::bad_request)?,
                );
            }
        }
        if batches.is_empty() {
            batches.push(RecordBatch::new_empty(
                arrow_schema_from_incremental_relation_schema(schema)?,
            ));
        }
        MaterializedViewPage {
            view,
            logical_epoch: record.checkpoint.logical_epoch,
            schema_fingerprint: schema.schema_fingerprint.clone(),
            batches,
            next_page_token: None,
        }
    } else {
        // Decode persisted output using the admitted layout, without loading
        // accumulator state or recomputing any source rows.
        let mut decoding_checkpoint = record.checkpoint.clone();
        decoding_checkpoint.state_payload = match execution {
            VelorixLogicalViewExecutionV1::TumblingEventTimeAggregate { .. } => Some(
                velorix_core::standing_program::RuntimeCheckpointStatePayload {
                    codec_identity: String::new(),
                    payload: serde_json::json!({"runtime_kind":"tumbling_event_time_aggregate"})
                        .to_string(),
                },
            ),
            VelorixLogicalViewExecutionV1::OrderedWindows { plan } => Some(
                velorix_core::standing_program::RuntimeCheckpointStatePayload {
                    codec_identity: String::new(),
                    payload: serde_json::json!({"runtime_kind":"ordered_window","plan":plan})
                        .to_string(),
                },
            ),
            _ => None,
        };
        velorix_runtime::materialized_view_runtime::materialized_delta_to_page_with_checkpoint(
            schema,
            &delta,
            view,
            record.checkpoint.logical_epoch,
            SnapshotPageRequest {
                committed_epoch: Some(record.checkpoint.logical_epoch),
                page_token: None,
                max_rows: Some(total.max(1)),
            },
            aggregates.as_deref(),
            &decoding_checkpoint,
        )
        .map_err(ApiError::bad_request)?
    };
    validate_standing_runtime_full_snapshot_page(
        active,
        output_id,
        &result,
        Some(record.checkpoint.logical_epoch),
    )?;
    let memory = result.batches.iter().try_fold(0u64, |total, batch| {
        total
            .checked_add(batch.get_array_memory_size() as u64)
            .ok_or_else(invalid)
    })?;
    if policy
        .memory_limit_bytes
        .is_some_and(|limit| memory > limit)
        || policy.max_output_bytes.is_some_and(|limit| memory > limit)
    {
        return Err(ApiError::bad_request(
            "pinned materialized snapshot exceeds query byte limit",
        ));
    }
    Ok(result)
}

fn persisted_manifest_ref<'a>(
    references: &'a [String],
    output_id: &str,
) -> Result<&'a str, ApiError> {
    references
        .iter()
        .find(|reference| {
            reference
                .strip_prefix(STANDING_RUNTIME_OUTPUT_MANIFEST_REF_PREFIX)
                .and_then(|key| {
                    ObjectKey::parse_standing_runtime_output_manifest(key.to_string()).ok()
                })
                .is_some_and(|(_, parts)| parts.view_id == output_id)
        })
        .map(String::as_str)
        .ok_or_else(|| {
            ApiError::service_unavailable(
                "certified checkpoint has no persisted output manifest for the requested output",
            )
        })
}

fn invalid() -> ApiError {
    ApiError::bad_request("invalid or expired certified demand continuation")
}

// shortcut: the MAC key is process-local: restart and cross-instance routing
// reject existing tokens. This does not invalidate the durable materialized
// cache or prevent a new query. Shared token keys are deliberately deferred.
fn signing_key() -> Result<&'static [u8; 32], ApiError> {
    static KEY: std::sync::OnceLock<Result<[u8; 32], ()>> = std::sync::OnceLock::new();
    KEY.get_or_init(|| {
        let mut key = [0u8; 32];
        rustls::crypto::ring::default_provider()
            .secure_random
            .fill(&mut key)
            .map_err(|_| ())?;
        Ok(key)
    })
    .as_ref()
    .map_err(|_| ApiError::internal("continuation signing entropy unavailable"))
}

fn mac(bytes: &[u8]) -> Result<Vec<u8>, ApiError> {
    Ok(mac_with_key(signing_key()?, bytes))
}

// RFC 2104 HMAC-SHA256 for keys no longer than its 64-byte block. Production
// uses exactly 32 random bytes. sha2 is an existing API dependency; ring/hmac
// is not a direct dependency here. RFC 4231 vectors below verify this small
// construction independently of the token round-trip tests.
fn mac_with_key(key: &[u8], bytes: &[u8]) -> Vec<u8> {
    assert!(key.len() <= 64);
    let mut inner = [0x36u8; 64];
    let mut outer = [0x5cu8; 64];
    for (i, byte) in key.iter().enumerate() {
        inner[i] ^= byte;
        outer[i] ^= byte;
    }
    let mut hash = Sha256::new();
    hash.update(inner);
    hash.update(bytes);
    let digest = hash.finalize();
    let mut hash = Sha256::new();
    hash.update(outer);
    hash.update(digest);
    hash.finalize().to_vec()
}

pub(super) fn is_continuation(token: &str) -> bool {
    token.starts_with(PREFIX)
}

/// Recover the scope predicate certified by a signed continuation token.
///
/// A continuation request carries only the token, so the caller cannot repeat
/// the original `materialization.scope` predicate. The signed cursor is the
/// authority for that predicate: the server replays it instead of
/// re-inferring one, which would otherwise widen the certified result.
pub(super) fn continuation_scope_sql(token: &str) -> Result<Option<String>, ApiError> {
    Ok(decode(token)?.scope_sql)
}

fn decode(token: &str) -> Result<Cursor, ApiError> {
    decode_with_key(token, signing_key()?)
}

fn decode_with_key(token: &str, key: &[u8; 32]) -> Result<Cursor, ApiError> {
    if token.len() > MAX_TOKEN_BYTES {
        return Err(invalid());
    }
    let envelope: Envelope = serde_json::from_str(token.strip_prefix(PREFIX).ok_or_else(invalid)?)
        .map_err(|_| invalid())?;
    let bytes = serde_json::to_vec(&envelope.cursor).map_err(ApiError::internal)?;
    let expected = mac_with_key(key, &bytes);
    if envelope.mac.len() != expected.len()
        || envelope
            .mac
            .iter()
            .zip(&expected)
            .fold(0u8, |diff, (a, b)| diff | (a ^ b))
            != 0
    {
        return Err(invalid());
    }
    Ok(envelope.cursor)
}

fn encode_token(cursor: &Cursor) -> Result<String, ApiError> {
    let bytes = serde_json::to_vec(cursor).map_err(ApiError::internal)?;
    let envelope = Envelope {
        cursor: cursor.clone(),
        mac: mac(&bytes)?,
    };
    let token = format!(
        "{PREFIX}{}",
        serde_json::to_string(&envelope).map_err(ApiError::internal)?
    );
    if token.len() > MAX_TOKEN_BYTES {
        return Err(ApiError::bad_request(
            "certified pagination scope is too large to paginate",
        ));
    }
    Ok(token)
}

fn ensure_token_fits(cursor: &Cursor) -> Result<(), ApiError> {
    // Future offsets cannot exceed usize::MAX. Check that largest serialized
    // offset before the caller executes the query that may return a token.
    let mut largest_cursor = cursor.clone();
    largest_cursor.offset = usize::MAX;
    encode_token(&largest_cursor).map(|_| ())
}

/// Resolve an authenticated historical checkpoint through its immutable
/// predecessor chain. This validates headers only; the query executor must
/// retain all existing payload/schema/policy validation when reading output.
pub(super) async fn prepare_page(
    state: &ApiState,
    active: &ActiveMaterializedView,
    output_id: &str,
    request: &QueryViewRequest,
    scope_sql: Option<&str>,
    first_record: Option<StandingRuntimeCheckpointRecord>,
    policy: QueryPolicy,
) -> Result<PreparedPage, ApiError> {
    let identity = active_standing_runtime_identity(active).ok_or_else(invalid)?;
    let schema = active
        .spec
        .output_relations
        .iter()
        .find(|schema| schema.relation_id == output_id)
        .ok_or_else(invalid)?;
    let scope_hash = demand_materialization::scope_hash(scope_sql)?;
    let query_hash = stable_bytes_hash(
        &serde_json::to_vec(&(
            &request.sql,
            &request.parameters,
            &identity.tenant_id,
            &identity.program_id,
            state.materialized_read_cache_store_id(),
            state.materialized_read_cache_namespace(),
        ))
        .map_err(ApiError::internal)?,
    );
    let limit = request
        .max_rows
        .unwrap_or(1000)
        .min(policy.max_output_rows.unwrap_or(usize::MAX));
    if limit == 0 {
        return Err(ApiError::bad_request("page max_rows must be positive"));
    }
    let (record, cursor) = if let Some(token) = request.page_token.as_deref() {
        let cursor = decode(token)?;
        // The signed cursor is the only authority for the certified scope on a
        // continuation. A caller-supplied scope that disagrees with the
        // certified one must not silently retarget the pinned result.
        if scope_sql.is_some() && scope_sql != cursor.scope_sql.as_deref() {
            return Err(invalid());
        }
        if cursor.spec_hash != active.spec_hash
            || cursor.output_id != output_id
            || cursor.output_schema_hash != schema.schema_fingerprint
            || cursor.scope_hash != scope_hash
            || cursor.query_hash != query_hash
            || request.epoch.is_some_and(|epoch| epoch != cursor.epoch)
        {
            return Err(invalid());
        }
        let mut record =
            read_latest_standing_runtime_checkpoint_header(state, identity, &active.spec.view_id)
                .await?
                .ok_or_else(invalid)?;
        let mut hops = 0;
        while record.checkpoint_key != cursor.checkpoint_key {
            if hops >= MAX_LINEAGE_HOPS || record.checkpoint.logical_epoch <= cursor.epoch {
                return Err(invalid());
            }
            let previous = record.previous_checkpoint.as_ref().ok_or_else(invalid)?;
            if previous.logical_epoch >= record.checkpoint.logical_epoch {
                return Err(invalid());
            }
            record = checkpoint_publication::read_standing_runtime_checkpoint_header_from_pointer(
                state,
                &identity.tenant_id,
                &identity.program_id,
                &active.spec.view_id,
                previous,
            )
            .await?;
            hops += 1;
        }
        if record.manifest_hash != cursor.checkpoint_hash
            || record.checkpoint.logical_epoch != cursor.epoch
        {
            return Err(invalid());
        }
        (record, cursor)
    } else {
        let record = first_record.ok_or_else(invalid)?;
        if request
            .epoch
            .is_some_and(|epoch| epoch != record.checkpoint.logical_epoch)
        {
            return Err(invalid());
        }
        let cursor = Cursor {
            checkpoint_key: record.checkpoint_key.clone(),
            checkpoint_hash: record.manifest_hash.clone(),
            epoch: record.checkpoint.logical_epoch,
            spec_hash: active.spec_hash.clone(),
            output_id: output_id.into(),
            output_schema_hash: schema.schema_fingerprint.clone(),
            scope_hash,
            scope_sql: scope_sql.map(str::to_owned),
            query_hash,
            offset: 0,
        };
        (record, cursor)
    };
    let progress = record.demand_materialization.as_ref().ok_or_else(invalid)?;
    progress.validate_checkpoint(&record.checkpoint)?;
    let plan_hash = active
        .runtime
        .as_ref()
        .and_then(|binding| binding.logical_plan.as_ref())
        .and_then(|plan| plan.plan_hash.as_ref())
        .ok_or_else(invalid)?;
    if record.checkpoint.identity != *identity
        || record.manifest_hash.is_empty()
        || progress.spec_hash != active.spec_hash
        || &progress.plan_hash != plan_hash
        || !progress
            .certificates
            .iter()
            .any(|certificate| certificate.scope_hash == cursor.scope_hash)
    {
        return Err(invalid());
    }
    ensure_token_fits(&cursor)?;
    Ok(PreparedPage {
        record,
        cursor,
        limit,
    })
}

/// Slice only an already policy-validated, scope-filtered result. The caller
/// keeps its existing query permit and response byte/memory limits unchanged.
pub(super) fn finish_page(
    mut prepared: PreparedPage,
    mut result: ViewQueryBatches,
) -> Result<ViewQueryBatches, ApiError> {
    if result.response.logical_epoch != prepared.cursor.epoch
        || result.response.next_page_token.is_some()
    {
        return Err(ApiError::conflict(
            "certified pagination requires the complete pinned bounded result",
        ));
    }
    let total = result
        .response
        .batches
        .iter()
        .try_fold(0usize, |sum, batch| {
            sum.checked_add(batch.num_rows()).ok_or_else(invalid)
        })?;
    if prepared.cursor.offset > total {
        return Err(invalid());
    }
    let end = prepared
        .cursor
        .offset
        .saturating_add(prepared.limit)
        .min(total);
    let mut base = 0usize;
    let mut batches = Vec::new();
    for batch in &result.response.batches {
        let next = base.checked_add(batch.num_rows()).ok_or_else(invalid)?;
        let lo = base.max(prepared.cursor.offset);
        let hi = next.min(end);
        if lo < hi {
            batches.push(batch.slice(lo - base, hi - lo));
        }
        base = next;
    }
    if batches.is_empty() {
        batches.push(RecordBatch::new_empty(result.response.schema.clone()));
    }
    result.response.batches = batches;
    if end < total {
        prepared.cursor.offset = end;
        result.response.next_page_token = Some(encode_token(&prepared.cursor)?);
    }
    Ok(result)
}

pub(super) fn finish_native_page(
    mut prepared: PreparedPage,
    mut result: ViewQueryBatches,
    has_more: bool,
) -> Result<ViewQueryBatches, ApiError> {
    if result.response.logical_epoch != prepared.cursor.epoch
        || result.response.next_page_token.is_some()
    {
        return Err(ApiError::conflict(
            "certified native pagination requires the pinned checkpoint",
        ));
    }
    let rows = result
        .response
        .batches
        .iter()
        .try_fold(0usize, |sum, batch| {
            sum.checked_add(batch.num_rows()).ok_or_else(invalid)
        })?;
    if rows > prepared.limit {
        return Err(ApiError::conflict(
            "native page reader exceeded the certified row limit",
        ));
    }
    if has_more {
        prepared.cursor.offset = prepared
            .cursor
            .offset
            .checked_add(rows)
            .ok_or_else(invalid)?;
        result.response.next_page_token = Some(encode_token(&prepared.cursor)?);
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_output_manifest_is_not_a_token_error() {
        let error = persisted_manifest_ref(&[], "output").unwrap_err();
        assert_eq!(error.status, StatusCode::SERVICE_UNAVAILABLE);
        let key = ObjectKey::standing_runtime_output_manifest(
            "tenant",
            "program",
            "output",
            7,
            &stable_bytes_hash(b"output"),
        )
        .unwrap();
        let refs = vec![format!(
            "{STANDING_RUNTIME_OUTPUT_MANIFEST_REF_PREFIX}{}",
            key.as_str()
        )];
        assert_eq!(persisted_manifest_ref(&refs, "output").unwrap(), refs[0]);
        assert!(persisted_manifest_ref(&refs, "another-output").is_err());
    }

    #[test]
    fn hmac_sha256_matches_rfc_4231_vectors() {
        for (key, message, expected) in [
            (
                vec![0x0b; 20],
                b"Hi There".as_slice(),
                "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7",
            ),
            (
                b"Jefe".to_vec(),
                b"what do ya want for nothing?".as_slice(),
                "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843",
            ),
        ] {
            let actual: String = mac_with_key(&key, message)
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect();
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn continuation_authenticates_checkpoint_scope_query_and_offset() {
        let cursor = Cursor {
            checkpoint_key: "checkpoint/7".into(),
            checkpoint_hash: "digest".into(),
            epoch: 7,
            spec_hash: "spec".into(),
            output_id: "output".into(),
            output_schema_hash: "schema".into(),
            scope_hash: "scope".into(),
            scope_sql: Some("user_id >= 'k2'".into()),
            query_hash: "query-and-bindings".into(),
            offset: 1,
        };
        let bytes = serde_json::to_vec(&cursor).unwrap();
        let signature = mac(&bytes).unwrap();
        let token = format!(
            "{PREFIX}{}",
            serde_json::to_string(&Envelope {
                cursor: cursor.clone(),
                mac: signature.clone(),
            })
            .unwrap()
        );
        assert_eq!(decode(&token).unwrap().offset, 1);
        // Simulate another instance or restart without mutating the global key.
        let mut lost_key = *signing_key().unwrap();
        lost_key[0] ^= 1;
        assert!(decode_with_key(&token, &lost_key).is_err());
        for field in [
            "checkpoint_key",
            "checkpoint_hash",
            "scope_hash",
            "scope_sql",
            "query_hash",
            "output_id",
            "output_schema_hash",
            "spec_hash",
            "epoch",
            "offset",
        ] {
            let mut changed = serde_json::to_value(&cursor).unwrap();
            changed[field] = if field == "offset" || field == "epoch" {
                serde_json::json!(2)
            } else {
                serde_json::json!("changed")
            };
            let token = format!(
                "{PREFIX}{}",
                serde_json::to_string(&Envelope {
                    cursor: serde_json::from_value(changed).unwrap(),
                    mac: signature.clone(),
                })
                .unwrap()
            );
            assert!(decode(&token).is_err(), "tampering {field} must fail");
        }
    }

    fn test_cursor(scope_sql: Option<String>) -> Cursor {
        Cursor {
            checkpoint_key: "checkpoint/7".into(),
            checkpoint_hash: "digest".into(),
            epoch: 7,
            spec_hash: "spec".into(),
            output_id: "output".into(),
            output_schema_hash: "schema".into(),
            scope_hash: "scope".into(),
            scope_sql,
            query_hash: "query-and-bindings".into(),
            offset: 0,
        }
    }

    #[test]
    fn oversized_scope_is_rejected_and_normal_pages_keep_decoding() {
        let oversized = test_cursor(Some("\\\n".repeat(MAX_TOKEN_BYTES)));
        let error = ensure_token_fits(&oversized).unwrap_err();
        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert!(error.message.contains("scope is too large to paginate"));

        let mut cursor = test_cursor(Some("user_id >= 'k2'".into()));
        ensure_token_fits(&cursor).unwrap();
        let mut next_offset = 0;
        for expected_offset in [3, 6] {
            cursor.offset = next_offset + 3;
            let token = encode_token(&cursor).unwrap();
            assert!(token.len() <= MAX_TOKEN_BYTES);
            let decoded = decode(&token).unwrap();
            assert_eq!(decoded.offset, expected_offset);
            assert_eq!(decoded.scope_sql.as_deref(), Some("user_id >= 'k2'"));
            next_offset = decoded.offset;
        }
    }
}
