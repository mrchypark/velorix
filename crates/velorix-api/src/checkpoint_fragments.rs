use super::*;
use futures::StreamExt;

const MANIFEST_KIND: &str = "standing_runtime_state_fragments_v2";

pub(super) fn durable_state_object_count(payload_bytes: usize, limit: usize) -> usize {
    if payload_bytes > limit {
        payload_bytes.div_ceil(limit) + 1
    } else {
        1
    }
}

#[derive(Deserialize)]
struct KindTag {
    record_kind: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct FragmentRef {
    content_hash: String,
    byte_count: usize,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct FragmentManifest {
    schema_version: u16,
    record_kind: String,
    byte_count: usize,
    state_bytes: usize,
    header: StandingRuntimeStatePayloadRecord,
    fragments: Vec<FragmentRef>,
    fragment_bytes: usize,
}

fn fragment_path(root: &ObjectKey, ordinal: usize, hash: &str) -> ObjectPath {
    ObjectPath::from(format!("{}.fragment-{ordinal}-{hash}", root.as_str()))
}

/// Validate all manifest structure and declared lengths before allocating its
/// fragment buffer. Returns the exact payload byte count for a fragment root.
pub(super) fn validate_manifest(
    state: &ApiState,
    key: &ObjectKey,
    bytes: &[u8],
) -> Result<Option<usize>, ApiError> {
    if bytes.starts_with(&[255; 4]) {
        return Ok(None);
    }
    let tag: KindTag = serde_json::from_slice(bytes).map_err(ApiError::bad_request)?;
    if tag.record_kind != MANIFEST_KIND {
        return Ok(None);
    }
    if bytes.len() > state.max_standing_runtime_state_fragment_bytes {
        return Err(ApiError::bad_request(
            "checkpoint manifest exceeds configured fragment byte limit",
        ));
    }
    let manifest: FragmentManifest =
        serde_json::from_slice(bytes).map_err(ApiError::bad_request)?;
    if manifest.schema_version != 2
        || manifest.state_bytes > state.max_standing_runtime_state_payload_bytes
        || manifest.state_bytes == 0
        || manifest.byte_count
            > checkpoint_arrow::encoded_limit(state.max_standing_runtime_state_payload_bytes)
        || manifest.byte_count == 0
        || !manifest.header.payload.payload.is_empty()
        || manifest.fragments.is_empty()
        || manifest.fragment_bytes == 0
        || manifest.fragment_bytes > state.max_standing_runtime_state_fragment_bytes
        || manifest.fragments.len() != manifest.byte_count.div_ceil(manifest.fragment_bytes)
    {
        return Err(ApiError::bad_request(
            "invalid checkpoint fragment manifest",
        ));
    }
    let (_, parts) = ObjectKey::parse_standing_runtime_state_payload(key.as_str().to_string())
        .map_err(ApiError::bad_request)?;
    if parts.tenant_id != manifest.header.tenant_id
        || parts.program_id != manifest.header.program_id
        || parts.view_id != manifest.header.view_id
        || parts.logical_epoch != manifest.header.logical_epoch
        || parts.state_content_hash != manifest.header.state_content_hash
    {
        return Err(ApiError::bad_request(
            "checkpoint fragment identity mismatch",
        ));
    }
    let mut sum = 0usize;
    for (ordinal, fragment) in manifest.fragments.iter().enumerate() {
        let expected = manifest
            .fragment_bytes
            .min(manifest.byte_count - ordinal * manifest.fragment_bytes);
        if fragment.byte_count != expected || !is_sha256_identity(&fragment.content_hash) {
            return Err(ApiError::bad_request(
                "invalid checkpoint fragment descriptor",
            ));
        }
        sum = sum
            .checked_add(fragment.byte_count)
            .ok_or_else(|| ApiError::bad_request("checkpoint fragment byte count overflow"))?;
    }
    if sum != manifest.byte_count {
        return Err(ApiError::bad_request(
            "checkpoint fragment lengths do not match manifest total",
        ));
    }
    Ok(Some(sum))
}

/// Read one immutable fragment with an enforced upper bound before and during
/// streaming. The returned allocation can never exceed `max_bytes`.
pub(super) async fn read_bounded(
    state: &ApiState,
    path: &ObjectPath,
    max_bytes: usize,
) -> Result<Vec<u8>, ApiError> {
    if state.materialized_read_cache.is_some() {
        let bytes =
            checkpoint_publication::read_object_bytes_bounded(state, path, Some(max_bytes as u64))
                .await?;
        return Ok(bytes);
    }
    let size = state
        .store
        .head(path)
        .await
        .map_err(ApiError::internal)?
        .size;
    if size > max_bytes as u64 {
        return Err(ApiError::bad_request(
            "checkpoint fragment exceeds its declared byte limit",
        ));
    }
    let mut stream = state
        .store
        .get(path)
        .await
        .map_err(ApiError::internal)?
        .into_stream();
    let mut bytes = Vec::with_capacity(size as usize);
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(ApiError::internal)?;
        if chunk.len() > max_bytes.saturating_sub(bytes.len()) {
            return Err(ApiError::bad_request(
                "checkpoint fragment stream exceeds its declared byte limit",
            ));
        }
        bytes.extend_from_slice(&chunk);
    }
    if bytes.len() as u64 != size {
        return Err(ApiError::bad_request(
            "checkpoint fragment stream length changed",
        ));
    }
    Ok(bytes)
}

/// Verify fragment objects and return their actual, bounded byte total.
pub(super) async fn verify_manifest_fragments(
    state: &ApiState,
    key: &ObjectKey,
    bytes: &[u8],
    max_bytes: usize,
) -> Result<Option<usize>, ApiError> {
    let Some(total) = validate_manifest(state, key, bytes)? else {
        return Ok(None);
    };
    if total > max_bytes {
        return Err(ApiError {
            status: StatusCode::PAYLOAD_TOO_LARGE,
            message: "checkpoint fragments exceed demand migration budget".into(),
            details: Some(json!({"code":"DEMAND_MIGRATION_BUDGET_REQUIRED",
                "required_min_bytes":total,"supplied_max_bytes":max_bytes})),
            retryable_materialization: false,
        });
    }
    let manifest: FragmentManifest =
        serde_json::from_slice(bytes).map_err(ApiError::bad_request)?;
    let mut read = 0usize;
    for (ordinal, fragment) in manifest.fragments.iter().enumerate() {
        let path = fragment_path(key, ordinal, &fragment.content_hash);
        let remaining = max_bytes.saturating_sub(read);
        let payload = read_bounded(state, &path, fragment.byte_count.min(remaining)).await?;
        if payload.len() != fragment.byte_count
            || stable_bytes_hash(&payload) != fragment.content_hash
        {
            return Err(ApiError::bad_request(
                "checkpoint fragment hash/length mismatch",
            ));
        }
        read = read
            .checked_add(payload.len())
            .ok_or_else(|| ApiError::bad_request("checkpoint fragment byte count overflow"))?;
    }
    if read != total {
        return Err(ApiError::bad_request("checkpoint fragment total mismatch"));
    }
    Ok(Some(read))
}

async fn put_exact(state: &ApiState, path: &ObjectPath, bytes: Vec<u8>) -> Result<(), ApiError> {
    match state
        .store
        .put_opts(
            path,
            bytes::Bytes::from(bytes.clone()).into(),
            PutMode::Create.into(),
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(object_store::Error::AlreadyExists { .. }) => {
            let existing = state
                .store
                .get(path)
                .await
                .map_err(ApiError::internal)?
                .bytes()
                .await
                .map_err(ApiError::internal)?;
            if existing.as_ref() == bytes.as_slice() {
                Ok(())
            } else {
                Err(ApiError::conflict("state fragment publication conflict"))
            }
        }
        Err(error) => Err(ApiError::internal(error)),
    }
}

#[cfg(test)]
pub(super) async fn persist_fragments(
    state: &ApiState,
    key: &ObjectKey,
    record: &StandingRuntimeStatePayloadRecord,
) -> Result<bool, ApiError> {
    let bytes = checkpoint_arrow::encode(record)?;
    persist_encoded_fragments(state, key, record, &bytes).await
}

pub(super) async fn persist_encoded_fragments(
    state: &ApiState,
    key: &ObjectKey,
    record: &StandingRuntimeStatePayloadRecord,
    bytes: &[u8],
) -> Result<bool, ApiError> {
    if record.payload.payload.len() > state.max_standing_runtime_state_payload_bytes {
        return Err(ApiError::payload_too_large(
            "checkpoint exceeds configured total state byte limit",
        ));
    }
    let fragment_bytes = state.max_standing_runtime_state_fragment_bytes;
    if bytes.len() <= fragment_bytes {
        return Ok(false);
    }
    let mut fragments = Vec::new();
    for (ordinal, bytes) in bytes.chunks(fragment_bytes).enumerate() {
        let content_hash = stable_bytes_hash(bytes);
        put_exact(
            state,
            &fragment_path(key, ordinal, &content_hash),
            bytes.to_vec(),
        )
        .await?;
        fragments.push(FragmentRef {
            content_hash,
            byte_count: bytes.len(),
        });
    }
    let mut header = record.clone();
    header.payload.payload.clear();
    let manifest = FragmentManifest {
        schema_version: 2,
        record_kind: MANIFEST_KIND.to_string(),
        byte_count: bytes.len(),
        state_bytes: record.payload.payload.len(),
        header,
        fragments,
        fragment_bytes,
    };
    let bytes = serde_json::to_vec(&manifest).map_err(ApiError::internal)?;
    if bytes.len() > fragment_bytes {
        return Err(ApiError::payload_too_large(
            "checkpoint fragment manifest exceeds state budget",
        ));
    }
    put_exact(state, &ObjectPath::from(key.as_str()), bytes).await?;
    Ok(true)
}

pub(super) async fn hydrate_fragments(
    state: &ApiState,
    key: &ObjectKey,
    bytes: &[u8],
) -> Result<Option<StandingRuntimeStatePayloadRecord>, ApiError> {
    if validate_manifest(state, key, bytes)?.is_none() {
        return Ok(None);
    }
    let manifest: FragmentManifest =
        serde_json::from_slice(bytes).map_err(ApiError::bad_request)?;
    let mut payload = Vec::with_capacity(manifest.byte_count);
    for (ordinal, fragment) in manifest.fragments.iter().enumerate() {
        let expected = manifest
            .fragment_bytes
            .min(manifest.byte_count - ordinal * manifest.fragment_bytes);
        if fragment.byte_count != expected || !is_sha256_identity(&fragment.content_hash) {
            return Err(ApiError::bad_request(
                "invalid checkpoint fragment descriptor",
            ));
        }
        let bytes = read_bounded(
            state,
            &fragment_path(key, ordinal, &fragment.content_hash),
            expected,
        )
        .await?;
        if bytes.len() != expected || stable_bytes_hash(&bytes) != fragment.content_hash {
            return Err(ApiError::bad_request(
                "checkpoint fragment hash/length mismatch",
            ));
        }
        payload.extend_from_slice(&bytes);
    }
    let key = key.clone();
    let max_state_bytes = state.max_standing_runtime_state_payload_bytes;
    tokio::task::spawn_blocking(move || {
        let mut restored = checkpoint_arrow::decode(&payload, max_state_bytes)?;
        if restored.payload.payload.len() != manifest.state_bytes {
            return Err(ApiError::bad_request(
                "checkpoint restored state length mismatch",
            ));
        }
        let body = std::mem::take(&mut restored.payload.payload);
        if manifest.header != restored {
            return Err(ApiError::bad_request("checkpoint fragment header mismatch"));
        }
        restored.payload.payload = body;
        validate_standing_runtime_state_payload_record(&key, &restored)?;
        Ok(Some(restored))
    })
    .await
    .map_err(ApiError::internal)?
}
