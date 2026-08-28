use super::*;

const MAX_RETAINED_IDEMPOTENCY_KEYS: usize = 1_024;

/// Maximum allowed checkpoint payload size in bytes (16 MiB).
/// Checkpoints exceeding this limit are rejected during restore to prevent
/// excessive memory allocation and recovery time.
pub const MAX_CHECKPOINT_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;

/// Validate that a checkpoint payload does not exceed the size limit.
pub fn validate_checkpoint_size(payload: &str) -> Result<(), StandingProgramRuntimeError> {
    if payload.len() > MAX_CHECKPOINT_PAYLOAD_BYTES {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "checkpoint_payload_size_exceeded",
        });
    }
    Ok(())
}

/// Keeps checkpointed idempotency history to 1,024 entries while retaining the most recent epochs.
///
/// Idempotency keys older than this window may be applied again once evicted; callers must
/// therefore retry promptly rather than rely on the runtime as an unbounded deduplication log.
pub(super) fn retain_recent_applied_epochs(applied_epochs: &mut BTreeMap<String, LogicalEpoch>) {
    applied_epochs.retain(|key, _| key.len() <= EpochIdempotencyKey::MAX_BYTES);
    let overflow = applied_epochs
        .len()
        .saturating_sub(MAX_RETAINED_IDEMPOTENCY_KEYS);
    if overflow == 0 {
        return;
    }

    let mut oldest_first = applied_epochs
        .iter()
        .map(|(idempotency_key, logical_epoch)| (idempotency_key.clone(), *logical_epoch))
        .collect::<Vec<_>>();
    oldest_first.sort_unstable_by(|(left_key, left_epoch), (right_key, right_epoch)| {
        left_epoch
            .cmp(right_epoch)
            .then_with(|| left_key.cmp(right_key))
    });
    for (idempotency_key, _) in oldest_first.into_iter().take(overflow) {
        applied_epochs.remove(&idempotency_key);
    }
}
