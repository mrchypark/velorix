//! State replay plan for recovery.
//!
//! Structures the recovery process by defining which source epochs
//! need to be replayed to rebuild state from a checkpoint.
//!
//! # Design
//!
//! ```text
//! ReplayPlan {
//!     checkpoint_version: u64,
//!     replay_epochs: Vec<ReplayEpoch>,
//!     total_replay_bytes: u64,
//!     estimated_replay_time_ms: u64,
//! }
//! ```
//!
//! # Benefits
//!
//! - Bounds recovery time by limiting replay scope
//! - Enables progress reporting during recovery
//! - Supports partial replay (resume from last successful epoch)
//! - Separates checkpoint restore from source replay

use serde::{Deserialize, Serialize};

/// A source epoch to be replayed during recovery.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayEpoch {
    /// Source epoch identifier.
    pub epoch_id: String,
    /// Stream ID for this epoch.
    pub stream_id: String,
    /// Partition ID for this epoch.
    pub partition_id: u32,
    /// Start offset (inclusive).
    pub start_offset: u64,
    /// End offset (exclusive).
    pub end_offset: u64,
    /// Size of the epoch payload in bytes.
    pub payload_bytes: u64,
    /// Digest of the epoch payload.
    pub payload_digest: String,
}

/// A plan for replaying source epochs during recovery.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayPlan {
    /// Checkpoint version to restore from.
    pub checkpoint_version: u64,
    /// Epochs to replay (in order).
    pub replay_epochs: Vec<ReplayEpoch>,
    /// Total bytes to replay.
    pub total_replay_bytes: u64,
    /// Estimated replay time in milliseconds.
    pub estimated_replay_time_ms: u64,
    /// Maximum allowed replay bytes (safety limit).
    pub max_replay_bytes: u64,
}

impl ReplayPlan {
    /// Create a new replay plan.
    pub fn new(checkpoint_version: u64, max_replay_bytes: u64) -> Self {
        Self {
            checkpoint_version,
            replay_epochs: Vec::new(),
            total_replay_bytes: 0,
            estimated_replay_time_ms: 0,
            max_replay_bytes,
        }
    }

    /// Add an epoch to the replay plan.
    ///
    /// Returns false if adding the epoch would exceed the byte limit.
    pub fn add_epoch(&mut self, epoch: ReplayEpoch) -> bool {
        let new_total = self.total_replay_bytes.saturating_add(epoch.payload_bytes);
        if new_total > self.max_replay_bytes {
            return false;
        }
        self.total_replay_bytes = new_total;
        // Estimate 1 MB/s replay speed
        self.estimated_replay_time_ms = self.total_replay_bytes / 1024;
        self.replay_epochs.push(epoch);
        true
    }

    /// Check if the plan is empty (no replay needed).
    pub fn is_empty(&self) -> bool {
        self.replay_epochs.is_empty()
    }

    /// Get the number of epochs to replay.
    pub fn epoch_count(&self) -> usize {
        self.replay_epochs.len()
    }

    /// Check if the plan exceeds the byte limit.
    pub fn exceeds_limit(&self) -> bool {
        self.total_replay_bytes > self.max_replay_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replay_plan_empty() {
        let plan = ReplayPlan::new(1, 1024 * 1024);
        assert!(plan.is_empty());
        assert_eq!(plan.epoch_count(), 0);
        assert!(!plan.exceeds_limit());
    }

    #[test]
    fn replay_plan_add_epoch() {
        let mut plan = ReplayPlan::new(1, 1024 * 1024);
        let epoch = ReplayEpoch {
            epoch_id: "e1".to_string(),
            stream_id: "s1".to_string(),
            partition_id: 0,
            start_offset: 0,
            end_offset: 100,
            payload_bytes: 1024,
            payload_digest: "d1".to_string(),
        };
        assert!(plan.add_epoch(epoch));
        assert_eq!(plan.epoch_count(), 1);
        assert_eq!(plan.total_replay_bytes, 1024);
    }

    #[test]
    fn replay_plan_exceeds_limit() {
        let mut plan = ReplayPlan::new(1, 100);
        let epoch = ReplayEpoch {
            epoch_id: "e1".to_string(),
            stream_id: "s1".to_string(),
            partition_id: 0,
            start_offset: 0,
            end_offset: 100,
            payload_bytes: 200,
            payload_digest: "d1".to_string(),
        };
        assert!(!plan.add_epoch(epoch));
        assert!(plan.is_empty());
        // exceeds_limit checks total_replay_bytes > max_replay_bytes
        // Since no epoch was added, total_replay_bytes is 0
        assert!(!plan.exceeds_limit());
    }
}
