//! Partition and owner head metadata for CAS-based updates.
//!
//! Provides compact metadata structures that serve as the single source
//! of truth for partition state and ownership. These are the objects
//! that should be conditionally updated (CAS) instead of scanning
//! full history on every operation.
//!
//! # Design
//!
//! ```text
//! PartitionHead {
//!     stream_id, partition_id,
//!     generation,
//!     committed_high_watermark,
//!     latest_checkpoint_id,
//!     owner_epoch,
//!     updated_at,
//! }
//!
//! OwnerHead {
//!     partition,
//!     generation,
//!     claim_id,
//!     worker_id,
//!     lease_expiry,
//!     lineage_root,
//! }
//! ```
//!
//! # Benefits over history scan
//!
//! - O(1) read instead of O(H) scan
//! - CAS update ensures linearizability
//! - Single object per partition instead of N objects per history

use serde::{Deserialize, Serialize};

/// Compact partition head metadata.
///
/// One object per (stream, partition) pair. Serves as the single
/// source of truth for partition state, replacing full history scans.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionHead {
    /// Schema version for forward compatibility.
    pub schema_version: u32,
    /// Stream identifier.
    pub stream_id: String,
    /// Partition identifier.
    pub partition_id: u32,
    /// Monotonic generation counter for CAS updates.
    pub generation: u64,
    /// Highest committed offset in this partition.
    pub committed_high_watermark: u64,
    /// ID of the latest checkpoint for this partition.
    pub latest_checkpoint_id: Option<String>,
    /// Current owner epoch (for fencing).
    pub owner_epoch: u64,
    /// Timestamp of last update.
    pub updated_at: u64,
}

impl PartitionHead {
    /// Create a new partition head.
    pub fn new(stream_id: String, partition_id: u32) -> Self {
        Self {
            schema_version: 1,
            stream_id,
            partition_id,
            generation: 0,
            committed_high_watermark: 0,
            latest_checkpoint_id: None,
            owner_epoch: 0,
            updated_at: 0,
        }
    }

    /// Advance the head to a new generation.
    pub fn advance(&mut self, committed_high_watermark: u64, owner_epoch: u64, updated_at: u64) {
        self.generation += 1;
        self.committed_high_watermark = committed_high_watermark;
        self.owner_epoch = owner_epoch;
        self.updated_at = updated_at;
    }

    /// Get the object key for this partition head.
    pub fn object_key(&self) -> String {
        format!(
            "v1/partition-head/{}/p={}",
            self.stream_id, self.partition_id
        )
    }
}

/// Compact owner head metadata.
///
/// One object per partition. Tracks ownership and lease state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnerHead {
    /// Schema version for forward compatibility.
    pub schema_version: u32,
    /// Partition identifier.
    pub partition: String,
    /// Monotonic generation counter for CAS updates.
    pub generation: u64,
    /// Unique claim identifier.
    pub claim_id: String,
    /// Worker that holds the claim.
    pub worker_id: String,
    /// Lease expiry timestamp.
    pub lease_expiry: u64,
    /// Root of the lineage (for audit).
    pub lineage_root: String,
    /// Timestamp of last update.
    pub updated_at: u64,
}

impl OwnerHead {
    /// Create a new owner head.
    pub fn new(
        partition: String,
        claim_id: String,
        worker_id: String,
        lease_expiry: u64,
        lineage_root: String,
    ) -> Self {
        Self {
            schema_version: 1,
            partition,
            generation: 0,
            claim_id,
            worker_id,
            lease_expiry,
            lineage_root,
            updated_at: 0,
        }
    }

    /// Check if the lease has expired.
    pub fn is_expired(&self, current_time: u64) -> bool {
        current_time > self.lease_expiry
    }

    /// Get the object key for this owner head.
    pub fn object_key(&self) -> String {
        format!("v1/owner-head/{}", self.partition)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_head_advance() {
        let mut head = PartitionHead::new("stream1".to_string(), 0);
        assert_eq!(head.generation, 0);

        head.advance(100, 1, 1000);
        assert_eq!(head.generation, 1);
        assert_eq!(head.committed_high_watermark, 100);
        assert_eq!(head.owner_epoch, 1);
    }

    #[test]
    fn partition_head_object_key() {
        let head = PartitionHead::new("stream1".to_string(), 0);
        assert_eq!(head.object_key(), "v1/partition-head/stream1/p=0");
    }

    #[test]
    fn owner_head_expiry() {
        let head = OwnerHead::new(
            "stream1/p=0".to_string(),
            "claim1".to_string(),
            "worker1".to_string(),
            1000,
            "lineage1".to_string(),
        );
        assert!(!head.is_expired(999));
        assert!(head.is_expired(1001));
    }

    #[test]
    fn owner_head_object_key() {
        let head = OwnerHead::new(
            "stream1/p=0".to_string(),
            "claim1".to_string(),
            "worker1".to_string(),
            1000,
            "lineage1".to_string(),
        );
        assert_eq!(head.object_key(), "v1/owner-head/stream1/p=0");
    }
}
