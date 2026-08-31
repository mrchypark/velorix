//! Checkpoint compaction planning.
//!
//! Determines which segments to merge, split, or discard during
//! checkpoint compaction. Compaction reduces storage costs by:
//!
//! - Merging small segments into larger ones
//! - Deduplicating unchanged segments across checkpoints
//! - Discarding superseded state/output segments
//!
//! # Compaction Strategy
//!
//! ```text
//! 1. Identify segments eligible for compaction:
//!    - Segments smaller than target_chunk_size
//!    - Segments with high overlap (same key ranges)
//!    - Segments superseded by newer checkpoints
//!
//! 2. Plan compaction actions:
//!    - Merge: combine small adjacent segments
//!    - Split: break large segments into smaller pieces
//!    - Dedup: reuse unchanged segments (content-addressed)
//!    - Discard: remove segments not reachable from any root
//!
//! 3. Execute compaction:
//!    - Create new merged segments
//!    - Update manifest references
//!    - Delete old segments after grace period
//! ```

use serde::{Deserialize, Serialize};

/// Target chunk size in bytes for compaction.
pub const DEFAULT_TARGET_CHUNK_BYTES: usize = 4 * 1024 * 1024; // 4 MiB

/// A compaction action to be performed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompactionAction {
    /// Merge multiple small segments into one larger segment.
    Merge {
        input_segments: Vec<String>,
        output_segment: String,
    },
    /// Split a large segment into multiple smaller segments.
    Split {
        input_segment: String,
        output_segments: Vec<String>,
    },
    /// Reuse an existing segment (content-addressed deduplication).
    Reuse {
        segment_hash: String,
        references: Vec<String>,
    },
    /// Discard a segment that is no longer reachable.
    Discard {
        segment_hash: String,
        reason: String,
    },
}

/// A compaction plan for a single checkpoint.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionPlan {
    pub checkpoint_version: u64,
    pub actions: Vec<CompactionAction>,
    pub total_input_bytes: u64,
    pub total_output_bytes: u64,
    pub estimated_savings_bytes: u64,
    pub segment_count_before: usize,
    pub segment_count_after: usize,
}

impl CompactionPlan {
    /// Create an empty compaction plan.
    pub fn new(checkpoint_version: u64) -> Self {
        Self {
            checkpoint_version,
            actions: Vec::new(),
            total_input_bytes: 0,
            total_output_bytes: 0,
            estimated_savings_bytes: 0,
            segment_count_before: 0,
            segment_count_after: 0,
        }
    }

    /// Add a merge action.
    pub fn add_merge(&mut self, input_segments: Vec<String>, output_segment: String) {
        self.actions.push(CompactionAction::Merge {
            input_segments,
            output_segment,
        });
    }

    /// Add a discard action.
    pub fn add_discard(&mut self, segment_hash: String, reason: String) {
        self.actions.push(CompactionAction::Discard {
            segment_hash,
            reason,
        });
    }

    /// Check if the plan has any actions.
    pub fn is_empty(&self) -> bool {
        self.actions.is_empty()
    }

    /// Get the number of actions.
    pub fn action_count(&self) -> usize {
        self.actions.len()
    }

    /// Compute estimated savings.
    pub fn compute_savings(&mut self) {
        self.estimated_savings_bytes = self
            .total_input_bytes
            .saturating_sub(self.total_output_bytes);
    }
}

/// Determine compaction actions for a list of segments.
///
/// Segments smaller than `target_bytes` are candidates for merging.
/// Segments with identical content hashes are deduplication candidates.
pub fn plan_compaction(
    segments: &[(String, u64)], // (content_hash, byte_size)
    target_bytes: usize,
) -> CompactionPlan {
    let mut plan = CompactionPlan::new(0);
    plan.segment_count_before = segments.len();

    // Identify small segments for merging
    let mut small_segments: Vec<(String, u64)> = segments
        .iter()
        .filter(|(_, size)| (*size as usize) < target_bytes)
        .cloned()
        .collect();

    // Merge small segments into groups
    let mut current_group = Vec::new();
    let mut current_size: u64 = 0;
    let mut group_index = 0;

    for (hash, size) in small_segments.drain(..) {
        current_group.push(hash.clone());
        current_size += size;

        if current_size >= target_bytes as u64 {
            if current_group.len() > 1 {
                let output = format!("merged_{}", group_index);
                plan.add_merge(current_group.clone(), output);
                plan.total_input_bytes += current_size;
                plan.total_output_bytes += current_size; // Merged segment has same total size
            }
            current_group.clear();
            current_size = 0;
            group_index += 1;
        }
    }

    // Handle remaining small segments
    if current_group.len() > 1 {
        let output = format!("merged_{}", group_index);
        plan.add_merge(current_group, output);
        plan.total_input_bytes += current_size;
        plan.total_output_bytes += current_size;
    }

    plan.segment_count_after = segments.len() - plan.action_count();
    plan.compute_savings();
    plan
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compaction_plan_empty() {
        let plan = CompactionPlan::new(1);
        assert!(plan.is_empty());
        assert_eq!(plan.action_count(), 0);
    }

    #[test]
    fn compaction_plan_merge_small_segments() {
        let segments = vec![
            ("hash1".to_string(), 1000),
            ("hash2".to_string(), 2000),
            ("hash3".to_string(), 500),
        ];

        let plan = plan_compaction(&segments, 4000);
        // All segments are small (< 4000), should be merged
        assert!(plan.action_count() > 0);
    }

    #[test]
    fn compaction_plan_skip_large_segments() {
        let segments = vec![("hash1".to_string(), 10000), ("hash2".to_string(), 20000)];

        let plan = plan_compaction(&segments, 4000);
        // All segments are large, no merging needed
        assert_eq!(plan.action_count(), 0);
    }

    #[test]
    fn compaction_plan_savings() {
        let mut plan = CompactionPlan::new(1);
        plan.total_input_bytes = 10000;
        plan.total_output_bytes = 8000;
        plan.compute_savings();
        assert_eq!(plan.estimated_savings_bytes, 2000);
    }
}
