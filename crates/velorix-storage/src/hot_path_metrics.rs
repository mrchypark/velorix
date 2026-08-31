//! Hot path LIST prohibition metric.
//!
//! Detects when LIST operations occur on data hot paths (ingest,
//! query, checkpoint publish) and increments a counter. This metric
//! can be used to enforce a policy that no LIST should occur on
//! hot paths, only on repair/audit/background compaction.
//!
//! # Design
//!
//! ```text
//! HotPathMetrics {
//!     list_count: AtomicU64,
//!     get_count: AtomicU64,
//!     put_count: AtomicU64,
//!     hot_path_violations: AtomicU64,
//! }
//! ```
//!
//! # Usage
//!
//! ```text
//! metrics.record_list("ingest_admission");
//! // If this fires on an ingest hot path, it's a violation
//! ```

use std::sync::atomic::{AtomicU64, Ordering};

/// Metrics for tracking object store operations on hot paths.
///
/// Hot paths are: ingest, query serving, checkpoint publish.
/// LIST operations on these paths indicate potential cost scaling issues.
pub struct HotPathMetrics {
    /// Total LIST operations observed.
    pub list_count: AtomicU64,
    /// Total GET operations observed.
    pub get_count: AtomicU64,
    /// Total PUT operations observed.
    pub put_count: AtomicU64,
    /// LIST operations that occurred on hot paths (violations).
    pub hot_path_violations: AtomicU64,
}

impl HotPathMetrics {
    /// Create a new metrics instance.
    pub fn new() -> Self {
        Self {
            list_count: AtomicU64::new(0),
            get_count: AtomicU64::new(0),
            put_count: AtomicU64::new(0),
            hot_path_violations: AtomicU64::new(0),
        }
    }

    /// Record a LIST operation.
    ///
    /// `path` identifies the logical path (e.g., "ingest_admission",
    /// "checkpoint_manifest"). If this is a hot path, increment violations.
    pub fn record_list(&self, path: &str, is_hot_path: bool) {
        self.list_count.fetch_add(1, Ordering::Relaxed);
        if is_hot_path {
            self.hot_path_violations.fetch_add(1, Ordering::Relaxed);
            eprintln!(
                "[HOT_PATH_VIOLATION] LIST on hot path: {} (total violations: {})",
                path,
                self.hot_path_violations.load(Ordering::Relaxed)
            );
        }
    }

    /// Record a GET operation.
    pub fn record_get(&self, _path: &str) {
        self.get_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a PUT operation.
    pub fn record_put(&self, _path: &str) {
        self.put_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Get the current LIST count.
    pub fn list_count(&self) -> u64 {
        self.list_count.load(Ordering::Relaxed)
    }

    /// Get the current GET count.
    pub fn get_count(&self) -> u64 {
        self.get_count.load(Ordering::Relaxed)
    }

    /// Get the current PUT count.
    pub fn put_count(&self) -> u64 {
        self.put_count.load(Ordering::Relaxed)
    }

    /// Get the current hot path violation count.
    pub fn hot_path_violations(&self) -> u64 {
        self.hot_path_violations.load(Ordering::Relaxed)
    }

    /// Check if any hot path violations have occurred.
    pub fn has_violations(&self) -> bool {
        self.hot_path_violations.load(Ordering::Relaxed) > 0
    }

    /// Reset all counters.
    pub fn reset(&self) {
        self.list_count.store(0, Ordering::Relaxed);
        self.get_count.store(0, Ordering::Relaxed);
        self.put_count.store(0, Ordering::Relaxed);
        self.hot_path_violations.store(0, Ordering::Relaxed);
    }
}

impl Default for HotPathMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Trait for object stores that support hot path metrics.
pub trait HotPathAware {
    /// Get a reference to the metrics.
    fn metrics(&self) -> &HotPathMetrics;

    /// Check if the object store has any hot path violations.
    fn has_hot_path_violations(&self) -> bool {
        self.metrics().has_violations()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hot_path_metrics_basic() {
        let metrics = HotPathMetrics::new();
        assert_eq!(metrics.list_count(), 0);
        assert!(!metrics.has_violations());

        metrics.record_list("test_path", false);
        assert_eq!(metrics.list_count(), 1);
        assert!(!metrics.has_violations());
    }

    #[test]
    fn hot_path_metrics_violation() {
        let metrics = HotPathMetrics::new();
        metrics.record_list("ingest_admission", true);
        assert!(metrics.has_violations());
        assert_eq!(metrics.hot_path_violations(), 1);
    }

    #[test]
    fn hot_path_metrics_reset() {
        let metrics = HotPathMetrics::new();
        metrics.record_list("test", true);
        metrics.record_get("test");
        metrics.record_put("test");

        metrics.reset();
        assert_eq!(metrics.list_count(), 0);
        assert_eq!(metrics.get_count(), 0);
        assert_eq!(metrics.put_count(), 0);
        assert!(!metrics.has_violations());
    }
}
