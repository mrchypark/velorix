//! Reachability-based garbage collection for object store.
//!
//! Instead of re-reading payload objects and computing SHA-256 hashes
//! during GC, this module uses checkpoint reachability to determine
//! which objects are safe to delete.
//!
//! # Design
//!
//! ```text
//! 1. Mark phase: Starting from latest checkpoint roots, traverse
//!    all reachable objects via state/output references.
//!
//! 2. Sweep phase: Delete objects not reachable from any root.
//!
//! 3. Safety: Objects are retained for a grace period after becoming
//!    unreachable to handle concurrent readers.
//! ```
//!
//! # Benefits over current approach
//!
//! - No need to read object payloads for hash computation
//! - GC cost proportional to reachable object count, not total objects
//! - Deterministic: same roots → same reachable set
//! - Supports concurrent readers via grace period

use std::collections::{BTreeMap, BTreeSet, HashSet};

use serde::{Deserialize, Serialize};

/// A node in the reachability graph.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ObjectRef {
    pub key: String,
    pub kind: ObjectKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ObjectKind {
    CheckpointManifest,
    StateObject,
    OutputObject,
    LifecycleRecord,
    AdmissionRecord,
}

/// Reachability graph for GC.
///
/// Tracks which objects are reachable from checkpoint roots.
pub struct ReachabilityGraph {
    /// Root objects (latest checkpoints per view).
    roots: BTreeSet<String>,
    /// Object references: key -> set of referenced object keys.
    edges: BTreeMap<String, HashSet<String>>,
    /// Object metadata: key -> kind.
    objects: BTreeMap<String, ObjectKind>,
    /// Grace period in seconds. Objects unreachable for this long are deletable.
    grace_period_secs: u64,
    /// When each object became unreachable (epoch seconds).
    unreachable_at: BTreeMap<String, u64>,
}

impl ReachabilityGraph {
    /// Create a new reachability graph.
    pub fn new(grace_period_secs: u64) -> Self {
        Self {
            roots: BTreeSet::new(),
            edges: BTreeMap::new(),
            objects: BTreeMap::new(),
            grace_period_secs,
            unreachable_at: BTreeMap::new(),
        }
    }

    /// Add a root object (e.g., latest checkpoint).
    pub fn add_root(&mut self, key: String) {
        self.roots.insert(key);
    }

    /// Remove a root object.
    pub fn remove_root(&mut self, key: &str) {
        self.roots.remove(key);
    }

    /// Register an object with its kind.
    pub fn register_object(&mut self, key: String, kind: ObjectKind) {
        self.objects.insert(key, kind);
    }

    /// Add a reference from one object to another.
    pub fn add_reference(&mut self, from: &str, to: &str) {
        self.edges
            .entry(from.to_string())
            .or_default()
            .insert(to.to_string());
    }

    /// Compute the set of reachable objects from all roots.
    ///
    /// Uses BFS traversal through the reference graph.
    pub fn compute_reachable(&self) -> HashSet<String> {
        let mut reachable = HashSet::new();
        let mut queue = Vec::new();

        // Start from roots
        for root in &self.roots {
            if self.objects.contains_key(root) {
                reachable.insert(root.clone());
                queue.push(root.clone());
            }
        }

        // BFS traversal
        while let Some(current) = queue.pop() {
            if let Some(refs) = self.edges.get(&current) {
                for neighbor in refs {
                    if reachable.insert(neighbor.clone()) {
                        queue.push(neighbor.clone());
                    }
                }
            }
        }

        reachable
    }

    /// Find garbage objects (not reachable from any root).
    ///
    /// Returns objects that are safe to delete after grace period.
    pub fn find_garbage(&self, current_time_secs: u64) -> Vec<String> {
        let reachable = self.compute_reachable();
        let mut garbage = Vec::new();

        for key in self.objects.keys() {
            if !reachable.contains(key) {
                // Check grace period
                let unreachable_since = self
                    .unreachable_at
                    .get(key)
                    .copied()
                    .unwrap_or(current_time_secs);

                if current_time_secs.saturating_sub(unreachable_since) >= self.grace_period_secs {
                    garbage.push(key.clone());
                }
            }
        }

        garbage.sort();
        garbage
    }

    /// Mark objects as unreachable at the given time.
    ///
    /// Called after each GC cycle to track when objects became unreachable.
    pub fn mark_unreachable(&mut self, current_time_secs: u64) {
        let reachable = self.compute_reachable();
        for key in self.objects.keys() {
            if !reachable.contains(key) {
                self.unreachable_at
                    .entry(key.clone())
                    .or_insert(current_time_secs);
            } else {
                // Object is reachable, clear unreachable timestamp
                self.unreachable_at.remove(key);
            }
        }
    }

    /// Get the total number of registered objects.
    pub fn object_count(&self) -> usize {
        self.objects.len()
    }

    /// Get the number of root objects.
    pub fn root_count(&self) -> usize {
        self.roots.len()
    }

    /// Get the number of reachable objects.
    pub fn reachable_count(&self) -> usize {
        self.compute_reachable().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reachability_basic() {
        let mut graph = ReachabilityGraph::new(0);
        graph.register_object("root".to_string(), ObjectKind::CheckpointManifest);
        graph.register_object("state1".to_string(), ObjectKind::StateObject);
        graph.register_object("state2".to_string(), ObjectKind::StateObject);
        graph.register_object("orphan".to_string(), ObjectKind::OutputObject);

        graph.add_root("root".to_string());
        graph.add_reference("root", "state1");
        graph.add_reference("root", "state2");

        let reachable = graph.compute_reachable();
        assert!(reachable.contains("root"));
        assert!(reachable.contains("state1"));
        assert!(reachable.contains("state2"));
        assert!(!reachable.contains("orphan"));
    }

    #[test]
    fn reachability_transitive() {
        let mut graph = ReachabilityGraph::new(0);
        graph.register_object("root".to_string(), ObjectKind::CheckpointManifest);
        graph.register_object("a".to_string(), ObjectKind::StateObject);
        graph.register_object("b".to_string(), ObjectKind::StateObject);
        graph.register_object("c".to_string(), ObjectKind::OutputObject);

        graph.add_root("root".to_string());
        graph.add_reference("root", "a");
        graph.add_reference("a", "b");
        graph.add_reference("b", "c");

        let reachable = graph.compute_reachable();
        assert_eq!(reachable.len(), 4);
    }

    #[test]
    fn reachability_garbage_collection() {
        let mut graph = ReachabilityGraph::new(0);
        graph.register_object("root".to_string(), ObjectKind::CheckpointManifest);
        graph.register_object("state1".to_string(), ObjectKind::StateObject);
        graph.register_object("orphan".to_string(), ObjectKind::OutputObject);

        graph.add_root("root".to_string());
        graph.add_reference("root", "state1");

        let garbage = graph.find_garbage(0);
        assert_eq!(garbage, vec!["orphan".to_string()]);
    }

    #[test]
    fn reachability_grace_period() {
        let mut graph = ReachabilityGraph::new(3600); // 1 hour grace period
        graph.register_object("root".to_string(), ObjectKind::CheckpointManifest);
        graph.register_object("orphan".to_string(), ObjectKind::OutputObject);

        graph.add_root("root".to_string());

        // Immediately after becoming unreachable, not yet deletable
        graph.mark_unreachable(0);
        let garbage = graph.find_garbage(0);
        assert!(garbage.is_empty());

        // After grace period, deletable
        let garbage = graph.find_garbage(3601);
        assert_eq!(garbage, vec!["orphan".to_string()]);
    }

    #[test]
    fn reachability_root_change() {
        let mut graph = ReachabilityGraph::new(0);
        graph.register_object("old_root".to_string(), ObjectKind::CheckpointManifest);
        graph.register_object("new_root".to_string(), ObjectKind::CheckpointManifest);
        graph.register_object("state1".to_string(), ObjectKind::StateObject);

        graph.add_root("old_root".to_string());
        graph.add_reference("old_root", "state1");

        // Switch to new root
        graph.remove_root("old_root");
        graph.add_root("new_root".to_string());
        graph.add_reference("new_root", "state1");

        let reachable = graph.compute_reachable();
        assert!(reachable.contains("new_root"));
        assert!(reachable.contains("state1"));
        assert!(!reachable.contains("old_root"));
    }
}
