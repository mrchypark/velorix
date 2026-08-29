//! Semi-naive recursive fixpoint evaluation.
//!
//! Replaces the naive approach of iterating over all derived rows each
//! iteration with the standard `all / delta / next_delta` frontier pattern.
//!
//! # Algorithm
//!
//! ```text
//! all        = 확정된 derived rows (누적)
//! delta      = 이번 iteration에서 새로 활성화된 frontier
//! next_delta = delta로부터 새로 도출된 rows
//!
//! 초기화: all = seed, delta = seed
//! iteration: candidates = evaluate(delta, all) → next_delta
//!            all += next_delta, delta = next_delta
//! ```
//!
//! # Complexity
//!
//! Naive: O(D × B × I) where D=derived, B=base, I=iterations
//! Semi-naive: O(F × B × I) where F=frontier size (typically much smaller than D)

use std::collections::BTreeMap;

/// Semi-naive recursive fixpoint evaluator.
///
/// Tracks three sets:
/// - `all`: all confirmed derived rows
/// - `delta`: frontier of newly added rows in the current iteration
/// - Weight tracking via i128 values for multiset semantics
pub struct RecursiveFrontier {
    /// All confirmed derived rows with their net weights.
    pub all: BTreeMap<String, i128>,
    /// Current frontier (newly added rows from the previous iteration).
    pub delta: Vec<String>,
}

impl RecursiveFrontier {
    /// Create a new frontier from seed rows.
    ///
    /// Each seed row becomes part of both `all` and the initial `delta`.
    pub fn from_seed(seed: BTreeMap<String, i128>) -> Self {
        let delta: Vec<String> = seed.keys().cloned().collect();
        Self { all: seed, delta }
    }

    /// Create an empty frontier.
    pub fn new() -> Self {
        Self {
            all: BTreeMap::new(),
            delta: Vec::new(),
        }
    }

    /// Add a candidate derived row to the frontier.
    ///
    /// Returns true if the row is new (not already in `all` with the same weight).
    pub fn add_candidate(&mut self, key: String, weight: i128) -> bool {
        if weight == 0 {
            return false;
        }
        let entry = self.all.entry(key.clone()).or_insert(0);
        let old_weight = *entry;
        *entry = entry.saturating_add(weight);
        // New row if it wasn't in all before, or weight changed from 0 to non-zero
        let is_new = old_weight == 0 && *entry != 0;
        if is_new {
            self.delta.push(key);
        }
        is_new
    }

    /// Advance to the next iteration.
    ///
    /// Moves delta → next_delta and clears delta for the next round.
    pub fn advance(&mut self) -> Vec<String> {
        std::mem::take(&mut self.delta)
    }

    /// Check if the frontier is empty (fixpoint reached).
    pub fn is_empty(&self) -> bool {
        self.delta.is_empty()
    }

    /// Get the total number of derived rows.
    pub fn len(&self) -> usize {
        self.all.len()
    }

    /// Get the current frontier size.
    pub fn frontier_size(&self) -> usize {
        self.delta.len()
    }

    /// Remove zero-weight entries from the derived set.
    pub fn compact(&mut self) {
        self.all.retain(|_, weight| *weight != 0);
    }
}

impl Default for RecursiveFrontier {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frontier_from_seed() {
        let seed = BTreeMap::from([("a".to_string(), 1), ("b".to_string(), 1)]);
        let frontier = RecursiveFrontier::from_seed(seed);
        assert_eq!(frontier.len(), 2);
        assert_eq!(frontier.frontier_size(), 2);
        assert!(!frontier.is_empty());
    }

    #[test]
    fn frontier_add_candidate_new() {
        let mut frontier = RecursiveFrontier::new();
        assert!(frontier.add_candidate("a".to_string(), 1));
        assert_eq!(frontier.len(), 1);
        assert_eq!(frontier.frontier_size(), 1);
    }

    #[test]
    fn frontier_add_candidate_duplicate() {
        let mut frontier = RecursiveFrontier::new();
        frontier.add_candidate("a".to_string(), 1);
        // Same key, same weight → not new
        assert!(!frontier.add_candidate("a".to_string(), 1));
        assert_eq!(frontier.len(), 1);
    }

    #[test]
    fn frontier_add_candidate_weight_accumulation() {
        let mut frontier = RecursiveFrontier::new();
        frontier.add_candidate("a".to_string(), 1);
        frontier.add_candidate("a".to_string(), 1);
        assert_eq!(frontier.all.get("a"), Some(&2));
    }

    #[test]
    fn frontier_add_candidate_zero_weight() {
        let mut frontier = RecursiveFrontier::new();
        assert!(!frontier.add_candidate("a".to_string(), 0));
        assert_eq!(frontier.len(), 0);
    }

    #[test]
    fn frontier_advance() {
        let mut frontier = RecursiveFrontier::new();
        frontier.add_candidate("a".to_string(), 1);
        frontier.add_candidate("b".to_string(), 1);

        let delta = frontier.advance();
        assert_eq!(delta.len(), 2);
        assert!(frontier.is_empty());
        // all still contains the rows
        assert_eq!(frontier.len(), 2);
    }

    #[test]
    fn frontier_compact_removes_zero_weight() {
        let mut frontier = RecursiveFrontier::new();
        frontier.add_candidate("a".to_string(), 1);
        frontier.add_candidate("a".to_string(), -1); // net zero
        frontier.compact();
        assert_eq!(frontier.len(), 0);
    }

    #[test]
    fn frontier_semi_naive_simulation() {
        // Simulate a simple transitive closure: R(x,y) :- R(x,y)
        // seed: a→b, b→c
        // expected: a→b, b→c, a→c

        let seed = BTreeMap::from([("a→b".to_string(), 1), ("b→c".to_string(), 1)]);
        let mut frontier = RecursiveFrontier::from_seed(seed);

        // Iteration 1: delta = {a→b, b→c}
        // Join delta × all: a→b × b→c → a→c (new)
        let delta = frontier.advance();
        assert_eq!(delta.len(), 2);

        let mut new_rows = Vec::new();
        for key in &delta {
            if key == "a→b" {
                // Match with b→c
                new_rows.push(("a→c".to_string(), 1));
            }
        }

        for (key, weight) in new_rows {
            frontier.add_candidate(key, weight);
        }

        // Iteration 2: delta = {a→c}
        let delta = frontier.advance();
        assert_eq!(delta.len(), 1);
        assert_eq!(delta[0], "a→c");

        // No new candidates from a→c
        // Fixpoint reached
        assert!(frontier.is_empty());
        assert_eq!(frontier.len(), 3);
    }
}
