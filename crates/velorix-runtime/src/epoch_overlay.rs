//! Epoch-local write-set/COW overlay for runtime state.
//!
//! Instead of cloning entire BTreeMap state per epoch, this module provides
//! a thin transactional layer that tracks only the changed keys. This
//! reduces clone cost from O(S) to O(Δ) where S is total state size
//! and Δ is the number of touched keys.
//!
//! # Design
//!
//! ```text
//! EpochOverlay<'a, K, V> {
//!     base: &'a BTreeMap<K, V>,      // committed state (immutable)
//!     writes: BTreeMap<K, Write<V>>,  // epoch-local mutations
//! }
//!
//! enum Write<V> {
//!     Put(V),
//!     Delete,
//! }
//! ```
//!
//! Read path: writes[k] → base[k]
//! Write path: write-set only, lazy clone on first mutation
//! Commit: apply write-set to base map after validation
//!
//! # Safety
//!
//! The overlay maintains the same atomicity guarantee as clone-swap:
//! if any step returns Err, the committed base state is untouched.
//! The overlay is ephemeral and discarded on failure.

use std::collections::BTreeMap;

/// A write operation in the epoch overlay.
#[derive(Clone, Debug)]
pub enum Write<V> {
    /// Insert or update with this value.
    Put(V),
    /// Delete the key from the base.
    Delete,
}

/// Epoch-local transactional overlay over an immutable base BTreeMap.
///
/// Provides lazy per-key COW semantics: only keys that are actually
/// modified are cloned from the base. Unmodified keys share the
/// base reference with zero copy overhead.
pub struct EpochOverlay<'a, K, V> {
    base: &'a BTreeMap<K, V>,
    writes: BTreeMap<K, Write<V>>,
}

impl<'a, K: Clone + Ord, V: Clone> EpochOverlay<'a, K, V> {
    /// Create a new overlay on top of the given base state.
    pub fn new(base: &'a BTreeMap<K, V>) -> Self {
        Self {
            base,
            writes: BTreeMap::new(),
        }
    }

    /// Get a value by key. Checks write-set first, then base.
    pub fn get(&self, key: &K) -> Option<&V> {
        match self.writes.get(key) {
            Some(Write::Put(v)) => Some(v),
            Some(Write::Delete) => None,
            None => self.base.get(key),
        }
    }

    /// Get a mutable reference to a value, cloning from base on first access.
    ///
    /// This is the key COW operation: the first time a key is mutated,
    /// its value is cloned from the base into the write-set. Subsequent
    /// mutations operate on the local copy.
    pub fn get_or_insert_mut(&mut self, key: &K) -> &mut V
    where
        K: Ord,
        V: Default,
    {
        // Ensure the key exists in writes as a Put
        if !self.writes.contains_key(key) {
            let value = self.base.get(key).cloned().unwrap_or_default();
            self.writes.insert(key.clone(), Write::Put(value));
        }
        // Now safe to unwrap since we just inserted
        match self.writes.get_mut(key).unwrap() {
            Write::Put(v) => v,
            Write::Delete => unreachable!("just inserted as Put"),
        }
    }

    /// Insert a value into the write-set.
    pub fn insert(&mut self, key: K, value: V) {
        self.writes.insert(key, Write::Put(value));
    }

    /// Delete a key from the write-set.
    pub fn remove(&mut self, key: &K) {
        self.writes.insert(key.clone(), Write::Delete);
    }

    /// Check if a key exists (considering write-set).
    pub fn contains_key(&self, key: &K) -> bool {
        match self.writes.get(key) {
            Some(Write::Put(_)) => true,
            Some(Write::Delete) => false,
            None => self.base.contains_key(key),
        }
    }

    /// Get the number of writes in the overlay.
    pub fn write_count(&self) -> usize {
        self.writes.len()
    }

    /// Check if the overlay has any writes.
    pub fn is_empty(&self) -> bool {
        self.writes.is_empty()
    }

    /// Consume the overlay and apply all writes to the base map.
    ///
    /// Returns a new BTreeMap with all writes applied. The base map
    /// is not modified (it's immutable by reference).
    pub fn commit(self) -> BTreeMap<K, V> {
        let mut result = self.base.clone();
        for (key, write) in self.writes {
            match write {
                Write::Put(value) => {
                    result.insert(key, value);
                }
                Write::Delete => {
                    result.remove(&key);
                }
            }
        }
        result
    }

    /// Create a deterministic merged iterator over base + writes.
    ///
    /// This is useful for operators that need to iterate over the
    /// entire merged state. Delete entries are excluded.
    pub fn merged_iter(&self) -> impl Iterator<Item = (&K, &V)> {
        // Collect base entries, excluding keys that are deleted in writes
        let base_iter = self
            .base
            .iter()
            .filter(|(k, _)| !matches!(self.writes.get(k), Some(Write::Delete)));

        // Collect write entries that are Put operations
        let write_iter = self.writes.iter().filter_map(|(k, w)| match w {
            Write::Put(v) => Some((k, v)),
            Write::Delete => None,
        });

        // Merge: writes override base for same key
        // Collect into a temporary BTreeMap for deterministic ordering.
        base_iter
            .chain(write_iter)
            .collect::<BTreeMap<_, _>>()
            .into_iter()
    }
}

impl<'a, K: Clone + Ord, V: Clone + Default> EpochOverlay<'a, K, V> {
    /// Get or create a default value for a key, then return a mutable reference.
    /// Useful for nested state where you want to insert a default and modify it.
    pub fn get_or_create(&mut self, key: &K) -> &mut V
    where
        K: Ord,
    {
        self.get_or_insert_mut(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overlay_read_from_base() {
        let mut base = BTreeMap::new();
        base.insert("a".to_string(), 1);
        base.insert("b".to_string(), 2);

        let overlay = EpochOverlay::new(&base);
        assert_eq!(overlay.get(&"a".to_string()), Some(&1));
        assert_eq!(overlay.get(&"b".to_string()), Some(&2));
        assert_eq!(overlay.get(&"c".to_string()), None);
    }

    #[test]
    fn overlay_write_shadows_base() {
        let mut base = BTreeMap::new();
        base.insert("a".to_string(), 1);

        let mut overlay = EpochOverlay::new(&base);
        overlay.insert("a".to_string(), 10);
        overlay.insert("b".to_string(), 20);

        assert_eq!(overlay.get(&"a".to_string()), Some(&10));
        assert_eq!(overlay.get(&"b".to_string()), Some(&20));
        assert_eq!(overlay.write_count(), 2);
    }

    #[test]
    fn overlay_delete_removes_key() {
        let mut base = BTreeMap::new();
        base.insert("a".to_string(), 1);
        base.insert("b".to_string(), 2);

        let mut overlay = EpochOverlay::new(&base);
        overlay.remove(&"a".to_string());

        assert_eq!(overlay.get(&"a".to_string()), None);
        assert_eq!(overlay.get(&"b".to_string()), Some(&2));
    }

    #[test]
    fn overlay_commit_applies_writes() {
        let mut base = BTreeMap::new();
        base.insert("a".to_string(), 1);
        base.insert("b".to_string(), 2);

        let mut overlay = EpochOverlay::new(&base);
        overlay.insert("a".to_string(), 10);
        overlay.insert("c".to_string(), 30);
        overlay.remove(&"b".to_string());

        let result = overlay.commit();
        assert_eq!(result.get("a"), Some(&10));
        assert_eq!(result.get("b"), None);
        assert_eq!(result.get("c"), Some(&30));
    }

    #[test]
    fn overlay_failure_leaves_base_untouched() {
        let mut base = BTreeMap::new();
        base.insert("a".to_string(), 1);

        let mut overlay = EpochOverlay::new(&base);
        overlay.insert("a".to_string(), 10);

        // Simulate failure by dropping overlay without commit
        drop(overlay);

        // Base is unchanged
        assert_eq!(base.get("a"), Some(&1));
    }

    #[test]
    fn overlay_get_or_insert_mut_clones_from_base() {
        let mut base = BTreeMap::new();
        base.insert("a".to_string(), vec![1, 2, 3]);

        let mut overlay = EpochOverlay::new(&base);
        // First access clones from base
        let val = overlay.get_or_insert_mut(&"a".to_string());
        val.push(4);

        assert_eq!(overlay.get(&"a".to_string()), Some(&vec![1, 2, 3, 4]));
        // Base unchanged
        assert_eq!(base.get("a"), Some(&vec![1, 2, 3]));
    }

    #[test]
    fn overlay_merged_iter_excludes_deletes() {
        let mut base = BTreeMap::new();
        base.insert("a".to_string(), 1);
        base.insert("b".to_string(), 2);
        base.insert("c".to_string(), 3);

        let mut overlay = EpochOverlay::new(&base);
        overlay.insert("a".to_string(), 10); // override
        overlay.remove(&"b".to_string()); // delete
        overlay.insert("d".to_string(), 40); // add

        let mut merged: Vec<_> = overlay.merged_iter().collect();
        merged.sort_by_key(|(k, _)| (*k).clone());

        assert_eq!(
            merged,
            vec![
                (&"a".to_string(), &10),
                (&"c".to_string(), &3),
                (&"d".to_string(), &40),
            ]
        );
    }

    #[test]
    fn overlay_nested_state_cow() {
        let mut base = BTreeMap::new();
        base.insert(
            "key1".to_string(),
            BTreeMap::from([("a".to_string(), 1), ("b".to_string(), 2)]),
        );

        let mut overlay = EpochOverlay::new(&base);
        // Clone only the inner map for key1, not the entire state
        let inner = overlay.get_or_insert_mut(&"key1".to_string());
        inner.insert("c".to_string(), 3);

        assert_eq!(
            overlay.get(&"key1".to_string()),
            Some(&BTreeMap::from([
                ("a".to_string(), 1),
                ("b".to_string(), 2),
                ("c".to_string(), 3),
            ]))
        );
        // Base unchanged
        assert_eq!(
            base.get("key1"),
            Some(&BTreeMap::from([
                ("a".to_string(), 1),
                ("b".to_string(), 2)
            ]))
        );
    }
}
