//! Disk-based operator state management using foyer (hybrid memory/disk cache).
//!
//! Each incremental operator (Join, Aggregate, Distinct, etc.) stores its state
//! on disk via foyer's `HybridCache`, with a configurable hot set in memory.
//! This allows state that exceeds RAM to spill to disk transparently.

use std::path::PathBuf;

use foyer::{
    BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCache, HybridCacheBuilder,
};
use thiserror::Error;
use velorix_core::circuit::NodeId;
use velorix_core::delta::DeltaBatch;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the disk-backed operator state store.
#[derive(Clone, Debug)]
pub struct DiskStateConfig {
    /// Directory for foyer's block engine files.
    pub state_dir: PathBuf,
    /// Maximum bytes to keep in memory (hot set).
    pub memory_capacity_bytes: usize,
    /// Maximum bytes to use on disk.
    pub disk_capacity_bytes: usize,
}

impl DiskStateConfig {
    pub fn new(
        state_dir: impl Into<PathBuf>,
        memory_capacity_bytes: usize,
        disk_capacity_bytes: usize,
    ) -> Self {
        Self {
            state_dir: state_dir.into(),
            memory_capacity_bytes,
            disk_capacity_bytes,
        }
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug, Error)]
pub enum DiskStateError {
    #[error("foyer error: {0}")]
    Foyer(#[from] foyer::Error),
    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("delta error: {0}")]
    Delta(#[from] velorix_core::delta::DeltaError),
}

// ---------------------------------------------------------------------------
// OperatorStateStore
// ---------------------------------------------------------------------------

/// A persistent key-value store for operator state, backed by foyer's
/// hybrid cache (memory + disk).
///
/// Keys are operator identifiers (e.g., `"node-3-left-state"` for a Join's
/// left side). Values are serialized `DeltaBatch` data representing the
/// operator's accumulated state.
pub struct OperatorStateStore {
    cache: HybridCache<String, Vec<u8>>,
}

impl Default for OperatorStateStore {
    fn default() -> Self {
        // Use a temporary directory for in-memory-only cache
        let temp_dir = std::env::temp_dir().join(format!("velorix-state-{}", std::process::id()));
        std::fs::create_dir_all(&temp_dir).ok();
        let device = FsDeviceBuilder::new(&temp_dir)
            .with_capacity(1024 * 1024) // 1MB
            .build()
            .expect("failed to build default device");
        let cache = futures::executor::block_on(
            HybridCacheBuilder::new()
                .with_name("velorix-default-state")
                .memory(1024 * 1024) // 1MB in-memory
                .with_weighter(|_key, value: &Vec<u8>| value.len())
                .storage()
                .with_engine_config(BlockEngineConfig::new(device))
                .build()
        ).expect("failed to build default cache");
        Self { cache }
    }
}

impl OperatorStateStore {
    /// Open (or create) the state store at the given directory.
    pub async fn open(config: &DiskStateConfig) -> Result<Self, DiskStateError> {
        let device = FsDeviceBuilder::new(&config.state_dir)
            .with_capacity(config.disk_capacity_bytes)
            .build()?;

        let cache = HybridCacheBuilder::new()
            .with_name("velorix-incremental-state")
            .memory(config.memory_capacity_bytes)
            .with_weighter(|_key, value: &Vec<u8>| value.len())
            .storage()
            .with_engine_config(BlockEngineConfig::new(device))
            .build()
            .await?;

        Ok(Self { cache })
    }

    /// Load the state for the given operator, if any.
    pub async fn load(&self, operator_key: &str) -> Result<Option<DeltaBatch>, DiskStateError> {
        match self.cache.get(operator_key).await? {
            Some(entry) => {
                let batch: DeltaBatch = serde_json::from_slice(entry.value())?;
                Ok(Some(batch))
            }
            None => Ok(None),
        }
    }

    /// Persist the full state for the given operator.
    pub fn save(&self, operator_key: &str, state: &DeltaBatch) -> Result<(), DiskStateError> {
        let bytes = serde_json::to_vec(state)?;
        self.cache.insert(operator_key.to_string(), bytes);
        Ok(())
    }

    /// Update the state incrementally: load existing, apply delta, save.
    ///
    /// This is the core incremental state update pattern:
    /// ```text
    /// new_state = old_state.combine(delta).net_rows()
    /// ```
    pub async fn apply_delta(
        &self,
        operator_key: &str,
        delta: &DeltaBatch,
    ) -> Result<DeltaBatch, DiskStateError> {
        let existing = self.load(operator_key).await?.unwrap_or_default();
        let combined = existing.combine(delta);
        let net = DeltaBatch::from_records(combined.net_rows()?);
        self.save(operator_key, &net)?;
        Ok(net)
    }

    /// Remove the state for the given operator.
    pub fn remove(&self, operator_key: &str) -> Result<(), DiskStateError> {
        self.cache.remove(operator_key);
        Ok(())
    }

    /// Flush all pending writes to disk.
    pub async fn flush(&self) -> Result<(), DiskStateError> {
        self.cache.close().await?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// OperatorStateKey helper
// ---------------------------------------------------------------------------

/// Generate a deterministic state key for an operator node.
pub fn operator_state_key(node_id: NodeId, suffix: &str) -> String {
    format!("op-{node_id}-{suffix}")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use velorix_core::delta::{DeltaKey, DeltaRecord, DeltaValue};
    use serde_json::json;

    #[tokio::test]
    async fn save_and_load_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let config = DiskStateConfig::new(dir.path(), 1024 * 1024, 10 * 1024 * 1024);
        let store = OperatorStateStore::open(&config).await.unwrap();

        let batch = DeltaBatch::from_records(vec![
            DeltaRecord::new(
                DeltaKey::from_json(json!("k1")),
                DeltaValue::from_json(json!({"v": 1})),
                1,
            ),
        ]);

        store.save("test-op", &batch).unwrap();
        let loaded = store.load("test-op").await.unwrap().unwrap();
        assert_eq!(loaded.records().len(), 1);
    }

    #[tokio::test]
    async fn apply_delta_accumulates_state() {
        let dir = tempfile::tempdir().unwrap();
        let config = DiskStateConfig::new(dir.path(), 1024 * 1024, 10 * 1024 * 1024);
        let store = OperatorStateStore::open(&config).await.unwrap();

        let delta1 = DeltaBatch::from_records(vec![
            DeltaRecord::new(
                DeltaKey::from_json(json!("k1")),
                DeltaValue::from_json(json!({"sum": 10})),
                1,
            ),
        ]);
        store.apply_delta("agg", &delta1).await.unwrap();

        // Same key and value: weight accumulates
        let delta2 = DeltaBatch::from_records(vec![
            DeltaRecord::new(
                DeltaKey::from_json(json!("k1")),
                DeltaValue::from_json(json!({"sum": 10})),
                1,
            ),
        ]);
        let state = store.apply_delta("agg", &delta2).await.unwrap();

        // Two inserts of the same key-value pair: weight becomes 2
        assert_eq!(state.records().len(), 1);
        assert_eq!(state.records()[0].weight, 2);
    }

    #[tokio::test]
    async fn remove_clears_state() {
        let dir = tempfile::tempdir().unwrap();
        let config = DiskStateConfig::new(dir.path(), 1024 * 1024, 10 * 1024 * 1024);
        let store = OperatorStateStore::open(&config).await.unwrap();

        let batch = DeltaBatch::from_records(vec![
            DeltaRecord::new(
                DeltaKey::from_json(json!("k1")),
                DeltaValue::from_json(json!({"v": 1})),
                1,
            ),
        ]);
        store.save("test", &batch).unwrap();
        assert!(store.load("test").await.unwrap().is_some());

        store.remove("test").unwrap();
        assert!(store.load("test").await.unwrap().is_none());
    }
}
