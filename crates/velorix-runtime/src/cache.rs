use std::{
    collections::{HashMap, VecDeque},
    fs,
    path::{Path as FsPath, PathBuf},
    sync::{Arc, Mutex},
};

use object_store::{path::Path, ObjectStore};
use thiserror::Error;
use velorix_storage::object_key::ObjectKey;

#[derive(Debug, Error)]
pub enum CacheError {
    #[error("cache io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("object store error: {0}")]
    ObjectStore(#[from] object_store::Error),
    #[error("cache memory lock poisoned")]
    PoisonedLock,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CacheStats {
    pub memory_entries: usize,
    pub memory_bytes: usize,
}

#[derive(Debug)]
pub struct HybridLocalCache {
    store: Arc<dyn ObjectStore>,
    cache_dir: PathBuf,
    memory_capacity_bytes: usize,
    memory: Mutex<MemoryCache>,
}

#[derive(Debug, Default)]
struct MemoryCache {
    entries: HashMap<ObjectKey, Vec<u8>>,
    order: VecDeque<ObjectKey>,
    bytes: usize,
}

impl HybridLocalCache {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        cache_dir: impl AsRef<FsPath>,
        memory_capacity_bytes: usize,
    ) -> Result<Self, CacheError> {
        let cache_dir = cache_dir.as_ref().to_path_buf();
        fs::create_dir_all(&cache_dir)?;

        Ok(Self {
            store,
            cache_dir,
            memory_capacity_bytes,
            memory: Mutex::new(MemoryCache::default()),
        })
    }

    pub async fn fetch(&self, key: &ObjectKey) -> Result<Vec<u8>, CacheError> {
        let object_path = Path::from(key.as_str());
        self.store.head(&object_path).await?;

        if let Some(bytes) = self.memory_entry(key)? {
            return Ok(bytes);
        }

        let disk_path = self.disk_path(key);
        if disk_path.exists() {
            let bytes = fs::read(&disk_path)?;
            self.insert_memory(key.clone(), bytes.clone())?;
            return Ok(bytes);
        }

        let bytes = self.store.get(&object_path).await?.bytes().await?.to_vec();
        self.write_disk(key, &bytes)?;
        self.insert_memory(key.clone(), bytes.clone())?;

        Ok(bytes)
    }

    pub fn contains_memory(&self, key: &ObjectKey) -> bool {
        self.memory
            .lock()
            .map(|memory| memory.entries.contains_key(key))
            .unwrap_or(false)
    }

    pub fn stats(&self) -> CacheStats {
        self.memory
            .lock()
            .map(|memory| CacheStats {
                memory_entries: memory.entries.len(),
                memory_bytes: memory.bytes,
            })
            .unwrap_or(CacheStats {
                memory_entries: 0,
                memory_bytes: 0,
            })
    }

    pub fn disk_path(&self, key: &ObjectKey) -> PathBuf {
        self.cache_dir.join(key.as_str())
    }

    fn memory_entry(&self, key: &ObjectKey) -> Result<Option<Vec<u8>>, CacheError> {
        let memory = self.memory.lock().map_err(|_| CacheError::PoisonedLock)?;
        Ok(memory.entries.get(key).cloned())
    }

    fn insert_memory(&self, key: ObjectKey, bytes: Vec<u8>) -> Result<(), CacheError> {
        if bytes.len() > self.memory_capacity_bytes {
            return Ok(());
        }

        let mut memory = self.memory.lock().map_err(|_| CacheError::PoisonedLock)?;
        if let Some(previous) = memory.entries.remove(&key) {
            memory.bytes -= previous.len();
            memory.order.retain(|existing| existing != &key);
        }

        memory.bytes += bytes.len();
        memory.entries.insert(key.clone(), bytes);
        memory.order.push_back(key);

        while memory.bytes > self.memory_capacity_bytes {
            let Some(evicted_key) = memory.order.pop_front() else {
                break;
            };
            if let Some(evicted) = memory.entries.remove(&evicted_key) {
                memory.bytes -= evicted.len();
            }
        }

        Ok(())
    }

    fn write_disk(&self, key: &ObjectKey, bytes: &[u8]) -> Result<(), CacheError> {
        let disk_path = self.disk_path(key);
        if let Some(parent) = disk_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(disk_path, bytes)?;
        Ok(())
    }
}
