use std::{path::Path as FsPath, sync::Arc};

use foyer::{BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCache, HybridCacheBuilder};
use object_store::{path::Path, ObjectStore};
use thiserror::Error;
use velorix_storage::object_key::ObjectKey;

#[derive(Debug, Error)]
pub enum CacheError {
    #[error("object store error: {0}")]
    ObjectStore(#[from] object_store::Error),
    #[error("foyer cache error: {0}")]
    Foyer(#[from] foyer::Error),
}

#[derive(Debug)]
pub struct HybridLocalCache {
    store: Arc<dyn ObjectStore>,
    cache: HybridCache<String, Vec<u8>>,
}

impl HybridLocalCache {
    pub async fn open(
        store: Arc<dyn ObjectStore>,
        cache_dir: impl AsRef<FsPath>,
        memory_capacity_bytes: usize,
        disk_capacity_bytes: usize,
    ) -> Result<Self, CacheError> {
        let device = FsDeviceBuilder::new(cache_dir)
            .with_capacity(disk_capacity_bytes)
            .build()?;
        let cache = HybridCacheBuilder::new()
            .with_name("velorix-runtime-local-cache")
            .memory(memory_capacity_bytes)
            .with_weighter(|_key, value: &Vec<u8>| value.len())
            .storage()
            .with_engine_config(BlockEngineConfig::new(device))
            .build()
            .await?;

        Ok(Self { store, cache })
    }

    pub async fn fetch(&self, key: &ObjectKey) -> Result<Vec<u8>, CacheError> {
        let object_path = Path::from(key.as_str());
        self.store.head(&object_path).await?;

        let cache_key = key.as_str().to_owned();
        let store = Arc::clone(&self.store);
        let fetch_path = object_path.clone();
        let entry = self
            .cache
            .get_or_fetch(&cache_key, move || async move {
                let bytes = store.get(&fetch_path).await?.bytes().await?;
                Ok::<Vec<u8>, object_store::Error>(bytes.to_vec())
            })
            .await?;

        Ok(entry.value().clone())
    }

    pub async fn close(&self) -> Result<(), CacheError> {
        self.cache.close().await?;
        Ok(())
    }
}
