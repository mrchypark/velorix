//! Disposable caches for immutable objects selected by fresh authoritative metadata.
//! Raw payloads are validated on read; decoded artifacts are admitted only after
//! canonical validation and rebound to fresh authority by their callers.

use std::sync::Arc;
use std::{fs::File, future::Future, path::PathBuf};

use anyhow::{ensure, Context};
use foyer::{
    BlockEngineConfig, Cache, DeviceBuilder, FsDeviceBuilder, HybridCache, HybridCacheBuilder,
    HybridCachePolicy, PsyncIoEngineConfig, RecoverMode,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug)]
pub struct MaterializedReadCacheConfig {
    pub directory: PathBuf,
    pub memory_bytes: usize,
    pub disk_bytes: usize,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct MaterializedReadCacheKey {
    pub store_id: String,
    pub namespace: String,
    pub tenant_id: String,
    pub program_id: String,
    pub output_id: String,
    pub object_path: String,
    pub content_hash: String,
    pub codec: String,
    pub checkpoint_epoch: u64,
    pub root_hash: String,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum DecodedArtifactKey {
    OutputPage {
        source: MaterializedReadCacheKey,
        schema_fingerprint: String,
    },
    CheckpointRecord {
        source: MaterializedReadCacheKey,
        tenant_id: String,
        program_id: String,
        view_id: String,
        record_codec: String,
    },
}

// Checkpoint output schema is authenticated by the immutable source content
// hash. Keep it out of the lookup key so callers with a different schema
// commitment domain (published relation vs checkpoint identity) can still
// reuse the decoded artifact; any requested checkpoint-schema assertion is
// checked against the decoded record below.

impl DecodedArtifactKey {
    fn weight(&self) -> usize {
        match self {
            Self::OutputPage {
                source,
                schema_fingerprint,
            } => std::mem::size_of::<Self>()
                .saturating_add(source.weight())
                .saturating_add(schema_fingerprint.capacity()),
            Self::CheckpointRecord {
                source,
                tenant_id,
                program_id,
                view_id,
                record_codec,
            } => std::mem::size_of::<Self>()
                .saturating_add(source.weight())
                .saturating_add(tenant_id.capacity())
                .saturating_add(program_id.capacity())
                .saturating_add(view_id.capacity())
                .saturating_add(record_codec.capacity()),
        }
    }
}

enum DecodedArtifact {
    OutputPage(Arc<super::materialized_output_pages::DecodedOutputPage>),
    CheckpointRecord {
        record: Arc<super::StandingRuntimeCheckpointRecord>,
        heap_weight_bytes: usize,
        encoded_size: u64,
    },
}

impl DecodedArtifact {
    fn weight(&self) -> usize {
        match self {
            Self::OutputPage(page) => page.cache_weight_bytes(),
            Self::CheckpointRecord {
                heap_weight_bytes, ..
            } => *heap_weight_bytes,
        }
    }
}

impl MaterializedReadCacheKey {
    fn weight(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.store_id.capacity()
            + self.namespace.capacity()
            + self.tenant_id.capacity()
            + self.program_id.capacity()
            + self.output_id.capacity()
            + self.object_path.capacity()
            + self.content_hash.capacity()
            + self.codec.capacity()
            + self.root_hash.capacity()
    }
}

impl MaterializedReadCacheConfig {
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            self.memory_bytes > 0 && self.memory_bytes <= isize::MAX as usize,
            "materialized read cache memory capacity must be positive and fit addressable memory"
        );
        ensure!(
            self.disk_bytes >= 8 * 1024 * 1024
                && self.disk_bytes <= isize::MAX as usize
                && self.disk_bytes.is_multiple_of(4096),
            "materialized read cache disk capacity must be at least 8 MiB, addressable and 4 KiB aligned"
        );
        ensure!(
            !self.directory.as_os_str().is_empty(),
            "materialized read cache directory is empty"
        );
        Ok(())
    }
}

pub struct MaterializedReadCache {
    cache: HybridCache<MaterializedReadCacheKey, Vec<u8>>,
    decoded_artifacts: Option<Cache<DecodedArtifactKey, DecodedArtifact>>,
    decoded_output_page_capacity_bytes: usize,
    // Foyer owns a directory of writable blocks; concurrent processes cannot share it.
    _directory_lock: File,
}

impl MaterializedReadCache {
    pub async fn open(config: MaterializedReadCacheConfig) -> anyhow::Result<Self> {
        config.validate()?;
        std::fs::create_dir_all(&config.directory)
            .context("create materialized read cache directory")?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&config.directory, std::fs::Permissions::from_mode(0o700))?;
        }
        let directory_lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(config.directory.join(".velorix-cache.lock"))?;
        directory_lock
            .try_lock()
            .context("materialized read cache directory is already in use")?;
        let block_bytes = (config.disk_bytes / 4).min(16 * 1024 * 1024) / 4096 * 4096;
        let device = FsDeviceBuilder::new(&config.directory)
            .with_capacity(config.disk_bytes)
            .build()?;
        // Decoded artifacts avoid repeated IPC validation/recompression under bounded memory.
        let decoded_page_memory = config.memory_bytes / 2;
        let page_cache_memory = config.memory_bytes - decoded_page_memory;
        let decoded_artifacts: Option<Cache<DecodedArtifactKey, DecodedArtifact>> =
            (decoded_page_memory > 0).then(|| {
                Cache::builder(decoded_page_memory)
                    .with_name("velorix-materialized-decoded-artifact")
                    .with_shards(4)
                    .with_weighter(|key: &DecodedArtifactKey, artifact: &DecodedArtifact| {
                        key.weight().saturating_add(artifact.weight())
                    })
                    .build()
            });
        let cache = HybridCacheBuilder::new()
            .with_name("velorix-materialized-read")
            .with_policy(HybridCachePolicy::WriteOnInsertion)
            .memory(page_cache_memory)
            .with_shards(4)
            .with_weighter(|key: &MaterializedReadCacheKey, bytes: &Vec<u8>| {
                key.weight()
                    .saturating_add(std::mem::size_of::<Vec<u8>>())
                    .saturating_add(bytes.capacity())
            })
            .storage()
            .with_io_engine_config(PsyncIoEngineConfig::new())
            .with_engine_config(
                BlockEngineConfig::new(device)
                    .with_block_size(block_bytes)
                    .with_buffer_pool_size(block_bytes)
                    .with_submit_queue_size_threshold(block_bytes)
                    .with_recover_concurrency(1),
            )
            .with_recover_mode(RecoverMode::Quiet)
            .build()
            .await?;
        Ok(Self {
            cache,
            decoded_artifacts,
            decoded_output_page_capacity_bytes: decoded_page_memory,
            _directory_lock: directory_lock,
        })
    }

    /// Read only previously validated decoded source pages from bounded memory.
    pub(crate) fn decoded_output_page(
        &self,
        source: &MaterializedReadCacheKey,
        schema_fingerprint: &str,
    ) -> Option<Arc<super::materialized_output_pages::DecodedOutputPage>> {
        let key = DecodedArtifactKey::OutputPage {
            source: source.clone(),
            schema_fingerprint: schema_fingerprint.to_owned(),
        };
        self.decoded_artifacts
            .as_ref()?
            .get(&key)
            .and_then(|entry| match entry.value() {
                DecodedArtifact::OutputPage(page) => Some(Arc::clone(page)),
                DecodedArtifact::CheckpointRecord { .. } => None,
            })
    }

    /// Insert only after the source page hash, schema, and row bindings validate.
    pub(crate) fn cache_decoded_output_page(
        &self,
        source: &MaterializedReadCacheKey,
        schema_fingerprint: &str,
        page: Arc<super::materialized_output_pages::DecodedOutputPage>,
    ) {
        let Some(cache) = &self.decoded_artifacts else {
            return;
        };
        cache.insert(
            DecodedArtifactKey::OutputPage {
                source: source.clone(),
                schema_fingerprint: schema_fingerprint.to_owned(),
            },
            DecodedArtifact::OutputPage(page),
        );
    }

    pub(crate) fn decoded_output_page_capacity_bytes(&self) -> usize {
        self.decoded_output_page_capacity_bytes
    }

    #[cfg(test)]
    pub(crate) fn decoded_artifact_usage_bytes(&self) -> usize {
        self.decoded_artifacts
            .as_ref()
            .map_or(0, |cache| cache.usage())
    }

    pub(crate) fn decoded_output_page_weight_bytes(
        &self,
        source: &MaterializedReadCacheKey,
        schema_fingerprint: &str,
        page: &super::materialized_output_pages::DecodedOutputPage,
    ) -> usize {
        DecodedArtifactKey::OutputPage {
            source: source.clone(),
            schema_fingerprint: schema_fingerprint.to_owned(),
        }
        .weight()
        .saturating_add(page.cache_weight_bytes())
    }

    pub(crate) fn decoded_checkpoint_record(
        &self,
        source: &MaterializedReadCacheKey,
        tenant_id: &str,
        program_id: &str,
        view_id: &str,
        expected_output_schema_hash: Option<&str>,
        record_codec: &str,
    ) -> Option<(Arc<super::StandingRuntimeCheckpointRecord>, u64)> {
        let key = DecodedArtifactKey::CheckpointRecord {
            source: source.clone(),
            tenant_id: tenant_id.to_owned(),
            program_id: program_id.to_owned(),
            view_id: view_id.to_owned(),
            record_codec: record_codec.to_owned(),
        };
        let cache = self.decoded_artifacts.as_ref()?;
        let started = std::time::Instant::now();
        let result = cache.get(&key).and_then(|entry| match entry.value() {
            DecodedArtifact::CheckpointRecord {
                record,
                encoded_size,
                ..
            } if expected_output_schema_hash.is_none_or(|expected| {
                record.checkpoint.identity.output_schema_hash == expected
            }) =>
            {
                Some((Arc::clone(record), *encoded_size))
            }
            DecodedArtifact::OutputPage(_) => None,
            _ => None,
        });
        if std::env::var_os("VELORIX_DEMAND_QUERY_DIAGNOSTICS").is_some() {
            eprintln!(
                "demand_query_stage stage=decoded_checkpoint_cache_lookup elapsed_us={} hit={}",
                started.elapsed().as_micros(),
                usize::from(result.is_some()),
            );
        }
        result
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn cache_decoded_checkpoint_record(
        &self,
        source: &MaterializedReadCacheKey,
        tenant_id: &str,
        program_id: &str,
        view_id: &str,
        record_codec: &str,
        record: Arc<super::StandingRuntimeCheckpointRecord>,
        encoded_size: u64,
    ) {
        let Some(cache) = &self.decoded_artifacts else {
            return;
        };
        let Some(decoded_size) =
            super::checkpoint_publication::checkpoint_record_heap_weight(&record)
        else {
            return;
        };
        let heap_weight_bytes = std::mem::size_of::<DecodedArtifact>()
            .saturating_add(std::mem::size_of_val(record.as_ref()))
            .saturating_add(decoded_size)
            .saturating_add(usize::try_from(encoded_size).unwrap_or(usize::MAX));
        cache.insert(
            DecodedArtifactKey::CheckpointRecord {
                source: source.clone(),
                tenant_id: tenant_id.to_owned(),
                program_id: program_id.to_owned(),
                view_id: view_id.to_owned(),
                record_codec: record_codec.to_owned(),
            },
            DecodedArtifact::CheckpointRecord {
                record,
                heap_weight_bytes,
                encoded_size,
            },
        );
    }

    /// Invalid disk entries and cache I/O errors are misses, never authoritative errors.
    pub async fn get<E>(
        &self,
        key: &MaterializedReadCacheKey,
        validate: impl Fn(&[u8]) -> Result<(), E>,
    ) -> Option<Vec<u8>> {
        let started = std::time::Instant::now();
        let result = match self.cache.get(key).await {
            Ok(Some(entry)) if validate(entry.value()).is_ok() => Some(entry.value().clone()),
            Ok(None) => None,
            _ => {
                self.remove(key);
                None
            }
        };
        if std::env::var_os("VELORIX_DEMAND_QUERY_DIAGNOSTICS").is_some() {
            eprintln!(
                "demand_query_stage stage=materialized_read_cache_get elapsed_us={} hit={}",
                started.elapsed().as_micros(),
                usize::from(result.is_some()),
            );
        }
        result
    }

    /// Content hashes can be codec-specific; insertion must use the production validator.
    pub fn insert<E>(
        &self,
        key: MaterializedReadCacheKey,
        bytes: Vec<u8>,
        validate: impl Fn(&[u8]) -> Result<(), E>,
    ) -> Result<(), E> {
        validate(&bytes)?;
        self.cache.insert(key, bytes);
        Ok(())
    }

    pub fn remove(&self, key: &MaterializedReadCacheKey) {
        // shortcut: Foyer may retain an in-flight write after remove; validate hits until strict deletion exists.
        self.cache.remove(key);
    }

    pub async fn read_through<E, F, Fut, V>(
        &self,
        key: MaterializedReadCacheKey,
        fetch: F,
        validate: V,
    ) -> Result<Vec<u8>, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Vec<u8>, E>>,
        V: Fn(&[u8]) -> Result<(), E>,
    {
        if let Some(bytes) = self.get(&key, &validate).await {
            return Ok(bytes);
        }
        let bytes = fetch().await?;
        self.insert(key, bytes.clone(), validate)?;
        Ok(bytes)
    }

    /// Drop all Arc handles after closing before reopening the same directory.
    pub async fn close(&self) -> anyhow::Result<()> {
        self.cache
            .close()
            .await
            .context("close materialized read cache")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn config(dir: &std::path::Path) -> MaterializedReadCacheConfig {
        MaterializedReadCacheConfig {
            directory: dir.to_owned(),
            memory_bytes: 64 * 1024,
            disk_bytes: 8 * 1024 * 1024,
        }
    }

    fn key() -> MaterializedReadCacheKey {
        MaterializedReadCacheKey {
            store_id: "s3-a".into(),
            namespace: "ns".into(),
            tenant_id: "tenant".into(),
            program_id: "program".into(),
            output_id: "output".into(),
            object_path: "immutable/page".into(),
            content_hash: "verified-hash".into(),
            codec: "verified-object-v1".into(),
            checkpoint_epoch: 7,
            root_hash: "root".into(),
        }
    }

    fn validate(bytes: &[u8]) -> Result<(), &'static str> {
        if bytes == b"verified payload" {
            Ok(())
        } else {
            Err("invalid payload hash/root")
        }
    }

    #[tokio::test]
    async fn memory_hit_and_key_isolation() {
        let dir = tempfile::tempdir().unwrap();
        let cache = MaterializedReadCache::open(config(dir.path()))
            .await
            .unwrap();
        let fetches = AtomicUsize::new(0);
        for _ in 0..2 {
            let bytes = cache
                .read_through(
                    key(),
                    || async {
                        fetches.fetch_add(1, Ordering::Relaxed);
                        Ok(b"verified payload".to_vec())
                    },
                    validate,
                )
                .await
                .unwrap();
            assert_eq!(bytes, b"verified payload");
        }
        assert_eq!(fetches.load(Ordering::Relaxed), 1);
        assert!(cache.cache.memory().get(&key()).is_some());
        for field in 0..10 {
            let mut other = key();
            match field {
                0 => other.store_id.push('x'),
                1 => other.namespace.push('x'),
                2 => other.tenant_id.push('x'),
                3 => other.program_id.push('x'),
                4 => other.output_id.push('x'),
                5 => other.object_path.push('x'),
                6 => other.content_hash.push('x'),
                7 => other.checkpoint_epoch += 1,
                8 => other.codec.push('x'),
                _ => other.root_hash.push('x'),
            }
            assert!(cache.get(&other, validate).await.is_none(), "field {field}");
        }
        let total_limit = 64 * 1024;
        assert!(cache.cache.memory().usage() <= total_limit / 2);
        assert!(cache.decoded_artifacts.as_ref().unwrap().usage() <= total_limit / 2);
        cache.close().await.unwrap();
    }

    #[tokio::test]
    async fn decoded_page_capacity_scales_with_configured_memory() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = config(dir.path());
        cfg.memory_bytes = 128 * 1024 * 1024;
        let total_limit = cfg.memory_bytes;
        let cache = MaterializedReadCache::open(cfg).await.unwrap();

        assert_eq!(cache.decoded_output_page_capacity_bytes(), total_limit / 2);
        assert!(cache.decoded_output_page_capacity_bytes() > 16 * 1024 * 1024);
        let decoded_usage = cache.decoded_artifacts.as_ref().unwrap().usage();
        let raw_usage = cache.cache.memory().usage();
        assert!(decoded_usage <= total_limit / 2);
        assert!(raw_usage <= total_limit / 2);
        assert!(decoded_usage.saturating_add(raw_usage) <= total_limit);
        cache.close().await.unwrap();
    }

    #[tokio::test]
    async fn disk_hit_after_close_reopen_and_remove() {
        let dir = tempfile::tempdir().unwrap();
        let cache = MaterializedReadCache::open(config(dir.path()))
            .await
            .unwrap();
        cache
            .insert(key(), b"verified payload".to_vec(), validate)
            .unwrap();
        cache.close().await.unwrap();
        drop(cache);
        let cache = MaterializedReadCache::open(config(dir.path()))
            .await
            .unwrap();
        assert!(cache.cache.memory().get(&key()).is_none());
        assert_eq!(
            cache.get(&key(), validate).await.unwrap(),
            b"verified payload"
        );
        cache.remove(&key());
        assert!(cache.get(&key(), validate).await.is_none());
        cache.close().await.unwrap();
    }

    #[tokio::test]
    async fn byte_weighted_memory_evicts() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = config(dir.path());
        let limit = cfg.memory_bytes;
        let cache = MaterializedReadCache::open(cfg).await.unwrap();
        for epoch in 0..512 {
            let mut k = key();
            k.checkpoint_epoch = epoch;
            cache
                .insert(k, vec![42; 1024], |_| Ok::<_, ()>(()))
                .unwrap();
        }
        assert!(cache.cache.memory().usage() <= limit);
        let present = (0..512)
            .filter(|epoch| {
                let mut k = key();
                k.checkpoint_epoch = *epoch;
                cache.cache.memory().get(&k).is_some()
            })
            .count();
        assert!(present > 0 && present < 512);
        cache.close().await.unwrap();
    }

    #[tokio::test]
    async fn corrupt_disk_payload_is_miss_and_source_is_validated() {
        let dir = tempfile::tempdir().unwrap();
        let cache = MaterializedReadCache::open(config(dir.path()))
            .await
            .unwrap();
        // Bypass only the helper to simulate a bad disk payload with a valid Foyer checksum.
        cache.cache.insert(key(), b"corrupt payload".to_vec());
        cache.close().await.unwrap();
        drop(cache);
        let cache = MaterializedReadCache::open(config(dir.path()))
            .await
            .unwrap();
        assert!(cache.get(&key(), validate).await.is_none());
        let bytes = cache
            .read_through(
                key(),
                || async { Ok(b"verified payload".to_vec()) },
                validate,
            )
            .await
            .unwrap();
        assert_eq!(bytes, b"verified payload");
        let mut source_key = key();
        source_key.object_path.push_str("/invalid-source");
        assert_eq!(
            cache
                .read_through(
                    source_key.clone(),
                    || async { Ok(b"corrupt source".to_vec()) },
                    validate
                )
                .await
                .unwrap_err(),
            "invalid payload hash/root"
        );
        assert!(cache.get(&source_key, validate).await.is_none());
        cache.close().await.unwrap();
    }

    #[tokio::test]
    async fn cache_directory_has_one_writer() {
        let dir = tempfile::tempdir().unwrap();
        let cache = MaterializedReadCache::open(config(dir.path()))
            .await
            .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(dir.path()).unwrap().permissions().mode() & 0o777,
                0o700
            );
        }
        assert!(MaterializedReadCache::open(config(dir.path()))
            .await
            .is_err());
        cache.close().await.unwrap();
        drop(cache);
        let reopened = MaterializedReadCache::open(config(dir.path()))
            .await
            .unwrap();
        reopened.close().await.unwrap();
    }

    #[test]
    fn rejects_invalid_capacity_before_opening_directory() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = config(&dir.path().join("not-created"));
        cfg.memory_bytes = 0;
        assert!(cfg.validate().is_err());
        cfg.memory_bytes = 64 * 1024;
        cfg.disk_bytes = 1024;
        assert!(cfg.validate().is_err());
        cfg.disk_bytes = 8 * 1024 * 1024 + 1;
        assert!(cfg.validate().is_err());
        cfg.disk_bytes = 8 * 1024 * 1024;
        assert!(cfg.validate().is_ok());
        assert!(!cfg.directory.exists());
    }
}
