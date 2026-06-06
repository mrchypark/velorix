use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use object_store::{local::LocalFileSystem, path::Path, ObjectStore};
use tempfile::TempDir;
use velorix_runtime::cache::HybridLocalCache;
use velorix_storage::capability::{
    probe_authoritative_object_store_capabilities, AuthoritativeNamespace,
    AuthoritativeObjectStoreCapabilitiesV1, ObjectStoreCapabilityProfile,
    RequiredObjectStoreCapability,
};
use velorix_storage::object_key::ObjectKey;

const MEMORY_CAPACITY_BYTES: usize = 64;
const DISK_CAPACITY_BYTES: usize = 256 * 1024 * 1024;

#[derive(Debug)]
struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    get_count: AtomicUsize,
    head_count: AtomicUsize,
}

impl CountingStore {
    fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            get_count: AtomicUsize::new(0),
            head_count: AtomicUsize::new(0),
        }
    }

    fn get_count(&self) -> usize {
        self.get_count.load(Ordering::SeqCst)
    }

    fn head_count(&self) -> usize {
        self.head_count.load(Ordering::SeqCst)
    }
}

impl std::fmt::Display for CountingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CountingStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        if options.head {
            self.head_count.fetch_add(1, Ordering::SeqCst);
        } else {
            self.get_count.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.get_opts(location, options).await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.inner.delete(location).await
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

fn temp_store() -> (TempDir, Arc<dyn ObjectStore>) {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();

    (temp_dir, Arc::new(store))
}

async fn open_cache(
    store: Arc<dyn ObjectStore>,
    cache_dir: &std::path::Path,
    memory_capacity_bytes: usize,
) -> HybridLocalCache {
    HybridLocalCache::open(store, cache_dir, memory_capacity_bytes, DISK_CAPACITY_BYTES)
        .await
        .unwrap()
}

async fn put_object(store: &dyn ObjectStore, key: &ObjectKey, payload: &'static [u8]) {
    store
        .put(
            &Path::from(key.as_str()),
            Bytes::from_static(payload).into(),
        )
        .await
        .unwrap();
}

fn ingest_key(start: u64, end: u64) -> ObjectKey {
    ObjectKey::ingest_batch("orders", 0, start, end).unwrap()
}

fn all_namespace_capabilities() -> AuthoritativeObjectStoreCapabilitiesV1 {
    AuthoritativeObjectStoreCapabilitiesV1::new(
        AuthoritativeNamespace::all()
            .into_iter()
            .map(|namespace| (namespace, ObjectStoreCapabilityProfile::local_development()))
            .collect(),
    )
}

fn capabilities_missing(
    namespace: AuthoritativeNamespace,
) -> AuthoritativeObjectStoreCapabilitiesV1 {
    let mut profiles = all_namespace_capabilities().profiles;
    profiles.remove(&namespace);

    AuthoritativeObjectStoreCapabilitiesV1::new(profiles)
}

fn capabilities_missing_required(
    namespace: AuthoritativeNamespace,
    required_capability: RequiredObjectStoreCapability,
) -> AuthoritativeObjectStoreCapabilitiesV1 {
    let mut profiles = all_namespace_capabilities().profiles;
    let mut profile = ObjectStoreCapabilityProfile::local_development();
    match required_capability {
        RequiredObjectStoreCapability::ConditionalCreate => profile.conditional_create = false,
        RequiredObjectStoreCapability::ConditionalUpdate => profile.conditional_update = false,
        RequiredObjectStoreCapability::AtomicVisibility => profile.atomic_visibility = false,
        RequiredObjectStoreCapability::ListAfterWrite => profile.list_after_write = false,
        RequiredObjectStoreCapability::ReadAfterWrite => profile.read_after_write = false,
    }
    profiles.insert(namespace, profile);

    AuthoritativeObjectStoreCapabilitiesV1::new(profiles)
}

#[tokio::test]
async fn production_cache_open_rejects_missing_namespace_capability_evidence() {
    let (_store_dir, store) = temp_store();
    let cache_dir = tempfile::tempdir().unwrap();
    let capabilities = capabilities_missing(AuthoritativeNamespace::Ingest);

    let err = HybridLocalCache::open_production(
        store,
        cache_dir.path(),
        MEMORY_CAPACITY_BYTES,
        DISK_CAPACITY_BYTES,
        &capabilities,
    )
    .await
    .unwrap_err();

    assert!(err.to_string().contains("namespace `ingest` is missing"));
}

#[tokio::test]
async fn production_cache_open_rejects_weak_namespace_capability_evidence() {
    let (_store_dir, store) = temp_store();
    let cache_dir = tempfile::tempdir().unwrap();
    let capabilities = capabilities_missing_required(
        AuthoritativeNamespace::Output,
        RequiredObjectStoreCapability::ConditionalCreate,
    );

    let err = HybridLocalCache::open_production(
        store,
        cache_dir.path(),
        MEMORY_CAPACITY_BYTES,
        DISK_CAPACITY_BYTES,
        &capabilities,
    )
    .await
    .unwrap_err();

    assert!(err.to_string().contains("namespace `output`"));
    assert!(err.to_string().contains("conditional_create"));
}

#[tokio::test]
async fn production_cache_fetches_after_startup_capabilities_are_validated() {
    let (_store_dir, store) = temp_store();
    let cache_dir = tempfile::tempdir().unwrap();
    let capabilities =
        probe_authoritative_object_store_capabilities(store.as_ref(), "local-test", "v1/probes")
            .await
            .unwrap();
    let cache = HybridLocalCache::open_production(
        Arc::clone(&store),
        cache_dir.path(),
        MEMORY_CAPACITY_BYTES,
        DISK_CAPACITY_BYTES,
        &capabilities,
    )
    .await
    .unwrap();
    let key = ingest_key(70, 80);
    put_object(store.as_ref(), &key, b"production-authoritative").await;

    let fetched = cache.fetch(&key).await.unwrap();

    assert_eq!(fetched, b"production-authoritative");
    cache.close().await.unwrap();
}

#[tokio::test]
async fn cache_fetch_returns_memory_hit_when_object_remains_authoritative() {
    let (_store_dir, inner_store) = temp_store();
    let counting_store = Arc::new(CountingStore::new(inner_store));
    let store: Arc<dyn ObjectStore> = counting_store.clone();
    let cache_dir = tempfile::tempdir().unwrap();
    let cache = open_cache(Arc::clone(&store), cache_dir.path(), MEMORY_CAPACITY_BYTES).await;
    let key = ingest_key(0, 10);
    put_object(store.as_ref(), &key, b"orders-0-10").await;

    let first = cache.fetch(&key).await.unwrap();
    let second = cache.fetch(&key).await.unwrap();

    assert_eq!(first, b"orders-0-10");
    assert_eq!(second, b"orders-0-10");
    assert_eq!(counting_store.head_count(), 2);
    assert_eq!(counting_store.get_count(), 1);
    cache.close().await.unwrap();
}

#[tokio::test]
async fn cache_fetch_handles_objects_larger_than_memory_capacity() {
    let (_store_dir, store) = temp_store();
    let cache_dir = tempfile::tempdir().unwrap();
    let cache = open_cache(Arc::clone(&store), cache_dir.path(), 4).await;
    let key = ingest_key(10, 20);
    put_object(store.as_ref(), &key, b"larger-than-memory").await;

    let fetched = cache.fetch(&key).await.unwrap();

    assert_eq!(fetched, b"larger-than-memory");
    cache.close().await.unwrap();
}

#[tokio::test]
async fn cache_fetch_delegates_capacity_eviction_to_foyer() {
    let (_store_dir, store) = temp_store();
    let cache_dir = tempfile::tempdir().unwrap();
    let cache = open_cache(Arc::clone(&store), cache_dir.path(), 8).await;
    let first = ingest_key(20, 30);
    let second = ingest_key(30, 40);
    put_object(store.as_ref(), &first, b"first").await;
    put_object(store.as_ref(), &second, b"second").await;

    let first_fetched = cache.fetch(&first).await.unwrap();
    let second_fetched = cache.fetch(&second).await.unwrap();

    assert_eq!(first_fetched, b"first");
    assert_eq!(second_fetched, b"second");
    cache.close().await.unwrap();
}

#[tokio::test]
async fn cache_restart_may_recover_only_after_authoritative_head_succeeds() {
    let (_store_dir, store) = temp_store();
    let cache_dir = tempfile::tempdir().unwrap();
    let key = ingest_key(40, 50);
    put_object(store.as_ref(), &key, b"after-restart").await;

    let first_cache = open_cache(Arc::clone(&store), cache_dir.path(), MEMORY_CAPACITY_BYTES).await;
    first_cache.fetch(&key).await.unwrap();
    first_cache.close().await.unwrap();

    let restarted_cache =
        open_cache(Arc::clone(&store), cache_dir.path(), MEMORY_CAPACITY_BYTES).await;
    store.delete(&Path::from(key.as_str())).await.unwrap();

    let err = restarted_cache.fetch(&key).await.unwrap_err();

    assert!(err.to_string().contains("object store"));
    restarted_cache.close().await.unwrap();
}

#[tokio::test]
async fn cache_fetch_uses_object_storage_when_cache_directory_is_independent() {
    let (_store_dir, store) = temp_store();
    let first_cache_dir = tempfile::tempdir().unwrap();
    let second_cache_dir = tempfile::tempdir().unwrap();
    let first_cache = open_cache(Arc::clone(&store), first_cache_dir.path(), 1).await;
    let key = ingest_key(50, 60);
    put_object(store.as_ref(), &key, b"authoritative").await;

    first_cache.fetch(&key).await.unwrap();
    first_cache.close().await.unwrap();

    let independent_cache = open_cache(Arc::clone(&store), second_cache_dir.path(), 1).await;
    let fetched = independent_cache.fetch(&key).await.unwrap();

    assert_eq!(fetched, b"authoritative");
    independent_cache.close().await.unwrap();
}

#[tokio::test]
async fn cache_fetch_errors_when_object_storage_is_missing_even_with_stale_cache_entries() {
    let (_store_dir, store) = temp_store();
    let cache_dir = tempfile::tempdir().unwrap();
    let cache = open_cache(Arc::clone(&store), cache_dir.path(), MEMORY_CAPACITY_BYTES).await;
    let key = ingest_key(60, 70);
    put_object(store.as_ref(), &key, b"stale").await;

    cache.fetch(&key).await.unwrap();
    store.delete(&Path::from(key.as_str())).await.unwrap();

    let err = cache.fetch(&key).await.unwrap_err();

    assert!(err.to_string().contains("object store"));
    cache.close().await.unwrap();
}
