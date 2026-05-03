use std::sync::Arc;

use bytes::Bytes;
use object_store::{local::LocalFileSystem, path::Path, ObjectStore};
use tempfile::TempDir;
use velorix_runtime::cache::HybridLocalCache;
use velorix_storage::object_key::ObjectKey;

fn temp_store() -> (TempDir, Arc<dyn ObjectStore>) {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();

    (temp_dir, Arc::new(store))
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

#[tokio::test]
async fn cache_fetch_returns_memory_hit_when_object_remains_authoritative() {
    let (_store_dir, store) = temp_store();
    let cache_dir = tempfile::tempdir().unwrap();
    let cache = HybridLocalCache::new(Arc::clone(&store), cache_dir.path(), 64).unwrap();
    let key = ingest_key(0, 10);
    put_object(store.as_ref(), &key, b"orders-0-10").await;

    let first = cache.fetch(&key).await.unwrap();
    let second = cache.fetch(&key).await.unwrap();

    assert_eq!(first, b"orders-0-10");
    assert_eq!(second, b"orders-0-10");
    assert!(cache.contains_memory(&key));
    assert_eq!(cache.stats().memory_entries, 1);
}

#[tokio::test]
async fn cache_fetch_spills_objects_to_runtime_local_disk_cache() {
    let (_store_dir, store) = temp_store();
    let cache_dir = tempfile::tempdir().unwrap();
    let cache = HybridLocalCache::new(Arc::clone(&store), cache_dir.path(), 4).unwrap();
    let key = ingest_key(10, 20);
    put_object(store.as_ref(), &key, b"larger-than-memory").await;

    let fetched = cache.fetch(&key).await.unwrap();

    assert_eq!(fetched, b"larger-than-memory");
    assert!(!cache.contains_memory(&key));
    assert!(cache.disk_path(&key).exists());
}

#[tokio::test]
async fn cache_fetch_evicts_oldest_memory_entries_to_stay_within_byte_capacity() {
    let (_store_dir, store) = temp_store();
    let cache_dir = tempfile::tempdir().unwrap();
    let cache = HybridLocalCache::new(Arc::clone(&store), cache_dir.path(), 8).unwrap();
    let first = ingest_key(20, 30);
    let second = ingest_key(30, 40);
    put_object(store.as_ref(), &first, b"first").await;
    put_object(store.as_ref(), &second, b"second").await;

    cache.fetch(&first).await.unwrap();
    cache.fetch(&second).await.unwrap();

    assert!(!cache.contains_memory(&first));
    assert!(cache.contains_memory(&second));
    assert!(cache.stats().memory_bytes <= 8);
}

#[tokio::test]
async fn cache_restart_starts_with_empty_memory_without_relying_on_disk_cache() {
    let (_store_dir, store) = temp_store();
    let cache_dir = tempfile::tempdir().unwrap();
    let key = ingest_key(40, 50);
    put_object(store.as_ref(), &key, b"after-restart").await;

    let first_cache = HybridLocalCache::new(Arc::clone(&store), cache_dir.path(), 64).unwrap();
    first_cache.fetch(&key).await.unwrap();
    assert!(first_cache.contains_memory(&key));

    let restarted_cache = HybridLocalCache::new(Arc::clone(&store), cache_dir.path(), 64).unwrap();
    assert!(!restarted_cache.contains_memory(&key));

    std::fs::remove_file(restarted_cache.disk_path(&key)).unwrap();
    let fetched = restarted_cache.fetch(&key).await.unwrap();

    assert_eq!(fetched, b"after-restart");
    assert!(restarted_cache.contains_memory(&key));
}

#[tokio::test]
async fn cache_fetch_falls_back_to_object_storage_when_disk_cache_file_is_missing() {
    let (_store_dir, store) = temp_store();
    let cache_dir = tempfile::tempdir().unwrap();
    let cache = HybridLocalCache::new(Arc::clone(&store), cache_dir.path(), 1).unwrap();
    let key = ingest_key(50, 60);
    put_object(store.as_ref(), &key, b"authoritative").await;

    cache.fetch(&key).await.unwrap();
    std::fs::remove_file(cache.disk_path(&key)).unwrap();

    let fetched = cache.fetch(&key).await.unwrap();

    assert_eq!(fetched, b"authoritative");
    assert!(cache.disk_path(&key).exists());
}

#[tokio::test]
async fn cache_fetch_errors_when_object_storage_is_missing_even_with_stale_cache_entries() {
    let (_store_dir, store) = temp_store();
    let cache_dir = tempfile::tempdir().unwrap();
    let cache = HybridLocalCache::new(Arc::clone(&store), cache_dir.path(), 64).unwrap();
    let key = ingest_key(60, 70);
    put_object(store.as_ref(), &key, b"stale").await;

    cache.fetch(&key).await.unwrap();
    store.delete(&Path::from(key.as_str())).await.unwrap();

    let err = cache.fetch(&key).await.unwrap_err();

    assert!(err.to_string().contains("object store"));
}
