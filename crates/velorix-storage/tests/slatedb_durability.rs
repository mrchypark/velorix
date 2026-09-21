//! These tests use the production wrapper and run against either SlateDB version.
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::{memory::InMemory, path::Path, *};
use std::{
    fmt,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::Semaphore;
use velorix_storage::{
    state::StateObjectWrite,
    state_store::{SlateDbStateStore, StateObjectStore},
};

#[derive(Debug)]
struct WalGate {
    inner: Arc<InMemory>,
    observed: Mutex<Vec<String>>,
    mode: AtomicU8,
    entered: Semaphore,
    release: Semaphore,
}

impl WalGate {
    fn new() -> Self {
        Self {
            inner: Arc::new(InMemory::new()),
            observed: Mutex::new(Vec::new()),
            mode: AtomicU8::new(0),
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
        }
    }
    async fn intercept(&self, path: &Path) -> object_store::Result<()> {
        if !path.as_ref().contains("/wal/") {
            return Ok(());
        }
        let mode = self.mode.load(Ordering::SeqCst);
        if mode == 0 {
            return Ok(());
        }
        self.observed.lock().unwrap().push(path.to_string());
        self.entered.add_permits(1);
        if mode == 1 {
            self.release.acquire().await.unwrap().forget();
            Ok(())
        } else {
            Err(object_store::Error::Generic {
                store: "durability-test",
                source: std::io::Error::other("injected WAL PUT failure").into(),
            })
        }
    }
}
impl fmt::Display for WalGate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "WAL durability gate")
    }
}

#[async_trait::async_trait]
impl ObjectStore for WalGate {
    async fn put_opts(
        &self,
        path: &Path,
        data: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.intercept(path).await?;
        self.inner.put_opts(path, data, options).await
    }
    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        // Block before any multipart bytes can be submitted.
        self.intercept(path).await?;
        self.inner.put_multipart_opts(path, options).await
    }
    async fn get_opts(&self, path: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        self.inner.get_opts(path, options).await
    }
    fn delete_stream(
        &self,
        paths: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(paths)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

async fn exercise(release: bool, fail: bool) {
    let objects = Arc::new(WalGate::new());
    let store = Arc::new(
        SlateDbStateStore::open("durability", objects.clone())
            .await
            .unwrap(),
    );
    let write = StateObjectWrite::new(
        "durability",
        0,
        1,
        "binary",
        Bytes::from_static(&[0, 255, 128, 1]),
    )
    .unwrap();
    let prior = store.write_state_object(&write).await.unwrap();
    if !release {
        assert!(store.release_state_object(&prior).await.unwrap());
    }
    // Arm only after open (and, for delete tests, after a durable initial write).
    objects
        .mode
        .store(if fail { 2 } else { 1 }, Ordering::SeqCst);
    let task_store = store.clone();
    let task_ref = prior.clone();
    let mut task = tokio::spawn(async move {
        if release {
            task_store
                .release_state_object(&task_ref)
                .await
                .map(|removed| {
                    assert!(removed);
                    task_ref
                })
        } else {
            task_store.write_state_object(&write).await
        }
    });
    tokio::select! {
        biased;
        entered = objects.entered.acquire() => { entered.unwrap().forget(); }
        result = &mut task => { panic!("operation returned before attempted WAL persistence: {result:?}"); }
        _ = tokio::time::sleep(Duration::from_secs(10)) => { panic!("no WAL PUT observed"); }
    }
    assert!(objects
        .observed
        .lock()
        .unwrap()
        .iter()
        .all(|p| p.starts_with("durability/wal/") && p.ends_with(".sst")));
    if fail {
        // SlateDB may retry failed writes instead of immediately propagating an
        // error. Both an error and pending completion are safe; success is not.
        if let Ok(joined) = tokio::time::timeout(Duration::from_millis(300), &mut task).await {
            assert!(
                joined.unwrap().is_err(),
                "failed WAL PUT acknowledged success"
            );
        }
        task.abort();
        // Separate opener bypasses the fault injector, not the stored data.
        // Original writer keeps failing every WAL upload throughout verification.
        let independent = SlateDbStateStore::open("durability", objects.inner.clone())
            .await
            .unwrap();
        assert_eq!(
            independent.state_object_exists(&prior).await.unwrap(),
            release
        );
        if release {
            assert_eq!(
                independent
                    .read_state_object(&prior)
                    .await
                    .unwrap()
                    .as_ref(),
                &[0, 255, 128, 1]
            );
        } else {
            assert!(matches!(
                independent.read_state_object(&prior).await,
                Err(velorix_storage::state::CheckpointPublishError::MissingStateObject(_))
            ));
        }
        independent.close().await.unwrap();
        // Close is cleanup only; a faulted/fenced writer may not close promptly.
        let _ = tokio::time::timeout(Duration::from_secs(1), store.close()).await;
        return;
    }
    // Once the WAL PUT is blocked, no successful ACK is allowed, even if staging finished.
    assert!(
        tokio::time::timeout(Duration::from_millis(150), &mut task)
            .await
            .is_err(),
        "operation acknowledged while WAL PUT was blocked"
    );
    objects.mode.store(0, Ordering::SeqCst);
    objects.release.add_permits(1);
    let state_ref = tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    // Do NOT close the original writer before this independent opener. It fences
    // that writer, so never attempt further writes through the old handle.
    let independent = SlateDbStateStore::open("durability", objects.clone())
        .await
        .unwrap();
    if release {
        assert!(!independent.state_object_exists(&state_ref).await.unwrap());
        assert!(matches!(
            independent.read_state_object(&state_ref).await,
            Err(velorix_storage::state::CheckpointPublishError::MissingStateObject(_))
        ));
    } else {
        assert!(
            independent.state_object_exists(&state_ref).await.unwrap(),
            "persisted marker missing"
        );
        assert_eq!(
            independent
                .read_state_object(&state_ref)
                .await
                .unwrap()
                .as_ref(),
            &[0, 255, 128, 1]
        );
    }
    independent.close().await.unwrap();
    let _ = store.close().await;
}

#[tokio::test]
async fn state_write_waits_for_wal_and_is_independently_readable_before_close() {
    tokio::time::timeout(Duration::from_secs(30), exercise(false, false))
        .await
        .unwrap();
}
#[tokio::test]
async fn state_release_waits_for_wal_and_is_independently_visible_before_close() {
    tokio::time::timeout(Duration::from_secs(30), exercise(true, false))
        .await
        .unwrap();
}
#[tokio::test]
async fn state_write_never_acknowledges_failed_wal() {
    tokio::time::timeout(Duration::from_secs(30), exercise(false, true))
        .await
        .unwrap();
}
#[tokio::test]
async fn state_release_never_acknowledges_failed_wal() {
    tokio::time::timeout(Duration::from_secs(30), exercise(true, true))
        .await
        .unwrap();
}
