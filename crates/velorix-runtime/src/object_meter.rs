use std::{
    fmt,
    ops::Range,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
};

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::object_store::{
    path::Path, CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore as DataFusionObjectStore, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, RenameOptions, Result as ObjectStoreResult,
};
use futures::{stream, StreamExt};
use velorix_core::query::QueryPolicyError;

use crate::benchmark_gate::ObjectRequestMetricsV1;

#[derive(Clone, Debug, Default)]
pub(crate) struct ObjectStoreMeter {
    state: Arc<ObjectStoreMeterState>,
}

#[derive(Debug, Default)]
struct ObjectStoreMeterState {
    requests: AtomicUsize,
    bytes_returned: AtomicU64,
    list_requests: AtomicUsize,
    list_with_offset_requests: AtomicUsize,
    list_with_delimiter_requests: AtomicUsize,
    get_opts_requests: AtomicUsize,
    get_ranges_requests: AtomicUsize,
    put_opts_requests: AtomicUsize,
    put_multipart_opts_requests: AtomicUsize,
    delete_stream_requests: AtomicUsize,
    copy_opts_requests: AtomicUsize,
    rename_opts_requests: AtomicUsize,
}

impl ObjectStoreMeter {
    fn observe(
        &self,
        operation: MeteredOperation,
        max_requests: Option<usize>,
    ) -> ObjectStoreResult<()> {
        let observed_requests = self.state.requests.fetch_add(1, Ordering::SeqCst) + 1;
        operation
            .counter(&self.state)
            .fetch_add(1, Ordering::SeqCst);

        if let Some(max_requests) = max_requests {
            if observed_requests > max_requests {
                return Err(object_request_error(observed_requests, max_requests));
            }
        }

        Ok(())
    }

    fn add_bytes_returned(&self, bytes: u64) {
        self.state.bytes_returned.fetch_add(bytes, Ordering::SeqCst);
    }

    pub(crate) fn snapshot(&self) -> ObjectRequestMetricsV1 {
        ObjectRequestMetricsV1 {
            put_count: self
                .state
                .put_opts_requests
                .load(Ordering::SeqCst)
                .saturating_add(
                    self.state
                        .put_multipart_opts_requests
                        .load(Ordering::SeqCst),
                ) as u64,
            get_count: self.state.get_opts_requests.load(Ordering::SeqCst) as u64,
            list_count: self
                .state
                .list_requests
                .load(Ordering::SeqCst)
                .saturating_add(self.state.list_with_offset_requests.load(Ordering::SeqCst))
                .saturating_add(
                    self.state
                        .list_with_delimiter_requests
                        .load(Ordering::SeqCst),
                ) as u64,
            range_read_count: self.state.get_ranges_requests.load(Ordering::SeqCst) as u64,
            bytes_written: 0,
            bytes_read: self.state.bytes_returned.load(Ordering::SeqCst),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum MeteredOperation {
    List,
    ListWithOffset,
    ListWithDelimiter,
    GetOpts,
    GetRanges,
    PutOpts,
    PutMultipartOpts,
    DeleteStream,
    CopyOpts,
    RenameOpts,
}

impl MeteredOperation {
    fn counter(self, state: &ObjectStoreMeterState) -> &AtomicUsize {
        match self {
            Self::List => &state.list_requests,
            Self::ListWithOffset => &state.list_with_offset_requests,
            Self::ListWithDelimiter => &state.list_with_delimiter_requests,
            Self::GetOpts => &state.get_opts_requests,
            Self::GetRanges => &state.get_ranges_requests,
            Self::PutOpts => &state.put_opts_requests,
            Self::PutMultipartOpts => &state.put_multipart_opts_requests,
            Self::DeleteStream => &state.delete_stream_requests,
            Self::CopyOpts => &state.copy_opts_requests,
            Self::RenameOpts => &state.rename_opts_requests,
        }
    }
}

#[derive(Debug)]
pub(crate) struct MeteredObjectStore {
    inner: Arc<dyn DataFusionObjectStore>,
    meter: ObjectStoreMeter,
    max_requests: Option<usize>,
}

impl MeteredObjectStore {
    pub(crate) fn new(inner: Arc<dyn DataFusionObjectStore>, max_requests: Option<usize>) -> Self {
        Self::with_meter(inner, ObjectStoreMeter::default(), max_requests)
    }

    pub(crate) fn with_meter(
        inner: Arc<dyn DataFusionObjectStore>,
        meter: ObjectStoreMeter,
        max_requests: Option<usize>,
    ) -> Self {
        Self {
            inner,
            meter,
            max_requests,
        }
    }

    fn observe(&self, operation: MeteredOperation) -> ObjectStoreResult<()> {
        self.meter.observe(operation, self.max_requests)
    }
}

impl fmt::Display for MeteredObjectStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "MeteredObjectStore({})", self.inner)
    }
}

#[async_trait]
impl DataFusionObjectStore for MeteredObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        self.observe(MeteredOperation::PutOpts)?;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.observe(MeteredOperation::PutMultipartOpts)?;
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        self.observe(MeteredOperation::GetOpts)?;
        let result = self.inner.get_opts(location, options).await?;
        Ok(meter_get_result(result, self.meter.clone()))
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> ObjectStoreResult<Vec<Bytes>> {
        self.observe(MeteredOperation::GetRanges)?;
        let results = self.inner.get_ranges(location, ranges).await?;
        let bytes_returned = results
            .iter()
            .map(|bytes| u64::try_from(bytes.len()).unwrap_or(u64::MAX))
            .fold(0u64, u64::saturating_add);
        self.meter.add_bytes_returned(bytes_returned);
        Ok(results)
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<'static, ObjectStoreResult<Path>>,
    ) -> futures::stream::BoxStream<'static, ObjectStoreResult<Path>> {
        if let Err(error) = self.observe(MeteredOperation::DeleteStream) {
            return stream::once(async { Err(error) }).boxed();
        }

        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> futures::stream::BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        if let Err(error) = self.observe(MeteredOperation::List) {
            return stream::once(async { Err(error) }).boxed();
        }

        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> futures::stream::BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        if let Err(error) = self.observe(MeteredOperation::ListWithOffset) {
            return stream::once(async { Err(error) }).boxed();
        }

        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        self.observe(MeteredOperation::ListWithDelimiter)?;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> ObjectStoreResult<()> {
        self.observe(MeteredOperation::CopyOpts)?;
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> ObjectStoreResult<()> {
        self.observe(MeteredOperation::RenameOpts)?;
        self.inner.rename_opts(from, to, options).await
    }
}

fn meter_get_result(result: GetResult, meter: ObjectStoreMeter) -> GetResult {
    let meta = result.meta.clone();
    let range = result.range.clone();
    let attributes = result.attributes.clone();
    let payload = GetResultPayload::Stream(
        result
            .into_stream()
            .map(move |result| {
                if let Ok(bytes) = &result {
                    meter.add_bytes_returned(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
                }
                result
            })
            .boxed(),
    );

    GetResult {
        payload,
        meta,
        range,
        attributes,
    }
}

fn object_request_error(
    observed_requests: usize,
    max_requests: usize,
) -> datafusion::object_store::Error {
    datafusion::object_store::Error::Generic {
        store: "MeteredObjectStore",
        source: Box::new(QueryPolicyError::ObjectRequestsExceeded {
            observed_requests,
            max_requests,
        }),
    }
}

pub(crate) fn object_request_policy_error(
    error: &datafusion::object_store::Error,
) -> Option<QueryPolicyError> {
    let datafusion::object_store::Error::Generic { source, .. } = error else {
        return None;
    };

    match source.downcast_ref::<QueryPolicyError>() {
        Some(QueryPolicyError::ObjectRequestsExceeded {
            observed_requests,
            max_requests,
        }) => Some(QueryPolicyError::ObjectRequestsExceeded {
            observed_requests: *observed_requests,
            max_requests: *max_requests,
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use datafusion::object_store::{
        local::LocalFileSystem, memory::InMemory, path::Path, GetOptions, ObjectStoreExt,
    };
    use futures::TryStreamExt;

    use super::*;

    #[tokio::test]
    async fn metered_object_store_counts_direct_wrapper_operations() {
        let inner = Arc::new(InMemory::new());
        inner
            .put(
                &Path::from("input/part-000"),
                Bytes::from_static(b"abcdef").into(),
            )
            .await
            .unwrap();
        let meter = ObjectStoreMeter::default();
        let wrapped_inner: Arc<dyn DataFusionObjectStore> = inner.clone();
        let store = MeteredObjectStore::with_meter(wrapped_inner, meter.clone(), None);

        store
            .list(Some(&Path::from("input")))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let result = store
            .get_opts(&Path::from("input/part-000"), GetOptions::default())
            .await
            .unwrap();
        result.bytes().await.unwrap();
        store
            .get_ranges(&Path::from("input/part-000"), &[0..2, 2..4])
            .await
            .unwrap();

        assert_eq!(meter.state.requests.load(Ordering::SeqCst), 3);
        assert_eq!(meter.state.list_requests.load(Ordering::SeqCst), 1);
        assert_eq!(meter.state.get_opts_requests.load(Ordering::SeqCst), 1);
        assert_eq!(meter.state.get_ranges_requests.load(Ordering::SeqCst), 1);
        assert_eq!(meter.state.bytes_returned.load(Ordering::SeqCst), 10);
    }

    #[tokio::test]
    async fn get_opts_counts_stream_bytes_as_chunks_are_consumed() {
        let inner = Arc::new(InMemory::new());
        inner
            .put(
                &Path::from("input/part-000"),
                Bytes::from_static(b"abcdef").into(),
            )
            .await
            .unwrap();
        let meter = ObjectStoreMeter::default();
        let chunked_inner: Arc<dyn DataFusionObjectStore> = Arc::new(ChunkedGetStore {
            inner,
            chunks: vec![Bytes::from_static(b"abc"), Bytes::from_static(b"def")],
        });
        let store = MeteredObjectStore::with_meter(chunked_inner, meter.clone(), None);

        let result = store
            .get_opts(&Path::from("input/part-000"), GetOptions::default())
            .await
            .unwrap();
        assert_eq!(meter.state.get_opts_requests.load(Ordering::SeqCst), 1);
        assert_eq!(meter.state.bytes_returned.load(Ordering::SeqCst), 0);

        let mut stream = result.into_stream();
        assert_eq!(stream.try_next().await.unwrap().unwrap(), b"abc"[..]);
        assert_eq!(meter.state.bytes_returned.load(Ordering::SeqCst), 3);

        assert_eq!(stream.try_next().await.unwrap().unwrap(), b"def"[..]);
        assert_eq!(meter.state.bytes_returned.load(Ordering::SeqCst), 6);
        assert!(stream.try_next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn get_opts_counts_file_bytes_as_chunks_are_consumed() {
        let temp_dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(temp_dir.path().join("input")).unwrap();
        std::fs::write(temp_dir.path().join("input/part-000"), b"abcdef").unwrap();
        let file_store: Arc<dyn DataFusionObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        let meter = ObjectStoreMeter::default();
        let store = MeteredObjectStore::with_meter(file_store, meter.clone(), None);

        let result = store
            .get_opts(&Path::from("input/part-000"), GetOptions::default())
            .await
            .unwrap();
        assert_eq!(meter.state.get_opts_requests.load(Ordering::SeqCst), 1);
        assert_eq!(meter.state.bytes_returned.load(Ordering::SeqCst), 0);

        let mut stream = result.into_stream();
        let first_chunk = stream.try_next().await.unwrap().unwrap();
        assert!(!first_chunk.is_empty());
        assert!(meter.state.bytes_returned.load(Ordering::SeqCst) > 0);
        while stream.try_next().await.unwrap().is_some() {}
        assert_eq!(meter.state.bytes_returned.load(Ordering::SeqCst), 6);
    }

    #[tokio::test]
    async fn metered_object_store_rejects_before_delegating_when_budget_is_exhausted() {
        let inner = Arc::new(InMemory::new());
        inner
            .put(
                &Path::from("input/part-000"),
                Bytes::from_static(b"abcdef").into(),
            )
            .await
            .unwrap();
        let wrapped_inner: Arc<dyn DataFusionObjectStore> = inner.clone();
        let store = MeteredObjectStore::new(wrapped_inner, Some(1));

        store
            .list(Some(&Path::from("input")))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let error = store
            .get_opts(&Path::from("input/part-000"), GetOptions::default())
            .await
            .unwrap_err();

        assert_eq!(
            object_request_policy_error(&error),
            Some(QueryPolicyError::ObjectRequestsExceeded {
                observed_requests: 2,
                max_requests: 1,
            })
        );
    }

    #[derive(Debug)]
    struct ChunkedGetStore {
        inner: Arc<InMemory>,
        chunks: Vec<Bytes>,
    }

    impl fmt::Display for ChunkedGetStore {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(formatter, "ChunkedGetStore")
        }
    }

    #[async_trait]
    impl DataFusionObjectStore for ChunkedGetStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> ObjectStoreResult<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> ObjectStoreResult<GetResult> {
            let result = self.inner.get_opts(location, options).await?;
            let stream = stream::iter(self.chunks.clone().into_iter().map(Ok)).boxed();
            Ok(GetResult {
                payload: GetResultPayload::Stream(stream),
                ..result
            })
        }

        async fn get_ranges(
            &self,
            location: &Path,
            ranges: &[Range<u64>],
        ) -> ObjectStoreResult<Vec<Bytes>> {
            self.inner.get_ranges(location, ranges).await
        }

        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, ObjectStoreResult<Path>>,
        ) -> futures::stream::BoxStream<'static, ObjectStoreResult<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> futures::stream::BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
            self.inner.list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&Path>,
            offset: &Path,
        ) -> futures::stream::BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
            self.inner.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> ObjectStoreResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> ObjectStoreResult<()> {
            self.inner.copy_opts(from, to, options).await
        }

        async fn rename_opts(
            &self,
            from: &Path,
            to: &Path,
            options: RenameOptions,
        ) -> ObjectStoreResult<()> {
            self.inner.rename_opts(from, to, options).await
        }
    }
}
