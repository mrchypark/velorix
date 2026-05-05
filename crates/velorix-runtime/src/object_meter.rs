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
    path::Path, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore as DataFusionObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    RenameOptions, Result as ObjectStoreResult,
};
use futures::{stream, StreamExt};
use velorix_core::query::QueryPolicyError;

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
        self.meter
            .add_bytes_returned(result.range.end.saturating_sub(result.range.start));
        Ok(result)
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

    use datafusion::object_store::{memory::InMemory, path::Path, GetOptions, ObjectStoreExt};
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
        store
            .get_opts(&Path::from("input/part-000"), GetOptions::default())
            .await
            .unwrap();
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
}
