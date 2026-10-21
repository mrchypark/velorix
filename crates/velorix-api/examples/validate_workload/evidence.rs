//! Logical object-store observations, not HTTP retry counts or engine CPU counters.
use futures::{stream::BoxStream, StreamExt};
use object_store::{path::Path, *};
use serde::Serialize;
use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex},
};

#[derive(Debug, Default, Clone, Serialize)]
pub struct Counts {
    pub get_attempts: u64,
    pub head_attempts: u64,
    pub get_errors: u64,
    pub delivered_bytes: u64,
    pub stream_errors: u64,
    pub file_payloads_unmeasured: u64,
}
#[derive(Debug, Default)]
pub struct Evidence(Mutex<BTreeMap<String, Counts>>);
impl Evidence {
    fn record(&self, class: &str, f: impl FnOnce(&mut Counts)) {
        f(self
            .0
            .lock()
            .expect("evidence mutex poisoned")
            .entry(class.into())
            .or_default());
    }
    pub fn snapshot(&self) -> BTreeMap<String, Counts> {
        self.0.lock().expect("evidence mutex poisoned").clone()
    }
}
fn class(path: &Path) -> &'static str {
    let key = path.as_ref();
    if key.starts_with("v1/ingest-staging/") || key.starts_with("v1/ingest/") {
        "source_payload"
    } else if key.contains("standing-runtime-checkpoint") {
        "checkpoint"
    } else {
        "other"
    }
}
#[derive(Debug)]
pub struct ObservedStore {
    pub inner: Arc<dyn ObjectStore>,
    pub evidence: Arc<Evidence>,
}
impl fmt::Display for ObservedStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("validation-observed-store")
    }
}
#[async_trait::async_trait]
impl ObjectStore for ObservedStore {
    async fn put_opts(&self, p: &Path, data: PutPayload, opts: PutOptions) -> Result<PutResult> {
        self.inner.put_opts(p, data, opts).await
    }
    async fn put_multipart_opts(
        &self,
        p: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(p, opts).await
    }
    async fn get_opts(&self, p: &Path, opts: GetOptions) -> Result<GetResult> {
        let kind = class(p);
        let head = opts.head;
        self.evidence.record(kind, |c| {
            if head {
                c.head_attempts += 1
            } else {
                c.get_attempts += 1
            }
        });
        let mut result = match self.inner.get_opts(p, opts).await {
            Ok(result) => result,
            Err(error) => {
                self.evidence.record(kind, |c| c.get_errors += 1);
                return Err(error);
            }
        };
        if !head {
            result.payload = match result.payload {
                GetResultPayload::Stream(stream) => {
                    let evidence = self.evidence.clone();
                    GetResultPayload::Stream(
                        stream
                            .map(move |chunk| {
                                evidence.record(kind, |c| match &chunk {
                                    Ok(bytes) => c.delivered_bytes += bytes.len() as u64,
                                    Err(_) => c.stream_errors += 1,
                                });
                                chunk
                            })
                            .boxed(),
                    )
                }
                payload => {
                    self.evidence
                        .record(kind, |c| c.file_payloads_unmeasured += 1);
                    payload
                }
            };
        }
        Ok(result)
    }
    fn delete_stream(
        &self,
        paths: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(paths)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(&self, from: &Path, to: &Path, opts: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, opts).await
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn measures_consumed_source_bytes_and_separates_head() {
        let evidence = Arc::new(Evidence::default());
        let store = ObservedStore {
            inner: Arc::new(memory::InMemory::new()),
            evidence: evidence.clone(),
        };
        let path = Path::from("v1/ingest/example");
        store
            .put(&path, PutPayload::from_static(b"abc"))
            .await
            .unwrap();
        store.head(&path).await.unwrap();
        assert_eq!(
            store.get(&path).await.unwrap().bytes().await.unwrap().len(),
            3
        );
        let counts = evidence.snapshot();
        assert_eq!(counts["source_payload"].get_attempts, 1);
        assert_eq!(counts["source_payload"].head_attempts, 1);
        assert_eq!(counts["source_payload"].delivered_bytes, 3);
    }
}
