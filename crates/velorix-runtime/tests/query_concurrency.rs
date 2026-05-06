use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::object_store::{
    memory::InMemory as DataFusionInMemory, path::Path as DataFusionPath, CopyOptions, GetOptions,
    GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore as DataFusionObjectStore,
    ObjectStoreExt as DataFusionObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, Result as ObjectStoreResult,
};
use futures::{stream, stream::BoxStream, StreamExt};
use parquet::arrow::ArrowWriter;
use tokio::sync::Barrier;
use velorix_core::query::{QueryError, QueryPolicy, QueryPolicyError};
use velorix_runtime::query::{
    query_object_backed_input_with_policy, query_object_backed_input_with_policy_and_limiter,
    query_recovered_materialized_view_with_policy, QueryExecutionLimiter, RuntimeQueryError,
};

#[tokio::test]
async fn query_recovered_materialized_view_requires_shared_limiter_when_concurrency_limit_is_set() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());

    let error = query_recovered_materialized_view_with_policy(
        Arc::clone(&store),
        "select key_json, value_json, weight from input",
        QueryPolicy {
            max_concurrent_queries: Some(1),
            ..QueryPolicy::default()
        },
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        RuntimeQueryError::Query(QueryError::Policy(
            QueryPolicyError::ConcurrencyLimiterRequired {
                max_concurrent_queries: 1
            }
        ))
    ));
}

#[tokio::test]
async fn query_object_backed_input_requires_shared_limiter_when_concurrency_limit_is_set() {
    let store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());

    let error = query_object_backed_input_with_policy(
        Arc::clone(&store),
        "memory://velorix/input/",
        "select key_json, value_json, weight from input",
        QueryPolicy {
            max_concurrent_queries: Some(1),
            ..QueryPolicy::default()
        },
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        RuntimeQueryError::Query(QueryError::Policy(
            QueryPolicyError::ConcurrencyLimiterRequired {
                max_concurrent_queries: 1
            }
        ))
    ));
}

#[tokio::test]
async fn query_object_backed_input_fails_immediately_when_shared_limiter_is_already_held() {
    let inner_store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    put_parquet_input(
        &inner_store,
        "input/part-000.parquet",
        &parquet_input_batch(&["\"account-a\""], &["10"], &[1]),
    )
    .await;

    let first_query_reached_list = Arc::new(Barrier::new(2));
    let blocking_store: Arc<dyn DataFusionObjectStore> = Arc::new(BlockingListStore {
        inner: Arc::clone(&inner_store),
        first_query_reached_list: Arc::clone(&first_query_reached_list),
    });
    let policy = QueryPolicy {
        max_concurrent_queries: Some(1),
        ..QueryPolicy::default()
    };
    let limiter = QueryExecutionLimiter::from_policy(policy).unwrap();

    let first_query = tokio::spawn(query_object_backed_input_with_policy_and_limiter(
        Arc::clone(&blocking_store),
        "memory://velorix/input/",
        "select key_json, value_json, weight from input",
        policy,
        Some(limiter.clone()),
    ));
    tokio::time::timeout(Duration::from_secs(1), first_query_reached_list.wait())
        .await
        .expect("first query should acquire the limiter and reach object listing");

    let error = tokio::time::timeout(
        Duration::from_millis(50),
        query_object_backed_input_with_policy_and_limiter(
            Arc::clone(&inner_store),
            "memory://velorix/input/",
            "select key_json, value_json, weight from input",
            policy,
            Some(limiter),
        ),
    )
    .await
    .expect("second query should fail without waiting for a permit")
    .unwrap_err();

    first_query.abort();

    assert!(matches!(
        error,
        RuntimeQueryError::Query(QueryError::Policy(
            QueryPolicyError::ConcurrencyLimitExceeded {
                max_concurrent_queries: 1
            }
        ))
    ));
}

#[tokio::test]
async fn query_object_backed_input_runs_without_limiter_when_concurrency_limit_is_unset() {
    let store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    put_parquet_input(
        &store,
        "input/part-000.parquet",
        &parquet_input_batch(&["\"account-a\""], &["10"], &[1]),
    )
    .await;

    let output = query_object_backed_input_with_policy(
        Arc::clone(&store),
        "memory://velorix/input/",
        "select key_json, value_json, weight from input",
        QueryPolicy::default(),
    )
    .await
    .unwrap();

    assert_eq!(output.len(), 1);
    assert_eq!(output[0].num_rows(), 1);
}

#[tokio::test]
async fn query_object_backed_input_returns_planning_timeout_when_table_registration_stalls() {
    let inner_store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    put_parquet_input(
        &inner_store,
        "input/part-000.parquet",
        &parquet_input_batch(&["\"account-a\""], &["10"], &[1]),
    )
    .await;

    let first_query_reached_list = Arc::new(Barrier::new(2));
    let blocking_store: Arc<dyn DataFusionObjectStore> = Arc::new(BlockingListStore {
        inner: Arc::clone(&inner_store),
        first_query_reached_list,
    });

    let result = tokio::time::timeout(
        Duration::from_secs(1),
        query_object_backed_input_with_policy(
            Arc::clone(&blocking_store),
            "memory://velorix/input/",
            "select key_json, value_json, weight from input",
            QueryPolicy {
                planning_timeout_ms: Some(25),
                ..QueryPolicy::default()
            },
        ),
    )
    .await
    .expect("query policy timeout should complete before the test guard");

    assert!(matches!(
        result.unwrap_err(),
        RuntimeQueryError::Query(QueryError::Policy(QueryPolicyError::PlanningTimeout {
            timeout_ms: 25
        }))
    ));
}

#[tokio::test]
async fn query_object_backed_input_returns_execution_timeout_when_scan_preflight_stalls() {
    let inner_store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    let first_query_reached_list = Arc::new(Barrier::new(2));
    let blocking_store: Arc<dyn DataFusionObjectStore> = Arc::new(BlockingListStore {
        inner: Arc::clone(&inner_store),
        first_query_reached_list,
    });

    let result = tokio::time::timeout(
        Duration::from_secs(1),
        query_object_backed_input_with_policy(
            Arc::clone(&blocking_store),
            "memory://velorix/input/",
            "select key_json, value_json, weight from input",
            QueryPolicy {
                execution_timeout_ms: Some(25),
                max_scan_files: Some(usize::MAX),
                ..QueryPolicy::default()
            },
        ),
    )
    .await
    .expect("query policy timeout should complete before the test guard");

    assert!(matches!(
        result.unwrap_err(),
        RuntimeQueryError::Query(QueryError::Policy(QueryPolicyError::ExecutionTimeout {
            timeout_ms: 25
        }))
    ));
}

#[derive(Debug)]
struct BlockingListStore {
    inner: Arc<dyn DataFusionObjectStore>,
    first_query_reached_list: Arc<Barrier>,
}

impl fmt::Display for BlockingListStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BlockingListStore")
    }
}

#[async_trait]
impl DataFusionObjectStore for BlockingListStore {
    async fn put_opts(
        &self,
        location: &DataFusionPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &DataFusionPath,
        opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &DataFusionPath,
        options: GetOptions,
    ) -> ObjectStoreResult<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, ObjectStoreResult<DataFusionPath>>,
    ) -> BoxStream<'static, ObjectStoreResult<DataFusionPath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        _prefix: Option<&DataFusionPath>,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        let first_query_reached_list = Arc::clone(&self.first_query_reached_list);
        stream::once(async move {
            first_query_reached_list.wait().await;
            std::future::pending::<ObjectStoreResult<ObjectMeta>>().await
        })
        .boxed()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&DataFusionPath>,
    ) -> ObjectStoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &DataFusionPath,
        to: &DataFusionPath,
        options: CopyOptions,
    ) -> ObjectStoreResult<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

fn parquet_input_batch(keys: &[&str], values: &[&str], weights: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key_json", DataType::Utf8, false),
            Field::new("value_json", DataType::Utf8, false),
            Field::new("weight", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(keys.to_vec())) as ArrayRef,
            Arc::new(StringArray::from(values.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from(weights.to_vec())) as ArrayRef,
        ],
    )
    .unwrap()
}

fn parquet_bytes(batch: &RecordBatch) -> Bytes {
    let mut bytes = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut bytes, batch.schema(), None).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
    }
    Bytes::from(bytes)
}

async fn put_parquet_input(
    store: &Arc<dyn DataFusionObjectStore>,
    path: &str,
    batch: &RecordBatch,
) {
    store
        .put(&DataFusionPath::from(path), parquet_bytes(batch).into())
        .await
        .unwrap();
}
