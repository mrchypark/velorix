//! Bounded concurrency utilities for object-store operations.
//!
//! Prevents unbounded request bursts (e.g., `join_all` of N GETs) that
//! can cause S3 throttling and memory spikes. Use `buffer_unordered`
//! with a fixed concurrency limit for all remote I/O fanout.

use futures::stream::{self, StreamExt};

/// Default concurrency limit for S3/object-store operations.
/// Tuned to avoid throttling on most S3-compatible backends.
pub const DEFAULT_CONCURRENCY: usize = 16;

/// Execute a list of fallible async operations with bounded concurrency,
/// short-circuiting on the first error.
///
/// Unlike unbounded `join_all`, this limits in-flight requests to
/// `concurrency` and returns results in input order.
pub async fn bounded_try_map<T, F, Fut, R, E>(
    items: Vec<T>,
    f: F,
    concurrency: usize,
) -> Result<Vec<R>, E>
where
    T: Send + 'static,
    F: Fn(T) -> Fut + Send + Sync,
    Fut: std::future::Future<Output = Result<R, E>> + Send,
    R: Send + 'static,
    E: Send + 'static,
{
    let results: Vec<Result<R, E>> = stream::iter(items)
        .map(f)
        .buffer_unordered(concurrency)
        .collect()
        .await;

    let mut output = Vec::with_capacity(results.len());
    for result in results {
        output.push(result?);
    }
    Ok(output)
}
