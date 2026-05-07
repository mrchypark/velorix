use std::{future::Future, sync::Arc, time::Duration};

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::error::DataFusionError;
use datafusion::prelude::{SessionConfig, SessionContext};
use thiserror::Error;

use crate::delta::DeltaBatch;

pub use crate::resource_policy::{QueryExecutionPolicyV1, QueryPolicy, QueryPolicyError};

pub const INPUT_TABLE_NAME: &str = "input";

#[derive(Debug, Error)]
pub enum QueryError {
    #[error(transparent)]
    Arrow(#[from] ArrowError),
    #[error(transparent)]
    DataFusion(#[from] DataFusionError),
    #[error(transparent)]
    Policy(#[from] QueryPolicyError),
}

/// Runs caller SQL over a delta batch through DataFusion.
///
/// Velorix exposes each delta record as a row in the `input` table with these
/// stable columns:
/// - `key_json`: UTF-8 JSON serialization of the delta key
/// - `value_json`: UTF-8 JSON serialization of the delta value
/// - `weight`: signed delta weight
pub async fn query_delta_batch(
    batch: &DeltaBatch,
    sql: &str,
) -> Result<Vec<RecordBatch>, QueryError> {
    query_delta_batch_with_policy(batch, sql, QueryPolicy::default()).await
}

pub async fn query_delta_batch_with_policy(
    batch: &DeltaBatch,
    sql: &str,
    policy: QueryPolicy,
) -> Result<Vec<RecordBatch>, QueryError> {
    policy.validate()?;
    validate_sql_text_policy(sql, policy)?;

    let input = RecordBatch::try_new(
        input_schema(),
        vec![
            Arc::new(StringArray::from(
                batch
                    .records()
                    .iter()
                    .map(|record| record.key.as_json().to_string())
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(StringArray::from(
                batch
                    .records()
                    .iter()
                    .map(|record| record.value.as_json().to_string())
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int64Array::from(
                batch
                    .records()
                    .iter()
                    .map(|record| record.weight)
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
    )?;
    let context = input_context(vec![input], policy)?;

    let dataframe = context.sql(sql).await?;
    if let Some(max_rows) = policy.max_output_rows {
        let fetch = max_rows.checked_add(1);
        let output = match fetch {
            Some(fetch) => dataframe.limit(0, Some(fetch))?.collect().await?,
            None => dataframe.collect().await?,
        };
        let observed_rows = output.iter().map(RecordBatch::num_rows).sum();
        if observed_rows > max_rows {
            return Err(QueryPolicyError::OutputRowsExceeded {
                observed_rows,
                max_rows,
            }
            .into());
        }

        return Ok(output);
    }

    Ok(dataframe.collect().await?)
}

pub async fn validate_input_query_with_policy(
    sql: &str,
    policy: QueryPolicy,
) -> Result<(), QueryError> {
    validate_table_query_with_policy(sql, INPUT_TABLE_NAME, input_schema(), policy).await
}

pub async fn validate_table_query_with_policy(
    sql: &str,
    table_name: &str,
    table_schema: Arc<Schema>,
    policy: QueryPolicy,
) -> Result<(), QueryError> {
    policy.validate()?;
    validate_sql_text_policy(sql, policy)?;

    let input = RecordBatch::new_empty(table_schema.clone());
    let context = table_context(table_name, table_schema, vec![input], policy)?;

    run_planning_with_policy(policy, async {
        let dataframe = context.sql(sql).await?;
        dataframe.into_optimized_plan()?;
        Ok(())
    })
    .await
}

async fn run_planning_with_policy<T, F>(policy: QueryPolicy, operation: F) -> Result<T, QueryError>
where
    F: Future<Output = Result<T, QueryError>>,
{
    let Some(timeout_ms) = policy.planning_timeout_ms else {
        return operation.await;
    };

    tokio::time::timeout(Duration::from_millis(timeout_ms), operation)
        .await
        .map_err(|_| QueryError::from(QueryPolicyError::PlanningTimeout { timeout_ms }))?
}

fn validate_sql_text_policy(sql: &str, policy: QueryPolicy) -> Result<(), QueryPolicyError> {
    if let Some(max_bytes) = policy.max_sql_bytes {
        let actual_bytes = sql.len();
        if actual_bytes > max_bytes {
            return Err(QueryPolicyError::SqlTextTooLarge {
                actual_bytes,
                max_bytes,
            });
        }
    }

    Ok(())
}

fn input_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("key_json", DataType::Utf8, false),
        Field::new("value_json", DataType::Utf8, false),
        Field::new("weight", DataType::Int64, false),
    ]))
}

fn input_context(
    input_batches: Vec<RecordBatch>,
    policy: QueryPolicy,
) -> Result<SessionContext, QueryError> {
    table_context(INPUT_TABLE_NAME, input_schema(), input_batches, policy)
}

fn table_context(
    table_name: &str,
    table_schema: Arc<Schema>,
    table_batches: Vec<RecordBatch>,
    policy: QueryPolicy,
) -> Result<SessionContext, QueryError> {
    let table = MemTable::try_new(table_schema, vec![table_batches])?;
    let mut config = SessionConfig::new();
    if let Some(batch_size) = policy.batch_size {
        config = config.with_batch_size(batch_size.get());
    }
    if let Some(target_partitions) = policy.target_partitions {
        config = config.with_target_partitions(target_partitions.get());
    }
    let context = SessionContext::new_with_config(config);

    context.register_table(table_name, Arc::new(table))?;

    Ok(context)
}
