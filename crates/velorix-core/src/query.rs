use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::error::DataFusionError;
use datafusion::prelude::{SessionConfig, SessionContext};
use thiserror::Error;

use crate::delta::DeltaBatch;

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

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct QueryPolicy {
    pub max_sql_bytes: Option<usize>,
    pub max_output_rows: Option<usize>,
    pub batch_size: Option<std::num::NonZeroUsize>,
    pub target_partitions: Option<std::num::NonZeroUsize>,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum QueryPolicyError {
    #[error("SQL text is {actual_bytes} bytes, above query policy limit of {max_bytes} bytes")]
    SqlTextTooLarge {
        actual_bytes: usize,
        max_bytes: usize,
    },
    #[error(
        "query returned at least {observed_rows} rows, above query policy limit of {max_rows} rows"
    )]
    OutputRowsExceeded {
        observed_rows: usize,
        max_rows: usize,
    },
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
    if let Some(max_bytes) = policy.max_sql_bytes {
        let actual_bytes = sql.len();
        if actual_bytes > max_bytes {
            return Err(QueryPolicyError::SqlTextTooLarge {
                actual_bytes,
                max_bytes,
            }
            .into());
        }
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("key_json", DataType::Utf8, false),
        Field::new("value_json", DataType::Utf8, false),
        Field::new("weight", DataType::Int64, false),
    ]));
    let input = RecordBatch::try_new(
        Arc::clone(&schema),
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
    let table = MemTable::try_new(schema, vec![vec![input]])?;
    let mut config = SessionConfig::new();
    if let Some(batch_size) = policy.batch_size {
        config = config.with_batch_size(batch_size.get());
    }
    if let Some(target_partitions) = policy.target_partitions {
        config = config.with_target_partitions(target_partitions.get());
    }
    let context = SessionContext::new_with_config(config);

    context.register_table(INPUT_TABLE_NAME, Arc::new(table))?;

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
