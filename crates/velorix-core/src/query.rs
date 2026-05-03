use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use thiserror::Error;

use crate::delta::DeltaBatch;

pub const INPUT_TABLE_NAME: &str = "input";

#[derive(Debug, Error)]
pub enum QueryError {
    #[error(transparent)]
    Arrow(#[from] ArrowError),
    #[error(transparent)]
    DataFusion(#[from] DataFusionError),
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
    let context = SessionContext::new();

    context.register_table(INPUT_TABLE_NAME, Arc::new(table))?;

    Ok(context.sql(sql).await?.collect().await?)
}
