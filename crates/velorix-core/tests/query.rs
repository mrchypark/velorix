use arrow::array::{Int64Array, StringArray};
use serde_json::json;
use velorix_core::delta::{DeltaBatch, DeltaKey, DeltaRecord, DeltaValue};
use velorix_core::query::query_delta_batch;

#[tokio::test]
async fn query_delta_batch_returns_arrow_record_batches_when_sql_projects_input_columns() {
    let input = DeltaBatch::from_records([
        record("order:1", json!({ "amount": 12, "region": "us" }), 2),
        record("order:2", json!({ "amount": 7, "region": "eu" }), -1),
    ]);

    let output = query_delta_batch(
        &input,
        "select key_json, value_json, weight from input where weight > 0",
    )
    .await
    .unwrap();

    assert_eq!(output.len(), 1);
    assert_eq!(output[0].num_rows(), 1);
    assert_eq!(
        output[0]
            .schema()
            .fields()
            .iter()
            .map(|field| field.name())
            .collect::<Vec<_>>(),
        vec!["key_json", "value_json", "weight"]
    );
    assert_eq!(string_value(&output[0], 0, 0), "\"order:1\"");
    assert_eq!(
        string_value(&output[0], 1, 0),
        "{\"amount\":12,\"region\":\"us\"}"
    );
    assert_eq!(int64_value(&output[0], 2, 0), 2);
}

#[tokio::test]
async fn query_delta_batch_lets_datafusion_own_sql_planning_and_aggregation() {
    let input = DeltaBatch::from_records([
        record("acct:1", json!({ "amount": 10 }), 3),
        record("acct:1", json!({ "amount": 10 }), -1),
        record("acct:2", json!({ "amount": 4 }), 5),
    ]);

    let output = query_delta_batch(
        &input,
        "select key_json, sum(weight) as net_weight \
         from input \
         group by key_json \
         order by key_json",
    )
    .await
    .unwrap();

    assert_eq!(output.len(), 1);
    assert_eq!(output[0].num_rows(), 2);
    assert_eq!(string_value(&output[0], 0, 0), "\"acct:1\"");
    assert_eq!(int64_value(&output[0], 1, 0), 2);
    assert_eq!(string_value(&output[0], 0, 1), "\"acct:2\"");
    assert_eq!(int64_value(&output[0], 1, 1), 5);
}

#[tokio::test]
async fn query_delta_batch_returns_a_typed_error_when_datafusion_rejects_sql() {
    let input = DeltaBatch::from_records([record("acct:1", json!({ "amount": 10 }), 1)]);

    let error = query_delta_batch(&input, "select missing_column from input")
        .await
        .unwrap_err();

    assert!(error.to_string().contains("missing_column"));
}

fn record(key: &str, value: serde_json::Value, weight: i64) -> DeltaRecord {
    DeltaRecord::new(
        DeltaKey::from_json(json!(key)),
        DeltaValue::from_json(value),
        weight,
    )
}

fn string_value(batch: &arrow::record_batch::RecordBatch, column: usize, row: usize) -> &str {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(row)
}

fn int64_value(batch: &arrow::record_batch::RecordBatch, column: usize, row: usize) -> i64 {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(row)
}
