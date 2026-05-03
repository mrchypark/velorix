use arrow::array::{Array, Int64Array, StringArray};
use serde_json::json;
use std::num::NonZeroUsize;
use velorix_core::delta::{DeltaBatch, DeltaKey, DeltaRecord, DeltaValue};

use velorix_core::query::{
    query_delta_batch, query_delta_batch_with_policy, validate_input_query_with_policy, QueryError,
    QueryPolicy, QueryPolicyError,
};

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
async fn query_delta_batch_with_policy_rejects_sql_text_above_byte_limit() {
    let input = DeltaBatch::from_records([record("acct:1", json!({ "amount": 10 }), 1)]);
    let policy = QueryPolicy {
        max_sql_bytes: Some("select * from input".len() - 1),
        ..QueryPolicy::default()
    };

    let error = query_delta_batch_with_policy(&input, "select * from input", policy)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        QueryError::Policy(QueryPolicyError::SqlTextTooLarge {
            actual_bytes,
            max_bytes
        }) if actual_bytes == "select * from input".len() && max_bytes == "select * from input".len() - 1
    ));
}

#[tokio::test]
async fn query_delta_batch_with_policy_returns_results_at_row_limit() {
    let input = DeltaBatch::from_records([
        record("acct:1", json!({ "amount": 10 }), 1),
        record("acct:2", json!({ "amount": 4 }), 1),
    ]);
    let policy = QueryPolicy {
        max_output_rows: Some(2),
        batch_size: NonZeroUsize::new(1),
        target_partitions: NonZeroUsize::new(1),
        ..QueryPolicy::default()
    };

    let output = query_delta_batch_with_policy(
        &input,
        "select key_json, value_json, weight from input order by key_json",
        policy,
    )
    .await
    .unwrap();

    assert_eq!(num_rows(&output), 2);
    assert_eq!(string_values(&output, 0), vec!["\"acct:1\"", "\"acct:2\""]);
}

#[tokio::test]
async fn query_delta_batch_with_policy_rejects_results_above_row_limit() {
    let input = DeltaBatch::from_records([
        record("acct:1", json!({ "amount": 10 }), 1),
        record("acct:2", json!({ "amount": 4 }), 1),
        record("acct:3", json!({ "amount": 8 }), 1),
    ]);
    let policy = QueryPolicy {
        max_output_rows: Some(2),
        ..QueryPolicy::default()
    };

    let error = query_delta_batch_with_policy(
        &input,
        "select key_json, value_json, weight from input order by key_json",
        policy,
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        QueryError::Policy(QueryPolicyError::OutputRowsExceeded {
            observed_rows,
            max_rows: 2
        }) if observed_rows == 3
    ));
}

#[tokio::test]
async fn query_delta_batch_with_policy_still_lets_datafusion_handle_aggregation_planning() {
    let input = DeltaBatch::from_records([
        record("acct:1", json!({ "amount": 10 }), 3),
        record("acct:1", json!({ "amount": 10 }), -1),
        record("acct:2", json!({ "amount": 4 }), 5),
    ]);
    let policy = QueryPolicy {
        max_output_rows: Some(2),
        batch_size: NonZeroUsize::new(1),
        target_partitions: NonZeroUsize::new(1),
        ..QueryPolicy::default()
    };

    let output = query_delta_batch_with_policy(
        &input,
        "select key_json, sum(weight) as net_weight \
         from input \
         group by key_json \
         order by key_json",
        policy,
    )
    .await
    .unwrap();

    assert_eq!(num_rows(&output), 2);
    assert_eq!(string_values(&output, 0), vec!["\"acct:1\"", "\"acct:2\""]);
    assert_eq!(int64_values(&output, 1), vec![2, 5]);
}

#[tokio::test]
async fn query_delta_batch_returns_a_typed_error_when_datafusion_rejects_sql() {
    let input = DeltaBatch::from_records([record("acct:1", json!({ "amount": 10 }), 1)]);

    let error = query_delta_batch(&input, "select missing_column from input")
        .await
        .unwrap_err();

    assert!(error.to_string().contains("missing_column"));
}

#[tokio::test]
async fn validate_input_query_accepts_valid_select_over_empty_input() {
    validate_input_query_with_policy(
        "select key_json, value_json, weight from input where weight >= 0",
        QueryPolicy::default(),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn validate_input_query_rejects_missing_input_column() {
    let error = validate_input_query_with_policy(
        "select missing_column from input",
        QueryPolicy::default(),
    )
    .await
    .unwrap_err();

    assert!(error.to_string().contains("missing_column"));
}

#[tokio::test]
async fn validate_input_query_rejects_sql_text_above_policy_limit() {
    let sql = "select * from input";
    let error = validate_input_query_with_policy(
        sql,
        QueryPolicy {
            max_sql_bytes: Some(sql.len() - 1),
            ..QueryPolicy::default()
        },
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        QueryError::Policy(QueryPolicyError::SqlTextTooLarge {
            actual_bytes,
            max_bytes
        }) if actual_bytes == sql.len() && max_bytes == sql.len() - 1
    ));
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

fn num_rows(batches: &[arrow::record_batch::RecordBatch]) -> usize {
    batches
        .iter()
        .map(arrow::record_batch::RecordBatch::num_rows)
        .sum()
}

fn string_values(batches: &[arrow::record_batch::RecordBatch], column: usize) -> Vec<&str> {
    batches
        .iter()
        .flat_map(|batch| {
            let values = batch
                .column(column)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..values.len()).map(|row| values.value(row))
        })
        .collect()
}

fn int64_values(batches: &[arrow::record_batch::RecordBatch], column: usize) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|batch| {
            let values = batch
                .column(column)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            (0..values.len()).map(|row| values.value(row))
        })
        .collect()
}
