use std::error::Error as StdError;

use arrow::error::ArrowError;
use thiserror::Error;

pub use crate::resource_policy::{QueryExecutionPolicyV1, QueryPolicy, QueryPolicyError};

pub const INPUT_TABLE_NAME: &str = "input";

#[derive(Clone, Debug, PartialEq)]
pub enum QueryBindValue {
    Utf8(String),
    Json(String),
    Int64(i64),
    Float64(f64),
    Boolean(bool),
    Date(String),
    Time(String),
    Timestamp(String),
    Uuid(String),
    Decimal(String),
    Binary(Vec<u8>),
    Utf8Array(Vec<String>),
    JsonArray(Vec<String>),
    Int64Array(Vec<i64>),
    Float64Array(Vec<f64>),
    BooleanArray(Vec<bool>),
    DateArray(Vec<String>),
    TimeArray(Vec<String>),
    TimestampArray(Vec<String>),
    UuidArray(Vec<String>),
    DecimalArray(Vec<String>),
    BinaryArray(Vec<Vec<u8>>),
}

#[derive(Debug, Error)]
pub enum QueryError {
    #[error(transparent)]
    Arrow(#[from] ArrowError),
    #[error(transparent)]
    Engine(#[from] Box<dyn StdError + Send + Sync>),
    #[error(transparent)]
    Policy(#[from] QueryPolicyError),
}

impl QueryError {
    pub fn engine(error: impl StdError + Send + Sync + 'static) -> Self {
        Self::Engine(Box::new(error))
    }
}
