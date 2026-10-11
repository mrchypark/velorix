use super::*;
use arrow::{datatypes::SchemaRef, ipc::writer::StreamWriter};
use axum::http::{HeaderMap, HeaderValue};
use std::io::{self, Write};

pub(super) const ARROW_STREAM: &str = "application/vnd.apache.arrow.stream";

pub(super) struct QueryBatchResponse {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
    pub logical_epoch: u64,
    pub next_page_token: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum QueryResponseFormat {
    Arrow,
    Json,
}

impl QueryResponseFormat {
    pub fn negotiate(headers: &HeaderMap) -> Result<Self, ApiError> {
        if !headers.contains_key(header::ACCEPT) {
            return Ok(Self::Arrow);
        }
        let mut matches = [None::<(u8, u16)>; 2];
        for value in headers.get_all(header::ACCEPT) {
            let value = value.to_str().map_err(|_| not_acceptable())?;
            for range in value.split(',') {
                let mut parts = range.trim().split(';');
                let media = parts.next().unwrap_or_default().trim().to_ascii_lowercase();
                let (kind, subtype) = media.split_once('/').ok_or_else(not_acceptable)?;
                if !media_token(kind) || !media_token(subtype) || (kind == "*" && subtype != "*") {
                    return Err(not_acceptable());
                }
                let mut quality = 1000;
                let mut saw_quality = false;
                let mut media_parameters = false;
                for parameter in parts {
                    let (name, value) = parameter
                        .trim()
                        .split_once('=')
                        .ok_or_else(not_acceptable)?;
                    if name.trim().eq_ignore_ascii_case("q") {
                        if saw_quality {
                            return Err(not_acceptable());
                        }
                        quality = parse_quality(value.trim()).ok_or_else(not_acceptable)?;
                        saw_quality = true;
                    } else if !saw_quality {
                        media_parameters = true;
                    }
                }
                if media_parameters {
                    continue;
                }
                for (index, candidate) in [ARROW_STREAM, "application/json"].iter().enumerate() {
                    let specificity = if media == *candidate {
                        2
                    } else if media == "application/*" {
                        1
                    } else if media == "*/*" {
                        0
                    } else {
                        continue;
                    };
                    let score = (specificity, quality);
                    if matches[index].is_none_or(|previous| score > previous) {
                        matches[index] = Some(score);
                    }
                }
            }
        }
        let arrow = matches[0].map_or(0, |(_, quality)| quality);
        let json = matches[1].map_or(0, |(_, quality)| quality);
        if arrow == 0 && json == 0 {
            Err(not_acceptable())
        } else if json > arrow || (json == arrow && matches[1] > matches[0]) {
            Ok(Self::Json)
        } else {
            Ok(Self::Arrow)
        }
    }
}

#[cfg(test)]
#[path = "query_response_tests.rs"]
mod tests;

fn media_token(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte))
        && (value == "*" || !value.contains('*'))
}

fn parse_quality(value: &str) -> Option<u16> {
    let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
    if !matches!(whole, "0" | "1")
        || fraction.len() > 3
        || !fraction.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    if whole == "1" {
        return fraction.bytes().all(|b| b == b'0').then_some(1000);
    }
    let mut quality = 0;
    for index in 0..3 {
        quality = quality * 10
            + fraction
                .as_bytes()
                .get(index)
                .map_or(0, |b| u16::from(b - b'0'));
    }
    Some(quality)
}

fn not_acceptable() -> ApiError {
    let mut error = ApiError::bad_request("Accept must allow application/vnd.apache.arrow.stream or application/json with a valid nonzero quality");
    error.status = StatusCode::NOT_ACCEPTABLE;
    error
}

pub(super) fn apply_response_schema(
    mut response: QueryBatchResponse,
    specification: &MaterializedViewResponseSchema,
) -> Result<QueryBatchResponse, ApiError> {
    let mut fields = Vec::new();
    let mut columns = Vec::new();
    for column in &specification.columns {
        let root = column.source.split('.').next().unwrap_or_default();
        let source_index = response
            .schema
            .index_of(root)
            .or_else(|_| {
                let alias = match root {
                    "key" => "key_json",
                    "key_json" => "key",
                    "value" => "value_json",
                    "value_json" => "value",
                    _ => root,
                };
                response.schema.index_of(alias)
            })
            .map_err(ApiError::bad_request)?;
        let source_type = response.schema.field(source_index).data_type();
        let direct = !column.source.contains('.')
            && match column.r#type.as_str() {
                "int64" | "integer" => matches!(
                    source_type,
                    DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
                ),
                "float64" | "number" => {
                    matches!(source_type, DataType::Float32 | DataType::Float64)
                }
                "bool" | "boolean" => *source_type == DataType::Boolean,
                "string" => *source_type == DataType::Utf8,
                "array" => matches!(source_type, DataType::List(_)),
                "object" => matches!(source_type, DataType::Struct(_) | DataType::Map(_, _)),
                "json" => !matches!(source_type, DataType::Utf8),
                _ => false,
            };
        let target_type = match column.r#type.as_str() {
            "int64" | "integer" => DataType::Int64,
            "float64" | "number" => DataType::Float64,
            "bool" | "boolean" => DataType::Boolean,
            _ if direct => source_type.clone(),
            _ => DataType::Utf8,
        };
        let mut arrays = Vec::new();
        for batch in &response.batches {
            let array = if direct {
                arrow::compute::cast(batch.column(source_index), &target_type)
                    .map_err(ApiError::bad_request)?
            } else {
                // Legacy encoded JSON sources and canonical API literals require their existing coercions.
                let values = (0..batch.num_rows())
                    .map(|row_index| {
                        let source = arrow_value_to_json(batch.column(source_index), row_index)?;
                        let row = json!({ root: source });
                        response_column_value(&row, column)
                    })
                    .collect::<Result<Vec<_>, ApiError>>()?;
                match target_type {
                    DataType::Int64 => Arc::new(Int64Array::from(
                        values.iter().map(Value::as_i64).collect::<Vec<_>>(),
                    )) as ArrayRef,
                    DataType::Float64 => Arc::new(Float64Array::from(
                        values.iter().map(Value::as_f64).collect::<Vec<_>>(),
                    )) as ArrayRef,
                    DataType::Boolean => Arc::new(BooleanArray::from(
                        values.iter().map(Value::as_bool).collect::<Vec<_>>(),
                    )) as ArrayRef,
                    _ => Arc::new(StringArray::from(
                        values
                            .into_iter()
                            .map(|value| match value {
                                Value::Null => None,
                                Value::String(value) => Some(value),
                                value => Some(value.to_string()),
                            })
                            .collect::<Vec<_>>(),
                    )) as ArrayRef,
                }
            };
            arrays.push(array);
        }
        fields.push(Field::new(&column.name, target_type, true));
        columns.push(arrays);
    }
    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        response.schema.metadata().clone(),
    ));
    let batches = response
        .batches
        .iter()
        .enumerate()
        .map(|(index, batch)| {
            let arrays = columns.iter().map(|arrays| arrays[index].clone()).collect();
            let options = arrow::record_batch::RecordBatchOptions::new()
                .with_row_count(Some(batch.num_rows()));
            RecordBatch::try_new_with_options(schema.clone(), arrays, &options)
                .map_err(ApiError::bad_request)
        })
        .collect::<Result<Vec<_>, _>>()?;
    response.schema = schema;
    response.batches = batches;
    Ok(response)
}

impl QueryBatchResponse {
    pub fn into_http_response(
        self,
        format: QueryResponseFormat,
        policy: QueryPolicy,
        json_rows: impl FnOnce(&[RecordBatch]) -> Result<Vec<Value>, ApiError>,
    ) -> Result<Response, ApiError> {
        let row_count = self.batches.iter().fold(0usize, |count, batch| {
            count.saturating_add(batch.num_rows())
        });
        if policy
            .max_output_rows
            .is_some_and(|limit| row_count > limit)
        {
            return Err(ApiError::bad_request(
                "query response exceeds max_output_rows",
            ));
        }
        let mut output = BoundedOutput {
            bytes: Vec::new(),
            limit: policy.max_output_bytes,
            exceeded: false,
        };
        let content_type = match format {
            QueryResponseFormat::Arrow => {
                let mut metadata = self.schema.metadata().clone();
                metadata.insert(
                    "velorix.logical_epoch".into(),
                    self.logical_epoch.to_string(),
                );
                if let Some(token) = &self.next_page_token {
                    metadata.insert("velorix.next_page_token".into(), token.clone());
                } else {
                    metadata.remove("velorix.next_page_token");
                }
                let schema = self.schema.as_ref().clone().with_metadata(metadata);
                let encoded = (|| {
                    let mut writer = StreamWriter::try_new(&mut output, &schema)?;
                    for batch in &self.batches {
                        if batch.schema().fields() != schema.fields() {
                            return Err(arrow::error::ArrowError::SchemaError(
                                "query response batches have inconsistent schemas".into(),
                            ));
                        }
                        writer.write(batch)?;
                    }
                    writer.finish()
                })();
                encoded.map_err(|error| output.encoding_error(error))?;
                ARROW_STREAM
            }
            QueryResponseFormat::Json => {
                let response = QueryResponse {
                    rows: json_rows(&self.batches)?,
                    logical_epoch: Some(self.logical_epoch),
                    next_page_token: self.next_page_token.clone(),
                };
                serde_json::to_writer(&mut output, &response)
                    .map_err(|error| output.encoding_error(error))?;
                "application/json"
            }
        };
        let mut response = (
            [
                (header::CONTENT_TYPE, content_type),
                (header::VARY, "Accept"),
            ],
            output.bytes,
        )
            .into_response();
        response.headers_mut().insert(
            "x-velorix-logical-epoch",
            HeaderValue::from(self.logical_epoch),
        );
        if let Some(token) = self.next_page_token {
            response.headers_mut().insert(
                "x-velorix-next-page-token",
                HeaderValue::from_str(&token).map_err(ApiError::internal)?,
            );
        }
        Ok(response)
    }
}

struct BoundedOutput {
    bytes: Vec<u8>,
    limit: Option<u64>,
    exceeded: bool,
}

impl BoundedOutput {
    fn encoding_error(&self, error: impl std::fmt::Display) -> ApiError {
        if self.exceeded {
            ApiError::bad_request("query response exceeds max_output_bytes")
        } else {
            ApiError::internal(error)
        }
    }
}

impl Write for BoundedOutput {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.limit.is_some_and(|limit| {
            (self.bytes.len() as u64).saturating_add(bytes.len() as u64) > limit
        }) {
            self.exceeded = true;
            return Err(io::Error::other("query response exceeds max_output_bytes"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
