use super::*;

pub(super) async fn openapi_json(State(state): State<ApiState>) -> Result<Json<Value>, ApiError> {
    let views = state
        .view_registry()?
        .list_active()
        .await
        .map_err(materialized_view_registry_error_to_api)?;
    let mut paths = serde_json::Map::new();

    paths.insert(
        "/v1/relations".to_string(),
        json!({
            "post": {
                "summary": "Create a relation catalog",
                "responses": { "201": { "description": "Relation created" } }
            }
        }),
    );
    paths.insert(
        "/v1/relations/{relation_id}/ingest".to_string(),
        json!({
            "post": {
                "summary": "Ingest rows into one relation and update active views",
                "description": "Preferred product ingest path. The relation is selected by the URL path; successful materialized ack updates all active materialized views that depend on the relation before returning.",
                "parameters": [{
                    "name": "relation_id",
                    "in": "path",
                    "required": true,
                    "schema": { "type": "string" }
                }],
                "requestBody": {
                    "required": true,
                    "content": {
                        "application/json": {
                            "schema": openapi_ingest_relation_rows_request_schema()
                        }
                    }
                },
                "responses": {
                    "201": {
                        "description": "Rows ingested and matching active views materialized",
                        "content": {
                            "application/json": {
                                "schema": openapi_ingest_epoch_response_schema()
                            }
                        }
                    }
                }
            }
        }),
    );
    paths.insert(
        "/v1/relations/ingest".to_string(),
        json!({
            "post": {
                "summary": "Ingest ordered batches for one or more relations",
                "description": "Public multi-relation ingest convenience path. It applies the submitted relation batches in request order and publishes materialized views at the resulting per-relation frontier vector; it is not an atomic multi-relation transaction API. A successful materialized ack durably appends every batch and updates every active dependent materialized view before returning.",
                "requestBody": {
                    "required": true,
                    "content": {
                        "application/json": {
                            "schema": openapi_ingest_relations_request_schema()
                        }
                    }
                },
                "responses": {
                    "201": {
                        "description": "Batches ingested and matching active views materialized",
                        "content": {
                            "application/json": {
                                "schema": openapi_ingest_epoch_response_schema()
                            }
                        }
                    }
                }
            }
        }),
    );
    paths.insert(
        "/v1/views".to_string(),
        json!({
            "get": {
                "summary": "List view APIs",
                "responses": { "200": { "description": "View catalog" } }
            },
            "post": {
                "summary": "Create a view API",
                "responses": { "201": { "description": "View created" } }
            }
        }),
    );
    for view in views {
        let response = active_view_response(&view, None)?;
        if !response.query_enabled {
            continue;
        }
        paths.insert(
            openapi_path_from_query_endpoint(&response.query_endpoint),
            json!({
                "get": {
                    "summary": response.description.clone().unwrap_or_else(|| {
                        format!("Query {}", response.view_id)
                    }),
                    "x-velorix-view-id": response.view_id,
                    "x-velorix-url-path": response.url_path,
                    "x-velorix-output-relation-id": response.output_relation_id,
                    "x-velorix-input-relation-id": response.input_relation_id,
                    "x-velorix-input-relation-version": response.input_relation_version,
                    "x-velorix-spec-hash": response.spec_hash,
                    "x-velorix-request": response.request.clone(),
                    "x-velorix-response-schema": response.response_schema.clone(),
                    "x-velorix-sql-template": response.sql_template.clone(),
                    "x-velorix-query-policy-id": response.query_policy_id.clone(),
                    "parameters": openapi_view_query_parameters(
                        &response.request,
                        !(response.execution_mode == MaterializedViewExecutionMode::StandingRuntime
                            && response.sql_template.is_some()),
                        response.url_path.is_none()
                    ),
                    "responses": {
                        "200": {
                            "description": "View query result rows",
                            "content": {
                                "application/json": {
                                    "schema": openapi_query_response_schema(
                                        response.response_schema.as_ref()
                                    )
                                }
                            }
                        }
                    }
                }
            }),
        );
        for output in &response.output_relations {
            paths.insert(
                format!(
                    "/v1/views/{}/outputs/{}/query",
                    response.view_id, output.relation_id
                ),
                json!({
                    "get": {
                        "summary": format!("Query {} output {}", response.view_id, output.relation_id),
                        "x-velorix-view-id": response.view_id,
                        "x-velorix-output-relation-id": output.relation_id,
                        "x-velorix-output-schema-fingerprint": output.schema_fingerprint,
                        "parameters": openapi_view_query_parameters(
                            &[],
                            true,
                            true,
                        ),
                        "responses": { "200": { "description": "Rows" } }
                    },
                    "post": {
                        "summary": format!("Query {} output {}", response.view_id, output.relation_id),
                        "x-velorix-view-id": response.view_id,
                        "x-velorix-output-relation-id": output.relation_id,
                        "x-velorix-output-schema-fingerprint": output.schema_fingerprint,
                        "responses": { "200": { "description": "Rows" } }
                    }
                }),
            );
        }
    }

    Ok(Json(json!({
        "openapi": "3.0.3",
        "info": {
            "title": "Velorix View APIs",
            "version": "0.1.0"
        },
        "paths": Value::Object(paths)
    })))
}

fn openapi_view_query_parameters(
    request: &[MaterializedViewRequestFieldSpec],
    include_cursor_parameters: bool,
    include_sql_parameter: bool,
) -> Value {
    let mut parameters = request
        .iter()
        .map(|field| {
            json!({
                "name": field.field_name,
                "in": field.field_in,
                "required": request_field_has_validator(field, "required") || field.field_in == "path",
                "description": field.description,
                "schema": openapi_request_field_schema(field)
            })
        })
        .collect::<Vec<_>>();
    parameters.push(json!({
        "name": "epoch",
        "in": "query",
        "required": false,
        "description": "Committed logical epoch to read",
        "schema": { "type": "integer", "minimum": 0 }
    }));
    if include_sql_parameter {
        parameters.push(json!({
            "name": "sql",
            "in": "query",
            "required": false,
            "description": "DataFusion SQL to run against the materialized view output table",
            "schema": { "type": "string" }
        }));
    }
    if include_cursor_parameters {
        parameters.push(json!({
            "name": "page_token",
            "in": "query",
            "required": false,
            "description": "Cursor returned by next_page_token",
            "schema": { "type": "string" }
        }));
        parameters.push(json!({
            "name": "max_rows",
            "in": "query",
            "required": false,
            "description": "Maximum materialized rows to return",
            "schema": { "type": "integer", "minimum": 1 }
        }));
    }
    Value::Array(parameters)
}

fn openapi_ingest_relation_rows_request_schema() -> Value {
    json!({
        "type": "object",
        "required": [
            "relation_version",
            "stream_id",
            "partition_id",
            "start_offset_inclusive",
            "rows"
        ],
        "properties": {
            "relation_version": { "type": "string" },
            "stream_id": { "type": "string" },
            "partition_id": { "type": "integer", "minimum": 0 },
            "start_offset_inclusive": { "type": "integer", "minimum": 0 },
            "event_time_watermark": {
                "type": "object",
                "properties": {
                    "event_time_column_id": { "type": "string" },
                    "max_observed_event_time_ns": { "type": "integer", "format": "int64" },
                    "watermark_ns": { "type": "integer", "format": "int64" }
                }
            },
            "rows": {
                "type": "array",
                "items": { "type": "object" }
            }
        }
    })
}

fn openapi_ingest_relations_request_schema() -> Value {
    json!({
        "type": "object",
        "required": ["batches"],
        "properties": {
            "batches": {
                "type": "array",
                "minItems": 1,
                "items": {
                    "type": "object",
                    "required": [
                        "relation_id",
                        "relation_version",
                        "stream_id",
                        "partition_id",
                        "start_offset_inclusive",
                        "rows"
                    ],
                    "properties": {
                        "relation_id": { "type": "string" },
                        "relation_version": { "type": "string" },
                        "stream_id": { "type": "string" },
                        "partition_id": { "type": "integer", "minimum": 0 },
                        "start_offset_inclusive": { "type": "integer", "minimum": 0 },
                        "event_time_watermark": {
                            "type": "object",
                            "properties": {
                                "event_time_column_id": { "type": "string" },
                                "max_observed_event_time_ns": { "type": "integer", "format": "int64" },
                                "watermark_ns": { "type": "integer", "format": "int64" }
                            }
                        },
                        "rows": {
                            "type": "array",
                            "items": { "type": "object" }
                        }
                    }
                }
            }
        }
    })
}

fn openapi_ingest_materialization_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "status": { "type": "string", "enum": ["completed", "skipped", "duplicate", "epoch_scoped"] },
            "active_views": { "type": "integer", "minimum": 0 },
            "applied_batches": { "type": "integer", "minimum": 0 },
            "materialized_through": {
                "type": ["integer", "null"],
                "format": "int64",
                "description": "Conservative logical epoch boundary published by every active dependent view affected by this ingest epoch; null when no dependent active view had to advance."
            },
            "checkpoint_writes": { "type": "integer", "minimum": 0 },
            "applied_batches_per_checkpoint_write": {
                "type": ["integer", "null"],
                "minimum": 0,
                "description": "Integer ratio showing checkpoint coalescing. For example, 2 means two applied batches were published with one checkpoint write."
            },
            "output_delta_writes": { "type": "integer", "minimum": 0 },
            "state_payload_writes": { "type": "integer", "minimum": 0 },
            "checkpoint_record_writes": { "type": "integer", "minimum": 0 },
            "checkpoint_pointer_writes": { "type": "integer", "minimum": 0 },
            "checkpoint_publication_writes": { "type": "integer", "minimum": 0 }
        }
    })
}

fn openapi_ingest_timing_schema() -> Value {
    json!({
        "type": "object",
        "description": "Stable wall-clock ingest timing counters. Detailed per-stage timings are intentionally not part of the public 1.0 response contract; publish them through traces or metrics.",
        "properties": {
            "total_ms": { "type": "integer", "minimum": 0 },
            "total_us": { "type": "integer", "minimum": 0 },
            "avg_batch_us": { "type": ["integer", "null"], "minimum": 0 },
            "avg_row_us": { "type": ["integer", "null"], "minimum": 0 },
            "rows_per_second": { "type": ["integer", "null"], "minimum": 0 },
            "batch_count": { "type": "integer", "minimum": 0 },
            "row_count": { "type": "integer", "minimum": 0 }
        }
    })
}

fn openapi_ingest_ack_mode_schema() -> Value {
    json!({
        "type": "string",
        "enum": ["materialized"],
        "default": "materialized",
        "readOnly": true,
        "description": "A successful public 1.0 ingest response means the input is durable and every active dependent view has published a checkpoint at or beyond materialized_through"
    })
}

fn openapi_ingest_descriptor_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "stream_id": { "type": "string" },
            "partition_id": { "type": "integer", "minimum": 0 },
            "start_offset_inclusive": { "type": "integer", "minimum": 0 },
            "end_offset_exclusive": { "type": "integer", "minimum": 0 },
            "object_key": { "type": "string" }
        }
    })
}

fn openapi_ingest_response_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "outcome": { "type": "string" },
            "descriptor": openapi_ingest_descriptor_schema(),
            "epoch_manifest_id": { "type": "string" },
            "ingest_epoch": { "type": "string" },
            "materialized_through": {
                "type": ["integer", "null"],
                "format": "int64"
            },
            "ack_mode": openapi_ingest_ack_mode_schema(),
            "materialization": openapi_ingest_materialization_schema(),
            "timings": openapi_ingest_timing_schema()
        }
    })
}

fn openapi_ingest_epoch_response_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "outcome": { "type": "string" },
            "epoch_manifest_id": { "type": "string" },
            "epoch_manifest_key": { "type": "string" },
            "ingest_epoch": { "type": "string" },
            "materialized_through": {
                "type": ["integer", "null"],
                "format": "int64"
            },
            "ack_mode": openapi_ingest_ack_mode_schema(),
            "materialization": openapi_ingest_materialization_schema(),
            "timings": openapi_ingest_timing_schema(),
            "batches": {
                "type": "array",
                "items": openapi_ingest_response_schema()
            }
        }
    })
}

fn openapi_request_field_schema(field: &MaterializedViewRequestFieldSpec) -> Value {
    let mut schema = openapi_scalar_schema(&field.r#type);
    if let (Some(object), Some(default_value)) = (schema.as_object_mut(), &field.default_value) {
        object.insert("default".to_string(), default_value.clone());
    }
    schema
}

fn openapi_query_response_schema(
    response_schema: Option<&MaterializedViewResponseSchema>,
) -> Value {
    let mut row_properties = serde_json::Map::new();
    if let Some(response_schema) = response_schema {
        for column in &response_schema.columns {
            row_properties.insert(
                column.name.clone(),
                openapi_response_column_schema(&column.r#type),
            );
        }
    } else {
        row_properties.insert("key".to_string(), openapi_scalar_schema("string"));
        row_properties.insert("value".to_string(), openapi_scalar_schema("string"));
        row_properties.insert("weight".to_string(), openapi_scalar_schema("int64"));
    }

    json!({
        "type": "object",
        "properties": {
            "rows": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": Value::Object(row_properties)
                }
            },
            "logical_epoch": {
                "type": "integer",
                "format": "int64"
            },
            "next_page_token": {
                "type": "string"
            }
        }
    })
}

fn openapi_response_column_schema(type_name: &str) -> Value {
    let mut schema = openapi_scalar_schema(type_name);
    if let Some(object) = schema.as_object_mut() {
        object.insert("nullable".to_string(), Value::Bool(true));
    }
    schema
}

fn openapi_scalar_schema(type_name: &str) -> Value {
    match type_name {
        "string" => json!({ "type": "string" }),
        "int64" => json!({ "type": "integer", "format": "int64" }),
        "integer" => json!({ "type": "integer" }),
        "float64" => json!({ "type": "number", "format": "double" }),
        "number" => json!({ "type": "number" }),
        "bool" | "boolean" => json!({ "type": "boolean" }),
        "array" => json!({ "type": "array", "items": {} }),
        "object" => json!({ "type": "object" }),
        "date" => json!({ "type": "string", "format": "date" }),
        "time" => json!({ "type": "string", "format": "time" }),
        "timestamp" => json!({ "type": "string", "format": "date-time" }),
        "uuid" => json!({ "type": "string", "format": "uuid" }),
        "decimal" => json!({ "type": "string", "format": "decimal" }),
        "binary_hex" => json!({ "type": "string", "pattern": "^(0[xX])?[0-9a-fA-F]*$" }),
        "json" => json!({}),
        _ => json!({}),
    }
}
