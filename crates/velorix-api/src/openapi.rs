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
                            "schema": openapi_ingest_relations_request_schema(
                                state.authoritative_relation_ingest_enabled()
                            )
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
    paths.insert(
        "/v1/views/{view_id}/backfill".to_string(),
        json!({
            "get": {
                "summary": "Read durable backfill progress for one view",
                "description": "Reports the backfill outcome, lifecycle, materialization coverage, and committed batch progress for the requested view.",
                "parameters": [{"name":"view_id","in":"path","required":true,"schema":{"type":"string"}}],
                "responses": {
                    "200": {
                        "description": "Backfill status for the requested view",
                        "content": {
                            "application/json": {
                                "schema": openapi_backfill_status_response_schema()
                            }
                        }
                    }
                }
            },
            "post": {
                "summary": "Advance explicit historical backfill",
                "description": "Steps bounded backfill work, or persists a pause/resume control acknowledgement. A control acknowledgement does not imply materialization completeness. Demand backfill controls require an authoritative demand-managed view.",
                "parameters": [{"name":"view_id","in":"path","required":true,"schema":{"type":"string"}}],
                "requestBody": {
                    "required": true,
                    "content": {
                        "application/json": {
                            "schema": openapi_demand_backfill_schema()
                        }
                    }
                },
                "responses": {
                    "200": {
                        "description": "Completed step, or an acknowledged pause/resume control state",
                        "content": {
                            "application/json": {
                                "schema": openapi_demand_materialization_response_schema()
                            }
                        }
                    },
                    "202": {
                        "description": "Incomplete, paused, or yielded work; this JSON progress response is not a complete materialization result",
                        "content": {
                            "application/json": {
                                "schema": openapi_demand_materialization_response_schema()
                            }
                        }
                    },
                    "400": {
                        "description": "Invalid backfill request, conflicting batch limits, unsupported demand control, or a non-positive materialization budget",
                        "content": {
                            "application/json": {
                                "schema": openapi_error_response_schema()
                            }
                        }
                    },
                    "413": openapi_demand_budget_response()
                }
            }
        }),
    );
    for view in views {
        let response = active_view_response(&view, None)?;
        if !response.query_enabled {
            continue;
        }
        let query_responses = json!({
            "200": {
                "description": "Arrow IPC stream by default; request Accept: application/json for JSON rows",
                "headers": {
                    "x-velorix-logical-epoch": {"schema": {"type": "integer", "format": "int64"}},
                    "x-velorix-next-page-token": {"schema": {"type": "string"}}
                },
                "content": {
                    "application/vnd.apache.arrow.stream": {
                        "schema": {"type": "string", "format": "binary"}
                    },
                    "application/json": {
                        "schema": openapi_query_response_schema(response.response_schema.as_ref())
                    }
                }
            },
            "202": openapi_demand_incomplete_response(),
            "400": {
                "description": "Invalid query request or query parameters",
                "content": {
                    "application/json": {
                        "schema": openapi_error_response_schema()
                    }
                }
            },
            "406": {"description": "No supported query response format is acceptable"},
            "413": openapi_demand_budget_response()
        });
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
                    "responses": query_responses.clone()
                },
                "post": {
                    "summary": response.description.clone().unwrap_or_else(|| {
                        format!("Query {}", response.view_id)
                    }),
                    "x-velorix-view-id": response.view_id,
                    "x-velorix-url-path": response.url_path,
                    "x-velorix-output-relation-id": response.output_relation_id,
                    "x-velorix-input-relation-id": response.input_relation_id,
                    "x-velorix-input-relation-version": response.input_relation_version,
                    "x-velorix-spec-hash": response.spec_hash,
                    "x-velorix-request": response.request,
                    "x-velorix-response-schema": response.response_schema,
                    "x-velorix-sql-template": response.sql_template,
                    "x-velorix-query-policy-id": response.query_policy_id,
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": openapi_query_request_schema()
                            }
                        }
                    },
                    "responses": query_responses
                }
            }),
        );
        for output in &response.output_relations {
            let output_query_responses = json!({
                "200": {
                    "description": "Arrow IPC stream by default; request Accept: application/json for JSON rows",
                    "headers": {
                        "x-velorix-logical-epoch": {"schema": {"type": "integer", "format": "int64"}},
                        "x-velorix-next-page-token": {"schema": {"type": "string"}}
                    },
                    "content": {
                        "application/vnd.apache.arrow.stream": {
                            "schema": {"type": "string", "format": "binary"}
                        },
                        "application/json": {
                            "schema": openapi_query_response_schema(None)
                        }
                    }
                },
                "202": openapi_demand_incomplete_response(),
                "400": {
                    "description": "Invalid query request or query parameters",
                    "content": {
                        "application/json": {
                            "schema": openapi_error_response_schema()
                        }
                    }
                },
                "406": {"description": "No supported query response format is acceptable"},
                "413": openapi_demand_budget_response()
            });
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
                        "responses": output_query_responses.clone()
                    },
                    "post": {
                        "summary": format!("Query {} output {}", response.view_id, output.relation_id),
                        "x-velorix-view-id": response.view_id,
                        "x-velorix-output-relation-id": output.relation_id,
                        "x-velorix-output-schema-fingerprint": output.schema_fingerprint,
                        "requestBody": {
                            "required": true,
                            "content": {
                                "application/json": {
                                    "schema": openapi_query_request_schema()
                                }
                            }
                        },
                        "responses": output_query_responses
                    }
                }),
            );
        }
    }

    Ok(Json(json!({
        "openapi": "3.0.3",
        "info": {
            "title": "Velorix View APIs",
            "version": "0.1.0",
            "x-velorix-relation-ingest-mode": state.relation_ingest_mode.as_str()
        },
        "paths": Value::Object(paths)
    })))
}

fn openapi_query_request_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "sql": {"type": "string"},
            "parameters": {"type": "object", "additionalProperties": true},
            "epoch": {"type": "integer", "minimum": 0},
            "page_token": {"type": "string"},
            "max_rows": {"type": "integer", "minimum": 1},
            "materialization": {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "scope": {
                        "type": "object",
                        "additionalProperties": false,
                        "properties": {
                            "where": {"type": "string"}
                        }
                    },
                    "budget": {
                        "type": "object",
                        "additionalProperties": false,
                        "properties": {
                            "batch_limit": {"type": "integer", "minimum": 1},
                            "max_rows": {"type": "integer", "minimum": 1},
                            "max_bytes": {"type": "integer", "minimum": 1},
                            "max_duration_ms": {"type": "integer", "minimum": 1}
                        }
                    }
                }
            }
        }
    })
}

/// Error body shared by every `ApiError` response: `{"error": ..., "details": ...}`.
fn openapi_error_response_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "error": {"type": "string"},
            "details": {"type": "object", "additionalProperties": true}
        }
    })
}

/// Demand materialization progress body returned by demand reads and backfill
/// steps: `{"materialization": <outcome>}`.
fn openapi_demand_materialization_response_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "materialization": {
                "type": "object",
                "properties": {
                    "status": {"type": "string"},
                    "complete": {"type": "boolean"},
                    "processed_rows": {"type": "integer"},
                    "read_bytes": {"type": "integer"},
                    "remaining_rows": {"type": "integer"},
                    "dependency_mode": {"type": "string"}
                }
            }
        }
    })
}

/// `202` response for a demand read that could not complete materialization
/// within the requested budget.
fn openapi_demand_incomplete_response() -> Value {
    json!({
        "description": "Materialization is incomplete; this JSON progress response is not a complete query result",
        "content": {
            "application/json": {
                "schema": openapi_demand_materialization_response_schema()
            }
        }
    })
}

/// `413` response for an authenticated-object or signed atomic unit that
/// exceeds the supplied materialization budget.
fn openapi_demand_budget_response() -> Value {
    json!({
        "description": "The authenticated whole-object byte unit or signed atomic row unit exceeds the supplied budget. Required minima are returned without silently exceeding the limit. These budgets do not guarantee CPU time or peak RSS.",
        "content": {
            "application/json": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "error": {"type": "string"},
                        "details": {
                            "type": "object",
                            "properties": {
                                "code": {"type": "string", "description": "Budget-specific error code"},
                                "required_min_rows": {"type": "integer", "minimum": 1, "description": "Minimum rows for an indivisible signed update, when the row budget is insufficient"},
                                "required_min_bytes": {"type": "integer", "minimum": 0},
                                "supplied_max_bytes": {"type": "integer", "minimum": 0},
                                "max_supported_bytes": {"type": "integer", "minimum": 0}
                            }
                        }
                    }
                }
            }
        }
    })
}

/// Request body for `POST /v1/views/{view_id}/backfill`, matching the
/// `BackfillViewRequest` wire contract (unknown fields are rejected).
fn openapi_demand_backfill_schema() -> Value {
    let query = openapi_query_request_schema();
    let controls = &query["properties"]["materialization"]["properties"];
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "batch_limit": {"type": "integer", "minimum": 1, "description": "Compatibility alias for budget.batch_limit; conflicting values are rejected"},
            "scope": controls["scope"],
            "budget": controls["budget"],
            "action": {"type": "string", "enum": ["step", "pause", "resume"], "default": "step"},
            "order": {"type": "string", "enum": ["forward", "reverse"], "default": "forward"}
        }
    })
}

/// Response body for `GET /v1/views/{view_id}/backfill`.
fn openapi_backfill_status_response_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "view_id": {"type": "string"},
            "outcome": {"type": "string"},
            "mode": {"type": "string"},
            "lifecycle": {
                "type": "object",
                "properties": {
                    "runtime_engine": {"type": "string"},
                    "admission_status": {"type": "string", "enum": ["pending", "admitted", "system_error"]},
                    "deployment_status": {"type": "string", "enum": ["not_required", "not_deployed", "deploying", "running", "failed"]},
                    "message": {"type": "string"}
                }
            },
            "query_enabled": {"type": "boolean"},
            "coverage": {
                "type": "object",
                "properties": {
                    "state": {"type": "string"},
                    "full_view": {
                        "type": "object",
                        "properties": {
                            "status": {"type": "string"},
                            "reason": {"type": "string"}
                        }
                    }
                }
            },
            "progress": {
                "type": "object",
                "properties": {
                    "processed_batches": {"type": "integer"},
                    "remaining_batches": {"type": "integer"},
                    "total_batches": {"type": "integer"},
                    "percent": {"type": "number"}
                }
            },
            "applied_batches": {"type": "integer"},
            "remaining_batches": {"type": "integer"}
        }
    })
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
            "relation_id": { "type": "string", "description": "Optional body relation identity; when present it must match the URL relation_id" },
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

fn openapi_ingest_relations_request_schema(authoritative: bool) -> Value {
    let mut schema = json!({
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
    });
    if authoritative {
        schema["properties"]["batches"]["maxItems"] = json!(1);
        schema["properties"]["batches"]["description"] =
            json!("Authoritative mode accepts exactly one relation batch per request");
    }
    schema
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
            "relation_id": { "type": "string" },
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

#[cfg(test)]
mod openapi_spec_verification {
    use super::*;
    use axum::body::Body;
    use axum::http::{header, Method, Request};
    use object_store::memory::InMemory;
    use std::sync::Arc;
    use tower::ServiceExt;

    async fn spec() -> Value {
        let store = Arc::new(InMemory::new());
        let validated = velorix_control::operator_authority::validate_operator_authority(
            velorix_control::operator_authority::ObjectStoreAuthorityRef {
                store_id: "test".to_string(),
                namespace: "unit".to_string(),
            },
            store,
            "memory",
            "api-test",
        )
        .await
        .unwrap();
        let state = ApiState::from_validated_authority_with_ingest_admission_startup(
            validated,
            "ignored",
            "api-test-openapi-spec-owner",
            false,
        )
        .await
        .unwrap()
        .with_experimental_advanced_view_features(true)
        .with_view_bootstrap_meta_store(Arc::new(
            velorix_control::meta_admin::InMemoryMetaStore::default(),
        ));
        let router = crate::app(state);
        // Register one view so the spec also contains generated view query paths.
        for uri in [
            "/v1/relations/scores-default",
            "/v1/views/scores-positive-default",
        ] {
            let response = router
                .clone()
                .oneshot(
                    Request::builder()
                        .method(Method::POST)
                        .uri(uri)
                        .header(header::CONTENT_TYPE, "application/json")
                        .header(header::ACCEPT, "application/json")
                        .body(Body::from("{}"))
                        .unwrap(),
                )
                .await
                .unwrap();
            let status = response.status();
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            assert_eq!(
                status,
                StatusCode::CREATED,
                "{uri}: {}",
                String::from_utf8_lossy(&bytes)
            );
        }
        let response = router
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/v1/openapi.json")
                    .header(header::ACCEPT, "application/json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn spec_documents_demand_statuses_backfill_path_and_positive_budgets() {
        let spec = spec().await;

        // Finding 2: the parameterized backfill path with GET and POST.
        let backfill = &spec["paths"]["/v1/views/{view_id}/backfill"];
        assert!(backfill["get"].is_object(), "backfill GET missing: {spec}");
        assert!(
            backfill["post"].is_object(),
            "backfill POST missing: {spec}"
        );
        assert_eq!(
            backfill["post"]["responses"]["202"]["content"]["application/json"]["schema"]
                ["properties"]["materialization"]["properties"]["complete"]["type"],
            "boolean"
        );
        assert!(backfill["post"]["responses"]["413"].is_object());
        assert!(backfill["post"]["responses"]["400"].is_object());

        // Finding 3: budget minimums are 1.
        let budget = &spec["paths"]["/v1/views/{view_id}/backfill"]["post"]["requestBody"]
            ["content"]["application/json"]["schema"]["properties"]["budget"]["properties"];
        for field in ["batch_limit", "max_rows", "max_bytes", "max_duration_ms"] {
            assert_eq!(budget[field]["minimum"], 1, "{field} minimum must be 1");
        }

        // The shared query response map carries 202/413 for every generated
        // view query path and for output query paths.
        for (path, item) in spec["paths"].as_object().unwrap() {
            if !path.ends_with("/query") {
                continue;
            }
            for op in ["get", "post"] {
                if !item[op].is_object() {
                    continue;
                }
                assert!(
                    item[op]["responses"]["202"].is_object(),
                    "{path} {op} missing 202"
                );
                assert!(
                    item[op]["responses"]["413"].is_object(),
                    "{path} {op} missing 413"
                );
                if op == "post" {
                    assert!(
                        item[op]["responses"]["400"].is_object(),
                        "{path} POST missing 400"
                    );
                }
            }
        }
    }
}
