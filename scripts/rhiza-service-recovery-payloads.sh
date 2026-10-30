#!/bin/sh
# Sourceable JSON payload writers for the Rhiza service recovery harness.
# Sourcing this file only defines functions; it performs no writes.

rhiza_service_recovery_write_relation_payload() {
    if [ "$#" -ne 1 ] || [ -z "$1" ]; then
        echo "usage: rhiza_service_recovery_write_relation_payload OUTPUT_DIR" >&2
        return 2
    fi
    mkdir -p "$1" || return
    cat >"$1/relation.json" <<'RHIZA_RELATION_JSON'
{
  "catalog": {
    "schema_version": 1,
    "relation_schema": {
      "relation_id": "samples",
      "relation_name": "samples",
      "relation_version": "v1",
      "columns": [
        {
          "column_id": "reading",
          "name": "reading",
          "logical_type": {"kind": "float64"},
          "physical_arrow_type": {"kind": "float64"},
          "nullable": false,
          "ordinal": 0,
          "semantic_role": "value"
        },
        {
          "column_id": "delta",
          "name": "delta",
          "logical_type": {"kind": "int64"},
          "physical_arrow_type": {"kind": "int64"},
          "nullable": false,
          "ordinal": 1,
          "semantic_role": "weight"
        }
      ],
      "primary_key_column_ids": [],
      "weight_column_id": "delta",
      "allowed_operations": ["insert"],
      "event_time_column_id": null
    },
    "schema_fingerprint": "sha256:268ecf9d783fd3f994cbc290753a0949584aba557abdd78e4387252adc159f46",
    "datafusion_registration": {
      "name": "samples",
      "mode": "table"
    },
    "incremental_relation": {
      "relation_id": "samples",
      "schema_fingerprint": "sha256:268ecf9d783fd3f994cbc290753a0949584aba557abdd78e4387252adc159f46"
    },
    "incremental_adapter": {
      "adapter_id": "incremental-adapter-generic-v1"
    },
    "relation_source": {"kind": "source_relation"}
  }
}
RHIZA_RELATION_JSON
}

rhiza_service_recovery_write_view_sum_payload() {
    if [ "$#" -ne 1 ] || [ -z "$1" ]; then
        echo "usage: rhiza_service_recovery_write_view_sum_payload OUTPUT_DIR" >&2
        return 2
    fi
    mkdir -p "$1" || return
    cat >"$1/view-sum.json" <<'RHIZA_VIEW_SUM_JSON'
{
  "view_id": "sample_sum",
  "input_relation_id": "samples",
  "input_relation_version": "v1",
  "sql": "SELECT SUM(reading) AS result FROM samples",
  "response_formats": ["json"]
}
RHIZA_VIEW_SUM_JSON
}

rhiza_service_recovery_write_view_count_payload() {
    if [ "$#" -ne 1 ] || [ -z "$1" ]; then
        echo "usage: rhiza_service_recovery_write_view_count_payload OUTPUT_DIR" >&2
        return 2
    fi
    mkdir -p "$1" || return
    cat >"$1/view-count.json" <<'RHIZA_VIEW_COUNT_JSON'
{
  "view_id": "sample_count",
  "input_relation_id": "samples",
  "input_relation_version": "v1",
  "sql": "SELECT COUNT(*) AS result FROM samples",
  "response_formats": ["json"]
}
RHIZA_VIEW_COUNT_JSON
}

rhiza_service_recovery_write_ingest_payload() {
    if [ "$#" -ne 1 ] || [ -z "$1" ]; then
        echo "usage: rhiza_service_recovery_write_ingest_payload OUTPUT_DIR" >&2
        return 2
    fi
    mkdir -p "$1" || return
    cat >"$1/ingest.json" <<'RHIZA_INGEST_JSON'
{
  "relation_version": "v1",
  "stream_id": "samples-stream",
  "partition_id": 0,
  "start_offset_inclusive": 0,
  "rows": [
    {"reading": 2.5, "delta": 1}
  ]
}
RHIZA_INGEST_JSON
}

rhiza_service_recovery_write_query_payload() {
    if [ "$#" -ne 1 ] || [ -z "$1" ]; then
        echo "usage: rhiza_service_recovery_write_query_payload OUTPUT_DIR" >&2
        return 2
    fi
    mkdir -p "$1" || return
    cat >"$1/query.json" <<'RHIZA_QUERY_JSON'
{}
RHIZA_QUERY_JSON
}

rhiza_service_recovery_write_payloads() {
    if [ "$#" -ne 1 ] || [ -z "$1" ]; then
        echo "usage: rhiza_service_recovery_write_payloads OUTPUT_DIR" >&2
        return 2
    fi
    rhiza_service_recovery_write_relation_payload "$1" || return
    rhiza_service_recovery_write_view_sum_payload "$1" || return
    rhiza_service_recovery_write_view_count_payload "$1" || return
    rhiza_service_recovery_write_ingest_payload "$1" || return
    rhiza_service_recovery_write_query_payload "$1"
}
