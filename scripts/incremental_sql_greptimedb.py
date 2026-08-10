#!/usr/bin/env python3
"""Run the shared incremental SQL corpus against GreptimeDB Flow."""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import time
import urllib.request


WORKLOADS = {
    "filter_project": {
        "flow": "fp_flow",
        "sink": "fp_sink",
        "columns": ["order_id", "doubled"],
        "create": """CREATE FLOW fp_flow SINK TO fp_sink AS
            SELECT order_id, amount * 2 AS doubled FROM orders WHERE amount >= 50""",
    },
    "aggregate": {
        "flow": "aggregate_flow",
        "sink": "aggregate_sink",
        "columns": ["customer_id", "total", "order_count", "minimum", "maximum", "average"],
        "create": """CREATE FLOW aggregate_flow SINK TO aggregate_sink EVAL INTERVAL '1s' AS
            SELECT customer_id, SUM(amount) AS total, COUNT(*) AS order_count,
                   MIN(amount) AS minimum, MAX(amount) AS maximum, AVG(amount) AS average
            FROM orders GROUP BY customer_id""",
    },
    "distinct_aggregate": {
        "flow": "distinct_flow",
        "sink": "distinct_sink",
        "columns": ["customer_id", "product_count"],
        "create": """CREATE FLOW distinct_flow SINK TO distinct_sink EVAL INTERVAL '1s' AS
            SELECT customer_id, COUNT(DISTINCT product_id) AS product_count
            FROM orders GROUP BY customer_id""",
    },
    "inner_join": {
        "flow": "inner_join_flow",
        "sink": "inner_join_sink",
        "columns": ["region", "total"],
        "create": """CREATE FLOW inner_join_flow SINK TO inner_join_sink EVAL INTERVAL '1s' AS
            SELECT c.\"region\", SUM(o.amount) AS total
            FROM customers c JOIN orders o ON c.customer_id = o.customer_id
            GROUP BY c.\"region\"""",
    },
    "left_join": {
        "flow": "left_join_flow",
        "sink": "left_join_sink",
        "columns": ["customer_id", "order_count"],
        "create": """CREATE FLOW left_join_flow SINK TO left_join_sink EVAL INTERVAL '1s' AS
            SELECT c.customer_id, COUNT(o.order_id) AS order_count
            FROM customers c LEFT JOIN orders o ON c.customer_id = o.customer_id
            GROUP BY c.customer_id""",
    },
    "top_k": {
        "flow": "topk_flow",
        "sink": "topk_sink",
        "columns": ["customer_id", "total"],
        "create": """CREATE FLOW topk_flow SINK TO topk_sink EVAL INTERVAL '1s' AS
            SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id
            ORDER BY total DESC, customer_id ASC LIMIT 1""",
    },
    "fixed_window": {
        "flow": "window_flow",
        "sink": "window_sink",
        "columns": ["customer_id", "window_start", "total"],
        "create": """CREATE FLOW window_flow SINK TO window_sink AS
            SELECT customer_id, date_bin('2 minutes'::INTERVAL, event_time) AS window_start,
                   SUM(amount) AS total FROM orders GROUP BY customer_id, window_start""",
    },
    "ranking": {
        "flow": "ranking_flow",
        "sink": "ranking_sink",
        "columns": ["customer_id", "order_id", "rank"],
        "create": """CREATE FLOW ranking_flow SINK TO ranking_sink EVAL INTERVAL '1s' AS
            SELECT customer_id, order_id,
                   ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount DESC, order_id ASC) AS rank
            FROM orders""",
    },
}

ORACLE_SQL = {
    "filter_project": "SELECT order_id, amount * 2 AS doubled FROM orders WHERE amount >= 50",
    "aggregate": """SELECT customer_id, SUM(amount) AS total, COUNT(*) AS order_count,
        MIN(amount) AS minimum, MAX(amount) AS maximum, AVG(amount) AS average
        FROM orders GROUP BY customer_id""",
    "distinct_aggregate": """SELECT customer_id, COUNT(DISTINCT product_id) AS product_count
        FROM orders GROUP BY customer_id""",
    "inner_join": """SELECT c.region, SUM(o.amount) AS total FROM customers c
        JOIN orders o ON c.customer_id = o.customer_id GROUP BY c.region""",
    "left_join": """SELECT c.customer_id, COUNT(o.order_id) AS order_count FROM customers c
        LEFT JOIN orders o ON c.customer_id = o.customer_id GROUP BY c.customer_id""",
    "top_k": """SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id
        ORDER BY total DESC, customer_id ASC LIMIT 1""",
    "fixed_window": """SELECT customer_id, time_bucket(INTERVAL '2 minutes', event_time) AS window_start,
        SUM(amount) AS total FROM orders GROUP BY customer_id, window_start""",
    "ranking": """SELECT customer_id, order_id,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount DESC, order_id ASC) AS rank
        FROM orders""",
    "chained_view": """SELECT c.region, SUM(t.total) AS total FROM customers c
        JOIN (SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id) t
        ON c.customer_id = t.customer_id GROUP BY c.region""",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--greptime-bin", required=True)
    parser.add_argument("--corpus", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--base-port", type=int, default=14000)
    return parser.parse_args()


def run(command: list[str], *, input_text: str | None = None, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, input=input_text, text=True, capture_output=True, check=check)


class Greptime:
    def __init__(self, binary: str, data_home: Path, base_port: int, log_path: Path) -> None:
        self.binary = binary
        self.data_home = data_home
        self.base_port = base_port
        self.log_path = log_path
        self.process: subprocess.Popen[bytes] | None = None

    @property
    def dsn(self) -> str:
        return f"postgresql://localhost:{self.base_port + 3}/public"

    def start(self) -> None:
        log = self.log_path.open("ab")
        self.process = subprocess.Popen(
            [
                self.binary,
                "standalone",
                "start",
                "--http-addr",
                f"127.0.0.1:{self.base_port}",
                "--grpc-bind-addr",
                f"127.0.0.1:{self.base_port + 1}",
                "--mysql-addr",
                f"127.0.0.1:{self.base_port + 2}",
                "--postgres-addr",
                f"127.0.0.1:{self.base_port + 3}",
                "--data-home",
                str(self.data_home),
                "--log-level",
                "error",
            ],
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(f"GreptimeDB exited during startup; see {self.log_path}")
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{self.base_port}/health", timeout=1):
                    return
            except OSError:
                time.sleep(0.2)
        raise RuntimeError(f"GreptimeDB did not become healthy; see {self.log_path}")

    def stop(self) -> None:
        if self.process is None or self.process.poll() is not None:
            return
        self.process.send_signal(signal.SIGINT)
        try:
            self.process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=5)
        self.process = None

    def sql(self, sql: str, *, check: bool = True) -> subprocess.CompletedProcess[str]:
        return run(["psql", self.dsn, "-X", "-q", "-v", "ON_ERROR_STOP=1", "-c", sql], check=check)

    def rows(self, sql: str) -> list[dict[str, str]]:
        result = run(
            ["psql", self.dsn, "-X", "-q", "--csv", "-v", "ON_ERROR_STOP=1", "-c", sql]
        )
        return list(csv.DictReader(io.StringIO(result.stdout)))


def sql_literal(value: object) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def bag_digest(rows: list[dict[str, object]]) -> str:
    bag: dict[str, int] = {}
    for row in rows:
        key = canonical(row)
        bag[key] = bag.get(key, 0) + 1
    return "sha256:" + hashlib.sha256(canonical(dict(sorted(bag.items()))).encode()).hexdigest()


def normalize_timestamp(value: str) -> str:
    value = value.replace(" ", "T")
    if value.endswith("Z"):
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    elif "+" in value[10:] or "-" in value[10:]:
        parsed = datetime.fromisoformat(value)
    else:
        parsed = datetime.fromisoformat(value).replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def coerce_rows(rows: list[dict[str, str]], expected_shape: list[dict[str, object]]) -> list[dict[str, object]]:
    types: dict[str, type] = {}
    for expected in expected_shape:
        for key, value in expected.items():
            if value is not None:
                types.setdefault(key, type(value))
    result: list[dict[str, object]] = []
    for row in rows:
        converted: dict[str, object] = {}
        for key, value in row.items():
            target = types.get(key, str)
            if target is int:
                converted[key] = int(value)
            elif target is float:
                converted[key] = float(value)
            elif "time" in key or key in {"window_start", "window_end"}:
                converted[key] = normalize_timestamp(value)
            else:
                converted[key] = value
        result.append(converted)
    return sorted(result, key=canonical)


def oracle_rows(state: dict[str, dict[str, dict[str, object]]], workload_id: str, shape: list[dict[str, object]]) -> list[dict[str, object]]:
    statements = [
        "SET TimeZone='UTC'",
        "CREATE TABLE orders(order_id VARCHAR, customer_id VARCHAR, product_id VARCHAR, amount BIGINT, event_time TIMESTAMPTZ)",
        "CREATE TABLE customers(customer_id VARCHAR, region VARCHAR)",
        "CREATE TABLE products(product_id VARCHAR, category VARCHAR)",
    ]
    for relation, columns in {
        "orders": ["order_id", "customer_id", "product_id", "amount", "event_time"],
        "customers": ["customer_id", "region"],
        "products": ["product_id", "category"],
    }.items():
        values = state[relation].values()
        if values:
            tuples = ["(" + ",".join(sql_literal(row[column]) for column in columns) + ")" for row in values]
            statements.append(f"INSERT INTO {relation} VALUES " + ",".join(tuples))
    query = ORACLE_SQL[workload_id]
    statements.append(f"COPY ({query}) TO STDOUT (FORMAT CSV, HEADER true)")
    result = run(["duckdb", "-noheader", "-init", "/dev/null"], input_text=";\n".join(statements) + ";\n")
    return coerce_rows(list(csv.DictReader(io.StringIO(result.stdout))), shape)


def apply_events(engine: Greptime, state: dict[str, dict[str, dict[str, object]]], events: list[dict[str, object]]) -> None:
    keys = {"orders": "order_id", "customers": "customer_id", "products": "product_id"}
    columns = {
        "orders": ["order_id", "customer_id", "product_id", "amount", "event_time"],
        "customers": ["customer_id", "region", "ts"],
        "products": ["product_id", "category", "ts"],
    }
    for event in events:
        relation = str(event["relation"])
        before = event.get("before")
        after = event.get("after")
        if before is not None:
            time_predicate = ""
            if relation == "orders":
                time_predicate = f" AND event_time={sql_literal(before['event_time'])}"
            engine.sql(
                f"DELETE FROM {relation} WHERE {keys[relation]}={sql_literal(before[keys[relation]])}{time_predicate}"
            )
            state[relation].pop(str(before[keys[relation]]), None)
        if after is not None:
            engine_row = dict(after)
            if relation != "orders":
                engine_row["ts"] = "2026-08-09T00:00:00Z"
            values = ",".join(sql_literal(engine_row[column]) for column in columns[relation])
            engine.sql(f"INSERT INTO {relation} VALUES ({values})")
            state[relation][str(after[keys[relation]])] = dict(after)


def create_schema(engine: Greptime) -> None:
    engine.sql("""CREATE TABLE orders (
        order_id STRING, customer_id STRING, product_id STRING, amount BIGINT,
        event_time TIMESTAMP TIME INDEX, PRIMARY KEY(order_id))""")
    engine.sql("""CREATE TABLE customers (
        customer_id STRING, \"region\" STRING, ts TIMESTAMP TIME INDEX, PRIMARY KEY(customer_id))""")
    engine.sql("""CREATE TABLE products (
        product_id STRING, \"category\" STRING, ts TIMESTAMP TIME INDEX, PRIMARY KEY(product_id))""")


def create_flows(engine: Greptime) -> tuple[dict[str, dict[str, object]], dict[str, str]]:
    admitted: dict[str, dict[str, object]] = {}
    rejected: dict[str, str] = {}
    for workload_id, spec in WORKLOADS.items():
        result = engine.sql(str(spec["create"]), check=False)
        if result.returncode == 0:
            admitted[workload_id] = spec
        else:
            rejected[workload_id] = (result.stderr or result.stdout).strip()
    first = engine.sql("""CREATE FLOW customer_totals SINK TO customer_totals_sink EVAL INTERVAL '1s' AS
        SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id""", check=False)
    second = engine.sql("""CREATE FLOW region_totals SINK TO region_totals_sink EVAL INTERVAL '1s' AS
        SELECT c.\"region\", SUM(t.total) AS total FROM customers c
        JOIN customer_totals_sink t ON c.customer_id = t.customer_id GROUP BY c.\"region\"""", check=False)
    if first.returncode == 0 and second.returncode == 0:
        admitted["chained_view"] = {
            "flows": ["customer_totals", "region_totals"],
            "sink": "region_totals_sink",
            "columns": ["region", "total"],
        }
    else:
        rejected["chained_view"] = (first.stderr or second.stderr or first.stdout or second.stdout).strip()
    return admitted, rejected


def unavailable_plan() -> dict[str, object]:
    return {
        "native_logical_plan": {
            "availability": "unavailable",
            "reason_code": "greptimedb_flow_does_not_expose_stable_native_logical_identity",
        },
        "native_physical_dag": {
            "availability": "unavailable",
            "reason_code": "greptimedb_flow_does_not_expose_stable_native_physical_dag_identity",
        },
        "diagnostic_explain_digest": None,
    }


def main() -> None:
    args = parse_args()
    corpus = json.loads(Path(args.corpus).read_text())
    workload_shapes = {workload["id"]: workload["expected_final"] for workload in corpus["workloads"]}
    state: dict[str, dict[str, dict[str, object]]] = {"orders": {}, "customers": {}, "products": {}}
    phase_names = [phase["name"] for phase in corpus["phases"]]
    phase_observed: dict[str, list[dict[str, object]]] = {}
    failures: dict[str, str] = {}

    version_text = run([args.greptime_bin, "--version"]).stdout
    version = next(line.split(":", 1)[1].strip() for line in version_text.splitlines() if line.startswith("version:"))
    revision = next(line.split(":", 1)[1].strip() for line in version_text.splitlines() if line.startswith("commit:"))
    binary_sha = hashlib.sha256(Path(args.greptime_bin).read_bytes()).hexdigest()

    with tempfile.TemporaryDirectory(prefix="velorix-greptimedb-") as temp:
        temp_path = Path(temp)
        engine = Greptime(args.greptime_bin, temp_path / "data", args.base_port, temp_path / "greptime.log")
        try:
            engine.start()
            create_schema(engine)
            admitted, rejected = create_flows(engine)

            for phase in corpus["phases"]:
                phase_name = phase["name"]
                if phase_name == "checkpoint_restart":
                    engine.stop()
                    engine.start()
                else:
                    apply_events(engine, state, phase["events"])

                for workload_id, spec in admitted.items():
                    if workload_id in failures:
                        continue
                    flows = spec["flows"] if "flows" in spec else [spec["flow"]]
                    for flow in flows:
                        engine.sql(f"ADMIN FLUSH_FLOW('{flow}')")
                    columns = list(spec["columns"])
                    observed_raw = engine.rows(
                        f"SELECT {','.join('\\"' + column + '\\"' if column in {'rank'} else column for column in columns)} "
                        f"FROM {spec['sink']} ORDER BY {','.join(str(index + 1) for index in range(len(columns)))}"
                    )
                    expected = oracle_rows(state, workload_id, workload_shapes[workload_id])
                    observed = coerce_rows(observed_raw, workload_shapes[workload_id])
                    if observed != expected:
                        failures[workload_id] = (
                            f"phase {phase_name} mismatch: expected {canonical(expected)}, "
                            f"observed {canonical(observed)}"
                        )
                    else:
                        phase_observed[workload_id] = observed
        finally:
            engine.stop()

    correctness = []
    for workload in corpus["workloads"]:
        workload_id = workload["id"]
        if workload_id in rejected:
            outcome = {"status": "unsupported", "reason": rejected[workload_id]}
        elif workload_id in failures:
            outcome = {"status": "failed", "reason": failures[workload_id]}
        else:
            expected_digest = bag_digest(workload["expected_final"])
            observed_digest = bag_digest(phase_observed[workload_id])
            outcome = {
                "status": "passed",
                "expected_digest": expected_digest,
                "observed_digest": observed_digest,
                "verified_phases": phase_names,
                "plan_evidence": unavailable_plan(),
            }
        correctness.append({"workload_id": workload_id, "outcome": outcome})

    result = {
        "schema_version": 2,
        "corpus_version": "incremental-sql-corpus-v1",
        "engine": {
            "name": "greptimedb",
            "version": version,
            "source_revision": revision,
            "configuration": {
                "binary_sha256": binary_sha,
                "deployment": "standalone_local_file_storage",
                "flow_modes": "streaming_filter_and_window; eval_interval_full_query_aggregate_join_topk_chain",
                "oracle": "duckdb_batch_recomputation_per_phase",
                "runner": "greptimedb-flow-baseline-v1",
            },
            "durability_mode": "standalone_local_file_storage_restart",
            "input_semantics": "primary_key_update_lowered_to_delete_then_insert",
            "state_retention_policy": "source_history_retained; no_expire_after",
        },
        "protocol": {
            "warm_up_iterations": 0,
            "measured_iterations": 1,
            "initial_rows": 7,
            "change_events": 4,
            "change_mix": {"delete": 1, "insert": 2, "update": 1},
        },
        "correctness": correctness,
        "performance": [],
    }
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2, sort_keys=False) + "\n")
    print(output)


if __name__ == "__main__":
    main()
