#!/usr/bin/env python3
"""Run the shared incremental SQL corpus against RisingWave single-node."""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import re
import subprocess
import tempfile
import time
import uuid


WORKLOADS = {
    "filter_project": {
        "view": "fp_mv",
        "columns": ["order_id", "doubled"],
        "create": """CREATE MATERIALIZED VIEW fp_mv AS
            SELECT order_id, amount * 2 AS doubled FROM orders WHERE amount >= 50""",
    },
    "aggregate": {
        "view": "aggregate_mv",
        "columns": ["customer_id", "total", "order_count", "minimum", "maximum", "average"],
        "create": """CREATE MATERIALIZED VIEW aggregate_mv AS
            SELECT customer_id, SUM(amount) AS total, COUNT(*) AS order_count,
                   MIN(amount) AS minimum, MAX(amount) AS maximum, AVG(amount) AS average
            FROM orders GROUP BY customer_id""",
    },
    "distinct_aggregate": {
        "view": "distinct_mv",
        "columns": ["customer_id", "product_count"],
        "create": """CREATE MATERIALIZED VIEW distinct_mv AS
            SELECT customer_id, COUNT(DISTINCT product_id) AS product_count
            FROM orders GROUP BY customer_id""",
    },
    "inner_join": {
        "view": "inner_join_mv",
        "columns": ["region", "total"],
        "create": """CREATE MATERIALIZED VIEW inner_join_mv AS
            SELECT c.region, SUM(o.amount) AS total
            FROM customers c JOIN orders o ON c.customer_id = o.customer_id
            GROUP BY c.region""",
    },
    "left_join": {
        "view": "left_join_mv",
        "columns": ["customer_id", "order_count"],
        "create": """CREATE MATERIALIZED VIEW left_join_mv AS
            SELECT c.customer_id, COUNT(o.order_id) AS order_count
            FROM customers c LEFT JOIN orders o ON c.customer_id = o.customer_id
            GROUP BY c.customer_id""",
    },
    "top_k": {
        "view": "topk_mv",
        "columns": ["customer_id", "total"],
        "create": """CREATE MATERIALIZED VIEW topk_mv AS
            SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id
            ORDER BY total DESC, customer_id ASC LIMIT 1""",
    },
    "fixed_window": {
        "view": "window_mv",
        "columns": ["customer_id", "window_start", "total"],
        "create": """CREATE MATERIALIZED VIEW window_mv AS
            SELECT customer_id, window_start, SUM(amount) AS total
            FROM TUMBLE(orders, event_time, INTERVAL '2 MINUTES')
            GROUP BY customer_id, window_start""",
    },
    "ranking": {
        "view": "ranking_mv",
        "columns": ["customer_id", "order_id", "rank"],
        "create": """CREATE MATERIALIZED VIEW ranking_mv AS
            SELECT customer_id, order_id,
                   ROW_NUMBER() OVER (
                       PARTITION BY customer_id ORDER BY amount DESC, order_id ASC
                   ) AS rank
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
    "customer_totals": """SELECT customer_id, SUM(amount) AS total
        FROM orders GROUP BY customer_id""",
}

RELATION_COLUMNS = {
    "orders": ["order_id", "customer_id", "product_id", "amount", "event_time"],
    "customers": ["customer_id", "region"],
    "products": ["product_id", "category"],
}

SESSION_SETUP = """SET TIME ZONE 'UTC';
SET RW_IMPLICIT_FLUSH = false;
SET BACKGROUND_DDL = false;
SET STREAMING_PARALLELISM = 1;"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--risingwave-bin", required=True)
    parser.add_argument("--runtime-image", required=True)
    parser.add_argument("--runtime-image-digest", required=True)
    parser.add_argument("--runtime-image-platform-digest", required=True)
    parser.add_argument("--runtime-platform", required=True)
    parser.add_argument("--package-sha256", required=True)
    parser.add_argument("--corpus", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--port", type=int, default=14566)
    return parser.parse_args()


def run(
    command: list[str], *, input_text: str | None = None, check: bool = True
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command, input=input_text, text=True, capture_output=True, check=check
    )


class RisingWave:
    def __init__(
        self,
        binary: Path,
        runtime_image: str,
        data_dir: Path,
        port: int,
        runtime_platform: str,
    ) -> None:
        self.binary = binary.resolve()
        self.runtime_image = runtime_image
        self.data_dir = data_dir.resolve()
        self.port = port
        self.runtime_platform = runtime_platform
        self.container = f"velorix-risingwave-{uuid.uuid4().hex[:12]}"
        self.started = False

    @property
    def dsn(self) -> str:
        return f"postgresql://root@127.0.0.1:{self.port}/dev"

    def start(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        result = run(
            [
                "docker",
                "run",
                "--detach",
                "--name",
                self.container,
                "--platform",
                self.runtime_platform,
                "--publish",
                f"127.0.0.1:{self.port}:4566",
                "--mount",
                f"type=bind,source={self.binary.parent},target=/opt/velorix-tools,readonly",
                "--mount",
                f"type=bind,source={self.data_dir},target=/var/lib/risingwave",
                "--env",
                "ENABLE_TELEMETRY=false",
                self.runtime_image,
                f"/opt/velorix-tools/{self.binary.name}",
                "single_node",
                "--store-directory",
                "/var/lib/risingwave",
                "--listen-addr",
                "0.0.0.0:4566",
                "--total-memory-bytes",
                str(4 * 1024 * 1024 * 1024),
                "--parallelism",
                "1",
            ],
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError((result.stderr or result.stdout).strip())
        self.started = True
        self.wait_ready()

    def wait_ready(self) -> None:
        deadline = time.monotonic() + 90
        last_error = ""
        while time.monotonic() < deadline:
            result = run(
                [
                    "psql",
                    self.dsn,
                    "-X",
                    "-q",
                    "-v",
                    "ON_ERROR_STOP=1",
                    "-c",
                    "SELECT 1",
                ],
                check=False,
            )
            if result.returncode == 0:
                return
            last_error = (result.stderr or result.stdout).strip()
            time.sleep(0.25)
        logs = run(["docker", "logs", self.container], check=False)
        raise RuntimeError(
            f"RisingWave did not become ready: {last_error}\n"
            + (logs.stderr or logs.stdout)[-4000:]
        )

    def crash_restart(self) -> None:
        previous_container = self.container
        result = run(
            ["docker", "kill", "--signal", "KILL", self.container],
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError((result.stderr or result.stdout).strip())
        result = run(["docker", "rm", previous_container], check=False)
        if result.returncode != 0:
            raise RuntimeError((result.stderr or result.stdout).strip())
        self.started = False
        self.container = f"velorix-risingwave-{uuid.uuid4().hex[:12]}"
        self.start()
        if self.container == previous_container:
            raise RuntimeError("crash recovery reused the previous container identity")
        self.wait_recovery_ready()

    def wait_recovery_ready(self) -> None:
        deadline = time.monotonic() + 90
        last_rows: list[dict[str, str]] = []
        while time.monotonic() < deadline:
            try:
                last_rows = self.rows("SELECT * FROM rw_catalog.rw_recovery_info")
            except subprocess.CalledProcessError:
                time.sleep(0.25)
                continue
            values = [
                str(value).lower()
                for row in last_rows
                for value in row.values()
                if value is not None
            ]
            if not any("recovering" in value for value in values) and any(
                "running" in value for value in values
            ):
                return
            time.sleep(0.25)
        raise RuntimeError(
            "RisingWave recovery did not reach global running state: "
            + canonical(last_rows)
        )

    def stop(self) -> None:
        if not self.started:
            return
        run(["docker", "rm", "--force", self.container], check=False)
        self.started = False

    def sql(self, sql: str, *, check: bool = True) -> subprocess.CompletedProcess[str]:
        return run(
            [
                "psql",
                self.dsn,
                "-X",
                "-v",
                "ON_ERROR_STOP=1",
                "-c",
                SESSION_SETUP + "\n" + sql,
            ],
            check=check,
        )

    def rows(self, sql: str) -> list[dict[str, str]]:
        result = run(
            [
                "psql",
                self.dsn,
                "-X",
                "-q",
                "--csv",
                "-v",
                "ON_ERROR_STOP=1",
                "-c",
                SESSION_SETUP + "\n" + sql,
            ]
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


def coerce_rows(
    rows: list[dict[str, str]], expected_shape: list[dict[str, object]]
) -> list[dict[str, object]]:
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


def oracle_rows(
    state: dict[str, dict[str, dict[str, object]]],
    workload_id: str,
    shape: list[dict[str, object]],
) -> list[dict[str, object]]:
    statements = [
        "SET TimeZone='UTC'",
        "CREATE TABLE orders(order_id VARCHAR, customer_id VARCHAR, product_id VARCHAR, amount BIGINT, event_time TIMESTAMPTZ)",
        "CREATE TABLE customers(customer_id VARCHAR, region VARCHAR)",
        "CREATE TABLE products(product_id VARCHAR, category VARCHAR)",
    ]
    for relation, columns in RELATION_COLUMNS.items():
        values = state[relation].values()
        if values:
            tuples = [
                "(" + ",".join(sql_literal(row[column]) for column in columns) + ")"
                for row in values
            ]
            statements.append(f"INSERT INTO {relation} VALUES " + ",".join(tuples))
    statements.append(
        f"COPY ({ORACLE_SQL[workload_id]}) TO STDOUT (FORMAT CSV, HEADER true)"
    )
    result = run(
        ["duckdb", "-noheader", "-init", "/dev/null"],
        input_text=";\n".join(statements) + ";\n",
    )
    return coerce_rows(list(csv.DictReader(io.StringIO(result.stdout))), shape)


def apply_events(
    engine: RisingWave,
    state: dict[str, dict[str, dict[str, object]]],
    events: list[dict[str, object]],
) -> None:
    keys = {"orders": "order_id", "customers": "customer_id", "products": "product_id"}
    for event in events:
        relation = str(event["relation"])
        before = event.get("before")
        after = event.get("after")
        key = keys[relation]
        if before is not None and after is not None:
            if str(before[key]) != str(after[key]):
                raise RuntimeError(f"native UPDATE cannot change primary key {key}")
            assignments = ",".join(
                f"{column}={sql_literal(after[column])}"
                for column in RELATION_COLUMNS[relation]
            )
            result = engine.sql(
                f"UPDATE {relation} SET {assignments} WHERE {key}={sql_literal(before[key])}"
            )
            require_affected_rows(result, "UPDATE", 1)
            state[relation].pop(str(before[key]), None)
            state[relation][str(after[key])] = dict(after)
        elif before is not None:
            result = engine.sql(
                f"DELETE FROM {relation} WHERE {key}={sql_literal(before[key])}"
            )
            require_affected_rows(result, "DELETE", 1)
            state[relation].pop(str(before[key]), None)
        elif after is not None:
            values = ",".join(
                sql_literal(after[column]) for column in RELATION_COLUMNS[relation]
            )
            result = engine.sql(f"INSERT INTO {relation} VALUES ({values})")
            require_affected_rows(result, "INSERT", 1)
            state[relation][str(after[key])] = dict(after)


def require_affected_rows(
    result: subprocess.CompletedProcess[str], operation: str, expected: int
) -> None:
    lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    pattern = (
        rf"INSERT (?:0 )?{expected}"
        if operation == "INSERT"
        else rf"{operation} {expected}"
    )
    if not lines or re.fullmatch(pattern, lines[-1]) is None:
        raise RuntimeError(
            f"{operation} did not acknowledge exactly {expected} affected row: "
            + canonical(lines)
        )


def create_schema(engine: RisingWave) -> None:
    engine.sql("""CREATE TABLE orders (
        order_id VARCHAR PRIMARY KEY, customer_id VARCHAR NOT NULL,
        product_id VARCHAR NOT NULL, amount BIGINT NOT NULL,
        event_time TIMESTAMPTZ NOT NULL)""")
    engine.sql("""CREATE TABLE customers (
        customer_id VARCHAR PRIMARY KEY, region VARCHAR NOT NULL)""")
    engine.sql("""CREATE TABLE products (
        product_id VARCHAR PRIMARY KEY, category VARCHAR NOT NULL)""")


def create_views(
    engine: RisingWave,
) -> tuple[dict[str, dict[str, object]], dict[str, str]]:
    admitted: dict[str, dict[str, object]] = {}
    rejected: dict[str, str] = {}
    for workload_id, spec in WORKLOADS.items():
        result = engine.sql(str(spec["create"]), check=False)
        if result.returncode == 0:
            admitted[workload_id] = spec
        else:
            rejected[workload_id] = (result.stderr or result.stdout).strip()

    first = engine.sql("""CREATE MATERIALIZED VIEW customer_totals AS
        SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id""", check=False)
    if first.returncode == 0:
        engine.sql("FLUSH")
        stage_one = engine.rows(
            "SELECT customer_id, total FROM customer_totals ORDER BY customer_id"
        )
        if stage_one:
            raise RuntimeError("customer_totals was not empty before initial load")
    second = engine.sql("""CREATE MATERIALIZED VIEW region_totals AS
        SELECT c.region, SUM(t.total) AS total FROM customers c
        JOIN customer_totals t ON c.customer_id = t.customer_id GROUP BY c.region""", check=False)
    if first.returncode == 0 and second.returncode == 0:
        engine.sql("FLUSH")
        if engine.rows("SELECT region, total FROM region_totals ORDER BY region"):
            raise RuntimeError("region_totals was not empty before initial load")
        admitted["chained_view"] = {
            "view": "region_totals",
            "columns": ["region", "total"],
            "intermediate": {
                "view": "customer_totals",
                "columns": ["customer_id", "total"],
                "oracle": "customer_totals",
                "shape": [{"customer_id": "", "total": 0}],
            },
        }
    else:
        rejected["chained_view"] = (
            first.stderr or second.stderr or first.stdout or second.stdout
        ).strip()
    return admitted, rejected


def unavailable_plan() -> dict[str, object]:
    return {
        "native_logical_plan": {
            "availability": "unavailable",
            "reason_code": "risingwave_does_not_expose_stable_native_logical_identity",
        },
        "native_physical_dag": {
            "availability": "unavailable",
            "reason_code": "risingwave_explain_is_diagnostic_not_a_stable_physical_dag_identity",
        },
        "diagnostic_explain_digest": None,
    }


def read_observed(
    engine: RisingWave,
    spec: dict[str, object],
    shape: list[dict[str, object]],
) -> list[dict[str, object]]:
    columns = list(spec["columns"])
    selected = ",".join(f'"{column}"' if column == "rank" else str(column) for column in columns)
    order = ",".join(str(index + 1) for index in range(len(columns)))
    return coerce_rows(
        engine.rows(f"SELECT {selected} FROM {spec['view']} ORDER BY {order}"),
        shape,
    )


def verify_source_state(
    engine: RisingWave,
    state: dict[str, dict[str, dict[str, object]]],
    phase_name: str,
) -> dict[str, dict[str, object]]:
    evidence: dict[str, dict[str, object]] = {}
    for relation, columns in RELATION_COLUMNS.items():
        expected = sorted(
            [
                {column: row[column] for column in columns}
                for row in state[relation].values()
            ],
            key=canonical,
        )
        selected = ",".join(columns)
        order = ",".join(str(index + 1) for index in range(len(columns)))
        observed = coerce_rows(
            engine.rows(f"SELECT {selected} FROM {relation} ORDER BY {order}"),
            expected,
        )
        if observed != expected:
            raise RuntimeError(
                f"phase {phase_name} source mismatch for {relation}: "
                f"expected {canonical(expected)}, observed {canonical(observed)}"
            )
        evidence[relation] = {
            "row_count": len(observed),
            "multiset_digest": bag_digest(observed),
        }
    return evidence


def verify_workload_expected(
    engine: RisingWave,
    state: dict[str, dict[str, dict[str, object]]],
    workload_id: str,
    spec: dict[str, object],
    shape: list[dict[str, object]],
    expected: list[dict[str, object]],
) -> tuple[list[dict[str, object]], str | None, dict[str, dict[str, object]]]:
    evidence: dict[str, dict[str, object]] = {}
    intermediate = spec.get("intermediate")
    if isinstance(intermediate, dict):
        intermediate_shape = list(intermediate["shape"])
        intermediate_expected = oracle_rows(
            state, str(intermediate["oracle"]), intermediate_shape
        )
        intermediate_observed = read_observed(engine, intermediate, intermediate_shape)
        if intermediate_observed != intermediate_expected:
            return (
                [],
                f"{workload_id} intermediate mismatch: expected "
                f"{canonical(intermediate_expected)}, observed "
                f"{canonical(intermediate_observed)}",
                evidence,
            )
        evidence[str(intermediate["view"])] = {
            "row_count": len(intermediate_observed),
            "multiset_digest": bag_digest(intermediate_observed),
        }
    observed = read_observed(engine, spec, shape)
    evidence[str(spec["view"])] = {
        "row_count": len(observed),
        "multiset_digest": bag_digest(observed),
    }
    return observed, None, evidence


def validate_corpus_mutations(corpus: dict[str, object]) -> None:
    state = {relation: set() for relation in RELATION_COLUMNS}
    keys = {"orders": "order_id", "customers": "customer_id", "products": "product_id"}
    for phase in corpus["phases"]:
        for event in phase["events"]:
            relation = str(event["relation"])
            key = keys[relation]
            before = event.get("before")
            after = event.get("after")
            if before is None and after is not None:
                identity = str(after[key])
                if identity in state[relation]:
                    raise RuntimeError(f"corpus inserts duplicate {relation} key {identity}")
                state[relation].add(identity)
            elif before is not None and after is not None:
                identity = str(before[key])
                if identity not in state[relation] or identity != str(after[key]):
                    raise RuntimeError(f"corpus has invalid native UPDATE for {relation}")
            elif before is not None:
                identity = str(before[key])
                if identity not in state[relation]:
                    raise RuntimeError(f"corpus deletes missing {relation} key {identity}")
                state[relation].remove(identity)


def verify_window_contract(engine: RisingWave) -> dict[str, object]:
    engine.sql("""CREATE TABLE window_contract_probe (
        probe_id VARCHAR PRIMARY KEY, event_time TIMESTAMPTZ NOT NULL)""")
    engine.sql("""CREATE MATERIALIZED VIEW window_contract_probe_mv AS
        SELECT probe_id, window_start, window_end
        FROM TUMBLE(window_contract_probe, event_time, INTERVAL '2 MINUTES')""")
    for probe_id, event_time in [
        ("before", "2026-08-09T00:01:59.999999Z"),
        ("boundary", "2026-08-09T00:02:00Z"),
        ("after", "2026-08-09T00:02:00.000001Z"),
    ]:
        result = engine.sql(
            "INSERT INTO window_contract_probe VALUES "
            f"({sql_literal(probe_id)}, {sql_literal(event_time)})"
        )
        require_affected_rows(result, "INSERT", 1)
    engine.sql("FLUSH")
    shape = [{"probe_id": "", "window_start": "", "window_end": ""}]
    observed = coerce_rows(
        engine.rows("""SELECT probe_id, window_start, window_end
            FROM window_contract_probe_mv ORDER BY probe_id"""),
        shape,
    )
    expected = sorted(
        [
            {
                "probe_id": "before",
                "window_start": "2026-08-09T00:00:00Z",
                "window_end": "2026-08-09T00:02:00Z",
            },
            {
                "probe_id": "boundary",
                "window_start": "2026-08-09T00:02:00Z",
                "window_end": "2026-08-09T00:04:00Z",
            },
            {
                "probe_id": "after",
                "window_start": "2026-08-09T00:02:00Z",
                "window_end": "2026-08-09T00:04:00Z",
            },
        ],
        key=canonical,
    )
    if observed != expected:
        raise RuntimeError(
            "RisingWave TUMBLE does not match the UTC [start,end) contract: "
            f"expected {canonical(expected)}, observed {canonical(observed)}"
        )
    engine.sql("DROP MATERIALIZED VIEW window_contract_probe_mv")
    engine.sql("DROP TABLE window_contract_probe")
    engine.sql("FLUSH")
    return {"row_count": len(observed), "multiset_digest": bag_digest(observed)}


def main() -> None:
    args = parse_args()
    corpus_bytes = Path(args.corpus).read_bytes()
    corpus = json.loads(corpus_bytes)
    validate_corpus_mutations(corpus)
    workload_shapes = {
        workload["id"]: workload["expected_final"] for workload in corpus["workloads"]
    }
    state: dict[str, dict[str, dict[str, object]]] = {
        "orders": {},
        "customers": {},
        "products": {},
    }
    phase_names = [phase["name"] for phase in corpus["phases"]]
    phase_observed: dict[str, list[dict[str, object]]] = {}
    failures: dict[str, str] = {}
    phase_evidence: list[dict[str, object]] = []

    binary = Path(args.risingwave_bin)
    binary_sha = hashlib.sha256(binary.read_bytes()).hexdigest()
    with tempfile.TemporaryDirectory(prefix="velorix-risingwave-") as temp:
        temp_path = Path(temp)
        engine = RisingWave(
            binary,
            args.runtime_image,
            temp_path / "data",
            args.port,
            args.runtime_platform,
        )
        try:
            engine.start()
            version = engine.rows("SELECT version() AS version")[0]["version"]
            create_schema(engine)
            window_contract_evidence = verify_window_contract(engine)
            admitted, rejected = create_views(engine)

            for phase in corpus["phases"]:
                phase_name = phase["name"]
                if phase_name == "checkpoint_restart":
                    engine.crash_restart()
                else:
                    apply_events(engine, state, phase["events"])
                    engine.sql("FLUSH")

                current_phase_evidence: dict[str, object] = {
                    "phase": phase_name,
                    "change_ids": [event["change_id"] for event in phase["events"]],
                    "commit_boundary": (
                        "prior_flush_then_sigkill_fresh_container_recovery"
                        if phase_name == "checkpoint_restart"
                        else "explicit_flush_ack"
                    ),
                    "fresh_process_verified": phase_name == "checkpoint_restart",
                    "sources": verify_source_state(engine, state, phase_name),
                    "views": {},
                }

                for workload_id, spec in admitted.items():
                    if workload_id in failures:
                        continue
                    expected = oracle_rows(
                        state, workload_id, workload_shapes[workload_id]
                    )
                    observed, intermediate_error, view_evidence = verify_workload_expected(
                        engine,
                        state,
                        workload_id,
                        spec,
                        workload_shapes[workload_id],
                        expected,
                    )
                    if intermediate_error is not None:
                        failures[workload_id] = (
                            f"phase {phase_name} {intermediate_error}"
                        )
                    elif observed != expected:
                        failures[workload_id] = (
                            f"phase {phase_name} mismatch: expected {canonical(expected)}, "
                            f"observed {canonical(observed)}"
                        )
                    else:
                        phase_observed[workload_id] = observed
                        current_phase_evidence["views"][workload_id] = view_evidence
                phase_evidence.append(current_phase_evidence)
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
            "name": "risingwave",
            "version": version,
            "source_revision": "391c3a16ef26d0cd86d1236c9b7c122a9a27fb1e",
            "configuration": {
                "binary_sha256": binary_sha,
                "container_runtime": "docker",
                "corpus_sha256": hashlib.sha256(corpus_bytes).hexdigest(),
                "deployment": "official_release_binary_in_pinned_ubuntu_single_node",
                "dml_acknowledgement": "exact_affected_row_command_tag_then_explicit_flush_ack",
                "oracle": "duckdb_batch_recomputation_per_phase",
                "package_sha256": args.package_sha256,
                "phase_evidence": canonical(phase_evidence),
                "polling": "startup_and_recovery_readiness_only_no_result_polling",
                "restart_protocol": "container_pid1_sigkill_delete_fresh_container_same_persistent_store",
                "runtime_image_index_digest": args.runtime_image_digest,
                "runtime_image_platform_digest": args.runtime_image_platform_digest,
                "runtime_platform": args.runtime_platform,
                "runner": "risingwave-single-node-baseline-v1",
                "source_verification": "all_registered_relations_exact_snapshot_every_phase",
                "streaming_parallelism": "1",
                "time_window_equivalence": "utc_epoch_aligned_non_late_two_minute_tumble",
                "window_contract_evidence": canonical(window_contract_evidence),
                "workload_sql_sha256": hashlib.sha256(
                    canonical({"engine": WORKLOADS, "oracle": ORACLE_SQL}).encode()
                ).hexdigest(),
                "view_chain_verification": "customer_totals_then_region_totals_every_phase",
            },
            "durability_mode": "single_node_sqlite_meta_and_local_hummock_file_store_sigkill_recovery",
            "input_semantics": "primary_key_native_insert_update_delete",
            "state_retention_policy": "source_tables_and_materialized_state_retained",
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
