#!/usr/bin/env python3
"""Run the shared incremental SQL corpus against Feldera Community Edition."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import time
from urllib.error import HTTPError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen
import uuid

from incremental_sql_risingwave import (
    RELATION_COLUMNS,
    bag_digest,
    canonical,
    coerce_rows,
    oracle_rows,
    run,
    validate_corpus_mutations,
)


PRE_RESTART_PHASES = ["initial_load", "insert", "update", "delete"]
BLOCKED_RECOVERY_PHASES = ["checkpoint_restart", "replay_tail"]
KEYS = {"orders": "order_id", "customers": "customer_id", "products": "product_id"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--image", required=True)
    parser.add_argument("--image-digest", required=True)
    parser.add_argument("--image-platform-digest", required=True)
    parser.add_argument("--runtime-platform", required=True)
    parser.add_argument("--corpus", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--port", type=int, default=18080)
    return parser.parse_args()


class FelderaCommunity:
    def __init__(self, image: str, port: int, runtime_platform: str) -> None:
        self.image = image
        self.port = port
        self.runtime_platform = runtime_platform
        self.container = f"velorix-feldera-{uuid.uuid4().hex[:12]}"
        self.started = False

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def start(self) -> None:
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
                f"127.0.0.1:{self.port}:8080",
                self.image,
            ],
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError((result.stderr or result.stdout).strip())
        self.started = True
        self.wait_ready()

    def wait_ready(self) -> None:
        deadline = time.monotonic() + 120
        last_error = ""
        while time.monotonic() < deadline:
            try:
                self.request("GET", "/healthz", expected={200})
                return
            except RuntimeError as error:
                last_error = str(error)
                time.sleep(0.25)
        logs = run(["docker", "logs", self.container], check=False)
        raise RuntimeError(
            f"Feldera did not become ready: {last_error}\n"
            + (logs.stderr or logs.stdout)[-4000:]
        )

    def stop(self) -> None:
        if not self.started:
            return
        run(["docker", "rm", "--force", self.container], check=False)
        self.started = False

    def request(
        self,
        method: str,
        path: str,
        *,
        body: object | None = None,
        raw_body: bytes | None = None,
        expected: set[int] = {200},
    ) -> tuple[bytes, dict[str, str]]:
        data = raw_body
        headers: dict[str, str] = {}
        if body is not None:
            data = json.dumps(body, separators=(",", ":")).encode()
            headers["Content-Type"] = "application/json"
        elif raw_body is not None:
            headers["Content-Type"] = "application/json"
        request = Request(self.base_url + path, data=data, headers=headers, method=method)
        try:
            with urlopen(request, timeout=30) as response:
                payload = response.read()
                status = response.status
                response_headers = dict(response.headers.items())
        except HTTPError as error:
            payload = error.read()
            status = error.code
            response_headers = dict(error.headers.items())
        if status not in expected:
            raise RuntimeError(
                f"{method} {path} returned HTTP {status}: "
                + payload.decode(errors="replace")
            )
        return payload, response_headers

    def create_pipeline(self, name: str, program_code: str) -> None:
        self.request(
            "POST",
            "/v0/pipelines",
            body={
                "name": name,
                "program_code": program_code,
                "program_config": {"profile": "optimized", "cache": True},
                "runtime_config": {
                    "workers": 1,
                    "fault_tolerance": {"model": "none"},
                },
            },
            expected={201},
        )

    def pipeline(self, name: str) -> dict[str, object]:
        payload, _ = self.request("GET", f"/v0/pipelines/{quote(name)}")
        return json.loads(payload)

    def wait_compiled(self, name: str) -> dict[str, object]:
        deadline = time.monotonic() + 900
        while time.monotonic() < deadline:
            pipeline = self.pipeline(name)
            status = str(pipeline["program_status"])
            if status == "Success" or status.endswith("Error"):
                return pipeline
            time.sleep(0.5)
        raise RuntimeError(f"Feldera compilation timed out for {name}")

    def start_pipeline(self, name: str) -> None:
        self.request(
            "POST",
            f"/v0/pipelines/{quote(name)}/start?initial=running",
            raw_body=b"",
            expected={200, 202},
        )
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            status = str(self.pipeline(name)["deployment_status"])
            if status == "Running":
                return
            if status == "Unavailable":
                raise RuntimeError(f"Feldera pipeline {name} became unavailable")
            time.sleep(0.25)
        raise RuntimeError(f"Feldera pipeline start timed out for {name}")

    def ingress(self, name: str, relation: str, changes: list[dict[str, object]]) -> None:
        query = urlencode(
            {
                "force": "false",
                "format": "json",
                "array": "true",
                "update_format": "insert_delete",
            }
        )
        payload, _ = self.request(
            "POST",
            f"/v0/pipelines/{quote(name)}/ingress/{relation.upper()}?{query}",
            raw_body=json.dumps(changes, separators=(",", ":")).encode(),
        )
        token = str(json.loads(payload)["token"])
        deadline = time.monotonic() + 60
        token_query = urlencode({"token": token})
        while time.monotonic() < deadline:
            try:
                self.request(
                    "GET",
                    f"/v0/pipelines/{quote(name)}/completion_status?{token_query}",
                )
                return
            except RuntimeError as error:
                if "HTTP 202" not in str(error):
                    raise
                time.sleep(0.1)
        raise RuntimeError(f"Feldera completion token timed out for {name}/{relation}")

    def rows(self, name: str, sql: str) -> list[dict[str, object]]:
        query = urlencode({"sql": sql, "format": "json"})
        payload, _ = self.request(
            "GET", f"/v0/pipelines/{quote(name)}/query?{query}"
        )
        text = payload.decode().strip()
        return [json.loads(line) for line in text.splitlines() if line]


def schema_sql() -> str:
    return """
CREATE TABLE orders (
  order_id VARCHAR NOT NULL PRIMARY KEY,
  customer_id VARCHAR NOT NULL,
  product_id VARCHAR NOT NULL,
  amount BIGINT NOT NULL,
  event_time TIMESTAMP NOT NULL
);
CREATE TABLE customers (
  customer_id VARCHAR NOT NULL PRIMARY KEY,
  region VARCHAR NOT NULL
);
CREATE TABLE products (
  product_id VARCHAR NOT NULL PRIMARY KEY,
  category VARCHAR NOT NULL
);
""".strip()


def program_for(workload: dict[str, object]) -> tuple[str, dict[str, object]]:
    workload_id = str(workload["id"])
    statements = list(workload["sql"])
    if workload_id == "chained_view":
        view = "region_totals"
        program = ";\n".join(str(statement).rstrip(";") for statement in statements)
        spec: dict[str, object] = {
            "view": view,
            "columns": ["region", "total"],
            "intermediate": {
                "view": "customer_totals",
                "columns": ["customer_id", "total"],
                "oracle": "customer_totals",
                "shape": [{"customer_id": "", "total": 0}],
            },
        }
    else:
        view = f"velorix_{workload_id}"
        program = f"CREATE MATERIALIZED VIEW {view} AS {statements[0].rstrip(';')}"
        spec = {
            "view": view,
            "columns": list(workload["expected_final"][0].keys()),
        }
    return schema_sql() + "\n" + program + ";\n", spec


def apply_state(
    state: dict[str, dict[str, dict[str, object]]],
    events: list[dict[str, object]],
) -> None:
    for event in events:
        relation = str(event["relation"])
        key = KEYS[relation]
        before = event.get("before")
        after = event.get("after")
        if before is not None:
            state[relation].pop(str(before[key]), None)
        if after is not None:
            state[relation][str(after[key])] = dict(after)


def ingress_events(
    engine: FelderaCommunity,
    pipeline: str,
    events: list[dict[str, object]],
) -> None:
    by_relation: dict[str, list[dict[str, object]]] = {}
    for event in events:
        relation = str(event["relation"])
        before = event.get("before")
        after = event.get("after")
        if before is not None and after is not None:
            change = {"update": after}
        elif before is not None:
            change = {"delete": {KEYS[relation]: before[KEYS[relation]]}}
        elif after is not None:
            change = {"insert": after}
        else:
            raise RuntimeError("corpus event has neither before nor after")
        by_relation.setdefault(relation, []).append(change)
    for relation, changes in by_relation.items():
        engine.ingress(pipeline, relation, changes)


def read_rows(
    engine: FelderaCommunity,
    pipeline: str,
    spec: dict[str, object],
    shape: list[dict[str, object]],
) -> list[dict[str, object]]:
    columns = ",".join(str(column) for column in spec["columns"])
    rows = engine.rows(pipeline, f"SELECT {columns} FROM {spec['view']}")
    return coerce_rows(rows, shape)


def verify_sources(
    engine: FelderaCommunity,
    pipeline: str,
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
        observed = coerce_rows(
            engine.rows(pipeline, f"SELECT {','.join(columns)} FROM {relation}"),
            expected,
        )
        if observed != expected:
            raise RuntimeError(
                f"phase {phase_name} source mismatch for {pipeline}/{relation}: "
                f"expected {canonical(expected)}, observed {canonical(observed)}"
            )
        evidence[relation] = {
            "row_count": len(observed),
            "multiset_digest": bag_digest(observed),
        }
    return evidence


def semantic_difference(expected_digest: str) -> dict[str, object]:
    return {
        "status": "semantic_difference",
        "reason_code": "durable_restart_unavailable_in_edition",
        "reason": (
            "Feldera Community Edition supports incremental pipelines but not "
            "Enterprise checkpoint-based fault tolerance, so fresh-process "
            "recovery and post-restart replay cannot be executed"
        ),
        "scope": "edition_wide",
        "expected_digest": expected_digest,
        "verified_phases": PRE_RESTART_PHASES,
        "blocked_phases": BLOCKED_RECOVERY_PHASES,
        "recovery_parity_claimed": False,
        "performance_comparable": False,
    }


def main() -> None:
    args = parse_args()
    corpus_bytes = Path(args.corpus).read_bytes()
    corpus = json.loads(corpus_bytes)
    validate_corpus_mutations(corpus)
    phase_names = [str(phase["name"]) for phase in corpus["phases"]]
    if phase_names != PRE_RESTART_PHASES + BLOCKED_RECOVERY_PHASES:
        raise RuntimeError("Feldera runner requires the canonical six-phase corpus")

    engine = FelderaCommunity(args.image, args.port, args.runtime_platform)
    admitted: dict[str, tuple[str, dict[str, object]]] = {}
    rejected: dict[str, str] = {}
    failures: dict[str, str] = {}
    phase_observed: dict[str, list[dict[str, object]]] = {}
    phase_evidence: list[dict[str, object]] = []
    platform_version = "unknown"
    state: dict[str, dict[str, dict[str, object]]] = {
        "orders": {},
        "customers": {},
        "products": {},
    }
    shapes = {
        str(workload["id"]): list(workload["expected_final"])
        for workload in corpus["workloads"]
    }

    try:
        engine.start()
        pending: dict[str, tuple[str, dict[str, object]]] = {}
        for workload in corpus["workloads"]:
            workload_id = str(workload["id"])
            pipeline = f"velorix-{workload_id.replace('_', '-')}"
            program, spec = program_for(workload)
            engine.create_pipeline(pipeline, program)
            pending[workload_id] = (pipeline, spec)

        for workload_id, (pipeline, spec) in pending.items():
            compiled = engine.wait_compiled(pipeline)
            platform_version = str(compiled.get("platform_version", platform_version))
            if compiled["program_status"] == "Success":
                engine.start_pipeline(pipeline)
                admitted[workload_id] = (pipeline, spec)
            else:
                rejected[workload_id] = canonical(compiled.get("program_error"))

        for phase in corpus["phases"]:
            phase_name = str(phase["name"])
            if phase_name in BLOCKED_RECOVERY_PHASES:
                phase_evidence.append(
                    {
                        "phase": phase_name,
                        "status": "semantic_difference",
                        "reason_code": "durable_restart_unavailable_in_edition",
                        "change_ids": [event["change_id"] for event in phase["events"]],
                    }
                )
                continue

            for pipeline, _ in admitted.values():
                ingress_events(engine, pipeline, phase["events"])
            apply_state(state, phase["events"])
            current: dict[str, object] = {
                "phase": phase_name,
                "status": "executed",
                "change_ids": [event["change_id"] for event in phase["events"]],
                "commit_boundary": "ingress_completion_token_then_read_after_write_query",
                "fresh_process_verified": False,
                "pipelines": {},
            }
            for workload_id, (pipeline, spec) in admitted.items():
                if workload_id in failures:
                    continue
                source_evidence = verify_sources(
                    engine, pipeline, state, phase_name
                )
                expected = oracle_rows(state, workload_id, shapes[workload_id])
                view_evidence: dict[str, dict[str, object]] = {}
                intermediate = spec.get("intermediate")
                if isinstance(intermediate, dict):
                    intermediate_shape = list(intermediate["shape"])
                    intermediate_expected = oracle_rows(
                        state, str(intermediate["oracle"]), intermediate_shape
                    )
                    intermediate_observed = read_rows(
                        engine, pipeline, intermediate, intermediate_shape
                    )
                    if intermediate_observed != intermediate_expected:
                        failures[workload_id] = (
                            f"phase {phase_name} intermediate mismatch: expected "
                            f"{canonical(intermediate_expected)}, observed "
                            f"{canonical(intermediate_observed)}"
                        )
                        continue
                    view_evidence[str(intermediate["view"])] = {
                        "row_count": len(intermediate_observed),
                        "multiset_digest": bag_digest(intermediate_observed),
                    }
                observed = read_rows(
                    engine, pipeline, spec, shapes[workload_id]
                )
                if observed != expected:
                    failures[workload_id] = (
                        f"phase {phase_name} mismatch: expected {canonical(expected)}, "
                        f"observed {canonical(observed)}"
                    )
                    continue
                phase_observed[workload_id] = observed
                view_evidence[str(spec["view"])] = {
                    "row_count": len(observed),
                    "multiset_digest": bag_digest(observed),
                }
                current["pipelines"][workload_id] = {
                    "sources": source_evidence,
                    "views": view_evidence,
                }
            phase_evidence.append(current)
    finally:
        engine.stop()

    correctness = []
    for workload in corpus["workloads"]:
        workload_id = str(workload["id"])
        if workload_id in rejected:
            outcome = {"status": "unsupported", "reason": rejected[workload_id]}
        elif workload_id in failures:
            outcome = {"status": "failed", "reason": failures[workload_id]}
        else:
            if workload_id not in phase_observed:
                raise RuntimeError(f"admitted workload {workload_id} has no evidence")
            outcome = semantic_difference(bag_digest(workload["expected_final"]))
        correctness.append({"workload_id": workload_id, "outcome": outcome})

    result = {
        "schema_version": 2,
        "corpus_version": "incremental-sql-corpus-v1",
        "engine": {
            "name": "feldera",
            "version": platform_version,
            "source_revision": f"official-image:{args.image_digest}",
            "configuration": {
                "corpus_sha256": hashlib.sha256(corpus_bytes).hexdigest(),
                "deployment": "official_feldera_community_single_container",
                "edition": "community",
                "fault_tolerance": "enterprise_only_unavailable_in_edition",
                "image_index_digest": args.image_digest,
                "image_platform_digest": args.image_platform_digest,
                "inspection_query": "nonincremental_datafusion_read_of_materialized_state",
                "oracle": "duckdb_batch_recomputation_per_executed_phase",
                "performance_evidence_published": "false",
                "phase_evidence": canonical(phase_evidence),
                "restart_protocol": "not_executed_semantic_difference",
                "runner": "feldera-community-baseline-v1",
                "runtime_platform": args.runtime_platform,
                "source_verification": "all_relations_exact_snapshot_every_executed_phase_per_pipeline",
            },
            "durability_mode": "community_without_checkpoint_fault_tolerance",
            "input_semantics": "primary_keyed_insert_update_delete_ingress",
            "state_retention_policy": "process_lifetime_only_for_this_evidence_run",
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
    output.write_text(json.dumps(result, indent=2) + "\n")
    print(output)


if __name__ == "__main__":
    main()
