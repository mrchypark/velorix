#!/usr/bin/env python3
"""Run the shared incremental SQL corpus against Materialize Emulator."""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
from pathlib import Path
import tempfile
import time
import uuid

from incremental_sql_risingwave import (
    WORKLOADS,
    apply_events,
    bag_digest,
    canonical,
    oracle_rows,
    run,
    validate_corpus_mutations,
    verify_source_state,
    verify_workload_expected,
)


PRE_RESTART_PHASES = ["initial_load", "insert", "update", "delete"]
BLOCKED_RECOVERY_PHASES = ["checkpoint_restart", "replay_tail"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--image", required=True)
    parser.add_argument("--image-digest", required=True)
    parser.add_argument("--image-platform-digest", required=True)
    parser.add_argument("--runtime-platform", required=True)
    parser.add_argument("--corpus", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--port", type=int, default=16875)
    return parser.parse_args()


class MaterializeEmulator:
    def __init__(self, image: str, port: int, runtime_platform: str) -> None:
        self.image = image
        self.port = port
        self.runtime_platform = runtime_platform
        self.container = f"velorix-materialize-{uuid.uuid4().hex[:12]}"
        self.started = False

    @property
    def dsn(self) -> str:
        return f"postgresql://materialize@127.0.0.1:{self.port}/materialize"

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
                f"127.0.0.1:{self.port}:6875",
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
            f"Materialize Emulator did not become ready: {last_error}\n"
            + (logs.stderr or logs.stdout)[-4000:]
        )

    def stop(self) -> None:
        if not self.started:
            return
        run(["docker", "rm", "--force", self.container], check=False)
        self.started = False

    def sql(self, sql: str, *, check: bool = True):
        return run(
            [
                "psql",
                self.dsn,
                "-X",
                "-q",
                "-v",
                "ON_ERROR_STOP=1",
                "-c",
                sql,
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
                sql,
            ]
        )
        return list(csv.DictReader(io.StringIO(result.stdout)))


def create_schema(engine: MaterializeEmulator) -> None:
    # Materialize read-write tables do not provide primary-key or unique
    # constraints. The corpus has stable identities and the harness proves the
    # complete source snapshots after every committed phase.
    engine.sql(
        """CREATE TABLE orders (
        order_id TEXT NOT NULL, customer_id TEXT NOT NULL,
        product_id TEXT NOT NULL, amount BIGINT NOT NULL,
        event_time TIMESTAMPTZ NOT NULL)"""
    )
    engine.sql(
        """CREATE TABLE customers (
        customer_id TEXT NOT NULL, region TEXT NOT NULL)"""
    )
    engine.sql(
        """CREATE TABLE products (
        product_id TEXT NOT NULL, category TEXT NOT NULL)"""
    )


def create_views(
    engine: MaterializeEmulator,
) -> tuple[dict[str, dict[str, object]], dict[str, str]]:
    admitted: dict[str, dict[str, object]] = {}
    rejected: dict[str, str] = {}
    for workload_id, spec in WORKLOADS.items():
        result = engine.sql(str(spec["create"]), check=False)
        if result.returncode == 0:
            admitted[workload_id] = spec
        else:
            rejected[workload_id] = (result.stderr or result.stdout).strip()

    first = engine.sql(
        """CREATE MATERIALIZED VIEW customer_totals AS
        SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id""",
        check=False,
    )
    second = engine.sql(
        """CREATE MATERIALIZED VIEW region_totals AS
        SELECT c.region, SUM(t.total) AS total FROM customers c
        JOIN customer_totals t ON c.customer_id = t.customer_id GROUP BY c.region""",
        check=False,
    )
    if first.returncode == 0 and second.returncode == 0:
        if engine.rows("SELECT customer_id, total FROM customer_totals"):
            raise RuntimeError("customer_totals was not empty before initial load")
        if engine.rows("SELECT region, total FROM region_totals"):
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


def phase_names(corpus: dict[str, object]) -> list[str]:
    return [str(phase["name"]) for phase in corpus["phases"]]


def validate_phase_contract(corpus: dict[str, object]) -> None:
    observed = phase_names(corpus)
    expected = PRE_RESTART_PHASES + BLOCKED_RECOVERY_PHASES
    if observed != expected:
        raise RuntimeError(
            "Materialize baseline requires the canonical six-phase order: "
            f"expected {canonical(expected)}, observed {canonical(observed)}"
        )


def semantic_difference(expected_digest: str) -> dict[str, object]:
    return {
        "status": "semantic_difference",
        "reason_code": "durable_restart_unavailable_in_edition",
        "reason": (
            "Materialize Emulator provides neither data persistence nor fault "
            "tolerance, so the required fresh-process durable restart and "
            "post-restart replay phases cannot be executed in this edition"
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
    validate_phase_contract(corpus)
    workload_shapes = {
        workload["id"]: workload["expected_final"] for workload in corpus["workloads"]
    }
    state: dict[str, dict[str, dict[str, object]]] = {
        "orders": {},
        "customers": {},
        "products": {},
    }
    phase_observed: dict[str, list[dict[str, object]]] = {}
    failures: dict[str, str] = {}
    phase_evidence: list[dict[str, object]] = []

    with tempfile.TemporaryDirectory(prefix="velorix-materialize-"):
        engine = MaterializeEmulator(args.image, args.port, args.runtime_platform)
        try:
            engine.start()
            version = engine.rows("SELECT mz_version() AS version")[0]["version"]
            create_schema(engine)
            admitted, rejected = create_views(engine)

            for phase in corpus["phases"]:
                phase_name = str(phase["name"])
                if phase_name in BLOCKED_RECOVERY_PHASES:
                    phase_evidence.append(
                        {
                            "phase": phase_name,
                            "status": "semantic_difference",
                            "reason_code": "durable_restart_unavailable_in_edition",
                            "change_ids": [
                                event["change_id"] for event in phase["events"]
                            ],
                        }
                    )
                    continue

                apply_events(engine, state, phase["events"])
                current_phase_evidence: dict[str, object] = {
                    "phase": phase_name,
                    "status": "executed",
                    "change_ids": [event["change_id"] for event in phase["events"]],
                    "commit_boundary": "dml_command_tag_then_read_after_write_select",
                    "fresh_process_verified": False,
                    "sources": verify_source_state(engine, state, phase_name),
                    "views": {},
                }
                for workload_id, spec in admitted.items():
                    if workload_id in failures:
                        continue
                    expected = oracle_rows(
                        state, workload_id, workload_shapes[workload_id]
                    )
                    observed, intermediate_error, view_evidence = (
                        verify_workload_expected(
                            engine,
                            state,
                            workload_id,
                            spec,
                            workload_shapes[workload_id],
                            expected,
                        )
                    )
                    if intermediate_error is not None:
                        failures[workload_id] = (
                            f"phase {phase_name} {intermediate_error}"
                        )
                    elif observed != expected:
                        failures[workload_id] = (
                            f"phase {phase_name} mismatch: expected "
                            f"{canonical(expected)}, observed {canonical(observed)}"
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
            if workload_id not in phase_observed:
                raise RuntimeError(f"admitted workload {workload_id} has no observed phase")
            outcome = semantic_difference(bag_digest(workload["expected_final"]))
        correctness.append({"workload_id": workload_id, "outcome": outcome})

    result = {
        "schema_version": 2,
        "corpus_version": "incremental-sql-corpus-v1",
        "engine": {
            "name": "materialize",
            "version": version,
            "source_revision": f"official-image:{args.image_digest}",
            "configuration": {
                "corpus_sha256": hashlib.sha256(corpus_bytes).hexdigest(),
                "deployment": "official_materialize_emulator_single_container",
                "dml_acknowledgement": "exact_affected_row_command_tag_then_read_after_write_select",
                "edition": "emulator",
                "fault_tolerance": "unavailable_in_edition",
                "image_index_digest": args.image_digest,
                "image_platform_digest": args.image_platform_digest,
                "native_table_key_constraint": "unavailable_corpus_identity_verified_by_full_snapshot",
                "oracle": "duckdb_batch_recomputation_per_executed_phase",
                "performance_evidence_published": "false",
                "persistence": "unavailable_in_edition",
                "phase_evidence": canonical(phase_evidence),
                "restart_protocol": "not_executed_semantic_difference",
                "runner": "materialize-emulator-baseline-v1",
                "runtime_platform": args.runtime_platform,
                "source_verification": "all_registered_relations_exact_snapshot_every_executed_phase",
                "workload_sql_sha256": hashlib.sha256(
                    canonical(WORKLOADS).encode()
                ).hexdigest(),
            },
            "durability_mode": "emulator_no_data_persistence_or_fault_tolerance",
            "input_semantics": "application_keyed_native_insert_update_delete",
            "state_retention_policy": "emulator_process_lifetime_only",
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
