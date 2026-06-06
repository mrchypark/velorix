#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
script_path="${repo_root}/scripts/run-vind-product.sh"
first_e2e_path="${repo_root}/scripts/run-first-e2e-readiness.sh"
cli_path="${repo_root}/crates/velorix-cli/src/main.rs"
meta_cargo_path="${repo_root}/crates/velorix-meta/Cargo.toml"
doc_path="${repo_root}/docs/development/vind-product.md"
release_copy_path="${repo_root}/scripts/copy-readiness-sibling-evidence.py"
attest_path="${repo_root}/scripts/attest-ingress-tls-auth.sh"
durability_attest_path="${repo_root}/scripts/attest-object-store-durability-policy.sh"
backend_time_attest_path="${repo_root}/scripts/attest-hiqlite-backend-time.sh"
backend_time_release_preflight_path="${repo_root}/scripts/check-hiqlite-backend-time-release-inputs.sh"
backend_time_release_env_path="${repo_root}/scripts/write-hiqlite-backend-time-release-env.sh"
external_rustfs_path="${repo_root}/scripts/run-vind-product-external-rustfs.sh"
external_s3_path="${repo_root}/scripts/run-vind-product-external-s3.sh"
durability_assess_path="${repo_root}/scripts/assess-object-store-durability-policy.sh"
attach_rest_path="${repo_root}/scripts/attach-vind-product-rest.sh"
rest_api_smoke_path="${repo_root}/scripts/smoke-vind-rest-api.sh"
product_completion_report_path="${repo_root}/scripts/report-vind-product-completion.sh"
refresh_deployed_images_path="${repo_root}/scripts/refresh-vind-product-deployed-images.sh"
product_ingress_attest_path="${repo_root}/scripts/attest-vind-product-ingress.sh"
product_ingress_apply_path="${repo_root}/scripts/apply-vind-product-ingress.sh"
product_ingress_attach_path="${repo_root}/scripts/attach-vind-product-ingress.sh"
product_ingress_complete_path="${repo_root}/scripts/complete-vind-product-ingress.sh"
object_store_durability_attach_path="${repo_root}/scripts/attach-vind-object-store-durability.sh"
object_store_durability_complete_path="${repo_root}/scripts/complete-vind-object-store-durability.sh"
product_complete_path="${repo_root}/scripts/complete-vind-product.sh"
failover_evidence_writer_path="${repo_root}/scripts/write-standing-runtime-failover-evidence.py"
complete_input_preflight_path="${repo_root}/scripts/write-complete-vind-product-input-preflight.py"
next_product_step_path="${repo_root}/scripts/next-vind-product-step.sh"

python3 - "$script_path" "$first_e2e_path" "$cli_path" "$meta_cargo_path" "$doc_path" "$release_copy_path" "$attest_path" "$durability_attest_path" "$backend_time_attest_path" "$backend_time_release_preflight_path" "$backend_time_release_env_path" "$external_rustfs_path" "$external_s3_path" "$durability_assess_path" "$attach_rest_path" "$rest_api_smoke_path" "$product_completion_report_path" "$refresh_deployed_images_path" "$product_ingress_attest_path" "$product_ingress_apply_path" "$product_ingress_attach_path" "$product_ingress_complete_path" "$object_store_durability_attach_path" "$object_store_durability_complete_path" "$product_complete_path" "$failover_evidence_writer_path" "$complete_input_preflight_path" "$next_product_step_path" <<'PY'
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

script_path, first_e2e_path, cli_path, meta_cargo_path, doc_path, release_copy_path, attest_path, durability_attest_path, backend_time_attest_path, backend_time_release_preflight_path, backend_time_release_env_path, external_rustfs_path, external_s3_path, durability_assess_path, attach_rest_path, rest_api_smoke_path, product_completion_report_path, refresh_deployed_images_path, product_ingress_attest_path, product_ingress_apply_path, product_ingress_attach_path, product_ingress_complete_path, object_store_durability_attach_path, object_store_durability_complete_path, product_complete_path, failover_evidence_writer_path, complete_input_preflight_path, next_product_step_path = sys.argv[1:]
repo_root = Path(script_path).parents[1]
with open(script_path, "r", encoding="utf-8") as f:
    script = f.read()
with open(first_e2e_path, "r", encoding="utf-8") as f:
    first_e2e = f.read()
with open(cli_path, "r", encoding="utf-8") as f:
    cli = f.read()
with open(meta_cargo_path, "r", encoding="utf-8") as f:
    meta_cargo = f.read()
with open(doc_path, "r", encoding="utf-8") as f:
    doc = f.read()
with open(repo_root / ".github" / "workflows" / "release-gate.yml", "r", encoding="utf-8") as f:
    release_gate = f.read()
with open(repo_root / "docs" / "release" / "1.0-readiness-checklist.md", "r", encoding="utf-8") as f:
    release_doc = f.read()
with open(release_copy_path, "r", encoding="utf-8") as f:
    release_copy = f.read()
with open(attest_path, "r", encoding="utf-8") as f:
    attest = f.read()
with open(durability_attest_path, "r", encoding="utf-8") as f:
    durability_attest = f.read()
with open(backend_time_attest_path, "r", encoding="utf-8") as f:
    backend_time_attest = f.read()
with open(backend_time_release_preflight_path, "r", encoding="utf-8") as f:
    backend_time_release_preflight = f.read()
with open(backend_time_release_env_path, "r", encoding="utf-8") as f:
    backend_time_release_env = f.read()
with open(external_rustfs_path, "r", encoding="utf-8") as f:
    external_rustfs = f.read()
with open(external_s3_path, "r", encoding="utf-8") as f:
    external_s3 = f.read()
with open(durability_assess_path, "r", encoding="utf-8") as f:
    durability_assess = f.read()
with open(attach_rest_path, "r", encoding="utf-8") as f:
    attach_rest = f.read()
with open(rest_api_smoke_path, "r", encoding="utf-8") as f:
    rest_api_smoke = f.read()
with open(product_completion_report_path, "r", encoding="utf-8") as f:
    product_completion_report = f.read()
with open(refresh_deployed_images_path, "r", encoding="utf-8") as f:
    refresh_deployed_images = f.read()
with open(product_ingress_attest_path, "r", encoding="utf-8") as f:
    product_ingress_attest = f.read()
with open(product_ingress_apply_path, "r", encoding="utf-8") as f:
    product_ingress_apply = f.read()
with open(product_ingress_attach_path, "r", encoding="utf-8") as f:
    product_ingress_attach = f.read()
with open(product_ingress_complete_path, "r", encoding="utf-8") as f:
    product_ingress_complete = f.read()
with open(object_store_durability_attach_path, "r", encoding="utf-8") as f:
    object_store_durability_attach = f.read()
with open(object_store_durability_complete_path, "r", encoding="utf-8") as f:
    object_store_durability_complete = f.read()
with open(product_complete_path, "r", encoding="utf-8") as f:
    product_complete = f.read()
with open(failover_evidence_writer_path, "r", encoding="utf-8") as f:
    failover_evidence_writer = f.read()
with open(complete_input_preflight_path, "r", encoding="utf-8") as f:
    complete_input_preflight = f.read()
with open(next_product_step_path, "r", encoding="utf-8") as f:
    next_product_step = f.read()
with open(repo_root / "scripts" / "write-complete-vind-product-env.sh", "r", encoding="utf-8") as f:
    complete_product_env = f.read()
with open(repo_root / "crates" / "velorix-api" / "src" / "lib.rs", "r", encoding="utf-8") as f:
    api = f.read()
with open(repo_root / "crates" / "velorix-ingest-writer" / "src" / "main.rs", "r", encoding="utf-8") as f:
    ingest_writer = f.read()
with open(repo_root / "crates" / "velorix-meta" / "src" / "main.rs", "r", encoding="utf-8") as f:
    meta = f.read()
ingress_validator = cli.split(
    "fn validate_product_ingress_tls_auth_attestation", 1
)[1].split("\nfn ", 1)[0]


def durability_false_ready_fixture_rejected():
    target_dir = repo_root / "target" / "vind-contract-fixtures"
    target_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="durability-false-ready-", dir=target_dir) as raw_dir:
        fixture_dir = Path(raw_dir)
        product_path = fixture_dir / "product-evidence.json"
        preflight_path = fixture_dir / "preflight.json"
        report_path = fixture_dir / "report.json"
        product = {
            "evidence_kind": "velorix_product_slice_evidence",
            "object_store": {
                "mode": "external-s3",
                "authority_store_id": "s3://external/velorix-product/product/current",
                "bucket": "velorix-product",
                "s3_prefix": "product/current",
                "local_development_authority": False,
                "external_s3_bucket_validated": True,
                "external_s3_prefix_validated": True,
                "durability_policy_attestation": {
                    "validated": True,
                    "evidence": "object-store-durability-attestation.json",
                    "evidence_kind": "velorix_object_store_durability_policy_attestation",
                    "schema_version": 1,
                    "provider_kind": "s3-compatible",
                    "authority_store_id": "s3://external/velorix-product/product/old",
                    "bucket": "wrong-bucket",
                    "s3_prefix": "product/old",
                    "versioning_or_object_lock_enabled": False,
                    "server_side_encryption_enabled": True,
                    "backup_or_replication_configured": True,
                    "lifecycle_delete_policy_reviewed": True,
                    "destructive_delete_protection_reviewed": True,
                    "cost_controls_reviewed": True,
                    "attested_at": "2026-06-02T00:00:00Z",
                    "attester": "contract-fixture",
                },
            },
        }
        product_path.write_text(json.dumps(product, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        preflight = subprocess.run(
            [
                sys.executable,
                str(repo_root / "scripts" / "write-complete-vind-product-input-preflight.py"),
                "--product-evidence",
                str(product_path),
                "--output",
                str(preflight_path),
                "--external-s3-mode",
                "0",
                "--ingress-mode",
                "0",
                "--durability-mode",
                "1",
                "--hiqlite-mode",
                "0",
            ],
            cwd=repo_root,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if not preflight_path.is_file():
            return False
        preflight_data = json.loads(preflight_path.read_text(encoding="utf-8"))
        durability_step = (preflight_data.get("steps") or {}).get("durability") or {}
        if durability_step.get("status") == "already_validated" or durability_step.get("ready") is True:
            return False
        if preflight.returncode == 0:
            return False
        report_env = os.environ.copy()
        report_env.update(
            {
                "VELORIX_VIND_PRODUCT_DIR": str(fixture_dir),
                "VELORIX_VIND_PRODUCT_EVIDENCE": str(product_path),
                "VELORIX_PRODUCT_COMPLETION_REPORT": str(report_path),
                "VELORIX_PRODUCT_COMPLETE_REQUIRE_EXTERNAL_S3": "1",
            }
        )
        report = subprocess.run(
            [str(repo_root / "scripts" / "report-vind-product-completion.sh")],
            cwd=repo_root,
            env=report_env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if report.returncode != 0 or not report_path.is_file():
            return False
        report_data = json.loads(report_path.read_text(encoding="utf-8"))
        gates = {
            gate.get("id"): gate
            for gate in report_data.get("gates") or []
            if isinstance(gate, dict)
        }
        durability_gate = gates.get("object_store_durability_policy") or {}
        evidence = durability_gate.get("evidence") or {}
        invalid_subjects = evidence.get("durability_policy_attestation_invalid_subjects") or []
        return (
            durability_gate.get("status") != "pass"
            and "object_store.durability_policy_attestation.bucket" in invalid_subjects
            and "object_store.durability_policy_attestation.versioning_or_object_lock_enabled"
            in invalid_subjects
        )


def hiqlite_release_input_required_wins_over_will_run_fixture():
    target_dir = repo_root / "target" / "vind-contract-fixtures"
    target_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="hiqlite-next-step-", dir=target_dir) as raw_dir:
        fixture_dir = Path(raw_dir)
        report_path = fixture_dir / "product-completion-report.json"
        report = {
            "schema_version": 1,
            "report_kind": "velorix_product_completion_report",
            "product_complete": False,
            "gate_summary": {"pass": 9, "blocked": 0, "diagnostic": 1, "missing": 0},
            "completion_handoff": {
                "env_file": "target/velorix-product/complete-vind-product.env",
                "next_action": "scripts/complete-vind-product.sh --env-file target/velorix-product/complete-vind-product.env",
            },
            "completion_execution_plan": {
                "run_order": ["hiqlite_backend_time"],
                "will_run_steps": ["hiqlite_backend_time"],
                "blocked_steps": [],
                "waiting_steps": [],
                "steps": {
                    "hiqlite_backend_time": {
                        "state": "release_preflight_required",
                        "will_run": True,
                        "mode": "1",
                        "helper": "scripts/check-hiqlite-backend-time-release-inputs.sh + scripts/attest-hiqlite-backend-time.sh",
                        "status": "deferred_to_release_preflight",
                        "missing_count": 0,
                        "invalid_count": 0,
                        "missing_subjects": [],
                        "invalid_subjects": [],
                    }
                },
            },
            "completion_plan": {
                "input_required_steps": ["hiqlite_backend_time_release"],
                "waiting_steps": [],
                "runnable_steps": [],
                "blocked_without_action_steps": [],
                "steps": [
                    {
                        "id": "hiqlite_backend_time_release",
                        "state": "input_required",
                        "status": "diagnostic",
                        "summary": "Hiqlite backend-time release provenance is incomplete",
                        "next_action": "scripts/write-hiqlite-backend-time-release-env.sh --product-evidence target/velorix-product/product-evidence.json && scripts/check-hiqlite-backend-time-release-inputs.sh --env-file target/velorix-product/hiqlite-backend-time-release.env --product-evidence target/velorix-product/product-evidence.json",
                        "input_summary_requires_input": True,
                        "input_summary": {
                            "placeholder_count": 2,
                            "secret_placeholder_count": 1,
                            "placeholders": [
                                "VELORIX_RELEASE_COMMIT",
                                "VELORIX_CI_SIGSTORE_BUNDLE_BASE64",
                            ],
                            "secret_placeholders": ["VELORIX_CI_SIGSTORE_BUNDLE_BASE64"],
                            "preflight_steps": [
                                {
                                    "step": "hiqlite_backend_time",
                                    "status": "deferred_to_release_preflight",
                                    "ready": None,
                                    "missing_count": 0,
                                    "invalid_count": 0,
                                    "missing_subjects": [],
                                    "invalid_subjects": [],
                                }
                            ],
                            "release_preflight": {
                                "evidence": "hiqlite-backend-time-release-preflight.json",
                                "status": "blocked",
                                "missing_count": 1,
                                "invalid_count": 1,
                                "missing": [
                                    {
                                        "subject": "VELORIX_CI_SIGSTORE_BUNDLE_BASE64",
                                        "detail": "VELORIX_CI_SIGSTORE_BUNDLE_BASE64 is required",
                                    }
                                ],
                                "invalid": [
                                    {
                                        "subject": "VELORIX_RELEASE_COMMIT",
                                        "detail": "VELORIX_RELEASE_COMMIT still contains a REPLACE_WITH placeholder",
                                    }
                                ],
                                "missing_subjects": ["VELORIX_CI_SIGSTORE_BUNDLE_BASE64"],
                                "invalid_subjects": ["VELORIX_RELEASE_COMMIT"],
                            },
                            "creates_product_complete_evidence": False,
                        },
                    }
                ],
            },
        }
        report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        result = subprocess.run(
            [
                str(repo_root / "scripts" / "next-vind-product-step.sh"),
                "--report",
                str(report_path),
                "--json",
            ],
            cwd=repo_root,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if result.returncode != 0:
            return False
        next_step = json.loads(result.stdout)
        step = next_step.get("next_step") or {}
        return (
            next_step.get("state") == "input_required"
            and step.get("id") == "hiqlite_backend_time"
            and step.get("gate") == "hiqlite_backend_time_release"
            and step.get("gate_state") == "input_required"
            and step.get("will_run") is True
            and "write-hiqlite-backend-time-release-env.sh" in (step.get("command") or "")
            and "complete-vind-product.sh --env-file" not in (step.get("command") or "")
            and (step.get("input_summary") or {}).get("release_preflight", {}).get("status") == "blocked"
        )


def external_s3_out_of_scope_fixture_completes_required_gates():
    target_dir = repo_root / "target" / "vind-contract-fixtures"
    target_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="external-s3-out-of-scope-", dir=target_dir) as raw_dir:
        fixture_dir = Path(raw_dir)
        product_path = fixture_dir / "product-evidence.json"
        report_path = fixture_dir / "product-completion-report.json"
        rest_smoke_path = fixture_dir / "rest-api-smoke.json"
        digest = "sha256:" + ("a" * 64)
        product = {
            "evidence_kind": "velorix_product_slice_evidence",
            "rest_callable": True,
            "api": {
                "generic_query_enabled": False,
                "openapi": {
                    "catalog_smoke_passed": True,
                    "promoted_api_path": "/v1/api/scores/positive",
                },
                "query_policy": {"catalog_smoke_passed": True},
                "auth": {
                    "mode": "bearer-token",
                    "missing_token_rejected": True,
                    "wrong_token_rejected": True,
                    "data_plane_token_rejected_on_admin_route": True,
                    "local_tls_auth_smoke": {
                        "enabled": True,
                        "passed": True,
                        "evidence": "tls-auth-smoke.json",
                        "scope": "local port-forwarded vind/vCluster service",
                        "public_ingress_attestation": False,
                        "trusted_for_product_complete": False,
                    },
                    "ingress_tls_auth_attestation": {
                        "public_ingress_attestation": True,
                        "trusted_for_product_complete": True,
                        "evidence": "ingress-tls-auth-attestation.json",
                    },
                },
                "compile_deploy": {
                    "worker_run_verified": True,
                    "activated_view_id": "positive_scores_by_user",
                },
            },
            "object_store": {
                "mode": "rustfs-local",
                "local_development_authority": True,
            },
            "metadata_store": {
                "standing_runtime_adversarial_smoke": {"status": "pass"},
                "hiqlite_backend_time_assessment": {
                    "validated": True,
                    "evidence": "hiqlite-backend-time-assessment.json",
                    "backend_time_source_kind": "raft_replicated_authority_time",
                    "bounded_wall_clock_failover": True,
                    "can_generate_product_complete_backend_time_attestation": True,
                },
                "hiqlite_backend_time_attestation": {
                    "validated": True,
                    "evidence": "hiqlite-backend-time-attestation.json",
                    "authoritative_backend_time": True,
                    "time_source_kind": "raft_replicated_authority_time",
                    "bounded_wall_clock_failover": True,
                    "release_validator_fail_closed": True,
                    "trusted_for_release_validator": False,
                    "trusted_for_product_complete": False,
                    "attestation_origin": "diagnostic_deployed_product",
                    "source_kind": "local_diagnostic",
                },
            },
            "standing_runtime_fencing": {
                "required_mode": True,
                "configured_mode": "required",
                "multi_writer_fencing_safe": True,
                "production_bounded_failover_safe": True,
                "multi_replica_fencing_smoke": {"status": "pass"},
            },
            "no_pvc": {
                "namespace_validated": True,
                "evidence": "no-pvc-namespace.json",
            },
            "ingest_writer": {
                "job_completed": True,
                "append_outcome": "appended",
                "lifecycle_attestation": {
                    "trusted_for_product_complete": True,
                    "source": "generated",
                },
            },
            "deployed_images": {
                "velorix-api": {"image_digest": digest},
                "velorix-meta": {"image_digest": digest},
            },
        }
        rest_smoke = {
            "status": "pass",
            "generic_query_disabled": True,
            "ingested_positive_sum": 25,
            "ingested_positive_count": 2,
        }
        product_path.write_text(json.dumps(product, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        rest_smoke_path.write_text(json.dumps(rest_smoke, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        report_env = os.environ.copy()
        report_env.update(
            {
                "VELORIX_VIND_PRODUCT_DIR": str(fixture_dir),
                "VELORIX_VIND_PRODUCT_EVIDENCE": str(product_path),
                "VELORIX_PRODUCT_COMPLETION_REPORT": str(report_path),
                "VELORIX_PRODUCT_COMPLETE_REQUIRE_EXTERNAL_S3": "0",
            }
        )
        report = subprocess.run(
            [str(repo_root / "scripts" / "report-vind-product-completion.sh")],
            cwd=repo_root,
            env=report_env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if report.returncode != 0 or not report_path.is_file():
            return False
        report_data = json.loads(report_path.read_text(encoding="utf-8"))
        gates = {
            gate.get("id"): gate
            for gate in report_data.get("gates") or []
            if isinstance(gate, dict)
        }
        next_result = subprocess.run(
            [
                str(repo_root / "scripts" / "next-vind-product-step.sh"),
                "--report",
                str(report_path),
                "--json",
            ],
            cwd=repo_root,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if next_result.returncode != 0:
            return False
        next_data = json.loads(next_result.stdout)
        return (
            gates.get("object_store_external_authority", {}).get("status") == "out_of_scope"
            and gates.get("object_store_durability_policy", {}).get("status") == "out_of_scope"
            and report_data.get("product_complete") is True
            and report_data.get("completion_scope", {}).get("external_s3_required") is False
            and "object_store_external_authority" in report_data.get("completion_scope", {}).get("excluded_gates", [])
            and "object_store_durability_policy" in report_data.get("completion_scope", {}).get("excluded_gates", [])
            and next_data.get("state") == "complete"
            and next_data.get("next_step") is None
        )


def public_ingress_out_of_scope_local_tls_boundary_fixture():
    target_dir = repo_root / "target" / "vind-contract-fixtures"
    target_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="public-ingress-out-of-scope-", dir=target_dir) as raw_dir:
        fixture_dir = Path(raw_dir)
        product_path = fixture_dir / "product-evidence.json"
        report_path = fixture_dir / "product-completion-report.json"
        rest_smoke_path = fixture_dir / "rest-api-smoke.json"
        digest = "sha256:" + ("b" * 64)
        product = {
            "evidence_kind": "velorix_product_slice_evidence",
            "rest_callable": True,
            "api": {
                "generic_query_enabled": False,
                "openapi": {
                    "catalog_smoke_passed": True,
                    "promoted_api_path": "/v1/api/scores/positive",
                },
                "query_policy": {"catalog_smoke_passed": True},
                "auth": {
                    "mode": "bearer-token",
                    "missing_token_rejected": True,
                    "wrong_token_rejected": True,
                    "data_plane_token_rejected_on_admin_route": True,
                    "ingress_tls_auth_attestation": None,
                    "local_tls_auth_smoke": {
                        "enabled": True,
                        "passed": True,
                        "evidence": "tls-auth-smoke.json",
                        "scope": "local port-forwarded vind/vCluster service",
                        "public_ingress_attestation": False,
                        "trusted_for_product_complete": False,
                    },
                },
                "compile_deploy": {
                    "worker_run_verified": True,
                    "activated_view_id": "positive_scores_by_user",
                },
            },
            "object_store": {
                "mode": "rustfs-local",
                "local_development_authority": True,
            },
            "metadata_store": {
                "standing_runtime_adversarial_smoke": {"status": "pass"},
                "hiqlite_backend_time_assessment": {
                    "validated": True,
                    "evidence": "hiqlite-backend-time-assessment.json",
                    "backend_time_source_kind": "raft_replicated_authority_time",
                    "bounded_wall_clock_failover": True,
                    "can_generate_product_complete_backend_time_attestation": True,
                },
                "hiqlite_backend_time_attestation": {
                    "validated": True,
                    "evidence": "hiqlite-backend-time-attestation.json",
                    "authoritative_backend_time": True,
                    "time_source_kind": "raft_replicated_authority_time",
                    "bounded_wall_clock_failover": True,
                    "release_validator_fail_closed": True,
                    "trusted_for_release_validator": False,
                    "trusted_for_product_complete": False,
                    "attestation_origin": "diagnostic_deployed_product",
                    "source_kind": "local_diagnostic",
                },
            },
            "standing_runtime_fencing": {
                "required_mode": True,
                "configured_mode": "required",
                "multi_writer_fencing_safe": True,
                "production_bounded_failover_safe": True,
                "multi_replica_fencing_smoke": {"status": "pass"},
            },
            "no_pvc": {
                "namespace_validated": True,
                "evidence": "no-pvc-namespace.json",
            },
            "ingest_writer": {
                "job_completed": True,
                "append_outcome": "appended",
                "lifecycle_attestation": {
                    "trusted_for_product_complete": True,
                    "source": "generated",
                },
            },
            "deployed_images": {
                "velorix-api": {"image_digest": digest},
                "velorix-meta": {"image_digest": digest},
            },
        }
        rest_smoke = {
            "status": "pass",
            "generic_query_disabled": True,
            "ingested_positive_sum": 25,
            "ingested_positive_count": 2,
        }
        product_path.write_text(json.dumps(product, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        rest_smoke_path.write_text(json.dumps(rest_smoke, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        report_env = os.environ.copy()
        report_env.update(
            {
                "VELORIX_VIND_PRODUCT_DIR": str(fixture_dir),
                "VELORIX_VIND_PRODUCT_EVIDENCE": str(product_path),
                "VELORIX_PRODUCT_COMPLETION_REPORT": str(report_path),
                "VELORIX_PRODUCT_COMPLETE_REQUIRE_EXTERNAL_S3": "0",
                "VELORIX_PRODUCT_COMPLETE_REQUIRE_PUBLIC_INGRESS": "0",
            }
        )
        report = subprocess.run(
            [str(repo_root / "scripts" / "report-vind-product-completion.sh")],
            cwd=repo_root,
            env=report_env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if report.returncode != 0 or not report_path.is_file():
            return False
        report_data = json.loads(report_path.read_text(encoding="utf-8"))
        gates = {
            gate.get("id"): gate
            for gate in report_data.get("gates") or []
            if isinstance(gate, dict)
        }
        next_result = subprocess.run(
            [
                str(repo_root / "scripts" / "next-vind-product-step.sh"),
                "--report",
                str(report_path),
                "--json",
            ],
            cwd=repo_root,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if next_result.returncode != 0:
            return False
        next_data = json.loads(next_result.stdout)
        return (
            gates.get("tls_auth_boundary", {}).get("status") == "pass"
            and gates.get("public_ingress_tls_auth", {}).get("status") == "out_of_scope"
            and report_data.get("completion_scope", {}).get("public_ingress_required") is False
            and "public_ingress_tls_auth" in report_data.get("completion_scope", {}).get("excluded_gates", [])
            and "public_ingress_tls_auth_out_of_scope_does_not_prove_public_dns_tls_or_external_client_reachability"
            in report_data.get("completion_scope", {}).get("warnings", [])
            and report_data.get("product_complete") is True
            and next_data.get("state") == "complete"
        )


checks = {
    "rejects stale durability attestation false-ready fixture": durability_false_ready_fixture_rejected(),
    "Hiqlite release input-required gate wins over will-run fixture": hiqlite_release_input_required_wins_over_will_run_fixture(),
    "external S3 out-of-scope fixture completes required gates": external_s3_out_of_scope_fixture_completes_required_gates(),
    "public ingress out-of-scope fixture uses local TLS/auth boundary": public_ingress_out_of_scope_local_tls_boundary_fixture(),
    "defines admin API curl helper": (
        "curl_admin_api()" in script
        and 'authorization: Bearer ${admin_bearer_token}' in script
    ),
    "product smoke calls compile/deploy job catalog": (
        'curl_admin_api "http://127.0.0.1:${api_local_port}/v1/view-compile-deploy/jobs"'
        in script
        and 'tee "${output_dir}/view-compile-deploy-jobs.json"' in script
    ),
    "product smoke runs compile/deploy worker activation": (
        'curl_admin_api \\' in script
        and '-X POST "http://127.0.0.1:${api_local_port}/v1/view-compile-deploy/run-once"'
        in script
        and 'tee "${output_dir}/view-compile-deploy-run-once.json"' in script
        and 'pending-scores-view-after-compile-deploy.json' in script
        and 'pending-scores-query-after-compile-deploy.json' in script
        and 'api_compile_deploy_worker_run_verified=1' in script
    ),
    "validates self-contained compiler request": (
        "feldera_standing_view_compile_request_v1" in script
        and "compiler_request.get(\"input_relations\")" in script
        and "compiler_request.get(\"output_relations\")" in script
        and 'shape.get("is_materialized") is not True' in script
    ),
    "records product evidence field": bool(
        re.search(
            r'"compile_deploy": \{\s+"job_catalog_verified": api_compile_deploy_job_catalog_verified == "1"',
            script,
        )
    )
    and '"job_catalog_evidence_file": "view-compile-deploy-jobs.json"' in script
    and '"worker_run_verified": api_compile_deploy_worker_run_verified == "1"' in script
    and '"run_once_evidence_file": "view-compile-deploy-run-once.json"' in script
    and '"activated_view_id": api_compile_deploy_activated_view_id' in script
    and '"activated_execution_mode": "standing_runtime"' in script,
    "records no-PVC namespace sibling evidence": (
        '"evidence": "no-pvc-namespace.json" if no_pvc_namespace_validated == "1" else None'
        in script
        and '"contract": "no PersistentVolumeClaim objects in the Velorix product namespace"'
        in script
    ),
    "records Hiqlite authority sibling evidence": (
        "hiqlite_authority_sibling_attestation" in script
        and '"evidence": "hiqlite-authority-attestation.json"' in script
        and 'cp "$hiqlite_authority_attestation_file" "$hiqlite_authority_sibling_attestation"'
        in script
    ),
    "meta deployment uses recreate rollout to avoid concurrent metadata writers": (
        "name: velorix-meta" in script
        and re.search(
            r"kind: Deployment\s+metadata:\s+name: velorix-meta.*?spec:\s+replicas: 1\s+strategy:\s+type: Recreate",
            script,
            re.S,
        )
    ),
    "external Hiqlite reuse does not mutate the authority secret": (
        'hiqlite_api_secret_ref_name="velorix-hiqlite-auth"' in script
        and 'if [ "$hiqlite_deploy" = "1" ]; then' in script
        and 'hiqlite_api_secret_ref_name="velorix-meta-hiqlite-auth"' in script
        and "name: velorix-meta-hiqlite-auth" in script
        and "name: ${hiqlite_api_secret_ref_name}" in script
    ),
    "managed Hiqlite selectors are stable across reruns": (
        re.search(
            r"name: velorix-hiqlite-headless.*?spec:\s+clusterIP: None\s+selector:\s+app: velorix-hiqlite\s+ports:",
            script,
            re.S,
        )
        and re.search(
            r"name: velorix-hiqlite\s+namespace: \$\{namespace\}.*?spec:\s+selector:\s+app: velorix-hiqlite\s+ports:",
            script,
            re.S,
        )
        and re.search(
            r"kind: StatefulSet\s+metadata:\s+name: velorix-hiqlite.*?selector:\s+matchLabels:\s+app: velorix-hiqlite\s+template:",
            script,
            re.S,
        )
    ),
    "product Services use stable selectors across reruns": (
        "remove_service_run_id_selector()" in script
        and "remove_service_run_id_selector velorix-hiqlite-headless" in script
        and "remove_service_run_id_selector velorix-hiqlite" in script
        and "remove_service_run_id_selector velorix-meta" in script
        and "remove_service_run_id_selector velorix-api" in script
        and re.search(
            r"kind: Service\s+metadata:\s+name: velorix-meta.*?spec:\s+selector:\s+app: velorix-meta\s+ports:",
            script,
            re.S,
        )
        and re.search(
            r"kind: Service\s+metadata:\s+name: velorix-api.*?spec:\s+selector:\s+app: velorix-api\s+ports:",
            script,
            re.S,
        )
    ),
    "product smoke reuses metadata but compile/deploy catalog proof fails closed": (
        "query-policy-interactive-create.json" in script
        and "positive-scores-view-create.json" in script
        and '409)' in script
        and 'product-smoke-\'"${run_id}"\'' in script
        and "pending_scores_by_user is already active" in script
        and not re.search(r"if already_active:\s+raise SystemExit\(0\)", script)
        and "unrelated pending job catalog as valid product" in doc
    ),
    "generates and validates Hiqlite encryption keys compatible with cryptr": (
        "secrets.token_bytes(32)" in script
        and "validate_hiqlite_enc_keys()" in script
        and "VELORIX_HIQLITE_ENC_KEYS key {key_id} must decode to exactly 32 bytes" in script
        and 'validate_hiqlite_enc_keys "$hiqlite_enc_key_active" "$hiqlite_enc_keys"' in script
    ),
    "uses Hiqlite remote client node addresses without URL schemes": (
        "validate_hiqlite_nodes_for_remote_client()" in script
        and 'hiqlite_nodes="velorix-hiqlite-0.velorix-hiqlite-headless:8200,velorix-hiqlite-1.velorix-hiqlite-headless:8200,velorix-hiqlite-2.velorix-hiqlite-headless:8200"'
        in script
        and "must use Hiqlite remote client addresses without URL schemes" in script
        and 'validate_hiqlite_nodes_for_remote_client "$hiqlite_nodes"' in script
    ),
    "compiles Hiqlite remote client with server-compatible stream API features": (
        'features = ["full"]' in meta_cargo
        and "server image uses `--features server`, which enables `full`" in meta_cargo
    ),
    "Docker product builds include local Hiqlite authority-time source context": (
        "VELORIX_HIQLITE_LOCAL_SOURCE_DIR" in script
        and "--build-context" in script
        and "velorix-hiqlite-source=${hiqlite_local_source_dir}" in script
        and "COPY --from=velorix-hiqlite-source . /hiqlite" in (repo_root / "Dockerfile.api").read_text()
        and "COPY --from=velorix-hiqlite-source . /hiqlite" in (repo_root / "Dockerfile.meta").read_text()
        and "COPY --from=velorix-hiqlite-source . /hiqlite" in (repo_root / "Dockerfile.ingest-writer").read_text()
        and "COPY --from=velorix-hiqlite-source . /hiqlite" in (repo_root / "Dockerfile.all-in-one").read_text()
        and "COPY --from=velorix-hiqlite-source . ./hiqlite" in (repo_root / "Dockerfile.hiqlite").read_text()
        and "--path /workspace/hiqlite/hiqlite" in (repo_root / "Dockerfile.hiqlite").read_text()
    ),
    "Hiqlite required fencing is governed by capability evidence, not stale static block": (
        "VELORIX_STANDING_RUNTIME_FENCING=required is not yet supported with VELORIX_META_BACKEND=hiqlite"
        not in script
        and "run_hiqlite_backend_time_assessment" in script
        and "VELORIX_REQUIRE_HIQLITE_BACKEND_TIME" in script
        and "production_multi_writer_safe" in script
        and "bounded_wall_clock_failover" in script
    ),
    "records ingress/TLS/auth sibling evidence": (
        "ingress_tls_auth_sibling_attestation" in script
        and '"evidence": "ingress-tls-auth-attestation.json"' in script
        and 'cp "$ingress_tls_auth_attestation_file" "$ingress_tls_auth_sibling_attestation"'
        in script
    ),
    "uses target-backed local scratch instead of mktemp for ingress attestation": (
        "VELORIX_LOCAL_SCRATCH_DIR" in doc
        and "local_scratch_dir=\"${VELORIX_LOCAL_SCRATCH_DIR:-target/velorix-product/scratch}\""
        in attest
        and "mktemp -d" not in attest
    ),
    "external S3 validation job avoids /tmp and uses no-PVC emptyDir scratch": (
        "mountPath: /work" in script
        and "emptyDir: {}" in script
        and "/tmp/" not in script
    ),
    "external S3 validation bounds list calls to the exact probe key": (
        'list-objects-v2 --bucket "${bucket}" --prefix "${external_s3_validation_key}" --max-keys 1'
        in script
        and 'list-objects-v2 --bucket \\"{bucket}\\" --prefix \\"{key}\\" --max-keys 1'
        in cli
        and "exact-key `list-objects-v2 --max-keys 1`" in doc
    ),
    "external S3 path-style setting reaches validation and product clients": (
        's3_force_path_style="${VELORIX_S3_FORCE_PATH_STYLE:-1}"' in script
        and "VELORIX_S3_FORCE_PATH_STYLE must be 0 or 1" in script
        and "aws configure set default.s3.addressing_style path" in script
        and '"force_path_style": s3_force_path_style == "1"' in script
        and "HQL_S3_PATH_STYLE" in script
        and "s3_force_path_style_bool" in script
        and "VELORIX_S3_FORCE_PATH_STYLE" in external_s3
        and '"force_path_style": s3_force_path_style == "1"' in external_s3
        and "VELORIX_S3_FORCE_PATH_STYLE" in complete_input_preflight
        and "VELORIX_S3_FORCE_PATH_STYLE" in complete_product_env
        and "with_virtual_hosted_style_request(!self.force_path_style)" in api
        and "with_virtual_hosted_style_request(!config.force_path_style)" in ingest_writer
        and "with_virtual_hosted_style_request(!force_path_style)" in meta
        and "VELORIX_S3_FORCE_PATH_STYLE=1" in doc
    ),
    "external S3 supports existing Kubernetes credential Secret and session token": (
        "VELORIX_S3_CREDENTIALS_SECRET_NAME" in script
        and "VELORIX_S3_CREDENTIALS_SECRET_MANAGED" in script
        and "existing-kubernetes-secret" in script
        and "existing S3 credentials Secret is missing keys" in script
        and "key: session-token" in script
        and "optional: true" in script
        and "AWS_SESSION_TOKEN" in external_s3
        and "VELORIX_S3_CREDENTIALS_SECRET_MANAGED=0" in external_s3
        and "AWS_SESSION_TOKEN" in complete_input_preflight
        and "VELORIX_S3_CREDENTIALS_SECRET_MANAGED" in complete_input_preflight
        and "AWS_SESSION_TOKEN" in complete_product_env
        and "blank AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY/AWS_SESSION_TOKEN" in complete_product_env
        and "with_token(session_token)" in api
        and "with_token(session_token)" in ingest_writer
        and "with_token(session_token)" in meta
        and "session-token" in doc
        and "VELORIX_S3_CREDENTIALS_SECRET_MANAGED=0" in doc
        and "leave\n`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_SESSION_TOKEN` empty" in doc
    ),
    "release validator parses external S3 validation job and log evidence": (
        "validate_product_external_s3_validation_siblings" in cli
        and '"external-s3-validate-job.json"' in cli
        and '"external-s3-validate.log"' in cli
        and '"velorix-external-s3-validate"' in cli
        and "velorix external-s3 validation ok bucket=" in cli
        and "readiness_report_rejects_product_evidence_with_malformed_external_s3_validation_job_sibling"
        in cli
        and "readiness_report_rejects_product_evidence_with_mismatched_external_s3_validation_log"
        in cli
    ),
    "release validator parses compile/deploy job catalog sibling evidence": (
        "validate_product_compile_deploy_job_catalog_sibling_evidence" in cli
        and "compile_deploy_job_catalog_fixture_json" in cli
        and "feldera_standing_view_compile_request_v1" in cli
        and "compiler_request sql does not prove scores aggregation semantics" in cli
        and "readiness_report_rejects_product_compile_deploy_job_catalog_sibling_without_compiler_request"
        in cli
        and "readiness_report_rejects_product_compile_deploy_job_catalog_sibling_with_wrong_view"
        in cli
        and "readiness_report_rejects_product_compile_deploy_job_catalog_sibling_with_wrong_schema"
        in cli
    ),
    "release validator parses OpenAPI and query-policy sibling evidence": (
        "validate_product_openapi_sibling_evidence" in cli
        and "validate_product_query_policy_sibling_evidence" in cli
        and "openapi_fixture_json" in cli
        and "query_policy_interactive_fixture_json" in cli
        and "must not expose generic /v1/query" in cli
        and "does not prove weak policy rejection" in cli
        and "readiness_report_rejects_product_openapi_claim_with_mismatched_sibling_evidence"
        in cli
        and "readiness_report_rejects_product_openapi_claim_with_forbidden_generic_query_path"
        in cli
        and "readiness_report_rejects_product_openapi_claim_with_wrong_response_schema_sibling"
        in cli
        and "readiness_report_rejects_product_query_policy_claim_with_mismatched_readback_sibling"
        in cli
        and "readiness_report_rejects_product_query_policy_claim_without_weak_rejection_sibling"
        in cli
        and "readiness_report_rejects_product_query_policy_claim_with_unbounded_policy_sibling"
        in cli
    ),
    "external RustFS wrapper runs product in external-s3 mode without PVC": (
        "VELORIX_OBJECT_STORE_MODE=external-s3" in external_rustfs
        and "scripts/run-vind-product.sh" in external_rustfs
        and "docker volume create" in external_rustfs
        and "docker run -d" in external_rustfs
        and "RUSTFS_ACCESS_KEY" in external_rustfs
        and "RustFS default credentials are not allowed" in external_rustfs
        and "trusted_for_product_complete" in external_rustfs
        and "False" in external_rustfs
        and "external-rustfs.env" in external_rustfs
        and "s3://external/${bucket}/${prefix}" in external_rustfs
        and "resolve_pod_endpoint()" in external_rustfs
        and "resolved k3d pod endpoint for external RustFS" in external_rustfs
        and "host.docker.internal" in external_rustfs
        and "VELORIX_OBJECT_STORE_LOCAL_DEVELOPMENT_AUTHORITY=1" in external_rustfs
        and "local_development_authority" in script
        and "local development object-store authorities cannot satisfy product-complete durability policy attestation" in cli
        and "PersistentVolumeClaim" not in external_rustfs
    ),
    "nonlocal external S3 wrapper is distinct from local RustFS and fail-closed": (
        "velorix_external_s3_product_input" in external_s3
        and "VELORIX_OBJECT_STORE_MODE=external-s3" in external_s3
        and "VELORIX_OBJECT_STORE_LOCAL_DEVELOPMENT_AUTHORITY=0" in external_s3
        and "refuses VELORIX_OBJECT_STORE_LOCAL_DEVELOPMENT_AUTHORITY=1" in external_s3
        and "AWS_ENDPOINT_URL looks like a local development endpoint" in external_s3
        and "VELORIX_EXTERNAL_S3_ALLOW_LOCAL_ENDPOINT=1 only for diagnostics" in external_s3
        and "ip.is_private" in external_s3
        and "AWS_ENDPOINT_URL still contains a placeholder" in external_s3
        and "AWS_ENDPOINT_URL must be the S3/OSS service endpoint only, without bucket, prefix, query, or fragment" in external_s3
        and "AWS_ENDPOINT_URL must be the S3/OSS service endpoint only, without bucket, prefix, query, or fragment" in complete_input_preflight
        and "AWS_ACCESS_KEY_ID is placeholder or known development default" in external_s3
        and "AWS_SECRET_ACCESS_KEY is placeholder or known development default" in external_s3
        and "VELORIX_S3_BUCKET still contains a placeholder" in external_s3
        and "VELORIX_S3_PREFIX is required for --validate-only" in external_s3
        and "scripts/run-vind-product.sh" in external_s3
	        and "VELORIX_EXTERNAL_S3_RUN_PRODUCT=0" in external_s3
	        and "--env-file" in external_s3
	        and "--output-dir" in external_s3
	        and "--input-evidence" in external_s3
	        and "--validate-only" in external_s3
	        and "source_env_file_preserving_overrides" in external_s3
	        and "run_product_cli=1" in external_s3
	        and "output_dir_cli=1" in external_s3
	        and "input_evidence_cli=1" in external_s3
	        and 'input_evidence="${VELORIX_EXTERNAL_S3_INPUT_EVIDENCE:-${output_dir}/external-s3-product-input.json}"' in external_s3
	        and "path.parent.mkdir(parents=True, exist_ok=True)" in external_s3
	        and "VELORIX_OBJECT_STORE_DURABILITY_ATTESTATION_FILE" in external_s3
        and "PersistentVolumeClaim" not in external_s3
        and "scripts/run-vind-product-external-s3.sh" in doc
        and "--env-file target/velorix-product/complete-vind-product.env" in doc
        and "--validate-only" in doc
        and "external-s3-product-input.json" in doc
        and "Local Docker RustFS remains intentionally separate" in doc
        and "scripts/run-vind-product-external-s3.sh" in product_completion_report
    ),
    "vCluster bootstrap retries clean transient failed standalone resources": (
        "VELORIX_VCLUSTER_CREATE_RETRIES" in script
        and "vcluster_bootstrap_log_is_retryable()" in script
        and "cleanup_failed_vcluster_create_attempt()" in script
        and "procready not received" in script
        and "vm_container_systemd_exit" in script
        and "exit status 137" in script
        and 'vcluster delete "$cluster" --driver docker' in script
        and "vcluster-create-attempt-${attempt}.log" in script
        and "retrying vCluster create after local bootstrap transient" in script
        and "write_local_environment_doctor_snapshot()" in script
        and "VELORIX_DOCTOR_VCLUSTER_STANDALONE_PROBE=1" in script
        and "local-environment-doctor.json" in script
        and 'evidence_files["local_environment_doctor"] = doctor_path' in script
        and '"remediation_commands": doctor.get("remediation_commands")' in script
        and "retry vCluster bootstrap twice by default after" in doc
        and "standalone failures such as `procReady not received`" in doc
        and "status `137`" in doc
        and "vCluster standalone compatibility probe" in doc
        and "remediation_commands" in doc
        and "VELORIX_DOCTOR_VCLUSTER_STANDALONE_PROBE" in doc
        and "socket bind mount" in doc
        and "VELORIX_DOCTOR_VCLUSTER_STANDALONE_PROBE" in open(attest_path.replace("attest-ingress-tls-auth.sh", "doctor-vind-local.sh"), "r", encoding="utf-8").read()
        and "--mount type=bind,src=/run/containerd/containerd.sock" in open(attest_path.replace("attest-ingress-tls-auth.sh", "doctor-vind-local.sh"), "r", encoding="utf-8").read()
    ),
    "supports existing local Kubernetes context when docker vCluster is unavailable": (
        "VELORIX_VIND_CLUSTER_DRIVER" in script
        and "existing-context" in script
        and "VELORIX_K8S_CONTEXT" in script
        and "validate_existing_kubernetes_context()" in script
        and "VELORIX_EXISTING_CONTEXT_ALLOW_REMOTE" in script
        and "VELORIX_IMAGE_LOAD_MODE" in script
        and "load_image_into_k3d()" in script
        and "ctr -n k8s.io images import -" in script
        and "k3d_node_containers()" in script
        and "created_namespace=0" in script
        and 'delete namespace "$namespace"' in script
        and "VELORIX_VIND_CLUSTER_DRIVER=existing-context" in doc
        and "VELORIX_K8S_CONTEXT=k3d-certd-k3d" in doc
        and "VELORIX_IMAGE_LOAD_MODE=auto" in doc
        and "No PVCs" in doc
    ),
    "post-restart expected rows use observed multi-replica smoke pass status": (
        '"$multi_replica_fencing_smoke_passed" "$run_id"' in script
        and '"$multi_replica_fencing_smoke" "$run_id"' not in script
    ),
    "records and validates external object-store durability attestation": (
        "VELORIX_OBJECT_STORE_DURABILITY_ATTESTATION_FILE" in script
        and "validate_object_store_durability_attestation()" in script
        and "velorix_object_store_durability_policy_attestation" in script
        and "object-store-durability-attestation.json" in script
        and "external S3-compatible authority lacks operator-reviewed durability policy attestation"
        in script
        and '"durability_policy_attestation": object_store_durability_attestation'
        in script
        and "validate_product_object_store_durability_policy_attestation" in cli
        and '"/object_store/durability_policy_attestation"' in cli
        and "object-store durability policy evidence" in cli
        and "VELORIX_OBJECT_STORE_DURABILITY_ATTESTATION_FILE" in doc
        and "velorix_object_store_durability_policy_attestation" in doc
        and "scripts/complete-vind-object-store-durability.sh" in product_completion_report
    ),
    "generates object-store durability attestation only from explicit operator review": (
        (repo_root / "scripts" / "attest-object-store-durability-policy.sh").is_file()
        and "VELORIX_OBJECT_STORE_VERSIONING_OR_OBJECT_LOCK_ENABLED" in durability_attest
        and "VELORIX_OBJECT_STORE_SERVER_SIDE_ENCRYPTION_ENABLED" in durability_attest
        and "VELORIX_OBJECT_STORE_BACKUP_OR_REPLICATION_CONFIGURED" in durability_attest
        and "VELORIX_OBJECT_STORE_LIFECYCLE_DELETE_POLICY_REVIEWED" in durability_attest
        and "VELORIX_OBJECT_STORE_DESTRUCTIVE_DELETE_PROTECTION_REVIEWED" in durability_attest
        and "VELORIX_OBJECT_STORE_COST_CONTROLS_REVIEWED" in durability_attest
        and "VELORIX_OBJECT_STORE_AUTHORITY_STORE_ID" in durability_attest
        and "--authority-store-id" in durability_attest
        and "--bucket" in durability_attest
        and "--s3-prefix" in durability_attest
        and "without product evidence" in durability_attest
        and "local development object-store authority cannot receive durability attestation"
        in durability_attest
        and "does not match product evidence" in durability_attest
        and "velorix_object_store_durability_policy_attestation" in durability_attest
        and "scripts/attest-object-store-durability-policy.sh" in doc
        and "--versioning-or-object-lock-enabled" in doc
        and "--destructive-delete-protection-reviewed" in doc
    ),
    "object-store durability completion wrapper attaches evidence without rerun": (
        "scripts/complete-vind-object-store-durability.sh" in doc
        and "scripts/assess-object-store-durability-policy.sh" in object_store_durability_complete
        and "scripts/attest-object-store-durability-policy.sh" in object_store_durability_complete
        and "scripts/attach-vind-object-store-durability.sh" in object_store_durability_complete
        and "--env-file" in object_store_durability_complete
        and "--output-dir" in object_store_durability_complete
        and "--input-evidence" in object_store_durability_complete
        and "--validate-only" in object_store_durability_complete
        and "source_env_file_preserving_overrides" in object_store_durability_complete
        and "product_dir_cli=1" in object_store_durability_complete
        and "input_evidence_cli=1" in object_store_durability_complete
        and 'input_evidence="${VELORIX_OBJECT_STORE_DURABILITY_INPUT_EVIDENCE:-${product_dir}/object-store-durability-input.json}"' in object_store_durability_complete
        and "velorix_object_store_durability_input" in object_store_durability_complete
        and "object-store-durability-input.json" in object_store_durability_complete
        and "invalid object-store durability inputs" in object_store_durability_complete
        and "creates_product_complete_evidence" in object_store_durability_complete
        and "VELORIX_OBJECT_STORE_DURABILITY_ASSESS" in object_store_durability_complete
        and "VELORIX_OBJECT_STORE_DURABILITY_ATTEST" in object_store_durability_complete
        and "VELORIX_OBJECT_STORE_DURABILITY_ATTACH" in object_store_durability_complete
        and "PersistentVolumeClaim" not in object_store_durability_complete
        and "--env-file target/velorix-product/complete-vind-product.env" in doc
        and "object-store-durability-input.json" in doc
        and "scripts/complete-vind-object-store-durability.sh"
        in product_completion_report
        and "--env-file target/velorix-product/complete-vind-product.env"
        in product_completion_report
        and "--validate-only" in product_completion_report
        and "scripts/attach-vind-object-store-durability.sh" in doc
        and "object_store.durability_policy_attestation" in doc
        and "product_complete_blockers" in doc
        and "velorix_object_store_durability_policy_attestation" in object_store_durability_attach
        and "object-store-durability-attestation.json" in object_store_durability_attach
        and '"validated": True' in object_store_durability_attach
        and "external S3-compatible authority lacks operator-reviewed durability policy attestation"
        in object_store_durability_attach
        and "product.get(\"product_complete\") is True" in object_store_durability_attach
        and not (
            'product["product_complete"] = len(product.get("product_complete_blockers", [])) == 0'
            in object_store_durability_attach
        )
        and "external_s3_bucket_validated" in object_store_durability_attach
        and "external_s3_prefix_validated" in object_store_durability_attach
        and "local development object-store authority cannot receive durability attestation"
        in object_store_durability_attach
        and "scripts/report-vind-product-completion.sh" in object_store_durability_attach
        and "PersistentVolumeClaim" not in object_store_durability_attach
    ),
    "assesses object-store durability without forging product-complete attestation": (
        "velorix_object_store_durability_policy_assessment" in durability_assess
        and "object-store-durability-assessment.json" in durability_assess
        and "trusted_for_product_complete" in durability_assess
        and "authority_class" in durability_assess
        and "local_single_node_docker_volume" in durability_assess
        and '"can_generate_product_complete_attestation": not missing' in durability_assess
        and "Do not mark missing fields true" in durability_assess
        and "object-store-durability-assessment.json" in doc
        and "does not create product-complete evidence" in doc
    ),
    "records admin auth rejection in product evidence": (
        "api_auth_data_plane_token_rejected_on_admin_route=1" in script
        and '"data_plane_token_rejected_on_admin_route": api_auth_data_plane_token_rejected_on_admin_route == "1"'
        in script
    ),
    "records and validates explicit ingress admin token acceptance": (
        '"admin_token_accepted_on_admin_route": True' in attest
        and '"admin_token_accepted_on_admin_route"' in script
        and '"/admin_token_accepted_on_admin_route"' in ingress_validator
        and "validate_recent_ingress_tls_auth_attested_at" in ingress_validator
        and '"admin_route_missing_token_rejected": True' in attest
        and '"admin_route_wrong_token_rejected": True' in attest
        and '"data_plane_token_rejected_on_admin_catalog_route": True' in attest
        and '"/admin_route_missing_token_rejected"' in ingress_validator
        and '"/admin_route_wrong_token_rejected"' in ingress_validator
        and '"/data_plane_token_rejected_on_admin_catalog_route"' in ingress_validator
        and '"admin_token_accepted_on_admin_route": true' in doc
        and '"admin_route_missing_token_rejected": true' in doc
    ),
    "manual usage prints admin catalog route": (
        'curl "$VELORIX_API_URL/v1/view-compile-deploy/jobs" -H "$VELORIX_ADMIN_AUTH_HEADER"'
        in script
    ),
    "final held REST port-forward reattaches to writer-owner pod": (
        "VELORIX_API_FINAL_OWNER_AWARE_ATTACH" in script
        and "attach_final_rest_to_writer_owner()" in script
        and "VELORIX_API_ATTACH_BACKGROUND=1" in script
        and "VELORIX_API_ATTACH_WRITER_OWNER=1" in script
        and "rest_attach_evidence=" in script
        and "VELORIX_API_ATTACH_BACKGROUND=0" in attach_rest
        and "writer-owner-acquire-" in attach_rest
        and "deletionTimestamp" in attach_rest
        and "tmux new-session" in attach_rest
        and "port-forward.attach.tmux-session" in attach_rest
        and "nohup kubectl" in attach_rest
        and 'if [ "$hold" = "1" ] && [ "$background" != "1" ]' in attach_rest
        and "VELORIX_API_FINAL_OWNER_AWARE_ATTACH=0" in doc
        and "VELORIX_API_ATTACH_BACKGROUND=1" in doc
        and "tmux" in doc
    ),
    "first-E2E validates compile/deploy evidence": (
        'compile_deploy = api.get("compile_deploy") or {}' in first_e2e
        and 'compile_deploy.get("job_catalog_verified") is not True' in first_e2e
        and 'require_sibling_evidence_file(product_path, "view-compile-deploy-jobs.json", "product compile/deploy job evidence")'
        in first_e2e
        and 'compile_deploy.get("worker_run_verified") is not True' in first_e2e
        and 'view-compile-deploy-run-once.json' in first_e2e
        and 'pending-scores-view-after-compile-deploy.json' in first_e2e
        and 'pending-scores-query-after-compile-deploy.json' in first_e2e
    ),
    "first-E2E validates no-PVC sibling evidence": (
        'no_pvc = product.get("no_pvc") or {}' in first_e2e
        and 'no_pvc.get("namespace_validated") is not True' in first_e2e
        and 'require_sibling_evidence_file(product_path, "no-pvc-namespace.json", "product no-PVC namespace evidence")'
        in first_e2e
    ),
    "first-E2E validates product ingest-writer append evidence": (
        'product_ingest_writer_files = product_ingest_writer.get("evidence_files") or {}'
        in first_e2e
        and '"ingest-writer-job-log.json"' in first_e2e
        and 'require_sibling_evidence_file(product_path, expected, "product ingest-writer append evidence")'
        in first_e2e
    ),
    "release validator validates compile/deploy evidence": (
        '"/api/compile_deploy/job_catalog_verified"' in cli
        and '"view-compile-deploy-jobs.json"' in cli
        and '"product compile/deploy job evidence"' in cli
        and '"/api/compile_deploy/worker_run_verified"' in cli
        and '"view-compile-deploy-run-once.json"' in cli
        and '"pending-scores-view-after-compile-deploy.json"' in cli
        and '"pending-scores-query-after-compile-deploy.json"' in cli
    ),
    "release validator validates no-PVC sibling evidence": (
        '"/no_pvc/namespace_validated"' in cli
        and '"/no_pvc/evidence"' in cli
        and '"no-pvc-namespace.json"' in cli
        and '"product no-PVC namespace evidence"' in cli
        and "validate_product_no_pvc_namespace_sibling" in cli
        and "readiness_report_rejects_release_product_evidence_with_pvc_in_no_pvc_namespace_sibling"
        in cli
    ),
    "release validator validates Hiqlite authority sibling evidence": (
        '"/metadata_store/hiqlite_authority_attestation"' in cli
        and 'format!("{prefix}/evidence")' in cli
        and '"hiqlite-authority-attestation.json"' in cli
        and '"product Hiqlite authority evidence"' in cli
        and "validate_product_hiqlite_authority_sibling" in cli
        and "validate_product_managed_hiqlite_no_pvc_siblings" in cli
        and '"namespace_pvc_list"' in cli
        and '"no-pvc-hiqlite-statefulset.json"' in cli
        and '"velorix-hiqlite.yaml"' in cli
        and "readiness_report_rejects_hiqlite_authority_with_mismatched_sibling_evidence"
        in cli
        and "readiness_report_rejects_managed_hiqlite_authority_with_pvc_statefulset_sibling"
        in cli
    ),
    "release validator validates Hiqlite backend-time evidence": (
        '"/metadata_store/hiqlite_backend_time_attestation"' in cli
        and '"hiqlite-backend-time-attestation.json"' in cli
        and '"product Hiqlite backend-time evidence"' in cli
        and '"authority_sampled_unix_time_ms_in_raft_operation"' in cli
        and '"metrics_time_source_rejected"' in cli
        and '"raft_log_index_time_source_rejected"' in cli
        and '"distributed_lock_ttl_source_rejected"' in cli
        and "read_sibling_json_artifact" in cli
        and "does not match {summary_pointer}" in cli
        and '"/evidence_files"' in cli
        and "sha256 mismatch" in cli
        and "standing-runtime failover smoke observed_failover_ms does not match"
        in cli
        and "metadata adversarial smoke log missing" in cli
        and "HIQLITE_BACKEND_TIME_ALLOWED_ATTESTERS" in cli
        and "validate_recent_hiqlite_backend_time_attested_at" in cli
        and "attester is not allowlisted" in cli
        and "parse_product_standing_runtime_fencing_capability" in cli
        and "validate_release_standing_runtime_fencing_capability" in cli
        and "standing-runtime capability schema is invalid" in cli
        and "validate_product_hiqlite_backend_time_trusted_provenance" in cli
        and "validate_full_git_commit_sha" in cli
        and "requires --release-commit" in cli
        and "source_revision does not match release_commit" in cli
        and "HIQLITE_BACKEND_TIME_REQUIRED_SUBJECT_IMAGE_ROLES" in cli
        and "missing array /subject_images" in cli
        and "missing required role {required_role}" in cli
        and "subject_images hiqlite-authority image_digest does not match" in cli
        and "validate_product_deployed_image_evidence" in cli
        and "subject_images {role} image_digest does not match product deployed image evidence" in cli
        and '"deployed_images": deployed_images' in script
        and "VELORIX_API_IMAGE_DIGEST" in script
        and "VELORIX_META_IMAGE_DIGEST" in script
        and "velorix-api-deployment-observed.json" in script
        and "velorix-meta-deployment-observed.json" in script
        and "validate_hiqlite_backend_time_failover_evidence" in cli
        and "release_bounded_wall_clock_failover" in cli
        and "release_ci_deployed_product" in cli
        and "authority_time_observed" in cli
        and "HIQLITE_BACKEND_TIME_TRUSTED_PROVENANCE_KIND" in cli
        and "canonical_bundle_sha256 mismatch" in cli
        and "requires trusted CI provenance" in cli
        and "Ed25519 signature is verified" in cli
        and "full Sigstore certificate-chain and transparency-log verification" in cli
        and "validate_hiqlite_backend_time_ci_identity" in cli
        and "validate_hiqlite_backend_time_trusted_release_ref" in cli
        and "refs/heads/main or refs/tags/v*" in cli
        and "VELORIX_CI_WORKFLOW_REF" in backend_time_attest
        and "require_trusted_release_ref" in backend_time_attest
        and "must use refs/heads/main or refs/tags/v*" in backend_time_attest
        and "github_actions_oidc" in cli
        and "token.actions.githubusercontent.com" in cli
        and "job_workflow_ref does not match release_commit" in cli
        and "validate_hiqlite_backend_time_signature_bundle" in cli
        and "sigstore_rekor_dsse" in cli
        and "signature_algorithm is unsupported" in cli
        and "public_key_sha256 does not match public_key_base64" in cli
        and "Ed25519 signature verification failed" in cli
        and "validate_hiqlite_backend_time_sigstore_bundle" in cli
        and "sigstore_bundle_sha256 does not match sigstore_bundle_base64" in cli
        and "Sigstore bundle verification failed" in cli
        and "verified Rekor integrated time is missing" in cli
        and "signed_payload_sha256 does not match canonical_bundle_sha256" in cli
        and "subject_image_digest does not match" in cli
        and "velorix_ci_evidence_bundle_provenance" in cli
        and "trusted CI provenance over the canonical backend-time evidence bundle"
        in cli
    ),
    "Hiqlite backend-time assessment detects authority-time support": (
        (repo_root / "scripts" / "assess-hiqlite-backend-time.sh").is_file()
        and '"velorix_hiqlite_backend_time_assessment"'
        in (repo_root / "scripts" / "assess-hiqlite-backend-time.sh").read_text()
        and '"required_mode_supported": required_mode_supported'
        in (repo_root / "scripts" / "assess-hiqlite-backend-time.sh").read_text()
        and '"can_generate_product_complete_backend_time_attestation": required_mode_supported'
        in (repo_root / "scripts" / "assess-hiqlite-backend-time.sh").read_text()
        and '"authority_time_transaction_api": authority_time_transaction_api'
        in (repo_root / "scripts" / "assess-hiqlite-backend-time.sh").read_text()
        and '"authority_unix_ms_transaction_param": authority_unix_ms_param'
        in (repo_root / "scripts" / "assess-hiqlite-backend-time.sh").read_text()
        and '"raft_replicated_authority_time_payload": raft_replicated_authority_time_payload'
        in (repo_root / "scripts" / "assess-hiqlite-backend-time.sh").read_text()
        and '"owner_read_uses_authority_time": owner_read_uses_authority_time'
        in (repo_root / "scripts" / "assess-hiqlite-backend-time.sh").read_text()
        and '"checkpoint_publish_insert_uses_authority_time": checkpoint_publish_insert_uses_authority_time'
        in (repo_root / "scripts" / "assess-hiqlite-backend-time.sh").read_text()
        and '"unsafe_runtime_time_sources_absent": unsafe_runtime_sources_absent'
        in (repo_root / "scripts" / "assess-hiqlite-backend-time.sh").read_text()
        and "VELORIX_HIQLITE_BACKEND_TIME_UPDATE_PRODUCT_EVIDENCE"
        in (repo_root / "scripts" / "assess-hiqlite-backend-time.sh").read_text()
        and '"hiqlite_backend_time_assessment"'
        in (repo_root / "scripts" / "assess-hiqlite-backend-time.sh").read_text()
        and '"trusted_for_product_complete": False'
        in (repo_root / "scripts" / "assess-hiqlite-backend-time.sh").read_text()
        and "VELORIX_HIQLITE_BACKEND_TIME_ASSESS"
        in (repo_root / "scripts" / "run-vind-product.sh").read_text()
        and "run_hiqlite_backend_time_assessment"
        in (repo_root / "scripts" / "run-vind-product.sh").read_text()
        and "VELORIX_REQUIRE_HIQLITE_BACKEND_TIME"
        in (repo_root / "docs" / "development" / "vind-product.md").read_text()
        and "metadata_store.hiqlite_backend_time_assessment"
        in (repo_root / "docs" / "development" / "vind-product.md").read_text()
        and "scripts/assess-hiqlite-backend-time.sh"
        in (repo_root / "docs" / "architecture" / "hiqlite-meta-service.md").read_text()
        and "trusted_for_product_complete=false"
        in (repo_root / "docs" / "architecture" / "hiqlite-meta-service.md").read_text()
    ),
    "Hiqlite backend-time attestation candidate binds deployed smoke evidence": (
        (repo_root / "scripts" / "attest-hiqlite-backend-time.sh").is_file()
        and '"velorix_hiqlite_backend_time_attestation"' in backend_time_attest
        and "hiqlite-backend-time-assessment.json" in backend_time_attest
        and "readyz.json" in backend_time_attest
        and "multi-replica-fencing-smoke.json" in backend_time_attest
        and "standing-runtime-failover-smoke.json" in backend_time_attest
        and "velorix-meta-smoke.log" in backend_time_attest
        and "observed_failover_ms" in backend_time_attest
        and "trusted_for_release_validator" in backend_time_attest
        and "VELORIX_HIQLITE_BACKEND_TIME_TRUSTED_PROVENANCE" in backend_time_attest
        and "VELORIX_RELEASE_COMMIT" in backend_time_attest
        and "VELORIX_API_IMAGE_DIGEST" in backend_time_attest
        and "VELORIX_META_IMAGE_DIGEST" in backend_time_attest
        and "VELORIX_HIQLITE_IMAGE_DIGEST" in backend_time_attest
        and "VELORIX_CI_OIDC_SUBJECT" in backend_time_attest
        and "VELORIX_CI_WORKFLOW_REF" in backend_time_attest
        and "VELORIX_CI_JOB_WORKFLOW_REF" in backend_time_attest
        and "VELORIX_CI_SIGNING_CERTIFICATE_SHA256" in backend_time_attest
        and "VELORIX_CI_PUBLIC_KEY_BASE64" in backend_time_attest
        and "VELORIX_CI_PUBLIC_KEY_SHA256" in backend_time_attest
        and "VELORIX_CI_SIGNATURE_BASE64" in backend_time_attest
        and "VELORIX_CI_SIGSTORE_BUNDLE_BASE64" in backend_time_attest
        and "VELORIX_CI_SIGSTORE_BUNDLE_SHA256" in backend_time_attest
        and "VELORIX_CI_SIGSTORE_CERTIFICATE_IDENTITY" in backend_time_attest
        and "VELORIX_HIQLITE_BACKEND_TIME_CANONICAL_BUNDLE_FILE" in backend_time_attest
        and "VELORIX_CI_TRANSPARENCY_LOG_ID" in backend_time_attest
        and "VELORIX_CI_TRANSPARENCY_LOG_INDEX" in backend_time_attest
        and "VELORIX_CI_INCLUSION_PROOF_SHA256" in backend_time_attest
        and "release_failover_shape" in backend_time_attest
        and "attestation_origin" in backend_time_attest
        and "diagnostic_release_failover_included" in backend_time_attest
        and "trusted backend-time provenance requires release-shaped failover evidence" in backend_time_attest
        and '"subject_images": subject_images' in backend_time_attest
        and '"ci_identity": {' in backend_time_attest
        and '"signature_bundle": signature_bundle' in backend_time_attest
        and "require_full_git_sha" in backend_time_attest
        and "full_git_sha_or_empty" in backend_time_attest
        and 'source_repository = require_env("VELORIX_SOURCE_REPOSITORY")' in backend_time_attest
        and "VELORIX_SOURCE_REPOSITORY must be github.com/mrchypark/velorix" in backend_time_attest
        and 'source_revision = require_env("VELORIX_SOURCE_REVISION")' in backend_time_attest
        and 'authority.get("source_revision") or ""' in backend_time_attest
        and 'os.environ.get("VELORIX_SOURCE_REVISION", "").strip() or str(\n        authority.get("source_revision") or ""' not in backend_time_attest
        and "must be the Velorix release commit, not metadata_store.hiqlite_authority_attestation.source_revision"
        in backend_time_attest
        and "source revision must match VELORIX_RELEASE_COMMIT" in backend_time_attest
        and "velorix_ci_evidence_bundle_provenance" in backend_time_attest
        and "canonical_bundle_sha256" in backend_time_attest
        and "canonical_bundle_entries" in backend_time_attest
        and "without_metadata_store_hiqlite_backend_time_attestation" in backend_time_attest
        and "without_metadata_store_hiqlite_backend_time_attestation" in cli
        and "release_bounded_wall_clock_failover" in backend_time_attest
        and "release_ci_deployed_product" in backend_time_attest
        and "authority_time_observed" in backend_time_attest
        and "trusted_for_product_complete = False" in backend_time_attest
        and "release_validator_fail_closed" in backend_time_attest
        and "VELORIX_HIQLITE_BACKEND_TIME_ATTESTATION_UPDATE_PRODUCT_EVIDENCE"
        in backend_time_attest
        and "hiqlite_backend_time_attestation" in backend_time_attest
        and "VELORIX_HIQLITE_BACKEND_TIME_ATTEST" in script
        and "run_hiqlite_backend_time_attestation" in script
        and "VELORIX_HIQLITE_BACKEND_TIME_ATTESTATION_UPDATE_PRODUCT_EVIDENCE=1"
        in script
        and re.search(
            r"run_hiqlite_backend_time_assessment\s+run_hiqlite_backend_time_attestation\s+write_product_evidence",
            script,
            re.S,
        )
        and '"hiqlite_backend_time_attestation": hiqlite_backend_time_attestation'
        in script
        and "Hiqlite backend-time attestation is diagnostic and release validator remains fail-closed"
        in script
        and "scripts/attest-hiqlite-backend-time.sh" in doc
        and "scripts/write-hiqlite-backend-time-release-env.sh" in doc
        and "scripts/check-hiqlite-backend-time-release-inputs.sh" in doc
        and "--env-file target/velorix-product/hiqlite-backend-time-release.env"
        in doc
        and "velorix_hiqlite_backend_time_release_preflight" in backend_time_release_preflight
        and "--env-file PATH" in backend_time_release_preflight
        and "VELORIX_HIQLITE_BACKEND_TIME_RELEASE_ENV_FILE" in backend_time_release_preflight
        and "product_evidence_explicit" in backend_time_release_preflight
        and "output_file_explicit" in backend_time_release_preflight
        and "source_env_file_preserving_overrides" in backend_time_release_preflight
        and "VELORIX_CI_SIGSTORE_BUNDLE_BASE64" in backend_time_release_preflight
        and "VELORIX_STANDING_RUNTIME_FAILOVER_RELEASE_ATTEST" in backend_time_release_preflight
        and "Refusing to run with shell xtrace enabled because release/Sigstore inputs may be logged"
        in backend_time_release_preflight
        and "velorix_hiqlite_backend_time_release_env_template" in backend_time_release_env
        and "scripts/check-hiqlite-backend-time-release-inputs.sh --env-file"
        in backend_time_release_env
        and "hiqlite-backend-time-release.env" in backend_time_release_env
        and "hiqlite-backend-time-release-env.json" in backend_time_release_env
        and "VELORIX_API_IMAGE_DIGEST" in backend_time_release_env
        and "VELORIX_META_IMAGE_DIGEST" in backend_time_release_env
        and "VELORIX_HIQLITE_IMAGE_DIGEST" in backend_time_release_env
        and '"VELORIX_SOURCE_REPOSITORY": "github.com/mrchypark/velorix"' in backend_time_release_env
        and '"fixed_release_values": fixed_release_values' in backend_time_release_env
        and "GITHUB_SHA" in backend_time_release_env
        and "hiqlite_authority_source_revision" in backend_time_release_env
        and 'source_revision = require_git_sha(\n    "VELORIX_SOURCE_REVISION",\n    require_env("VELORIX_SOURCE_REVISION"),\n    allow_missing=True,\n)' in backend_time_release_preflight
        and 'source_revision = env("VELORIX_SOURCE_REVISION") or authority_source_revision'
        not in backend_time_release_preflight
        and "must be the Velorix release commit, not metadata_store.hiqlite_authority_attestation.source_revision"
        in backend_time_release_preflight
        and "VELORIX_SOURCE_REPOSITORY must be github.com/mrchypark/velorix"
        in backend_time_release_preflight
        and "authority_source_revision_sha" in backend_time_release_preflight
        and "trusted_workflow_ref_prefix" in backend_time_release_preflight
        and "VELORIX_CI_OIDC_SUBJECT must match trusted release workflow ref"
        in backend_time_release_preflight
        and "VELORIX_CI_JOB_WORKFLOW_REF must match VELORIX_RELEASE_COMMIT"
        in backend_time_release_preflight
        and "VELORIX_CI_SIGSTORE_CERTIFICATE_IDENTITY must match trusted release workflow ref"
        in backend_time_release_preflight
        and "ci_identity.workflow_ref" in backend_time_release_preflight
        and "ci_identity.sigstore_certificate_identity" in backend_time_release_preflight
        and "never from the\nHiqlite authority source revision" in doc
        and "rejects a 40-character Hiqlite authority revision" in doc
        and "attestation\ngenerator enforces the same boundary" in doc
        and "preflight and attestation generation reject any other repository value" in doc
        and "VELORIX_CI_JOB_WORKFLOW_REF` is pinned to `VELORIX_RELEASE_COMMIT" in doc
        and "VELORIX_CI_SIGSTORE_CERTIFICATE_IDENTITY` names the same trusted workflow ref" in doc
        and "still contains a REPLACE_WITH placeholder" in backend_time_release_preflight
        and '"placeholder": "REPLACE_WITH" in value' in backend_time_release_preflight
        and "still contains a\n`REPLACE_WITH_*` placeholder" in doc
        and "VELORIX_CI_SIGSTORE_BUNDLE_BASE64" in backend_time_release_env
        and "REPLACE_WITH_SIGSTORE_BUNDLE_BASE64" in backend_time_release_env
        and "refs/heads/main" in backend_time_release_env
        and "refs/tags/v" in backend_time_release_env
        and "does not create release provenance" in backend_time_release_env
        and 'VELORIX_SOURCE_REPOSITORY="github.com/mrchypark/velorix"' in release_gate
        and "VELORIX_CI_SIGSTORE_BUNDLE_BASE64 is required for product-complete release readiness"
        in backend_time_release_preflight
        and 'require_sha(\n    "VELORIX_CI_SIGSTORE_BUNDLE_SHA256"'
        in backend_time_release_preflight
        and '"sha256:" + hashlib.sha256(sigstore_bundle_bytes).hexdigest()'
        in backend_time_release_preflight
        and "VELORIX_CI_SIGSTORE_BUNDLE_SHA256 must match VELORIX_CI_SIGSTORE_BUNDLE_BASE64"
        in backend_time_release_preflight
        and "Sigstore bundle missing verificationMaterial" in backend_time_release_preflight
        and "Sigstore bundle missing signing certificate rawBytes" in backend_time_release_preflight
        and "Sigstore bundle missing Rekor tlogEntries" in backend_time_release_preflight
        and "Sigstore bundle missing tlogEntries[0].inclusionProof" in backend_time_release_preflight
        and 'allow_missing=False,\n)' in backend_time_release_preflight
        and 'sigstore_bundle_sha256 = require_env("VELORIX_CI_SIGSTORE_BUNDLE_SHA256")'
        in backend_time_attest
        and 'or f"sha256:{hashlib.sha256(sigstore_bundle_bytes).hexdigest()}"'
        not in backend_time_attest
        and '"VELORIX_CI_SIGSTORE_BUNDLE_SHA256": "sha256:REPLACE_WITH_SIGSTORE_BUNDLE_SHA256"'
        in backend_time_release_env
        and "failover evidence requires evidence_scope='release_ci_deployed_product'" in backend_time_release_preflight
        and "scripts/check-hiqlite-backend-time-release-inputs.sh" in product_completion_report
        and "VELORIX_HIQLITE_BACKEND_TIME_TRUSTED_PROVENANCE=1" in doc
        and "--update-product-evidence" in product_completion_report
        and "Regenerate Hiqlite backend-time attestation in release CI" in product_completion_report
        and "trusted_for_release_validator" in product_completion_report
        and "release_validator_fail_closed" in product_completion_report
        and "scripts/attest-hiqlite-backend-time.sh"
        in (repo_root / "docs" / "architecture" / "hiqlite-meta-service.md").read_text()
    ),
    "managed Hiqlite authority evidence records live local source revision": (
        '"source_revision": source_revision' in script
        and 'git -C "$hiqlite_local_source_dir" rev-parse --short HEAD' in script
        and 'git -C "$hiqlite_local_source_dir" status --porcelain' in script
        and 'hiqlite_source_dirty="+dirty"' in script
        and '"source_revision": "mrchypark/hiqlite@b1dbcb3572558ac1fc09cc1eac080a5578600452"' in cli
        and '"source_revision": "sebadob/hiqlite@3e2112c"' not in script
        and '"source_revision": "sebadob/hiqlite@abcdefabcdefabcdefabcdefabcdefabcdefabcd"'
        not in cli
    ),
    "release readiness treats Feldera artifact hash as optional diagnostic": (
        "release gate requires inputs.feldera-spec-path" not in release_gate
        and "release gate requires inputs.feldera-metadata-path" not in release_gate
        and "release gate requires inputs.feldera-artifact-package-path" not in release_gate
        and "Feldera artifact hash verification is optional, but requires all three inputs when enabled" in release_gate
        and "FELDERA_READINESS_ARGS=()" in release_gate
        and '"${FELDERA_READINESS_ARGS[@]}"' in release_gate
        and "--feldera-artifact-hash-evidence target/release-evidence/feldera-artifact-hash.json --s3-release-benchmark-gate-evidence" not in release_doc
        and "Optional release diagnostic evidence with `evidence_kind=feldera_artifact_hash_verified`" in release_doc
        and "This optional diagnostic is not a\n  product-completion blocker when omitted" in release_doc
        and "readiness-report --require-release-artifacts requires --feldera-artifact-hash-evidence" not in cli
    ),
    "multi-replica product smoke attaches to writer owner before writes": (
        "start_api_writer_owner_port_forward_for_smoke()" in script
        and "smoke-owner-rest-attach.json" in script
        and "multi-replica fenced product smoke writes must target the standing-runtime writer owner"
        in script
        and re.search(
            r"positive-scores-view\.json.*?start_api_writer_owner_port_forward_for_smoke",
            script,
            re.S,
        )
    ),
    "local standing-runtime failover smoke stays explicitly non-product-complete": (
        (repo_root / "scripts" / "smoke-vind-standing-runtime-failover.sh").is_file()
        and (repo_root / "scripts" / "write-standing-runtime-failover-evidence.py").is_file()
        and "velorix_standing_runtime_failover_smoke"
        in failover_evidence_writer
        and "VELORIX_STANDING_RUNTIME_FAILOVER_UPDATE_PRODUCT_EVIDENCE"
        in (repo_root / "scripts" / "smoke-vind-standing-runtime-failover.sh").read_text()
        and 'standing["local_api_pod_failover_smoke"]'
        in (repo_root / "scripts" / "smoke-vind-standing-runtime-failover.sh").read_text()
        and "scripts/write-standing-runtime-failover-evidence.py"
        in (repo_root / "scripts" / "smoke-vind-standing-runtime-failover.sh").read_text()
        and "VELORIX_STANDING_RUNTIME_FAILOVER_SMOKE"
        in (repo_root / "scripts" / "run-vind-product.sh").read_text()
        and "run_standing_runtime_failover_smoke()"
        in (repo_root / "scripts" / "run-vind-product.sh").read_text()
        and "local_api_pod_failover_smoke"
        in (repo_root / "scripts" / "run-vind-product.sh").read_text()
        and "VELORIX_STANDING_RUNTIME_FAILOVER_RELEASE_ATTEST"
        in (repo_root / "scripts" / "smoke-vind-standing-runtime-failover.sh").read_text()
        and 'release_attest="${VELORIX_STANDING_RUNTIME_FAILOVER_RELEASE_ATTEST:-0}"'
        in (repo_root / "scripts" / "smoke-vind-standing-runtime-failover.sh").read_text()
        and '"trusted_for_product_complete": release_attest_enabled'
        in failover_evidence_writer
        and '"evidence_scope": "release_ci_deployed_product"'
        in failover_evidence_writer
        and '"failover_probe_kind": "release_bounded_wall_clock_failover"'
        in failover_evidence_writer
        and '"authority_time_observed": authority_time_observed'
        in failover_evidence_writer
        and '"owner_ttl_ms": owner_ttl_ms'
        in failover_evidence_writer
        and '"failover_time_bound_ms": failover_time_bound_ms'
        in failover_evidence_writer
        and '"pre_failover_owner_epoch": pre_failover_owner_epoch'
        in failover_evidence_writer
        and '"post_failover_owner_epoch": post_failover_owner_epoch'
        in failover_evidence_writer
        and '"affected_api_pods": affected_api_pods'
        in failover_evidence_writer
        and "production_wall_clock_failover_attestation"
        in failover_evidence_writer
        and "scripts/smoke-vind-standing-runtime-failover.sh" in doc
        and "VELORIX_STANDING_RUNTIME_FAILOVER_SMOKE" in doc
        and "VELORIX_STANDING_RUNTIME_FAILOVER_UPDATE_PRODUCT_EVIDENCE=0" in doc
        and "local_api_pod_failover_smoke.status=pass" in doc
        and "deliberately not" in doc
        and "product-complete wall-clock failover evidence" in doc
        and "VELORIX_STANDING_RUNTIME_FAILOVER_RELEASE_ATTEST=1" in doc
    ),
    "release validator validates ingress/TLS/auth sibling evidence": (
        '"/api/auth/ingress_tls_auth_attestation"' in cli
        and 'format!("{attestation}/evidence")' in cli
        and '"ingress-tls-auth-attestation.json"' in cli
        and '"product ingress/TLS/auth evidence"' in cli
        and "validate_product_ingress_tls_auth_sibling" in cli
        and "read_sibling_json_artifact(path, evidence_filename, label)" in cli
        and "readiness_report_rejects_release_product_evidence_with_malformed_ingress_tls_auth_sibling"
        in cli
        and "readiness_report_rejects_release_product_evidence_with_mismatched_ingress_tls_auth_sibling"
        in cli
    ),
    "release validator validates product ingest-writer append evidence": (
        '"/ingest_writer/evidence_files/{pointer}"' in cli
        and '"ingest-writer-job-log.json"' in cli
        and '"ingest-writer-job.json"' in cli
        and '"ingest-writer-pods.json"' in cli
        and '"product ingest-writer append evidence"' in cli
    ),
    "first-E2E validates product API auth": (
        'auth = api.get("auth") or {}' in first_e2e
        and 'auth.get("mode") != "bearer-token"' in first_e2e
        and 'data_plane_token_rejected_on_admin_route' in first_e2e
        and 'local_tls.get("passed") is not True' in first_e2e
        and 'require_sibling_evidence_file(product_path, "tls-auth-smoke.json", "product local TLS/auth evidence")'
        in first_e2e
    ),
    "release validator validates product API auth": (
        "fn validate_product_api_auth_evidence" in cli
        and '"/api/auth/data_plane_token_rejected_on_admin_route"' in cli
        and '"velorix-admin-auth"' in cli
        and '"/api/auth/local_tls_auth_smoke/passed"' in cli
        and '"tls-auth-smoke.json"' in cli
    ),
    "release copy validates OpenAPI and local TLS evidence filenames": (
        "product openapi.evidence_file must be openapi.json" in release_copy
        and "product local_tls_auth_smoke.evidence must be tls-auth-smoke.json"
        in release_copy
        and '"run_once_evidence_file": "view-compile-deploy-run-once.json"' in release_copy
        and "pending-scores-view-after-compile-deploy.json" in release_copy
        and "pending-scores-query-after-compile-deploy.json" in release_copy
    ),
    "documentation describes product evidence field": (
        "view-compile-deploy-jobs.json" in doc
        and "view-compile-deploy-run-once.json" in doc
        and "pending-scores-view-after-compile-deploy.json" in doc
        and "pending-scores-query-after-compile-deploy.json" in doc
        and "api.compile_deploy.job_catalog_verified" in doc
        and "api.compile_deploy.worker_run_verified" in doc
        and "data_plane_token_rejected_on_admin_route=true" in doc
        and "api.auth.local_tls_auth_smoke.passed=true" in doc
        and "no-pvc-namespace.json" in doc
        and "hiqlite-authority-attestation.json" in doc
        and "ingress-tls-auth-attestation.json" in doc
        and "ingest-writer-job-log.json" in doc
        and "scripts/run-vind-product-external-rustfs.sh" in doc
        and "external-rustfs.env" in doc
    ),
    "REST attach prefers standing-runtime writer owner": (
        '"/v1/standing-runtime/owners"' in (repo_root / "crates" / "velorix-api" / "src" / "lib.rs").read_text()
        and "current_owner_matches_local_process" in (repo_root / "crates" / "velorix-api" / "src" / "lib.rs").read_text()
        and "VELORIX_API_ATTACH_WRITER_OWNER" in (repo_root / "scripts" / "attach-vind-product-rest.sh").read_text()
        and "port_forward_target" in (repo_root / "scripts" / "attach-vind-product-rest.sh").read_text()
        and "openapi.attach.json" in (repo_root / "scripts" / "attach-vind-product-rest.sh").read_text()
        and "protected_openapi_passed" in (repo_root / "scripts" / "attach-vind-product-rest.sh").read_text()
        and "secret_value()" in (repo_root / "scripts" / "attach-vind-product-rest.sh").read_text()
        and "velorix-api-auth bearer-token" in (repo_root / "scripts" / "attach-vind-product-rest.sh").read_text()
        and "VELORIX_API_BEARER_TOKEN" in (repo_root / "scripts" / "attach-vind-product-rest.sh").read_text()
        and "GET /v1/standing-runtime/owners" in doc
    ),
    "existing product REST API smoke is executable and documented": (
        "VELORIX_REST_API_SMOKE_ATTACH" in rest_api_smoke
        and "scripts/attach-vind-product-rest.sh" in rest_api_smoke
        and "VELORIX_REST_API_SMOKE_DIR" in rest_api_smoke
        and "rest-api-smoke.json" in rest_api_smoke
        and "trusted_for_product_complete" in rest_api_smoke
        and "False" in rest_api_smoke
        and "openapi-auth-precheck.json" in rest_api_smoke
        and "authenticated REST API is not reachable after reattach" in rest_api_smoke
        and "VELORIX_API_URL/v1/relations/scores-default" in rest_api_smoke
        and "VELORIX_API_URL/v1/query-policies/interactive" in rest_api_smoke
        and "VELORIX_API_URL/v1/views/positive_scores_by_user" in rest_api_smoke
        and "VELORIX_API_URL/v1/query" in rest_api_smoke
        and "generic_query_disabled" in rest_api_smoke
        and "VELORIX_API_URL/v1/standing-runtime/owners" in rest_api_smoke
        and "VELORIX_API_URL/v1/ingest" in rest_api_smoke
        and "VELORIX_API_URL/v1/views/positive_scores_by_user/query?max_rows=1000" in rest_api_smoke
        and "VELORIX_API_URL/v1/api/scores/positive?max_rows=1000" in rest_api_smoke
        and "VELORIX_API_URL/v1/openapi.json" in rest_api_smoke
        and "mktemp" not in rest_api_smoke
        and "PersistentVolumeClaim" not in rest_api_smoke
        and "scripts/smoke-vind-rest-api.sh" in doc
        and "target/velorix-product-external-rustfs-corrected" in doc
        and "REST E2E check" in doc
    ),
    "product run wires existing-product REST API smoke into default authenticated path": (
        "VELORIX_VIND_REST_API_SMOKE" in script
        and "run_rest_api_smoke()" in script
        and 'if [ "$product_smoke" = "1" ] && [ "$api_auth_mode" = "bearer-token" ]; then'
        in script
        and "VELORIX_REST_API_SMOKE_ATTACH=0" in script
        and "scripts/smoke-vind-rest-api.sh" in script
        and "rest_api_smoke_status=\"pass\"" in script
        and "rest_api_smoke_evidence=${rest_api_smoke_evidence_file}" in script
        and "VELORIX_VIND_PRODUCT_DIR=${output_dir} scripts/smoke-vind-rest-api.sh" in script
        and "rest_api_smoke_status" in doc
        and "VELORIX_VIND_REST_API_SMOKE=0" in doc
    ),
    "product completion report summarizes blockers without forging evidence": (
        "velorix_product_completion_report" in product_completion_report
        and "product_complete_blockers" in product_completion_report
        and "completion_plan" in product_completion_report
        and '"derived_from": "report_gates"' in product_completion_report
        and "completion_plan_step(item)" in product_completion_report
        and "PLACEHOLDER_MARKERS" in product_completion_report
        and "action_has_placeholders" in product_completion_report
        and "input_summary_requires_input" in product_completion_report
        and "external_s3_required" in product_completion_report
        and "public_ingress_required" in product_completion_report
        and "tls_auth_boundary" in product_completion_report
        and "public_ingress_tls_auth" in product_completion_report
        and '"out_of_scope"' in product_completion_report
        and '"completion_scope"' in product_completion_report
        and "completion_scope_warnings" in product_completion_report
        and "public_ingress_tls_auth_out_of_scope_does_not_prove_public_dns_tls_or_external_client_reachability" in product_completion_report
        and "object_store_external_authority_out_of_scope_does_not_prove_object_store_durability" in product_completion_report
        and '"excluded_gates"' in product_completion_report
        and '"accepted_gate_statuses": ["pass", "out_of_scope"]' in product_completion_report
        and '"input_summary_requires_input": input_summary_has_required_input'
        in product_completion_report
        and "input_required_steps" in product_completion_report
        and "waiting_steps" in product_completion_report
        and "runnable_steps" in product_completion_report
        and "GATE_INPUT_MAP" in product_completion_report
        and '"placeholder_groups": ["public_ingress_tls_auth"]'
        in product_completion_report
        and '"placeholder_groups": ["external_s3"]' in product_completion_report
        and '"placeholder_groups": ["object_store_durability_review"]'
        in product_completion_report
        and '"placeholder_groups": ["release_identity", "sigstore_provenance"]'
        in product_completion_report
        and "input_summary_for_gate" in product_completion_report
        and "handoff_input_summary" in product_completion_report
        and "redacted_issues" in product_completion_report
        and '"env": step.get("env") or {}' in product_completion_report
        and "release_preflight_summary" in product_completion_report
        and '"release_preflight"' in product_completion_report
        and '"auth_token_source"' in product_completion_report
        and '"env_review_flags"' in product_completion_report
        and '"authority_ready"' in product_completion_report
        and '"authority"' in product_completion_report
        and '"hiqlite-backend-time-release-preflight.json"' in product_completion_report
        and '"input_summary"' in product_completion_report
        and '"missing": redacted_issues(step.get("missing") or [])'
        in product_completion_report
        and '"invalid": redacted_issues(step.get("invalid") or [])'
        in product_completion_report
        and "def issue_subjects(items):" in product_completion_report
        and '"missing_subjects": issue_subjects(step.get("missing") or [])'
        in product_completion_report
        and '"invalid_subjects": issue_subjects(step.get("invalid") or [])'
        in product_completion_report
        and '"forced_blocker_count": forced_blocker_counts.get(step_name, 0)'
        in product_completion_report
        and '"placeholder_count": len(set(placeholders))' in product_completion_report
        and '"secret_placeholder_count": len(set(secret_placeholders))'
        in product_completion_report
        and '"input_summary": handoff_input_summary()' in product_completion_report
        and '"secret_placeholders": sorted(set(secret_placeholders))'
        in product_completion_report
        and "complete_vind_product_env_handoff" in product_completion_report
        and "waiting_on_prerequisite" in product_completion_report
        and "input_required" in product_completion_report
        and "product_completion_source" in product_completion_report
        and '"derived_from": "report_gates"' in product_completion_report
        and "product_evidence_product_complete" in product_completion_report
        and "product_evidence_product_complete_blockers" in product_completion_report
        and "def gate_blocker(item):" in product_completion_report
        and '"gate": item.get("id")' in product_completion_report
        and 'if item.get("status") in {"blocked", "diagnostic", "missing"}'
        in product_completion_report
        and 'product_complete = all(item["status"] in {"pass", "out_of_scope"} for item in gates)'
        in product_completion_report
        and "next_actions" in product_completion_report
        and "hiqlite-backend-time-release-preflight.json" in product_completion_report
        and "hiqlite-backend-time-release-env.json" in product_completion_report
        and "complete-vind-product-input-preflight.json" in product_completion_report
        and "complete-vind-product-env.json" in product_completion_report
        and "complete-vind-product-plan.json" in product_completion_report
        and "complete_execution_plan_path" in product_completion_report
        and "complete_execution_plan = load_optional_json" in product_completion_report
        and "completion_execution_plan_summary" in product_completion_report
        and "step_summary[\"release_preflight\"] = release_summary" in product_completion_report
        and "release_preflight_status" in product_completion_report
        and "release_preflight_missing_subjects" in product_completion_report
        and "release_preflight_invalid_subjects" in product_completion_report
        and "def integer_or_zero(value):" in product_completion_report
        and '"completion_execution_plan": completion_execution_plan_summary()'
        in product_completion_report
        and '"will_run_steps"' in product_completion_report
        and '"blocked_steps"' in product_completion_report
        and '"waiting_steps"' in product_completion_report
        and '"creates_product_complete_evidence": False' in product_completion_report
        and "completion_handoff" in product_completion_report
        and "complete-vind-product.env" in product_completion_report
        and "scripts/write-complete-vind-product-env.sh" in product_completion_report
        and "VELORIX_COMPLETE_PRODUCT_DRY_RUN=1 scripts/complete-vind-product.sh "
        in product_completion_report
        and "--env-file target/velorix-product/complete-vind-product.env"
        in product_completion_report
        and "scripts/complete-vind-product.sh " in product_completion_report
        and "creates_product_complete_evidence" in product_completion_report
        and "fixed_release_values" in product_completion_report
        and "secret_placeholders" in product_completion_report
        and "placeholder_groups" in product_completion_report
        and "forced_blocker_count" in product_completion_report
        and "missing_count" in product_completion_report
        and "invalid_count" in product_completion_report
        and "placeholder_count" in product_completion_report
        and "--env-file target/velorix-product/hiqlite-backend-time-release.env"
        in product_completion_report
        and "public_ingress_tls_auth" in product_completion_report
        and "tls_auth_boundary" in product_completion_report
        and "scripts/complete-vind-product-ingress.sh" in product_completion_report
        and "object_store_durability_policy" in product_completion_report
        and "durability_attestation_issues" in product_completion_report
        and "durability_policy_attestation_invalid_subjects" in product_completion_report
        and "Attached object-store durability policy attestation does not match the external authority or required review flags" in product_completion_report
        and "--env-file target/velorix-product/complete-vind-product.env"
        in product_completion_report
        and "--output-dir target/velorix-product" in product_completion_report
        and "--validate-only" in product_completion_report
        and "scripts/run-vind-product-external-s3.sh" in product_completion_report
        and "Durability attestation is only accepted after the product slice is backed by a nonlocal external S3/OSS authority"
        in product_completion_report
        and "Product-complete durability policy attestation cannot be trusted before external object-store authority is proven"
        in product_completion_report
        and "staged_durability_attestation_summary" in product_completion_report
        and "object-store-durability-attestation.json" in product_completion_report
        and "staged_attestation" in product_completion_report
        and "requires_external_authority_before_attach" in product_completion_report
        and "creates_product_complete_evidence" in product_completion_report
        and 'blocked_by=None\n            if object_store_real_authority\n            else ["object_store_external_authority"]'
        in product_completion_report
        and "hiqlite_backend_time" in product_completion_report
        and "deployed_image_digests" in product_completion_report
        and "does not create product-complete evidence" in product_completion_report
        and "VELORIX_VIND_PRODUCT_COMPLETION_REPORT" in script
        and "run_product_completion_report()" in script
        and "scripts/report-vind-product-completion.sh" in script
        and "product_completion_report_status" in script
        and "product-completion-report.json" in doc
        and "`completion_execution_plan`" in doc
        and "`complete-vind-product-plan.json`" in doc
        and "`completion_plan` is\ngate-oriented product completion status" in doc
        and "scripts/next-vind-product-step.sh" in doc
        and "scripts/next-vind-product-step.sh --json" in doc
        and "scripts/next-vind-product-step.sh --fail-on-incomplete" in doc
        and "`completion_execution_plan.run_order`" in doc
        and "prefers external completion helpers over\nrepeatable local/report refresh helpers" in doc
        and "`missing_subjects`/`invalid_subjects`" in doc
        and "completion_handoff" in doc
        and "complete-vind-product-env.json" in doc
        and "derives\n`product_complete_blockers` from those non-passing in-scope gates"
        in doc
        and "product_completion_source.product_evidence_product_complete_blockers"
        in doc
        and "The same gate data is exposed as `completion_plan`" in doc
        and "in-scope gate is classified as `input_required`, `waiting_on_prerequisite`,\n`runnable`, or `blocked_without_action`"
        in doc
        and "any remaining placeholder group,\npreflight `missing`/`invalid` issue, or fail-closed release preflight issue keeps"
        in doc
        and "`input_required_steps`, `waiting_steps`, and `runnable_steps`"
        in doc
        and "Input-related plan steps also\ninclude `input_summary`" in doc
        and "including `secret_placeholders`" in doc
        and "redacted `missing` and `invalid` issue subjects/details" in doc
        and "`missing_subjects` and `invalid_subjects`" in doc
        and "redacted `release_preflight` summary" in doc
        and "`hiqlite-backend-time-release-preflight.json`" in doc
        and "placeholder_groups" in doc
        and "secret_placeholders" in doc
        and "report is diagnostic only" in doc
        and "does not create product-complete evidence" in doc.replace("\n", " ")
        and "only\nprints the durability attestation command after a nonlocal external S3/OSS\nauthority is already proven"
        in doc
        and "surfaces this staged file as\n`gates[].evidence.staged_attestation`" in doc
        and "never makes\n`product_complete=true` until the product slice itself proves the same nonlocal" in doc
        and '`blocked_by: ["object_store_external_authority"]`' in doc
        and "VELORIX_VIND_PRODUCT_COMPLETION_REPORT=0" in doc
    ),
    "top-level product completion driver sequences remaining gates": (
        "scripts/complete-vind-product.sh" in doc
        and (repo_root / "scripts" / "next-vind-product-step.sh").is_file()
        and "velorix_next_vind_product_step" in next_product_step
        and "EXECUTION_TO_GATE" in next_product_step
        and '"external_s3": "object_store_external_authority"' in next_product_step
        and '"ingress": "public_ingress_tls_auth"' in next_product_step
        and '"durability": "object_store_durability_policy"' in next_product_step
        and "completion_execution_plan" in next_product_step
        and "input_summary_requires_input" in next_product_step
        and "effective_gate_state" in next_product_step
        and 'gate.get("status") == "out_of_scope"' in next_product_step
        and "execution_step_requires_gate_input" in next_product_step
        and "execution_step_gate_state" in next_product_step
        and 'gate_state in {"input_required", "waiting_on_prerequisite", "out_of_scope"}'
        in next_product_step
        and "render_doctor" in next_product_step
        and "doctor_guidance_lines" in next_product_step
        and "guidance[external_s3].credential_mode" in next_product_step
        and "guidance[external_s3].env_precedence" in next_product_step
        and "guidance[external_s3].managed_secret" in next_product_step
        and "guidance[external_s3].existing_secret" in next_product_step
        and "guidance[external_s3].existing_secret_validation" in next_product_step
        and "guidance[external_s3].sequence" in next_product_step
        and "guidance[ingress].host" in next_product_step
        and "guidance[ingress].apply_mode" in next_product_step
        and "guidance[ingress].auth" in next_product_step
        and "guidance[durability].prerequisite" in next_product_step
        and "guidance[durability].review_flags" in next_product_step
        and "guidance[durability].cost" in next_product_step
        and "guidance[hiqlite_backend_time].scope" in next_product_step
        and "guidance[hiqlite_backend_time].release_identity" in next_product_step
        and "guidance[hiqlite_backend_time].sigstore" in next_product_step
        and "guidance[hiqlite_backend_time].failover" in next_product_step
        and "redacted_env_value" in next_product_step
        and 'return "<secret>"' in next_product_step
        and "--doctor" in next_product_step
        and 'payload["input_summary"] = input_summary' in next_product_step
        and "secret_placeholders=" in next_product_step
        and "preflight[{name}].env.{env_name}" in next_product_step
        and "preflight[{name}].review.{env_name}" in next_product_step
        and "secret:true" in next_product_step
        and "length:" in next_product_step
        and "preflight[{name}].invalid" in next_product_step
        and "release_preflight.invalid" in next_product_step
        and "auth_token_source" in next_product_step
        and "preflight[{name}].auth_token_source.{source_name}" in next_product_step
        and "preflight[{name}].authority_ready" in next_product_step
        and "preflight[{name}].authority.{field_name}" in next_product_step
        and '"reported_gate_state"' in next_product_step
        and "run_order" in next_product_step
        and "will_run_steps" in next_product_step
        and "blocked_steps" in next_product_step
        and "waiting_steps" in next_product_step
        and 'step in will_run and step not in {"local_evidence", "final_report"}' in next_product_step
        and "Only local/report refresh helpers are ready" in next_product_step
        and "missing_subjects" in next_product_step
        and "invalid_subjects" in next_product_step
        and "secrets_redacted" in next_product_step
        and "creates_product_complete_evidence" in next_product_step
        and "--fail-on-incomplete" in next_product_step
        and "raise SystemExit(75)" in next_product_step
        and "secrets_redacted=true" in next_product_step
        and "creates_product_complete_evidence=false" in next_product_step
        and "scripts/next-vind-product-step.sh --doctor" in doc
        and "redacted operator checklist" in doc
        and "redacted effective env-field status" in doc
        and "present/placeholder/length\nfor secret fields" in doc
        and "guidance[external_s3].*" in doc
        and "For `ingress`, `durability`, and `hiqlite_backend_time`, the same doctor output\nprints gate-specific `guidance[...]` lines" in doc
        and "Doctor output also expands Hiqlite release preflight\nmissing/invalid details, ingress bearer-token-source booleans" in doc
        and "The top-level input preflight intentionally mirrors the ingress helper's bearer\ntoken-source requirement" in doc
        and "It also mirrors the ingress attestation endpoint shape checks" in doc
        and "the product\ningress host must be a public DNS hostname, not `localhost`" in doc
        and "`VELORIX_INGRESS_ENDPOINT_URL` must be an HTTPS URL without query parameters or\nfragment" in doc
        and "It also mirrors the durability helper's authority prerequisite" in doc
        and "durability\nreview flags alone do not make the durability step ready" in doc
        and "An already attached `object_store.durability_policy_attestation` is not trusted\nonly because it says `validated=true`" in doc
        and "rechecks the\nsummary against the current `authority_store_id`, `bucket`, `s3_prefix`" in doc
        and "caller environment variables override them" in doc
        and "validate-only checks input\nshape while real execution checks Secret existence and keys" in doc
        and "`AWS_ENDPOINT_URL`\nmust be the service endpoint only" in doc
        and "managed credential Secret mode" in doc
        and "existing Kubernetes Secret mode" in doc
        and "does not print\ncredential values" in doc
        and "scripts/complete-vind-product.sh --env-file" in next_product_step
        and (repo_root / "scripts" / "write-complete-vind-product-input-preflight.py").is_file()
        and "velorix_complete_vind_product_input_preflight" in complete_input_preflight
        and "secrets_redacted" in complete_input_preflight
        and "product_external_s3_ready" in complete_input_preflight
        and "product_ingress_ready" in complete_input_preflight
        and "product_durability_ready" in complete_input_preflight
        and "durability_attestation_issues" in complete_input_preflight
        and "object_store.durability_policy_attestation.{field}" in complete_input_preflight
        and "DURABILITY_REVIEW_FLAGS" in complete_input_preflight
        and "already_validated" in complete_input_preflight
        and "AWS_ENDPOINT_URL looks local" in complete_input_preflight
        and "return any(prefix in value for prefix in PLACEHOLDER_PREFIXES)" in complete_input_preflight
        and "ip.is_private" in complete_input_preflight
        and 'prefix = env("VELORIX_S3_PREFIX")' in complete_input_preflight
        and "strftime('%Y%m%dT%H%M%SZ')}-preflight" not in complete_input_preflight
        and "AWS_ACCESS_KEY_ID is placeholder or known development default" in complete_input_preflight
        and "AWS_SESSION_TOKEN still contains a placeholder" in complete_input_preflight
        and "existing S3 credentials Secret mode requires {name} to be unset" in complete_input_preflight
        and "ingress endpoint must be an https URL" in complete_input_preflight
        and "ingress endpoint must not include query parameters or a fragment" in complete_input_preflight
        and "parse_env_file" in complete_input_preflight
        and "bearer_from_header" in complete_input_preflight
        and "auth_token_source" in complete_input_preflight
        and "api_token_from_auth_env" in complete_input_preflight
        and "VELORIX_API_BEARER_TOKEN" in complete_input_preflight
        and "VELORIX_ADMIN_BEARER_TOKEN" in complete_input_preflight
        and "host must be a valid DNS hostname" in complete_input_preflight
        and 'hostname.lower() == "localhost"' not in complete_input_preflight
        and 'if apply_ingress == "1":' in complete_input_preflight
        and '"existing_ingress_mode": apply_ingress == "0"' in complete_input_preflight
        and "VELORIX_OBJECT_STORE_COST_CONTROLS_REVIEWED" in complete_input_preflight
        and '"authority_ready": authority_ready' in complete_input_preflight
        and '"authority": authority' in complete_input_preflight
        and "validated nonlocal external S3/OSS authority is required before durability attestation" in complete_input_preflight
        and "deferred_to_release_preflight" in complete_input_preflight
        and "creates_product_complete_evidence" in complete_input_preflight
        and "scripts/run-vind-product-external-s3.sh" in product_complete
        and "VELORIX_COMPLETE_PRODUCT_LOCAL_EVIDENCE" in product_complete
        and "run_local_evidence_refresh()" in product_complete
        and "scripts/refresh-vind-product-deployed-images.sh" in product_complete
        and "scripts/smoke-vind-rest-api.sh" in product_complete
        and "local_evidence=refreshing_deployed_image_digests" in product_complete
        and "local_evidence=running_rest_api_smoke" in product_complete
        and "scripts/complete-vind-product-ingress.sh" in product_complete
        and "scripts/complete-vind-object-store-durability.sh" in product_complete
        and "scripts/write-complete-vind-product-input-preflight.py" in product_complete
        and "VELORIX_PRODUCT_COMPLETE_REQUIRE_EXTERNAL_S3" in product_complete
        and "VELORIX_PRODUCT_COMPLETE_REQUIRE_PUBLIC_INGRESS" in product_complete
        and 'external_s3_step="0"' in product_complete
        and 'durability_step="0"' in product_complete
        and 'ingress_step="0"' in product_complete
        and "complete-vind-product-input-preflight.json" in product_complete
        and "run_completion_input_preflight()" in product_complete
        and "preflight_step_ready()" in product_complete
        and "elif preflight_step_ready external_s3" in product_complete
        and "elif preflight_step_ready ingress" in product_complete
        and "product_external_s3_ready && preflight_step_ready durability" in product_complete
        and "complete product input preflight failed" in product_complete
        and "scripts/check-hiqlite-backend-time-release-inputs.sh" in product_complete
        and "scripts/write-hiqlite-backend-time-release-env.sh" in product_complete
        and "run_hiqlite_backend_time_release_preflight()" in product_complete
        and "VELORIX_HIQLITE_BACKEND_TIME_RELEASE_ENV_FORCE" in product_complete
        and "hiqlite_backend_time=using_existing_release_env" in product_complete
        and "hiqlite_backend_time=diagnostic_attestation" in product_complete
        and "--attester complete-vind-product" in product_complete
        and "--env-file \"$release_env\"" in product_complete
        and ". \"$release_env\"" not in product_complete
        and "--output \"$release_env\"" in product_complete
        and "--report \"$release_env_report\"" in product_complete
        and "VELORIX_STANDING_RUNTIME_FAILOVER_RELEASE_ATTEST=1" in product_complete
        and "release_failover_requested" in product_complete
        and "scripts/smoke-vind-standing-runtime-failover.sh" in product_complete
        and "scripts/attest-hiqlite-backend-time.sh" in product_complete
        and "scripts/report-vind-product-completion.sh" in product_complete
        and "VELORIX_COMPLETE_PRODUCT_EXTERNAL_S3" in product_complete
        and "VELORIX_COMPLETE_PRODUCT_INGRESS" in product_complete
        and "VELORIX_API_AUTH_ENV" in product_complete
        and "VELORIX_API_BEARER_TOKEN" in product_complete
        and "VELORIX_ADMIN_BEARER_TOKEN" in product_complete
        and "VELORIX_API_AUTH_HEADER" in product_complete
        and "VELORIX_ADMIN_AUTH_HEADER" in product_complete
        and "VELORIX_COMPLETE_PRODUCT_DURABILITY" in product_complete
        and "VELORIX_COMPLETE_PRODUCT_HIQLITE_BACKEND_TIME" in product_complete
	        and "--env-file" in product_complete
	        and "source_env_file_preserving_overrides" in product_complete
	        and "VELORIX_COMPLETE_PRODUCT_ENV_FILE" in product_complete
        and '"env_file": env_file or None' in product_complete
        and 'if [ "$dry_run" = "1" ]; then' in product_complete
        and "scripts/report-vind-product-completion.sh >/dev/null || true"
        in product_complete
        and "product_completion_report=${report_file}" in product_complete
        and "preflight_status" in product_complete
        and "forced_blocker_count" in product_complete
        and "external_s3_current_ready" in product_complete
        and "local_execution_allowed" in product_complete
        and "external_execution_allowed" in product_complete
        and "preflight_failed=0" in product_complete
        and 'if [ "$preflight_failed" = "1" ]; then' in product_complete
        and "run_order" in product_complete
        and "waiting_on_prerequisite" in product_complete
        and "ready_to_run" in product_complete
        and "input_incomplete" in product_complete
        and "missing_subjects" in product_complete
        and "invalid_subjects" in product_complete
        and "preflight_failed=1" in product_complete
        and "write_plan \"$@\"\n\nrun_local_evidence_refresh" in product_complete
        and "VELORIX_COMPLETE_PRODUCT_LOCAL_EVIDENCE=1" in doc
        and "VELORIX_COMPLETE_PRODUCT_LOCAL_EVIDENCE=0" in doc
        and "scripts/write-complete-vind-product-env.sh" in doc
        and "complete-vind-product.env" in doc
        and "complete-vind-product-env.json" in doc
        and "--env-file target/velorix-product/complete-vind-product.env" in doc
        and "dry-run also refreshes\n`product-completion-report.json`"
        in doc
        and "also preflight-backed" in doc
        and "`preflight_status`, `forced_blocker_count`, the fixed `run_order`" in doc
        and "mandatory preflight failure still leaves the plan behind" in doc
        and "Local evidence refresh and the final report can\nstill run first" in doc
        and "before external helpers run" in doc
        and "S3_OR_OSS_ENDPOINT value" in doc
        and "requires a stable explicit\n`VELORIX_S3_PREFIX`" in doc
        and "Direct\n`--validate-only` runs require the same stable prefix" in doc
        and "Raw private IP endpoints are rejected by default" in doc
        and "creates no product-complete evidence or PVCs" in doc.replace("\n", " ")
        and "velorix_complete_vind_product_env_template" in complete_product_env
        and "complete-vind-product.env" in complete_product_env
        and "complete-vind-product-env.json" in complete_product_env
        and "--env-file {output_path}" in complete_product_env
        and "scripts/write-hiqlite-backend-time-release-env.sh" in complete_product_env
        and "VELORIX_COMPLETE_PRODUCT_EXTERNAL_S3" in complete_product_env
        and "VELORIX_COMPLETE_PRODUCT_INGRESS" in complete_product_env
        and "VELORIX_COMPLETE_PRODUCT_DURABILITY" in complete_product_env
        and "VELORIX_COMPLETE_PRODUCT_HIQLITE_BACKEND_TIME" in complete_product_env
        and "AWS_ENDPOINT_URL" in complete_product_env
        and "S3_OR_OSS_ENDPOINT" in complete_product_env
        and "VELORIX_OBJECT_STORE_VERSIONING_OR_OBJECT_LOCK_ENABLED" in complete_product_env
        and "VELORIX_CI_SIGSTORE_BUNDLE_BASE64" in complete_product_env
        and "creates_product_complete_evidence" in complete_product_env
        and "fixed_release_values" in complete_product_env
	        and "secret_placeholders" in complete_product_env
	        and "placeholder_groups" in complete_product_env
	        and "the file acts as defaults" in doc
        and "take precedence over values exported by the\nenv file" in doc
        and "preserves an existing\n`hiqlite-backend-time-release.env`" in doc
        and "VELORIX_HIQLITE_BACKEND_TIME_RELEASE_ENV_FORCE=1" in doc
        and "VELORIX_PRODUCT_COMPLETE_REQUIRE_EXTERNAL_S3" in complete_product_env
        and "VELORIX_PRODUCT_COMPLETE_REQUIRE_PUBLIC_INGRESS" in complete_product_env
        and "external_s3_required" in complete_product_env
        and "public_ingress_required" in complete_product_env
        and "scope_warnings" in complete_product_env
        and "external-client reachability" in complete_product_env
        and "external_s3" in complete_product_env
        and "public_ingress_tls_auth" in complete_product_env
        and "object_store_durability_review" in complete_product_env
        and "release_identity" in complete_product_env
        and "sigstore_provenance" in complete_product_env
        and "always writes a local diagnostic\n`hiqlite-backend-time-attestation.json`" in doc
        and "VELORIX_COMPLETE_PRODUCT_DRY_RUN" in product_complete
        and "complete-vind-product-plan.json" in product_complete
        and "product_complete=" in product_complete
        and "raise SystemExit(0 if complete else 65)" in product_complete
        and "PersistentVolumeClaim" not in product_complete
        and "VELORIX_COMPLETE_PRODUCT_DRY_RUN=1" in doc
        and "product_complete=true" in doc
    ),
    "product ingress attestation wrapper reads product auth env": (
        "scripts/attest-vind-product-ingress.sh" in doc
        and "api-auth.env" in product_ingress_attest
        and "VELORIX_INGRESS_ENDPOINT_URL is required" in product_ingress_attest
        and "VELORIX_INGRESS_CONTROLLER is required" in product_ingress_attest
        and "VELORIX_API_BEARER_TOKEN" in product_ingress_attest
        and "VELORIX_ADMIN_BEARER_TOKEN" in product_ingress_attest
        and "scripts/attest-ingress-tls-auth.sh" in product_ingress_attest
        and "ingress-tls-auth-attestation.json" in product_ingress_attest
        and "product-evidence.json" in product_ingress_attest
        and "PersistentVolumeClaim" not in product_ingress_attest
        and '"public_ingress_attestation": True' in attest
        and '"trusted_for_product_complete": True' in attest
        and "VELORIX_INGRESS_TLS_AUTH_READY_TIMEOUT_SECONDS" in attest
        and "wait_for_certificate" in attest
        and "wait_for_status 401" in attest
        and '"ready_timeout_seconds": int(ready_timeout_seconds)' in attest
        and "public_ingress_attestation" in script
        and "trusted_for_product_complete" in script
    ),
    "product ingress completion wrapper applies attests attaches evidence": (
        "scripts/complete-vind-product-ingress.sh" in doc
        and "scripts/complete-vind-product-ingress.sh" in product_completion_report
        and "--env-file target/velorix-product/complete-vind-product.env"
        in product_completion_report
        and "--output-dir target/velorix-product" in product_completion_report
        and "--validate-only" in product_completion_report
        and "scripts/apply-vind-product-ingress.sh" in product_ingress_complete
        and "scripts/attest-vind-product-ingress.sh" in product_ingress_complete
        and "scripts/attach-vind-product-ingress.sh" in product_ingress_complete
	        and "--env-file" in product_ingress_complete
	        and "source_env_file_preserving_overrides" in product_ingress_complete
	        and "product_dir_cli=1" in product_ingress_complete
        and "VELORIX_API_AUTH_ENV" in product_ingress_complete
        and "auth_env_file=\"${VELORIX_API_AUTH_ENV:-${product_dir}/api-auth.env}\"" in product_ingress_complete
        and "--output-dir" in product_ingress_complete
        and "--input-evidence" in product_ingress_complete
        and "--validate-only" in product_ingress_complete
        and "velorix_product_ingress_input" in product_ingress_complete
        and "auth_token_source" in product_ingress_complete
        and "public ingress attestation requires VELORIX_API_BEARER_TOKEN" in product_ingress_complete
        and "public ingress attestation requires VELORIX_ADMIN_BEARER_TOKEN" in product_ingress_complete
        and "product-ingress-input.json" in product_ingress_complete
        and "creates_product_complete_evidence" in product_ingress_complete
        and "invalid public ingress inputs" in product_ingress_complete
        and "VELORIX_PRODUCT_INGRESS_APPLY" in product_ingress_complete
        and "VELORIX_PRODUCT_INGRESS_ATTEST" in product_ingress_complete
        and "VELORIX_PRODUCT_INGRESS_ATTACH" in product_ingress_complete
        and "existing_ingress_mode" in product_ingress_complete
        and 'if apply_ingress == "1":' in product_ingress_complete
        and "managed outside this helper" in product_ingress_complete
        and "PersistentVolumeClaim" not in product_ingress_complete
        and "--validate-only" in doc
        and "VELORIX_PRODUCT_INGRESS_APPLY=0" in doc
        and "Existing-ingress mode does not require" in doc
        and "`product-ingress-input.json` records only redacted\nsource booleans under `auth_token_source`, never token values" in doc
        and "product-ingress-input.json" in doc
        and "scripts/attach-vind-product-ingress.sh" in doc
        and "api.auth.ingress_tls_auth_attestation" in doc
        and "product_complete_blockers" in doc
        and "velorix_ingress_tls_auth_attestation" in product_ingress_attach
        and "ingress-tls-auth-attestation.json" in product_ingress_attach
        and '"validated": True' in product_ingress_attach
        and "public_ingress_attestation" in product_ingress_attach
        and "trusted_for_product_complete" in product_ingress_attach
        and "local vind TLS/auth smoke passed, but public ingress/TLS/auth attestation is missing"
        in product_ingress_attach
        and "product.get(\"product_complete\") is True" in product_ingress_attach
        and not (
            'product["product_complete"] = len(product.get("product_complete_blockers", [])) == 0'
            in product_ingress_attach
        )
        and "scripts/report-vind-product-completion.sh" in product_ingress_attach
        and "PersistentVolumeClaim" not in product_ingress_attach
    ),
    "product ingress apply helper creates no-PVC Kubernetes ingress": (
        "scripts/apply-vind-product-ingress.sh" in doc
        and "VELORIX_PRODUCT_INGRESS_APPLY" in script
        and "scripts/apply-vind-product-ingress.sh" in script
        and "VELORIX_PRODUCT_INGRESS_HOST is required" in product_ingress_apply
        and "VELORIX_PRODUCT_INGRESS_CLASS" in product_ingress_apply
        and "VELORIX_PRODUCT_INGRESS_TLS_SECRET is required" in product_ingress_apply
        and "VELORIX_PRODUCT_INGRESS_WAIT_TIMEOUT_SECONDS" in product_ingress_apply
        and "ingress_has_load_balancer_address" in product_ingress_apply
        and "status.loadBalancer.ingress" in product_ingress_apply
        and "networking.k8s.io/v1" in product_ingress_apply
        and '"kind": "Ingress"' in product_ingress_apply
        and "product-ingress-observed.json" in product_ingress_apply
        and "VELORIX_PRODUCT_INGRESS_DRY_RUN" in product_ingress_apply
        and "scripts/attest-vind-product-ingress.sh" in product_ingress_apply
        and "VELORIX_PRODUCT_INGRESS_WAIT_TIMEOUT_SECONDS" in complete_product_env
        and "VELORIX_INGRESS_TLS_AUTH_READY_TIMEOUT_SECONDS" in complete_product_env
        and "PersistentVolumeClaim" not in product_ingress_apply
        and "status.loadBalancer.ingress" in doc
        and "VELORIX_INGRESS_TLS_AUTH_READY_TIMEOUT_SECONDS" in doc
        and "does not create DNS records, public certificates, TLS\nSecrets, or PVCs" in doc
    ),
    "deployed image digest refresh is annotation and pod-status bound": (
        "VELORIX_VIND_PRODUCT_EVIDENCE_OUT" in refresh_deployed_images
        and "velorix.dev/image-digest" in refresh_deployed_images
        and "velorix.dev/image-digest-source" in refresh_deployed_images
        and "imageID digest" in refresh_deployed_images
        and "does not match deployment annotation" in refresh_deployed_images
        and "observed-pod-imageid-after-rollout" in refresh_deployed_images
        and "rollout status" in refresh_deployed_images
        and "product_complete_blockers" in refresh_deployed_images
        and "velorix-api deployed image digest was not recorded" in refresh_deployed_images
        and "velorix-meta deployed image digest was not recorded" in refresh_deployed_images
        and "VELORIX_VIND_PRODUCT_DIR=target/velorix-product " in product_completion_report
        and "scripts/refresh-vind-product-deployed-images.sh" in product_completion_report
        and "VELORIX_API_IMAGE_DIGEST=sha256:REPLACE_WITH_API_DIGEST" in product_completion_report
        and "scripts/run-vind-product.sh" in product_completion_report
        and "observed_pod_image_digest()" in script
        and "sync_deployed_image_digest_annotation()" in script
        and "observed-pod-imageid-after-rollout" in script
        and "scripts/refresh-vind-product-deployed-images.sh" in doc
        and "helper patches only that Deployment template annotation" in doc
        and "does not change\ncontainer images, and does not create PVCs" in doc
        and "infer release product evidence from Pod status alone" in doc
    ),
}

failed = [name for name, ok in checks.items() if not ok]
if failed:
    raise SystemExit(
        "vind product contract check failed:\n- " + "\n- ".join(failed)
    )

print("vind product contract check passed")
PY
