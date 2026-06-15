#!/usr/bin/env python3
import argparse
import json
import shutil
from pathlib import Path


def sibling(base: Path, filename: str, label: str) -> Path:
    if not isinstance(filename, str) or not filename.strip():
        raise SystemExit(f"{label} has empty evidence filename")
    if "/" in filename or "\\" in filename:
        raise SystemExit(f"{label} has non-sibling evidence filename: {filename!r}")
    path = base.parent / filename
    if not path.is_file():
        raise SystemExit(f"{label} requires sibling evidence file {path}")
    return path


def related_artifact(base: Path, reported: str, expected_name: str, label: str) -> Path:
    if not isinstance(reported, str) or Path(reported).name != expected_name:
        raise SystemExit(f"{label} must reference {expected_name}")
    return sibling(base, expected_name, label)


def copy_file(src: Path, dest_dir: Path, dest_name: str | None = None) -> Path:
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / (dest_name or src.name)
    shutil.copy2(src, dest)
    return dest


def product_siblings(artifact: Path, doc: dict) -> list[Path]:
    api = doc.get("api") or {}
    auth = api.get("auth") or {}
    metadata = doc.get("metadata_store") or {}
    object_store = doc.get("object_store") or {}
    ingest_writer = doc.get("ingest_writer") or {}
    deployed_images = doc.get("deployed_images") or {}

    openapi = api.get("openapi") or {}
    if openapi.get("evidence_file") != "openapi.json":
        raise SystemExit("product openapi.evidence_file must be openapi.json")
    local_tls = auth.get("local_tls_auth_smoke") or {}
    if local_tls.get("evidence") != "tls-auth-smoke.json":
        raise SystemExit("product local_tls_auth_smoke.evidence must be tls-auth-smoke.json")

    result = [
        sibling(artifact, "openapi.json", "product OpenAPI evidence"),
        sibling(artifact, "tls-auth-smoke.json", "product local TLS/auth evidence"),
        sibling(
            artifact,
            "external-s3-validate-job.json",
            "product external S3 validation job evidence",
        ),
        sibling(
            artifact,
            "external-s3-validate.log",
            "product external S3 validation log evidence",
        ),
    ]

    for role, expected_files in {
        "velorix-api": {
            "manifest": "velorix-api.yaml",
            "deployment": "velorix-api-deployment-observed.json",
            "pods": "velorix-api-pods.json",
        },
        "velorix-meta": {
            "manifest": "velorix-meta.yaml",
            "deployment": "velorix-meta-deployment-observed.json",
            "pods": "velorix-meta-pods.json",
        },
    }.items():
        image = deployed_images.get(role) or {}
        files = image.get("evidence_files") or {}
        for key, expected in expected_files.items():
            if files.get(key) != expected:
                raise SystemExit(
                    f"product deployed_images.{role}.evidence_files.{key} must be {expected}"
                )
            result.append(sibling(artifact, expected, f"product deployed image evidence {role}"))

    no_pvc = doc.get("no_pvc") or {}
    if no_pvc.get("evidence") != "no-pvc-namespace.json":
        raise SystemExit("product no_pvc.evidence must be no-pvc-namespace.json")
    result.append(
        sibling(
            artifact,
            "no-pvc-namespace.json",
            "product no-PVC namespace evidence",
        )
    )

    hiqlite = metadata.get("hiqlite_authority_attestation")
    if hiqlite:
        if hiqlite.get("evidence") != "hiqlite-authority-attestation.json":
            raise SystemExit(
                "product hiqlite_authority_attestation.evidence must be hiqlite-authority-attestation.json"
            )
        result.append(
            sibling(
                artifact,
                "hiqlite-authority-attestation.json",
                "product Hiqlite authority evidence",
            )
        )
        if hiqlite.get("authority_kind") == "velorix_managed_hiqlite":
            for key, expected in {
                "namespace_pvc_list": "no-pvc-namespace.json",
                "hiqlite_statefulset": "no-pvc-hiqlite-statefulset.json",
                "manifest": "velorix-hiqlite.yaml",
            }.items():
                filename = (hiqlite.get("no_pvc_evidence_files") or {}).get(key)
                if filename != expected:
                    raise SystemExit(
                        f"product hiqlite_authority_attestation.no_pvc_evidence_files.{key} must be {expected}"
                    )
                result.append(sibling(artifact, expected, "product Hiqlite no-PVC evidence"))

    hiqlite_backend_time = metadata.get("hiqlite_backend_time_attestation")
    if hiqlite_backend_time:
        if hiqlite_backend_time.get("evidence") != "hiqlite-backend-time-attestation.json":
            raise SystemExit(
                "product hiqlite_backend_time_attestation.evidence must be hiqlite-backend-time-attestation.json"
            )
        result.append(
            sibling(
                artifact,
                "hiqlite-backend-time-attestation.json",
                "product Hiqlite backend-time evidence",
            )
        )

    ingress = auth.get("ingress_tls_auth_attestation")
    if ingress:
        if ingress.get("evidence") != "ingress-tls-auth-attestation.json":
            raise SystemExit(
                "product ingress_tls_auth_attestation.evidence must be ingress-tls-auth-attestation.json"
            )
        result.append(
            sibling(
                artifact,
                "ingress-tls-auth-attestation.json",
                "product ingress/TLS/auth evidence",
            )
        )

    query_policy = api.get("query_policy") or {}
    for key, expected in {
        "created": "query-policy-interactive.json",
        "read_back": "query-policy-interactive-read.json",
        "weak_policy_rejection": "query-policy-weak-rejection.json",
        "missing_policy_rejection": "query-policy-missing-view.json",
    }.items():
        filename = (query_policy.get("evidence_files") or {}).get(key)
        if filename != expected:
            raise SystemExit(f"product query-policy evidence_files.{key} must be {expected}")
        result.append(sibling(artifact, expected, "product query-policy evidence"))

    external = object_store.get("external_s3_validation_evidence") or {}
    for key, expected in {
        "job": "external-s3-validate-job.json",
        "log": "external-s3-validate.log",
    }.items():
        if external.get(key) != expected:
            raise SystemExit(f"product external_s3_validation_evidence.{key} must be {expected}")

    product_ingest_files = ingest_writer.get("evidence_files") or {}
    for key, expected in {
        "job_log": "ingest-writer-job-log.json",
        "job": "ingest-writer-job.json",
        "pods": "ingest-writer-pods.json",
    }.items():
        if product_ingest_files.get(key) != expected:
            raise SystemExit(f"product ingest_writer.evidence_files.{key} must be {expected}")
        result.append(sibling(artifact, expected, "product ingest-writer append evidence"))

    lifecycle = ingest_writer.get("lifecycle_attestation") or {}
    result.extend(lifecycle_siblings(artifact, lifecycle, "product lifecycle evidence"))
    return unique_paths(result)


def lifecycle_siblings(artifact: Path, doc: dict, label: str) -> list[Path]:
    files = doc.get("evidence_files") or {}
    result = []
    for key, expected in {
        "pod_internal_job": "velorix-ingest-writer-smoke-log.json",
        "overlap_job": "velorix-ingest-lifecycle-overlap-log.json",
        "adjacent_job": "velorix-ingest-lifecycle-adjacent-log.json",
        "restart_job": "velorix-ingest-lifecycle-restart-log.json",
        "lease_loss_job": "velorix-ingest-lifecycle-lease-loss-log.json",
        "handoff_probe_job": "velorix-ingest-lifecycle-handoff-log.json",
    }.items():
        if files.get(key) != expected:
            raise SystemExit(f"{label} evidence_files.{key} must be {expected}")
        result.append(sibling(artifact, expected, label))
    return result


def rustfs_production_gc_siblings(artifact: Path, doc: dict) -> list[Path]:
    if doc.get("evidence_kind") != "rustfs_production_gc_evidence_family_validated":
        raise SystemExit(
            "RustFS production GC validation evidence_kind must be rustfs_production_gc_evidence_family_validated"
        )
    if doc.get("status") != "pass":
        raise SystemExit("RustFS production GC validation evidence must be pass")

    result = []
    for key, expected in {
        "gate_evidence_path": "rustfs-s3-gate-evidence.json",
        "seed_evidence_path": "rustfs-production-gc-seed.json",
        "execute_evidence_path": "rustfs-production-gc-run.json",
        "production_evidence_path": "rustfs-production-gc.json",
    }.items():
        result.append(
            related_artifact(
                artifact,
                doc.get(key),
                expected,
                f"RustFS production GC validation {key}",
            )
        )

    checks = set(doc.get("checks") or [])
    for required_check in {
        "rustfs_s3_compatible_gate_present",
        "seed_fixture_created_retired_checkpoint_state",
        "s3_gc_execute_deleted_seeded_candidate",
        "production_gc_evidence_verified_listing_retention_and_transition",
        "artifact_family_paths_and_identity_bound",
    }:
        if required_check not in checks:
            raise SystemExit(
                f"RustFS production GC validation evidence missing check {required_check}"
            )
    return unique_paths(result)


def unique_paths(paths: list[Path]) -> list[Path]:
    seen = set()
    result = []
    for path in paths:
        resolved = path.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        result.append(path)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Copy a readiness evidence artifact and its required sibling files."
    )
    parser.add_argument(
        "--kind",
        choices=["product", "ingest-writer-lifecycle", "rustfs-production-gc"],
        required=True,
    )
    parser.add_argument("--artifact", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--artifact-name", required=True)
    args = parser.parse_args()

    artifact = Path(args.artifact)
    out_dir = Path(args.out_dir)
    if not artifact.is_file():
        raise SystemExit(f"missing evidence artifact: {artifact}")

    with artifact.open("r", encoding="utf-8") as f:
        doc = json.load(f)

    copied_artifact = copy_file(artifact, out_dir, args.artifact_name)
    if args.kind == "product":
        siblings = product_siblings(artifact, doc)
    elif args.kind == "ingest-writer-lifecycle":
        siblings = lifecycle_siblings(artifact, doc, "ingest-writer lifecycle evidence")
    elif args.kind == "rustfs-production-gc":
        siblings = rustfs_production_gc_siblings(artifact, doc)
    else:
        raise AssertionError(args.kind)

    for path in siblings:
        copy_file(path, out_dir)

    print(copied_artifact)


if __name__ == "__main__":
    main()
