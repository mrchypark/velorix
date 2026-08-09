#!/usr/bin/env python3
"""Shared validation for release evidence references."""

import hashlib
import re
import sys
import tempfile
from pathlib import Path

URI_RE = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*://")
APPROVED_URI_PREFIX = "s3://"
SHA256_RE = re.compile(r"[0-9a-fA-F]{64}")
SHA256_ID_RE = re.compile(
    r"(?:sha256[:=/_-]|sha256%3[aA]|digest[:=](?:sha256[:=]|sha256%3[aA])?)([0-9a-fA-F]{64})"
)
ZERO_SHA256 = "0" * 64
GIT_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
IMAGE_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
RELEASE_IMAGE_ROLES = ("velorix-api", "velorix-meta")


def _sha256_identity(ref):
    match = SHA256_ID_RE.search(ref)
    if match:
        return match.group(1).lower()
    return None


def _local_ref_path(ref, evidence_path):
    local_ref = ref.split("#", 1)[0].split("?", 1)[0]
    candidate = Path(local_ref)
    evidence_dir = Path(evidence_path).resolve().parent
    if candidate.is_absolute():
        return None, f"absolute local evidence refs are not allowed: {local_ref}"

    resolved = (evidence_dir / candidate).resolve()
    try:
        resolved.relative_to(evidence_dir)
    except ValueError:
        return None, f"local evidence ref escapes evidence directory: {local_ref}"
    return resolved, None


def _sidecar_sha256(path):
    for suffix in (".sha256", ".sha256sum", ".sha256.txt"):
        sidecar = Path(str(path) + suffix)
        if sidecar.is_file():
            match = SHA256_RE.search(sidecar.read_text(encoding="utf-8"))
            if match:
                return match.group(0).lower()
    return None


def _file_sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_evidence_ref(ref, evidence_path, label):
    if not isinstance(ref, str) or not ref.strip():
        return [f"{label} must be a non-empty string"]

    ref = ref.strip()
    expected_sha256 = _sha256_identity(ref)
    if URI_RE.match(ref):
        if not ref.startswith(APPROVED_URI_PREFIX):
            return [f"{label} URI evidence refs must use s3:// release authority"]
        if expected_sha256 and expected_sha256 != ZERO_SHA256:
            return []
        if expected_sha256 == ZERO_SHA256:
            return [f"{label} sha256 identity must not be all zeroes"]
        return [f"{label} immutable URI must include sha256/digest identity"]

    local_path, path_error = _local_ref_path(ref, evidence_path)
    if path_error:
        return [f"{label} {path_error}"]
    if not local_path.is_file():
        return [f"{label} local evidence file does not exist: {local_path}"]

    expected_sha256 = expected_sha256 or _sidecar_sha256(local_path)
    if not expected_sha256:
        return [f"{label} local evidence file must include inline or sidecar sha256 identity"]
    if expected_sha256 == ZERO_SHA256:
        return [f"{label} sha256 identity must not be all zeroes"]
    actual_sha256 = _file_sha256(local_path)
    if actual_sha256 != expected_sha256:
        return [f"{label} sha256 mismatch: expected {expected_sha256}, got {actual_sha256}"]
    return []


def validate_release_identity_fields(value):
    errors = []
    if not isinstance(value, dict):
        return ["release identity must be a JSON object"]

    source_revision = value.get("source_revision")
    if not isinstance(source_revision, str) or not GIT_SHA_RE.fullmatch(source_revision):
        errors.append("source_revision must be a 40-character lowercase git SHA")

    image_digests = value.get("deployed_image_digests")
    if not isinstance(image_digests, dict):
        errors.append("deployed_image_digests must be an object")
        return errors

    for role in RELEASE_IMAGE_ROLES:
        if role not in image_digests:
            errors.append(f"deployed_image_digests.{role} must be present")
            continue
        digest = image_digests.get(role)
        if not isinstance(digest, str) or not IMAGE_DIGEST_RE.fullmatch(digest):
            errors.append(
                f"deployed_image_digests.{role} must be a sha256:<64 lowercase hex> digest"
            )
    return errors


def _assert(condition, message):
    if not condition:
        raise AssertionError(message)


def _self_test():
    zero_digest = "0" * 64
    nonzero_digest = "a" * 64
    with tempfile.TemporaryDirectory() as raw_dir:
        root = Path(raw_dir) / "evidence-dir"
        root.mkdir()
        evidence = root / "evidence.json"
        evidence.write_text("{}", encoding="utf-8")
        sibling = root / "sibling.log"
        sibling.write_text("release proof\n", encoding="utf-8")
        sibling_digest = _file_sha256(sibling)
        escape = Path(raw_dir) / "escape.log"
        escape.write_text("escaped proof\n", encoding="utf-8")
        escape_digest = _file_sha256(escape)

        _assert(
            validate_evidence_ref(
                f"s3://release-evidence/proof.json?sha256={nonzero_digest}",
                evidence,
                "evidence_refs.proof",
            )
            == [],
            "URI with non-zero sha256 should pass",
        )
        _assert(
            validate_evidence_ref(
                f"s3://release-evidence/proof.json?sha256={zero_digest}",
                evidence,
                "evidence_refs.proof",
            ),
            "URI with all-zero sha256 should fail",
        )
        _assert(
            validate_evidence_ref(
                "s3://release-evidence/proof.json",
                evidence,
                "evidence_refs.proof",
            ),
            "URI without digest should fail",
        )
        _assert(
            validate_evidence_ref(
                f"https://example.com/proof.json?sha256={nonzero_digest}",
                evidence,
                "evidence_refs.proof",
            ),
            "https URI should fail even with sha256 identity",
        )
        _assert(
            validate_evidence_ref(
                f"file:///tmp/proof.json?sha256={nonzero_digest}",
                evidence,
                "evidence_refs.proof",
            ),
            "file URI should fail even with sha256 identity",
        )
        _assert(
            validate_evidence_ref("sibling.log", evidence, "evidence_refs.proof"),
            "bare sibling file should fail without sha256 identity",
        )
        _assert(
            validate_evidence_ref(
                f"{sibling}#sha256={sibling_digest}",
                evidence,
                "evidence_refs.proof",
            ),
            "absolute local ref should fail",
        )
        _assert(
            validate_evidence_ref(
                f"../escape.log#sha256={escape_digest}",
                evidence,
                "evidence_refs.proof",
            ),
            "../ local ref escape should fail",
        )
        _assert(
            validate_evidence_ref("missing.log", evidence, "evidence_refs.proof"),
            "missing sibling file should fail",
        )
        _assert(
            validate_evidence_ref(
                f"sibling.log#sha256={sibling_digest}",
                evidence,
                "evidence_refs.proof",
            )
            == [],
            "inline local sha256 should pass",
        )
        _assert(
            validate_evidence_ref(
                f"sibling.log#sha256={zero_digest}",
                evidence,
                "evidence_refs.proof",
            ),
            "inline local all-zero sha256 should fail",
        )
        (root / "sidecar.log").write_text("sidecar proof\n", encoding="utf-8")
        sidecar_digest = _file_sha256(root / "sidecar.log")
        (root / "sidecar.log.sha256").write_text(
            f"{sidecar_digest}  sidecar.log\n", encoding="utf-8"
        )
        _assert(
            validate_evidence_ref("sidecar.log", evidence, "scenarios[0].evidence")
            == [],
            "sidecar sha256 should pass",
        )
        (root / "zero-sidecar.log").write_text("zero sidecar proof\n", encoding="utf-8")
        (root / "zero-sidecar.log.sha256").write_text(
            f"{zero_digest}  zero-sidecar.log\n", encoding="utf-8"
        )
        _assert(
            validate_evidence_ref(
                "zero-sidecar.log", evidence, "scenarios[1].evidence"
            ),
            "sidecar local all-zero sha256 should fail",
        )
        valid_identity = {
            "source_revision": "a" * 40,
            "deployed_image_digests": {
                "velorix-api": "sha256:" + ("1" * 64),
                "velorix-meta": "sha256:" + ("2" * 64),
            },
        }
        _assert(
            validate_release_identity_fields(valid_identity) == [],
            "valid release identity should pass",
        )
        _assert(
            validate_release_identity_fields(
                {
                    "source_revision": "A" * 40,
                    "deployed_image_digests": valid_identity["deployed_image_digests"],
                }
            ),
            "uppercase source_revision should fail",
        )
        _assert(
            validate_release_identity_fields(
                {
                    "source_revision": "a" * 40,
                    "deployed_image_digests": {
                        "velorix-api": "sha256:" + ("A" * 64),
                    },
                }
            ),
            "missing role and uppercase digest should fail",
        )


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-test"]:
        _self_test()
        raise SystemExit(0)
    print("usage: evidence_ref_validator.py --self-test", file=sys.stderr)
    raise SystemExit(64)
