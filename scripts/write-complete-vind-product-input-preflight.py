#!/usr/bin/env python3
import argparse
import ipaddress
import json
import os
import re
import shlex
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse


PLACEHOLDER_PREFIXES = (
    "REPLACE_WITH",
    "PUBLIC_HOST.",
    "TLS_SECRET_NAME",
    "INGRESS_CONTROLLER",
    "S3_OR_OSS_ENDPOINT",
)
DEFAULT_SECRET_VALUES = {
    "rustfsadmin",
    "minioadmin",
    "changeme",
    "password",
}
HOSTNAME_PATTERN = re.compile(
    r"^(?=.{1,253}$)([A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)+[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?$"
)
K8S_NAME_PATTERN = re.compile(r"^[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?$")
DURABILITY_REVIEW_FLAGS = [
    "versioning_or_object_lock_enabled",
    "server_side_encryption_enabled",
    "backup_or_replication_configured",
    "lifecycle_delete_policy_reviewed",
    "destructive_delete_protection_reviewed",
    "cost_controls_reviewed",
]


def env(name: str) -> str:
    return os.environ.get(name, "").strip()


def bool_env(name: str, default: str = "0") -> str:
    value = env(name) or default
    return value if value in {"0", "1"} else value


def has_placeholder(value: str) -> bool:
    value = (value or "").strip()
    return any(prefix in value for prefix in PLACEHOLDER_PREFIXES)


def env_field(name: str, *, secret: bool = False) -> dict:
    value = env(name)
    field = {
        "present": bool(value),
        "placeholder": has_placeholder(value),
        "secret": secret,
    }
    if not secret and value and len(value) <= 160:
        field["value"] = value
    elif value:
        field["length"] = len(value)
    return field


def add_issue(collection: list, subject: str, detail: str) -> None:
    collection.append({"subject": subject, "detail": detail})


def bearer_from_header(header: str) -> str:
    match = re.match(r"^\s*authorization\s*:\s*Bearer\s+(.+?)\s*$", header or "", re.IGNORECASE)
    return match.group(1) if match else ""


def parse_env_file(path: Path) -> dict:
    values = {}
    if not path.is_file():
        return values
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not re.fullmatch(r"[A-Z0-9_]+", key):
            continue
        try:
            parsed = shlex.split(value, posix=True)
        except ValueError:
            continue
        values[key] = parsed[0] if parsed else ""
    return values


def write_json_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def load_product(path: Path) -> dict:
    if not path.is_file():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return value if isinstance(value, dict) else {}


def pointer(value, path: str):
    current = value
    for part in path.strip("/").split("/"):
        if not part:
            continue
        if isinstance(current, dict):
            current = current.get(part)
        else:
            return None
    return current


def product_external_s3_ready(product: dict) -> bool:
    store = product.get("object_store") or {}
    return (
        store.get("mode") == "external-s3"
        and store.get("local_development_authority") is not True
        and store.get("external_s3_bucket_validated") is True
        and store.get("external_s3_prefix_validated") is True
    )


def product_ingress_ready(product: dict) -> bool:
    ingress = pointer(product, "/api/auth/ingress_tls_auth_attestation") or {}
    return (
        ingress.get("public_ingress_attestation") is True
        and ingress.get("trusted_for_product_complete") is True
    )


def product_durability_ready(product: dict) -> bool:
    return not durability_attestation_issues(product)


def durability_attestation_issues(product: dict) -> list:
    store = product.get("object_store") or {}
    attestation = store.get("durability_policy_attestation") or {}
    issues = []
    if not attestation:
        issues.append(
            {
                "subject": "object_store.durability_policy_attestation",
                "detail": "object_store.durability_policy_attestation is required",
            }
        )
        return issues
    if attestation.get("validated") is not True:
        issues.append(
            {
                "subject": "object_store.durability_policy_attestation.validated",
                "detail": "object_store.durability_policy_attestation.validated must be true",
            }
        )
    if attestation.get("schema_version") != 1:
        issues.append(
            {
                "subject": "object_store.durability_policy_attestation.schema_version",
                "detail": "object_store.durability_policy_attestation.schema_version must be 1",
            }
        )
    if attestation.get("evidence_kind") != "velorix_object_store_durability_policy_attestation":
        issues.append(
            {
                "subject": "object_store.durability_policy_attestation.evidence_kind",
                "detail": "object_store.durability_policy_attestation.evidence_kind must be velorix_object_store_durability_policy_attestation",
            }
        )
    for field in ["authority_store_id", "bucket", "s3_prefix"]:
        if attestation.get(field) != store.get(field):
            issues.append(
                {
                    "subject": f"object_store.durability_policy_attestation.{field}",
                    "detail": f"object_store.durability_policy_attestation.{field} must match object_store.{field}",
                }
            )
    for field in DURABILITY_REVIEW_FLAGS:
        if attestation.get(field) is not True:
            issues.append(
                {
                    "subject": f"object_store.durability_policy_attestation.{field}",
                    "detail": f"object_store.durability_policy_attestation.{field} must be true",
                }
            )
    return issues


def hostname_is_valid(hostname: str) -> bool:
    if not hostname or has_placeholder(hostname):
        return False
    return bool(HOSTNAME_PATTERN.fullmatch(hostname))


def validate_endpoint(endpoint: str, allow_local: bool, missing: list, invalid: list) -> dict:
    details = {"present": bool(endpoint), "local_endpoint": None, "scheme": None, "host": None}
    if not endpoint:
        add_issue(missing, "AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL is required for external S3")
        return details
    if has_placeholder(endpoint):
        add_issue(invalid, "AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL still contains a placeholder")
    parsed = urlparse(endpoint)
    details["scheme"] = parsed.scheme or None
    details["host"] = parsed.hostname or None
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        add_issue(invalid, "AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL must be an http(s) URL with a host")
        return details
    if parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
        add_issue(invalid, "AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL must be the S3/OSS service endpoint only, without bucket, prefix, query, or fragment")
    try:
        parsed.port
    except ValueError:
        add_issue(invalid, "AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL port is invalid")
        return details
    host = parsed.hostname or ""
    is_local = host.lower() in {
        "localhost",
        "host.docker.internal",
        "kubernetes.docker.internal",
    }
    try:
        ip = ipaddress.ip_address(host)
        if (
            ip.is_loopback
            or ip.is_link_local
            or ip.is_private
            or ip.is_unspecified
            or ip.is_multicast
            or ip.is_reserved
        ):
            is_local = True
    except ValueError:
        pass
    details["local_endpoint"] = is_local
    if is_local and not allow_local:
        add_issue(
            invalid,
            "AWS_ENDPOINT_URL",
            "AWS_ENDPOINT_URL looks local; use run-vind-product-external-rustfs.sh for local RustFS or set VELORIX_EXTERNAL_S3_ALLOW_LOCAL_ENDPOINT=1 only for diagnostics",
        )
    return details


def validate_bucket(bucket: str, missing: list, invalid: list) -> None:
    if not bucket:
        add_issue(missing, "VELORIX_S3_BUCKET", "VELORIX_S3_BUCKET is required for external S3")
        return
    if has_placeholder(bucket):
        add_issue(invalid, "VELORIX_S3_BUCKET", "VELORIX_S3_BUCKET still contains a placeholder")
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", bucket):
        add_issue(invalid, "VELORIX_S3_BUCKET", "VELORIX_S3_BUCKET must be a valid S3 bucket name")


def validate_prefix(prefix: str, missing: list, invalid: list) -> str:
    normalized = (prefix or "").strip("/")
    if not normalized:
        add_issue(missing, "VELORIX_S3_PREFIX", "VELORIX_S3_PREFIX must be nonempty")
        return normalized
    if has_placeholder(normalized):
        add_issue(invalid, "VELORIX_S3_PREFIX", "VELORIX_S3_PREFIX still contains a placeholder")
    parts = normalized.split("/")
    if normalized.startswith(".") or ".." in parts or any(part == "" for part in parts):
        add_issue(invalid, "VELORIX_S3_PREFIX", "VELORIX_S3_PREFIX must be a safe object prefix")
    return normalized


def already_validated_step(mode: str, status: str, delegates_to: str) -> dict:
    return {
        "mode": mode,
        "required": mode == "1",
        "ready": True,
        "status": status,
        "missing": [],
        "invalid": [],
        "delegates_to": delegates_to,
    }


def validate_external_s3(mode: str, product_dir: Path, product: dict) -> dict:
    if product_external_s3_ready(product):
        payload = already_validated_step(
            mode,
            "already_validated",
            "scripts/run-vind-product-external-s3.sh",
        )
        payload["input_evidence"] = str(product_dir / "external-s3-product-input.json")
        return payload

    missing: list = []
    invalid: list = []
    endpoint = env("AWS_ENDPOINT_URL")
    access_key = env("AWS_ACCESS_KEY_ID")
    secret_key = env("AWS_SECRET_ACCESS_KEY")
    session_token = env("AWS_SESSION_TOKEN")
    bucket = env("VELORIX_S3_BUCKET")
    prefix = env("VELORIX_S3_PREFIX")
    authority = env("VELORIX_AUTHORITY_STORE_ID")
    allow_local = bool_env("VELORIX_EXTERNAL_S3_ALLOW_LOCAL_ENDPOINT") == "1"
    force_path_style = bool_env("VELORIX_S3_FORCE_PATH_STYLE", "1")
    credentials_secret_name = env("VELORIX_S3_CREDENTIALS_SECRET_NAME") or "velorix-s3-credentials"
    credentials_secret_managed = bool_env("VELORIX_S3_CREDENTIALS_SECRET_MANAGED", "1")
    local_authority = bool_env("VELORIX_OBJECT_STORE_LOCAL_DEVELOPMENT_AUTHORITY") == "1"
    env_file = env("VELORIX_EXTERNAL_S3_ENV")

    if env_file and not Path(env_file).is_file():
        add_issue(invalid, "VELORIX_EXTERNAL_S3_ENV", f"VELORIX_EXTERNAL_S3_ENV does not exist: {env_file}")
    endpoint_details = validate_endpoint(endpoint, allow_local, missing, invalid)
    if credentials_secret_managed not in {"0", "1"}:
        add_issue(invalid, "VELORIX_S3_CREDENTIALS_SECRET_MANAGED", "VELORIX_S3_CREDENTIALS_SECRET_MANAGED must be 0 or 1")
    if not K8S_NAME_PATTERN.fullmatch(credentials_secret_name):
        add_issue(invalid, "VELORIX_S3_CREDENTIALS_SECRET_NAME", "VELORIX_S3_CREDENTIALS_SECRET_NAME must be a valid Kubernetes Secret name")
    if credentials_secret_managed == "0":
        for name, value in [
            ("AWS_ACCESS_KEY_ID", access_key),
            ("AWS_SECRET_ACCESS_KEY", secret_key),
            ("AWS_SESSION_TOKEN", session_token),
        ]:
            if value:
                add_issue(
                    invalid,
                    name,
                    f"existing S3 credentials Secret mode requires {name} to be unset",
                )
    else:
        if not access_key:
            add_issue(missing, "AWS_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID is required for external S3")
        elif has_placeholder(access_key) or access_key.lower() in DEFAULT_SECRET_VALUES:
            add_issue(invalid, "AWS_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID is placeholder or known development default")
        if not secret_key:
            add_issue(missing, "AWS_SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY is required for external S3")
        elif has_placeholder(secret_key) or secret_key.lower() in DEFAULT_SECRET_VALUES:
            add_issue(invalid, "AWS_SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY is placeholder or known development default")
        if session_token and has_placeholder(session_token):
            add_issue(invalid, "AWS_SESSION_TOKEN", "AWS_SESSION_TOKEN still contains a placeholder")
    validate_bucket(bucket, missing, invalid)
    normalized_prefix = validate_prefix(prefix, missing, invalid)
    if local_authority:
        add_issue(
            invalid,
            "VELORIX_OBJECT_STORE_LOCAL_DEVELOPMENT_AUTHORITY",
            "external S3 product completion refuses local development authority",
        )
    if bucket and normalized_prefix:
        expected_authority = f"s3://external/{bucket}/{normalized_prefix}"
        if authority and authority != expected_authority:
            invalid.append(
                {
                    "subject": "VELORIX_AUTHORITY_STORE_ID",
                    "detail": f"VELORIX_AUTHORITY_STORE_ID must equal {expected_authority}",
                }
            )
    if force_path_style not in {"0", "1"}:
        add_issue(invalid, "VELORIX_S3_FORCE_PATH_STYLE", "VELORIX_S3_FORCE_PATH_STYLE must be 0 or 1")
    durability_attestation = env("VELORIX_OBJECT_STORE_DURABILITY_ATTESTATION_FILE")
    if durability_attestation and not Path(durability_attestation).is_file():
        add_issue(
            invalid,
            "VELORIX_OBJECT_STORE_DURABILITY_ATTESTATION_FILE",
            f"object-store durability attestation does not exist: {durability_attestation}",
        )

    required = mode == "1"
    ready = not missing and not invalid
    return {
        "mode": mode,
        "required": required,
        "ready": ready,
        "status": "ready" if ready else ("blocked" if required else "incomplete"),
        "missing": missing,
        "invalid": invalid,
        "env": {
            "AWS_ENDPOINT_URL": env_field("AWS_ENDPOINT_URL"),
            "AWS_ACCESS_KEY_ID": env_field("AWS_ACCESS_KEY_ID", secret=True),
            "AWS_SECRET_ACCESS_KEY": env_field("AWS_SECRET_ACCESS_KEY", secret=True),
            "AWS_SESSION_TOKEN": env_field("AWS_SESSION_TOKEN", secret=True),
            "AWS_REGION": env_field("AWS_REGION"),
            "VELORIX_S3_BUCKET": env_field("VELORIX_S3_BUCKET"),
            "VELORIX_S3_PREFIX": {"present": True, "value": normalized_prefix},
            "VELORIX_AUTHORITY_STORE_ID": env_field("VELORIX_AUTHORITY_STORE_ID"),
            "VELORIX_S3_FORCE_PATH_STYLE": {"present": True, "value": force_path_style},
            "VELORIX_S3_CREDENTIALS_SECRET_NAME": {"present": True, "value": credentials_secret_name},
            "VELORIX_S3_CREDENTIALS_SECRET_MANAGED": {"present": True, "value": credentials_secret_managed},
            "VELORIX_EXTERNAL_S3_ENV": env_field("VELORIX_EXTERNAL_S3_ENV"),
            "VELORIX_EXTERNAL_S3_ALLOW_LOCAL_ENDPOINT": env_field("VELORIX_EXTERNAL_S3_ALLOW_LOCAL_ENDPOINT"),
            "VELORIX_OBJECT_STORE_LOCAL_DEVELOPMENT_AUTHORITY": env_field("VELORIX_OBJECT_STORE_LOCAL_DEVELOPMENT_AUTHORITY"),
            "VELORIX_OBJECT_STORE_DURABILITY_ATTESTATION_FILE": env_field("VELORIX_OBJECT_STORE_DURABILITY_ATTESTATION_FILE"),
        },
        "endpoint": endpoint_details,
        "delegates_to": "scripts/run-vind-product-external-s3.sh",
        "input_evidence": str(product_dir / "external-s3-product-input.json"),
    }


def validate_ingress(mode: str, product: dict, product_dir: Path) -> dict:
    if product_ingress_ready(product):
        return already_validated_step(
            mode,
            "already_validated",
            "scripts/complete-vind-product-ingress.sh",
        )

    missing: list = []
    invalid: list = []
    host = env("VELORIX_PRODUCT_INGRESS_HOST")
    ingress_class = env("VELORIX_PRODUCT_INGRESS_CLASS") or env("VELORIX_INGRESS_CONTROLLER")
    tls_secret = env("VELORIX_PRODUCT_INGRESS_TLS_SECRET")
    endpoint = env("VELORIX_INGRESS_ENDPOINT_URL")
    controller = env("VELORIX_INGRESS_CONTROLLER")
    apply_ingress = bool_env("VELORIX_PRODUCT_INGRESS_APPLY", "1")
    attest_ingress = bool_env("VELORIX_PRODUCT_INGRESS_ATTEST", "1")
    attach_ingress = bool_env("VELORIX_PRODUCT_INGRESS_ATTACH", "1")
    auth_env_file = Path(env("VELORIX_API_AUTH_ENV") or str(product_dir / "api-auth.env"))
    auth_env_values = parse_env_file(auth_env_file)
    api_token_from_environment = bool(env("VELORIX_API_BEARER_TOKEN") or bearer_from_header(env("VELORIX_API_AUTH_HEADER")))
    admin_token_from_environment = bool(env("VELORIX_ADMIN_BEARER_TOKEN") or bearer_from_header(env("VELORIX_ADMIN_AUTH_HEADER")))
    api_token_from_auth_env = bool(
        auth_env_values.get("VELORIX_API_BEARER_TOKEN")
        or bearer_from_header(auth_env_values.get("VELORIX_API_AUTH_HEADER", ""))
    )
    admin_token_from_auth_env = bool(
        auth_env_values.get("VELORIX_ADMIN_BEARER_TOKEN")
        or bearer_from_header(auth_env_values.get("VELORIX_ADMIN_AUTH_HEADER", ""))
    )

    required_inputs = [
        ("VELORIX_PRODUCT_INGRESS_HOST", host),
        ("VELORIX_INGRESS_ENDPOINT_URL", endpoint),
        ("VELORIX_INGRESS_CONTROLLER", controller),
    ]
    if apply_ingress == "1":
        required_inputs.extend(
            [
                ("VELORIX_PRODUCT_INGRESS_CLASS", ingress_class),
                ("VELORIX_PRODUCT_INGRESS_TLS_SECRET", tls_secret),
            ]
        )

    for name, value in required_inputs:
        if not value:
            add_issue(missing, name, f"{name} is required for public ingress completion")
        elif has_placeholder(value):
            add_issue(invalid, name, f"{name} still contains a placeholder")

    if host and ("://" in host or "/" in host):
        add_issue(invalid, "VELORIX_PRODUCT_INGRESS_HOST", "host must not include scheme or path")
    elif host and not hostname_is_valid(host):
        add_issue(invalid, "VELORIX_PRODUCT_INGRESS_HOST", "host must be a valid DNS hostname")
    parsed = urlparse(endpoint)
    if endpoint and (parsed.scheme != "https" or not parsed.netloc):
        add_issue(invalid, "VELORIX_INGRESS_ENDPOINT_URL", "ingress endpoint must be an https URL")
    if endpoint and (parsed.query or parsed.fragment):
        add_issue(invalid, "VELORIX_INGRESS_ENDPOINT_URL", "ingress endpoint must not include query parameters or a fragment")
    if endpoint and host and parsed.hostname and parsed.hostname != host:
        add_issue(invalid, "VELORIX_INGRESS_ENDPOINT_URL", "ingress endpoint host must match VELORIX_PRODUCT_INGRESS_HOST")
    for name, value in [
        ("VELORIX_PRODUCT_INGRESS_APPLY", apply_ingress),
        ("VELORIX_PRODUCT_INGRESS_ATTEST", attest_ingress),
        ("VELORIX_PRODUCT_INGRESS_ATTACH", attach_ingress),
    ]:
        if value not in {"0", "1"}:
            add_issue(invalid, name, f"{name} must be 0 or 1")
    if attest_ingress == "1":
        if not api_token_from_environment and not api_token_from_auth_env:
            add_issue(
                missing,
                "VELORIX_API_BEARER_TOKEN",
                "public ingress attestation requires VELORIX_API_BEARER_TOKEN, VELORIX_API_AUTH_HEADER, or api-auth.env with a data-plane bearer token",
            )
        if not admin_token_from_environment and not admin_token_from_auth_env:
            add_issue(
                missing,
                "VELORIX_ADMIN_BEARER_TOKEN",
                "public ingress attestation requires VELORIX_ADMIN_BEARER_TOKEN, VELORIX_ADMIN_AUTH_HEADER, or api-auth.env with an admin bearer token",
            )

    required = mode == "1"
    ready = not missing and not invalid
    return {
        "mode": mode,
        "required": required,
        "ready": ready,
        "status": "ready" if ready else ("blocked" if required else "incomplete"),
        "missing": missing,
        "invalid": invalid,
        "env": {
            "VELORIX_PRODUCT_INGRESS_HOST": env_field("VELORIX_PRODUCT_INGRESS_HOST"),
            "VELORIX_PRODUCT_INGRESS_CLASS": env_field("VELORIX_PRODUCT_INGRESS_CLASS"),
            "VELORIX_PRODUCT_INGRESS_TLS_SECRET": env_field("VELORIX_PRODUCT_INGRESS_TLS_SECRET"),
            "VELORIX_INGRESS_ENDPOINT_URL": env_field("VELORIX_INGRESS_ENDPOINT_URL"),
            "VELORIX_INGRESS_CONTROLLER": env_field("VELORIX_INGRESS_CONTROLLER"),
            "VELORIX_PRODUCT_INGRESS_APPLY": env_field("VELORIX_PRODUCT_INGRESS_APPLY"),
            "VELORIX_PRODUCT_INGRESS_ATTEST": env_field("VELORIX_PRODUCT_INGRESS_ATTEST"),
            "VELORIX_PRODUCT_INGRESS_ATTACH": env_field("VELORIX_PRODUCT_INGRESS_ATTACH"),
            "VELORIX_API_AUTH_ENV": env_field("VELORIX_API_AUTH_ENV"),
            "VELORIX_API_BEARER_TOKEN": env_field("VELORIX_API_BEARER_TOKEN", secret=True),
            "VELORIX_ADMIN_BEARER_TOKEN": env_field("VELORIX_ADMIN_BEARER_TOKEN", secret=True),
            "VELORIX_API_AUTH_HEADER": env_field("VELORIX_API_AUTH_HEADER", secret=True),
            "VELORIX_ADMIN_AUTH_HEADER": env_field("VELORIX_ADMIN_AUTH_HEADER", secret=True),
        },
        "auth_token_source": {
            "api_token_from_environment": api_token_from_environment,
            "admin_token_from_environment": admin_token_from_environment,
            "api_token_from_auth_env": api_token_from_auth_env,
            "admin_token_from_auth_env": admin_token_from_auth_env,
            "auth_env_exists": auth_env_file.is_file(),
            "auth_env_file": str(auth_env_file),
        },
        "existing_ingress_mode": apply_ingress == "0",
        "delegates_to": "scripts/complete-vind-product-ingress.sh",
    }


def validate_durability(mode: str, durability_args: list[str], product: dict) -> dict:
    if product_durability_ready(product):
        return already_validated_step(
            mode,
            "already_validated",
            "scripts/complete-vind-object-store-durability.sh",
        )

    missing: list = []
    invalid: list = []
    review_flags = [
        "VELORIX_OBJECT_STORE_VERSIONING_OR_OBJECT_LOCK_ENABLED",
        "VELORIX_OBJECT_STORE_SERVER_SIDE_ENCRYPTION_ENABLED",
        "VELORIX_OBJECT_STORE_BACKUP_OR_REPLICATION_CONFIGURED",
        "VELORIX_OBJECT_STORE_LIFECYCLE_DELETE_POLICY_REVIEWED",
        "VELORIX_OBJECT_STORE_DESTRUCTIVE_DELETE_PROTECTION_REVIEWED",
        "VELORIX_OBJECT_STORE_COST_CONTROLS_REVIEWED",
    ]
    has_cli_args = bool(durability_args)
    for name in review_flags:
        value = bool_env(name)
        if value not in {"0", "1"}:
            add_issue(invalid, name, f"{name} must be 0 or 1")
        elif not has_cli_args and value != "1":
            add_issue(missing, name, f"{name}=1 or explicit durability CLI flag is required")
    for name in [
        "VELORIX_OBJECT_STORE_DURABILITY_ASSESS",
        "VELORIX_OBJECT_STORE_DURABILITY_ATTEST",
        "VELORIX_OBJECT_STORE_DURABILITY_ATTACH",
    ]:
        value = bool_env(name, "1")
        if value not in {"0", "1"}:
            add_issue(invalid, name, f"{name} must be 0 or 1")
    store = product.get("object_store") or {}
    authority = {
        "mode": store.get("mode"),
        "authority_store_id": store.get("authority_store_id"),
        "bucket": store.get("bucket"),
        "s3_prefix": store.get("s3_prefix"),
        "local_development_authority": store.get("local_development_authority"),
        "external_s3_bucket_validated": store.get("external_s3_bucket_validated"),
        "external_s3_prefix_validated": store.get("external_s3_prefix_validated"),
    }
    authority_ready = (
        store.get("mode") == "external-s3"
        and store.get("local_development_authority") is not True
        and store.get("external_s3_bucket_validated") is True
        and store.get("external_s3_prefix_validated") is True
    )
    if not authority_ready:
        add_issue(
            invalid,
            "object_store_external_authority",
            "validated nonlocal external S3/OSS authority is required before durability attestation",
        )
    required = mode == "1"
    ready = not invalid and (has_cli_args or not missing)
    return {
        "mode": mode,
        "required": required,
        "ready": ready,
        "status": "ready" if ready else ("blocked" if required else "incomplete"),
        "missing": [] if has_cli_args else missing,
        "invalid": invalid,
        "cli_args_count": len(durability_args),
        "env_review_flags": {
            name: env_field(name) for name in review_flags
        },
        "authority_ready": authority_ready,
        "authority": authority,
        "delegates_to": "scripts/complete-vind-object-store-durability.sh",
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--product-evidence", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--external-s3-mode", choices=["auto", "0", "1"], required=True)
    parser.add_argument("--ingress-mode", choices=["auto", "0", "1"], required=True)
    parser.add_argument("--durability-mode", choices=["auto", "0", "1"], required=True)
    parser.add_argument("--hiqlite-mode", choices=["auto", "0", "1"], required=True)
    parser.add_argument("durability_args", nargs=argparse.REMAINDER)
    args = parser.parse_args()

    product_path = Path(args.product_evidence)
    output_path = Path(args.output)
    product_dir = product_path.parent
    product = load_product(product_path)
    durability_args = args.durability_args
    if durability_args and durability_args[0] == "--":
        durability_args = durability_args[1:]

    steps = {
        "external_s3": validate_external_s3(args.external_s3_mode, product_dir, product)
        if args.external_s3_mode != "0"
        else {"mode": "0", "required": False, "ready": False, "status": "disabled"},
        "ingress": validate_ingress(args.ingress_mode, product, product_dir)
        if args.ingress_mode != "0"
        else {"mode": "0", "required": False, "ready": False, "status": "disabled"},
        "durability": validate_durability(args.durability_mode, durability_args, product)
        if args.durability_mode != "0"
        else {"mode": "0", "required": False, "ready": False, "status": "disabled"},
        "hiqlite_backend_time": {
            "mode": args.hiqlite_mode,
            "required": args.hiqlite_mode == "1",
            "ready": None,
            "status": "disabled" if args.hiqlite_mode == "0" else "deferred_to_release_preflight",
            "release_failover_requested": env("VELORIX_STANDING_RUNTIME_FAILOVER_RELEASE_ATTEST") == "1",
            "trusted_provenance_requested": env("VELORIX_HIQLITE_BACKEND_TIME_TRUSTED_PROVENANCE") == "1",
        },
    }
    forced_blockers = [
        {"step": name, "missing": step.get("missing") or [], "invalid": step.get("invalid") or []}
        for name, step in steps.items()
        if step.get("required") and step.get("status") == "blocked"
    ]
    payload = {
        "schema_version": 1,
        "report_kind": "velorix_complete_vind_product_input_preflight",
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "product_evidence": str(product_path),
        "steps": steps,
        "forced_blockers": forced_blockers,
        "status": "blocked" if forced_blockers else "pass",
        "secrets_redacted": True,
        "creates_product_complete_evidence": False,
    }
    write_json_atomic(output_path, payload)
    print(f"input_preflight={output_path}")
    print(f"input_preflight_status={payload['status']}")
    return 65 if forced_blockers else 0


if __name__ == "__main__":
    raise SystemExit(main())
