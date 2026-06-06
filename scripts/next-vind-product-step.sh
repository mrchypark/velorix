#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
report_file="${VELORIX_PRODUCT_COMPLETION_REPORT:-${VELORIX_VIND_PRODUCT_DIR:-target/velorix-product}/product-completion-report.json}"
json_output=0
doctor_output=0
fail_on_incomplete=0

usage() {
  cat <<'EOF'
Print the next actionable step for completing a vind product slice.

Usage:
  scripts/next-vind-product-step.sh [options]

Options:
  --report PATH          Product completion report path.
                         Default: target/velorix-product/product-completion-report.json
  --json                 Print structured JSON instead of text.
  --doctor               Print a redacted operator checklist for the next step.
  --fail-on-incomplete   Exit 75 when product_complete is not true.
  -h, --help             Show this help.

This helper is read-only. It does not create product-complete evidence, does
not print secret values, and does not create PVCs.
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --report)
      report_file="${2:-}"
      if [ -z "$report_file" ]; then
        echo "--report requires a path" >&2
        exit 64
      fi
      shift 2
      ;;
    --json)
      json_output=1
      shift
      ;;
    --doctor)
      doctor_output=1
      shift
      ;;
    --fail-on-incomplete)
      fail_on_incomplete=1
      shift
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      echo "unknown option: $1" >&2
      usage >&2
      exit 64
      ;;
  esac
done

cd "$repo_root"

python3 - "$report_file" "$json_output" "$doctor_output" "$fail_on_incomplete" <<'PY'
import json
import sys
from pathlib import Path

report_path = Path(sys.argv[1])
json_output = sys.argv[2] == "1"
doctor_output = sys.argv[3] == "1"
fail_on_incomplete = sys.argv[4] == "1"

EXECUTION_TO_GATE = {
    "external_s3": "object_store_external_authority",
    "ingress": "public_ingress_tls_auth",
    "durability": "object_store_durability_policy",
    "hiqlite_backend_time": "hiqlite_backend_time_release",
}
STEP_TITLES = {
    "external_s3": "Provide and validate nonlocal S3/OSS authority",
    "ingress": "Provide and attest public ingress/TLS/auth",
    "durability": "Review and attest object-store durability policy",
    "hiqlite_backend_time": "Provide trusted Hiqlite release provenance",
    "local_evidence": "Refresh local product evidence",
    "final_report": "Regenerate product completion report",
}


def load_report(path: Path) -> dict:
    if not path.is_file():
        raise SystemExit(f"missing product completion report: {path}")
    with path.open("r", encoding="utf-8") as f:
        value = json.load(f)
    if not isinstance(value, dict):
        raise SystemExit(f"product completion report must be a JSON object: {path}")
    return value


def gate_steps_by_id(report: dict) -> dict:
    plan = report.get("completion_plan") or {}
    return {
        step.get("id"): step
        for step in plan.get("steps") or []
        if isinstance(step, dict) and step.get("id")
    }


def execution_steps(report: dict) -> dict:
    execution = report.get("completion_execution_plan") or {}
    return {
        name: step
        for name, step in (execution.get("steps") or {}).items()
        if isinstance(step, dict)
    }


def step_subjects(step: dict) -> dict:
    return {
        "missing_subjects": step.get("missing_subjects") or [],
        "invalid_subjects": step.get("invalid_subjects") or [],
    }


def input_summary_requires_input(summary: dict) -> bool:
    if not isinstance(summary, dict):
        return False
    if (summary.get("placeholder_count") or 0) > 0:
        return True
    for step in summary.get("preflight_steps") or []:
        if not isinstance(step, dict):
            continue
        if (step.get("missing_count") or 0) > 0 or (step.get("invalid_count") or 0) > 0:
            return True
        if step.get("status") in {"blocked", "incomplete"} or step.get("ready") is False:
            return True
    release = summary.get("release_preflight") or {}
    if isinstance(release, dict) and (
        (release.get("missing_count") or 0) > 0 or (release.get("invalid_count") or 0) > 0
    ):
        return True
    return False


def effective_gate_state(gate: dict):
    if not isinstance(gate, dict):
        return None
    if gate.get("status") == "out_of_scope":
        return "out_of_scope"
    if gate.get("input_summary_requires_input") is True:
        return "input_required"
    if gate.get("state") == "runnable" and input_summary_requires_input(gate.get("input_summary")):
        return "input_required"
    return gate.get("state")


def execution_step_gate(report: dict, execution_step: str):
    gate_id = EXECUTION_TO_GATE.get(execution_step)
    if not gate_id:
        return None
    return gate_steps_by_id(report).get(gate_id)


def execution_step_gate_state(report: dict, execution_step: str):
    return effective_gate_state(execution_step_gate(report, execution_step))


def execution_step_requires_gate_input(report: dict, execution_step: str) -> bool:
    gate = execution_step_gate(report, execution_step)
    if not isinstance(gate, dict):
        return False
    return effective_gate_state(gate) == "input_required" or input_summary_requires_input(
        gate.get("input_summary")
    )


def command_for(report: dict, execution_step: str, gate):
    handoff = report.get("completion_handoff") or {}
    env_file = handoff.get("env_file")
    if execution_step in {"local_evidence", "final_report"}:
        return handoff.get("next_action")
    if gate and (
        effective_gate_state(gate) in {"input_required", "waiting_on_prerequisite", "out_of_scope"}
        or input_summary_requires_input(gate.get("input_summary"))
    ):
        return gate.get("next_action") or handoff.get("next_action")
    if execution_step in set((report.get("completion_execution_plan") or {}).get("will_run_steps") or []):
        if env_file:
            return f"scripts/complete-vind-product.sh --env-file {env_file}"
        return "scripts/complete-vind-product.sh"
    if gate and gate.get("next_action"):
        return gate.get("next_action")
    return handoff.get("next_action")


def build_step(report: dict, execution_step: str, state_override=None) -> dict:
    exec_steps = execution_steps(report)
    gate_steps = gate_steps_by_id(report)
    execution = exec_steps.get(execution_step) or {}
    gate_id = EXECUTION_TO_GATE.get(execution_step)
    gate = gate_steps.get(gate_id) if gate_id else None
    reported_gate_state = (gate or {}).get("state")
    gate_state = effective_gate_state(gate)
    state = state_override or execution.get("state") or gate_state
    payload = {
        "id": execution_step,
        "gate": gate_id,
        "title": STEP_TITLES.get(execution_step, execution_step),
        "state": state,
        "execution_state": execution.get("state"),
        "gate_state": gate_state,
        "status": execution.get("status") or (gate or {}).get("status"),
        "helper": execution.get("helper"),
        "waiting_on": execution.get("waiting_on") or (gate or {}).get("waiting_on") or [],
        "will_run": execution.get("will_run") is True,
        "command": command_for(report, execution_step, gate),
        "summary": (gate or {}).get("summary"),
        **step_subjects(execution),
    }
    input_summary = (gate or {}).get("input_summary") or {}
    if input_summary:
        payload["placeholder_count"] = input_summary.get("placeholder_count")
        payload["secret_placeholder_count"] = input_summary.get("secret_placeholder_count")
        payload["input_summary"] = input_summary
    if reported_gate_state != gate_state:
        payload["reported_gate_state"] = reported_gate_state
    return payload


def issue_lines(kind: str, issues):
    lines = []
    for issue in issues or []:
        if not isinstance(issue, dict):
            continue
        subject = issue.get("subject")
        detail = issue.get("detail")
        if subject and detail:
            lines.append(f"{kind}={subject}: {detail}")
        elif subject:
            lines.append(f"{kind}={subject}")
    return lines


def redacted_env_value(summary: dict, name: str):
    if not isinstance(summary, dict):
        return None
    for step in summary.get("preflight_steps") or []:
        if not isinstance(step, dict):
            continue
        env_fields = step.get("env") or {}
        field = env_fields.get(name)
        if not isinstance(field, dict):
            continue
        if field.get("secret") is True:
            return "<secret>"
        return field.get("value")
    return None


def doctor_guidance_lines(next_step: dict, summary: dict):
    step_id = next_step.get("id")
    if step_id == "external_s3":
        credentials_managed = redacted_env_value(summary, "VELORIX_S3_CREDENTIALS_SECRET_MANAGED")
        prefix = redacted_env_value(summary, "VELORIX_S3_PREFIX")
        secret_name = redacted_env_value(summary, "VELORIX_S3_CREDENTIALS_SECRET_NAME")
        return [
            "guidance[external_s3].endpoint=Set AWS_ENDPOINT_URL to the S3/OSS service endpoint only, without bucket, prefix, query, or fragment; localhost, raw private IP, loopback, link-local, and local-development authorities are rejected by default.",
            "guidance[external_s3].bucket=Set VELORIX_S3_BUCKET to the existing provider bucket; this helper does not create buckets.",
            f"guidance[external_s3].prefix=Keep VELORIX_S3_PREFIX stable between validate-only and execution; use a safe object prefix with no empty segments or '..'; current value is {prefix or '<unset>'}.",
            "guidance[external_s3].path_style=Leave VELORIX_S3_FORCE_PATH_STYLE=1 for most S3-compatible OSS endpoints unless the provider requires virtual-hosted style.",
            f"guidance[external_s3].credential_mode=VELORIX_S3_CREDENTIALS_SECRET_MANAGED={credentials_managed or '<unset>'}.",
            "guidance[external_s3].env_precedence=Values from --env-file are defaults; caller environment variables override them.",
            "guidance[external_s3].managed_secret=When VELORIX_S3_CREDENTIALS_SECRET_MANAGED=1, provide AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in the effective environment or env file; AWS_SESSION_TOKEN is optional for temporary credentials.",
            f"guidance[external_s3].existing_secret=When VELORIX_S3_CREDENTIALS_SECRET_MANAGED=0, leave AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY/AWS_SESSION_TOKEN unset in the effective environment and create Kubernetes Secret {secret_name or '<secret-name>'} in VELORIX_K8S_NAMESPACE, default velorix-product, with access-key-id, secret-access-key, and optional session-token keys.",
            "guidance[external_s3].existing_secret_validation=Validate-only checks input shape; real execution checks the Kubernetes Secret exists and has the required keys.",
            "guidance[external_s3].authority=Leave VELORIX_AUTHORITY_STORE_ID unset unless overriding; if set, it must equal s3://external/${VELORIX_S3_BUCKET}/${VELORIX_S3_PREFIX}.",
            "guidance[external_s3].sequence=Run the command exactly as shown: validate-only first, then execute with the same env file/output dir.",
        ]
    if step_id == "ingress":
        apply_ingress = redacted_env_value(summary, "VELORIX_PRODUCT_INGRESS_APPLY")
        return [
            "guidance[ingress].host=Set VELORIX_PRODUCT_INGRESS_HOST to the public DNS hostname only, without scheme, port, or path.",
            "guidance[ingress].endpoint=Set VELORIX_INGRESS_ENDPOINT_URL to https://${VELORIX_PRODUCT_INGRESS_HOST}; the endpoint host must match the ingress host.",
            f"guidance[ingress].apply_mode=VELORIX_PRODUCT_INGRESS_APPLY={apply_ingress or '<unset>'}; use 1 to apply a Kubernetes Ingress and require class/TLS Secret, or 0 when public ingress, DNS, and TLS Secret are managed outside this helper.",
            "guidance[ingress].tls=When apply mode is 1, set VELORIX_PRODUCT_INGRESS_CLASS and VELORIX_PRODUCT_INGRESS_TLS_SECRET to existing cluster values; this helper does not issue public certificates.",
            "guidance[ingress].auth=Attestation requires data-plane and admin bearer tokens from VELORIX_API_BEARER_TOKEN/VELORIX_ADMIN_BEARER_TOKEN, auth headers, or api-auth.env; keep shell xtrace disabled.",
            "guidance[ingress].sequence=Run validate-only first, then execute with the same env file and output dir after DNS/TLS route to the velorix-api service is reachable.",
        ]
    if step_id == "durability":
        return [
            "guidance[durability].prerequisite=This gate is accepted only after object_store_external_authority proves a nonlocal external S3/OSS authority for the same product evidence.",
            "guidance[durability].review_flags=Set every VELORIX_OBJECT_STORE_*_REVIEWED/ENABLED durability flag to 1 only after operator review, or pass the explicit durability CLI flags to the helper.",
            "guidance[durability].scope=The helper records and attaches an operator review; it does not create buckets, lifecycle rules, replication, encryption policy, object lock, or PVCs.",
            "guidance[durability].cost=Cost-controls review must cover retention, lifecycle/delete policy, replication or backup scope, and expected object churn for the chosen prefix.",
            "guidance[durability].sequence=After external_s3 passes, run validate-only and then execute scripts/complete-vind-object-store-durability.sh against the same product evidence directory.",
        ]
    if step_id == "hiqlite_backend_time":
        return [
            "guidance[hiqlite_backend_time].scope=Product-complete Hiqlite backend-time evidence is release-CI scoped; the local diagnostic attestation is intentionally not enough.",
            "guidance[hiqlite_backend_time].env_template=Use hiqlite-backend-time-release.env as the release input template and replace every REPLACE_WITH_* value before validation.",
            "guidance[hiqlite_backend_time].release_identity=VELORIX_SOURCE_REVISION and VELORIX_RELEASE_COMMIT must be the same 40-character Velorix release commit; do not use metadata_store.hiqlite_authority_attestation.source_revision.",
            "guidance[hiqlite_backend_time].sigstore=Provide VELORIX_CI_SIGSTORE_BUNDLE_BASE64 and matching VELORIX_CI_SIGSTORE_BUNDLE_SHA256 from trusted release CI; the bundle is treated as secret and xtrace is refused by the preflight.",
            "guidance[hiqlite_backend_time].failover=Release-scoped standing-runtime failover evidence must prove trusted_for_product_complete=true, authority time observed, and post-failover owner epoch increase.",
            "guidance[hiqlite_backend_time].sequence=Run scripts/check-hiqlite-backend-time-release-inputs.sh with the release env file, then regenerate scripts/attest-hiqlite-backend-time.sh with trusted provenance in release CI.",
        ]
    return []


def render_doctor(result: dict) -> str:
    lines = [
        f"state={result['state']}",
        f"product_complete={str(result['product_complete']).lower()}",
        f"reason={result['reason']}",
    ]
    next_step = result.get("next_step")
    if not next_step:
        return "\n".join(lines)
    lines.extend(
        [
            f"next_step={next_step['id']}",
            f"title={next_step['title']}",
        ]
    )
    for key in ["gate", "state", "execution_state", "gate_state", "reported_gate_state", "status", "helper"]:
        if next_step.get(key) is not None:
            lines.append(f"{key}={next_step[key]}")
    if next_step.get("waiting_on"):
        lines.append("waiting_on=" + ",".join(next_step["waiting_on"]))

    summary = next_step.get("input_summary") or {}
    if summary:
        lines.append(f"placeholder_count={summary.get('placeholder_count', 0)}")
        if summary.get("placeholders"):
            lines.append("placeholders=" + ",".join(summary["placeholders"]))
        lines.append(f"secret_placeholder_count={summary.get('secret_placeholder_count', 0)}")
        if summary.get("secret_placeholders"):
            lines.append("secret_placeholders=" + ",".join(summary["secret_placeholders"]))
        for step in summary.get("preflight_steps") or []:
            if not isinstance(step, dict):
                continue
            name = step.get("step") or "unknown"
            lines.append(
                f"preflight[{name}]=status:{step.get('status')} ready:{step.get('ready')} "
                f"missing:{step.get('missing_count', 0)} invalid:{step.get('invalid_count', 0)}"
            )
            if step.get("missing_subjects"):
                lines.append(f"preflight[{name}].missing_subjects=" + ",".join(step["missing_subjects"]))
            if step.get("invalid_subjects"):
                lines.append(f"preflight[{name}].invalid_subjects=" + ",".join(step["invalid_subjects"]))
            lines.extend(issue_lines(f"preflight[{name}].missing", step.get("missing")))
            lines.extend(issue_lines(f"preflight[{name}].invalid", step.get("invalid")))
            auth_source = step.get("auth_token_source") or {}
            if isinstance(auth_source, dict):
                for source_name in sorted(auth_source):
                    value = auth_source.get(source_name)
                    if isinstance(value, bool):
                        rendered = str(value).lower()
                    else:
                        rendered = str(value)
                    lines.append(f"preflight[{name}].auth_token_source.{source_name}={rendered}")
            if "authority_ready" in step:
                lines.append(f"preflight[{name}].authority_ready={str(step.get('authority_ready') is True).lower()}")
            authority = step.get("authority") or {}
            if isinstance(authority, dict):
                for field_name in sorted(authority):
                    value = authority.get(field_name)
                    if value is None:
                        continue
                    if isinstance(value, bool):
                        rendered = str(value).lower()
                    else:
                        rendered = str(value)
                    lines.append(f"preflight[{name}].authority.{field_name}={rendered}")
            env_fields = step.get("env") or {}
            for env_name in sorted(env_fields):
                field = env_fields.get(env_name) or {}
                if not isinstance(field, dict):
                    continue
                parts = [
                    f"present:{str(field.get('present') is True).lower()}",
                    f"placeholder:{str(field.get('placeholder') is True).lower()}",
                ]
                if field.get("secret") is True:
                    parts.append("secret:true")
                    if field.get("length") is not None:
                        parts.append(f"length:{field.get('length')}")
                elif field.get("value") is not None:
                    parts.append(f"value:{field.get('value')}")
                lines.append(f"preflight[{name}].env.{env_name}=" + " ".join(parts))
            review_fields = step.get("env_review_flags") or {}
            for env_name in sorted(review_fields):
                field = review_fields.get(env_name) or {}
                if not isinstance(field, dict):
                    continue
                parts = [
                    f"present:{str(field.get('present') is True).lower()}",
                    f"placeholder:{str(field.get('placeholder') is True).lower()}",
                ]
                if field.get("value") is not None:
                    parts.append(f"value:{field.get('value')}")
                lines.append(f"preflight[{name}].review.{env_name}=" + " ".join(parts))
        release = summary.get("release_preflight") or {}
        if isinstance(release, dict) and release:
            lines.append(
                f"release_preflight=status:{release.get('status')} "
                f"missing:{release.get('missing_count', 0)} invalid:{release.get('invalid_count', 0)}"
            )
            if release.get("missing_subjects"):
                lines.append("release_preflight.missing_subjects=" + ",".join(release["missing_subjects"]))
            if release.get("invalid_subjects"):
                lines.append("release_preflight.invalid_subjects=" + ",".join(release["invalid_subjects"]))
            lines.extend(issue_lines("release_preflight.missing", release.get("missing")))
            lines.extend(issue_lines("release_preflight.invalid", release.get("invalid")))
        lines.extend(doctor_guidance_lines(next_step, summary))
    else:
        if next_step.get("invalid_subjects"):
            lines.append("invalid_subjects=" + ",".join(next_step["invalid_subjects"]))
        if next_step.get("missing_subjects"):
            lines.append("missing_subjects=" + ",".join(next_step["missing_subjects"]))
    if next_step.get("command"):
        lines.append(f"command={next_step['command']}")
    lines.append("secrets_redacted=true")
    lines.append("creates_product_complete_evidence=false")
    return "\n".join(lines)


def choose_next(report: dict) -> dict:
    execution = report.get("completion_execution_plan") or {}
    run_order = execution.get("run_order") or []
    will_run = set(execution.get("will_run_steps") or [])
    blocked = set(execution.get("blocked_steps") or [])
    waiting = set(execution.get("waiting_steps") or [])
    if report.get("product_complete") is True:
        return {
            "state": "complete",
            "next_step": None,
            "reason": "product-completion-report.json proves product_complete=true",
        }
    for step in run_order:
        if step in will_run and step not in {"local_evidence", "final_report"}:
            gate_state = execution_step_gate_state(report, step)
            if gate_state in {"input_required", "waiting_on_prerequisite", "out_of_scope"}:
                continue
            if execution_step_requires_gate_input(report, step):
                continue
            return {
                "state": "ready_to_execute",
                "next_step": build_step(report, step, "ready_to_execute"),
                "reason": "At least one completion helper is ready to run through the top-level driver.",
            }
    for step in run_order:
        if step in blocked:
            if execution_step_gate_state(report, step) == "out_of_scope":
                continue
            return {
                "state": "input_required",
                "next_step": build_step(report, step, "input_required"),
                "reason": "The earliest blocked completion step needs concrete external input.",
            }
    for step in run_order:
        if step in waiting:
            return {
                "state": "waiting_on_prerequisite",
                "next_step": build_step(report, step, "waiting_on_prerequisite"),
                "reason": "The next step is waiting for an earlier prerequisite gate.",
            }
    plan = report.get("completion_plan") or {}
    for gate_id in plan.get("input_required_steps") or []:
        for execution_step, mapped_gate in EXECUTION_TO_GATE.items():
            if mapped_gate == gate_id:
                return {
                    "state": "input_required",
                    "next_step": build_step(report, execution_step, "input_required"),
                "reason": "The gate-oriented completion plan still has required input.",
            }
    for step in run_order:
        if step in will_run:
            return {
                "state": "ready_to_execute",
                "next_step": build_step(report, step, "ready_to_execute"),
                "reason": "Only local/report refresh helpers are ready; no external completion helper is ready.",
            }
    return {
        "state": "blocked_without_action",
        "next_step": None,
        "reason": "No runnable, blocked, waiting, or input-required step was found in the report.",
    }


report = load_report(report_path)
result = {
    "schema_version": 1,
    "report_kind": "velorix_next_vind_product_step",
    "product_completion_report": str(report_path),
    "product_complete": report.get("product_complete") is True,
    "gate_summary": report.get("gate_summary") or {},
    **choose_next(report),
    "creates_product_complete_evidence": False,
    "secrets_redacted": True,
}

if json_output:
    print(json.dumps(result, indent=2, sort_keys=True))
elif doctor_output:
    print(render_doctor(result))
else:
    print(f"state={result['state']}")
    print(f"product_complete={str(result['product_complete']).lower()}")
    print(f"reason={result['reason']}")
    next_step = result.get("next_step")
    if next_step:
        print(f"next_step={next_step['id']}")
        if next_step.get("gate"):
            print(f"gate={next_step['gate']}")
        print(f"title={next_step['title']}")
        if next_step.get("waiting_on"):
            print("waiting_on=" + ",".join(next_step["waiting_on"]))
        if next_step.get("invalid_subjects"):
            print("invalid_subjects=" + ",".join(next_step["invalid_subjects"]))
        if next_step.get("missing_subjects"):
            print("missing_subjects=" + ",".join(next_step["missing_subjects"]))
        if next_step.get("command"):
            print(f"command={next_step['command']}")

if fail_on_incomplete and result["state"] != "complete":
    raise SystemExit(75)
PY
