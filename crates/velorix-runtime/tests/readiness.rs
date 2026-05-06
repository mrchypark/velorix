use velorix_runtime::readiness::ProductionReadinessEvidenceV1;

#[test]
fn readiness_report_is_production_ready_when_all_required_evidence_passes() {
    let report =
        ProductionReadinessEvidenceV1::from_json_str(&readiness_json(true, true, false, true, &[]))
            .unwrap()
            .try_into_report()
            .unwrap();

    assert!(report.production_ready);
    assert!(report.blocking_reasons.is_empty());
}

#[test]
fn readiness_report_blocks_when_s3_compatible_evidence_is_missing() {
    let report = ProductionReadinessEvidenceV1::from_json_str(&readiness_json(
        false,
        true,
        false,
        true,
        &[],
    ))
    .unwrap()
    .try_into_report()
    .unwrap();

    assert!(!report.production_ready);
    assert_eq!(
        report.blocking_reasons,
        vec!["capability_status missing s3_compatible evidence"]
    );
}

#[test]
fn readiness_report_blocks_when_kubernetes_lease_client_evidence_is_missing() {
    let report = ProductionReadinessEvidenceV1::from_json_str(&readiness_json(
        true,
        false,
        false,
        true,
        &[],
    ))
    .unwrap()
    .try_into_report()
    .unwrap();

    assert!(!report.production_ready);
    assert_eq!(
        report.blocking_reasons,
        vec!["kubernetes_status missing kubernetes_lease_client evidence"]
    );
}

#[test]
fn readiness_report_blocks_bootstrap_raw_state_path() {
    let report =
        ProductionReadinessEvidenceV1::from_json_str(&readiness_json(true, true, true, true, &[]))
            .unwrap()
            .try_into_report()
            .unwrap();

    assert!(!report.production_ready);
    assert_eq!(
        report.blocking_reasons,
        vec!["state_status uses bootstrap raw state path"]
    );
}

#[test]
fn readiness_report_blocks_failed_statuses() {
    let report = ProductionReadinessEvidenceV1::from_json_str(&readiness_json(
        true,
        true,
        false,
        true,
        &["benchmark_gate_status"],
    ))
    .unwrap()
    .try_into_report()
    .unwrap();

    assert!(!report.production_ready);
    assert_eq!(
        report.blocking_reasons,
        vec!["benchmark_gate_status failed: regression gate failed closed"]
    );
}

#[test]
fn readiness_report_blocks_when_catalog_evidence_is_missing() {
    let report = ProductionReadinessEvidenceV1::from_json_str(&readiness_json(
        true,
        true,
        false,
        false,
        &[],
    ))
    .unwrap()
    .try_into_report()
    .unwrap();

    assert!(!report.production_ready);
    assert_eq!(
        report.blocking_reasons,
        vec![
            "query_policy_status missing query_policy_catalog evidence",
            "table_catalog_status missing registry_backed_table_catalog evidence",
            "feldera_artifact_status missing feldera_artifact_registry evidence",
        ]
    );
}

#[test]
fn readiness_evidence_rejects_unknown_json_fields() {
    let error = ProductionReadinessEvidenceV1::from_json_str(
        r#"{
            "schema_version": 1,
            "deployment_id": "prod-a",
            "authority_store_id": "s3://velorix-prod",
            "capability_status": { "status": "pass", "evidence": "s3-compatible capability probe", "evidence_kind": ["s3_compatible"] },
            "ownership_status": { "status": "pass", "evidence": "durable epoch record" },
            "checkpoint_status": { "status": "pass", "evidence": "published checkpoint lifecycle" },
            "state_status": { "status": "pass", "evidence": "SlateDB checkpoint ref" },
            "query_policy_status": { "status": "pass", "evidence": "bounded DataFusion policy", "evidence_kind": ["query_policy_catalog"] },
            "table_catalog_status": { "status": "pass", "evidence": "registry-backed table catalog", "evidence_kind": ["registry_backed_table_catalog"] },
            "feldera_artifact_status": { "status": "pass", "evidence": "trusted artifact metadata", "evidence_kind": ["feldera_artifact_registry"] },
            "benchmark_gate_status": { "status": "pass", "evidence": "S3-compatible benchmark gate" },
            "kubernetes_status": { "status": "pass", "evidence": "Kubernetes Lease client", "evidence_kind": ["kubernetes_lease_client"] },
            "surprise": true
        }"#,
    )
    .unwrap_err();

    assert!(error.to_string().contains("unknown field"));
}

#[test]
fn readiness_report_rejects_unsupported_schema_version() {
    let error = ProductionReadinessEvidenceV1::from_json_str(
        &readiness_json(true, true, false, true, &[])
            .replace("\"schema_version\": 1", "\"schema_version\": 2"),
    )
    .unwrap()
    .try_into_report()
    .unwrap_err();

    assert!(error.contains("unsupported readiness schema_version 2"));
}

fn readiness_json(
    include_s3_evidence: bool,
    include_kubernetes_lease_evidence: bool,
    include_bootstrap_raw_state_path: bool,
    include_catalog_evidence: bool,
    failed_fields: &[&str],
) -> String {
    let capability_kind = if include_s3_evidence {
        r#", "evidence_kind": ["s3_compatible"]"#
    } else {
        ""
    };
    let kubernetes_kind = if include_kubernetes_lease_evidence {
        r#", "evidence_kind": ["kubernetes_lease_client"]"#
    } else {
        ""
    };
    let state_kind = if include_bootstrap_raw_state_path {
        r#", "evidence_kind": ["bootstrap_raw_state_path"]"#
    } else {
        ""
    };
    let query_policy_kind = if include_catalog_evidence {
        r#", "evidence_kind": ["query_policy_catalog"]"#
    } else {
        ""
    };
    let table_catalog_kind = if include_catalog_evidence {
        r#", "evidence_kind": ["registry_backed_table_catalog"]"#
    } else {
        ""
    };
    let feldera_artifact_kind = if include_catalog_evidence {
        r#", "evidence_kind": ["feldera_artifact_registry"]"#
    } else {
        ""
    };

    format!(
        r#"{{
            "schema_version": 1,
            "deployment_id": "prod-a",
            "authority_store_id": "s3://velorix-prod",
            "capability_status": {{ "status": "pass", "evidence": "s3-compatible capability probe"{capability_kind} }},
            "ownership_status": {{ "status": "pass", "evidence": "durable epoch record" }},
            "checkpoint_status": {{ "status": "pass", "evidence": "published checkpoint lifecycle" }},
            "state_status": {{ "status": "pass", "evidence": "SlateDB checkpoint ref"{state_kind} }},
            "query_policy_status": {{ "status": "pass", "evidence": "bounded DataFusion policy"{query_policy_kind} }},
            "table_catalog_status": {{ "status": "pass", "evidence": "registry-backed table catalog"{table_catalog_kind} }},
            "feldera_artifact_status": {{ "status": "pass", "evidence": "trusted artifact metadata"{feldera_artifact_kind} }},
            "benchmark_gate_status": {{ "status": "{benchmark_status}", "evidence": "{benchmark_evidence}" }},
            "kubernetes_status": {{ "status": "pass", "evidence": "Kubernetes Lease client"{kubernetes_kind} }}
        }}"#,
        benchmark_status = status_for("benchmark_gate_status", failed_fields),
        benchmark_evidence = evidence_for("benchmark_gate_status", failed_fields),
    )
}

fn status_for(field: &str, failed_fields: &[&str]) -> &'static str {
    if failed_fields.contains(&field) {
        "fail"
    } else {
        "pass"
    }
}

fn evidence_for(field: &str, failed_fields: &[&str]) -> &'static str {
    if failed_fields.contains(&field) {
        "regression gate failed closed"
    } else {
        "S3-compatible benchmark gate"
    }
}
