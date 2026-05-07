use std::path::PathBuf;

use velorix_runtime::readiness::{
    verify_feldera_artifact_release_provenance_evidence, ProductionReadinessEvidenceV1,
};

#[test]
fn readiness_report_is_production_ready_when_all_required_evidence_passes() {
    let report = ProductionReadinessEvidenceV1::from_json_str(&readiness_json(&[], false, &[]))
        .unwrap()
        .try_into_report()
        .unwrap();

    assert!(report.production_ready);
    assert!(report.blocking_reasons.is_empty());
}

#[test]
fn readiness_report_blocks_when_s3_compatible_evidence_is_missing() {
    let report = ProductionReadinessEvidenceV1::from_json_str(&readiness_json(
        &["s3_compatible"],
        false,
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
        &["kubernetes_lease_client"],
        false,
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
fn readiness_report_blocks_when_durable_ownership_epoch_record_evidence_is_missing() {
    let report = ProductionReadinessEvidenceV1::from_json_str(&readiness_json(
        &["durable_ownership_epoch_record"],
        false,
        &[],
    ))
    .unwrap()
    .try_into_report()
    .unwrap();

    assert!(!report.production_ready);
    assert_eq!(
        report.blocking_reasons,
        vec!["ownership_status missing durable_ownership_epoch_record evidence"]
    );
}

#[test]
fn readiness_report_blocks_when_published_checkpoint_lifecycle_record_evidence_is_missing() {
    let report = ProductionReadinessEvidenceV1::from_json_str(&readiness_json(
        &["published_checkpoint_lifecycle_record"],
        false,
        &[],
    ))
    .unwrap()
    .try_into_report()
    .unwrap();

    assert!(!report.production_ready);
    assert_eq!(
        report.blocking_reasons,
        vec!["checkpoint_status missing published_checkpoint_lifecycle_record evidence"]
    );
}

#[test]
fn readiness_report_blocks_when_slate_db_checkpoint_ref_evidence_is_missing() {
    let report = ProductionReadinessEvidenceV1::from_json_str(&readiness_json(
        &["slate_db_checkpoint_ref"],
        false,
        &[],
    ))
    .unwrap()
    .try_into_report()
    .unwrap();

    assert!(!report.production_ready);
    assert_eq!(
        report.blocking_reasons,
        vec!["state_status missing slate_db_checkpoint_ref evidence"]
    );
}

#[test]
fn readiness_report_blocks_when_s3_compatible_benchmark_gate_evidence_is_missing() {
    let report = ProductionReadinessEvidenceV1::from_json_str(&readiness_json(
        &["s3_compatible_benchmark_gate"],
        false,
        &[],
    ))
    .unwrap()
    .try_into_report()
    .unwrap();

    assert!(!report.production_ready);
    assert_eq!(
        report.blocking_reasons,
        vec!["benchmark_gate_status missing s3_compatible_benchmark_gate evidence"]
    );
}

#[test]
fn readiness_report_blocks_when_dependency_governance_evidence_is_missing() {
    let report = ProductionReadinessEvidenceV1::from_json_str(&readiness_json(
        &["dependency_governance_validated"],
        false,
        &[],
    ))
    .unwrap()
    .try_into_report()
    .unwrap();

    assert!(!report.production_ready);
    assert_eq!(
        report.blocking_reasons,
        vec!["dependency_governance_status missing dependency_governance_validated evidence"]
    );
}

#[test]
fn readiness_report_blocks_bootstrap_raw_state_path() {
    let report = ProductionReadinessEvidenceV1::from_json_str(&readiness_json(&[], true, &[]))
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
        &[],
        false,
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
fn readiness_report_blocks_failed_dependency_governance_status() {
    let report = ProductionReadinessEvidenceV1::from_json_str(&readiness_json(
        &[],
        false,
        &["dependency_governance_status"],
    ))
    .unwrap()
    .try_into_report()
    .unwrap();

    assert!(!report.production_ready);
    assert_eq!(
        report.blocking_reasons,
        vec!["dependency_governance_status failed: dependency governance failed closed"]
    );
}

#[test]
fn readiness_report_blocks_when_catalog_evidence_is_missing() {
    let report = ProductionReadinessEvidenceV1::from_json_str(&readiness_json(
        &[
            "query_policy_catalog",
            "registry_backed_table_catalog",
            "feldera_artifact_registry",
            "feldera_artifact_hash_verified",
        ],
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
            "feldera_artifact_status missing feldera_artifact_hash_verified evidence",
        ]
    );
}

#[test]
fn readiness_report_blocks_when_feldera_artifact_hash_verified_evidence_is_missing() {
    let report = ProductionReadinessEvidenceV1::from_json_str(&readiness_json(
        &["feldera_artifact_hash_verified"],
        false,
        &[],
    ))
    .unwrap()
    .try_into_report()
    .unwrap();

    assert!(!report.production_ready);
    assert_eq!(
        report.blocking_reasons,
        vec!["feldera_artifact_status missing feldera_artifact_hash_verified evidence"]
    );
}

#[test]
fn readiness_report_blocks_when_feldera_artifact_release_provenance_evidence_is_missing() {
    let report = ProductionReadinessEvidenceV1::from_json_str(&readiness_json(
        &["feldera_artifact_release_provenance"],
        false,
        &[],
    ))
    .unwrap()
    .try_into_report()
    .unwrap();

    assert!(!report.production_ready);
    assert_eq!(
        report.blocking_reasons,
        vec!["feldera_artifact_status missing feldera_artifact_release_provenance evidence"]
    );
}

#[test]
fn feldera_release_provenance_verifier_outputs_stable_readiness_evidence() {
    let metadata_json = fixture_json("compile_artifact_valid");
    let provenance_json = fixture_json("release_provenance_valid");

    let evidence =
        verify_feldera_artifact_release_provenance_evidence(&metadata_json, &provenance_json)
            .unwrap();
    let json = serde_json::to_value(evidence).unwrap();

    assert_eq!(
        json,
        serde_json::json!({
            "schema_version": 1,
            "status": "pass",
            "evidence_kind": "feldera_artifact_release_provenance",
            "release_id": "velorix-feldera-release-20260507",
            "release_version": "1.0.0-rc.1",
            "build_id": "feldera-build-20260507T000000Z",
            "builder_id": "github-actions/feldera-artifacts",
            "artifact_id": "feldera-artifact-orders-by-region-20260503",
            "artifact_hash": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "spec_hash": "velorix-feldera-spec-sha256-v1:0e24cbe06543d735a6d62868f230c4610fb9139cb91e5e8f72042f17da0ecbea",
            "generated_rust_abi_version": "feldera-generated-rust-abi-v1",
            "generated_rust_crate_name": "orders_by_region_pipeline",
            "source_repository": "https://github.com/mrchypark/velorix",
            "source_revision": "0123456789abcdef0123456789abcdef01234567"
        })
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
            "ownership_status": { "status": "pass", "evidence": "durable epoch record", "evidence_kind": ["durable_ownership_epoch_record"] },
            "checkpoint_status": { "status": "pass", "evidence": "published checkpoint lifecycle", "evidence_kind": ["published_checkpoint_lifecycle_record"] },
            "state_status": { "status": "pass", "evidence": "SlateDB checkpoint ref", "evidence_kind": ["slate_db_checkpoint_ref"] },
            "query_policy_status": { "status": "pass", "evidence": "bounded DataFusion policy", "evidence_kind": ["query_policy_catalog"] },
            "table_catalog_status": { "status": "pass", "evidence": "registry-backed table catalog", "evidence_kind": ["registry_backed_table_catalog"] },
            "feldera_artifact_status": { "status": "pass", "evidence": "trusted artifact metadata", "evidence_kind": ["feldera_artifact_registry", "feldera_artifact_hash_verified"] },
            "dependency_governance_status": { "status": "pass", "evidence": "dependency governance validated", "evidence_kind": ["dependency_governance_validated"] },
            "benchmark_gate_status": { "status": "pass", "evidence": "S3-compatible benchmark gate", "evidence_kind": ["s3_compatible_benchmark_gate"] },
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
        &readiness_json(&[], false, &[]).replace("\"schema_version\": 1", "\"schema_version\": 2"),
    )
    .unwrap()
    .try_into_report()
    .unwrap_err();

    assert!(error.contains("unsupported readiness schema_version 2"));
}

fn readiness_json(
    missing_evidence: &[&str],
    include_bootstrap_raw_state_path: bool,
    failed_fields: &[&str],
) -> String {
    let capability_kind = if !missing_evidence.contains(&"s3_compatible") {
        r#", "evidence_kind": ["s3_compatible"]"#
    } else {
        ""
    };
    let ownership_kind = if !missing_evidence.contains(&"durable_ownership_epoch_record") {
        r#", "evidence_kind": ["durable_ownership_epoch_record"]"#
    } else {
        ""
    };
    let checkpoint_kind = if !missing_evidence.contains(&"published_checkpoint_lifecycle_record") {
        r#", "evidence_kind": ["published_checkpoint_lifecycle_record"]"#
    } else {
        ""
    };
    let kubernetes_kind = if !missing_evidence.contains(&"kubernetes_lease_client") {
        r#", "evidence_kind": ["kubernetes_lease_client"]"#
    } else {
        ""
    };
    let mut state_evidence_kind = Vec::new();
    if include_bootstrap_raw_state_path {
        state_evidence_kind.push("bootstrap_raw_state_path");
    }
    if !missing_evidence.contains(&"slate_db_checkpoint_ref") {
        state_evidence_kind.push("slate_db_checkpoint_ref");
    }
    let state_kind = if state_evidence_kind.is_empty() {
        String::new()
    } else {
        format!(r#", "evidence_kind": {:?}"#, state_evidence_kind)
    };
    let query_policy_kind = if !missing_evidence.contains(&"query_policy_catalog") {
        r#", "evidence_kind": ["query_policy_catalog"]"#
    } else {
        ""
    };
    let table_catalog_kind = if !missing_evidence.contains(&"registry_backed_table_catalog") {
        r#", "evidence_kind": ["registry_backed_table_catalog"]"#
    } else {
        ""
    };
    let mut feldera_artifact_evidence_kind = Vec::new();
    if !missing_evidence.contains(&"feldera_artifact_registry") {
        feldera_artifact_evidence_kind.push("feldera_artifact_registry");
    }
    if !missing_evidence.contains(&"feldera_artifact_hash_verified") {
        feldera_artifact_evidence_kind.push("feldera_artifact_hash_verified");
    }
    if !missing_evidence.contains(&"feldera_artifact_release_provenance") {
        feldera_artifact_evidence_kind.push("feldera_artifact_release_provenance");
    }
    let feldera_artifact_kind = if feldera_artifact_evidence_kind.is_empty() {
        String::new()
    } else {
        format!(r#", "evidence_kind": {:?}"#, feldera_artifact_evidence_kind)
    };
    let benchmark_gate_kind = if !missing_evidence.contains(&"s3_compatible_benchmark_gate") {
        r#", "evidence_kind": ["s3_compatible_benchmark_gate"]"#
    } else {
        ""
    };
    let dependency_governance_kind =
        if !missing_evidence.contains(&"dependency_governance_validated") {
            r#", "evidence_kind": ["dependency_governance_validated"]"#
        } else {
            ""
        };

    format!(
        r#"{{
            "schema_version": 1,
            "deployment_id": "prod-a",
            "authority_store_id": "s3://velorix-prod",
            "capability_status": {{ "status": "pass", "evidence": "s3-compatible capability probe"{capability_kind} }},
            "ownership_status": {{ "status": "pass", "evidence": "durable epoch record"{ownership_kind} }},
            "checkpoint_status": {{ "status": "pass", "evidence": "published checkpoint lifecycle"{checkpoint_kind} }},
            "state_status": {{ "status": "pass", "evidence": "SlateDB checkpoint ref"{state_kind} }},
            "query_policy_status": {{ "status": "pass", "evidence": "bounded DataFusion policy"{query_policy_kind} }},
            "table_catalog_status": {{ "status": "pass", "evidence": "registry-backed table catalog"{table_catalog_kind} }},
            "feldera_artifact_status": {{ "status": "pass", "evidence": "trusted artifact metadata"{feldera_artifact_kind} }},
            "dependency_governance_status": {{ "status": "{dependency_governance_status}", "evidence": "{dependency_governance_evidence}"{dependency_governance_kind} }},
            "benchmark_gate_status": {{ "status": "{benchmark_status}", "evidence": "{benchmark_evidence}"{benchmark_gate_kind} }},
            "kubernetes_status": {{ "status": "pass", "evidence": "Kubernetes Lease client"{kubernetes_kind} }}
        }}"#,
        dependency_governance_status = status_for("dependency_governance_status", failed_fields),
        dependency_governance_evidence =
            evidence_for("dependency_governance_status", failed_fields),
        benchmark_status = status_for("benchmark_gate_status", failed_fields),
        benchmark_evidence = evidence_for("benchmark_gate_status", failed_fields),
    )
}

fn fixture_json(name: &str) -> String {
    std::fs::read_to_string(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("velorix-core")
            .join("tests")
            .join("fixtures")
            .join("feldera")
            .join(format!("{name}.json")),
    )
    .unwrap()
}

fn status_for(field: &str, failed_fields: &[&str]) -> &'static str {
    if failed_fields.contains(&field) {
        "fail"
    } else {
        "pass"
    }
}

fn evidence_for(field: &str, failed_fields: &[&str]) -> &'static str {
    match (field, failed_fields.contains(&field)) {
        ("dependency_governance_status", true) => "dependency governance failed closed",
        ("benchmark_gate_status", true) => "regression gate failed closed",
        ("dependency_governance_status", false) => "dependency governance validated",
        ("benchmark_gate_status", false) => "S3-compatible benchmark gate",
        _ => "readiness evidence",
    }
}
