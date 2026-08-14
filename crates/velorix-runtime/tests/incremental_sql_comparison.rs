use std::collections::BTreeMap;

use velorix_runtime::incremental_sql_comparison::{
    ComparisonEngineV1, ComparisonPlanEvidenceV1, ComparisonProtocolV1, CorrectnessOutcomeV2,
    CorrectnessStatusV2, IncrementalSqlComparisonError, IncrementalSqlComparisonResultV2,
    NativeIdentityEvidenceV1, PerformanceCellSemanticsV1, PerformanceMeasurementV1,
    SemanticDifferenceScopeV1,
};

const DIGEST: &str = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const PLAN_FINGERPRINT: &str = "velorix-logical-view-plan-sha256-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const PHYSICAL_DAG: &str = "velorix-physical-operator-dag-sha256-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const ARCHIVED_VELORIX_BASELINE: &str =
    include_str!("../../../baselines/incremental-sql/velorix-v0.1.0.json");
const ARCHIVED_GREPTIMEDB_BASELINE: &str =
    include_str!("../../../baselines/incremental-sql/greptimedb-flow-v1.1.4.json");

#[test]
fn comparison_contract_separates_correctness_status_from_performance() {
    let result = fixture();
    result
        .validate_for_workloads(&["filter_project", "join", "window", "ranking"])
        .unwrap();
    let json = serde_json::to_string(&result).unwrap();
    assert_eq!(
        IncrementalSqlComparisonResultV2::from_json_str(&json).unwrap(),
        result
    );
}

#[test]
fn archived_velorix_baseline_is_valid_and_covers_every_corpus_workload() {
    let result = IncrementalSqlComparisonResultV2::from_json_str(ARCHIVED_VELORIX_BASELINE)
        .expect("archived Velorix baseline must satisfy the comparison contract");
    result
        .validate_for_workloads(&[
            "filter_project",
            "aggregate",
            "distinct_aggregate",
            "inner_join",
            "left_join",
            "top_k",
            "fixed_window",
            "ranking",
            "chained_view",
        ])
        .unwrap();
    assert_eq!(result.engine.name, "velorix");
    assert!(result.performance.is_empty());
    assert!(matches!(
        &result.correctness[0].outcome,
        CorrectnessStatusV2::Passed {
            verified_phases,
            ..
        } if verified_phases == &[
            "initial_load",
            "insert",
            "update",
            "delete",
            "checkpoint_restart",
            "replay_tail",
        ]
    ));
    assert_eq!(
        result
            .correctness
            .iter()
            .filter(|outcome| matches!(outcome.outcome, CorrectnessStatusV2::Unsupported { .. }))
            .count(),
        8
    );
}

#[test]
fn archived_greptimedb_baseline_is_valid_and_preserves_failures() {
    let result = IncrementalSqlComparisonResultV2::from_json_str(ARCHIVED_GREPTIMEDB_BASELINE)
        .expect("archived GreptimeDB baseline must satisfy the comparison contract");
    result
        .validate_for_workloads(&[
            "filter_project",
            "aggregate",
            "distinct_aggregate",
            "inner_join",
            "left_join",
            "top_k",
            "fixed_window",
            "ranking",
            "chained_view",
        ])
        .unwrap();
    assert_eq!(result.engine.name, "greptimedb");
    assert!(result.performance.is_empty());
    assert_eq!(
        result
            .correctness
            .iter()
            .filter(|outcome| matches!(outcome.outcome, CorrectnessStatusV2::Passed { .. }))
            .count(),
        5
    );
    assert_eq!(
        result
            .correctness
            .iter()
            .filter(|outcome| matches!(outcome.outcome, CorrectnessStatusV2::Failed { .. }))
            .count(),
        3
    );
    assert!(result.correctness.iter().any(|outcome| {
        outcome.workload_id == "ranking"
            && matches!(outcome.outcome, CorrectnessStatusV2::Unsupported { .. })
    }));
    for outcome in &result.correctness {
        let CorrectnessStatusV2::Passed { plan_evidence, .. } = &outcome.outcome else {
            continue;
        };
        assert!(matches!(
            plan_evidence.native_logical_plan,
            NativeIdentityEvidenceV1::Unavailable { .. }
        ));
        assert!(matches!(
            plan_evidence.native_physical_dag,
            NativeIdentityEvidenceV1::Unavailable { .. }
        ));
    }
}

#[test]
fn comparison_contract_rejects_performance_for_non_passing_workload() {
    let mut result = fixture();
    result.performance[0].workload_id = "join".to_string();
    assert_eq!(
        result.validate().unwrap_err(),
        IncrementalSqlComparisonError::PerformanceWithoutPassedCorrectness {
            workload_id: "join".to_string()
        }
    );
}

#[test]
fn comparison_contract_rejects_mismatched_passed_digest() {
    let mut result = fixture();
    result.correctness[0].outcome = CorrectnessStatusV2::Passed {
        expected_digest: DIGEST.to_string(),
        observed_digest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            .to_string(),
        verified_phases: vec!["initial_load".to_string()],
        plan_evidence: plan_evidence(),
    };
    assert_eq!(
        result.validate().unwrap_err(),
        IncrementalSqlComparisonError::InvalidPassedDigest {
            workload_id: "filter_project".to_string()
        }
    );
}

#[test]
fn semantic_difference_requires_inspectable_non_parity_evidence() {
    let mut result = fixture();
    let CorrectnessStatusV2::SemanticDifference {
        verified_phases,
        blocked_phases,
        ..
    } = &mut result.correctness[2].outcome
    else {
        panic!("fixture workload must preserve semantic-difference evidence");
    };
    blocked_phases.push(verified_phases[0].clone());
    assert_eq!(
        result.validate().unwrap_err(),
        IncrementalSqlComparisonError::InvalidSemanticDifferenceEvidence {
            workload_id: "window".to_string(),
            field: "blocked_phases",
        }
    );

    let mut result = fixture();
    let CorrectnessStatusV2::SemanticDifference {
        recovery_parity_claimed,
        ..
    } = &mut result.correctness[2].outcome
    else {
        panic!("fixture workload must preserve semantic-difference evidence");
    };
    *recovery_parity_claimed = true;
    assert_eq!(
        result.validate().unwrap_err(),
        IncrementalSqlComparisonError::InvalidSemanticDifferenceEvidence {
            workload_id: "window".to_string(),
            field: "recovery_parity_claimed",
        }
    );
}

#[test]
fn comparison_contract_requires_one_outcome_per_expected_workload() {
    let result = fixture();
    assert_eq!(
        result
            .validate_for_workloads(&["filter_project", "join", "window", "ranking", "top_k"])
            .unwrap_err(),
        IncrementalSqlComparisonError::MissingCorrectnessWorkload {
            workload_id: "top_k".to_string()
        }
    );
}

#[test]
fn comparison_contract_rejects_change_mix_that_hides_events() {
    let mut result = fixture();
    result.protocol.change_events += 1;
    assert_eq!(
        result.validate().unwrap_err(),
        IncrementalSqlComparisonError::ChangeMixMismatch {
            expected: 8,
            actual: 7
        }
    );
}

#[test]
fn comparison_contract_requires_typed_plan_and_physical_dag_identity() {
    let mut result = fixture();
    let CorrectnessStatusV2::Passed { plan_evidence, .. } = &mut result.correctness[0].outcome
    else {
        panic!("fixture workload must pass");
    };
    plan_evidence.native_physical_dag = NativeIdentityEvidenceV1::Available {
        identity: "sha256:bad".to_string(),
    };
    assert_eq!(
        result.validate().unwrap_err(),
        IncrementalSqlComparisonError::InvalidPlanEvidence {
            workload_id: "filter_project".to_string(),
            field: "native_physical_dag",
        }
    );
}

#[test]
fn correctness_can_pass_with_explicitly_unavailable_native_plan_identity() {
    let mut result = fixture();
    let CorrectnessStatusV2::Passed { plan_evidence, .. } = &mut result.correctness[0].outcome
    else {
        panic!("fixture workload must pass");
    };
    *plan_evidence = unavailable_plan_evidence();
    result.performance.clear();
    result.validate().unwrap();
}

#[test]
fn performance_requires_native_plan_and_dag_identity() {
    let mut result = fixture();
    let CorrectnessStatusV2::Passed { plan_evidence, .. } = &mut result.correctness[0].outcome
    else {
        panic!("fixture workload must pass");
    };
    *plan_evidence = unavailable_plan_evidence();
    assert_eq!(
        result.validate().unwrap_err(),
        IncrementalSqlComparisonError::PerformanceWithoutNativePlanEvidence {
            workload_id: "filter_project".to_string(),
        }
    );
}

#[test]
fn plan_evidence_accepts_versioned_engine_native_namespaces() {
    let mut result = fixture();
    let CorrectnessStatusV2::Passed { plan_evidence, .. } = &mut result.correctness[0].outcome
    else {
        panic!("fixture workload must carry plan evidence");
    };
    result.engine.name = "risingwave".to_string();
    plan_evidence.native_logical_plan = NativeIdentityEvidenceV1::Available {
        identity: format!("risingwave-native-logical-plan-sha256-v1:{}", DIGEST),
    };
    plan_evidence.native_physical_dag = NativeIdentityEvidenceV1::Available {
        identity: format!("risingwave-stream-fragment-graph-sha256-v1:{}", DIGEST),
    };
    result.validate().unwrap();
}

#[test]
fn comparison_contract_excludes_semantically_mismatched_performance_cell() {
    for field in [
        "sql_identity",
        "durability_mode",
        "output_acknowledgement",
        "watermark_lateness",
        "state_retention",
        "restart_success",
    ] {
        let mut result = fixture();
        let observed = &mut result.performance[0].observed_semantics;
        match field {
            "sql_identity" => observed.sql_identity.push_str("-different"),
            "durability_mode" => observed.durability_mode.push_str("-different"),
            "output_acknowledgement" => observed.output_acknowledgement.push_str("-different"),
            "watermark_lateness" => observed.watermark_lateness.push_str("-different"),
            "state_retention" => observed.state_retention.push_str("-different"),
            "restart_success" => observed.restart_success.push_str("-different"),
            _ => unreachable!(),
        }
        assert_eq!(
            result.validate().unwrap_err(),
            IncrementalSqlComparisonError::IncomparablePerformanceCell {
                workload_id: "filter_project".to_string(),
                mismatched_fields: vec![field],
            }
        );
    }
}

#[test]
fn comparison_contract_rejects_inconsistent_amplification_and_composite_scores() {
    let mut result = fixture();
    result.performance[0].output_amplification = 2.0;
    assert_eq!(
        result.validate().unwrap_err(),
        IncrementalSqlComparisonError::InvalidOutputAmplification {
            workload_id: "filter_project".to_string(),
        }
    );

    let mut json = serde_json::to_value(fixture()).unwrap();
    json["performance"][0]["composite_score"] = serde_json::json!(99.0);
    assert!(matches!(
        IncrementalSqlComparisonResultV2::from_json_str(&json.to_string()).unwrap_err(),
        IncrementalSqlComparisonError::Json(_)
    ));
}

fn fixture() -> IncrementalSqlComparisonResultV2 {
    IncrementalSqlComparisonResultV2 {
        schema_version: 2,
        corpus_version: "incremental-sql-corpus-v1".to_string(),
        engine: ComparisonEngineV1 {
            name: "velorix".to_string(),
            version: "0.1.0".to_string(),
            source_revision: "0123456789abcdef".to_string(),
            configuration: BTreeMap::from([
                ("object_store".to_string(), "local".to_string()),
                ("workers".to_string(), "1".to_string()),
            ]),
            durability_mode: "checkpointed_object_storage".to_string(),
            input_semantics: "signed_rows".to_string(),
            state_retention_policy: "all_history".to_string(),
        },
        protocol: ComparisonProtocolV1 {
            warm_up_iterations: 1,
            measured_iterations: 5,
            initial_rows: 7,
            change_events: 7,
            change_mix: BTreeMap::from([
                ("insert".to_string(), 5),
                ("update".to_string(), 1),
                ("delete".to_string(), 1),
            ]),
        },
        correctness: vec![
            CorrectnessOutcomeV2 {
                workload_id: "filter_project".to_string(),
                outcome: CorrectnessStatusV2::Passed {
                    expected_digest: DIGEST.to_string(),
                    observed_digest: DIGEST.to_string(),
                    verified_phases: vec![
                        "initial_load".to_string(),
                        "insert".to_string(),
                        "update".to_string(),
                        "delete".to_string(),
                        "checkpoint_restart".to_string(),
                        "replay_tail".to_string(),
                    ],
                    plan_evidence: plan_evidence(),
                },
            },
            CorrectnessOutcomeV2 {
                workload_id: "join".to_string(),
                outcome: CorrectnessStatusV2::Unsupported {
                    reason: "join shape is outside admission".to_string(),
                },
            },
            CorrectnessOutcomeV2 {
                workload_id: "window".to_string(),
                outcome: CorrectnessStatusV2::SemanticDifference {
                    reason_code: "finite_retention_differs_from_required_semantics".to_string(),
                    reason: "engine uses a finite retention policy".to_string(),
                    scope: SemanticDifferenceScopeV1::WorkloadSpecific,
                    expected_digest: DIGEST.to_string(),
                    verified_phases: vec!["initial_load".to_string()],
                    blocked_phases: vec!["checkpoint_restart".to_string()],
                    recovery_parity_claimed: false,
                    performance_comparable: false,
                },
            },
            CorrectnessOutcomeV2 {
                workload_id: "ranking".to_string(),
                outcome: CorrectnessStatusV2::Failed {
                    reason: "observed rows differ from the oracle".to_string(),
                },
            },
        ],
        performance: vec![PerformanceMeasurementV1 {
            workload_id: "filter_project".to_string(),
            feature_family: "filter_project".to_string(),
            required_semantics: performance_semantics(),
            observed_semantics: performance_semantics(),
            repetitions: 5,
            input_rows: 7,
            change_events: 7,
            output_change_records: 7,
            input_rows_per_second: 1000.0,
            output_rows_per_second: 1000.0,
            output_amplification: 1.0,
            p50_ms: 1.0,
            p95_ms: 2.0,
            state_bytes: 4096,
            checkpoint_bytes: 2048,
            checkpoint_ms: 3.0,
            restore_ms: 4.0,
        }],
    }
}

fn performance_semantics() -> PerformanceCellSemanticsV1 {
    PerformanceCellSemanticsV1 {
        sql_identity: "incremental-sql-corpus-v1/filter_project".to_string(),
        durability_mode: "checkpointed_object_storage".to_string(),
        output_acknowledgement: "materialized".to_string(),
        watermark_lateness: "not_applicable".to_string(),
        state_retention: "all_history".to_string(),
        restart_success: "required".to_string(),
    }
}

fn plan_evidence() -> ComparisonPlanEvidenceV1 {
    ComparisonPlanEvidenceV1 {
        native_logical_plan: NativeIdentityEvidenceV1::Available {
            identity: PLAN_FINGERPRINT.to_string(),
        },
        native_physical_dag: NativeIdentityEvidenceV1::Available {
            identity: PHYSICAL_DAG.to_string(),
        },
        diagnostic_explain_digest: None,
    }
}

fn unavailable_plan_evidence() -> ComparisonPlanEvidenceV1 {
    ComparisonPlanEvidenceV1 {
        native_logical_plan: NativeIdentityEvidenceV1::Unavailable {
            reason_code: "engine_does_not_expose_native_logical_identity".to_string(),
        },
        native_physical_dag: NativeIdentityEvidenceV1::Unavailable {
            reason_code: "engine_does_not_expose_native_physical_dag_identity".to_string(),
        },
        diagnostic_explain_digest: Some(format!("velorix-diagnostic-explain-sha256-v1:{}", DIGEST)),
    }
}
