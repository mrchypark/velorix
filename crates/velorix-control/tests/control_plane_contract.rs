use velorix_control::{
    control_plane_contract::{
        ConditionStatus, ContractMetadata, VelorixCondition, VelorixView, VelorixViewSpec,
        VelorixViewStatus, WorkerIntent,
    },
    reconcile_plan::{
        plan_reconcile, EpochRecordFact, LeaseFact, ObservedControlPlaneFacts, ReconcileAction,
        ReconcileBlockReason, WorkerFact,
    },
};

fn contract() -> VelorixView {
    VelorixView {
        api_version: "control.velorix.io/v1alpha1".to_string(),
        kind: "VelorixView".to_string(),
        metadata: ContractMetadata {
            name: "balances-by-account".to_string(),
            namespace: "analytics".to_string(),
            generation: 7,
        },
        spec_version: 1,
        spec: VelorixViewSpec {
            view_id: "balances_by_account".to_string(),
            worker: WorkerIntent {
                stream_id: "orders".to_string(),
                partition_id: 3,
                owner_id: "worker-a".to_string(),
            },
        },
        status: VelorixViewStatus {
            observed_generation: Some(6),
            observed_checkpoint_version: Some(42),
            observed_owner_epoch: Some(4),
            conditions: vec![VelorixCondition {
                type_: "Ready".to_string(),
                status: ConditionStatus::False,
                reason: "Reconciling".to_string(),
                message: "Waiting for durable authority observations.".to_string(),
            }],
        },
    }
}

#[test]
fn velorix_view_json_round_trips_against_golden_contract_shape() {
    let view = contract();

    let json = serde_json::to_string_pretty(&view).unwrap();

    assert_eq!(
        json,
        r#"{
  "api_version": "control.velorix.io/v1alpha1",
  "kind": "VelorixView",
  "metadata": {
    "name": "balances-by-account",
    "namespace": "analytics",
    "generation": 7
  },
  "spec_version": 1,
  "spec": {
    "view_id": "balances_by_account",
    "worker": {
      "stream_id": "orders",
      "partition_id": 3,
      "owner_id": "worker-a"
    }
  },
  "status": {
    "observed_generation": 6,
    "observed_checkpoint_version": 42,
    "observed_owner_epoch": 4,
    "conditions": [
      {
        "type": "Ready",
        "status": "False",
        "reason": "Reconciling",
        "message": "Waiting for durable authority observations."
      }
    ]
  }
}"#
    );
    assert_eq!(serde_json::from_str::<VelorixView>(&json).unwrap(), view);
}

#[test]
fn velorix_view_rejects_unknown_contract_fields() {
    let json = serde_json::json!({
        "api_version": "control.velorix.io/v1alpha1",
        "kind": "VelorixView",
        "metadata": {
            "name": "balances-by-account",
            "namespace": "analytics",
            "generation": 7
        },
        "spec_version": 1,
        "spec": {
            "view_id": "balances_by_account",
            "worker": {
                "stream_id": "orders",
                "partition_id": 3,
                "owner_id": "worker-a",
                "leaseBackend": "production-writer"
            }
        },
        "status": {
            "observed_generation": 7,
            "observed_checkpoint_version": 43,
            "observed_owner_epoch": 5,
            "conditions": []
        }
    });

    let err = serde_json::from_value::<VelorixView>(json).unwrap_err();

    assert!(err.to_string().contains("leaseBackend"));
}

#[test]
fn reconcile_plan_ignores_status_only_progress_for_worker_start() {
    let mut desired = contract();
    desired.status.observed_generation = Some(7);
    desired.status.observed_checkpoint_version = Some(43);
    desired.status.observed_owner_epoch = Some(5);

    let plan = plan_reconcile(&desired, &ObservedControlPlaneFacts::default());

    assert!(plan.actions.contains(&ReconcileAction::AcquireLease {
        owner_id: "worker-a".to_string(),
    }));
    assert!(!plan
        .actions
        .iter()
        .any(|action| matches!(action, ReconcileAction::StartWorker { .. })));
}

#[test]
fn reconcile_plan_stops_worker_when_lease_is_missing() {
    let desired = contract();
    let facts = ObservedControlPlaneFacts {
        worker: Some(WorkerFact {
            owner_id: "worker-a".to_string(),
            owner_epoch: 5,
        }),
        ..ObservedControlPlaneFacts::default()
    };

    let plan = plan_reconcile(&desired, &facts);

    assert_eq!(
        plan.actions,
        vec![
            ReconcileAction::StopWorker {
                owner_id: "worker-a".to_string(),
                owner_epoch: 5,
            },
            ReconcileAction::AcquireLease {
                owner_id: "worker-a".to_string(),
            }
        ]
    );
}

#[test]
fn reconcile_plan_blocks_worker_when_only_lease_intent_is_observed() {
    let desired = contract();
    let facts = ObservedControlPlaneFacts {
        lease: Some(LeaseFact {
            owner_id: "worker-a".to_string(),
            owner_epoch: None,
        }),
        epoch_record: None,
        worker: None,
    };

    let plan = plan_reconcile(&desired, &facts);

    assert!(plan.actions.is_empty());
    assert_eq!(
        plan.block_reason,
        Some(ReconcileBlockReason::MissingDurableEpochRecordSupport)
    );
}

#[test]
fn reconcile_plan_stops_worker_when_lease_epoch_is_missing() {
    let desired = contract();
    let facts = ObservedControlPlaneFacts {
        lease: Some(LeaseFact {
            owner_id: "worker-a".to_string(),
            owner_epoch: None,
        }),
        epoch_record: None,
        worker: Some(WorkerFact {
            owner_id: "worker-a".to_string(),
            owner_epoch: 5,
        }),
    };

    let plan = plan_reconcile(&desired, &facts);

    assert_eq!(
        plan.actions,
        vec![ReconcileAction::StopWorker {
            owner_id: "worker-a".to_string(),
            owner_epoch: 5,
        }]
    );
    assert_eq!(
        plan.block_reason,
        Some(ReconcileBlockReason::MissingDurableEpochRecordSupport)
    );
}

#[test]
fn reconcile_plan_blocks_worker_when_lease_epoch_has_no_durable_record() {
    let desired = contract();
    let facts = ObservedControlPlaneFacts {
        lease: Some(LeaseFact {
            owner_id: "worker-a".to_string(),
            owner_epoch: Some(5),
        }),
        epoch_record: None,
        worker: None,
    };

    let plan = plan_reconcile(&desired, &facts);

    assert!(plan.actions.is_empty());
    assert_eq!(
        plan.block_reason,
        Some(ReconcileBlockReason::MissingDurableEpochRecordSupport)
    );
}

#[test]
fn reconcile_plan_stops_worker_when_epoch_record_is_missing() {
    let desired = contract();
    let facts = ObservedControlPlaneFacts {
        lease: Some(LeaseFact {
            owner_id: "worker-a".to_string(),
            owner_epoch: Some(5),
        }),
        epoch_record: None,
        worker: Some(WorkerFact {
            owner_id: "worker-a".to_string(),
            owner_epoch: 5,
        }),
    };

    let plan = plan_reconcile(&desired, &facts);

    assert_eq!(
        plan.actions,
        vec![ReconcileAction::StopWorker {
            owner_id: "worker-a".to_string(),
            owner_epoch: 5,
        }]
    );
    assert_eq!(
        plan.block_reason,
        Some(ReconcileBlockReason::MissingDurableEpochRecordSupport)
    );
}

#[test]
fn reconcile_plan_starts_worker_when_matching_epoch_record_is_observed() {
    let desired = contract();
    let facts = ObservedControlPlaneFacts {
        lease: Some(LeaseFact {
            owner_id: "worker-a".to_string(),
            owner_epoch: Some(5),
        }),
        epoch_record: Some(EpochRecordFact {
            owner_id: "worker-a".to_string(),
            owner_epoch: 5,
        }),
        worker: None,
    };

    let plan = plan_reconcile(&desired, &facts);

    assert_eq!(
        plan.actions,
        vec![ReconcileAction::StartWorker {
            owner_id: "worker-a".to_string(),
            owner_epoch: 5,
        }]
    );
    assert_eq!(plan.block_reason, None);
}

#[test]
fn reconcile_plan_stops_stale_worker_when_higher_epoch_is_observed() {
    let desired = contract();
    let facts = ObservedControlPlaneFacts {
        lease: Some(LeaseFact {
            owner_id: "worker-a".to_string(),
            owner_epoch: Some(6),
        }),
        epoch_record: Some(EpochRecordFact {
            owner_id: "worker-a".to_string(),
            owner_epoch: 6,
        }),
        worker: Some(WorkerFact {
            owner_id: "worker-a".to_string(),
            owner_epoch: 5,
        }),
    };

    let plan = plan_reconcile(&desired, &facts);

    assert_eq!(
        plan.actions,
        vec![
            ReconcileAction::StopWorker {
                owner_id: "worker-a".to_string(),
                owner_epoch: 5,
            },
            ReconcileAction::StartWorker {
                owner_id: "worker-a".to_string(),
                owner_epoch: 6,
            }
        ]
    );
    assert_eq!(plan.block_reason, None);
}

#[test]
fn reconcile_plan_fails_closed_when_epoch_record_conflicts_with_lease() {
    let desired = contract();
    let facts = ObservedControlPlaneFacts {
        lease: Some(LeaseFact {
            owner_id: "worker-a".to_string(),
            owner_epoch: Some(6),
        }),
        epoch_record: Some(EpochRecordFact {
            owner_id: "worker-b".to_string(),
            owner_epoch: 6,
        }),
        worker: Some(WorkerFact {
            owner_id: "worker-a".to_string(),
            owner_epoch: 5,
        }),
    };

    let plan = plan_reconcile(&desired, &facts);

    assert_eq!(
        plan.actions,
        vec![ReconcileAction::StopWorker {
            owner_id: "worker-a".to_string(),
            owner_epoch: 5,
        }]
    );
    assert_eq!(
        plan.block_reason,
        Some(ReconcileBlockReason::EpochRecordConflict)
    );
}

#[test]
fn reconcile_plan_fails_closed_when_lease_owner_conflicts_with_desired_worker() {
    let desired = contract();
    let facts = ObservedControlPlaneFacts {
        lease: Some(LeaseFact {
            owner_id: "worker-b".to_string(),
            owner_epoch: Some(6),
        }),
        epoch_record: Some(EpochRecordFact {
            owner_id: "worker-b".to_string(),
            owner_epoch: 6,
        }),
        worker: Some(WorkerFact {
            owner_id: "worker-b".to_string(),
            owner_epoch: 6,
        }),
    };

    let plan = plan_reconcile(&desired, &facts);

    assert_eq!(
        plan.actions,
        vec![ReconcileAction::StopWorker {
            owner_id: "worker-b".to_string(),
            owner_epoch: 6,
        }]
    );
    assert_eq!(
        plan.block_reason,
        Some(ReconcileBlockReason::LeaseOwnerConflict)
    );
}
