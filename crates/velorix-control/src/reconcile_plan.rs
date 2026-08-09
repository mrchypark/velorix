use crate::control_plane_contract::VelorixView;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ObservedControlPlaneFacts {
    pub lease: Option<LeaseFact>,
    pub epoch_record: Option<EpochRecordFact>,
    pub worker: Option<WorkerFact>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LeaseFact {
    pub owner_id: String,
    pub owner_epoch: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EpochRecordFact {
    pub owner_id: String,
    pub owner_epoch: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkerFact {
    pub owner_id: String,
    pub owner_epoch: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReconcilePlan {
    pub actions: Vec<ReconcileAction>,
    pub block_reason: Option<ReconcileBlockReason>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReconcileAction {
    AcquireLease { owner_id: String },
    StopWorker { owner_id: String, owner_epoch: u64 },
    StartWorker { owner_id: String, owner_epoch: u64 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReconcileBlockReason {
    MissingDurableEpochRecordSupport,
    EpochRecordConflict,
    LeaseOwnerConflict,
}

pub fn plan_reconcile(desired: &VelorixView, facts: &ObservedControlPlaneFacts) -> ReconcilePlan {
    let desired_owner = desired.spec.worker.owner_id.as_str();
    let mut plan = ReconcilePlan::default();

    let Some(lease) = facts.lease.as_ref() else {
        stop_worker_if_running(&mut plan, facts.worker.as_ref());
        plan.actions.push(ReconcileAction::AcquireLease {
            owner_id: desired_owner.to_string(),
        });
        return plan;
    };

    if lease.owner_id != desired_owner {
        stop_worker_if_running(&mut plan, facts.worker.as_ref());
        plan.block_reason = Some(ReconcileBlockReason::LeaseOwnerConflict);
        return plan;
    }

    let Some(lease_epoch) = lease.owner_epoch else {
        stop_worker_if_running(&mut plan, facts.worker.as_ref());
        plan.block_reason = Some(ReconcileBlockReason::MissingDurableEpochRecordSupport);
        return plan;
    };

    match facts.epoch_record.as_ref() {
        Some(epoch_record)
            if epoch_record.owner_id == lease.owner_id
                && epoch_record.owner_epoch == lease_epoch => {}
        Some(_) => {
            stop_worker_if_running(&mut plan, facts.worker.as_ref());
            plan.block_reason = Some(ReconcileBlockReason::EpochRecordConflict);
            return plan;
        }
        None => {
            stop_worker_if_running(&mut plan, facts.worker.as_ref());
            plan.block_reason = Some(ReconcileBlockReason::MissingDurableEpochRecordSupport);
            return plan;
        }
    }

    match facts.worker.as_ref() {
        Some(worker) if worker.owner_id == lease.owner_id && worker.owner_epoch == lease_epoch => {}
        Some(worker) => {
            plan.actions.push(ReconcileAction::StopWorker {
                owner_id: worker.owner_id.clone(),
                owner_epoch: worker.owner_epoch,
            });
            plan.actions.push(ReconcileAction::StartWorker {
                owner_id: lease.owner_id.clone(),
                owner_epoch: lease_epoch,
            });
        }
        None => plan.actions.push(ReconcileAction::StartWorker {
            owner_id: lease.owner_id.clone(),
            owner_epoch: lease_epoch,
        }),
    }

    plan
}

fn stop_worker_if_running(plan: &mut ReconcilePlan, worker: Option<&WorkerFact>) {
    if let Some(worker) = worker {
        plan.actions.push(ReconcileAction::StopWorker {
            owner_id: worker.owner_id.clone(),
            owner_epoch: worker.owner_epoch,
        });
    }
}
