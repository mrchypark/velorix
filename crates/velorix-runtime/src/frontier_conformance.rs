//! Per-frontier incremental-output conformance against a batch oracle.

use std::collections::BTreeMap;

pub type CanonicalBagV1 = BTreeMap<String, i64>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WeightedCanonicalRowV1 {
    pub row: String,
    pub weight: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommittedFrontierEvidenceV1 {
    pub frontier: u64,
    pub oracle_snapshot: Vec<WeightedCanonicalRowV1>,
    pub observed_delta: Vec<WeightedCanonicalRowV1>,
    pub observed_snapshot: Vec<WeightedCanonicalRowV1>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FrontierConformanceVerifierV1 {
    last_frontier: Option<u64>,
    oracle_bag: CanonicalBagV1,
    observed_bag: CanonicalBagV1,
}

#[derive(Clone, Debug, thiserror::Error, Eq, PartialEq)]
pub enum FrontierConformanceError {
    #[error("frontier {actual} must be greater than committed frontier {committed}")]
    NonMonotonicFrontier { committed: u64, actual: u64 },
    #[error("{field} row identity must not be empty")]
    EmptyRowIdentity { field: &'static str },
    #[error("{field} multiplicity overflowed for row {row}")]
    MultiplicityOverflow { field: &'static str, row: String },
    #[error("{field} contains negative committed multiplicity {weight} for row {row}")]
    NegativeCommittedMultiplicity {
        field: &'static str,
        row: String,
        weight: i64,
    },
    #[error("frontier {frontier} consolidated delta differs from the batch-oracle delta")]
    DeltaMismatch {
        frontier: u64,
        expected: CanonicalBagV1,
        observed: CanonicalBagV1,
    },
    #[error("frontier {frontier} observed delta does not produce the observed snapshot")]
    DeltaSnapshotMismatch {
        frontier: u64,
        delta_applied: CanonicalBagV1,
        observed_snapshot: CanonicalBagV1,
    },
    #[error("frontier {frontier} materialized snapshot differs from the batch oracle")]
    SnapshotMismatch {
        frontier: u64,
        expected: CanonicalBagV1,
        observed: CanonicalBagV1,
    },
}

impl FrontierConformanceVerifierV1 {
    pub fn verify_committed_frontier(
        &mut self,
        evidence: CommittedFrontierEvidenceV1,
    ) -> Result<(), FrontierConformanceError> {
        if let Some(committed) = self.last_frontier {
            if evidence.frontier <= committed {
                return Err(FrontierConformanceError::NonMonotonicFrontier {
                    committed,
                    actual: evidence.frontier,
                });
            }
        }

        let oracle_snapshot = committed_bag("oracle_snapshot", evidence.oracle_snapshot)?;
        let observed_delta = consolidated_bag("observed_delta", evidence.observed_delta)?;
        let observed_snapshot = committed_bag("observed_snapshot", evidence.observed_snapshot)?;
        let expected_delta = bag_difference(&oracle_snapshot, &self.oracle_bag)?;
        if observed_delta != expected_delta {
            return Err(FrontierConformanceError::DeltaMismatch {
                frontier: evidence.frontier,
                expected: expected_delta,
                observed: observed_delta,
            });
        }

        let delta_applied = apply_delta(&self.observed_bag, &observed_delta)?;
        if delta_applied != observed_snapshot {
            return Err(FrontierConformanceError::DeltaSnapshotMismatch {
                frontier: evidence.frontier,
                delta_applied,
                observed_snapshot,
            });
        }
        if observed_snapshot != oracle_snapshot {
            return Err(FrontierConformanceError::SnapshotMismatch {
                frontier: evidence.frontier,
                expected: oracle_snapshot,
                observed: observed_snapshot,
            });
        }

        self.last_frontier = Some(evidence.frontier);
        self.oracle_bag = oracle_snapshot;
        self.observed_bag = observed_snapshot;
        Ok(())
    }

    pub fn last_verified_frontier(&self) -> Option<u64> {
        self.last_frontier
    }
}

fn committed_bag(
    field: &'static str,
    rows: Vec<WeightedCanonicalRowV1>,
) -> Result<CanonicalBagV1, FrontierConformanceError> {
    let bag = consolidated_bag(field, rows)?;
    if let Some((row, weight)) = bag.iter().find(|(_, weight)| **weight < 0) {
        return Err(FrontierConformanceError::NegativeCommittedMultiplicity {
            field,
            row: row.clone(),
            weight: *weight,
        });
    }
    Ok(bag)
}

fn consolidated_bag(
    field: &'static str,
    rows: Vec<WeightedCanonicalRowV1>,
) -> Result<CanonicalBagV1, FrontierConformanceError> {
    let mut bag = CanonicalBagV1::new();
    for row in rows {
        if row.row.is_empty() {
            return Err(FrontierConformanceError::EmptyRowIdentity { field });
        }
        let next = bag
            .get(&row.row)
            .copied()
            .unwrap_or_default()
            .checked_add(row.weight)
            .ok_or_else(|| FrontierConformanceError::MultiplicityOverflow {
                field,
                row: row.row.clone(),
            })?;
        if next == 0 {
            bag.remove(&row.row);
        } else {
            bag.insert(row.row, next);
        }
    }
    Ok(bag)
}

fn bag_difference(
    current: &CanonicalBagV1,
    previous: &CanonicalBagV1,
) -> Result<CanonicalBagV1, FrontierConformanceError> {
    let mut delta = current.clone();
    for (row, previous_weight) in previous {
        let next = delta
            .get(row)
            .copied()
            .unwrap_or_default()
            .checked_sub(*previous_weight)
            .ok_or_else(|| FrontierConformanceError::MultiplicityOverflow {
                field: "oracle_delta",
                row: row.clone(),
            })?;
        if next == 0 {
            delta.remove(row);
        } else {
            delta.insert(row.clone(), next);
        }
    }
    Ok(delta)
}

fn apply_delta(
    previous: &CanonicalBagV1,
    delta: &CanonicalBagV1,
) -> Result<CanonicalBagV1, FrontierConformanceError> {
    let mut next = previous.clone();
    for (row, weight) in delta {
        let next_weight = next
            .get(row)
            .copied()
            .unwrap_or_default()
            .checked_add(*weight)
            .ok_or_else(|| FrontierConformanceError::MultiplicityOverflow {
                field: "observed_delta",
                row: row.clone(),
            })?;
        if next_weight < 0 {
            return Err(FrontierConformanceError::NegativeCommittedMultiplicity {
                field: "delta_applied_snapshot",
                row: row.clone(),
                weight: next_weight,
            });
        }
        if next_weight == 0 {
            next.remove(row);
        } else {
            next.insert(row.clone(), next_weight);
        }
    }
    Ok(next)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(row: &str, weight: i64) -> WeightedCanonicalRowV1 {
        WeightedCanonicalRowV1 {
            row: row.to_string(),
            weight,
        }
    }

    #[test]
    fn verifies_insert_update_delete_at_each_frontier() {
        let mut verifier = FrontierConformanceVerifierV1::default();
        verifier
            .verify_committed_frontier(CommittedFrontierEvidenceV1 {
                frontier: 1,
                oracle_snapshot: vec![row("alice:10", 1), row("bob:4", 1)],
                observed_delta: vec![row("bob:4", 1), row("alice:10", 1)],
                observed_snapshot: vec![row("alice:10", 1), row("bob:4", 1)],
            })
            .unwrap();
        verifier
            .verify_committed_frontier(CommittedFrontierEvidenceV1 {
                frontier: 2,
                oracle_snapshot: vec![row("alice:12", 1)],
                observed_delta: vec![row("alice:10", -1), row("alice:12", 1), row("bob:4", -1)],
                observed_snapshot: vec![row("alice:12", 1)],
            })
            .unwrap();
        assert_eq!(verifier.last_verified_frontier(), Some(2));
    }

    #[test]
    fn missing_retraction_fails_the_delta_check_without_advancing() {
        let mut verifier = FrontierConformanceVerifierV1::default();
        verifier
            .verify_committed_frontier(CommittedFrontierEvidenceV1 {
                frontier: 1,
                oracle_snapshot: vec![row("alice:10", 1)],
                observed_delta: vec![row("alice:10", 1)],
                observed_snapshot: vec![row("alice:10", 1)],
            })
            .unwrap();
        let error = verifier
            .verify_committed_frontier(CommittedFrontierEvidenceV1 {
                frontier: 2,
                oracle_snapshot: vec![row("alice:12", 1)],
                observed_delta: vec![row("alice:12", 1)],
                observed_snapshot: vec![row("alice:12", 1)],
            })
            .unwrap_err();
        assert!(matches!(
            error,
            FrontierConformanceError::DeltaMismatch { frontier: 2, .. }
        ));
        assert_eq!(verifier.last_verified_frontier(), Some(1));
    }

    #[test]
    fn wrong_snapshot_fails_separately_from_a_correct_delta() {
        let mut verifier = FrontierConformanceVerifierV1::default();
        let error = verifier
            .verify_committed_frontier(CommittedFrontierEvidenceV1 {
                frontier: 1,
                oracle_snapshot: vec![row("alice:10", 1)],
                observed_delta: vec![row("alice:10", 1)],
                observed_snapshot: vec![row("bob:10", 1)],
            })
            .unwrap_err();
        assert!(matches!(
            error,
            FrontierConformanceError::DeltaSnapshotMismatch { frontier: 1, .. }
        ));
        assert_eq!(verifier.last_verified_frontier(), None);
    }
}
