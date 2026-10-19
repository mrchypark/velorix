//! Checked sparse ordinal progress within one immutable artifact.
//! Artifact identity and output-scope completeness are separate caller contracts.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ProcessedSpansError {
    #[error("processed span must be nonempty and within the artifact row count")]
    InvalidBounds,
    #[error("persisted spans must be sorted, disjoint, and already coalesced")]
    NonCanonical,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "ProcessedSpansWire")]
pub struct ProcessedSpans {
    rows_total: u64,
    spans: Vec<[u64; 2]>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessedSpansWire {
    rows_total: u64,
    spans: Vec<[u64; 2]>,
}

impl TryFrom<ProcessedSpansWire> for ProcessedSpans {
    type Error = ProcessedSpansError;

    fn try_from(wire: ProcessedSpansWire) -> Result<Self, Self::Error> {
        Self::from_spans(wire.rows_total, wire.spans)
    }
}

impl ProcessedSpans {
    pub fn new(rows_total: u64) -> Self {
        Self {
            rows_total,
            spans: Vec::new(),
        }
    }

    pub fn from_prefix(rows_total: u64, count: u64) -> Result<Self, ProcessedSpansError> {
        if count > rows_total {
            return Err(ProcessedSpansError::InvalidBounds);
        }
        Self::from_spans(
            rows_total,
            if count == 0 {
                Vec::new()
            } else {
                vec![[0, count]]
            },
        )
    }

    /// Persisted input is rejected rather than sorted, clamped, or repaired.
    pub fn from_spans(rows_total: u64, spans: Vec<[u64; 2]>) -> Result<Self, ProcessedSpansError> {
        let mut previous_end = None;
        for &[start, end] in &spans {
            if start >= end || end > rows_total {
                return Err(ProcessedSpansError::InvalidBounds);
            }
            if previous_end.is_some_and(|previous| start <= previous) {
                return Err(ProcessedSpansError::NonCanonical);
            }
            previous_end = Some(end);
        }
        Ok(Self { rows_total, spans })
    }

    /// Union a nonempty interval with processed ordinals. Invalid input leaves
    /// progress unchanged. Adjacent spans coalesce; repeated insertion is inert.
    pub fn insert(&mut self, start: u64, end: u64) -> Result<(), ProcessedSpansError> {
        if start >= end || end > self.rows_total {
            return Err(ProcessedSpansError::InvalidBounds);
        }
        let mut merged_start = start;
        let mut merged_end = end;
        let mut kept = Vec::new();
        for [start, end] in std::mem::take(&mut self.spans) {
            if end < merged_start {
                kept.push([start, end]);
            } else if start > merged_end {
                kept.push([merged_start, merged_end]);
                merged_start = start;
                merged_end = end;
            } else {
                merged_start = merged_start.min(start);
                merged_end = merged_end.max(end);
            }
        }
        kept.push([merged_start, merged_end]);
        self.spans = kept;
        Ok(())
    }

    pub fn rows_total(&self) -> u64 {
        self.rows_total
    }

    pub fn contains(&self, ordinal: u64) -> bool {
        self.spans
            .iter()
            .any(|&[start, end]| start <= ordinal && ordinal < end)
    }

    pub fn processed_len(&self) -> u64 {
        // Checked constructors/deserialization and insertion ensure disjoint
        // bounds, so the sum is at most rows_total, including u64::MAX.
        self.spans.iter().fold(0u64, |sum, &[start, end]| {
            sum.checked_add(end.checked_sub(start).expect("validated span bounds"))
                .expect("disjoint spans cannot exceed rows_total")
        })
    }

    pub fn is_complete(&self) -> bool {
        self.contiguous_frontier() == self.rows_total
    }

    /// Exclusive end of the processed prefix starting at zero. Sparse work
    /// beyond the first hole never advances this frontier.
    pub fn contiguous_frontier(&self) -> u64 {
        self.spans
            .first()
            .filter(|span| span[0] == 0)
            .map_or(0, |span| span[1])
    }

    pub fn unprocessed_runs(&self) -> Vec<[u64; 2]> {
        let mut runs = Vec::new();
        let mut cursor = 0;
        for &[start, end] in &self.spans {
            if cursor < start {
                runs.push([cursor, start]);
            }
            cursor = end;
        }
        if cursor < self.rows_total {
            runs.push([cursor, self.rows_total]);
        }
        runs
    }

    pub fn as_spans(&self) -> Vec<[u64; 2]> {
        self.spans.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overlap_adjacency_and_repeated_work_coalesce() {
        let mut spans = ProcessedSpans::new(20);
        for (start, end) in [(8, 10), (2, 5), (4, 8), (2, 10), (15, 18)] {
            spans.insert(start, end).unwrap();
        }
        assert_eq!(spans.as_spans(), vec![[2, 10], [15, 18]]);
        assert_eq!(spans.processed_len(), 11);
        assert_eq!(spans.unprocessed_runs(), vec![[0, 2], [10, 15], [18, 20]]);
        assert!(spans.contains(9));
        assert!(!spans.contains(10));
    }

    #[test]
    fn sparse_work_does_not_advance_prefix() {
        let mut spans = ProcessedSpans::new(10);
        spans.insert(5, 10).unwrap();
        assert_eq!(spans.contiguous_frontier(), 0);
        spans.insert(0, 3).unwrap();
        assert_eq!(spans.contiguous_frontier(), 3);
        assert!(!spans.is_complete());
        spans.insert(3, 5).unwrap();
        assert_eq!(spans.contiguous_frontier(), 10);
        assert!(spans.is_complete());
    }

    #[test]
    fn invalid_insertion_is_atomic_and_never_clamps() {
        let mut spans = ProcessedSpans::from_prefix(10, 3).unwrap();
        let before = spans.clone();
        for (start, end) in [(3, 3), (7, 6), (9, 11), (11, 12)] {
            assert!(spans.insert(start, end).is_err());
            assert_eq!(spans, before);
        }
        assert!(ProcessedSpans::from_prefix(10, 11).is_err());
    }

    #[test]
    fn empty_and_maximum_domains_do_not_overflow() {
        let empty = ProcessedSpans::new(0);
        assert!(empty.is_complete());
        assert!(empty.unprocessed_runs().is_empty());
        let mut spans = ProcessedSpans::from_prefix(u64::MAX, 1).unwrap();
        spans.insert(2, u64::MAX).unwrap();
        assert_eq!(spans.processed_len(), u64::MAX - 1);
        spans.insert(1, 2).unwrap();
        assert_eq!(spans.processed_len(), u64::MAX);
        assert!(spans.is_complete());
        assert!(!spans.contains(u64::MAX));
    }

    #[test]
    fn serde_rejects_noncanonical_or_invalid_persisted_spans() {
        for spans in [
            vec![[1, 1]],
            vec![[4, 2]],
            vec![[0, 11]],
            vec![[0, 2], [2, 4]],
            vec![[0, 3], [2, 4]],
            vec![[5, 7], [0, 2]],
        ] {
            let wire = serde_json::json!({"rows_total": 10, "spans": spans});
            assert!(serde_json::from_value::<ProcessedSpans>(wire).is_err());
        }
        assert!(serde_json::from_str::<ProcessedSpans>(
            r#"{"rows_total":0,"spans":[],"extra":true}"#
        )
        .is_err());
        let valid = ProcessedSpans::from_spans(10, vec![[0, 2], [5, 10]]).unwrap();
        let restored: ProcessedSpans =
            serde_json::from_slice(&serde_json::to_vec(&valid).unwrap()).unwrap();
        assert_eq!(valid, restored);
    }
}
