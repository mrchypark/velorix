//! Minimal embedded Rhiza SQL transport.
//!
//! This SQL transport intentionally does not implement `MetaStore`. Rhiza
//! 0.12 SQL has no engine-assigned authority timestamp; the separate KV
//! snapshot path carries one proposer-sampled time inside each root-CAS
//! transition and relies on epoch/token predicates for stale-writer safety.

use std::sync::Arc;

use serde_json::{json, Value};
use thiserror::Error;

/// Embedded Rhiza database shared by async callers.
#[derive(Clone)]
pub struct RhizaSqlStore {
    db: Arc<rhizadb::Db>,
}

#[derive(Debug, Error)]
pub enum RhizaSqlError {
    #[error("Rhiza operation failed ({code}): {message}")]
    Operation { code: String, message: String },
    #[error("Rhiza task failed: {0}")]
    Task(String),
    #[error("Rhiza SQL arguments must be an array")]
    InvalidArguments,
    #[error("Rhiza mutation {request_id} has indeterminate commit state: {detail}")]
    Indeterminate { request_id: String, detail: String },
    #[error("Rhiza database still has {0} outstanding references")]
    OutstandingReferences(usize),
}

#[derive(Clone, Debug)]
pub struct SqlStatement {
    pub sql: String,
    pub args: Vec<Value>,
    pub want_rows: bool,
    pub expected_rows_affected: Option<i64>,
}

impl serde::Serialize for SqlStatement {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("SqlStatement", 4)?;
        state.serialize_field("sql", &self.sql)?;
        if !self.args.is_empty() {
            state.serialize_field("args", &self.args)?;
        }
        if self.want_rows {
            state.serialize_field("want_rows", &self.want_rows)?;
        }
        if let Some(rows) = self.expected_rows_affected {
            state.serialize_field("expected_rows_affected", &rows)?;
        }
        state.end()
    }
}

/// Result of a committed SQL mutation.
#[derive(Clone, Debug)]
pub struct MutationOutcome {
    pub rows_affected: Option<i64>,
    pub last_insert_id: Option<i64>,
}

fn reconcile_status(
    request_id: &str,
    result: Result<rhizadb::RequestStatus, rhizadb::Error>,
) -> Result<rhizadb::MutationReceipt, RhizaSqlError> {
    let status = result.map_err(|e| RhizaSqlError::Indeterminate {
        request_id: request_id.into(),
        detail: e.to_string(),
    })?;
    match status.receipt {
        Some(r) if status.state == "committed" && r.status.as_deref() == Some("committed") => Ok(r),
        Some(r) if status.state == "rejected" && r.status.as_deref() == Some("rejected") => {
            Err(RhizaSqlError::Operation {
                code: r.error_code.unwrap_or_else(|| "rejected".into()),
                message: "mutation was rejected".into(),
            })
        }
        _ => Err(RhizaSqlError::Indeterminate {
            request_id: request_id.into(),
            detail: format!("request status is {}", status.state),
        }),
    }
}

impl RhizaSqlStore {
    /// Open the persistent database in `data_dir`.
    pub async fn open(
        data_dir: impl Into<String> + Send + 'static,
        node_id: impl Into<String> + Send + 'static,
    ) -> Result<Self, RhizaSqlError> {
        let data_dir = data_dir.into();
        let node_id = node_id.into();
        tokio::task::spawn_blocking(move || {
            let db = rhizadb::Db::open(rhizadb::Config::new(data_dir).node_id(node_id)).map_err(
                |error| RhizaSqlError::Operation {
                    code: error.code,
                    message: error.message,
                },
            )?;
            Ok(Self { db: Arc::new(db) })
        })
        .await
        .map_err(|error| RhizaSqlError::Task(format!("Rhiza open task failed: {error}")))?
    }

    pub async fn open_config(config: rhizadb::Config) -> Result<Self, RhizaSqlError> {
        tokio::task::spawn_blocking(move || {
            let db = rhizadb::Db::open(config).map_err(|error| RhizaSqlError::Operation {
                code: error.code,
                message: error.message,
            })?;
            Ok(Self { db: Arc::new(db) })
        })
        .await
        .map_err(|error| RhizaSqlError::Task(format!("Rhiza open task failed: {error}")))?
    }

    /// Construct a transport around an already-open database (primarily for
    /// tests and callers that own the Config/open lifecycle).
    pub fn from_db(db: rhizadb::Db) -> Self {
        Self { db: Arc::new(db) }
    }

    /// Rhiza's SQL convenience query uses local consistency. This transport
    /// deliberately asks the native operation for a linearizable read.
    pub async fn query_linearizable(
        &self,
        sql: impl Into<String> + Send + 'static,
        args: Value,
    ) -> Result<rhizadb::QueryResult, RhizaSqlError> {
        let db = Arc::clone(&self.db);
        let sql = sql.into();
        tokio::task::spawn_blocking(move || {
            db.call(
                "query",
                json!({"sql": sql, "args": args, "consistency": "linearizable"}),
            )
            .map_err(|error: rhizadb::Error| RhizaSqlError::Operation {
                code: error.code,
                message: error.message,
            })
        })
        .await
        .map_err(|error| RhizaSqlError::Task(format!("Rhiza query task failed: {error}")))?
    }

    /// Execute one SQL batch atomically. The caller-supplied request ID is
    /// reused for status reconciliation; this method never invents a retry ID.
    /// Dropping this future does not cancel a native mutation already running.
    /// Retain the request ID and reconcile its outcome before submitting new work.
    pub async fn execute_atomic(
        &self,
        request_id: impl Into<String> + Send + 'static,
        sql: impl Into<String> + Send + 'static,
        args: Value,
    ) -> Result<MutationOutcome, RhizaSqlError> {
        let args = args
            .as_array()
            .cloned()
            .ok_or(RhizaSqlError::InvalidArguments)?;
        self.execute_atomic_statements(
            request_id,
            vec![SqlStatement {
                sql: sql.into(),
                args,
                want_rows: false,
                expected_rows_affected: None,
            }],
        )
        .await
    }

    pub async fn execute_atomic_statements(
        &self,
        request_id: impl Into<String> + Send + 'static,
        statements: Vec<SqlStatement>,
    ) -> Result<MutationOutcome, RhizaSqlError> {
        let db = Arc::clone(&self.db);
        let request_id = request_id.into();
        let join_request_id = request_id.clone();
        tokio::task::spawn_blocking(move || {
            let receipt = match db.call::<rhizadb::ExecuteReturningResult>(
                "execute",
                json!({"request_id": request_id, "statements": statements}),
            ) {
                Ok(response) => response.receipt,
                Err(error)
                    if matches!(
                        error.code.as_str(),
                        "commit_unknown" | "timeout" | "deadline_exceeded" | "invalid_response"
                    ) =>
                {
                    reconcile_status(&request_id, db.request_status("sql", &request_id))?
                }
                Err(error) => {
                    return Err(RhizaSqlError::Operation {
                        code: error.code,
                        message: error.message,
                    })
                }
            };
            validate_receipt(&request_id, &receipt)?;
            Ok(MutationOutcome {
                rows_affected: receipt.rows_affected,
                last_insert_id: receipt.last_insert_id,
            })
        })
        .await
        .map_err(|e| RhizaSqlError::Indeterminate {
            request_id: join_request_id,
            detail: format!("blocking task join failed; reconcile original request: {e}"),
        })?
    }

    /// Close on a blocking thread with exclusive ownership. Await completion
    /// before reopening the directory: cancelling this future does not cancel
    /// an in-progress native close. Unclosed handles use SDK Drop as best effort.
    pub async fn close(self) -> Result<(), RhizaSqlError> {
        let db = Arc::try_unwrap(self.db)
            .map_err(|arc| RhizaSqlError::OutstandingReferences(Arc::strong_count(&arc)))?;
        tokio::task::spawn_blocking(move || {
            let mut db = db;
            db.close().map_err(|e| RhizaSqlError::Operation {
                code: e.code,
                message: e.message,
            })
        })
        .await
        .map_err(|e| RhizaSqlError::Task(format!("Rhiza close task failed: {e}")))?
    }

    /// Rhiza 0.12 has no replicated authority timestamp API.
    pub const fn server_serialized_timestamp_available() -> bool {
        false
    }

    /// Authority operations must fail closed until server-serialized time is
    /// available; process wall-clock substitution would break fencing.
    pub const fn authority_operations_available() -> bool {
        false
    }
}

fn validate_receipt(
    request_id: &str,
    receipt: &rhizadb::MutationReceipt,
) -> Result<(), RhizaSqlError> {
    receipt.require_committed().map_err(|error| {
        if receipt.status.as_deref() == Some("rejected") {
            RhizaSqlError::Operation {
                code: error.code,
                message: error.message,
            }
        } else {
            RhizaSqlError::Indeterminate {
                request_id: request_id.into(),
                detail: error.to_string(),
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn receipt(status: &str, code: Option<&str>) -> rhizadb::MutationReceipt {
        serde_json::from_value(json!({"status": status, "error_code": code})).unwrap()
    }

    #[test]
    fn reconcile_status_requires_matching_state_and_receipt() {
        let ok = rhizadb::RequestStatus {
            state: "committed".into(),
            tip: 1,
            receipt: Some(receipt("committed", None)),
        };
        assert!(reconcile_status("r", Ok(ok)).is_ok());
        let rejected = rhizadb::RequestStatus {
            state: "rejected".into(),
            tip: 1,
            receipt: Some(receipt("rejected", Some("request_conflict"))),
        };
        match reconcile_status("r", Ok(rejected)).unwrap_err() {
            RhizaSqlError::Operation { code, .. } => assert_eq!(code, "request_conflict"),
            other => panic!("{other:?}"),
        }
        let unknown = rhizadb::RequestStatus {
            state: "unknown_or_expired".into(),
            tip: 1,
            receipt: None,
        };
        assert!(
            matches!(reconcile_status("r", Ok(unknown)), Err(RhizaSqlError::Indeterminate { request_id, .. }) if request_id == "r")
        );
        let mismatch = rhizadb::RequestStatus {
            state: "committed".into(),
            tip: 1,
            receipt: Some(receipt("rejected", None)),
        };
        assert!(matches!(
            reconcile_status("r", Ok(mismatch)),
            Err(RhizaSqlError::Indeterminate { .. })
        ));
        let err = rhizadb::Error {
            code: "timeout".into(),
            message: "late".into(),
        };
        assert!(
            matches!(reconcile_status("r", Err(err)), Err(RhizaSqlError::Indeterminate { request_id, .. }) if request_id == "r")
        );
        let missing = rhizadb::RequestStatus {
            state: "committed".into(),
            tip: 1,
            receipt: None,
        };
        assert!(matches!(
            reconcile_status("r", Ok(missing)),
            Err(RhizaSqlError::Indeterminate { .. })
        ));
    }

    #[test]
    fn malformed_direct_receipt_is_indeterminate() {
        assert!(
            matches!(validate_receipt("r", &receipt("unexpected", None)),
            Err(RhizaSqlError::Indeterminate { request_id, .. }) if request_id == "r")
        );
        assert!(
            matches!(validate_receipt("r", &receipt("rejected", Some("precondition_failed"))),
            Err(RhizaSqlError::Operation { code, .. }) if code == "precondition_failed")
        );
    }
}
