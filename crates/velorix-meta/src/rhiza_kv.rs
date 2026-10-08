//! Embedded Rhiza KV transport. This is deliberately not an ObjectStore or
//! full MetaStore: it exposes only linearizable single-key operations.
use base64::{engine::general_purpose::STANDARD, Engine as _};
use serde_json::{json, Value};
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RhizaKvError {
    #[error("Rhiza KV operation failed ({code}): {message}")]
    Operation { code: String, message: String },
    #[error("Rhiza KV mutation {request_id} has indeterminate commit state: {detail}")]
    Indeterminate { request_id: String, detail: String },
    #[error("Rhiza KV database still has {0} outstanding references")]
    OutstandingReferences(usize),
}

#[derive(Clone)]
pub struct RhizaKvStore {
    db: Arc<rhizadb::Db>,
    /// The address the native recovery listener actually bound, when
    /// [`RhizaKvStore::open_operator_managed`] started one. `None` means this
    /// store serves no operator endpoints.
    recovery_address: Option<String>,
}

impl RhizaKvStore {
    pub async fn open_config(config: rhizadb::Config) -> Result<Self, RhizaKvError> {
        tokio::task::spawn_blocking(move || {
            rhizadb::Db::open(config)
                .map(Self::shared)
                .map_err(|e| RhizaKvError::Operation {
                    code: e.code,
                    message: e.message,
                })
        })
        .await
        .map_err(|e| RhizaKvError::Operation {
            code: "join_error".into(),
            message: e.to_string(),
        })?
    }
    pub async fn open(
        data_dir: impl Into<String> + Send + 'static,
        node_id: impl Into<String> + Send + 'static,
    ) -> Result<Self, RhizaKvError> {
        Self::open_config(rhizadb::Config::new(data_dir.into()).node_id(node_id.into())).await
    }

    /// Open the database from the canonical `RHIZA_*` process environment and
    /// start the private recovery listener before the handle is shared.
    ///
    /// The Rhiza Operator replaces `RHIZA_CLUSTER_ID`, `RHIZA_CLUSTER_MEMBERS`,
    /// `RHIZA_PEER_TOKENS`, `RHIZA_ADMIN_TOKEN`, and
    /// `RHIZA_OBJSTORE_DURABILITY` between generations, so native reads that
    /// environment itself on every process start. This method never constructs a
    /// `rhizadb::Config`, never names a Velorix-owned `RHIZA_*` shadow, and
    /// never mutates the environment at runtime.
    ///
    /// `start_operator` takes `&mut Db`, which is why open and operator startup
    /// share one blocking task: the listener must be running before the handle
    /// is wrapped in `Arc`, and it is closed by the same `Db` that owns the
    /// metadata operations.
    pub async fn open_operator_managed(
        recovery_bind: impl Into<String> + Send + 'static,
    ) -> Result<Self, RhizaKvError> {
        let recovery_bind = recovery_bind.into();
        tokio::task::spawn_blocking(move || {
            let mut db = rhizadb::Db::open_from_env().map_err(operation)?;
            let address = db.start_operator(&recovery_bind).map_err(operation)?;
            eprintln!("rhiza operator recovery listener bound at {address}");
            Ok(Self {
                db: Arc::new(db),
                recovery_address: Some(address),
            })
        })
        .await
        .map_err(|e| RhizaKvError::Operation {
            code: "join_error".into(),
            message: format!("Rhiza operator open task failed: {e}"),
        })?
    }

    pub fn from_db(db: rhizadb::Db) -> Self {
        Self::shared(db)
    }

    fn shared(db: rhizadb::Db) -> Self {
        Self {
            db: Arc::new(db),
            recovery_address: None,
        }
    }

    /// The bound address of the native recovery listener, if this store started
    /// one. Native returns the real address, so a `:0` request reports the port
    /// actually in use.
    pub fn recovery_address(&self) -> Option<&str> {
        self.recovery_address.as_deref()
    }

    /// Native local readiness. This is not quorum: the Operator distinguishes
    /// them, and a learner cloning this binary must be able to answer
    /// `/recovery/status` and local readiness before it has a quorum.
    pub async fn ready(&self) -> Result<bool, RhizaKvError> {
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || db.ready().map_err(operation))
            .await
            .map_err(|e| RhizaKvError::Operation {
                code: "join_error".into(),
                message: format!("Rhiza ready task failed: {e}"),
            })?
    }

    pub async fn get(
        &self,
        key: impl Into<String> + Send + 'static,
    ) -> Result<Option<Vec<u8>>, RhizaKvError> {
        let db = Arc::clone(&self.db);
        let key = key.into();
        tokio::task::spawn_blocking(move || {
            let value: Value = db
                .call("kv_get", json!({"key": key, "consistency": "linearizable"}))
                .map_err(|e| RhizaKvError::Operation {
                    code: e.code,
                    message: e.message,
                })?;
            let found = value.get("found").and_then(Value::as_bool).ok_or_else(|| {
                RhizaKvError::Operation {
                    code: "invalid_response".into(),
                    message: "KV response missing found".into(),
                }
            })?;
            if !found {
                return Ok(None);
            }
            // Go's KV response uses `omitempty`, so a found empty value has
            // no `value` field. A present non-string is malformed.
            let Some(encoded) = value.get("value") else {
                return Ok(Some(Vec::new()));
            };
            let encoded = encoded.as_str().ok_or_else(|| RhizaKvError::Operation {
                code: "invalid_response".into(),
                message: "KV response value is not a string".into(),
            })?;
            STANDARD
                .decode(encoded)
                .map(Some)
                .map_err(|e| RhizaKvError::Operation {
                    code: "invalid_response".into(),
                    message: e.to_string(),
                })
        })
        .await
        .map_err(|e| RhizaKvError::Operation {
            code: "join_error".into(),
            message: e.to_string(),
        })?
    }

    pub async fn put_if_absent(
        &self,
        request_id: impl Into<String> + Send + 'static,
        key: impl Into<String> + Send + 'static,
        value: Vec<u8>,
    ) -> Result<bool, RhizaKvError> {
        self.mutate(
            "kv_cas",
            request_id,
            json!({"key": key.into(), "value": STANDARD.encode(value), "expected": "", "expected_exists": false}),
        )
        .await
    }
    pub async fn compare_and_set(
        &self,
        request_id: impl Into<String> + Send + 'static,
        key: impl Into<String> + Send + 'static,
        expected: Option<Vec<u8>>,
        value: Vec<u8>,
    ) -> Result<bool, RhizaKvError> {
        let expected = expected.map(|v| STANDARD.encode(v));
        if expected.is_none() {
            return self.put_if_absent(request_id, key, value).await;
        }
        let expected_exists = expected.is_some();
        self.mutate("kv_cas", request_id, json!({"key": key.into(), "value": STANDARD.encode(value), "expected": expected, "expected_exists": expected_exists})).await
    }
    async fn mutate(
        &self,
        operation: &'static str,
        request_id: impl Into<String> + Send + 'static,
        mut request: Value,
    ) -> Result<bool, RhizaKvError> {
        let db = Arc::clone(&self.db);
        let request_id = request_id.into();
        let join_request_id = request_id.clone();
        request["request_id"] = Value::String(request_id.clone());
        tokio::task::spawn_blocking(move || {
            let receipt = match db.call::<rhizadb::MutationReceipt>(operation, request.clone()) {
                Ok(r) => r,
                Err(e)
                    if matches!(
                        e.code.as_str(),
                        "commit_unknown" | "timeout" | "deadline_exceeded" | "invalid_response"
                    ) =>
                {
                    let status = db.request_status("kv", &request_id).map_err(|x| {
                        RhizaKvError::Indeterminate {
                            request_id: request_id.clone(),
                            detail: x.to_string(),
                        }
                    })?;
                    match (status.state.as_str(), status.receipt) {
                        ("committed", Some(r)) if r.status.as_deref() == Some("committed") => r,
                        ("rejected", Some(r)) if r.status.as_deref() == Some("rejected") => {
                            return Err(RhizaKvError::Operation {
                                code: r.error_code.unwrap_or_else(|| "rejected".into()),
                                message: "mutation was rejected".into(),
                            })
                        }
                        _ => {
                            return Err(RhizaKvError::Indeterminate {
                                request_id: request_id.clone(),
                                detail: format!("request status is {}", status.state),
                            })
                        }
                    }
                }
                Err(e) => {
                    return Err(RhizaKvError::Operation {
                        code: e.code,
                        message: e.message,
                    })
                }
            };
            validate_receipt(&request_id, &receipt)?;
            Ok(receipt.applied == Some(true))
        })
        .await
        .map_err(|e| RhizaKvError::Indeterminate {
            request_id: join_request_id,
            detail: e.to_string(),
        })?
    }
    pub async fn close(self) -> Result<(), RhizaKvError> {
        let db = Arc::try_unwrap(self.db)
            .map_err(|a| RhizaKvError::OutstandingReferences(Arc::strong_count(&a)))?;
        tokio::task::spawn_blocking(move || {
            let mut db = db;
            db.close().map_err(|e| RhizaKvError::Operation {
                code: e.code,
                message: e.message,
            })
        })
        .await
        .map_err(|e| RhizaKvError::Operation {
            code: "join_error".into(),
            message: e.to_string(),
        })?
    }
}

fn operation(e: rhizadb::Error) -> RhizaKvError {
    RhizaKvError::Operation {
        code: e.code,
        message: e.message,
    }
}

fn validate_receipt(
    request_id: &str,
    receipt: &rhizadb::MutationReceipt,
) -> Result<(), RhizaKvError> {
    match receipt.status.as_deref() {
        Some("committed") => Ok(()),
        Some("rejected") => Err(RhizaKvError::Operation {
            code: receipt
                .error_code
                .clone()
                .unwrap_or_else(|| "rejected".into()),
            message: "mutation was rejected".into(),
        }),
        _ => Err(RhizaKvError::Indeterminate {
            request_id: request_id.into(),
            detail: "mutation response has no valid commit status".into(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn base64_upgrade_preserves_binary_kv_wire_fixture() {
        let bytes = [0, 1, 2, 127, 128, 254, 255];
        assert_eq!(STANDARD.encode(bytes), "AAECf4D+/w==");
        assert_eq!(STANDARD.decode("AAECf4D+/w==").unwrap(), bytes);
        assert_eq!(STANDARD.encode([]), "");
        assert_eq!(STANDARD.encode([0]), "AA==");
        assert_eq!(STANDARD.encode([0, 255]), "AP8=");
        assert_eq!(STANDARD.decode("AA==").unwrap(), [0]);
        assert_eq!(STANDARD.decode("AP8=").unwrap(), [0, 255]);
    }

    fn receipt(status: &str, code: Option<&str>) -> rhizadb::MutationReceipt {
        serde_json::from_value(json!({"status": status, "error_code": code})).unwrap()
    }

    #[test]
    fn malformed_and_unknown_receipts_are_indeterminate() {
        assert!(matches!(
            validate_receipt("request", &receipt("", None)),
            Err(RhizaKvError::Indeterminate { request_id, .. }) if request_id == "request"
        ));
        assert!(matches!(
            validate_receipt("request", &receipt("unknown", None)),
            Err(RhizaKvError::Indeterminate { request_id, .. }) if request_id == "request"
        ));
    }

    #[test]
    fn rejected_receipt_preserves_error_code() {
        assert!(matches!(
            validate_receipt("request", &receipt("rejected", Some("precondition_failed"))),
            Err(RhizaKvError::Operation { code, .. }) if code == "precondition_failed"
        ));
    }
}
