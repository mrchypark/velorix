use super::*;

/// Deployment limits are deliberately not persisted in checkpoints.
/// Total checkpoint limits are enforced before commit by set operations, typed maps,
/// ordered windows, single-key aggregates and typed join projections. Other runtime
/// families currently enforce this limit only at the central restore entry point;
/// they retain their existing apply/checkpoint behavior. Expansion and snapshot
/// limits are currently wired only through set operations/typed maps and ordered windows.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeResourceLimits {
    pub max_checkpoint_bytes: usize,
    pub max_expansion_bytes: usize,
    pub max_expansion_rows: usize,
    pub max_snapshot_bytes: usize,
}

impl Default for RuntimeResourceLimits {
    fn default() -> Self {
        Self {
            max_checkpoint_bytes: checkpoint_common::MAX_CHECKPOINT_PAYLOAD_BYTES,
            max_expansion_bytes: 16 * 1024 * 1024,
            max_expansion_rows: 16 * 1024 * 1024 / std::mem::size_of::<DeltaRecord>(),
            max_snapshot_bytes: 16 * 1024 * 1024,
        }
    }
}

impl RuntimeResourceLimits {
    pub fn validate(&self) -> Result<(), StandingProgramRuntimeError> {
        if [
            self.max_checkpoint_bytes,
            self.max_expansion_bytes,
            self.max_expansion_rows,
            self.max_snapshot_bytes,
        ]
        .contains(&0)
        {
            return Err(limit_error("runtime_resource_limits_must_be_positive"));
        }
        Ok(())
    }
    pub(super) fn validate_checkpoint(
        &self,
        payload: &str,
    ) -> Result<(), StandingProgramRuntimeError> {
        if payload.len() > self.max_checkpoint_bytes {
            return Err(limit_error("runtime_checkpoint_bytes"));
        }
        Ok(())
    }

    pub(super) fn checkpoint_json<T: Serialize>(
        &self,
        value: &T,
    ) -> Result<String, StandingProgramRuntimeError> {
        struct Writer {
            bytes: Vec<u8>,
            limit: usize,
            exceeded: bool,
        }
        impl std::io::Write for Writer {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                let Some(required) = self
                    .bytes
                    .len()
                    .checked_add(bytes.len())
                    .filter(|size| *size <= self.limit)
                else {
                    self.exceeded = true;
                    return Err(std::io::Error::other("checkpoint byte limit exceeded"));
                };
                if required > self.bytes.capacity() {
                    let capacity = required
                        .max(self.bytes.capacity().saturating_mul(2))
                        .min(self.limit);
                    self.bytes
                        .try_reserve_exact(capacity - self.bytes.len())
                        .map_err(std::io::Error::other)?;
                }
                self.bytes.extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let mut writer = Writer {
            bytes: Vec::new(),
            limit: self.max_checkpoint_bytes,
            exceeded: false,
        };
        serde_json::to_writer(&mut writer, value).map_err(|error| {
            if writer.exceeded {
                limit_error("runtime_checkpoint_bytes")
            } else {
                StandingProgramRuntimeError::InvalidProgramIdentityDynamic {
                    field: std::borrow::Cow::Owned(format!(
                        "runtime_checkpoint_serialization: {error}"
                    )),
                }
            }
        })?;
        String::from_utf8(writer.bytes).map_err(|_| invalid_checkpoint())
    }

    pub(super) fn charge_expansion(
        &self,
        bytes: &mut usize,
        rows: &mut usize,
        width: usize,
        count: usize,
    ) -> Result<(), StandingProgramRuntimeError> {
        let next_rows = rows
            .checked_add(count)
            .filter(|rows| *rows <= self.max_expansion_rows)
            .ok_or_else(|| limit_error("runtime_expansion_rows"))?;
        let next_bytes = width
            .checked_mul(count)
            .and_then(|size| bytes.checked_add(size))
            .filter(|bytes| *bytes <= self.max_expansion_bytes)
            .ok_or_else(|| limit_error("runtime_expansion_bytes"))?;
        *rows = next_rows;
        *bytes = next_bytes;
        Ok(())
    }

    pub(super) fn validate_page(
        &self,
        output: &DeltaBatch,
        epoch: u64,
        page: &SnapshotPageRequest,
    ) -> Result<(), StandingProgramRuntimeError> {
        let offset = match &page.page_token {
            Some(token) => {
                let (token_epoch, offset): (u64, usize) =
                    serde_json::from_str(token).map_err(|_| invalid_runtime_state())?;
                if token_epoch != epoch {
                    return Err(invalid_runtime_state());
                }
                offset
            }
            None => 0,
        };
        let end = offset.saturating_add(page.max_rows.unwrap_or(usize::MAX));
        let mut position = 0usize;
        let mut bytes = 0usize;
        for row in output.net_rows().map_err(|_| invalid_runtime_state())? {
            let count = usize::try_from(row.weight).map_err(|_| invalid_runtime_state())?;
            let next = position
                .checked_add(count)
                .ok_or_else(invalid_runtime_state)?;
            let take = next.min(end).saturating_sub(position.max(offset));
            bytes = row_width(&row)?
                .checked_mul(take)
                .and_then(|size| bytes.checked_add(size))
                .filter(|bytes| *bytes <= self.max_snapshot_bytes)
                .ok_or_else(|| limit_error("runtime_snapshot_bytes"))?;
            position = next;
            if position >= end {
                break;
            }
        }
        Ok(())
    }
}

pub(super) fn row_width(row: &DeltaRecord) -> Result<usize, StandingProgramRuntimeError> {
    velorix_core::delta::encode_kv_ordered(row.key.as_json(), row.value.as_json())
        .as_bytes()
        .len()
        .checked_add(std::mem::size_of::<DeltaRecord>())
        .ok_or_else(invalid_runtime_state)
}

fn limit_error(field: &'static str) -> StandingProgramRuntimeError {
    StandingProgramRuntimeError::InvalidProgramIdentity { field }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn budgets_are_independent_positive_and_serialization_errors_are_distinct() {
        let defaults = RuntimeResourceLimits::default();
        for limits in [
            RuntimeResourceLimits {
                max_checkpoint_bytes: 0,
                ..defaults
            },
            RuntimeResourceLimits {
                max_expansion_bytes: 0,
                ..defaults
            },
            RuntimeResourceLimits {
                max_expansion_rows: 0,
                ..defaults
            },
            RuntimeResourceLimits {
                max_snapshot_bytes: 0,
                ..defaults
            },
        ] {
            assert!(limits.validate().is_err());
        }
        let limits = RuntimeResourceLimits {
            max_checkpoint_bytes: 2,
            ..defaults
        };
        assert!(limits
            .checkpoint_json(&"long")
            .unwrap_err()
            .to_string()
            .contains("runtime_checkpoint_bytes"));
        let mut bytes = 0;
        let mut rows = 0;
        limits
            .charge_expansion(&mut bytes, &mut rows, 100, 2)
            .unwrap();
        assert!(limits
            .charge_expansion(&mut bytes, &mut rows, usize::MAX, 2)
            .is_err());
        assert_eq!((bytes, rows), (200, 2));
        struct Invalid;
        impl Serialize for Invalid {
            fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
                Err(serde::ser::Error::custom(
                    "deliberate serialization failure",
                ))
            }
        }
        assert!(defaults
            .checkpoint_json(&Invalid)
            .unwrap_err()
            .to_string()
            .contains("runtime_checkpoint_serialization: deliberate serialization failure"));
    }
}
