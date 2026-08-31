//! Upload receipt for avoiding redundant HEAD requests after PUT.
//!
//! When an object is uploaded, the PUT response contains an ETag and
//! content hash. This receipt captures that information so subsequent
//! operations can verify the object without a redundant HEAD request.
//!
//! # Design
//!
//! ```text
//! UploadReceipt {
//!     object_key: String,
//!     etag: Option<String>,
//!     content_hash: String,
//!     byte_size: u64,
//!     owner_generation: u64,
//! }
//! ```
//!
//! # Benefits
//!
//! - Eliminates redundant HEAD requests after PUT
//! - Provides content hash for downstream operations
//! - Enables deterministic verification without network round-trip

use serde::{Deserialize, Serialize};

/// Receipt from a successful object upload.
///
/// Captures metadata from the PUT response to avoid redundant HEAD
/// requests for verification.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadReceipt {
    /// The object key that was uploaded.
    pub object_key: String,
    /// ETag from the PUT response (if available).
    pub etag: Option<String>,
    /// Content hash (SHA-256) of the uploaded bytes.
    pub content_hash: String,
    /// Size of the uploaded object in bytes.
    pub byte_size: u64,
    /// Owner generation for fencing (if applicable).
    pub owner_generation: u64,
    /// Timestamp when the upload completed.
    pub uploaded_at: u64,
}

impl UploadReceipt {
    /// Create a new upload receipt.
    pub fn new(
        object_key: String,
        etag: Option<String>,
        content_hash: String,
        byte_size: u64,
        owner_generation: u64,
        uploaded_at: u64,
    ) -> Self {
        Self {
            object_key,
            etag,
            content_hash,
            byte_size,
            owner_generation,
            uploaded_at,
        }
    }

    /// Verify that a receipt matches expected values.
    pub fn verify(&self, expected_key: &str, expected_hash: &str) -> Result<(), String> {
        if self.object_key != expected_key {
            return Err(format!(
                "key mismatch: expected {}, got {}",
                expected_key, self.object_key
            ));
        }
        if self.content_hash != expected_hash {
            return Err(format!(
                "hash mismatch: expected {}, got {}",
                expected_hash, self.content_hash
            ));
        }
        Ok(())
    }
}

/// A collection of upload receipts for a batch of uploads.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadReceiptBatch {
    receipts: Vec<UploadReceipt>,
}

impl UploadReceiptBatch {
    /// Create a new empty batch.
    pub fn new() -> Self {
        Self {
            receipts: Vec::new(),
        }
    }

    /// Add a receipt to the batch.
    pub fn push(&mut self, receipt: UploadReceipt) {
        self.receipts.push(receipt);
    }

    /// Get the number of receipts.
    pub fn len(&self) -> usize {
        self.receipts.len()
    }

    /// Check if the batch is empty.
    pub fn is_empty(&self) -> bool {
        self.receipts.is_empty()
    }

    /// Get a receipt by object key.
    pub fn get(&self, object_key: &str) -> Option<&UploadReceipt> {
        self.receipts.iter().find(|r| r.object_key == object_key)
    }

    /// Get all receipts.
    pub fn receipts(&self) -> &[UploadReceipt] {
        &self.receipts
    }

    /// Compute total bytes uploaded.
    pub fn total_bytes(&self) -> u64 {
        self.receipts.iter().map(|r| r.byte_size).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upload_receipt_basic() {
        let receipt = UploadReceipt::new(
            "obj/key".to_string(),
            Some("etag123".to_string()),
            "sha256:abc".to_string(),
            1024,
            1,
            1000,
        );
        assert_eq!(receipt.object_key, "obj/key");
        assert_eq!(receipt.content_hash, "sha256:abc");
    }

    #[test]
    fn upload_receipt_verify_success() {
        let receipt = UploadReceipt::new(
            "obj/key".to_string(),
            None,
            "sha256:abc".to_string(),
            1024,
            1,
            1000,
        );
        assert!(receipt.verify("obj/key", "sha256:abc").is_ok());
    }

    #[test]
    fn upload_receipt_verify_failure() {
        let receipt = UploadReceipt::new(
            "obj/key".to_string(),
            None,
            "sha256:abc".to_string(),
            1024,
            1,
            1000,
        );
        assert!(receipt.verify("obj/other", "sha256:abc").is_err());
    }

    #[test]
    fn upload_receipt_batch() {
        let mut batch = UploadReceiptBatch::new();
        assert!(batch.is_empty());

        batch.push(UploadReceipt::new(
            "a".to_string(),
            None,
            "h1".to_string(),
            100,
            1,
            1000,
        ));
        batch.push(UploadReceipt::new(
            "b".to_string(),
            None,
            "h2".to_string(),
            200,
            1,
            1001,
        ));

        assert_eq!(batch.len(), 2);
        assert_eq!(batch.total_bytes(), 300);
        assert!(batch.get("a").is_some());
        assert!(batch.get("c").is_none());
    }
}
