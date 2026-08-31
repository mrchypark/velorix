//! Compressed chunk support for output persistence.
//!
//! Instead of fixed 256-row JSON pages, this module supports
//! compressed byte-size chunks that reduce storage costs and
//! network transfer.
//!
//! # Design
//!
//! ```text
//! CompressedChunk {
//!     chunk_hash: String,
//!     original_bytes: u64,
//!     compressed_bytes: u64,
//!     compression_ratio: f64,
//!     row_count: u64,
//!     min_key: Option<String>,
//!     max_key: Option<String>,
//! }
//! ```
//!
//! # Benefits over 256-row JSON pages
//!
//! - Fewer objects (4-16 MiB chunks vs 256-row pages)
//! - Reduced JSON overhead (column names not repeated)
//! - Compression reduces storage and transfer costs
//! - Content-addressed deduplication across checkpoints

use serde::{Deserialize, Serialize};

/// Default target chunk size in bytes.
pub const DEFAULT_CHUNK_TARGET_BYTES: usize = 4 * 1024 * 1024; // 4 MiB

/// Compression algorithm used for chunks.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompressionAlgorithm {
    None,
    Zstd,
    Lz4,
    Snappy,
}

/// A compressed output chunk.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CompressedChunk {
    /// Content hash of the compressed data.
    pub chunk_hash: String,
    /// Original (uncompressed) size in bytes.
    pub original_bytes: u64,
    /// Compressed size in bytes.
    pub compressed_bytes: u64,
    /// Compression ratio (compressed/original).
    pub compression_ratio: f64,
    /// Number of rows in the chunk.
    pub row_count: u64,
    /// Minimum key in the chunk (for range pruning).
    pub min_key: Option<String>,
    /// Maximum key in the chunk (for range pruning).
    pub max_key: Option<String>,
    /// Compression algorithm used.
    pub compression: CompressionAlgorithm,
    /// Object key where the chunk is stored.
    pub object_key: String,
}

impl CompressedChunk {
    /// Create a new compressed chunk record.
    pub fn new(
        chunk_hash: String,
        original_bytes: u64,
        compressed_bytes: u64,
        row_count: u64,
        compression: CompressionAlgorithm,
        object_key: String,
    ) -> Self {
        let compression_ratio = if original_bytes > 0 {
            compressed_bytes as f64 / original_bytes as f64
        } else {
            1.0
        };
        Self {
            chunk_hash,
            original_bytes,
            compressed_bytes,
            compression_ratio,
            row_count,
            min_key: None,
            max_key: None,
            compression,
            object_key,
        }
    }

    /// Set the key range for this chunk.
    pub fn with_key_range(mut self, min_key: String, max_key: String) -> Self {
        self.min_key = Some(min_key);
        self.max_key = Some(max_key);
        self
    }

    /// Check if a key falls within this chunk's range.
    pub fn contains_key(&self, key: &str) -> bool {
        match (&self.min_key, &self.max_key) {
            (Some(min), Some(max)) => key >= min.as_str() && key <= max.as_str(),
            _ => true, // No range info, assume contains
        }
    }
}

/// A manifest of compressed chunks for a checkpoint output.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CompressedChunkManifest {
    /// Checkpoint version this manifest belongs to.
    pub checkpoint_version: u64,
    /// All chunks in this output.
    pub chunks: Vec<CompressedChunk>,
    /// Total original bytes across all chunks.
    pub total_original_bytes: u64,
    /// Total compressed bytes across all chunks.
    pub total_compressed_bytes: u64,
    /// Total row count across all chunks.
    pub total_row_count: u64,
}

impl CompressedChunkManifest {
    /// Create a new empty manifest.
    pub fn new(checkpoint_version: u64) -> Self {
        Self {
            checkpoint_version,
            chunks: Vec::new(),
            total_original_bytes: 0,
            total_compressed_bytes: 0,
            total_row_count: 0,
        }
    }

    /// Add a chunk to the manifest.
    pub fn add_chunk(&mut self, chunk: CompressedChunk) {
        self.total_original_bytes += chunk.original_bytes;
        self.total_compressed_bytes += chunk.compressed_bytes;
        self.total_row_count += chunk.row_count;
        self.chunks.push(chunk);
    }

    /// Get chunks that contain a specific key (for range pruning).
    pub fn chunks_for_key(&self, key: &str) -> Vec<&CompressedChunk> {
        self.chunks.iter().filter(|c| c.contains_key(key)).collect()
    }

    /// Get the overall compression ratio.
    pub fn compression_ratio(&self) -> f64 {
        if self.total_original_bytes > 0 {
            self.total_compressed_bytes as f64 / self.total_original_bytes as f64
        } else {
            1.0
        }
    }

    /// Get the number of chunks.
    pub fn chunk_count(&self) -> usize {
        self.chunks.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compressed_chunk_basic() {
        let chunk = CompressedChunk::new(
            "hash1".to_string(),
            1000,
            500,
            100,
            CompressionAlgorithm::Zstd,
            "obj/key".to_string(),
        );
        assert_eq!(chunk.compression_ratio, 0.5);
        assert_eq!(chunk.row_count, 100);
    }

    #[test]
    fn compressed_chunk_key_range() {
        let chunk = CompressedChunk::new(
            "hash1".to_string(),
            1000,
            500,
            100,
            CompressionAlgorithm::None,
            "obj/key".to_string(),
        )
        .with_key_range("aaa".to_string(), "mzz".to_string());

        assert!(chunk.contains_key("bbb"));
        assert!(chunk.contains_key("aaa"));
        assert!(chunk.contains_key("mzz"));
        assert!(!chunk.contains_key("nzz"));
        assert!(!chunk.contains_key("ZZZ"));
    }

    #[test]
    fn compressed_chunk_manifest() {
        let mut manifest = CompressedChunkManifest::new(1);
        manifest.add_chunk(CompressedChunk::new(
            "h1".to_string(),
            1000,
            500,
            100,
            CompressionAlgorithm::Zstd,
            "k1".to_string(),
        ));
        manifest.add_chunk(CompressedChunk::new(
            "h2".to_string(),
            2000,
            1000,
            200,
            CompressionAlgorithm::Zstd,
            "k2".to_string(),
        ));

        assert_eq!(manifest.chunk_count(), 2);
        assert_eq!(manifest.total_original_bytes, 3000);
        assert_eq!(manifest.total_compressed_bytes, 1500);
        assert_eq!(manifest.total_row_count, 300);
        assert_eq!(manifest.compression_ratio(), 0.5);
    }
}
