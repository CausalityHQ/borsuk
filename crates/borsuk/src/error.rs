use std::{io, path::PathBuf};

/// Result type used by the BORSUK core crate.
pub type Result<T> = std::result::Result<T, BorsukError>;

/// Errors returned by BORSUK operations.
#[derive(Debug, thiserror::Error)]
pub enum BorsukError {
    /// A vector or query dimension did not match the index dimension.
    #[error("dimension mismatch: expected {expected}, got {actual}")]
    DimensionMismatch {
        /// Expected number of dimensions.
        expected: usize,
        /// Actual number of dimensions.
        actual: usize,
    },

    /// A metric received invalid input.
    #[error("invalid metric input: {0}")]
    InvalidMetricInput(String),

    /// Compaction options were invalid.
    #[error("invalid compaction input: {0}")]
    InvalidCompactionInput(String),

    /// Search options were invalid.
    #[error("invalid search options: {0}")]
    InvalidSearchOptions(String),

    /// Resident routing memory exceeded the configured budget.
    #[error(
        "RAM budget exceeded: resident estimate {resident_bytes} bytes exceeds budget {budget_bytes} bytes"
    )]
    RamBudgetExceeded {
        /// Estimated resident bytes.
        resident_bytes: u64,
        /// Configured resident byte budget.
        budget_bytes: u64,
    },

    /// Durable storage bytes could not be decoded.
    #[error("invalid storage: {0}")]
    InvalidStorage(String),

    /// A requested index does not exist or has no CURRENT pointer.
    #[error("index not found at `{0}`")]
    IndexNotFound(String),

    /// A stored segment failed checksum validation.
    #[error("checksum mismatch for segment `{path}`: expected {expected}, got {actual}")]
    ChecksumMismatch {
        /// Segment path relative to the index root.
        path: String,
        /// Expected BLAKE3 checksum.
        expected: String,
        /// Actual BLAKE3 checksum.
        actual: String,
    },

    /// Local filesystem I/O failed.
    #[error("I/O error at `{path}`: {source}")]
    Io {
        /// Path involved in the failed operation.
        path: PathBuf,
        /// Source I/O error.
        #[source]
        source: io::Error,
    },

    /// Object storage operation failed.
    #[error("object storage error: {0}")]
    ObjectStore(#[from] object_store::Error),

    /// Arrow record batch handling failed.
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),

    /// Parquet serialization failed.
    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
}
