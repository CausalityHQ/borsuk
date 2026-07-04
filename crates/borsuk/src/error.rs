use std::{io, path::PathBuf};

use crate::record::SearchTerminationReason;

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

    /// Vector records received invalid input.
    #[error("invalid record input: {0}")]
    InvalidRecordInput(String),

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

    /// Guaranteed-recall search could not honor a hard search budget.
    #[error("recall guarantee violated by search termination `{reason}`")]
    RecallGuaranteeViolated {
        /// Budget or approximation reason that would have degraded recall.
        reason: SearchTerminationReason,
    },

    /// Durable storage bytes could not be decoded.
    #[error("invalid storage: {0}")]
    InvalidStorage(String),

    /// A requested index does not exist or has no CURRENT pointer.
    #[error("index not found at `{0}`")]
    IndexNotFound(String),

    /// A publish lost optimistic concurrency arbitration.
    #[error("concurrent modification while publishing `{path}`")]
    ConcurrentModification {
        /// Object path relative to the index root that detected the conflict.
        path: String,
    },

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

    /// A retryable or transient object storage operation failed after backend retries.
    #[error("retryable object storage error at `{path}`: {source}")]
    ObjectStoreRetryable {
        /// Object path relative to the index root.
        path: String,
        /// Source object-store error.
        #[source]
        source: object_store::Error,
    },

    /// A referenced object was missing from object storage.
    #[error("object storage path `{path}` not found: {source}")]
    ObjectStoreNotFound {
        /// Object path relative to the index root.
        path: String,
        /// Source object-store error.
        #[source]
        source: object_store::Error,
    },

    /// Object storage rejected the operation because credentials are missing or insufficient.
    #[error("object storage permission denied at `{path}`: {source}")]
    ObjectStorePermissionDenied {
        /// Object path relative to the index root.
        path: String,
        /// Source object-store error.
        #[source]
        source: object_store::Error,
    },

    /// Arrow record batch handling failed.
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),

    /// Parquet serialization failed.
    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
}

impl BorsukError {
    /// Stable machine-readable error code for language bindings.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::DimensionMismatch { .. } => "dimension_mismatch",
            Self::InvalidMetricInput(_) => "invalid_metric_input",
            Self::InvalidRecordInput(_) => "invalid_record_input",
            Self::InvalidCompactionInput(_) => "invalid_compaction_input",
            Self::InvalidSearchOptions(_) => "invalid_search_options",
            Self::RamBudgetExceeded { .. } => "ram_budget_exceeded",
            Self::RecallGuaranteeViolated { .. } => "recall_guarantee_violated",
            Self::InvalidStorage(_) => "invalid_storage",
            Self::IndexNotFound(_) => "index_not_found",
            Self::ConcurrentModification { .. } => "concurrent_modification",
            Self::ChecksumMismatch { .. } => "checksum_mismatch",
            Self::Io { .. } => "io_error",
            Self::ObjectStore(_) => "object_store_error",
            Self::ObjectStoreRetryable { .. } => "object_store_retryable",
            Self::ObjectStoreNotFound { .. } => "object_store_not_found",
            Self::ObjectStorePermissionDenied { .. } => "object_store_permission_denied",
            Self::Arrow(_) => "arrow_error",
            Self::Parquet(_) => "parquet_error",
        }
    }
}
