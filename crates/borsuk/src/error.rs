use std::{io, path::PathBuf, sync::Arc};

use crate::record::{LeafCapability, LeafMode, SearchTerminationReason};

/// Result type used by the BORSUK core crate.
pub type Result<T> = std::result::Result<T, BorsukError>;

/// Errors returned by BORSUK operations.
#[derive(Debug, thiserror::Error)]
pub enum BorsukError {
    /// One immutable asynchronous operation failed for multiple overlapping
    /// callers. Preserve the original classification while sharing ownership.
    #[error(transparent)]
    Shared(Arc<BorsukError>),
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

    /// Index-open runtime and flow-control options were invalid.
    #[error("invalid open options: {0}")]
    InvalidOpenOptions(String),

    /// The bounded search admission queue is full.
    #[error("search overloaded: {active} active and {waiting} waiting")]
    Overloaded {
        /// Searches currently holding an active permit.
        active: usize,
        /// Searches already waiting for a permit.
        waiting: usize,
    },

    /// A search requested a leaf mode the index was not built for.
    ///
    /// A `PqScanOnly` index skips per-segment graph construction, so a search
    /// requesting a graph-backed leaf mode (`Graph`/`VamanaPq`/`Hybrid`) has no
    /// graph to read. Recreate the index with `LeafCapability::GraphEnabled`, or
    /// search with a scan leaf mode (`PqScan`/`SqScan`/`FlatScan`).
    #[error("leaf mode `{requested}` not configured: index leaf capability is `{capability}`")]
    LeafModeNotConfigured {
        /// Leaf mode the search requested.
        requested: LeafMode,
        /// Leaf capability the index was created with.
        capability: LeafCapability,
    },

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

    /// Foreground ingest reached the configured durable-tail bound and must
    /// wait for background materialization to advance the lane.
    #[error(
        "ingest backpressure on lane {lane}: tail would reach {tail_bytes} bytes/{tail_records} records (limits {max_bytes} bytes/{max_records} records)"
    )]
    IngestBackpressure {
        /// Lane whose durable tail is full.
        lane: u16,
        /// Bytes the append would retain in the tail.
        tail_bytes: u64,
        /// Records the append would retain in the tail.
        tail_records: u64,
        /// Hard byte limit.
        max_bytes: u64,
        /// Hard record limit.
        max_records: u64,
    },

    /// The online immutable delta beside a corpus-wide ANN base is full.
    /// Already-committed WAL mutations remain visible; background or explicit
    /// compaction must publish a fresh base before another flush can drain them.
    #[error(
        "global ANN delta requires maintenance: {segments} segments/{rows} rows/{vector_bytes} vector bytes would exceed limits {max_segments}/{max_rows}/{max_vector_bytes}"
    )]
    GlobalDeltaCapacityExceeded {
        /// Segment count the requested flush would publish.
        segments: usize,
        /// Logical row count the requested flush would publish.
        rows: usize,
        /// Conservative uncompressed vector bytes represented by those rows.
        vector_bytes: usize,
        /// Maximum online delta segments.
        max_segments: usize,
        /// Maximum online delta rows.
        max_rows: usize,
        /// Maximum online delta vector bytes.
        max_vector_bytes: usize,
    },

    /// A prior mutation is durable, but this handle could not finish its one
    /// deferred claim-authorization cleanup before starting another mutation.
    #[error(
        "deferred claim cleanup for positioned commit {source_epoch}/{shard}/{sequence} ({envelope_checksum}) failed: {cleanup}"
    )]
    DeferredClaimCleanupFailed {
        /// Durable source epoch containing the committed mutation.
        source_epoch: u64,
        /// Durable source shard containing the committed mutation.
        shard: u8,
        /// Durable source sequence containing the committed mutation.
        sequence: u64,
        /// Checksum of the authoritative position-bearing envelope.
        envelope_checksum: String,
        /// Cleanup failure from the backing coordination store.
        cleanup: String,
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
            Self::Shared(error) => error.code(),
            Self::DimensionMismatch { .. } => "dimension_mismatch",
            Self::InvalidMetricInput(_) => "invalid_metric_input",
            Self::InvalidRecordInput(_) => "invalid_record_input",
            Self::InvalidCompactionInput(_) => "invalid_compaction_input",
            Self::InvalidSearchOptions(_) => "invalid_search_options",
            Self::InvalidOpenOptions(_) => "invalid_open_options",
            Self::Overloaded { .. } => "overloaded",
            Self::LeafModeNotConfigured { .. } => "leaf_mode_not_configured",
            Self::RamBudgetExceeded { .. } => "ram_budget_exceeded",
            Self::IngestBackpressure { .. } => "ingest_backpressure",
            Self::GlobalDeltaCapacityExceeded { .. } => "maintenance_required",
            Self::DeferredClaimCleanupFailed { .. } => "deferred_claim_cleanup_failed",
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
