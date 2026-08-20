//! BORSUK core library.
#![forbid(unsafe_code)]
//!
//! BORSUK stands for Blob-Oriented Retrieval with Segmental Unified KNN. The
//! core crate stores vectors in immutable external segments and keeps only
//! manifest-level segment summaries in memory while searching.

mod arrow_vector_sidecar;
mod bm25;
pub(crate) mod build_timing;
mod cell_wal;
mod centroid_hnsw;
mod collection_control;
mod error;
mod float8;
mod format;
#[allow(
    dead_code,
    reason = "V17 cell-card codec is wired into publication and query in the next planned slices"
)]
mod global_cell_card;
mod global_leaf;
mod global_leaf_run;
mod global_pq_sidecar;
mod group_commit;
mod index;
mod lane_log;
mod late_interaction;
mod late_interaction_sidecar;
mod lexical_build;
mod lexical_root;
mod lexical_simd;
mod logical_cell_catalog;
mod maintenance;
mod manifest;
mod metadata;
mod metric;
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "mutation clock foundation is wired into persistence in the next planned slice"
    )
)]
mod mutation;
#[cfg_attr(
    not(test),
    allow(
        dead_code,
        reason = "standard mutation extent is connected to the lane writer in the next planned slice"
    )
)]
mod mutation_extent;
mod observability;
mod parallel;
mod physical_layout;
mod positioned_candidate;
mod positioned_log;
mod positioned_materializer;
mod quantizer_sidecar;
mod record;
mod rotated_product_quantizer;
#[allow(
    dead_code,
    reason = "pure row-bundle construction is wired only at the Task 4 atomic format switch"
)]
mod row_bundle;
mod scalar_decode;
mod segment;
mod segment_cache;
mod simd_control;
pub mod sparse;
pub mod sparse_index;
mod storage;
mod storage_trace;
/// Text tokenization helpers for per-record term-frequency storage.
pub mod text;
mod turboquant;

/// Print and reset the env-gated (`BORSUK_BUILD_TIMING=1`) per-phase build timing
/// breakdown accumulated since the last call. When
/// `BORSUK_BUILD_TIMING_OUTPUT` is set, the same fixed-schema rows are appended
/// to that CSV. A no-op when timing is disabled.
pub fn report_build_timing(label: &str) -> std::io::Result<()> {
    build_timing::report_and_reset(label)
}

/// Public API policy checks.
///
/// Low-level mutation-log storage types are implementation details, not an
/// alternate production durability API.
///
/// ```compile_fail
/// use borsuk::{CellWalRunInput, CellWalStore};
/// ```
pub mod public_api_policy {}

pub use cell_wal::{
    CellWalConfig, CellWalRunKind, DEFAULT_CELL_WAL_LANES, LogicalCellId, MAX_CELL_WAL_LANES,
};
pub use error::{BorsukError, Result};
pub use format::{vector_records_from_parquet, vector_records_to_parquet};
pub use group_commit::{
    GroupCommitConfig, GroupCommitReceipt, GroupCommitTicket, GroupCommitWriter,
};
pub use index::{
    AdmissionStats, BorsukIndex, CanonicalMutationBatch,
    DEFAULT_EXACT_READ_MAX_PHYSICAL_AMPLIFICATION, DEFAULT_LATE_INTERACTION_BATCH_CACHE_BYTES,
    DEFAULT_LEAF_READ_WIDTH, DEFAULT_MAX_ACTIVE_SEARCHES, DEFAULT_MAX_INFLIGHT_LEAF_READS,
    DEFAULT_MAX_WAITING_SEARCHES, DEFAULT_RAM_BUDGET_BYTES, DEFAULT_ROUTING_PAGE_CACHE_BYTES,
    DEFAULT_TARGET_SEGMENT_VECTOR_BYTES, DEFAULT_WAL_TAIL_CACHE_BYTES,
    DEFAULT_WAL_TAIL_DECODE_BYTES, FlowControlStats, IndexConfig,
    MAX_RECOMMENDED_SEGMENT_MAX_VECTORS, MIN_RECOMMENDED_SEGMENT_MAX_VECTORS, OpenOptions,
    WarmReport, parse_byte_size, parse_ram_budget, recommended_segment_max_vectors,
};
pub use lane_log::GROUP_COMMIT_STRIPE_COUNT;
#[doc(hidden)]
pub use logical_cell_catalog::train_logical_cell_centroids;

/// Maximum CPU worker threads selected by the automatic small-runtime policy.
pub const DEFAULT_BUILD_THREADS: usize = 4;
/// Process-wide build/query CPU worker override. Values must be in `1..=64`;
/// missing or invalid values use the automatic policy: one fewer than the
/// available CPUs, clamped to `1..=DEFAULT_BUILD_THREADS`.
pub const CPU_THREADS_ENV: &str = "BORSUK_CPU_THREADS";
/// Default shared blocking-I/O waiter count. These are parked network waiters,
/// separate from CPU workers, and cover one qualified page-32 object-store
/// wave without adding a hidden second round trip.
pub const DEFAULT_IO_THREADS: usize = 88;
/// Process-wide blocking-I/O waiter override. Values must be in `1..=256` and
/// at least the configured physical GET concurrency.
pub const IO_THREADS_ENV: &str = "BORSUK_IO_THREADS";
/// Default process-wide concurrency limit for physical backing-store GETs.
pub const DEFAULT_BACKING_GET_CONCURRENCY: usize = 64;
/// Process-wide physical backing-store GET concurrency override.
pub const BACKING_GET_CONCURRENCY_ENV: &str = "BORSUK_BACKING_GET_CONCURRENCY";

/// Process-wide physical-resource limits shared by every BORSUK handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessLimits {
    /// Workers shared by verification, decode, SIMD ranking, and builds.
    pub cpu_threads: usize,
    /// Blocking I/O waiters. This must be at least the backing GET cap so the
    /// network semaphore, rather than the waiter pool, is the truthful limit.
    pub io_threads: usize,
    /// Physical backing-store GETs in flight across the process.
    pub s3_get_concurrency: usize,
}

impl Default for ProcessLimits {
    fn default() -> Self {
        let cpus = std::thread::available_parallelism().map_or(1, usize::from);
        Self {
            cpu_threads: automatic_cpu_threads(cpus),
            io_threads: DEFAULT_IO_THREADS,
            s3_get_concurrency: DEFAULT_BACKING_GET_CONCURRENCY,
        }
    }
}

static PROCESS_LIMITS: std::sync::OnceLock<ProcessLimits> = std::sync::OnceLock::new();

/// Configure process-wide pools before their first use. Repeating the same
/// configuration is idempotent; a conflicting late configuration fails.
pub fn configure_process(limits: ProcessLimits) -> Result<()> {
    validate_process_limits(&limits)?;
    if let Some(installed) = PROCESS_LIMITS.get() {
        return if installed == &limits {
            Ok(())
        } else {
            Err(BorsukError::InvalidOpenOptions(
                "process limits are already configured differently".to_string(),
            ))
        };
    }
    PROCESS_LIMITS.set(limits).map_err(|_| {
        BorsukError::InvalidOpenOptions(
            "process limits were configured concurrently with different values".to_string(),
        )
    })
}

fn automatic_cpu_threads(cpus: usize) -> usize {
    cpus.saturating_sub(1).clamp(1, DEFAULT_BUILD_THREADS)
}

fn validate_process_limits(limits: &ProcessLimits) -> Result<()> {
    if !(1..=64).contains(&limits.cpu_threads) {
        return Err(BorsukError::InvalidOpenOptions(
            "process cpu_threads must be in 1..=64".to_string(),
        ));
    }
    if !(1..=128).contains(&limits.s3_get_concurrency) {
        return Err(BorsukError::InvalidOpenOptions(
            "process s3_get_concurrency must be in 1..=128".to_string(),
        ));
    }
    if !(limits.s3_get_concurrency..=256).contains(&limits.io_threads) {
        return Err(BorsukError::InvalidOpenOptions(format!(
            "process io_threads must be in {}..=256",
            limits.s3_get_concurrency
        )));
    }
    Ok(())
}

fn configured_process_limits() -> &'static ProcessLimits {
    PROCESS_LIMITS.get_or_init(|| {
        let defaults = ProcessLimits::default();
        let cpu_threads = std::env::var(CPU_THREADS_ENV)
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|threads| (1..=64).contains(threads))
            .unwrap_or(defaults.cpu_threads);
        let s3_get_concurrency = parse_backing_get_concurrency(
            std::env::var(BACKING_GET_CONCURRENCY_ENV).ok().as_deref(),
        );
        let io_threads = std::env::var(IO_THREADS_ENV)
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|threads| (s3_get_concurrency..=256).contains(threads))
            .unwrap_or_else(|| DEFAULT_IO_THREADS.max(s3_get_concurrency));
        ProcessLimits {
            cpu_threads,
            io_threads,
            s3_get_concurrency,
        }
    })
}

/// Return the process-wide CPU worker budget used by build and query pools.
#[must_use]
pub fn configured_cpu_threads() -> usize {
    configured_process_limits().cpu_threads
}

/// Return the process-wide blocking-I/O waiter budget.
#[must_use]
pub fn configured_io_threads() -> usize {
    configured_process_limits().io_threads
}

/// Return the validated process-wide physical backing-store GET concurrency limit.
#[must_use]
pub fn configured_backing_get_concurrency() -> usize {
    configured_process_limits().s3_get_concurrency
}

fn parse_backing_get_concurrency(value: Option<&str>) -> usize {
    value
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| (1..=128).contains(value))
        .unwrap_or(DEFAULT_BACKING_GET_CONCURRENCY)
}

pub use late_interaction::{
    LateInteractionSearchOptions, LateInteractionSearchReport, LateInteractionVector,
    late_interaction_maxsim,
};
pub use maintenance::{
    DEFAULT_MAINTENANCE_LEASE_TTL, MaintenanceConfig, MaintenanceHandle, MaintenanceReport,
};
pub use manifest::{
    DEFAULT_GRAPH_NEIGHBORS, DEFAULT_ROUTING_PAGE_FANOUT,
    DEFAULT_WAL_COLLECTION_FLUSH_THRESHOLD_BYTES, DEFAULT_WAL_FLUSH_THRESHOLD_BYTES,
    DEFAULT_WAL_FLUSH_THRESHOLD_RECORDS, DEFAULT_WAL_FLUSH_THRESHOLD_RUNS, Manifest, PivotSummary,
    SegmentSummary, WalConfig,
};
pub use metadata::{
    Filter, MetaValue, Metadata, MetadataIndex, MetadataStats, Op, metadata_from_json,
    metadata_to_json,
};
/// A dense vector and its stored metadata returned by point-read APIs.
pub type StoredRecord = (Vec<f32>, Metadata);
pub use metric::{VectorMetric, recall_at_k, tie_aware_recall_at_k, vector_metric_names};
#[doc(hidden)]
pub use object_store::ObjectStore;
pub use physical_layout::{
    CURRENT_LAYOUT_POLICY_VERSION, PhysicalFormat, PhysicalLayoutPolicy, PhysicalLayoutRef,
    production_object_roles,
};
pub use positioned_log::{
    CommitSourcePosition, CommittedPositionedMutation, MAX_APPEND_ENCODED_BYTES, MAX_APPEND_ROWS,
    MAX_HEAD_CAS_ATTEMPTS, MAX_PAYLOADS_PER_TRANSACTION, MAX_PENDING_ENVELOPES_PER_SHARD,
    MAX_RECENT_COMMIT_RECEIPTS_PER_SHARD, MAX_SHARD_HEAD_BYTES, MAX_UNMATERIALIZED_BYTES_PER_SHARD,
    MAX_UNMATERIALIZED_ROWS_PER_SHARD, PositionedLogReader, PositionedLogSnapshot,
    PositionedLogWriter, PositionedMaterializationWatermark, PositionedMutationEnvelope,
    PositionedMutationModality, PositionedMutationPayloadInput, PositionedMutationPayloadRef,
    PositionedMutationStamp, PositionedPayloadFormat, SOURCE_SHARD_COUNT,
};
pub use record::{
    AddReport, BuildConfig, CacheExecutionPolicy, CompactionOptions, CompactionReport,
    DEFAULT_COMPACTION_MAX_SEGMENTS, DEFAULT_GARBAGE_COLLECTION_MIN_AGE,
    DEFAULT_SEARCH_PREFETCH_DEPTH, DEFAULT_TURBOQUANT_SEED, DeleteReport, ExplainReport, Fusion,
    GarbageCollectionOptions, GarbageCollectionReport, GlobalPqLayout, GlobalScanCodec,
    HybridOptions, HybridQuery, IncrementalMaintenanceOptions, IncrementalReport, IndexStats,
    LeafCapability, LeafMode, PurgeReport, QuantizerKind, QueryCostModel, RebuildOptions,
    RebuildReport, RecallGuarantee, RecordId, RequestCounts, SearchHit, SearchMode, SearchOptions,
    SearchReport, SearchTerminationReason, SidecarCompression, StorageEncoding, VectorElementType,
    VectorKind, VectorRecord, VectorSpec, leaf_mode_names,
};
pub use sparse::{
    SparseVector, VectorView, cosine_distance, dot, euclidean_distance, inner_product_distance,
    sparse_dense_dot, sparse_dot, squared_euclidean_distance, squared_norm, squared_norm_dense,
    squared_norm_sparse,
};
pub use sparse_index::SparseIndex;
pub use storage_trace::{
    PhysicalObjectRole, StorageAccessEvent, StorageAccessTrace, install_storage_access_trace,
    physical_object_role_for_path,
};
pub use text::{CharNgram, Tokenizer, UnicodeWordLowercase, Whitespace, term_frequencies, term_id};

#[cfg(test)]
mod configuration_tests {
    #[test]
    fn automatic_cpu_budget_reserves_capacity_for_the_embedding_app() {
        assert_eq!(super::automatic_cpu_threads(1), 1);
        assert_eq!(super::automatic_cpu_threads(2), 1);
        assert_eq!(super::automatic_cpu_threads(4), 3);
        assert_eq!(super::automatic_cpu_threads(64), 4);
    }

    #[test]
    fn default_io_waiters_make_the_physical_get_cap_truthful() {
        let limits = super::ProcessLimits::default();
        assert!(limits.io_threads >= limits.s3_get_concurrency);
        super::validate_process_limits(&limits).unwrap();
        let invalid = super::ProcessLimits {
            io_threads: limits.s3_get_concurrency - 1,
            ..limits
        };
        assert!(super::validate_process_limits(&invalid).is_err());
    }

    #[test]
    fn physical_get_admission_configuration_is_fail_closed() {
        assert_eq!(super::parse_backing_get_concurrency(None), 64);
        assert_eq!(super::parse_backing_get_concurrency(Some("1")), 1);
        assert_eq!(super::parse_backing_get_concurrency(Some("128")), 128);
        for invalid in ["", "0", "129", "1024", "many"] {
            assert_eq!(super::parse_backing_get_concurrency(Some(invalid)), 64);
        }
    }
}
