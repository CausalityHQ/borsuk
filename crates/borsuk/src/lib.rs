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
#[cfg(test)]
mod global_graph;
mod global_pq_sidecar;
mod global_read_planner;
mod group_commit;
mod index;
mod late_interaction;
mod late_interaction_sidecar;
mod lexical_build;
mod lexical_root;
mod lexical_simd;
mod maintenance;
mod manifest;
mod metadata;
mod metric;
mod observability;
mod parallel;
mod physical_layout;
mod quantizer_sidecar;
mod record;
mod rotated_product_quantizer;
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
mod vortex_table;

/// Print and reset the env-gated (`BORSUK_BUILD_TIMING=1`) per-phase build timing
/// breakdown accumulated since the last call. A no-op when timing is disabled.
pub fn report_build_timing(label: &str) {
    build_timing::report_and_reset(label);
}

pub use cell_wal::{
    CellWalConfig, CellWalObjectPaths, CellWalRunInput, CellWalRunKind, CellWalStore,
    CellWalTransactionDescriptor, CommittedCellWalTransaction, DEFAULT_CELL_WAL_LANES,
    LogicalCellId, MAX_CELL_WAL_LANES, PreparedCellWalRun, PreparedCellWalTransaction,
    cell_wal_transaction_id,
};
pub use error::{BorsukError, Result};
pub use format::{vector_records_from_parquet, vector_records_to_parquet};
pub use group_commit::{GroupCommitConfig, GroupCommitReceipt, GroupCommitWriter};
pub use index::{
    BorsukIndex, DEFAULT_LATE_INTERACTION_BATCH_CACHE_BYTES, DEFAULT_MAX_CONCURRENT_CELL_DECODES,
    DEFAULT_MAX_CONCURRENT_SEARCHES, DEFAULT_RAM_BUDGET_BYTES, DEFAULT_TARGET_SEGMENT_VECTOR_BYTES,
    DEFAULT_WAL_TAIL_CACHE_BYTES, DEFAULT_WAL_TAIL_DECODE_BYTES, IndexConfig,
    MAX_RECOMMENDED_SEGMENT_MAX_VECTORS, MIN_RECOMMENDED_SEGMENT_MAX_VECTORS, OpenOptions,
    WarmReport, parse_byte_size, parse_ram_budget, recommended_segment_max_vectors,
};

/// Maximum CPU worker threads used by default index-build phases. Query
/// concurrency has separate admission controls; this cap prevents an offline
/// build from monopolizing a large production host.
pub const DEFAULT_BUILD_THREADS: usize = 4;
/// Process-wide build/query CPU worker override. Values must be in `1..=64`;
/// missing or invalid values use [`DEFAULT_BUILD_THREADS`].
pub const CPU_THREADS_ENV: &str = "BORSUK_CPU_THREADS";
/// Default shared blocking-I/O waiter count. These are small-stack waiters,
/// separate from CPU workers, so object-store fan-out does not consume more
/// compute cores.
pub const DEFAULT_IO_THREADS: usize = 24;
/// Process-wide blocking-I/O waiter override. Values must be in `1..=128`.
pub const IO_THREADS_ENV: &str = "BORSUK_IO_THREADS";

/// Return the process-wide CPU worker budget used by build and query pools.
#[must_use]
pub fn configured_cpu_threads() -> usize {
    std::env::var(CPU_THREADS_ENV)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|threads| (1..=64).contains(threads))
        .unwrap_or(DEFAULT_BUILD_THREADS)
}

/// Return the process-wide blocking-I/O waiter budget.
#[must_use]
pub fn configured_io_threads() -> usize {
    std::env::var(IO_THREADS_ENV)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|threads| (1..=128).contains(threads))
        .unwrap_or(DEFAULT_IO_THREADS)
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
pub use metric::{VectorMetric, recall_at_k, tie_aware_recall_at_k, vector_metric_names};
#[doc(hidden)]
pub use object_store::ObjectStore;
pub use physical_layout::{
    CURRENT_LAYOUT_POLICY_VERSION, PhysicalFormat, PhysicalLayoutContext, PhysicalLayoutPolicy,
    PhysicalLayoutPolicyKind, PhysicalLayoutRef, PhysicalLayoutRule, RANGE_INTEGRITY_CHUNK_BYTES,
    WAL_VORTEX_CANDIDATE_ELEMENT_TYPES, WAL_VORTEX_CANDIDATE_MIN_DIMENSIONS,
    WAL_VORTEX_CANDIDATE_MIN_ROWS, production_object_roles,
};
pub use record::{
    AddReport, BuildConfig, CacheExecutionPolicy, CompactionOptions, CompactionReport,
    DEFAULT_COMPACTION_MAX_SEGMENTS, DEFAULT_GARBAGE_COLLECTION_MIN_AGE,
    DEFAULT_SEARCH_PREFETCH_DEPTH, DEFAULT_TURBOQUANT_SEED, DeleteReport, DurableTableFormat,
    ExplainReport, Fusion, GarbageCollectionOptions, GarbageCollectionReport,
    GlobalCellGraphConfig, GlobalPqLayout, GlobalScanCodec, HybridOptions, HybridQuery,
    IncrementalMaintenanceOptions, IncrementalReport, IndexStats, LeafCapability, LeafMode,
    PurgeReport, QuantizerKind, QueryCostModel, RebuildOptions, RebuildReport, RecallGuarantee,
    RecordId, RequestCounts, SearchHit, SearchMode, SearchOptions, SearchReport,
    SearchTerminationReason, SidecarCompression, StorageEncoding, VectorElementType, VectorKind,
    VectorRecord, VectorSpec, leaf_mode_names,
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
