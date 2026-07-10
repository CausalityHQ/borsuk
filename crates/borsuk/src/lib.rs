//! BORSUK core library.
#![forbid(unsafe_code)]
//!
//! BORSUK stands for Blob-Oriented Retrieval with Segmental Unified KNN. The
//! core crate stores vectors in immutable external segments and keeps only
//! manifest-level segment summaries in memory while searching.

mod bm25;
mod error;
mod format;
mod index;
mod maintenance;
mod manifest;
mod metadata;
mod metric;
mod observability;
mod record;
mod segment;
mod segment_cache;
pub mod sparse;
mod storage;
/// Text tokenization helpers for per-record term-frequency storage.
pub mod text;

pub use error::{BorsukError, Result};
pub use format::{vector_records_from_parquet, vector_records_to_parquet};
pub use index::{BorsukIndex, IndexConfig, OpenOptions, parse_byte_size, parse_ram_budget};
pub use maintenance::{
    DEFAULT_MAINTENANCE_LEASE_TTL, MaintenanceConfig, MaintenanceHandle, MaintenanceReport,
};
pub use manifest::{
    DEFAULT_GRAPH_NEIGHBORS, DEFAULT_ROUTING_PAGE_FANOUT, Manifest, PivotSummary, SegmentSummary,
};
pub use metadata::{
    Filter, MetaValue, Metadata, MetadataIndex, MetadataStats, Op, metadata_from_json,
    metadata_to_json,
};
pub use metric::{VectorMetric, recall_at_k, tie_aware_recall_at_k, vector_metric_names};
#[doc(hidden)]
pub use object_store::ObjectStore;
pub use record::{
    AddReport, CompactionOptions, CompactionReport, DEFAULT_COMPACTION_MAX_SEGMENTS,
    DEFAULT_GARBAGE_COLLECTION_MIN_AGE, DEFAULT_SEARCH_PREFETCH_DEPTH, DeleteReport, Fusion,
    GarbageCollectionOptions, GarbageCollectionReport, HybridOptions, HybridQuery,
    IncrementalMaintenanceOptions, IncrementalReport, IndexStats, LeafMode, PurgeReport,
    RebuildOptions, RebuildReport, RecallGuarantee, RecordId, RequestCounts, SearchHit, SearchMode,
    SearchOptions, SearchReport, SearchTerminationReason, StorageEncoding, VectorRecord,
    VectorSpec, leaf_mode_names,
};
pub use sparse::SparseVector;
pub use text::{CharNgram, Tokenizer, UnicodeWordLowercase, Whitespace, term_frequencies, term_id};
