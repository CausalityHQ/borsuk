//! BORSUK core library.
#![forbid(unsafe_code)]
//!
//! BORSUK stands for Blob-Oriented Retrieval with Segmental Unified KNN. The
//! core crate stores vectors in immutable external segments and keeps only
//! manifest-level segment summaries in memory while searching.

mod error;
mod format;
mod index;
mod manifest;
mod metric;
mod record;
mod segment;
mod storage;

pub use error::{BorsukError, Result};
pub use format::{vector_records_from_parquet, vector_records_to_parquet};
pub use index::{BorsukIndex, IndexConfig, OpenOptions, parse_byte_size, parse_ram_budget};
pub use manifest::{Manifest, PivotSummary, SegmentSummary};
pub use metric::{VectorMetric, recall_at_k, vector_metric_names};
pub use record::{
    CompactionOptions, CompactionReport, DEFAULT_COMPACTION_MAX_SEGMENTS, GarbageCollectionOptions,
    GarbageCollectionReport, IndexStats, LeafMode, RebuildOptions, RebuildReport, RecordId,
    SearchHit, SearchMode, SearchOptions, SearchReport, SearchTerminationReason, VectorRecord,
    leaf_mode_names,
};
