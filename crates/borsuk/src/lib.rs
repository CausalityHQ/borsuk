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
pub use index::{BorsukIndex, IndexConfig, parse_ram_budget};
pub use manifest::{Manifest, PivotSummary, SegmentSummary};
pub use metric::{StringMetric, VectorMetric};
pub use record::{
    CompactionOptions, CompactionReport, GarbageCollectionOptions, GarbageCollectionReport,
    SearchHit, SearchMode, SearchOptions, SearchReport, VectorRecord,
};
