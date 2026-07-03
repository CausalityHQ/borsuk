use std::{fmt, str::FromStr};

use crate::{BorsukError, Result};

const LEAF_MODE_NAMES: &[&str] = &[
    "flat-scan",
    "sq-scan",
    "pq-scan",
    "graph",
    "vamana-pq",
    "hybrid",
];

/// External record identifier stored as opaque bytes.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RecordId(Vec<u8>);

impl RecordId {
    /// Construct an identifier from raw bytes.
    #[must_use]
    pub fn from_bytes(bytes: impl Into<Vec<u8>>) -> Self {
        Self(bytes.into())
    }

    /// Return the raw identifier bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Return the identifier as UTF-8, panicking if it is not valid UTF-8.
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.try_as_str().expect("record id is not valid UTF-8")
    }

    /// Try to return the identifier as UTF-8 for legacy string APIs.
    pub fn try_as_str(&self) -> Result<&str> {
        std::str::from_utf8(&self.0).map_err(|err| {
            BorsukError::InvalidRecordInput(format!("record id is not valid UTF-8: {err}"))
        })
    }

    /// Return the identifier as an owned UTF-8 string for legacy string APIs.
    pub fn to_utf8_string(&self) -> Result<String> {
        self.try_as_str().map(ToOwned::to_owned)
    }

    /// True when the identifier is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Remove all identifier bytes.
    pub fn clear(&mut self) {
        self.0.clear();
    }

    /// True when a UTF-8 identifier starts with `prefix`.
    #[must_use]
    pub fn starts_with(&self, prefix: &str) -> bool {
        self.try_as_str().is_ok_and(|id| id.starts_with(prefix))
    }

    /// Parse a UTF-8 identifier using `FromStr`.
    pub fn parse<T: FromStr>(&self) -> std::result::Result<T, T::Err> {
        self.as_str().parse()
    }
}

impl fmt::Debug for RecordId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Ok(id) = std::str::from_utf8(&self.0) {
            return write!(formatter, "{id:?}");
        }
        write!(formatter, "0x")?;
        for byte in &self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Display for RecordId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Ok(id) = std::str::from_utf8(&self.0) {
            return formatter.write_str(id);
        }
        write!(formatter, "0x")?;
        for byte in &self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl AsRef<[u8]> for RecordId {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl From<String> for RecordId {
    fn from(value: String) -> Self {
        Self(value.into_bytes())
    }
}

impl From<&String> for RecordId {
    fn from(value: &String) -> Self {
        Self(value.as_bytes().to_vec())
    }
}

impl From<&str> for RecordId {
    fn from(value: &str) -> Self {
        Self(value.as_bytes().to_vec())
    }
}

impl From<Vec<u8>> for RecordId {
    fn from(value: Vec<u8>) -> Self {
        Self(value)
    }
}

impl From<&[u8]> for RecordId {
    fn from(value: &[u8]) -> Self {
        Self(value.to_vec())
    }
}

impl PartialEq<&str> for RecordId {
    fn eq(&self, other: &&str) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl PartialEq<RecordId> for &str {
    fn eq(&self, other: &RecordId) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl serde::Serialize for RecordId {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if let Ok(id) = std::str::from_utf8(&self.0) {
            serializer.serialize_str(id)
        } else {
            serializer.serialize_bytes(&self.0)
        }
    }
}

impl<'de> serde::Deserialize<'de> for RecordId {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct RecordIdVisitor;

        impl<'de> serde::de::Visitor<'de> for RecordIdVisitor {
            type Value = RecordId;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a string or byte record id")
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(RecordId::from(value))
            }

            fn visit_string<E>(self, value: String) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(RecordId::from(value))
            }

            fn visit_bytes<E>(self, value: &[u8]) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(RecordId::from(value))
            }

            fn visit_byte_buf<E>(self, value: Vec<u8>) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(RecordId::from(value))
            }
        }

        deserializer.deserialize_any(RecordIdVisitor)
    }
}

/// Vector record inserted into an index.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct VectorRecord {
    /// External object identifier.
    pub id: RecordId,
    /// Dense vector payload.
    pub vector: Vec<f32>,
}

impl VectorRecord {
    /// Construct a vector record.
    pub fn new(id: impl Into<RecordId>, vector: Vec<f32>) -> Self {
        Self {
            id: id.into(),
            vector,
        }
    }

    /// Construct a vector record from raw id bytes.
    pub fn new_bytes(id: impl Into<Vec<u8>>, vector: Vec<f32>) -> Self {
        Self {
            id: RecordId::from_bytes(id),
            vector,
        }
    }
}

/// A nearest-neighbor hit returned by search.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SearchHit {
    /// External object identifier.
    pub id: RecordId,
    /// Distance to the query under the index metric.
    pub distance: f32,
}

/// Manifest-derived index statistics for capacity, storage, and RAM-budget diagnostics.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct IndexStats {
    /// Stable metric name for this physical index.
    pub metric: String,
    /// Required vector dimensionality.
    pub dimensions: usize,
    /// Maximum vectors written to each immutable segment.
    pub segment_max_vectors: usize,
    /// Effective resident metadata RAM budget in bytes, if configured.
    pub ram_budget_bytes: Option<u64>,
    /// Active manifest version.
    pub manifest_version: u64,
    /// Highest persisted routing layer for this manifest version.
    pub routing_max_level: u8,
    /// Number of active immutable segments.
    pub segments: usize,
    /// Number of active vector records.
    pub records: usize,
    /// Total bytes in active segment Parquet objects.
    pub segment_bytes: u64,
    /// Total bytes in active graph Parquet objects.
    pub graph_bytes: u64,
    /// Estimated resident bytes for manifest/config/segment summaries/pivots.
    pub resident_bytes_estimate: u64,
}

/// Search hits plus execution measurements useful for performance smoke tests and tuning.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SearchReport {
    /// Top-k hits returned by the search.
    pub hits: Vec<SearchHit>,
    /// Leaf engine used inside searched segments.
    pub leaf_mode: String,
    /// Reason the query stopped reading additional segment payloads.
    pub termination_reason: SearchTerminationReason,
    /// Total number of segment summaries ranked by the router.
    pub segments_total: usize,
    /// Number of segment payloads fetched and searched.
    pub segments_searched: usize,
    /// Number of ranked segments skipped by exact pruning or approximate budgets.
    pub segments_skipped: usize,
    /// Routing page-index, routing-page, and segment payload bytes read during the query.
    pub bytes_read: u64,
    /// Segment-local graph bytes read during approximate local traversal.
    pub graph_bytes_read: u64,
    /// Segment or graph objects served from the local read-through cache.
    pub object_cache_hits: usize,
    /// Segment or graph objects fetched from storage instead of the local cache.
    pub object_cache_misses: usize,
    /// Vector records loaded from fetched segments and considered by local routing.
    pub records_considered: usize,
    /// Vector records exact-scored with the index metric.
    pub records_scored: usize,
    /// Additional exact-scored candidates reached from segment-local graph edges.
    pub graph_candidates_added: usize,
    /// Estimated RAM bytes for manifest/config/segment summaries kept resident while searching.
    pub resident_bytes_estimate: u64,
    /// Wall-clock query time in milliseconds.
    pub elapsed_ms: u64,
}

/// Reason a search stopped reading additional segment payloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SearchTerminationReason {
    /// All selected routing candidates were searched.
    Complete,
    /// Exact lower-bound pruning proved no remaining candidate can improve the top-k set.
    ExactPruned,
    /// Approximate epsilon stopping allowed the remaining candidates to be skipped.
    Epsilon,
    /// Approximate search reached `max_segments`.
    MaxSegments,
    /// Approximate search reached `max_bytes`.
    MaxBytes,
    /// Approximate search reached `max_latency_ms`.
    MaxLatency,
}

impl SearchTerminationReason {
    /// Canonical public API name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::ExactPruned => "exact-pruned",
            Self::Epsilon => "epsilon",
            Self::MaxSegments => "max-segments",
            Self::MaxBytes => "max-bytes",
            Self::MaxLatency => "max-latency",
        }
    }
}

impl std::fmt::Display for SearchTerminationReason {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Segment-local search implementation used after global routing selects a leaf.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum LeafMode {
    /// Exact/routing-code scan over selected segment records without reading graph blocks.
    FlatScan,
    /// Scalar routing-code scan over selected segment records followed by exact rerank.
    SqScan,
    /// Product-quantized compressed scan path followed by exact rerank.
    PqScan,
    /// Segment-local graph traversal followed by exact rerank of selected candidates.
    #[default]
    Graph,
    /// PQ-seeded segment-local graph traversal followed by exact rerank.
    VamanaPq,
    /// Use each segment's stored leaf-mode metadata to choose its local search path.
    Hybrid,
}

impl LeafMode {
    /// Canonical leaf mode names accepted by the public API.
    #[must_use]
    pub fn names() -> &'static [&'static str] {
        LEAF_MODE_NAMES
    }
}

impl FromStr for LeafMode {
    type Err = BorsukError;

    fn from_str(value: &str) -> Result<Self> {
        let normalized = value.trim().to_ascii_lowercase().replace('_', "-");
        match normalized.as_str() {
            "flat" | "flat-scan" | "flatscan" => Ok(Self::FlatScan),
            "sq" | "sq-scan" | "sqscan" | "scalar-scan" | "scalar-quantized-scan" => {
                Ok(Self::SqScan)
            }
            "pq" | "pq-scan" | "pqscan" | "product-quantized-scan" => Ok(Self::PqScan),
            "graph" | "local-graph" | "segment-graph" => Ok(Self::Graph),
            "vamana" | "vamana-pq" | "vamanapq" | "diskann" | "diskann-pq" => Ok(Self::VamanaPq),
            "hybrid" | "auto" | "stored" | "stored-leaf" | "segment-leaf" => Ok(Self::Hybrid),
            _ => Err(BorsukError::InvalidSearchOptions(format!(
                "unknown leaf mode `{value}`"
            ))),
        }
    }
}

impl fmt::Display for LeafMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FlatScan => formatter.write_str("flat-scan"),
            Self::SqScan => formatter.write_str("sq-scan"),
            Self::PqScan => formatter.write_str("pq-scan"),
            Self::Graph => formatter.write_str("graph"),
            Self::VamanaPq => formatter.write_str("vamana-pq"),
            Self::Hybrid => formatter.write_str("hybrid"),
        }
    }
}

/// Canonical leaf mode names accepted by the public API.
#[must_use]
pub fn leaf_mode_names() -> &'static [&'static str] {
    LeafMode::names()
}

/// Search execution mode.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum SearchMode {
    /// Exact search using safe lower-bound pruning for metrics that support it.
    Exact,
    /// Approximate search with optional traversal budgets.
    Approx {
        /// Segment-local leaf engine used after global routing.
        #[serde(default)]
        leaf_mode: LeafMode,
        /// Epsilon used for bounded early stopping.
        eps: Option<f32>,
        /// Maximum number of segments to fetch and search.
        max_segments: Option<usize>,
        /// Best-effort segment payload byte budget.
        max_bytes: Option<u64>,
        /// Best-effort wall-clock budget in milliseconds.
        max_latency_ms: Option<u64>,
        /// Maximum exact-scored records per fetched segment after sketch ranking.
        max_candidates_per_segment: Option<usize>,
    },
}

impl SearchMode {
    /// Leaf engine used by this search mode.
    #[must_use]
    pub fn leaf_mode(&self) -> LeafMode {
        match self {
            Self::Exact => LeafMode::FlatScan,
            Self::Approx { leaf_mode, .. } => *leaf_mode,
        }
    }
}

/// Search options.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SearchOptions {
    /// Number of nearest hits to return.
    pub k: usize,
    /// Search execution mode.
    pub mode: SearchMode,
}

impl SearchOptions {
    /// Construct exact-search options.
    #[must_use]
    pub fn exact(k: usize) -> Self {
        Self {
            k,
            mode: SearchMode::Exact,
        }
    }

    /// Construct approximate-search options with a typed segment-local leaf mode.
    #[must_use]
    pub fn approx(k: usize, leaf_mode: LeafMode) -> Self {
        Self {
            k,
            mode: SearchMode::Approx {
                leaf_mode,
                eps: None,
                max_segments: None,
                max_bytes: None,
                max_latency_ms: None,
                max_candidates_per_segment: None,
            },
        }
    }

    /// Set the approximate-search epsilon budget.
    #[must_use]
    pub fn with_eps(mut self, eps: f32) -> Self {
        if let SearchMode::Approx {
            eps: current_eps, ..
        } = &mut self.mode
        {
            *current_eps = Some(eps);
        }
        self
    }

    /// Set the maximum number of segments fetched by approximate search.
    #[must_use]
    pub fn with_max_segments(mut self, max_segments: usize) -> Self {
        if let SearchMode::Approx {
            max_segments: current_max_segments,
            ..
        } = &mut self.mode
        {
            *current_max_segments = Some(max_segments);
        }
        self
    }

    /// Set the best-effort segment payload byte budget for approximate search.
    #[must_use]
    pub fn with_max_bytes(mut self, max_bytes: u64) -> Self {
        if let SearchMode::Approx {
            max_bytes: current_max_bytes,
            ..
        } = &mut self.mode
        {
            *current_max_bytes = Some(max_bytes);
        }
        self
    }

    /// Set the best-effort wall-clock budget in milliseconds for approximate search.
    #[must_use]
    pub fn with_max_latency_ms(mut self, max_latency_ms: u64) -> Self {
        if let SearchMode::Approx {
            max_latency_ms: current_max_latency_ms,
            ..
        } = &mut self.mode
        {
            *current_max_latency_ms = Some(max_latency_ms);
        }
        self
    }

    /// Set the maximum exact-scored records per fetched segment.
    #[must_use]
    pub fn with_max_candidates_per_segment(mut self, max_candidates_per_segment: usize) -> Self {
        if let SearchMode::Approx {
            max_candidates_per_segment: current_max_candidates_per_segment,
            ..
        } = &mut self.mode
        {
            *current_max_candidates_per_segment = Some(max_candidates_per_segment);
        }
        self
    }
}

impl Default for SearchOptions {
    fn default() -> Self {
        Self {
            k: 10,
            mode: SearchMode::Exact,
        }
    }
}

/// Default bounded source-segment batch for incremental compaction.
pub const DEFAULT_COMPACTION_MAX_SEGMENTS: usize = 32;

/// Options for out-of-place segment compaction.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CompactionOptions {
    /// Level to compact from, typically L0.
    pub source_level: u8,
    /// Level to write compacted output into, typically L1 or L2.
    pub target_level: u8,
    /// Maximum number of source segments to compact. `None` means all matching
    /// segments at `source_level` and is intended for explicit offline rebuilds.
    /// The default keeps compaction scoped to a bounded source-leaf batch.
    pub max_segments: Option<usize>,
    /// Minimum number of matching source segments required before compaction runs.
    /// Must be less than or equal to `max_segments` when `max_segments` is set.
    pub min_segments: usize,
    /// Maximum vectors per compacted output segment. Defaults to the index segment size.
    /// Must be greater than zero when set; invalid values are rejected before storage reads.
    pub target_segment_max_vectors: Option<usize>,
}

impl Default for CompactionOptions {
    fn default() -> Self {
        Self {
            source_level: 0,
            target_level: 1,
            max_segments: Some(DEFAULT_COMPACTION_MAX_SEGMENTS),
            min_segments: 2,
            target_segment_max_vectors: None,
        }
    }
}

/// Result of an out-of-place compaction attempt.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CompactionReport {
    /// Whether any segments were rewritten and a new manifest was published.
    pub compacted: bool,
    /// Level compacted from.
    pub source_level: u8,
    /// Level compacted into.
    pub target_level: u8,
    /// Number of source segment payloads read.
    pub segments_read: usize,
    /// Number of compacted segment payloads written.
    pub segments_written: usize,
    /// Number of vector records copied into compacted segments.
    pub records_rewritten: usize,
    /// Routing page-index objects read while selecting and publishing the compaction.
    pub routing_page_indexes_read: usize,
    /// Routing page content objects read while selecting and publishing the compaction.
    pub routing_pages_read: usize,
    /// Routing page-index objects written while publishing the compacted version.
    pub routing_page_indexes_written: usize,
    /// Routing page content objects written while publishing the compacted version.
    pub routing_pages_written: usize,
    /// Old graph payload objects read by compaction. Graphs are derived and should stay zero.
    pub graph_payloads_read: usize,
    /// Old graph payload bytes read by compaction. Graphs are derived and should stay zero.
    pub graph_bytes_read: u64,
    /// Routing page-index, routing-page, and source segment payload bytes read.
    pub bytes_read: u64,
    /// Compacted segment payload bytes written.
    pub bytes_written: u64,
    /// Routing-page or source segment objects served from the local read-through cache.
    pub object_cache_hits: usize,
    /// Routing-page or source segment objects fetched from storage instead of the local cache.
    pub object_cache_misses: usize,
    /// Manifest version active after the compaction attempt.
    pub manifest_version: u64,
}

/// Options for garbage collecting inactive segment objects.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GarbageCollectionOptions {
    /// When true, report obsolete objects without deleting them.
    pub dry_run: bool,
}

impl Default for GarbageCollectionOptions {
    fn default() -> Self {
        Self { dry_run: true }
    }
}

/// Result of scanning obsolete segment objects.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GarbageCollectionReport {
    /// Whether this run only reported candidates.
    pub dry_run: bool,
    /// Number of segment objects scanned under the segment prefix.
    pub objects_scanned: usize,
    /// Number of obsolete segment objects deleted.
    pub objects_deleted: usize,
    /// Bytes that could be reclaimed from the reported candidates.
    pub bytes_reclaimable: u64,
    /// Bytes actually reclaimed by deletion.
    pub bytes_reclaimed: u64,
    /// Obsolete segment paths relative to the index root.
    pub candidates: Vec<String>,
}

/// Options for a full source-level rebuild followed by obsolete-object cleanup.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RebuildOptions {
    /// Level to rewrite from, typically L0.
    pub source_level: u8,
    /// Level to write rebuilt output into, typically L1 or L2.
    pub target_level: u8,
    /// Minimum matching source segments required before the rebuild compaction runs.
    pub min_segments: usize,
    /// Maximum vectors per rebuilt output segment. Defaults to the index segment size.
    pub target_segment_max_vectors: Option<usize>,
    /// Delete obsolete segment and graph objects after publishing the rebuilt manifest.
    pub delete_obsolete: bool,
}

impl Default for RebuildOptions {
    fn default() -> Self {
        Self {
            source_level: 0,
            target_level: 1,
            min_segments: 1,
            target_segment_max_vectors: None,
            delete_obsolete: false,
        }
    }
}

/// Result of a full source-level rebuild and cleanup pass.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RebuildReport {
    /// Full-scope compaction report for the requested source and target levels.
    pub compaction: CompactionReport,
    /// Obsolete-object cleanup report run after compaction.
    pub garbage_collection: GarbageCollectionReport,
}
