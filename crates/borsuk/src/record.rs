use std::{fmt, str::FromStr, time::Duration};

use crate::{BorsukError, Result};

/// Default maximum number of segment payload reads that search may prefetch.
pub const DEFAULT_SEARCH_PREFETCH_DEPTH: usize = 8;

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
    /// Optional typed metadata carried with the record (empty map = none).
    #[serde(default)]
    pub metadata: crate::Metadata,
}

impl VectorRecord {
    /// Construct a vector record with no metadata.
    pub fn new(id: impl Into<RecordId>, vector: Vec<f32>) -> Self {
        Self {
            id: id.into(),
            vector,
            metadata: crate::Metadata::new(),
        }
    }

    /// Construct a vector record from raw id bytes with no metadata.
    pub fn new_bytes(id: impl Into<Vec<u8>>, vector: Vec<f32>) -> Self {
        Self {
            id: RecordId::from_bytes(id),
            vector,
            metadata: crate::Metadata::new(),
        }
    }

    /// Attach typed metadata to this record.
    #[must_use]
    pub fn with_metadata(mut self, metadata: crate::Metadata) -> Self {
        self.metadata = metadata;
        self
    }
}

/// A nearest-neighbor hit returned by search.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SearchHit {
    /// External object identifier.
    pub id: RecordId,
    /// Distance to the query under the index metric.
    pub distance: f32,
    /// The hit's metadata, present only when `include_metadata` was requested.
    #[serde(default)]
    pub metadata: Option<crate::Metadata>,
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
    /// Number of routing page refs grouped into one parent routing page.
    pub routing_page_fanout: usize,
    /// Number of L0 leaf routing pages for active segment summaries.
    pub routing_leaf_pages: usize,
    /// Total routing page content objects across all active routing layers.
    pub routing_pages: usize,
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

/// Object-store requests issued while executing an operation.
///
/// Counts every request the storage layer sent to the backing object store,
/// including retries, so soak tests and production monitors can derive request
/// rate (requests per query, per add) independently of bytes transferred.
/// Multipart uploads count as a single put per initiation; ranged and batched
/// reads each count as one get.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RequestCounts {
    /// GET requests (full object, ranged, and batched range reads).
    pub gets: u64,
    /// PUT requests, counting each multipart upload initiation as one put.
    pub puts: u64,
    /// DELETE requests.
    pub deletes: u64,
    /// HEAD requests (object existence and size probes).
    pub heads: u64,
    /// LIST requests.
    pub lists: u64,
}

impl RequestCounts {
    /// Total requests across all operation kinds.
    #[must_use]
    pub fn total(&self) -> u64 {
        self.gets + self.puts + self.deletes + self.heads + self.lists
    }

    /// Per-field difference from an earlier snapshot, saturating at zero, giving
    /// the requests issued between the two snapshots.
    #[must_use]
    pub fn delta(&self, earlier: &RequestCounts) -> RequestCounts {
        RequestCounts {
            gets: self.gets.saturating_sub(earlier.gets),
            puts: self.puts.saturating_sub(earlier.puts),
            deletes: self.deletes.saturating_sub(earlier.deletes),
            heads: self.heads.saturating_sub(earlier.heads),
            lists: self.lists.saturating_sub(earlier.lists),
        }
    }
}

/// Result of a logical delete: records tombstoned so reads skip them. Physical
/// space is reclaimed later by compaction or an explicit purge.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DeleteReport {
    /// Record ids that were newly tombstoned by this call (already-deleted ids
    /// and re-requests are not counted).
    pub deleted: usize,
    /// Total record ids in the cumulative tombstone after this delete.
    pub total_tombstoned: usize,
    /// True when this delete changed the index (published a new version).
    pub published: bool,
    /// Object-store requests issued while publishing this delete.
    #[serde(default)]
    pub requests: RequestCounts,
}

/// Result of an on-demand purge: tombstoned rows physically removed by rewriting
/// the segments that held them, reclaiming storage synchronously.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PurgeReport {
    /// Segments rewritten to drop tombstoned rows.
    pub segments_rewritten: usize,
    /// Tombstoned rows physically removed.
    pub records_purged: usize,
    /// Tombstone ids cleared from the cumulative tombstone after purge.
    pub tombstones_cleared: usize,
    /// True when this purge changed the index (published a new version).
    pub published: bool,
    /// Object-store requests issued while purging.
    #[serde(default)]
    pub requests: RequestCounts,
}

/// Thresholds for one incremental-maintenance pass (SPFresh/LIRE-style local
/// split and merge that touches only the affected bubbles, not whole levels).
#[derive(Debug, Clone, PartialEq)]
pub struct IncrementalMaintenanceOptions {
    /// A segment is split when it holds more than this many vectors.
    pub max_segment_vectors: usize,
    /// Optional radius cap: a segment is also split when its bubble radius
    /// exceeds this, so a spread-out cluster becomes tighter bubbles.
    pub max_segment_radius: Option<f32>,
    /// A segment is merged into its nearest neighbour when its live vector count
    /// (after tombstones) falls below this, consolidating fragmentation left by
    /// deletes.
    pub min_segment_vectors: usize,
    /// Maximum number of local split/merge operations to apply in one pass, so
    /// each pass stays bounded and incremental.
    pub max_operations: usize,
}

impl Default for IncrementalMaintenanceOptions {
    fn default() -> Self {
        Self {
            max_segment_vectors: DEFAULT_COMPACTION_MAX_SEGMENTS.max(4096),
            max_segment_radius: None,
            min_segment_vectors: 64,
            max_operations: 8,
        }
    }
}

/// Result of one incremental-maintenance pass.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct IncrementalReport {
    /// Oversized segments split into tighter bubbles this pass.
    pub splits: usize,
    /// Sparse segments merged into a neighbour this pass.
    pub merges: usize,
    /// New segment objects written by the split/merge operations.
    pub segments_created: usize,
    /// Old segment objects removed from the active manifest.
    pub segments_removed: usize,
    /// Live records rewritten into new segments (tombstoned rows dropped).
    pub records_moved: usize,
    /// True when the pass changed the index (published a new version).
    pub published: bool,
    /// Object-store requests issued while applying the pass.
    #[serde(default)]
    pub requests: RequestCounts,
}

/// Objects and bytes written by an add operation.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AddReport {
    /// Immutable segment payload objects written.
    pub segments_written: usize,
    /// Derived segment-local graph payload objects written.
    pub graph_payloads_written: usize,
    /// Versioned manifest/routing/pivot and routing layer-index tables written.
    pub manifest_tables_written: usize,
    /// Content-addressed routing page objects written.
    pub routing_pages_written: usize,
    /// Total payload bytes written by the add publish, including the CURRENT pointer.
    pub total_bytes_written: u64,
    /// Total written bytes divided by the number of vectors accepted by this add.
    pub bytes_per_vector: f64,
    /// Object-store requests issued while publishing this add.
    #[serde(default)]
    pub requests: RequestCounts,
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
    /// Recall guarantee represented by this query execution.
    pub recall_guarantee: RecallGuarantee,
    /// Total number of segment summaries ranked by the router.
    pub segments_total: usize,
    /// Number of segment payloads fetched and searched.
    pub segments_searched: usize,
    /// Number of ranked segments skipped by exact pruning or approximate budgets.
    pub segments_skipped: usize,
    /// Routing page-index objects read while selecting query candidate leaves.
    pub routing_page_indexes_read: usize,
    /// Routing page content objects read while selecting and decoding query candidate leaves.
    pub routing_pages_read: usize,
    /// Routing page-index, routing-page, and segment payload bytes read during the query.
    pub bytes_read: u64,
    /// Segment payload bytes prefetched but not consumed because the query stopped early.
    pub prefetched_bytes_unused: u64,
    /// Segment-local graph bytes read during approximate local traversal.
    pub graph_bytes_read: u64,
    /// Segment or graph objects served from the local read-through cache.
    pub object_cache_hits: usize,
    /// Segment or graph objects fetched from storage instead of the local cache.
    pub object_cache_misses: usize,
    /// Cached objects that failed checksum verification and were repaired by refetching.
    #[serde(default)]
    pub cache_repairs: usize,
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
    /// Object-store requests issued while executing this query.
    #[serde(default)]
    pub requests: RequestCounts,
    /// Rows whose metadata was evaluated against the filter (0 when unfiltered).
    #[serde(default)]
    pub rows_evaluated: usize,
    /// Rows that passed the metadata filter and competed for top-k.
    #[serde(default)]
    pub rows_passed_filter: usize,
    /// Candidate segments skipped because their metadata stats could not match the filter.
    #[serde(default)]
    pub segments_pruned_by_filter: usize,
}

/// Recall guarantee represented by a search execution report.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RecallGuarantee {
    /// Exact mode returned true nearest neighbors under the index metric.
    Exact,
    /// Approximate mode covered every routed segment and scored every record candidate.
    BudgetComplete,
    /// Approximate mode used pruning, budgets, or local candidate truncation.
    Degraded,
}

impl RecallGuarantee {
    /// Canonical public API name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Exact => "exact",
            Self::BudgetComplete => "budget-complete",
            Self::Degraded => "degraded",
        }
    }
}

impl std::fmt::Display for RecallGuarantee {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
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
    /// Production, graph-free: exact/routing-code scan over selected segment
    /// records without reading graph blocks.
    FlatScan,
    /// Production, graph-free: scalar routing-code scan followed by exact rerank.
    SqScan,
    /// Production (recommended): product-quantized compressed scan path followed
    /// by exact rerank. Graph-free and lowest on memory.
    PqScan,
    /// Experimental: segment-local graph traversal followed by exact rerank.
    /// Reads extra graph objects; prefer `PqScan` for production.
    #[default]
    Graph,
    /// Experimental: PQ-seeded segment-local graph traversal followed by exact
    /// rerank. Reads extra graph objects; prefer `PqScan` for production.
    VamanaPq,
    /// Experimental: use each segment's stored leaf-mode metadata to choose its
    /// local search path. Reads graph objects for graph-backed segments.
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
        /// Routing metadata page overfetch multiplier for approximate search.
        ///
        /// This tunes cheap routing-page reads separately from expensive
        /// segment payload reads. The default is chosen by the search engine.
        #[serde(default)]
        routing_page_overfetch: Option<usize>,
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
    /// Require a guaranteed-recall approximate execution or return a typed error.
    #[serde(default)]
    pub guaranteed_recall: bool,
    /// Maximum number of segment payload reads scheduled concurrently.
    #[serde(default = "default_search_prefetch_depth")]
    pub prefetch_depth: usize,
    /// Optional metadata filter; only records matching it are eligible hits.
    #[serde(default)]
    pub filter: Option<crate::Filter>,
    /// Return each hit's metadata when true (default false).
    #[serde(default)]
    pub include_metadata: bool,
}

impl SearchOptions {
    /// Construct exact-search options.
    #[must_use]
    pub fn exact(k: usize) -> Self {
        Self {
            k,
            mode: SearchMode::Exact,
            guaranteed_recall: false,
            prefetch_depth: DEFAULT_SEARCH_PREFETCH_DEPTH,
            filter: None,
            include_metadata: false,
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
                routing_page_overfetch: None,
                max_candidates_per_segment: None,
            },
            guaranteed_recall: false,
            prefetch_depth: DEFAULT_SEARCH_PREFETCH_DEPTH,
            filter: None,
            include_metadata: false,
        }
    }

    /// Attach a metadata filter; only matching records are eligible hits.
    #[must_use]
    pub fn with_filter(mut self, filter: crate::Filter) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Return each hit's metadata in the results.
    #[must_use]
    pub fn with_include_metadata(mut self, include_metadata: bool) -> Self {
        self.include_metadata = include_metadata;
        self
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

    /// Set routing metadata page overfetch for approximate search.
    #[must_use]
    pub fn with_routing_page_overfetch(mut self, routing_page_overfetch: usize) -> Self {
        if let SearchMode::Approx {
            routing_page_overfetch: current_routing_page_overfetch,
            ..
        } = &mut self.mode
        {
            *current_routing_page_overfetch = Some(routing_page_overfetch);
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

    /// Require approximate search to avoid silent recall degradation.
    #[must_use]
    pub fn with_guaranteed_recall(mut self) -> Self {
        self.guaranteed_recall = true;
        self
    }

    /// Set the number of segment payload reads that search may prefetch.
    #[must_use]
    pub fn with_prefetch_depth(mut self, prefetch_depth: usize) -> Self {
        self.prefetch_depth = prefetch_depth;
        self
    }
}

impl Default for SearchOptions {
    fn default() -> Self {
        Self {
            k: 10,
            mode: SearchMode::Exact,
            guaranteed_recall: false,
            prefetch_depth: DEFAULT_SEARCH_PREFETCH_DEPTH,
            filter: None,
            include_metadata: false,
        }
    }
}

const fn default_search_prefetch_depth() -> usize {
    DEFAULT_SEARCH_PREFETCH_DEPTH
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
    /// Optional maximum bubble radius per compacted output segment. When set,
    /// compaction closes a segment early once its routing radius (max metric
    /// distance from the running centroid) would exceed this value, splitting a
    /// spread-out cluster into several tight, small-radius segments that prune
    /// far better than one large bubble. `None` keeps count-only chunking. Must
    /// be greater than zero when set.
    #[serde(default)]
    pub target_segment_max_radius: Option<f32>,
}

impl Default for CompactionOptions {
    fn default() -> Self {
        Self {
            source_level: 0,
            target_level: 1,
            max_segments: Some(DEFAULT_COMPACTION_MAX_SEGMENTS),
            min_segments: 2,
            target_segment_max_vectors: None,
            target_segment_max_radius: None,
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
    /// New compacted segment and derived graph payload bytes written.
    pub bytes_written: u64,
    /// Routing-page or source segment objects served from the local read-through cache.
    pub object_cache_hits: usize,
    /// Routing-page or source segment objects fetched from storage instead of the local cache.
    pub object_cache_misses: usize,
    /// Manifest version active after the compaction attempt.
    pub manifest_version: u64,
}

/// Default grace interval before garbage collection may reclaim unreferenced objects.
pub const DEFAULT_GARBAGE_COLLECTION_MIN_AGE: Duration = Duration::from_secs(24 * 60 * 60);

/// Options for garbage collecting inactive index objects.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GarbageCollectionOptions {
    /// When true, report obsolete objects without deleting them.
    pub dry_run: bool,
    /// Minimum object age required before an unreferenced object is a deletion candidate.
    pub min_age: Duration,
}

impl Default for GarbageCollectionOptions {
    fn default() -> Self {
        Self {
            dry_run: true,
            min_age: DEFAULT_GARBAGE_COLLECTION_MIN_AGE,
        }
    }
}

/// Result of scanning obsolete index objects.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GarbageCollectionReport {
    /// Whether this run only reported candidates.
    pub dry_run: bool,
    /// Number of GC-managed objects scanned.
    pub objects_scanned: usize,
    /// Number of obsolete objects deleted.
    pub objects_deleted: usize,
    /// Number of obsolete routing page/index objects deleted.
    pub routing_objects_deleted: usize,
    /// Number of obsolete manifest/routing/pivot table objects deleted.
    pub tables_deleted: usize,
    /// Routing page-index objects read while deriving active objects.
    pub routing_page_indexes_read: usize,
    /// Routing page content objects read while deriving active objects.
    pub routing_pages_read: usize,
    /// Routing page-index and routing-page bytes read while deriving active objects.
    pub bytes_read: u64,
    /// Bytes that could be reclaimed from the reported candidates.
    pub bytes_reclaimable: u64,
    /// Bytes actually reclaimed by deletion.
    pub bytes_reclaimed: u64,
    /// Routing objects served from the local read-through cache.
    pub object_cache_hits: usize,
    /// Routing objects fetched from storage instead of the local cache.
    pub object_cache_misses: usize,
    /// Obsolete object paths relative to the index root.
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
    /// Delete obsolete objects after publishing the rebuilt manifest.
    ///
    /// This cleanup uses `min_age = Duration::ZERO`; enabling it requires external quiescence
    /// with no concurrent readers or writers. For concurrent use, leave this disabled and run
    /// `gc_obsolete_segments` separately with an explicit retention interval.
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
