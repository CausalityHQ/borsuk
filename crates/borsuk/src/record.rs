use std::{collections::BTreeMap, fmt, str::FromStr, time::Duration};

use crate::{BorsukError, Result, VectorMetric};

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

/// Physical storage preference for a record's single dense vector.
///
/// This affects only segment size. Search, routing, metrics, PQ, centroids, and
/// public reads always operate on the reconstructed dense vector.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum StorageEncoding {
    /// Choose dense or sparse storage per record using BORSUK's size heuristic.
    #[default]
    Auto,
    /// Store the vector as a full fixed-width dense f32 list.
    Dense,
    /// Store only ascending non-zero coordinates and their values.
    Sparse,
}

impl StorageEncoding {
    pub(crate) fn resolve_for_vector(self, vector: &[f32]) -> Self {
        match self {
            Self::Auto if should_store_sparse(vector) => Self::Sparse,
            Self::Auto => Self::Dense,
            Self::Dense | Self::Sparse => self,
        }
    }
}

fn should_store_sparse(vector: &[f32]) -> bool {
    let nnz = vector.iter().filter(|value| **value != 0.0).count();
    nnz.saturating_mul(2) < vector.len()
}

/// Storage and retrieval backend for a named vector.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub enum VectorKind {
    /// Dense metric-tree backend (a child index), the default.
    #[default]
    Dense,
    /// Sparse inverted-index backend for high-dimensional sparse vectors:
    /// candidates are gathered by shared terms and scored with sparse dot,
    /// never densified.
    Sparse,
}

/// Declares the dimensions, distance metric, and backend for a named vector.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct VectorSpec {
    /// Required vector dimensionality for this named vector.
    pub dimensions: usize,
    /// Distance metric used by this named vector's sub-index.
    pub metric: VectorMetric,
    /// Whether this named vector is stored densely (metric tree) or sparsely
    /// (inverted index). Defaults to [`VectorKind::Dense`].
    #[serde(default)]
    pub kind: VectorKind,
}

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
    /// Additional named dense vector payloads keyed by declared vector name.
    #[serde(default)]
    pub extra_vectors: BTreeMap<String, Vec<f32>>,
    /// Additional named SPARSE vector payloads (kept in sparse form, never
    /// densified) keyed by declared sparse-kind vector name.
    #[serde(default)]
    pub extra_sparse: BTreeMap<String, crate::SparseVector>,
    /// Physical storage preference for this record's vector.
    #[serde(default)]
    pub storage: StorageEncoding,
    /// Optional text payload tokenized during add; raw text is not persisted in segments.
    #[serde(default)]
    pub text: Option<String>,
    /// Persisted text term ids sorted by term id.
    #[doc(hidden)]
    #[serde(default)]
    pub text_term_ids: Vec<u32>,
    /// Persisted text term frequencies corresponding one-for-one with [`VectorRecord::text_term_ids`].
    #[doc(hidden)]
    #[serde(default)]
    pub text_term_freqs: Vec<u32>,
    /// Optional typed metadata carried with the record (empty map = none).
    #[serde(default)]
    pub metadata: crate::Metadata,
    /// MVCC generation for versioned upserts. A record is a live version of its
    /// id only when its generation is at least the id's live generation in the
    /// tombstone overlay; older generations are suppressed by reads and dropped
    /// by compaction. Plain `add` uses generation `0`; each `upsert` of an id
    /// stamps a strictly higher generation. Never persisted for all-zero
    /// segments, so dense/plain data round-trips byte-for-byte.
    #[serde(default)]
    pub generation: u64,
}

impl VectorRecord {
    /// Construct a vector record with no metadata.
    pub fn new(id: impl Into<RecordId>, vector: Vec<f32>) -> Self {
        Self {
            id: id.into(),
            vector,
            extra_vectors: BTreeMap::new(),
            extra_sparse: BTreeMap::new(),
            storage: StorageEncoding::Auto,
            text: None,
            text_term_ids: Vec::new(),
            text_term_freqs: Vec::new(),
            metadata: crate::Metadata::new(),
            generation: 0,
        }
    }

    /// Construct a vector record from raw id bytes with no metadata.
    pub fn new_bytes(id: impl Into<Vec<u8>>, vector: Vec<f32>) -> Self {
        Self {
            id: RecordId::from_bytes(id),
            vector,
            extra_vectors: BTreeMap::new(),
            extra_sparse: BTreeMap::new(),
            storage: StorageEncoding::Auto,
            text: None,
            text_term_ids: Vec::new(),
            text_term_freqs: Vec::new(),
            metadata: crate::Metadata::new(),
            generation: 0,
        }
    }

    /// Construct a record from sparse coordinate input by immediately
    /// reconstructing the dense vector with zeros in unspecified dimensions.
    pub fn from_sparse(
        id: impl Into<RecordId>,
        indices: Vec<u32>,
        values: Vec<f32>,
        dimensions: usize,
    ) -> crate::Result<Self> {
        let vector = dense_vector_from_sparse(indices, values, dimensions)?;
        Ok(Self::new(id, vector))
    }

    /// Attach an additional named dense vector to this record.
    #[must_use]
    pub fn with_named_vector(mut self, name: impl Into<String>, vector: Vec<f32>) -> Self {
        self.extra_vectors.insert(name.into(), vector);
        self
    }

    /// Attach an additional named vector from sparse coordinate input.
    pub fn with_named_sparse(
        mut self,
        name: impl Into<String>,
        indices: Vec<u32>,
        values: Vec<f32>,
        dimensions: usize,
    ) -> crate::Result<Self> {
        let vector = dense_vector_from_sparse(indices, values, dimensions)?;
        self.extra_vectors.insert(name.into(), vector);
        Ok(self)
    }

    /// Attach an additional named vector to a SPARSE-kind named vector, kept in
    /// sparse form (never densified). Use this for high-dimensional sparse
    /// named vectors.
    pub fn with_named_sparse_vector(
        mut self,
        name: impl Into<String>,
        indices: Vec<u32>,
        values: Vec<f32>,
    ) -> crate::Result<Self> {
        let sparse = crate::SparseVector::new(indices, values)?;
        self.extra_sparse.insert(name.into(), sparse);
        Ok(self)
    }

    /// Set the physical storage preference for this record's vector.
    #[must_use]
    pub fn with_storage(mut self, storage: StorageEncoding) -> Self {
        self.storage = storage;
        self
    }

    /// Attach typed metadata to this record.
    #[must_use]
    pub fn with_metadata(mut self, metadata: crate::Metadata) -> Self {
        self.metadata = metadata;
        self
    }

    /// Attach text for tokenizer-based term-frequency storage during add.
    #[must_use]
    pub fn with_text(mut self, text: impl Into<String>) -> Self {
        self.text = Some(text.into());
        self
    }
}

fn dense_vector_from_sparse(
    indices: Vec<u32>,
    values: Vec<f32>,
    dimensions: usize,
) -> crate::Result<Vec<f32>> {
    let sparse = crate::SparseVector::new(indices, values)?;
    let mut vector = vec![0.0; dimensions];
    for (&index, &value) in sparse.indices().iter().zip(sparse.values()) {
        let position = usize::try_from(index).map_err(|_| {
            BorsukError::InvalidRecordInput(format!(
                "sparse vector index {index} does not fit usize"
            ))
        })?;
        if position >= dimensions {
            return Err(BorsukError::InvalidRecordInput(format!(
                "sparse vector index {index} is outside {dimensions} dimensions"
            )));
        }
        vector[position] = value;
    }
    Ok(vector)
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
    /// Whether this physical index stores optional per-record text term frequencies.
    pub text: bool,
    /// Declared named vector sub-indexes, sorted by name.
    #[serde(default)]
    pub named_vectors: Vec<String>,
    /// Number of active records physically encoded as sparse vectors.
    #[serde(default)]
    pub sparse_encoded_vectors: usize,
    /// Number of active records physically encoded as dense vectors.
    #[serde(default)]
    pub dense_encoded_vectors: usize,
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
    /// exceeds this, so a spread-out cluster becomes tighter bubbles. Cosine
    /// and angular indexes measure this radius as Euclidean distance between
    /// unit-L2-normalized vectors.
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

/// Object-storage pricing used to turn a query's request/byte counters into an
/// estimated dollar cost. Defaults to AWS S3 Standard (us-east-1) list prices.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct QueryCostModel {
    /// USD per 1,000,000 GET/HEAD requests. S3 Standard GET is $0.0004 / 1,000.
    pub request_price_per_million: f64,
    /// USD per GiB of payload read. Same-region reads to compute are typically
    /// free (`0.0`); set this to model cross-region or internet egress.
    pub data_price_per_gib: f64,
}

impl Default for QueryCostModel {
    fn default() -> Self {
        Self {
            request_price_per_million: 0.40,
            data_price_per_gib: 0.0,
        }
    }
}

impl QueryCostModel {
    /// Estimate the object-storage dollar cost of `requests` GET/HEADs reading
    /// `bytes_read` bytes under this model.
    #[must_use]
    pub fn estimate_usd(&self, requests: u64, bytes_read: u64) -> f64 {
        let request_cost = (requests as f64 / 1_000_000.0) * self.request_price_per_million;
        let data_cost = (bytes_read as f64 / (1024.0 * 1024.0 * 1024.0)) * self.data_price_per_gib;
        request_cost + data_cost
    }
}

/// A query's execution plan and estimated cost, derived from a measured
/// [`SearchReport`]. This is BORSUK's answer to "what did this query cost?" —
/// object-store requests, bytes read, cache effectiveness, routing pruning, and
/// a dollar estimate — which is opaque in RAM-first engines.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ExplainReport {
    /// The ranked hits (same as a normal search).
    pub hits: Vec<SearchHit>,
    /// Leaf engine used inside searched segments.
    pub leaf_mode: String,
    /// Segment summaries the router ranked.
    pub segments_total: usize,
    /// Segment payloads actually fetched and searched.
    pub segments_searched: usize,
    /// Ranked segments skipped by exact pruning or approximate budgets.
    pub segments_skipped: usize,
    /// Segments skipped entirely because their metadata statistics ruled out the filter.
    pub segments_pruned_by_filter: usize,
    /// GET + HEAD object-store requests issued (the S3-billable operations).
    pub get_requests: u64,
    /// Total payload/routing bytes read.
    pub bytes_read: u64,
    /// Decoded-segment cache hit ratio in `[0, 1]` (1.0 when nothing was cached-eligible).
    pub cache_hit_ratio: f64,
    /// Measured wall-clock latency of this execution in milliseconds.
    pub elapsed_ms: u64,
    /// Estimated object-storage dollar cost under the supplied [`QueryCostModel`].
    pub estimated_cost_usd: f64,
    /// The full underlying report, for callers that want every counter.
    pub report: SearchReport,
}

/// Recall guarantee represented by a search execution report.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RecallGuarantee {
    /// Exact mode returned true nearest neighbors under the index metric.
    Exact,
    /// Fusion or approximate execution returned a ranked result that is not an
    /// exact top-k set under one single metric.
    Approximate,
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
            Self::Approximate => "approximate",
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
    /// Adaptive early-stop: the top-k did not improve for `patience` consecutive
    /// segments, so the remaining candidates were skipped.
    AdaptiveStop,
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
            Self::AdaptiveStop => "adaptive-stop",
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

    /// Whether this leaf mode may read per-segment graph objects.
    ///
    /// `Graph`/`VamanaPq` always traverse a segment graph. `Hybrid` reads a
    /// graph for any segment whose stored leaf mode is graph-backed, so it also
    /// requires the graph to have been built at index creation. The scan modes
    /// (`FlatScan`/`SqScan`/`PqScan`) never touch a graph.
    #[must_use]
    pub fn requires_graph(self) -> bool {
        matches!(self, Self::Graph | Self::VamanaPq | Self::Hybrid)
    }
}

/// Leaf-search capability fixed at index creation and persisted in the manifest.
///
/// A graph is an extra per-segment object that only graph-backed leaf modes
/// (`Graph`/`VamanaPq`/`Hybrid`) read. Declaring [`LeafCapability::PqScanOnly`]
/// at creation skips that build work entirely and makes a graph-mode search a
/// typed error rather than a silent degrade. The default, [`LeafCapability::GraphEnabled`],
/// preserves the historical behavior: every segment builds a graph and any leaf
/// mode is searchable.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "kebab-case")]
pub enum LeafCapability {
    /// Build per-segment graphs and allow every leaf mode at search time.
    #[default]
    GraphEnabled,
    /// Skip per-segment graph construction; only graph-free scan leaf modes
    /// (`FlatScan`/`SqScan`/`PqScan`) may be searched.
    PqScanOnly,
}

impl LeafCapability {
    /// Whether this capability builds per-segment graph objects at write time.
    #[must_use]
    pub fn builds_graph(self) -> bool {
        matches!(self, Self::GraphEnabled)
    }

    /// Whether a search using `leaf_mode` is permitted for this capability.
    #[must_use]
    pub fn allows_leaf_mode(self, leaf_mode: LeafMode) -> bool {
        match self {
            Self::GraphEnabled => true,
            Self::PqScanOnly => !leaf_mode.requires_graph(),
        }
    }

    /// Stable machine-readable name used in errors and manifest persistence.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::GraphEnabled => "graph-enabled",
            Self::PqScanOnly => "pq-scan-only",
        }
    }
}

impl FromStr for LeafCapability {
    type Err = BorsukError;

    fn from_str(value: &str) -> Result<Self> {
        let normalized = value.trim().to_ascii_lowercase().replace('_', "-");
        match normalized.as_str() {
            "graph-enabled" | "graph" | "graphenabled" => Ok(Self::GraphEnabled),
            "pq-scan-only" | "pqscanonly" | "pq-only" | "pqscan-only" | "scan-only" => {
                Ok(Self::PqScanOnly)
            }
            _ => Err(BorsukError::InvalidStorage(format!(
                "unknown leaf capability `{value}`"
            ))),
        }
    }
}

impl fmt::Display for LeafCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Default zstd compression level for the dense-vector sidecar. Level 3 is
/// zstd's default and the historical value the sidecar shipped with — a good
/// speed/ratio trade-off on the tiny per-row payloads.
pub const DEFAULT_SIDECAR_ZSTD_LEVEL: i32 = 3;

/// How the per-segment dense-vector sidecar stores its rows.
///
/// The sidecar is the reranker's random-access store of full-precision vectors.
/// It is always **lossless** — a decoded row is the byte-identical `f32` values
/// that went in — so the compression choice trades build speed for storage
/// footprint without ever touching recall (rerank stays exact).
///
/// [`SidecarCompression::Zstd`] (the default) shares one trained dictionary
/// across independently-compressed rows; it is the smallest on disk and the
/// slowest to build. [`SidecarCompression::Uncompressed`] writes each row's raw
/// little-endian `f32` bytes with the SAME offset-table/footer layout, so it is
/// the fastest build (no per-row zstd, no dictionary training) at the cost of
/// the largest footprint. The reader auto-detects the mode from the sidecar
/// footer, so both are drop-in for every read path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case", tag = "kind")]
pub enum SidecarCompression {
    /// Per-row zstd with a shared trained dictionary at the given level.
    Zstd {
        /// zstd compression level applied to each row.
        level: i32,
    },
    /// Store each row's raw little-endian `f32` bytes uncompressed — fastest to
    /// build, largest on disk. Still lossless and still random-access.
    Uncompressed,
}

impl Default for SidecarCompression {
    fn default() -> Self {
        Self::Zstd {
            level: DEFAULT_SIDECAR_ZSTD_LEVEL,
        }
    }
}

impl SidecarCompression {
    /// Whether this mode compresses rows (vs. storing them raw).
    #[must_use]
    pub fn is_compressed(self) -> bool {
        matches!(self, Self::Zstd { .. })
    }
}

/// Default seed for the [`QuantizerKind::TurboQuant`] structured rotation. Fixed
/// so a default TurboQuant config is fully reproducible; overridable per index.
pub const DEFAULT_TURBOQUANT_SEED: u64 = 0x0B05_11C0_7A17_C0DE;

/// Which coarse-scoring quantizer builds the per-segment candidate codes and
/// scores candidates before the exact rerank.
///
/// The coarse codes only decide *candidate ordering*; the exact rerank from the
/// lossless sidecar restores the true distances, so this knob never touches
/// end-to-end correctness — only how good the coarse shortlist is at a given
/// budget. Persisted on the manifest [`BuildConfig`], fixed at index creation.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case", tag = "kind")]
pub enum QuantizerKind {
    /// The historical, default coarse quantizer: per-raw-dimension min/max
    /// scalar quantization, scored symmetrically. Byte-identical to pre-existing
    /// indexes.
    ScalarBounds,
    /// A TurboQuant/RabitQ-style quantizer: apply a seeded structured randomized
    /// rotation (SRHT: `H D`, `O(d log d)`) so rotated coordinates are
    /// near-independent, then per-coordinate scalar quantization on the rotated
    /// vector, scored asymmetrically (rotate the query, dequantize-and-dot). The
    /// rotation `seed` is persisted so queries rotate identically.
    TurboQuant {
        /// Seed for the structured rotation. Persisted so the query rotates the
        /// same way the database vectors did at build time.
        #[serde(default = "default_turboquant_seed")]
        seed: u64,
        /// Bits per rotated coordinate (clamped to `1..=8`; default 4, the
        /// paper's ANN setting).
        #[serde(default = "default_turboquant_bits")]
        bits: u8,
    },
}

impl Default for QuantizerKind {
    fn default() -> Self {
        // A/B validated (tests/turboquant_ab.rs): TurboQuant gives strictly
        // higher recall@10 at every tight coarse-candidate budget while storing
        // HALF the coarse bytes/vector (4 bits/rotated-coord vs 8 bits/raw-dim),
        // so it is the default. `ScalarBounds` stays selectable.
        Self::TurboQuant {
            seed: DEFAULT_TURBOQUANT_SEED,
            bits: crate::turboquant::DEFAULT_TURBOQUANT_BITS,
        }
    }
}

impl QuantizerKind {
    /// The quantizer actually applied for a given `dimensions`.
    ///
    /// [`QuantizerKind::TurboQuant`] stores its rotated codes in the segment's
    /// fixed-width (`dimensions`) code column, which only fits when the SRHT
    /// padding is a no-op, i.e. `dimensions` is already a power of two. For a
    /// non-power-of-two dimensionality this cut transparently falls back to
    /// [`QuantizerKind::ScalarBounds`] (padded-storage for non-pow2 dims is a
    /// documented follow-up). Both the build and the query side call this so they
    /// always agree on what a segment's codes mean.
    #[must_use]
    pub fn effective_for_dimensions(self, dimensions: usize) -> QuantizerKind {
        match self {
            QuantizerKind::TurboQuant { .. }
                if dimensions.max(1).next_power_of_two() != dimensions =>
            {
                QuantizerKind::ScalarBounds
            }
            other => other,
        }
    }
}

/// serde default for [`QuantizerKind::TurboQuant::seed`].
fn default_turboquant_seed() -> u64 {
    DEFAULT_TURBOQUANT_SEED
}

/// serde default for [`QuantizerKind::TurboQuant::bits`].
fn default_turboquant_bits() -> u8 {
    crate::turboquant::DEFAULT_TURBOQUANT_BITS
}

/// Typed, persisted knobs that trade index BUILD speed against storage footprint
/// and clustering cost. Stored on the manifest (checksum-covered) and fixed at
/// index creation; [`BuildConfig::default`] reproduces the historical behavior
/// exactly, so an absent config on an older manifest and a defaulted config
/// build byte-identical indexes.
///
/// None of these knobs affect recall on the exact-rerank path: the sidecar stays
/// lossless regardless of compression, and centroid sampling only perturbs which
/// segment a vector lands in (rerank still re-scores the true vectors).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct BuildConfig {
    /// How the per-segment dense-vector sidecar stores its rows. The sidecar is
    /// now the largest build phase, so this is the headline knob:
    /// [`SidecarCompression::Uncompressed`] skips per-row zstd entirely for the
    /// fastest build.
    #[serde(default)]
    pub sidecar_compression: SidecarCompression,
    /// Fraction of points used to FIT the Voronoi/k-means centroids, in `(0, 1]`.
    /// `1.0` (default) fits on every point. Below `1.0` the centroids are fit on
    /// a deterministic uniform subsample and then ALL points are assigned — a
    /// large clustering speedup for a tiny quality cost (rerank protects recall).
    #[serde(default = "default_kmeans_sample_fraction")]
    pub kmeans_sample_fraction: f32,
    /// Optional cap on Lloyd iterations per clustering level. `None` (default)
    /// keeps the built-in iteration cap; a smaller value trades cell quality for
    /// build speed.
    #[serde(default)]
    pub kmeans_max_iterations: Option<usize>,
    /// Optional cap on the number of vectors used to train the PQ codebook.
    /// `None` (default) uses every vector. Reserved for a trained-codebook build
    /// phase; the current scalar-quantization PQ derives per-dimension bounds
    /// from all rows, so this is carried through the manifest for forward
    /// compatibility without changing today's output.
    #[serde(default)]
    pub pq_codebook_sample: Option<usize>,
    /// Which coarse-scoring quantizer builds and scores the per-segment
    /// candidate codes. [`QuantizerKind::ScalarBounds`] (default) reproduces the
    /// historical behavior byte-identically; [`QuantizerKind::TurboQuant`] builds
    /// and scores via the rotated-coordinate path.
    #[serde(default)]
    pub quantizer: QuantizerKind,
}

/// serde default for [`BuildConfig::kmeans_sample_fraction`]: cluster on all
/// points (the historical behavior).
fn default_kmeans_sample_fraction() -> f32 {
    1.0
}

impl Default for BuildConfig {
    fn default() -> Self {
        Self {
            sidecar_compression: SidecarCompression::default(),
            kmeans_sample_fraction: default_kmeans_sample_fraction(),
            kmeans_max_iterations: None,
            pq_codebook_sample: None,
            quantizer: QuantizerKind::default(),
        }
    }
}

impl BuildConfig {
    /// The effective k-means sample fraction, clamped to `(0, 1]`. A
    /// non-finite or non-positive configured value falls back to `1.0` (fit on
    /// all points) rather than producing an empty training set.
    #[must_use]
    pub fn effective_kmeans_sample_fraction(&self) -> f32 {
        if self.kmeans_sample_fraction.is_finite() && self.kmeans_sample_fraction > 0.0 {
            self.kmeans_sample_fraction.min(1.0)
        } else {
            1.0
        }
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
        /// Adaptive early-stop: stop fetching segments once the running top-k has
        /// not improved for this many consecutive segments (query-adaptive
        /// `nprobe` — easy queries stop early, hard ones read on). `None` reads
        /// the full `max_segments` budget. See [`SearchOptions::with_adaptive_stop`].
        #[serde(default)]
        adaptive_stop: Option<usize>,
        /// Projected (PQ/SQ) reads: score from the compact code column and fetch
        /// full vectors only for the rerank set — 4–8× fewer bytes on a cold
        /// read. Applies to `PqScan`/`SqScan` leaf modes with a candidate budget.
        /// `None` uses the engine default (on when applicable); `Some(false)`
        /// forces full-vector reads. See [`SearchOptions::with_projected_reads`].
        #[serde(default)]
        projected_reads: Option<bool>,
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
    /// Named vector sub-index to search; empty string selects the primary vector.
    #[serde(default)]
    pub vector_name: String,
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
            vector_name: String::new(),
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
                adaptive_stop: None,
                projected_reads: None,
            },
            guaranteed_recall: false,
            prefetch_depth: DEFAULT_SEARCH_PREFETCH_DEPTH,
            filter: None,
            include_metadata: false,
            vector_name: String::new(),
        }
    }

    /// Set the number of nearest hits to return.
    #[must_use]
    pub fn with_k(mut self, k: usize) -> Self {
        self.k = k;
        self
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

    /// Search a declared named vector sub-index instead of the primary vector.
    #[must_use]
    pub fn with_vector_name(mut self, name: impl Into<String>) -> Self {
        self.vector_name = name.into();
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

    /// Enable adaptive early-stop: stop fetching segments once the running top-k
    /// has not improved for `patience` consecutive segments. This makes `nprobe`
    /// query-adaptive — easy queries stop early, hard ones read on up to
    /// `max_segments` — cutting average reads at matched recall. No effect on
    /// exact search.
    #[must_use]
    pub fn with_adaptive_stop(mut self, patience: usize) -> Self {
        if let SearchMode::Approx {
            adaptive_stop: current_adaptive_stop,
            ..
        } = &mut self.mode
        {
            *current_adaptive_stop = Some(patience);
        }
        self
    }

    /// Force projected (PQ/SQ) reads on or off. When on (and the leaf mode is
    /// `PqScan`/`SqScan` with a candidate budget), a cold search scores from the
    /// compact code column and fetches full vectors only for the rerank set —
    /// 4–8× fewer bytes. `true` forces it, `false` forces full-vector reads;
    /// leaving it unset uses the engine default.
    #[must_use]
    pub fn with_projected_reads(mut self, enabled: bool) -> Self {
        if let SearchMode::Approx {
            projected_reads: current_projected_reads,
            ..
        } = &mut self.mode
        {
            *current_projected_reads = Some(enabled);
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
            vector_name: String::new(),
        }
    }
}

/// A hybrid query: any combination of vector and text retrieval.
#[derive(Debug, Clone, Default)]
pub struct HybridQuery {
    /// Query vectors keyed by vector name (`""` = the primary vector).
    pub vectors: BTreeMap<String, Vec<f32>>,
    /// Sparse query vectors for `VectorKind::Sparse` named vectors, keyed by
    /// vector name. Each value is `(indices, values)` and is scored against the
    /// inverted-index backend without densifying.
    pub sparse_vectors: BTreeMap<String, (Vec<u32>, Vec<f32>)>,
    /// Text query for the BM25 leg.
    pub text: Option<String>,
}

impl HybridQuery {
    /// Construct an empty hybrid query.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Attach a vector query for the primary vector or a named vector.
    #[must_use]
    pub fn with_vector(mut self, name: impl Into<String>, query: Vec<f32>) -> Self {
        self.vectors.insert(name.into(), query);
        self
    }

    /// Attach a vector query from sparse coordinate input.
    pub fn with_sparse_vector(
        mut self,
        name: impl Into<String>,
        indices: Vec<u32>,
        values: Vec<f32>,
        dimensions: usize,
    ) -> Result<Self> {
        let vector = dense_vector_from_sparse(indices, values, dimensions)?;
        self.vectors.insert(name.into(), vector);
        Ok(self)
    }

    /// Attach a sparse query for a `VectorKind::Sparse` named vector, kept in
    /// sparse form and scored against its inverted index (never densified).
    #[must_use]
    pub fn with_named_sparse_query(
        mut self,
        name: impl Into<String>,
        indices: Vec<u32>,
        values: Vec<f32>,
    ) -> Self {
        self.sparse_vectors.insert(name.into(), (indices, values));
        self
    }

    /// Attach a text query.
    #[must_use]
    pub fn with_text(mut self, text: impl Into<String>) -> Self {
        self.text = Some(text.into());
        self
    }
}

/// How to combine per-modality ranked lists into one hybrid result set.
#[derive(Debug, Clone)]
pub enum Fusion {
    /// Reciprocal Rank Fusion, a tuning-free rank-based combiner.
    Rrf {
        /// Rank constant added to each zero-based rank before reciprocation.
        k: usize,
    },
    /// Weighted sum of per-modality scores normalized to `[0, 1]` by each
    /// modality's best score in the candidate set.
    Weighted {
        /// Per-modality weights keyed by vector name or `@text`; absent modalities default to 1.0.
        weights: BTreeMap<String, f32>,
    },
}

impl Default for Fusion {
    fn default() -> Self {
        Self::Rrf { k: 60 }
    }
}

/// Options controlling hybrid search and vector/text fusion.
#[derive(Debug, Clone)]
pub struct HybridOptions {
    /// Number of fused hits to return.
    pub k: usize,
    /// Fusion strategy used to combine per-modality ranked lists.
    pub fusion: Fusion,
    /// Candidate depth pulled from each present modality before fusion.
    ///
    /// The effective search depth is at least [`HybridOptions::k`].
    pub candidate_depth: usize,
    /// Search options used for every vector leg.
    pub dense_options: SearchOptions,
}

impl HybridOptions {
    /// Construct hybrid-search options for `k` fused hits.
    ///
    /// Defaults to Reciprocal Rank Fusion with `k = 60`, candidate depth
    /// `max(k, 100)`, and approximate vector search using [`LeafMode::PqScan`].
    #[must_use]
    pub fn new(k: usize) -> Self {
        let candidate_depth = k.max(100);
        Self {
            k,
            fusion: Fusion::default(),
            candidate_depth,
            dense_options: SearchOptions::approx(candidate_depth, LeafMode::PqScan),
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
    /// compaction closes a segment early once its routing radius would exceed
    /// this value, splitting a spread-out cluster into several tight,
    /// small-radius segments that prune far better than one large bubble. For
    /// cosine and angular indexes the radius is Euclidean distance between
    /// unit-L2-normalized vectors; other metrics keep their metric distance.
    /// `None` keeps count-only chunking. Must be greater than zero when set.
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
