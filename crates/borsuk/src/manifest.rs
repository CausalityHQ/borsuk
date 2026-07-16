use std::mem::size_of;

use chrono::{DateTime, Utc};

use crate::{
    error::{BorsukError, Result},
    index::IndexConfig,
    metric::{VectorMetric, unit_l2_normalized},
    record::{LeafCapability, LeafMode},
    segment::vector_signature,
};

pub(crate) const TABLE_EXTENSION: &str = "parquet";
pub(crate) const SEGMENT_ID_BLOOM_BYTES: usize = 128;
pub(crate) const SEGMENT_VECTOR_SIGNATURE_BLOOM_BYTES: usize = 256;
/// Default number of routing page refs grouped into each routing parent page.
pub const DEFAULT_ROUTING_PAGE_FANOUT: usize = 128;
/// Default maximum segment-local graph neighbors per source record.
pub const DEFAULT_GRAPH_NEIGHBORS: usize = 8;
const SEGMENT_ID_BLOOM_HASHES: usize = 4;
const SEGMENT_VECTOR_SIGNATURE_BLOOM_HASHES: usize = 4;

/// Write-ahead-log configuration for an index. Enabled by default: `add`/`upsert`
/// batches are appended to an immutable WAL object and the frontier is published
/// in the same atomic manifest swap — cutting per-`add` latency by skipping the
/// PQ/graph/segment build entirely. All of BORSUK's consistency guarantees are
/// preserved because WAL objects are durable and tracked in the atomically-published
/// manifest frontier, and reads union the WAL tail.
///
/// The flush thresholds are a **memory-safety cap**, not an eager build trigger:
/// the un-flushed tail is materialized into segments by the SINGLE build that
/// [`crate::BorsukIndex::compact`] (or an explicit [`crate::BorsukIndex::flush`])
/// runs — compaction consumes the tail records directly, so the expensive
/// per-record encode (Parquet, dense sidecar, graph, PQ, cell clustering) happens
/// exactly once between ingest and the first compaction rather than twice. Only a
/// long streaming workload that accumulates past the cap WITHOUT ever compacting
/// spills an intermediate L0 segment early, to bound the resident tail (and the
/// per-query brute-force tail scan). Disable the WAL explicitly for the classic
/// synchronous segment-per-`add` behavior.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WalConfig {
    /// Whether the write-ahead log is active for this index.
    pub enabled: bool,
    /// Flush the accumulated WAL tail into a real segment once the number of
    /// un-flushed records reaches this many.
    pub flush_threshold_records: usize,
    /// Flush the accumulated WAL tail into a real segment once its un-flushed
    /// byte size reaches this many bytes.
    pub flush_threshold_bytes: u64,
}

/// Default WAL flush memory-safety cap in records. Large by design: the tail is
/// normally materialized by `compact()`/`flush()` (which build directly from the
/// tail records — no intermediate L0), so this only spills an early L0 segment
/// when a long streaming workload accumulates this many un-flushed records
/// WITHOUT compacting, bounding both resident memory and the per-query
/// brute-force tail scan.
pub const DEFAULT_WAL_FLUSH_THRESHOLD_RECORDS: usize = 250_000;
/// Default WAL flush memory-safety cap in bytes (512 MiB). See
/// [`DEFAULT_WAL_FLUSH_THRESHOLD_RECORDS`].
pub const DEFAULT_WAL_FLUSH_THRESHOLD_BYTES: u64 = 512 * 1024 * 1024;

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            flush_threshold_records: DEFAULT_WAL_FLUSH_THRESHOLD_RECORDS,
            flush_threshold_bytes: DEFAULT_WAL_FLUSH_THRESHOLD_BYTES,
        }
    }
}

impl WalConfig {
    /// An enabled WAL with the default flush thresholds (equivalent to
    /// [`WalConfig::default`]).
    pub fn enabled() -> Self {
        Self::default()
    }

    /// A disabled WAL: the classic synchronous segment-per-`add` write path.
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Self::default()
        }
    }
}

/// Reference to one immutable, published, un-flushed WAL object. Each entry is
/// a single object-store PUT of raw records; the ordered list of entries is the
/// index's un-flushed WAL tail. Entries are content-checksummed and named by
/// `(manifest_version, seq)` so no two writers collide.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct WalObjectRef {
    /// WAL object path relative to the index root.
    pub path: String,
    /// BLAKE3 checksum of the WAL object bytes.
    pub checksum: String,
    /// Number of records serialized into the WAL object.
    pub record_count: usize,
    /// Serialized WAL object size in bytes.
    pub byte_len: u64,
    /// Time the WAL object was written.
    pub created_at: DateTime<Utc>,
}

impl WalObjectRef {
    pub(crate) fn resident_bytes_estimate(&self) -> usize {
        size_of::<Self>() + self.path.len() + self.checksum.len()
    }
}

/// Published index metadata kept in memory while an index is open.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Manifest {
    /// Monotonic manifest version.
    pub version: u64,
    /// Index creation and search configuration.
    pub config: IndexConfig,
    /// Fingerprint of the tokenizer used for persisted text terms, when known.
    pub text_tokenizer: Option<String>,
    /// Immutable segment summaries used for routing and lower-bound pruning.
    pub segments: Vec<SegmentSummary>,
    /// Global pivot/router rows kept resident with segment summaries.
    pub pivots: Vec<PivotSummary>,
    /// Next numeric id reserved for generated-id add paths.
    pub next_generated_id: u64,
    /// Highest persisted routing layer for this manifest version.
    pub(crate) routing_max_level: u8,
    /// Number of routing page refs grouped into each routing parent page.
    pub(crate) routing_page_fanout: usize,
    /// Maximum number of segment-local graph neighbors written per source record.
    pub(crate) graph_neighbors: usize,
    /// Leaf-search capability fixed at index creation. `GraphEnabled` (the
    /// default and historical behavior) builds a per-segment graph on every
    /// write and allows any leaf mode; `PqScanOnly` skips graph construction and
    /// rejects graph-backed leaf modes at search time.
    #[serde(default)]
    pub(crate) leaf_capability: LeafCapability,
    /// Cumulative tombstone summary listing every currently-deleted record id, or
    /// `None` when nothing is deleted.
    pub(crate) tombstone: Option<TombstoneSummary>,
    /// Write-ahead-log configuration fixed at index creation. Defaults to a
    /// disabled WAL, in which case the frontier is always empty.
    #[serde(default)]
    pub(crate) wal_config: WalConfig,
    /// Ordered list of published, un-flushed WAL objects making up the WAL tail.
    /// Empty when the WAL is disabled or fully flushed. Part of the atomically
    /// published manifest state, so a reader's snapshot pins its own frontier.
    #[serde(default)]
    pub(crate) wal_frontier: Vec<WalObjectRef>,
    /// Next monotonic WAL object sequence number for this index. Kept in the
    /// manifest so flushed-then-reused `(version, seq)` names never collide.
    #[serde(default)]
    pub(crate) wal_next_seq: u64,
    /// Manifest creation time.
    pub created_at: DateTime<Utc>,
}

/// Summary of the single cumulative tombstone object that lists every
/// currently-deleted record id. The id bloom stays resident so search hits and
/// id lookups get a zero-fetch "is this id maybe deleted?" check; the full id
/// list is fetched only on a bloom hit.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct TombstoneSummary {
    /// Content-addressed path of the tombstone id-list Parquet object.
    pub path: String,
    /// BLAKE3 checksum of the tombstone object bytes.
    pub checksum: String,
    /// Number of deleted record ids in the tombstone object.
    pub count: u64,
    /// Bloom filter over the deleted record ids.
    pub id_bloom: Vec<u8>,
    /// Time the tombstone object was written.
    pub created_at: DateTime<Utc>,
}

impl TombstoneSummary {
    /// Bloom fast-path: `false` means the id is definitely not deleted.
    pub(crate) fn might_contain_record_id(&self, id: impl AsRef<[u8]>) -> bool {
        if self.id_bloom.len() != SEGMENT_ID_BLOOM_BYTES {
            return true;
        }
        bloom_contains(&self.id_bloom, id)
    }

    pub(crate) fn resident_bytes_estimate(&self) -> usize {
        size_of::<Self>() + self.path.len() + self.checksum.len() + self.id_bloom.len()
    }
}

impl Manifest {
    pub(crate) fn new_with_routing_page_fanout(
        config: IndexConfig,
        routing_page_fanout: usize,
        graph_neighbors: usize,
        leaf_capability: LeafCapability,
    ) -> Self {
        Self {
            version: 1,
            config,
            text_tokenizer: None,
            segments: Vec::new(),
            pivots: Vec::new(),
            next_generated_id: 0,
            routing_max_level: 0,
            routing_page_fanout,
            graph_neighbors,
            leaf_capability,
            tombstone: None,
            wal_config: WalConfig::default(),
            wal_frontier: Vec::new(),
            wal_next_seq: 0,
            created_at: Utc::now(),
        }
    }

    pub(crate) fn next_version(&self) -> Self {
        Self {
            version: self.version + 1,
            config: self.config.clone(),
            text_tokenizer: self.text_tokenizer.clone(),
            segments: self.segments.clone(),
            pivots: self.pivots.clone(),
            next_generated_id: self.next_generated_id,
            routing_max_level: self.routing_max_level,
            routing_page_fanout: self.routing_page_fanout,
            graph_neighbors: self.graph_neighbors,
            leaf_capability: self.leaf_capability,
            tombstone: self.tombstone.clone(),
            wal_config: self.wal_config.clone(),
            wal_frontier: self.wal_frontier.clone(),
            wal_next_seq: self.wal_next_seq,
            created_at: Utc::now(),
        }
    }

    pub(crate) fn set_routing_max_level_for_leaf_pages(
        &mut self,
        leaf_page_count: usize,
    ) -> Result<()> {
        let mut page_count = leaf_page_count;
        let mut routing_level = 0_u8;
        while page_count > 1 {
            page_count = page_count.div_ceil(self.routing_page_fanout);
            routing_level = routing_level.checked_add(1).ok_or_else(|| {
                BorsukError::InvalidStorage("routing layer depth exceeds u8".to_string())
            })?;
        }
        self.routing_max_level = routing_level;
        Ok(())
    }

    pub(crate) fn rebuild_pivots(&mut self) {
        self.pivots = self
            .segments
            .iter()
            .enumerate()
            .map(|(ordinal, segment)| PivotSummary {
                id: segment.id.clone(),
                ordinal,
                vector: segment.centroid.clone(),
            })
            .collect();
    }

    /// Whether the write-ahead log is enabled for this index.
    #[must_use]
    pub fn wal_enabled(&self) -> bool {
        self.wal_config.enabled
    }

    /// Whether the published, un-flushed WAL frontier is empty (no un-flushed
    /// tail objects). Reads short-circuit the WAL union when this is true.
    #[must_use]
    pub fn wal_frontier_is_empty(&self) -> bool {
        self.wal_frontier.is_empty()
    }

    /// Number of published, un-flushed WAL objects in the frontier.
    #[must_use]
    pub fn wal_frontier_len(&self) -> usize {
        self.wal_frontier.len()
    }

    pub(crate) fn file_name(&self) -> String {
        format!("manifests/manifest-{:020}.{TABLE_EXTENSION}", self.version)
    }

    pub(crate) fn file_name_for_version(version: u64) -> String {
        format!("manifests/manifest-{version:020}.{TABLE_EXTENSION}")
    }

    pub(crate) fn routing_file_name(&self) -> String {
        Self::routing_file_name_for_version(self.version)
    }

    pub(crate) fn routing_file_name_for_version(version: u64) -> String {
        format!("routing/segments-{version:020}.{TABLE_EXTENSION}")
    }

    pub(crate) fn pivots_file_name(&self) -> String {
        Self::pivots_file_name_for_version(self.version)
    }

    pub(crate) fn pivots_file_name_for_version(version: u64) -> String {
        format!("routing/pivots-{version:020}.{TABLE_EXTENSION}")
    }

    pub(crate) fn routing_layer_page_index_file_name(version: u64, routing_level: u8) -> String {
        format!("routing/layers/{version:020}/L{routing_level}/pages.{TABLE_EXTENSION}")
    }

    pub(crate) fn routing_layer_page_content_file_name(
        routing_level: u8,
        checksum: &str,
    ) -> String {
        let prefix = &checksum[..2];
        format!("routing/pages/L{routing_level}/{prefix}/page-{checksum}.{TABLE_EXTENSION}")
    }

    /// Content-addressed path of a cumulative tombstone id-list object.
    pub(crate) fn tombstone_content_file_name(checksum: &str) -> String {
        let prefix = &checksum[..2];
        format!("tombstones/{prefix}/tomb-{checksum}.{TABLE_EXTENSION}")
    }

    pub(crate) fn resident_bytes_estimate(&self) -> u64 {
        let config_bytes = size_of::<IndexConfig>() + self.config.uri.len();
        let text_tokenizer_bytes = self
            .text_tokenizer
            .as_ref()
            .map(String::len)
            .unwrap_or_default();
        let segments_bytes = self
            .segments
            .iter()
            .map(SegmentSummary::resident_bytes_estimate)
            .sum::<usize>();
        let pivots_bytes = self
            .pivots
            .iter()
            .map(PivotSummary::resident_bytes_estimate)
            .sum::<usize>();
        let tombstone_bytes = self
            .tombstone
            .as_ref()
            .map(TombstoneSummary::resident_bytes_estimate)
            .unwrap_or(0);
        let wal_frontier_bytes = self
            .wal_frontier
            .iter()
            .map(WalObjectRef::resident_bytes_estimate)
            .sum::<usize>();
        (size_of::<Self>()
            + config_bytes
            + text_tokenizer_bytes
            + segments_bytes
            + pivots_bytes
            + tombstone_bytes
            + wal_frontier_bytes) as u64
    }
}

/// Reference from a versioned routing layer to an immutable routing page object.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RoutingLayerPageRef {
    pub routing_level: u8,
    pub page_ordinal: usize,
    pub path: String,
    pub checksum: String,
    pub page_segments: usize,
    pub leaf_segments: usize,
    pub leaf_pages: usize,
    pub routing_pages: usize,
    pub dimensions: usize,
    pub centroid: Vec<f32>,
    pub radius: f32,
    pub bounds_min: Vec<f32>,
    pub bounds_max: Vec<f32>,
    pub id_bloom: Vec<u8>,
    pub vector_signature_bloom: Vec<u8>,
    pub level_mask: u64,
    pub page_records: usize,
    pub page_segment_bytes: u64,
    pub page_graph_bytes: u64,
    pub page_sparse_encoded_vectors: usize,
    pub page_dense_encoded_vectors: usize,
}

impl RoutingLayerPageRef {
    pub(crate) fn might_contain_record_id(&self, id: impl AsRef<[u8]>) -> bool {
        if self.id_bloom.len() != SEGMENT_ID_BLOOM_BYTES {
            return true;
        }

        bloom_contains(&self.id_bloom, id)
    }

    pub(crate) fn might_contain_vector_signature(&self, signature: u64) -> bool {
        if self.vector_signature_bloom.len() != SEGMENT_VECTOR_SIGNATURE_BLOOM_BYTES {
            return true;
        }

        vector_signature_bloom_contains(&self.vector_signature_bloom, signature)
    }

    pub(crate) fn might_contain_level(&self, level: u8) -> bool {
        if self.level_mask == u64::MAX || level >= u64::BITS as u8 {
            return true;
        }

        self.level_mask & (1_u64 << level) != 0
    }
}

/// Global pivot/router row kept in memory for segment-level routing.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PivotSummary {
    /// Stable pivot identifier. Current pivots are derived from segment centroids.
    pub id: String,
    /// Pivot order inside the published routing table.
    pub ordinal: usize,
    /// Pivot vector used by router implementations.
    pub vector: Vec<f32>,
}

impl PivotSummary {
    pub(crate) fn resident_bytes_estimate(&self) -> usize {
        size_of::<Self>() + self.id.len() + self.vector.len() * size_of::<f32>()
    }
}

/// Summary for an immutable segment. This is the routing layer kept in memory.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SegmentSummary {
    /// Segment identifier.
    pub id: String,
    /// LSM level, where zero is the fresh insert level.
    pub level: u8,
    /// Segment object path relative to the index root.
    pub path: String,
    /// Number of records inside the segment.
    pub object_count: usize,
    /// Vector dimensionality.
    pub dimensions: usize,
    /// Segment centroid used for coarse lower-bound pruning. Cosine and angular
    /// indexes store the mean of unit-L2-normalized vectors here.
    pub centroid: Vec<f32>,
    /// Maximum distance from centroid to any vector in the segment. This is
    /// Euclidean distance in normalized space for cosine and angular indexes.
    pub radius: f32,
    /// Per-dimension minimum vector coordinates in this segment, after unit-L2
    /// normalization for cosine and angular indexes.
    pub bounds_min: Vec<f32>,
    /// Per-dimension maximum vector coordinates in this segment, after unit-L2
    /// normalization for cosine and angular indexes.
    pub bounds_max: Vec<f32>,
    /// BLAKE3 checksum of the segment bytes.
    pub checksum: String,
    /// Stored segment size.
    pub size_bytes: u64,
    /// Segment-local graph object path relative to the index root.
    pub graph_path: String,
    /// BLAKE3 checksum of the graph bytes.
    pub graph_checksum: String,
    /// Stored graph size.
    pub graph_size_bytes: u64,
    /// Segment-local leaf engine represented by this summary.
    pub leaf_mode: LeafMode,
    /// Fixed-size bloom filter over record ids in this segment.
    pub id_bloom: Vec<u8>,
    /// Fixed-size bloom filter over quantized vector signatures in this segment.
    pub vector_signature_bloom: Vec<u8>,
    /// Per-segment metadata pruning stats (numeric min/max + value blooms).
    #[serde(default)]
    pub metadata_stats: crate::MetadataStats,
    /// Number of records in this segment physically encoded as sparse vectors.
    #[serde(default)]
    pub sparse_encoded: usize,
    /// Number of records in this segment physically encoded as dense vectors.
    #[serde(default)]
    pub dense_encoded: usize,
    /// Number of records in this segment that have text term frequencies.
    #[serde(default)]
    pub text_doc_count: u32,
    /// Sum of text term frequencies across records with text in this segment.
    #[serde(default)]
    pub text_total_doc_length: u64,
    /// Segment creation time.
    pub created_at: DateTime<Utc>,
}

impl SegmentSummary {
    pub(crate) fn resident_bytes_estimate(&self) -> usize {
        size_of::<Self>()
            + self.id.len()
            + self.path.len()
            + self.checksum.len()
            + self.graph_path.len()
            + self.graph_checksum.len()
            + self.id_bloom.len()
            + self.vector_signature_bloom.len()
            + self.centroid.len() * size_of::<f32>()
            + self.bounds_min.len() * size_of::<f32>()
            + self.bounds_max.len() * size_of::<f32>()
            + self.metadata_stats.resident_bytes_estimate()
    }

    pub(crate) fn might_contain_record_id(&self, id: impl AsRef<[u8]>) -> bool {
        if self.id_bloom.len() != SEGMENT_ID_BLOOM_BYTES {
            return true;
        }

        bloom_contains(&self.id_bloom, id)
    }

    pub(crate) fn might_contain_vector_signature(&self, signature: u64) -> bool {
        if self.vector_signature_bloom.len() != SEGMENT_VECTOR_SIGNATURE_BLOOM_BYTES {
            return false;
        }

        vector_signature_bloom_contains(&self.vector_signature_bloom, signature)
    }

    pub(crate) fn lower_bound(&self, query: &[f32], metric: &VectorMetric) -> Result<f32> {
        if !metric.supports_centroid_lower_bound() {
            return Ok(f32::NEG_INFINITY);
        }

        if metric.uses_normalized_euclidean_geometry() {
            return normalized_euclidean_geometry_lower_bound(
                query,
                metric,
                &self.centroid,
                self.radius,
                &self.bounds_min,
                &self.bounds_max,
            );
        }

        if let Some(lower_bound) =
            vector_bounds_lower_bound(query, metric, &self.bounds_min, &self.bounds_max)?
        {
            return Ok(lower_bound);
        }

        let center_distance = metric.distance(query, &self.centroid)?;
        Ok((center_distance - self.radius).max(0.0))
    }
}

pub(crate) fn segment_id_bloom(ids: impl IntoIterator<Item = impl AsRef<[u8]>>) -> Vec<u8> {
    let mut bloom = vec![0_u8; SEGMENT_ID_BLOOM_BYTES];
    for id in ids {
        for position in bloom_positions(id) {
            bloom[position / 8] |= 1_u8 << (position % 8);
        }
    }
    bloom
}

pub(crate) fn segment_vector_signature_bloom<'a>(
    vectors: impl IntoIterator<Item = &'a [f32]>,
) -> Vec<u8> {
    let mut bloom = vec![0_u8; SEGMENT_VECTOR_SIGNATURE_BLOOM_BYTES];
    for vector in vectors {
        for position in vector_signature_bloom_positions(vector_signature(vector)) {
            bloom[position / 8] |= 1_u8 << (position % 8);
        }
    }
    bloom
}

fn bloom_contains(bloom: &[u8], id: impl AsRef<[u8]>) -> bool {
    bloom_positions(id)
        .into_iter()
        .all(|position| bloom[position / 8] & (1_u8 << (position % 8)) != 0)
}

fn vector_signature_bloom_contains(bloom: &[u8], signature: u64) -> bool {
    vector_signature_bloom_positions(signature)
        .into_iter()
        .all(|position| bloom[position / 8] & (1_u8 << (position % 8)) != 0)
}

fn vector_bounds_lower_bound(
    query: &[f32],
    metric: &VectorMetric,
    bounds_min: &[f32],
    bounds_max: &[f32],
) -> Result<Option<f32>> {
    if bounds_min.len() != query.len() || bounds_max.len() != query.len() {
        return Ok(None);
    }

    let outside_deltas = query
        .iter()
        .zip(bounds_min)
        .zip(bounds_max)
        .map(|((value, min), max)| {
            if !min.is_finite() || !max.is_finite() || min > max {
                return Err(BorsukError::InvalidStorage(
                    "routing vector bounds must contain finite min <= max values".to_string(),
                ));
            }
            if value < min {
                Ok(min - value)
            } else if value > max {
                Ok(value - max)
            } else {
                Ok(0.0)
            }
        })
        .collect::<Result<Vec<_>>>()?;

    let lower_bound = match metric {
        VectorMetric::Euclidean => outside_deltas
            .iter()
            .map(|delta| delta * delta)
            .sum::<f32>()
            .sqrt(),
        VectorMetric::Manhattan => outside_deltas.iter().sum(),
        VectorMetric::Gower => outside_deltas.iter().sum::<f32>() / outside_deltas.len() as f32,
        VectorMetric::Chebyshev => outside_deltas.into_iter().fold(0.0_f32, f32::max),
        VectorMetric::Minkowski { p } => outside_deltas
            .iter()
            .map(|delta| delta.powf(*p))
            .sum::<f32>()
            .powf(1.0 / *p),
        _ => return Ok(None),
    };
    Ok(Some(lower_bound))
}

fn normalized_euclidean_geometry_lower_bound(
    query: &[f32],
    metric: &VectorMetric,
    centroid: &[f32],
    radius: f32,
    bounds_min: &[f32],
    bounds_max: &[f32],
) -> Result<f32> {
    let query = unit_l2_normalized(query);
    if query.iter().all(|value| *value == 0.0) {
        return Ok(0.0);
    }

    let bounds_lower_bound =
        vector_bounds_lower_bound(&query, &VectorMetric::Euclidean, bounds_min, bounds_max)?;
    if bounds_lower_bound.is_some() && bounds_contain_zero(bounds_min, bounds_max) {
        return Ok(0.0);
    }

    let euclidean_lower_bound = if let Some(lower_bound) = bounds_lower_bound {
        lower_bound
    } else {
        let center_distance = VectorMetric::Euclidean.distance(&query, centroid)?;
        (center_distance - radius).max(0.0)
    };

    Ok(normalized_euclidean_lower_bound_to_metric(
        euclidean_lower_bound,
        metric,
    ))
}

fn bounds_contain_zero(bounds_min: &[f32], bounds_max: &[f32]) -> bool {
    !bounds_min.is_empty()
        && bounds_min.len() == bounds_max.len()
        && bounds_min
            .iter()
            .zip(bounds_max)
            .all(|(min, max)| *min <= 0.0 && *max >= 0.0)
}

fn normalized_euclidean_lower_bound_to_metric(
    euclidean_lower_bound: f32,
    metric: &VectorMetric,
) -> f32 {
    const ROUNDING_SAFETY_MARGIN: f32 = 16.0 * f32::EPSILON;
    let euclidean_lower_bound = (euclidean_lower_bound - ROUNDING_SAFETY_MARGIN).max(0.0);
    let cosine_lower_bound = (euclidean_lower_bound * euclidean_lower_bound / 2.0).clamp(0.0, 2.0);
    match metric {
        VectorMetric::Cosine => cosine_lower_bound,
        VectorMetric::Angular => {
            (1.0 - cosine_lower_bound).clamp(-1.0, 1.0).acos() / std::f32::consts::PI
        }
        _ => unreachable!("normalized Euclidean conversion requires cosine or angular metric"),
    }
}

fn bloom_positions(id: impl AsRef<[u8]>) -> [usize; SEGMENT_ID_BLOOM_HASHES] {
    let hash = blake3::hash(id.as_ref());
    let bytes = hash.as_bytes();
    let bit_count = SEGMENT_ID_BLOOM_BYTES * 8;
    let mut positions = [0_usize; SEGMENT_ID_BLOOM_HASHES];
    for (index, position) in positions.iter_mut().enumerate() {
        let start = index * 4;
        let value = u32::from_le_bytes([
            bytes[start],
            bytes[start + 1],
            bytes[start + 2],
            bytes[start + 3],
        ]);
        *position = value as usize % bit_count;
    }
    positions
}

fn vector_signature_bloom_positions(
    signature: u64,
) -> [usize; SEGMENT_VECTOR_SIGNATURE_BLOOM_HASHES] {
    let hash = blake3::hash(&signature.to_le_bytes());
    let bytes = hash.as_bytes();
    let bit_count = SEGMENT_VECTOR_SIGNATURE_BLOOM_BYTES * 8;
    let mut positions = [0_usize; SEGMENT_VECTOR_SIGNATURE_BLOOM_HASHES];
    for (index, position) in positions.iter_mut().enumerate() {
        let start = index * 4;
        let value = u32::from_le_bytes([
            bytes[start],
            bytes[start + 1],
            bytes[start + 2],
            bytes[start + 3],
        ]);
        *position = value as usize % bit_count;
    }
    positions
}
