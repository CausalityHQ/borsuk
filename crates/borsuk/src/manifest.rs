use std::{collections::BTreeSet, mem::size_of};

use crate::simd_control::f32x8;
use chrono::{DateTime, Utc};

use crate::{
    cell_wal::{CellWalConfig, LogicalCellId},
    error::{BorsukError, Result},
    index::IndexConfig,
    metric::{VectorMetric, unit_l2_normalized},
    record::{BuildConfig, LeafCapability, LeafMode},
    segment::vector_signature,
};

pub(crate) const TABLE_EXTENSION: &str = "parquet";
pub(crate) const SEGMENT_ID_BLOOM_BYTES: usize = 8 * 1024;
pub(crate) const TOMBSTONE_ID_BLOOM_BYTES: usize = 128;
pub(crate) const SEGMENT_VECTOR_SIGNATURE_BLOOM_BYTES: usize = 256;
/// Default number of routing page refs grouped into each routing parent page.
pub const DEFAULT_ROUTING_PAGE_FANOUT: usize = 128;
/// Default maximum segment-local graph neighbors per source record.
pub const DEFAULT_GRAPH_NEIGHBORS: usize = 8;
const SEGMENT_ID_BLOOM_HASHES: usize = 4;
const SEGMENT_VECTOR_SIGNATURE_BLOOM_HASHES: usize = 4;

/// Write-ahead-log configuration for an index. Enabled by default: `add`/`upsert`
/// batches are appended to immutable per-cell lane runs and made visible through
/// one transaction commit marker; foreground writes do not advance the
/// collection-wide `CURRENT` manifest. This cuts per-`add` latency by skipping
/// the PQ/graph/segment build entirely while preserving atomic multi-cell
/// visibility. Reads double-collect lane heads and union only committed runs.
///
/// The flush thresholds are a **memory-safety cap**, not an eager build trigger:
/// the un-flushed tail is materialized into segments by the SINGLE build that
/// [`crate::BorsukIndex::compact`] (or an explicit [`crate::BorsukIndex::flush`])
/// runs — compaction consumes the tail records directly, so the expensive
/// per-record encode (Parquet, dense sidecar, graph, PQ, cell clustering) happens
/// exactly once between ingest and the first compaction rather than twice. Only a
/// long streaming workload that accumulates past a local cap spills complete
/// transactions touching the hot logical cell into intermediate L0 segments.
/// A separate collection ceiling is divided across modalities and drains a
/// modality only when many individually cold cells would otherwise accumulate
/// an unbounded aggregate tail. Disable the WAL explicitly for the classic
/// synchronous segment-per-`add` behavior.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WalConfig {
    /// Whether the write-ahead log is active for this index.
    pub enabled: bool,
    /// Flush once any one logical cell's live record/tombstone/statistics
    /// frontier reaches this many committed transaction runs, bounding refresh
    /// request count even for workloads made of tiny writes.
    pub flush_threshold_runs: usize,
    /// Flush transactions touching a logical cell once that cell's number of
    /// un-flushed records reaches this many.
    pub flush_threshold_records: usize,
    /// Flush transactions touching a logical cell once that cell's un-flushed
    /// encoded byte size reaches this many bytes.
    pub flush_threshold_bytes: u64,
    /// Flush the complete modality tail once its aggregate encoded size reaches
    /// this collection-shared ceiling. The root divides this budget across all
    /// declared modalities, so many cold cells cannot accumulate one local
    /// threshold each.
    pub collection_flush_threshold_bytes: u64,
}

/// Default maximum immutable runs in any one live mutation frontier.
pub const DEFAULT_WAL_FLUSH_THRESHOLD_RUNS: usize = 64;
/// Default WAL record cap. The byte threshold normally wins for wide vectors;
/// this cap bounds brute-force delta work for low-dimensional/text workloads.
pub const DEFAULT_WAL_FLUSH_THRESHOLD_RECORDS: usize = 16_384;
/// Default WAL encoded-byte cap (32 MiB). Because vector width contributes to
/// encoded size, wide-vector indexes flush at fewer records automatically. See
/// [`DEFAULT_WAL_FLUSH_THRESHOLD_RECORDS`].
pub const DEFAULT_WAL_FLUSH_THRESHOLD_BYTES: u64 = 32 * 1024 * 1024;
/// Default aggregate unflushed WAL ceiling for the complete collection.
pub const DEFAULT_WAL_COLLECTION_FLUSH_THRESHOLD_BYTES: u64 = 256 * 1024 * 1024;

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            flush_threshold_runs: DEFAULT_WAL_FLUSH_THRESHOLD_RUNS,
            flush_threshold_records: DEFAULT_WAL_FLUSH_THRESHOLD_RECORDS,
            flush_threshold_bytes: DEFAULT_WAL_FLUSH_THRESHOLD_BYTES,
            collection_flush_threshold_bytes: DEFAULT_WAL_COLLECTION_FLUSH_THRESHOLD_BYTES,
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
    /// Leaf-search capability fixed at index creation. `GraphEnabled` is the
    /// historical behavior; `PqScanOnly` is the new-index production default.
    /// Missing fields must remain graph-enabled for storage compatibility.
    #[serde(default = "legacy_leaf_capability")]
    pub(crate) leaf_capability: LeafCapability,
    /// Typed BUILD-tuning knobs fixed at index creation (sidecar compression,
    /// k-means sampling, iteration/codebook caps). Absent on an older manifest,
    /// so `#[serde(default)]` restores the historical behavior; a defaulted
    /// config builds byte-identically to before this field existed.
    #[serde(default)]
    pub(crate) build_config: BuildConfig,
    /// Legacy/unconsolidated tombstone run. Fresh indexes consolidate into
    /// `tombstone_pages`; retained here only across a mutation boundary.
    pub(crate) tombstone: Option<TombstoneSummary>,
    /// Small immutable tombstone-delta runs committed by foreground mutations.
    /// They are folded into hash-routed pages at flush/compaction boundaries,
    /// keeping update/delete latency independent of the accumulated deleted-id
    /// set.
    #[serde(default)]
    pub(crate) tombstone_frontier: Vec<TombstoneSummary>,
    /// Hash-routed consolidated tombstone pages. A point lookup reads at most
    /// one stable page; foreground delta runs remain in `tombstone_frontier`
    /// until flush copy-on-writes only their affected buckets.
    #[serde(default)]
    pub(crate) tombstone_pages: Vec<TombstonePageRef>,
    /// Exact number of unique ids represented by the stable tombstone plus
    /// mutation frontier. Maintained incrementally so foreground writes and
    /// status calls never decode the corpus-wide overlay merely to count it.
    #[serde(default)]
    pub(crate) tombstone_id_count: u64,
    /// Write-ahead-log configuration fixed at index creation. Enabled with
    /// bounded production thresholds by default.
    #[serde(default)]
    pub(crate) wal_config: WalConfig,
    /// Active stable routing topology. Fresh production indexes start at epoch
    /// one; physical segment replacement does not change this identity.
    #[serde(default = "default_routing_epoch")]
    pub(crate) routing_epoch: u64,
    /// Independent lane count owned by every logical cell in this epoch.
    #[serde(default)]
    pub(crate) cell_wal_config: CellWalConfig,
    /// Active logical write cells, independent of replaceable segment IDs.
    #[serde(default = "default_logical_cells")]
    pub(crate) logical_cells: Vec<LogicalCellId>,
    /// Immutable routing centroids corresponding one-for-one with
    /// `logical_cells`. Empty denotes the bootstrap topology.
    #[serde(default)]
    pub(crate) logical_cell_centroids: Vec<Vec<f32>>,
    /// Cell-WAL run checksums already materialized into immutable base segments.
    #[serde(default)]
    pub(crate) cell_wal_consumed_runs: BTreeSet<String>,
    /// Handle-local count for public status reporting; reconstructed from the
    /// double-collected lane snapshot and never persisted in the catalog.
    #[serde(skip)]
    pub(crate) cell_wal_visible_runs: usize,
    /// Handle-local count of visible cell-WAL tombstone payloads.
    #[serde(skip)]
    pub(crate) cell_wal_visible_tombstone_runs: usize,
    /// Reference to the persisted IVF coarse-quantizer object for this manifest
    /// version, or `None` when none was written (too few cells, disabled by
    /// config, or an older manifest). Present so a COLD/paged index can load the
    /// quantizer with a single read and route approximate queries to the nprobe
    /// nearest cells without pulling every centroid resident. `#[serde(default)]`
    /// so an older manifest (which lacks the field) reloads with no reference.
    #[serde(default)]
    pub(crate) quantizer_ref: Option<QuantizerRef>,
    /// Reference to the compact global TurboQuant product-code artifact used by
    /// the resident `pq-scan` fast path. The field is null until a full
    /// compaction finalizes the current segment layout.
    pub(crate) global_pq_ref: Option<GlobalPqRef>,
    /// Content-addressed Arrow/Parquet roots for BM25 and named learned-sparse
    /// fields. Empty until lexical data exists; every segment-set change
    /// recreates the affected roots before this manifest is published.
    pub(crate) lexical_roots: Vec<LexicalRootRef>,
    /// Persisted, paged corrections from immutable physical BM25 shard
    /// statistics to the currently visible MVCC corpus. Queries load only the
    /// pages covering their terms, so deletes/upserts never require a
    /// full-corpus live-row scan.
    pub(crate) bm25_stats_delta: Option<Bm25StatsDeltaRef>,
    /// WAL-side BM25 corrections produced by un-flushed updates/deletes. Each
    /// entry covers only one mutation batch; queries add the query-term pages to
    /// the consolidated correction and flush merges them by page-level COW.
    #[serde(default)]
    pub(crate) bm25_stats_delta_frontier: Vec<Bm25StatsDeltaRef>,
    /// Manifest creation time.
    pub created_at: DateTime<Utc>,
}

const fn legacy_leaf_capability() -> LeafCapability {
    LeafCapability::GraphEnabled
}

const fn default_routing_epoch() -> u64 {
    1
}

fn default_logical_cells() -> Vec<LogicalCellId> {
    vec![LogicalCellId::new(default_routing_epoch(), 0)]
}

/// Reference from a manifest to its persisted coarse-quantizer object. The
/// object is content-addressed by its BLAKE3 checksum (its path embeds the
/// checksum), so a superseded quantizer is reclaimed by GC and a live one is
/// always byte-consistent with this reference.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct QuantizerRef {
    /// Content-addressed path of the quantizer object relative to the index root.
    pub path: String,
    /// BLAKE3 checksum of the quantizer object bytes.
    pub checksum: String,
    /// Number of cells (centroid nodes) the quantizer indexes.
    pub cells: usize,
}

/// Content-addressed resident global product-code artifact for one manifest.
pub(crate) const GLOBAL_PQ_REF_LAYOUT_VERSION: u8 = 5;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct GlobalPqRef {
    /// Version of this embedded reference graph. This is intentionally required:
    /// unreleased single-layer references must be rebuilt rather than silently
    /// interpreted as the bounded-delta layout.
    pub(crate) layout_version: u8,
    pub(crate) path: String,
    pub(crate) checksum: String,
    pub(crate) vectors: usize,
    pub(crate) subspaces: usize,
    pub(crate) candidates: usize,
    /// Default IVF cells probed when the caller leaves `max_segments` unset.
    pub(crate) probes: usize,
    /// Exact bytes retained for product codes, packed row locations, and the
    /// codebook after the descriptor is loaded.
    pub(crate) resident_bytes: u64,
    /// Bytes retained by global exact-page indexes. Fixed-width v8 exact pages
    /// need no index, so newly built references store zero.
    pub(crate) sidecar_index_bytes: u64,
    /// Physical bytes occupied by the Parquet descriptor, Arrow IPC ANN
    /// bundles, and optional cell graphs.
    #[serde(default)]
    pub(crate) storage_bytes: u64,
    /// Manifest version whose complete active segment set is covered by this
    /// base plus its optional delta. Equality skips a routing-tree walk; every
    /// segment-changing publish necessarily invalidates the shortcut.
    pub(crate) covered_manifest_version: u64,
    /// Active segment checksums in the exact ordinal order encoded by row
    /// locations in the artifact.
    pub(crate) segments: Vec<String>,
    /// Small materialized segments not yet worth encoding as a delta ANN.
    /// Persisting their summaries keeps post-drain exact-fringe reads bounded
    /// without forcing a cold reader to walk every routing page.
    #[serde(default)]
    pub(crate) exact_fringe: Vec<SegmentSummary>,
    /// A geometrically rebuilt ANN artifact over post-base segments. Nested
    /// deltas are invalid; the remaining uncovered segments form the exact
    /// fringe.
    pub(crate) delta: Option<Box<GlobalPqRef>>,
}

/// Reference to one global hierarchical lexical root Parquet table.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct LexicalRootRef {
    /// `"bm25"` or `"sparse"`.
    pub(crate) kind: String,
    /// Logical field name (`"text"` for BM25, named-vector name for sparse).
    pub(crate) name: String,
    pub(crate) path: String,
    pub(crate) checksum: String,
    pub(crate) encoded_bytes: u64,
}

/// Paged signed corrections from physical BM25 shard statistics to the live
/// MVCC corpus. Negative values describe persisted generations hidden by the
/// tombstone overlay; WAL documents remain query-local and are added
/// separately.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct Bm25StatsDeltaRef {
    pub(crate) document_count_delta: i64,
    pub(crate) total_document_length_delta: i64,
    pub(crate) pages: Vec<Bm25StatsDeltaPageRef>,
}

/// One independently readable Parquet page of term document-frequency deltas.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct Bm25StatsDeltaPageRef {
    pub(crate) first_term: u32,
    pub(crate) last_term: u32,
    pub(crate) path: String,
    pub(crate) checksum: String,
    pub(crate) encoded_bytes: u64,
    pub(crate) term_count: u32,
}

impl Bm25StatsDeltaRef {
    pub(crate) fn resident_bytes_estimate(&self) -> usize {
        size_of::<Self>()
            .saturating_add(
                self.pages
                    .capacity()
                    .saturating_mul(size_of::<Bm25StatsDeltaPageRef>()),
            )
            .saturating_add(
                self.pages
                    .iter()
                    .map(|page| page.path.len().saturating_add(page.checksum.len()))
                    .sum::<usize>(),
            )
    }
}

/// Per-segment Parquet shard used to rebuild a global lexical root without
/// reopening dense segment data.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct SegmentLexicalShardRef {
    pub(crate) kind: String,
    pub(crate) name: String,
    pub(crate) path: String,
    pub(crate) checksum: String,
    pub(crate) encoded_bytes: u64,
    pub(crate) document_count: u64,
    pub(crate) total_document_length: u64,
    pub(crate) dimensions: u32,
}

impl SegmentLexicalShardRef {
    fn resident_bytes_estimate(&self) -> usize {
        size_of::<Self>()
            .saturating_add(self.kind.len())
            .saturating_add(self.name.len())
            .saturating_add(self.path.len())
            .saturating_add(self.checksum.len())
    }
}

impl LexicalRootRef {
    pub(crate) fn resident_bytes_estimate(&self) -> usize {
        size_of::<Self>()
            .saturating_add(self.kind.len())
            .saturating_add(self.name.len())
            .saturating_add(self.path.len())
            .saturating_add(self.checksum.len())
    }
}

impl GlobalPqRef {
    pub(crate) fn validate_layout(&self) -> Result<()> {
        if self.layout_version != GLOBAL_PQ_REF_LAYOUT_VERSION {
            return Err(BorsukError::InvalidStorage(format!(
                "unsupported global PQ reference layout version {}; rebuild the unreleased index",
                self.layout_version
            )));
        }
        if self
            .delta
            .as_ref()
            .is_some_and(|delta| delta.delta.is_some())
        {
            return Err(BorsukError::InvalidStorage(
                "global PQ reference contains a nested delta; rebuild the unreleased index"
                    .to_string(),
            ));
        }
        if let Some(delta) = &self.delta {
            delta.validate_layout()?;
            let base = self
                .segments
                .iter()
                .collect::<std::collections::HashSet<_>>();
            if delta.segments.iter().any(|segment| base.contains(segment)) {
                return Err(BorsukError::InvalidStorage(
                    "global PQ base and delta segment coverage overlaps; rebuild the unreleased index"
                        .to_string(),
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn resident_bytes_estimate(&self) -> usize {
        [
            size_of::<Self>(),
            self.path.len(),
            self.checksum.len(),
            self.segments.capacity().saturating_mul(size_of::<String>()),
            self.segments.iter().map(String::len).sum::<usize>(),
            self.exact_fringe
                .iter()
                .map(|summary| summary.resident_bytes_estimate())
                .sum::<usize>(),
            usize::try_from(self.resident_bytes).unwrap_or(usize::MAX),
            usize::try_from(self.sidecar_index_bytes).unwrap_or(usize::MAX),
        ]
        .into_iter()
        .fold(0_usize, usize::saturating_add)
        .saturating_add(
            self.delta
                .as_ref()
                .map_or(0, |delta| delta.resident_bytes_estimate()),
        )
    }
}

impl QuantizerRef {
    pub(crate) fn resident_bytes_estimate(&self) -> usize {
        size_of::<Self>() + self.path.len() + self.checksum.len()
    }
}

/// Summary of one immutable tombstone run. Foreground mutations append small
/// runs whose blooms stay resident; consolidation hash-shards them into stable
/// pages so a point lookup fetches at most one stable bucket plus matching live
/// runs.
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
        if self.id_bloom.len() != TOMBSTONE_ID_BLOOM_BYTES {
            return true;
        }
        bloom_contains_with_bits(&self.id_bloom, id, TOMBSTONE_ID_BLOOM_BYTES * 8)
    }

    pub(crate) fn resident_bytes_estimate(&self) -> usize {
        size_of::<Self>() + self.path.len() + self.checksum.len() + self.id_bloom.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct TombstonePageRef {
    pub bucket: u16,
    pub path: String,
    pub checksum: String,
    pub count: u64,
    /// Resident negative-membership filter for the stable page. Search must
    /// consult this before fetching the page from object storage.
    pub id_bloom: Vec<u8>,
    pub created_at: DateTime<Utc>,
}

impl TombstonePageRef {
    pub(crate) fn from_summary(bucket: u16, summary: TombstoneSummary) -> Self {
        Self {
            bucket,
            path: summary.path,
            checksum: summary.checksum,
            count: summary.count,
            id_bloom: summary.id_bloom,
            created_at: summary.created_at,
        }
    }

    /// `false` is an exact negative and lets an unrelated candidate avoid a
    /// remote stable-page read. A malformed filter fails open for correctness.
    pub(crate) fn might_contain_record_id(&self, id: impl AsRef<[u8]>) -> bool {
        if self.id_bloom.len() != TOMBSTONE_ID_BLOOM_BYTES {
            return true;
        }
        bloom_contains_with_bits(&self.id_bloom, id, TOMBSTONE_ID_BLOOM_BYTES * 8)
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
        build_config: BuildConfig,
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
            build_config,
            tombstone: None,
            tombstone_frontier: Vec::new(),
            tombstone_pages: Vec::new(),
            tombstone_id_count: 0,
            wal_config: WalConfig::default(),
            routing_epoch: default_routing_epoch(),
            cell_wal_config: CellWalConfig::default(),
            logical_cells: default_logical_cells(),
            logical_cell_centroids: Vec::new(),
            cell_wal_consumed_runs: BTreeSet::new(),
            cell_wal_visible_runs: 0,
            cell_wal_visible_tombstone_runs: 0,
            quantizer_ref: None,
            global_pq_ref: None,
            lexical_roots: Vec::new(),
            bm25_stats_delta: None,
            bm25_stats_delta_frontier: Vec::new(),
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
            build_config: self.build_config.clone(),
            tombstone: self.tombstone.clone(),
            tombstone_frontier: self.tombstone_frontier.clone(),
            tombstone_pages: self.tombstone_pages.clone(),
            tombstone_id_count: self.tombstone_id_count,
            wal_config: self.wal_config.clone(),
            routing_epoch: self.routing_epoch,
            cell_wal_config: self.cell_wal_config,
            logical_cells: self.logical_cells.clone(),
            logical_cell_centroids: self.logical_cell_centroids.clone(),
            cell_wal_consumed_runs: self.cell_wal_consumed_runs.clone(),
            cell_wal_visible_runs: self.cell_wal_visible_runs,
            cell_wal_visible_tombstone_runs: self.cell_wal_visible_tombstone_runs,
            // The quantizer indexes the active cell set. Carry the reference into
            // the next version so metadata-only publishes (tombstone bumps, WAL
            // flushes) that leave the segments unchanged keep a valid quantizer.
            // Any publish that CHANGES the segment set explicitly recomputes or
            // clears this before publishing (see the compaction/flush paths).
            quantizer_ref: self.quantizer_ref.clone(),
            global_pq_ref: self.global_pq_ref.clone(),
            lexical_roots: self.lexical_roots.clone(),
            bm25_stats_delta: self.bm25_stats_delta.clone(),
            bm25_stats_delta_frontier: self.bm25_stats_delta_frontier.clone(),
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

    /// Active stable routing epoch.
    #[must_use]
    pub fn routing_epoch(&self) -> u64 {
        self.routing_epoch
    }

    /// Cell-local WAL lane policy frozen in the collection catalog.
    #[must_use]
    pub fn cell_wal_config(&self) -> CellWalConfig {
        self.cell_wal_config
    }

    /// Active logical write cells for the routing epoch.
    #[must_use]
    pub fn logical_cells(&self) -> &[LogicalCellId] {
        &self.logical_cells
    }

    /// Maximum records written into each future immutable physical segment.
    ///
    /// This is independent from the stable logical-cell routing catalog.
    #[must_use]
    pub fn segment_max_vectors(&self) -> usize {
        self.config.segment_max_vectors
    }

    /// Whether the published, un-flushed WAL frontier is empty (no un-flushed
    /// tail objects). Reads short-circuit the WAL union when this is true.
    #[must_use]
    pub fn wal_frontier_is_empty(&self) -> bool {
        self.cell_wal_visible_runs == 0
            && self.tombstone_frontier.is_empty()
            && self.bm25_stats_delta_frontier.is_empty()
    }

    /// Number of published, un-flushed WAL objects in the frontier.
    #[must_use]
    pub fn wal_frontier_len(&self) -> usize {
        self.cell_wal_visible_runs
            + self.tombstone_frontier.len()
            + self.bm25_stats_delta_frontier.len()
    }

    /// Number of consolidated hash-routed tombstone pages in this snapshot.
    #[must_use]
    pub fn tombstone_page_count(&self) -> usize {
        self.tombstone_pages.len()
    }

    /// Number of un-consolidated foreground tombstone WAL runs.
    #[must_use]
    pub fn tombstone_delta_run_count(&self) -> usize {
        self.tombstone_frontier.len() + self.cell_wal_visible_tombstone_runs
    }

    /// Whether this manifest references a persisted IVF coarse-quantizer object
    /// (enabling cold/paged approximate routing with one object read).
    #[must_use]
    pub fn has_persisted_quantizer(&self) -> bool {
        self.quantizer_ref.is_some()
    }

    /// The relative path of the persisted coarse-quantizer object, or `None`.
    #[must_use]
    pub fn persisted_quantizer_path(&self) -> Option<&str> {
        self.quantizer_ref.as_ref().map(|q| q.path.as_str())
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
        let tombstone_frontier_bytes = self
            .tombstone_frontier
            .iter()
            .map(TombstoneSummary::resident_bytes_estimate)
            .sum::<usize>();
        let tombstone_page_bytes = self
            .tombstone_pages
            .iter()
            .map(TombstonePageRef::resident_bytes_estimate)
            .sum::<usize>();
        let quantizer_ref_bytes = self
            .quantizer_ref
            .as_ref()
            .map(QuantizerRef::resident_bytes_estimate)
            .unwrap_or(0);
        let global_pq_ref_bytes = self
            .global_pq_ref
            .as_ref()
            .map(GlobalPqRef::resident_bytes_estimate)
            .unwrap_or(0);
        let lexical_root_bytes = self
            .lexical_roots
            .iter()
            .map(LexicalRootRef::resident_bytes_estimate)
            .sum::<usize>();
        let bm25_stats_delta_bytes = self
            .bm25_stats_delta
            .as_ref()
            .map(Bm25StatsDeltaRef::resident_bytes_estimate)
            .unwrap_or(0);
        let bm25_stats_delta_frontier_bytes = self
            .bm25_stats_delta_frontier
            .iter()
            .map(Bm25StatsDeltaRef::resident_bytes_estimate)
            .sum::<usize>();
        (size_of::<Self>()
            + config_bytes
            + text_tokenizer_bytes
            + segments_bytes
            + pivots_bytes
            + tombstone_bytes
            + tombstone_frontier_bytes
            + tombstone_page_bytes
            + quantizer_ref_bytes
            + global_pq_ref_bytes
            + lexical_root_bytes
            + bm25_stats_delta_bytes
            + bm25_stats_delta_frontier_bytes) as u64
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
    pub page_vector_bytes: u64,
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
    /// Persisted write-time role/codec/policy resolution. Readers dispatch only
    /// from this reference and never from collection-wide defaults.
    pub layout: crate::PhysicalLayoutRef,
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
    /// Stored standard Arrow IPC exact-vector sidecar size.
    #[serde(default)]
    pub vector_size_bytes: u64,
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
    /// Maximum decoded bytes of one BM25 Parquet posting/row-metadata block.
    ///
    /// The read planner uses this ingest-time property to choose decode
    /// parallelism under the runtime RAM/concurrency envelope. It is data
    /// dependent and deliberately independent of benchmark dataset identity.
    #[serde(default)]
    pub text_lexical_decoded_bytes: u64,
    /// Largest decoded Parquet posting/row-metadata block among this segment's
    /// named sparse fields. The maximum is the conservative per-run working-set
    /// estimate used by the shared lexical admission gate.
    #[serde(default)]
    pub sparse_lexical_max_decoded_bytes: u64,
    /// Immutable Parquet term-stat/run shards for this segment.
    pub(crate) lexical_shards: Vec<SegmentLexicalShardRef>,
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
            + self
                .lexical_shards
                .iter()
                .map(SegmentLexicalShardRef::resident_bytes_estimate)
                .sum::<usize>()
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

        // Query validated once at the search entry; the segment centroid is a
        // stored, already-validated vector — score through the unchecked kernel.
        let center_distance = metric.distance_unchecked(query, &self.centroid)?;
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

pub(crate) fn tombstone_id_bloom(ids: impl IntoIterator<Item = impl AsRef<[u8]>>) -> Vec<u8> {
    let mut bloom = vec![0_u8; TOMBSTONE_ID_BLOOM_BYTES];
    for id in ids {
        for position in bloom_positions_with_bits(id, TOMBSTONE_ID_BLOOM_BYTES * 8) {
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

    if bounds_min
        .iter()
        .zip(bounds_max)
        .any(|(min, max)| !min.is_finite() || !max.is_finite() || min > max)
    {
        return Err(BorsukError::InvalidStorage(
            "routing vector bounds must contain finite min <= max values".to_string(),
        ));
    }

    const LANES: usize = 8;
    let chunks = query.len() / LANES;
    let mut sum = f32x8::ZERO;
    let mut maximum = f32x8::ZERO;
    for chunk in 0..chunks {
        let base = chunk * LANES;
        let query_lanes = f32x8::from(
            <[f32; LANES]>::try_from(&query[base..base + LANES])
                .expect("routing-bound query SIMD lane width"),
        );
        let minimum_lanes = f32x8::from(
            <[f32; LANES]>::try_from(&bounds_min[base..base + LANES])
                .expect("routing-bound minimum SIMD lane width"),
        );
        let maximum_lanes = f32x8::from(
            <[f32; LANES]>::try_from(&bounds_max[base..base + LANES])
                .expect("routing-bound maximum SIMD lane width"),
        );
        let delta = (minimum_lanes - query_lanes)
            .max(query_lanes - maximum_lanes)
            .max(f32x8::ZERO);
        match metric {
            VectorMetric::Euclidean => sum += delta * delta,
            VectorMetric::Manhattan | VectorMetric::Gower => sum += delta,
            VectorMetric::Chebyshev => maximum = maximum.max(delta),
            VectorMetric::Minkowski { p } => sum += delta.powf(*p),
            _ => return Ok(None),
        }
    }
    let tail = chunks * LANES;
    let mut tail_sum = 0.0_f32;
    let mut tail_maximum = 0.0_f32;
    for index in tail..query.len() {
        let delta = (bounds_min[index] - query[index])
            .max(query[index] - bounds_max[index])
            .max(0.0);
        match metric {
            VectorMetric::Euclidean => tail_sum += delta * delta,
            VectorMetric::Manhattan | VectorMetric::Gower => tail_sum += delta,
            VectorMetric::Chebyshev => tail_maximum = tail_maximum.max(delta),
            VectorMetric::Minkowski { p } => tail_sum += delta.powf(*p),
            _ => return Ok(None),
        }
    }

    let lower_bound = match metric {
        VectorMetric::Euclidean => (sum.reduce_add() + tail_sum).sqrt(),
        VectorMetric::Manhattan => sum.reduce_add() + tail_sum,
        VectorMetric::Gower => (sum.reduce_add() + tail_sum) / query.len() as f32,
        VectorMetric::Chebyshev => maximum.to_array().into_iter().fold(tail_maximum, f32::max),
        VectorMetric::Minkowski { p } => (sum.reduce_add() + tail_sum).powf(1.0 / *p),
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
        // `query` is the locally L2-normalized (finite) query; `centroid` is a
        // stored vector — both trusted, so skip the finite/dim re-scan. Euclidean
        // never returns a degeneracy error, so `?` here is just the kernel value.
        let center_distance = VectorMetric::Euclidean.distance_unchecked(&query, centroid)?;
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
    bloom_positions_with_bits(id, SEGMENT_ID_BLOOM_BYTES * 8)
}

fn bloom_positions_with_bits(
    id: impl AsRef<[u8]>,
    bit_count: usize,
) -> [usize; SEGMENT_ID_BLOOM_HASHES] {
    let hash = blake3::hash(id.as_ref());
    let bytes = hash.as_bytes();
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

fn bloom_contains_with_bits(bloom: &[u8], id: impl AsRef<[u8]>, bit_count: usize) -> bool {
    bloom_positions_with_bits(id, bit_count)
        .into_iter()
        .all(|position| bloom[position / 8] & (1_u8 << (position % 8)) != 0)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn production_segment_id_bloom_keeps_absent_lookup_amplification_bounded() {
        let stored = (0..5_461)
            .map(|ordinal| format!("stored-{ordinal}"))
            .collect::<Vec<_>>();
        let bloom = segment_id_bloom(&stored);

        assert!(stored.iter().all(|id| bloom_contains(&bloom, id)));
        let false_positives = (0..10_000)
            .map(|ordinal| format!("absent-{ordinal}"))
            .filter(|id| bloom_contains(&bloom, id))
            .count();
        assert!(
            false_positives <= 100,
            "one production-sized segment admitted {false_positives}/10000 absent IDs"
        );
    }

    #[test]
    fn tombstone_run_bloom_stays_compact() {
        assert_eq!(tombstone_id_bloom(["deleted"]).len(), 128);
    }

    #[test]
    fn stable_tombstone_page_rejects_definitely_absent_ids_without_io() {
        let summary = TombstoneSummary {
            path: "tombstones/ab/page.parquet".to_string(),
            checksum: "ab".repeat(32),
            count: 1,
            id_bloom: tombstone_id_bloom(["mutated-id"]),
            created_at: chrono::Utc::now(),
        };
        let page = TombstonePageRef::from_summary(17, summary);

        assert!(page.might_contain_record_id("mutated-id"));
        assert!(!page.might_contain_record_id("never-mutated-id"));
    }

    #[test]
    fn simd_routing_bounds_match_scalar_reference() {
        for dimensions in [100_usize, 960] {
            let query = (0..dimensions)
                .map(|index| ((index * 29 % 127) as f32 - 63.0) * 0.0625)
                .collect::<Vec<_>>();
            let bounds_min = (0..dimensions)
                .map(|index| -1.5 + (index % 5) as f32 * 0.05)
                .collect::<Vec<_>>();
            let bounds_max = (0..dimensions)
                .map(|index| 1.25 + (index % 7) as f32 * 0.04)
                .collect::<Vec<_>>();
            let deltas = query
                .iter()
                .zip(&bounds_min)
                .zip(&bounds_max)
                .map(|((value, minimum), maximum)| (minimum - value).max(value - maximum).max(0.0))
                .collect::<Vec<_>>();

            for metric in [
                VectorMetric::Euclidean,
                VectorMetric::Manhattan,
                VectorMetric::Gower,
                VectorMetric::Chebyshev,
                VectorMetric::Minkowski { p: 3.0 },
            ] {
                let expected = match metric {
                    VectorMetric::Euclidean => {
                        deltas.iter().map(|delta| delta * delta).sum::<f32>().sqrt()
                    }
                    VectorMetric::Manhattan => deltas.iter().sum::<f32>(),
                    VectorMetric::Gower => deltas.iter().sum::<f32>() / dimensions as f32,
                    VectorMetric::Chebyshev => deltas.iter().copied().fold(0.0_f32, f32::max),
                    VectorMetric::Minkowski { p } => deltas
                        .iter()
                        .map(|delta| delta.powf(p))
                        .sum::<f32>()
                        .powf(1.0 / p),
                    _ => unreachable!(),
                };
                let actual = vector_bounds_lower_bound(&query, &metric, &bounds_min, &bounds_max)
                    .unwrap()
                    .unwrap();
                let tolerance = expected.abs().max(1.0) * 1.0e-5;
                assert!(
                    (actual - expected).abs() <= tolerance,
                    "dimensions={dimensions} metric={metric:?} simd={actual} scalar={expected}"
                );
            }
        }
    }
}
