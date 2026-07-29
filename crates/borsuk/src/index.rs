use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet, VecDeque},
    fmt,
    ops::Range,
    path::PathBuf,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicUsize, Ordering as AtomicOrdering},
    },
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use object_store::ObjectStore;
use rayon::prelude::*;
use tokio::sync::Semaphore;
use url::Url;
use uuid::Uuid;

use crate::{
    cell_wal::{
        CELL_WAL_CLAIM_SHARDS, CellWalClaimCheckpoint, CellWalRunInput, CellWalRunKind,
        CellWalStore, CommittedCellWalTransaction, LogicalCellId, PreparedCellWalRun,
        id_claim_shard,
    },
    centroid_hnsw::CentroidHnsw,
    collection_control::{
        COLLECTION_CURRENT, CollectionManifestRef, CollectionSnapshot, PRIMARY_MODALITY,
        collection_schema_fingerprint,
    },
    error::{BorsukError, Result},
    format::{
        bm25_postings_from_batches, bm25_stats_delta_page_from_parquet,
        bm25_stats_delta_page_to_parquet, graph_from_parquet, graph_to_parquet,
        lean_segment_from_table, lexical_root_from_parquet, lexical_root_to_parquet,
        lexical_row_metadata_from_batches, lexical_term_page_from_batches,
        lexical_term_page_from_parquet, lexical_term_page_to_parquet,
        routing_layer_page_from_parquet,
        routing_layer_page_index_from_parquet_relaxed_manifest_version, segment_from_table,
        segment_to_table, sparse_postings_from_batches, tombstone_ids_from_parquet,
        tombstone_ids_to_parquet, wal_records_from_table, wal_records_to_table,
    },
    global_pq_sidecar::{
        DEFAULT_GLOBAL_PQ_CHUNK_BYTES, GlobalCellGraph, GlobalCellGraphRef, GlobalCoarseQuantizer,
        GlobalPqCellSpool, GlobalPqChunkRef, GlobalPqDescriptor, GlobalPqRow, GlobalScanQuantizer,
        HierarchicalCoarseQuantizer, LocationEncoding, ResidentGlobalPq,
    },
    late_interaction::{LateInteractionSearchOptions, LateInteractionSearchReport},
    lexical_build::{
        DEFAULT_LEXICAL_BLOCK_BYTES, LexicalInputRow, LexicalSegmentBuild, build_lexical_segment,
    },
    lexical_root::{
        Bm25Posting, LexicalKind, LexicalRoot, LexicalRowMetadata, LexicalTermPage,
        LexicalTermPageRef, PlannedRun, SparsePosting, term_page_content_checksum,
    },
    maintenance::{self, MaintenanceConfig, MaintenanceHandle, MaintenanceReport},
    manifest::{
        Bm25StatsDeltaPageRef, Bm25StatsDeltaRef, DEFAULT_GRAPH_NEIGHBORS,
        DEFAULT_ROUTING_PAGE_FANOUT, LexicalRootRef, Manifest, QuantizerRef, RoutingLayerPageRef,
        SegmentLexicalShardRef, SegmentSummary, TombstonePageRef, TombstoneSummary, WalConfig,
        segment_id_bloom, segment_vector_signature_bloom,
    },
    metric::VectorMetric,
    observability,
    quantizer_sidecar::{PersistedQuantizer, is_quantizer_path, quantizer_relative_path},
    record::{
        AddReport, BuildConfig, CompactionOptions, CompactionReport, DEFAULT_SEARCH_PREFETCH_DEPTH,
        DeleteReport, ExplainReport, Fusion, GarbageCollectionOptions, GarbageCollectionReport,
        GlobalScanCodec, HybridOptions, HybridQuery, IncrementalMaintenanceOptions,
        IncrementalReport, IndexStats, LeafCapability, LeafMode, PurgeReport, QuantizerKind,
        QueryCostModel, RebuildOptions, RebuildReport, RecallGuarantee, RecordId, RequestCounts,
        SearchHit, SearchMode, SearchOptions, SearchReport, SearchTerminationReason,
        StorageEncoding, VectorKind, VectorRecord, VectorSpec,
    },
    rotated_product_quantizer::{ProductQuantizerConfig, RotatedProductQuantizer},
    segment::{
        Segment, SegmentGraph, VECTOR_LOCALITY_KEY_LEN, pq_code_for_query, routing_code,
        vector_bounds, vector_locality_key, vector_signature,
    },
    segment_cache::{
        AdmissionGate, ByteAdmissionGate, DecodedObjectCache, DecodedSegmentCache,
        InFlightGraphReads, InFlightReads, InFlightSegmentReads, decoded_graph_bytes,
        decoded_segment_bytes,
    },
    sparse::{SparseVector, sparse_dot},
    storage::{
        LoadedCollectionSnapshot, PrefetchedRead, RangedColumns, ReadBytes,
        RoutingLayerPageIndexRead, StagedManifest, Storage, StorageWriteReport, StoredObject,
    },
    storage_trace::{StorageAccessEvent, physical_format_for_path},
    text::{Tokenizer, UnicodeWordLowercase, term_frequencies},
};

const LOCAL_GRAPH_NEIGHBORS: usize = DEFAULT_GRAPH_NEIGHBORS;
const ROUTING_SEARCH_PAGE_OVERFETCH: usize = 8;
/// Hard entry-count guard for one global term page.
const DEFAULT_LEXICAL_TERM_PAGE_ENTRIES: usize = 4096;
const TOMBSTONE_BUCKETS: u16 = 4096;
const ID_DIRECTORY_MAGIC: &[u8; 4] = b"BID1";
const COORDINATION_COUNTER_MAGIC: &[u8; 4] = b"BCN1";
const CELL_WAL_MUTATION_METADATA_MAGIC: &[u8; 4] = b"BMM1";
const CELL_WAL_TOMBSTONE_METADATA_MAGIC: &[u8; 4] = b"BTM1";
const PACKED_INDEX_CONTROL_VERSION: u8 = 1;
const PACKED_INDEX_CONTROL_CHECKSUM_LEN: usize = 32;
/// Target decoded metadata working set of one global term page. The actual
/// builder accounts for variable paths/checksums, so high segment counts do not
/// turn a nominal page into an unbounded allocation.
const DEFAULT_LEXICAL_TERM_PAGE_BYTES: usize = 1024 * 1024;

fn elapsed_ns(started: Instant) -> u64 {
    started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64
}

fn join_rows(rows: &[usize]) -> String {
    rows.iter()
        .map(usize::to_string)
        .collect::<Vec<_>>()
        .join("|")
}

fn cell_wal_run_identity(run: &PreparedCellWalRun) -> String {
    format!(
        "{}:{}:{}:{}:{}",
        run.transaction_id, run.cell.routing_epoch, run.cell.cell_ordinal, run.lane, run.checksum
    )
}

fn cell_wal_run_count(transactions: &[CommittedCellWalTransaction]) -> usize {
    transactions.len()
}

fn cell_wal_tombstone_run_count(transactions: &[CommittedCellWalTransaction]) -> usize {
    transactions
        .iter()
        .flat_map(|transaction| &transaction.runs)
        .filter(|run| run.kind == CellWalRunKind::Tombstones)
        .count()
}

/// Below this many cells a flat centroid scan is already cheap, so the HNSW
/// coarse quantizer stays off (building a graph would not pay for itself).
const COARSE_QUANTIZER_MIN_CELLS: usize = 128;
/// Cells the coarse quantizer returns per unit of the segment budget, so filter
/// pruning still leaves the full nprobe cells to read.
const COARSE_QUANTIZER_OVERFETCH: usize = 4;
const DEFAULT_GLOBAL_CELL_GRAPH_CACHE_BYTES: u64 = 128 * 1024 * 1024;
/// Small, process-wide retention windows for immutable lexical objects.
///
/// These are deliberately fixed byte budgets rather than corpus-proportional
/// caches. They let staggered concurrent users reuse recently decoded Parquet
/// row groups/pages without making the complete postings index resident.
const DEFAULT_LEXICAL_RUN_CACHE_BYTES: u64 = 32 * 1024 * 1024;
const DEFAULT_LEXICAL_TERM_PAGE_CACHE_BYTES: u64 = 32 * 1024 * 1024;
const DEFAULT_TOMBSTONE_PAGE_CACHE_BYTES: u64 = 32 * 1024 * 1024;
const DEFAULT_BM25_STATS_PAGE_CACHE_BYTES: u64 = 16 * 1024 * 1024;
/// Default decoded late-interaction Arrow-batch retention window. It is fixed,
/// byte-bounded, and shared across callers; the full corpus is never resident.
pub const DEFAULT_LATE_INTERACTION_BATCH_CACHE_BYTES: u64 = 64 * 1024 * 1024;
const VERSION_SKIP_CURRENT_RECHECK_DELAY: Duration = Duration::from_millis(10);
const BM25_K1: f64 = 1.2;
const BM25_B: f64 = 0.75;

#[derive(Debug, Default)]
struct RoutingSummariesRead {
    summaries: Vec<SegmentSummary>,
    bytes_read: u64,
    routing_page_indexes_read: usize,
    routing_pages_read: usize,
    object_cache_hits: usize,
    object_cache_misses: usize,
    cache_repairs: usize,
}

#[derive(Debug, Default)]
struct ActiveGcObjectPathsRead {
    paths: HashSet<String>,
    bytes_read: u64,
    routing_page_indexes_read: usize,
    routing_pages_read: usize,
    object_cache_hits: usize,
    object_cache_misses: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GarbageCollectionObjectKind {
    SegmentOrGraph,
    Routing,
    Table,
}

#[derive(Debug, Clone)]
struct GarbageCollectionCandidate {
    path: String,
    size: u64,
    kind: GarbageCollectionObjectKind,
    last_modified: DateTime<Utc>,
}

struct GarbageCollectionCandidateScan<'a> {
    active_paths: &'a HashSet<String>,
    min_age: Duration,
    now: DateTime<Utc>,
    objects_scanned: &'a mut usize,
    candidates: &'a mut Vec<GarbageCollectionCandidate>,
}

#[derive(Debug, Default)]
struct RoutingPageRefsRead {
    page_refs: Vec<RoutingLayerPageRef>,
    bytes_read: u64,
    routing_pages_read: usize,
    object_cache_hits: usize,
    object_cache_misses: usize,
    cache_repairs: usize,
}

#[derive(Debug, Default)]
struct CompactionSourceSelectionRead {
    selected: Vec<SegmentSummary>,
    dirty_pages: Vec<(usize, Vec<SegmentSummary>)>,
    decoded_parent_pages: HashMap<String, Vec<RoutingLayerPageRef>>,
    bytes_read: u64,
    routing_page_indexes_read: usize,
    routing_pages_read: usize,
    object_cache_hits: usize,
    object_cache_misses: usize,
}

#[derive(Debug, Default)]
struct CompactionRoutingPatch {
    page_refs: Vec<RoutingLayerPageRef>,
    bytes_read: u64,
    routing_pages_read: usize,
    routing_pages_written: usize,
    object_cache_hits: usize,
    object_cache_misses: usize,
}

#[derive(Debug)]
struct CompactionRoutingPageUpdate {
    page_ref: RoutingLayerPageRef,
    patch: CompactionRoutingPatch,
}

#[derive(Debug)]
struct CompactionTopRoutingPageRefs {
    routing_level: u8,
    page_refs: Vec<RoutingLayerPageRef>,
    routing_pages_written: usize,
}

#[derive(Debug, Clone)]
struct SearchHitWithVector {
    hit: SearchHit,
    vector: Option<Vec<f32>>,
}

const HYBRID_TEXT_MODALITY: &str = "@text";

#[derive(Debug, Clone)]
struct HybridCandidate {
    id: RecordId,
    combined_score: f32,
    metadata: Option<crate::Metadata>,
}

#[derive(Debug)]
struct SearchExecution {
    report: SearchReport,
    vectors: Vec<Vec<f32>>,
}

#[derive(Debug, Default)]
struct RoutingPageReadCache {
    reads: HashMap<String, ReadBytes>,
}

#[derive(Debug)]
struct RoutingPageRead {
    read: ReadBytes,
    request_cache_hit: bool,
}

#[derive(Debug)]
struct SegmentPrefetch {
    candidate_index: usize,
    reserved_bytes: u64,
    read: PrefetchedRead,
}

/// Parse a human-readable byte budget.
///
/// Accepts plain bytes (`"1024"`), bytes (`"1024B"`), decimal units
/// (`KB`, `MB`, `GB`, `TB`), and binary units (`KiB`, `MiB`, `GiB`, `TiB`).
pub fn parse_byte_size(value: &str, field_name: &str) -> Result<u64> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(BorsukError::InvalidMetricInput(format!(
            "{field_name} must not be empty"
        )));
    }

    let split_at = trimmed
        .find(|ch: char| !ch.is_ascii_digit())
        .unwrap_or(trimmed.len());
    if split_at == 0 {
        return Err(BorsukError::InvalidMetricInput(format!(
            "{field_name} `{value}` must start with an integer byte count"
        )));
    }

    let amount = trimmed[..split_at].parse::<u64>().map_err(|err| {
        BorsukError::InvalidMetricInput(format!("invalid {field_name} `{value}`: {err}"))
    })?;
    let unit = trimmed[split_at..].trim().to_ascii_uppercase();
    let multiplier = match unit.as_str() {
        "" | "B" => 1_u64,
        "KB" => 1_000,
        "MB" => 1_000_000,
        "GB" => 1_000_000_000,
        "TB" => 1_000_000_000_000,
        "KIB" => 1_024,
        "MIB" => 1_048_576,
        "GIB" => 1_073_741_824,
        "TIB" => 1_099_511_627_776,
        _ => {
            return Err(BorsukError::InvalidMetricInput(format!(
                "unknown {field_name} unit `{}`",
                trimmed[split_at..].trim()
            )));
        }
    };

    amount.checked_mul(multiplier).ok_or_else(|| {
        BorsukError::InvalidMetricInput(format!("{field_name} `{value}` exceeds u64"))
    })
}

/// Parse a human-readable resident RAM budget.
///
/// Accepts the same units as [`parse_byte_size`].
pub fn parse_ram_budget(value: &str) -> Result<u64> {
    parse_byte_size(value, "ram_budget")
}

/// Configuration used when creating a new BORSUK index.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct IndexConfig {
    /// Index root URI. Plain local paths, `file://...`, and object-store URIs are supported.
    pub uri: String,
    /// Metric fixed for this physical index.
    pub metric: VectorMetric,
    /// Required vector dimensionality.
    pub dimensions: usize,
    /// Maximum number of vectors written to each immutable segment.
    pub segment_max_vectors: usize,
    /// Optional resident manifest/routing memory budget in bytes.
    pub ram_budget_bytes: Option<u64>,
    /// Whether records in this index may carry optional text payloads.
    pub text: bool,
    /// Declared named vector sub-indexes keyed by vector name.
    #[serde(default)]
    pub named_vectors: BTreeMap<String, VectorSpec>,
}

/// Target bytes of float32 vectors decoded in one default physical segment.
///
/// `segment_max_vectors` is a row count, but scan/decode cost scales with rows
/// times the quantizer's padded dimensionality. The default layout therefore
/// derives the row count from this byte target instead of applying one row count
/// to every vector width. Global IVF cells are independent from these physical
/// rerank/build units, so segment rows can cap decoded memory without degrading
/// the global routing resolution.
pub const DEFAULT_TARGET_SEGMENT_VECTOR_BYTES: usize = 16 * 1024 * 1024;
/// Smallest automatically selected cell row count, limiting object/GET fan-out
/// for very high-dimensional vectors.
pub const MIN_RECOMMENDED_SEGMENT_MAX_VECTORS: usize = 64;
/// Largest automatically selected cell row count, limiting build batches while
/// avoiding excessive routing metadata and object counts at 100M scale.
pub const MAX_RECOMMENDED_SEGMENT_MAX_VECTORS: usize = 131_072;

/// Recommend a dimension-aware immutable-cell row count for TurboQuant pq-scan.
///
/// Keeping `rows * dimensions * sizeof(float32)` near 16 MiB makes the largest
/// decoded physical segment predictable across vector widths. Explicit
/// `segment_max_vectors` values continue to override this recommendation.
#[must_use]
pub fn recommended_segment_max_vectors(dimensions: usize) -> usize {
    let dense_bytes_per_vector = dimensions.max(1).saturating_mul(std::mem::size_of::<f32>());
    (DEFAULT_TARGET_SEGMENT_VECTOR_BYTES / dense_bytes_per_vector).clamp(
        MIN_RECOMMENDED_SEGMENT_MAX_VECTORS,
        MAX_RECOMMENDED_SEGMENT_MAX_VECTORS,
    )
}

/// Default process-local admission cap for concurrent searches.
///
/// Each admitted search may itself issue up to
/// [`crate::DEFAULT_SEARCH_PREFETCH_DEPTH`] concurrent immutable-cell reads.
/// Keeping the outer search count bounded prevents multiple callers from
/// multiplying transient decode memory without limit. Research workloads can
/// opt out explicitly with [`OpenOptions::max_concurrent_searches`] set to
/// `None`.
pub const DEFAULT_MAX_CONCURRENT_SEARCHES: usize = 4;
/// Default process-local cap on cell payloads being decoded concurrently.
///
/// This is independent of whole-query admission and per-query prefetch width:
/// multiple admitted queries share these permits instead of multiplying their
/// individual cell fan-out into unbounded transient Arrow/Parquet memory.
pub const DEFAULT_MAX_CONCURRENT_CELL_DECODES: usize = 24;
/// Leave half the process RAM envelope available for routing, dense search,
/// caches, result assembly, allocator slack, and the application embedding the
/// library. Lexical transient decodes share the other half through a weighted
/// global gate.
const LEXICAL_RAM_BUDGET_DIVISOR: u64 = 2;
/// Default hard ceiling for resident index metadata and explicitly retained
/// serving structures.
///
/// Corpus-sized product codes and vectors are paged independently of this
/// budget. A deliberately unbounded research profile can set
/// [`OpenOptions::ram_budget_bytes`] to `None` explicitly.
pub const DEFAULT_RAM_BUDGET_BYTES: u64 = 512 * 1024 * 1024;
const DEFAULT_GLOBAL_PQ_RERANK_READS: usize = 64;
const DEFAULT_GLOBAL_PQ_CODE_READS: usize = 32;
/// Maximum unselected byte gap folded into one global-PQ code range GET.
///
/// Selected code slices that are adjacent (or separated only by a small bundle
/// header/alignment gap) share a request. Distant slices in the same packed
/// object remain separate so selecting two cells never downloads every
/// unrelated cell stored between them.
const DEFAULT_GLOBAL_PQ_CODE_COALESCE_GAP_BYTES: usize = 1024 * 1024;
/// Byte-equivalent cost assigned to one additional remote request by the code
/// range planner. Parent-local gaps below this are cheaper to transfer than to
/// pay as another S3 round trip.
const DEFAULT_GLOBAL_PQ_CODE_REQUEST_WEIGHT_BYTES: usize = 1024 * 1024;
/// Maximum compressed PQ-code payload retained by one query wave.
///
/// Four admitted production queries can therefore retain at most 128 MiB of
/// code objects between I/O and ADC scoring, independent of corpus size. A
/// single content-addressed chunk may be as large as this limit and is the
/// irreducible allocation.
const DEFAULT_GLOBAL_PQ_CODE_WAVE_BYTES: usize = 32 * 1024 * 1024;
/// Packed object limits. Keeping the code portion at 1 MiB means a query wave
/// cannot over-read more than its 32 MiB code budget even when each selected
/// slice comes from a different bundle. The total limit bounds build assembly
/// and the accompanying fixed-width exact pages.
const DEFAULT_GLOBAL_PQ_BUNDLE_CODE_BYTES: usize = 1024 * 1024;
const DEFAULT_GLOBAL_PQ_BUNDLE_BYTES: usize = 32 * 1024 * 1024;
const DEFAULT_SIDECAR_INDEX_CACHE_BYTES: u64 = 128 * 1024 * 1024;

/// Options used when opening an existing BORSUK index.
///
/// Defaults use paged routing (`resident_routing: false`), no local cache, no
/// decoded-segment cache, and a bounded concurrent-search admission gate.
#[derive(Debug, Clone)]
pub struct OpenOptions {
    /// Optional local read-through cache directory.
    pub cache_dir: Option<PathBuf>,
    /// Optional maximum local cache size in bytes. `None` leaves the cache unbounded.
    pub cache_max_bytes: Option<u64>,
    /// Optional runtime resident manifest/routing memory budget in bytes.
    pub ram_budget_bytes: Option<u64>,
    /// Keep full segment routing summaries resident after open.
    ///
    /// Defaults to `false`: search resolves segments from persisted routing
    /// pages, keeping resident memory near zero regardless of index size. Set to
    /// `true` for small, hot indexes that fit comfortably in RAM and want to
    /// avoid routing-page reads.
    pub resident_routing: bool,
    /// Optional budget for an in-memory decoded-segment cache, shared by all
    /// searches on this handle. When set, concurrent queries that touch the
    /// same segments share one decoded `Arc<Segment>` instead of each decoding
    /// its own copy, so peak memory tracks this budget rather than the number
    /// of concurrent readers. `None` disables the cache (decode per query).
    pub segment_cache_max_bytes: Option<u64>,
    /// Byte cap for shared decoded global-cell graphs. Graph objects must also
    /// be present in `cache_dir`; this cap controls only their read-only RAM
    /// representation. Zero disables retention while single-flight still
    /// prevents concurrent duplicate decodes.
    pub global_cell_graph_cache_max_bytes: u64,
    /// Byte cap for decoded immutable tombstone pages shared by all callers.
    /// The persisted overlay may grow with the corpus, while process memory
    /// remains independent of its total size. Zero disables retention.
    pub tombstone_page_cache_max_bytes: u64,
    /// Byte cap for decoded BM25 MVCC statistics-delta pages shared by all
    /// callers. Zero disables retention; overlapping reads are still
    /// coalesced.
    pub bm25_stats_page_cache_max_bytes: u64,
    /// Byte cap for recently decoded sparse/BM25 postings row groups shared by
    /// all callers. Zero disables retention; overlapping loads are still
    /// coalesced. The default is a corpus-size-independent 32 MiB window.
    pub lexical_run_cache_max_bytes: u64,
    /// Byte cap for recently decoded sparse/BM25 global term pages. Zero
    /// disables retention; overlapping loads are still coalesced. The default
    /// is a corpus-size-independent 32 MiB window.
    pub lexical_term_page_cache_max_bytes: u64,
    /// Byte cap for recently decoded immutable late-interaction Arrow record
    /// batches. Zero disables retention; overlapping callers still share one
    /// range read and decode through single-flight.
    pub late_interaction_batch_cache_max_bytes: u64,
    /// Eagerly load every active decoded segment into RAM before open returns.
    /// Graph-enabled indexes also decode and validate each immutable graph into
    /// the same byte-accounted cache entry.
    ///
    /// Preload also makes routing summaries resident, overriding
    /// `resident_routing: false`. It requires the decoded-segment cache. When
    /// `segment_cache_max_bytes` is `None`, the cache uses the effective RAM
    /// budget (512 MiB by default); only explicitly disabling both persisted
    /// and runtime RAM bounds permits an unbounded research preload. Entries
    /// remain evictable, and [`WarmReport::coverage_complete`] reports whether
    /// the complete snapshot actually fits.
    pub preload: bool,
    /// Optional cap on how many searches run their decode/score phase at once.
    /// With `Some(n)`, additional concurrent searches wait for a permit, so
    /// peak working memory scales with `n` rather than the caller thread count.
    /// `None` leaves search concurrency unbounded.
    pub max_concurrent_searches: Option<usize>,
    /// Optional process-local cap on active cell payload decodes.
    ///
    /// Per-query prefetch width remains a latency knob, while this shared gate
    /// bounds the decode working set across all admitted queries. `None` is for
    /// an explicitly measured research ceiling only.
    pub max_concurrent_cell_decodes: Option<usize>,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            cache_dir: None,
            cache_max_bytes: None,
            ram_budget_bytes: Some(DEFAULT_RAM_BUDGET_BYTES),
            resident_routing: false,
            segment_cache_max_bytes: None,
            global_cell_graph_cache_max_bytes: DEFAULT_GLOBAL_CELL_GRAPH_CACHE_BYTES,
            tombstone_page_cache_max_bytes: DEFAULT_TOMBSTONE_PAGE_CACHE_BYTES,
            bm25_stats_page_cache_max_bytes: DEFAULT_BM25_STATS_PAGE_CACHE_BYTES,
            lexical_run_cache_max_bytes: DEFAULT_LEXICAL_RUN_CACHE_BYTES,
            lexical_term_page_cache_max_bytes: DEFAULT_LEXICAL_TERM_PAGE_CACHE_BYTES,
            late_interaction_batch_cache_max_bytes: DEFAULT_LATE_INTERACTION_BATCH_CACHE_BYTES,
            preload: false,
            max_concurrent_searches: Some(DEFAULT_MAX_CONCURRENT_SEARCHES),
            max_concurrent_cell_decodes: Some(DEFAULT_MAX_CONCURRENT_CELL_DECODES),
        }
    }
}

/// Result of eagerly loading active decoded segments into RAM.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WarmReport {
    /// Active segments newly decoded and inserted into the RAM cache.
    pub segments_loaded: usize,
    /// Total active segments in the warmed manifest snapshot.
    pub segments_total: usize,
    /// Active decoded segments still resident after warming and LRU eviction.
    pub segments_resident: usize,
    /// Active decoded graphs still resident after warming and LRU eviction.
    pub graphs_resident: usize,
    /// Whether every active segment, and every required graph, is resident.
    pub coverage_complete: bool,
    /// Actual byte-accounted decoded segment and graph data still resident.
    pub bytes_resident: u64,
}

/// A BORSUK index handle.
#[derive(Clone)]
pub struct BorsukIndex {
    collection_storage: Storage,
    storage: Storage,
    manifest: Manifest,
    manifest_reference: CollectionManifestRef,
    collection_snapshot: Option<LoadedCollectionSnapshot>,
    /// Stable for this handle lifetime; clones retain the same lane identity.
    writer_id: Vec<u8>,
    /// Double-collected complete committed transactions pinned by this reader
    /// snapshot.
    cell_wal_snapshot: Vec<CommittedCellWalTransaction>,
    /// Unstable double-collect attempts before the current cell-WAL snapshot.
    cell_wal_snapshot_retries: Arc<AtomicUsize>,
    /// Claim-shard versions that correspond exactly to this handle's WAL
    /// snapshot. A matching version proves that no other explicit-ID writer
    /// committed through that shard since this handle last advanced.
    cell_wal_claim_checkpoint: CellWalClaimCheckpoint,
    named: BTreeMap<String, BorsukIndex>,
    tokenizer: Arc<dyn Tokenizer>,
    runtime_ram_budget_bytes: Option<u64>,
    /// Shared weighted cap for transient sparse/text decodes. Unlike the
    /// per-query wave size, this remains safe when heterogeneous queries and
    /// modalities overlap across callers.
    lexical_admission: Option<Arc<ByteAdmissionGate>>,
    segment_cache: Arc<OnceLock<Arc<DecodedSegmentCache>>>,
    resident_routing_summaries: ResidentRoutingSummaries,
    /// Lazily built HNSW coarse quantizer over cell centroids — the IVF probe
    /// list. Navigates to the nprobe nearest cells in ~O(log cells) instead of
    /// a flat centroid scan; rebuilt whenever the manifest version changes.
    coarse_quantizer: CoarseQuantizerCache,
    /// Lazily loaded PERSISTED coarse quantizer, keyed by the manifest version's
    /// `quantizer_ref` checksum. Loaded with one object read on a COLD/paged
    /// query (no resident summaries) when the active manifest references a
    /// persisted quantizer object, then reused across queries at that version —
    /// so cold approximate search routes through the same IVF probe list the
    /// warm path uses, without pulling every centroid resident.
    persisted_quantizer: PersistedQuantizerCache,
    /// Compact global/coarse product codebooks and immutable chunk references,
    /// loaded during open and shared read-only by every admitted query. The
    /// corpus-sized product-code payloads remain paged objects; row locations
    /// use the same segment ordinals as the summaries.
    resident_global_pq: ResidentGlobalPqCache,
    /// Compact term-range roots loaded before serving; postings remain paged.
    resident_lexical_roots: ResidentLexicalRoots,
    admission: Option<Arc<AdmissionGate>>,
    decode_admission: Option<Arc<AdmissionGate>>,
    /// Global cap for exact sidecar range reads issued by resident global-PQ
    /// reranks. Concurrent callers share this cap.
    global_pq_rerank_admission: Arc<AdmissionGate>,
    /// Same-cell reads shared only while they overlap. Unlike `segment_cache`,
    /// this never retains decoded cells after the active callers release them.
    inflight_segment_reads: Arc<InFlightSegmentReads>,
    /// Same-graph reads shared only while they overlap. Traversal state remains
    /// query-local; callers share only immutable decoded adjacency storage.
    inflight_graph_reads: Arc<InFlightGraphReads>,
    /// Overlapping callers share immutable decoded lexical row groups.
    inflight_lexical_reads: Arc<InFlightReads<LexicalRunRead>>,
    /// A bounded retention window closes the gap between staggered callers
    /// without retaining the corpus-sized postings set.
    decoded_lexical_reads: Arc<DecodedObjectCache<LexicalRunRead>>,
    /// Same single-flight policy for bounded global term pages.
    inflight_lexical_pages: Arc<InFlightReads<LexicalTermPage>>,
    /// Byte-bounded retention of recently used global term pages.
    decoded_lexical_pages: Arc<DecodedObjectCache<LexicalTermPage>>,
    /// Global cell graphs are shared read-only across callers under one byte
    /// cap. Only cells whose graph objects are already in the local disk cache
    /// may enter this cache; storage misses continue through the scan path.
    decoded_global_cell_graphs: Arc<DecodedObjectCache<GlobalCellGraph>>,
    inflight_global_cell_graph_reads: Arc<InFlightReads<GlobalCellGraph>>,
    /// Byte-bounded decoded immutable tombstone pages. Point lookups select one
    /// hash bucket plus the bounded foreground frontier; cloned handles share
    /// the same read-only pages.
    tombstone_cache: TombstoneCache,
    /// Single-flight and byte-bounded decoded BM25 MVCC correction pages.
    inflight_bm25_stats_pages: Arc<InFlightReads<Bm25StatsPage>>,
    decoded_bm25_stats_pages: Arc<DecodedObjectCache<Bm25StatsPage>>,
    /// Parsed standard Arrow IPC footer and record-batch table, cached per
    /// segment checksum. The process-wide LRU is byte-capped, so diverse
    /// queries over a 100M-vector index cannot eventually retain every cell's
    /// footer. Segment checksums are content-addressed, so retained entries are
    /// always valid for their checksum.
    vector_sidecar_indexes: Arc<Mutex<SidecarIndexCache>>,
    /// Parsed nested Arrow IPC footers for late-interaction entity matrices.
    /// Keys include field name and immutable segment checksum.
    late_interaction_sidecar_indexes: Arc<Mutex<LateInteractionSidecarIndexCache>>,
    /// Same immutable Arrow batch is decoded once for overlapping callers.
    inflight_late_interaction_batches: Arc<InFlightReads<LateInteractionBatch>>,
    /// Small corpus-independent reuse window for staggered callers.
    decoded_late_interaction_batches: Arc<DecodedObjectCache<LateInteractionBatch>>,
    /// Decoded, un-flushed WAL tail records, cached by the frontier's ordered
    /// object checksums. Empty when the WAL is disabled or the frontier is
    /// empty, in which case reads pay zero WAL I/O. Reloaded whenever the
    /// published frontier changes; each WAL object is content-addressed, so a
    /// cached entry is always valid for its key.
    wal_tail_cache: WalTailCache,
}

/// The ordered frontier identity (each entry's content checksum) a decoded WAL
/// tail was loaded from. Reused as a cache key so an unchanged frontier is not
/// re-read.
type WalFrontierKey = Vec<String>;

/// Lazily decoded WAL tail keyed by its [`WalFrontierKey`].
type WalTailCache = Arc<Mutex<Option<(WalFrontierKey, Arc<Vec<VectorRecord>>)>>>;
type ResidentLexicalRoots = Arc<Mutex<Option<(u64, BTreeMap<(String, String), Arc<LexicalRoot>>)>>>;

#[derive(Clone, Copy)]
struct CellWalAppendTransaction<'a> {
    id: &'a str,
    claimed: bool,
}

#[derive(Debug)]
struct LateInteractionBatch {
    rows: HashMap<usize, Option<crate::LateInteractionVector>>,
}

fn decoded_late_interaction_batch_bytes(batch: &LateInteractionBatch) -> u64 {
    let bytes = std::mem::size_of::<LateInteractionBatch>()
        .saturating_add(batch.rows.capacity().saturating_mul(std::mem::size_of::<(
            usize,
            Option<crate::LateInteractionVector>,
        )>()))
        .saturating_add(
            batch
                .rows
                .values()
                .filter_map(Option::as_ref)
                .map(crate::LateInteractionVector::resident_bytes)
                .sum::<usize>(),
        );
    u64::try_from(bytes).unwrap_or(u64::MAX)
}

impl fmt::Debug for BorsukIndex {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BorsukIndex")
            .field("storage", &self.storage)
            .field("manifest", &self.manifest)
            .field("named", &self.named.keys().collect::<Vec<_>>())
            .field("tokenizer", &self.tokenizer.fingerprint())
            .field("runtime_ram_budget_bytes", &self.runtime_ram_budget_bytes)
            .field("segment_cache", &self.segment_cache.get())
            .field(
                "resident_routing_summaries",
                &self.resident_routing_summaries.lock().map(|value| {
                    value
                        .as_ref()
                        .map(|(version, summaries)| (*version, summaries.len()))
                }),
            )
            .field(
                "coarse_quantizer",
                &self.coarse_quantizer.lock().map(|value| {
                    value
                        .as_ref()
                        .map(|(version, _, summaries)| (*version, summaries.len()))
                }),
            )
            .field("admission", &self.admission)
            .field("tombstone_cache", &self.tombstone_cache)
            .field("cell_wal_transactions", &self.cell_wal_snapshot.len())
            .finish()
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct StatsTotals {
    routing_leaf_pages: usize,
    routing_pages: usize,
    segments: usize,
    records: usize,
    segment_bytes: u64,
    vector_bytes: u64,
    graph_bytes: u64,
    sparse_encoded_vectors: usize,
    dense_encoded_vectors: usize,
}

/// Lazily loaded deleted-id set keyed by the active tombstone checksum.
/// Tombstone overlay: `id -> minimum visible generation`. A stored record of
/// that id is suppressed when its generation is below the mapped value (a plain
/// delete maps it above every stored generation; an upsert maps it to the newest
/// generation, suppressing the older copies).
type TombstoneOverlay = HashMap<Vec<u8>, u64>;

/// Byte-bounded immutable tombstone pages keyed by content checksum. Point
/// lookups load only bloom-matching runs; full overlay materialization is
/// reserved for explicit maintenance.
type TombstoneCache = Arc<DecodedObjectCache<TombstoneOverlay>>;
type Bm25StatsPage = Vec<(u32, i64)>;
type TextTermFrequencies = Vec<(u32, u32)>;

#[derive(Debug)]
struct LiveDeleteRecord {
    generation: u64,
    text_terms: Option<TextTermFrequencies>,
    persisted: bool,
}

fn decoded_tombstone_overlay_bytes(overlay: &TombstoneOverlay) -> u64 {
    let entries = overlay.iter().fold(0_u64, |total, (id, _)| {
        total.saturating_add(
            (std::mem::size_of::<Vec<u8>>() + std::mem::size_of::<u64>() + id.capacity() + 16)
                as u64,
        )
    });
    entries.saturating_add(
        overlay
            .capacity()
            .saturating_mul(std::mem::size_of::<usize>())
            .saturating_mul(2) as u64,
    )
}

fn tombstone_bucket(id: &[u8]) -> u16 {
    let digest = blake3::hash(id);
    let bytes = digest.as_bytes();
    u16::from_le_bytes([bytes[0], bytes[1]]) % TOMBSTONE_BUCKETS
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Bm25StatsDelta {
    document_count: i64,
    total_document_length: i64,
    document_frequencies: BTreeMap<u32, i64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct CellWalMutationMetadata {
    new_tombstone_ids: u64,
    next_generated_id_floor: u64,
    bm25_stats_delta: Option<Bm25StatsDeltaRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct CellWalTombstoneMetadata {
    id_bloom: Vec<u8>,
    created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct CellWalIdDirectoryEntry {
    id: Vec<u8>,
    owner: LogicalCellId,
    generation: u64,
    deleted: bool,
}

fn finish_packed_index_control(mut bytes: Vec<u8>) -> Vec<u8> {
    let checksum = blake3::hash(&bytes);
    bytes.extend_from_slice(checksum.as_bytes());
    bytes
}

fn write_packed_index_bytes(bytes: &mut Vec<u8>, value: &[u8], label: &str) -> Result<()> {
    bytes.extend_from_slice(
        &u32::try_from(value.len())
            .map_err(|_| {
                BorsukError::InvalidStorage(format!(
                    "packed index control {label} exceeds u32 bytes"
                ))
            })?
            .to_le_bytes(),
    );
    bytes.extend_from_slice(value);
    Ok(())
}

fn write_packed_index_string(bytes: &mut Vec<u8>, value: &str, label: &str) -> Result<()> {
    write_packed_index_bytes(bytes, value.as_bytes(), label)
}

fn cell_wal_id_directory_bytes(entries: &[CellWalIdDirectoryEntry]) -> Result<Vec<u8>> {
    if entries
        .windows(2)
        .any(|pair| pair[0].id.as_slice() >= pair[1].id.as_slice())
    {
        return Err(BorsukError::InvalidStorage(
            "cell WAL ID-directory entries must be strictly sorted by id".to_string(),
        ));
    }
    let mut bytes = Vec::new();
    bytes.extend_from_slice(ID_DIRECTORY_MAGIC);
    bytes.push(PACKED_INDEX_CONTROL_VERSION);
    bytes.extend_from_slice(
        &u32::try_from(entries.len())
            .map_err(|_| {
                BorsukError::InvalidStorage(
                    "cell WAL ID-directory run contains too many entries".to_string(),
                )
            })?
            .to_le_bytes(),
    );
    for entry in entries {
        if entry.id.is_empty() {
            return Err(BorsukError::InvalidStorage(
                "cell WAL ID-directory entry has an empty id".to_string(),
            ));
        }
        write_packed_index_bytes(&mut bytes, &entry.id, "ID-directory id")?;
        bytes.extend_from_slice(&entry.owner.routing_epoch.to_le_bytes());
        bytes.extend_from_slice(&entry.owner.cell_ordinal.to_le_bytes());
        bytes.extend_from_slice(&entry.generation.to_le_bytes());
        bytes.push(u8::from(entry.deleted));
    }
    Ok(finish_packed_index_control(bytes))
}

fn cell_wal_id_directory_from_slice(
    bytes: &[u8],
    path: &str,
) -> Result<Vec<CellWalIdDirectoryEntry>> {
    let payload = checked_packed_index_control_payload(bytes, ID_DIRECTORY_MAGIC, path)?;
    let mut cursor = ID_DIRECTORY_MAGIC.len() + 1;
    let count = read_packed_index_u32(payload, &mut cursor, path)? as usize;
    let mut entries = Vec::with_capacity(count.min(1_024));
    for _ in 0..count {
        let id_len = read_packed_index_u32(payload, &mut cursor, path)? as usize;
        let id = take_packed_index_bytes(payload, &mut cursor, id_len, path)?.to_vec();
        if id.is_empty() {
            return Err(BorsukError::InvalidStorage(format!(
                "cell WAL ID-directory run `{path}` contains an empty id"
            )));
        }
        let routing_epoch = read_packed_index_u64(payload, &mut cursor, path)?;
        let cell_ordinal = read_packed_index_u32(payload, &mut cursor, path)?;
        let generation = read_packed_index_u64(payload, &mut cursor, path)?;
        let deleted = match take_packed_index_bytes(payload, &mut cursor, 1, path)?[0] {
            0 => false,
            1 => true,
            value => {
                return Err(BorsukError::InvalidStorage(format!(
                    "cell WAL ID-directory run `{path}` has invalid deleted flag {value}"
                )));
            }
        };
        entries.push(CellWalIdDirectoryEntry {
            id,
            owner: LogicalCellId::new(routing_epoch, cell_ordinal),
            generation,
            deleted,
        });
    }
    if entries
        .windows(2)
        .any(|pair| pair[0].id.as_slice() >= pair[1].id.as_slice())
    {
        return Err(BorsukError::InvalidStorage(format!(
            "cell WAL ID-directory run `{path}` is not strictly sorted by id"
        )));
    }
    if cursor != payload.len() {
        return Err(BorsukError::InvalidStorage(format!(
            "packed index control object `{path}` contains trailing bytes"
        )));
    }
    Ok(entries)
}

fn coordination_counter_bytes(value: u64) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(
        COORDINATION_COUNTER_MAGIC.len() + 1 + 8 + PACKED_INDEX_CONTROL_CHECKSUM_LEN,
    );
    bytes.extend_from_slice(COORDINATION_COUNTER_MAGIC);
    bytes.push(PACKED_INDEX_CONTROL_VERSION);
    bytes.extend_from_slice(&value.to_le_bytes());
    finish_packed_index_control(bytes)
}

fn cell_wal_mutation_metadata_bytes(metadata: &CellWalMutationMetadata) -> Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(128);
    bytes.extend_from_slice(CELL_WAL_MUTATION_METADATA_MAGIC);
    bytes.push(PACKED_INDEX_CONTROL_VERSION);
    bytes.extend_from_slice(&metadata.new_tombstone_ids.to_le_bytes());
    bytes.extend_from_slice(&metadata.next_generated_id_floor.to_le_bytes());
    match &metadata.bm25_stats_delta {
        None => bytes.push(0),
        Some(delta) => {
            bytes.push(1);
            bytes.extend_from_slice(&delta.document_count_delta.to_le_bytes());
            bytes.extend_from_slice(&delta.total_document_length_delta.to_le_bytes());
            bytes.extend_from_slice(
                &u32::try_from(delta.pages.len())
                    .map_err(|_| {
                        BorsukError::InvalidStorage(
                            "cell WAL BM25 statistics delta contains too many pages".to_string(),
                        )
                    })?
                    .to_le_bytes(),
            );
            for page in &delta.pages {
                if page.path.is_empty()
                    || page.checksum.len() != 64
                    || !page.checksum.bytes().all(|byte| byte.is_ascii_hexdigit())
                    || page.first_term > page.last_term
                {
                    return Err(BorsukError::InvalidStorage(
                        "cell WAL BM25 statistics-delta page reference is invalid".to_string(),
                    ));
                }
                bytes.extend_from_slice(&page.first_term.to_le_bytes());
                bytes.extend_from_slice(&page.last_term.to_le_bytes());
                write_packed_index_string(&mut bytes, &page.path, "BM25 delta page path")?;
                write_packed_index_string(&mut bytes, &page.checksum, "BM25 delta page checksum")?;
                bytes.extend_from_slice(&page.encoded_bytes.to_le_bytes());
                bytes.extend_from_slice(&page.term_count.to_le_bytes());
            }
        }
    }
    Ok(finish_packed_index_control(bytes))
}

fn cell_wal_mutation_metadata_from_slice(
    bytes: &[u8],
    path: &str,
) -> Result<CellWalMutationMetadata> {
    let payload =
        checked_packed_index_control_payload(bytes, CELL_WAL_MUTATION_METADATA_MAGIC, path)?;
    let mut cursor = CELL_WAL_MUTATION_METADATA_MAGIC.len() + 1;
    let new_tombstone_ids = read_packed_index_u64(payload, &mut cursor, path)?;
    let next_generated_id_floor = read_packed_index_u64(payload, &mut cursor, path)?;
    let bm25_stats_delta = match take_packed_index_bytes(payload, &mut cursor, 1, path)?[0] {
        0 => None,
        1 => {
            let document_count_delta = read_packed_index_i64(payload, &mut cursor, path)?;
            let total_document_length_delta = read_packed_index_i64(payload, &mut cursor, path)?;
            let page_count = read_packed_index_u32(payload, &mut cursor, path)? as usize;
            let mut pages = Vec::with_capacity(page_count.min(1_024));
            for _ in 0..page_count {
                let first_term = read_packed_index_u32(payload, &mut cursor, path)?;
                let last_term = read_packed_index_u32(payload, &mut cursor, path)?;
                let page_path =
                    read_packed_index_string(payload, &mut cursor, path, "BM25 delta page path")?;
                let checksum = read_packed_index_string(
                    payload,
                    &mut cursor,
                    path,
                    "BM25 delta page checksum",
                )?;
                let encoded_bytes = read_packed_index_u64(payload, &mut cursor, path)?;
                let term_count = read_packed_index_u32(payload, &mut cursor, path)?;
                if page_path.is_empty()
                    || checksum.len() != 64
                    || !checksum.bytes().all(|byte| byte.is_ascii_hexdigit())
                    || first_term > last_term
                {
                    return Err(BorsukError::InvalidStorage(format!(
                        "cell WAL BM25 statistics-delta page reference in `{path}` is invalid"
                    )));
                }
                pages.push(Bm25StatsDeltaPageRef {
                    first_term,
                    last_term,
                    path: page_path,
                    checksum,
                    encoded_bytes,
                    term_count,
                });
            }
            Some(Bm25StatsDeltaRef {
                document_count_delta,
                total_document_length_delta,
                pages,
            })
        }
        value => {
            return Err(BorsukError::InvalidStorage(format!(
                "cell WAL mutation metadata `{path}` has invalid BM25 option tag {value}"
            )));
        }
    };
    if cursor != payload.len() {
        return Err(BorsukError::InvalidStorage(format!(
            "packed index control object `{path}` contains trailing bytes"
        )));
    }
    Ok(CellWalMutationMetadata {
        new_tombstone_ids,
        next_generated_id_floor,
        bm25_stats_delta,
    })
}

fn cell_wal_tombstone_metadata_bytes(metadata: &CellWalTombstoneMetadata) -> Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(64 + metadata.id_bloom.len());
    bytes.extend_from_slice(CELL_WAL_TOMBSTONE_METADATA_MAGIC);
    bytes.push(PACKED_INDEX_CONTROL_VERSION);
    bytes.extend_from_slice(&metadata.created_at.timestamp().to_le_bytes());
    bytes.extend_from_slice(&metadata.created_at.timestamp_subsec_nanos().to_le_bytes());
    write_packed_index_bytes(&mut bytes, &metadata.id_bloom, "tombstone ID bloom")?;
    Ok(finish_packed_index_control(bytes))
}

fn cell_wal_tombstone_metadata_from_slice(
    bytes: &[u8],
    path: &str,
) -> Result<CellWalTombstoneMetadata> {
    let payload =
        checked_packed_index_control_payload(bytes, CELL_WAL_TOMBSTONE_METADATA_MAGIC, path)?;
    let mut cursor = CELL_WAL_TOMBSTONE_METADATA_MAGIC.len() + 1;
    let seconds = read_packed_index_i64(payload, &mut cursor, path)?;
    let nanos = read_packed_index_u32(payload, &mut cursor, path)?;
    let id_bloom = read_packed_index_bytes(payload, &mut cursor, path, "tombstone ID bloom")?;
    if cursor != payload.len() {
        return Err(BorsukError::InvalidStorage(format!(
            "packed index control object `{path}` contains trailing bytes"
        )));
    }
    let created_at = DateTime::<Utc>::from_timestamp(seconds, nanos).ok_or_else(|| {
        BorsukError::InvalidStorage(format!(
            "cell WAL tombstone metadata `{path}` has an invalid timestamp"
        ))
    })?;
    Ok(CellWalTombstoneMetadata {
        id_bloom,
        created_at,
    })
}

fn coordination_counter_from_slice(bytes: &[u8], path: &str) -> Result<u64> {
    let payload = checked_packed_index_control_payload(bytes, COORDINATION_COUNTER_MAGIC, path)?;
    if payload.len() != COORDINATION_COUNTER_MAGIC.len() + 1 + 8 {
        return Err(BorsukError::InvalidStorage(format!(
            "coordination counter `{path}` has invalid packed length"
        )));
    }
    let mut cursor = COORDINATION_COUNTER_MAGIC.len() + 1;
    read_packed_index_u64(payload, &mut cursor, path)
}

fn checked_packed_index_control_payload<'a>(
    bytes: &'a [u8],
    magic: &[u8; 4],
    path: &str,
) -> Result<&'a [u8]> {
    if bytes.len() < magic.len() + 1 + PACKED_INDEX_CONTROL_CHECKSUM_LEN {
        return Err(BorsukError::InvalidStorage(format!(
            "packed index control object `{path}` is truncated"
        )));
    }
    let payload_len = bytes.len() - PACKED_INDEX_CONTROL_CHECKSUM_LEN;
    let (payload, stored_checksum) = bytes.split_at(payload_len);
    if stored_checksum != blake3::hash(payload).as_bytes() {
        return Err(BorsukError::InvalidStorage(format!(
            "packed index control object `{path}` checksum mismatch"
        )));
    }
    if payload.get(..magic.len()) != Some(magic.as_slice()) {
        return Err(BorsukError::InvalidStorage(format!(
            "packed index control object `{path}` has invalid magic"
        )));
    }
    let version = payload[magic.len()];
    if version != PACKED_INDEX_CONTROL_VERSION {
        return Err(BorsukError::InvalidStorage(format!(
            "packed index control object `{path}` uses unsupported version {version}"
        )));
    }
    Ok(payload)
}

fn take_packed_index_bytes<'a>(
    bytes: &'a [u8],
    cursor: &mut usize,
    length: usize,
    path: &str,
) -> Result<&'a [u8]> {
    let end = cursor.checked_add(length).ok_or_else(|| {
        BorsukError::InvalidStorage(format!(
            "packed index control object `{path}` length overflows usize"
        ))
    })?;
    let value = bytes.get(*cursor..end).ok_or_else(|| {
        BorsukError::InvalidStorage(format!("packed index control object `{path}` is truncated"))
    })?;
    *cursor = end;
    Ok(value)
}

fn read_packed_index_u32(bytes: &[u8], cursor: &mut usize, path: &str) -> Result<u32> {
    let bytes: [u8; 4] = take_packed_index_bytes(bytes, cursor, 4, path)?
        .try_into()
        .expect("packed index reader returned four bytes");
    Ok(u32::from_le_bytes(bytes))
}

fn read_packed_index_u64(bytes: &[u8], cursor: &mut usize, path: &str) -> Result<u64> {
    let bytes: [u8; 8] = take_packed_index_bytes(bytes, cursor, 8, path)?
        .try_into()
        .expect("packed index reader returned eight bytes");
    Ok(u64::from_le_bytes(bytes))
}

fn read_packed_index_i64(bytes: &[u8], cursor: &mut usize, path: &str) -> Result<i64> {
    let bytes: [u8; 8] = take_packed_index_bytes(bytes, cursor, 8, path)?
        .try_into()
        .expect("packed index reader returned eight bytes");
    Ok(i64::from_le_bytes(bytes))
}

fn read_packed_index_bytes(
    bytes: &[u8],
    cursor: &mut usize,
    path: &str,
    label: &str,
) -> Result<Vec<u8>> {
    let length = read_packed_index_u32(bytes, cursor, path)? as usize;
    take_packed_index_bytes(bytes, cursor, length, path)
        .map(<[u8]>::to_vec)
        .map_err(|error| {
            BorsukError::InvalidStorage(format!(
                "packed index control {label} in `{path}` is invalid: {error}"
            ))
        })
}

fn read_packed_index_string(
    bytes: &[u8],
    cursor: &mut usize,
    path: &str,
    label: &str,
) -> Result<String> {
    String::from_utf8(read_packed_index_bytes(bytes, cursor, path, label)?).map_err(|_| {
        BorsukError::InvalidStorage(format!(
            "packed index control {label} in `{path}` is not valid UTF-8"
        ))
    })
}

impl Bm25StatsDelta {
    fn suppress_document(&mut self, terms: &[(u32, u32)]) -> Result<()> {
        self.document_count = self.document_count.checked_sub(1).ok_or_else(|| {
            BorsukError::InvalidStorage("BM25 document-count delta underflow".to_string())
        })?;
        let document_length = terms.iter().try_fold(0_i64, |total, (_, tf)| {
            total.checked_add(i64::from(*tf)).ok_or_else(|| {
                BorsukError::InvalidStorage("BM25 document length exceeds i64".to_string())
            })
        })?;
        self.total_document_length = self
            .total_document_length
            .checked_sub(document_length)
            .ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "BM25 total-document-length delta underflow".to_string(),
                )
            })?;
        for (term, _) in terms {
            let entry = self.document_frequencies.entry(*term).or_default();
            *entry = entry.checked_sub(1).ok_or_else(|| {
                BorsukError::InvalidStorage("BM25 document-frequency delta underflow".to_string())
            })?;
        }
        self.document_frequencies.retain(|_, delta| *delta != 0);
        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.document_count == 0
            && self.total_document_length == 0
            && self.document_frequencies.is_empty()
    }
}

#[derive(Debug, Default)]
struct LexicalShardTermMutation {
    document_frequency_delta: i64,
    additions: Vec<crate::lexical_root::LexicalTermBlock>,
    removal_segment_key: Option<String>,
}

/// Resident active summaries keyed by the manifest version they describe.
type ResidentRoutingSummaries = Arc<Mutex<Option<(u64, Arc<Vec<SegmentSummary>>)>>>;

/// The coarse-quantizer HNSW over cell centroids plus the summaries it indexes
/// (node `i` is `summaries[i]`).
type ResolvedCoarseQuantizer = (Arc<CentroidHnsw>, Arc<Vec<SegmentSummary>>);

/// [`ResolvedCoarseQuantizer`] keyed by the manifest version it describes.
type CoarseQuantizerCache = Arc<Mutex<Option<(u64, Arc<CentroidHnsw>, Arc<Vec<SegmentSummary>>)>>>;

/// A persisted quantizer loaded from storage, keyed by the object checksum it
/// was loaded from (so a manifest that swaps in a new quantizer object reloads).
type PersistedQuantizerCache =
    Arc<Mutex<Option<(String, Arc<CentroidHnsw>, Arc<Vec<SegmentSummary>>)>>>;

type ResidentGlobalPqCache = Arc<
    Mutex<
        Option<(
            u64,
            String,
            Arc<ResidentGlobalPq>,
            Arc<Vec<SegmentSummary>>,
            Arc<Vec<SegmentSummary>>,
        )>,
    >,
>;
/// Resident immutable base plus the materialized segments not covered by it.
type LoadedResidentGlobalPq = (
    Arc<ResidentGlobalPq>,
    Arc<Vec<SegmentSummary>>,
    Arc<Vec<SegmentSummary>>,
);

struct SidecarIndexCache {
    entries: HashMap<String, (Arc<crate::arrow_vector_sidecar::SidecarIndex>, usize)>,
    order: VecDeque<String>,
    bytes: usize,
    max_bytes: usize,
}

impl Default for SidecarIndexCache {
    fn default() -> Self {
        Self::with_max_bytes(DEFAULT_SIDECAR_INDEX_CACHE_BYTES as usize)
    }
}

impl SidecarIndexCache {
    fn with_max_bytes(max_bytes: usize) -> Self {
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
            bytes: 0,
            max_bytes,
        }
    }

    fn get(&mut self, checksum: &str) -> Option<Arc<crate::arrow_vector_sidecar::SidecarIndex>> {
        let index = Arc::clone(&self.entries.get(checksum)?.0);
        self.order.retain(|key| key != checksum);
        self.order.push_back(checksum.to_string());
        Some(index)
    }

    fn insert(&mut self, checksum: String, index: Arc<crate::arrow_vector_sidecar::SidecarIndex>) {
        let bytes = index.resident_bytes();
        if let Some((_, previous)) = self.entries.remove(&checksum) {
            self.bytes = self.bytes.saturating_sub(previous);
            self.order.retain(|key| key != &checksum);
        }
        if bytes > self.max_bytes {
            return;
        }
        self.bytes = self.bytes.saturating_add(bytes);
        self.order.push_back(checksum.clone());
        self.entries.insert(checksum, (index, bytes));
        while self.bytes > self.max_bytes {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            if let Some((_, removed)) = self.entries.remove(&oldest) {
                self.bytes = self.bytes.saturating_sub(removed);
            }
        }
    }
}

struct LateInteractionSidecarIndexCache {
    entries: HashMap<String, (Arc<crate::late_interaction_sidecar::SidecarIndex>, usize)>,
    order: VecDeque<String>,
    bytes: usize,
    max_bytes: usize,
}

impl Default for LateInteractionSidecarIndexCache {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
            bytes: 0,
            max_bytes: DEFAULT_SIDECAR_INDEX_CACHE_BYTES as usize,
        }
    }
}

impl LateInteractionSidecarIndexCache {
    fn get(&mut self, key: &str) -> Option<Arc<crate::late_interaction_sidecar::SidecarIndex>> {
        let index = Arc::clone(&self.entries.get(key)?.0);
        self.order.retain(|value| value != key);
        self.order.push_back(key.to_string());
        Some(index)
    }

    fn insert(&mut self, key: String, index: Arc<crate::late_interaction_sidecar::SidecarIndex>) {
        let bytes = index.resident_bytes();
        if let Some((_, previous)) = self.entries.remove(&key) {
            self.bytes = self.bytes.saturating_sub(previous);
            self.order.retain(|value| value != &key);
        }
        if bytes > self.max_bytes {
            return;
        }
        self.bytes = self.bytes.saturating_add(bytes);
        self.order.push_back(key.clone());
        self.entries.insert(key, (index, bytes));
        while self.bytes > self.max_bytes {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            if let Some((_, removed)) = self.entries.remove(&oldest) {
                self.bytes = self.bytes.saturating_sub(removed);
            }
        }
    }
}

impl BorsukIndex {
    /// Create a new empty index and publish its first manifest.
    pub fn create(config: IndexConfig) -> Result<Self> {
        Self::create_with_cache(config, None)
    }

    /// Create a new empty index with an opt-in write-ahead log.
    ///
    /// With an enabled [`WalConfig`], small `add`/`upsert` batches are appended
    /// to an immutable WAL object and their frontier is published in the same
    /// atomic manifest swap — cutting per-`add` latency by skipping the
    /// PQ/graph/segment build — and are flushed into a real segment once the
    /// accumulated tail crosses the configured threshold. A disabled
    /// [`WalConfig`] is exactly equivalent to [`BorsukIndex::create`].
    pub fn create_with_wal(config: IndexConfig, wal: WalConfig) -> Result<Self> {
        let storage = Storage::from_uri(&config.uri)?;
        Self::create_with_storage_and_wal(
            config,
            storage,
            DEFAULT_ROUTING_PAGE_FANOUT,
            LOCAL_GRAPH_NEIGHBORS,
            wal,
        )
    }

    /// Create a new empty index with an explicit leaf-search capability fixed at
    /// creation and persisted in the manifest.
    ///
    /// [`LeafCapability::PqScanOnly`] skips per-segment graph construction on
    /// every write (ingest, WAL flush, and compaction), so a scan-only workload
    /// never pays to build a graph it will not read. A search that then requests
    /// a graph-backed leaf mode (`Graph`/`VamanaPq`/`Hybrid`) returns
    /// [`BorsukError::LeafModeNotConfigured`] rather than silently degrading.
    /// [`LeafCapability::GraphEnabled`] is the explicit opt-in for graph-backed
    /// experimental modes; ordinary [`BorsukIndex::create`] is graph-free.
    pub fn create_with_leaf_capability(
        config: IndexConfig,
        leaf_capability: LeafCapability,
    ) -> Result<Self> {
        let storage = Storage::from_uri(&config.uri)?;
        Self::create_with_storage_wal_and_capability(
            config,
            storage,
            DEFAULT_ROUTING_PAGE_FANOUT,
            LOCAL_GRAPH_NEIGHBORS,
            WalConfig::default(),
            leaf_capability,
        )
    }

    /// Create a new empty index with an explicit leaf-search capability and an
    /// opt-in write-ahead log. See [`BorsukIndex::create_with_leaf_capability`]
    /// and [`BorsukIndex::create_with_wal`].
    pub fn create_with_wal_and_leaf_capability(
        config: IndexConfig,
        wal: WalConfig,
        leaf_capability: LeafCapability,
    ) -> Result<Self> {
        let storage = Storage::from_uri(&config.uri)?;
        Self::create_with_storage_wal_and_capability(
            config,
            storage,
            DEFAULT_ROUTING_PAGE_FANOUT,
            LOCAL_GRAPH_NEIGHBORS,
            wal,
            leaf_capability,
        )
    }

    /// Create a new empty index with explicit typed BUILD-tuning knobs
    /// ([`BuildConfig`]) fixed at creation and persisted in the manifest.
    ///
    /// The headline knob is [`SidecarCompression`](crate::SidecarCompression):
    /// standard Arrow IPC ZSTD
    /// reduces exact-vector storage, while
    /// [`SidecarCompression::Uncompressed`](crate::SidecarCompression::Uncompressed)
    /// avoids buffer compression for the fastest build. The k-means sampling
    /// knobs trade a little cell quality for a large clustering speedup; rerank
    /// keeps recall exact regardless.
    /// [`BuildConfig::default`] is exactly equivalent to [`BorsukIndex::create`].
    pub fn create_with_build_config(
        config: IndexConfig,
        build_config: BuildConfig,
    ) -> Result<Self> {
        let storage = Storage::from_uri(&config.uri)?;
        Self::create_with_storage_wal_capability_and_build(
            config,
            storage,
            DEFAULT_ROUTING_PAGE_FANOUT,
            LOCAL_GRAPH_NEIGHBORS,
            WalConfig::default(),
            LeafCapability::PqScanOnly,
            build_config,
        )
    }

    /// Create a new empty index with explicit BUILD-tuning knobs, an opt-in WAL,
    /// and a leaf-search capability. See [`BorsukIndex::create_with_build_config`],
    /// [`BorsukIndex::create_with_wal`], and
    /// [`BorsukIndex::create_with_leaf_capability`].
    pub fn create_with_wal_capability_and_build_config(
        config: IndexConfig,
        wal: WalConfig,
        leaf_capability: LeafCapability,
        build_config: BuildConfig,
    ) -> Result<Self> {
        let storage = Storage::from_uri(&config.uri)?;
        Self::create_with_storage_wal_capability_and_build(
            config,
            storage,
            DEFAULT_ROUTING_PAGE_FANOUT,
            LOCAL_GRAPH_NEIGHBORS,
            wal,
            leaf_capability,
            build_config,
        )
    }

    #[doc(hidden)]
    pub fn create_with_object_store_and_build_config(
        store: Arc<dyn ObjectStore>,
        config: IndexConfig,
        build_config: BuildConfig,
    ) -> Result<Self> {
        // Test seam: integration tests can share or wrap an ObjectStore without URI parsing.
        let storage = Storage::from_object_store(config.uri.clone(), store)?;
        Self::create_with_storage_wal_capability_and_build(
            config,
            storage,
            DEFAULT_ROUTING_PAGE_FANOUT,
            LOCAL_GRAPH_NEIGHBORS,
            WalConfig::default(),
            LeafCapability::PqScanOnly,
            build_config,
        )
    }

    /// The typed BUILD-tuning knobs this index was created with.
    #[must_use]
    pub fn build_config(&self) -> &BuildConfig {
        &self.manifest.build_config
    }

    /// Finalize a freshly bulk-loaded index without rewriting its bounded
    /// ingest segments. This trains and publishes the configured global scan
    /// artifact in two bounded passes over the active segments. Graph-capable
    /// indexes retain this artifact as their storage-backed fallback; graph
    /// execution is considered only after a complete local snapshot exists.
    ///
    /// Use full compaction instead when reclustering the exact-vector sidecars
    /// is worth a larger build working set in exchange for fewer rerank GETs.
    pub fn finish_bulk_load(&mut self) -> Result<()> {
        self.flush()?;
        let summaries = self.active_segment_summaries()?;
        self.refresh_resident_global_pq_from_summaries(&summaries)?;
        self.finalize_logical_cell_topology(&summaries)
    }

    /// Freeze epoch-one logical write cells from the freshly built routing
    /// centroids. Physical segment replacement later does not rewrite this
    /// catalog; only an explicit routing-epoch rebuild may do so.
    fn finalize_logical_cell_topology(&mut self, summaries: &[SegmentSummary]) -> Result<()> {
        if summaries.is_empty() || !self.manifest.logical_cell_centroids.is_empty() {
            return Ok(());
        }
        let logical_cells = summaries
            .iter()
            .enumerate()
            .map(|(ordinal, _)| {
                let ordinal = u32::try_from(ordinal).map_err(|_| {
                    BorsukError::InvalidStorage(
                        "logical cell count exceeds the u32 catalog limit".to_string(),
                    )
                })?;
                Ok(LogicalCellId::new(self.manifest.routing_epoch, ordinal))
            })
            .collect::<Result<Vec<_>>>()?;
        let centroids = summaries
            .iter()
            .map(|summary| summary.centroid.clone())
            .collect::<Vec<_>>();
        let previous = self.manifest.clone();
        let mut manifest = self.manifest.next_version();
        manifest.logical_cells = logical_cells;
        manifest.logical_cell_centroids = centroids;
        enforce_ram_budget(&manifest, self.runtime_ram_budget_bytes)?;
        self.manifest =
            self.publish_manifest_reusing_routing_pages_with_recovery(manifest, Some(&previous))?;
        Ok(())
    }

    #[doc(hidden)]
    pub fn create_with_object_store_and_leaf_capability(
        store: Arc<dyn ObjectStore>,
        config: IndexConfig,
        leaf_capability: LeafCapability,
    ) -> Result<Self> {
        // Test seam: integration tests can share or wrap an ObjectStore without URI parsing.
        let storage = Storage::from_object_store(config.uri.clone(), store)?;
        Self::create_with_storage_wal_and_capability(
            config,
            storage,
            DEFAULT_ROUTING_PAGE_FANOUT,
            LOCAL_GRAPH_NEIGHBORS,
            WalConfig::default(),
            leaf_capability,
        )
    }

    /// The leaf-search capability this index was created with.
    #[must_use]
    pub fn leaf_capability(&self) -> LeafCapability {
        self.manifest.leaf_capability
    }

    #[doc(hidden)]
    pub fn create_with_object_store_and_wal(
        store: Arc<dyn ObjectStore>,
        config: IndexConfig,
        wal: WalConfig,
    ) -> Result<Self> {
        // Test seam: integration tests can share or wrap an ObjectStore without URI parsing.
        let storage = Storage::from_object_store(config.uri.clone(), store)?;
        Self::create_with_storage_and_wal(
            config,
            storage,
            DEFAULT_ROUTING_PAGE_FANOUT,
            LOCAL_GRAPH_NEIGHBORS,
            wal,
        )
    }

    #[doc(hidden)]
    pub fn create_with_object_store_wal_and_leaf_capability(
        store: Arc<dyn ObjectStore>,
        config: IndexConfig,
        wal: WalConfig,
        leaf_capability: LeafCapability,
    ) -> Result<Self> {
        let storage = Storage::from_object_store(config.uri.clone(), store)?;
        Self::create_with_storage_wal_and_capability(
            config,
            storage,
            DEFAULT_ROUTING_PAGE_FANOUT,
            LOCAL_GRAPH_NEIGHBORS,
            wal,
            leaf_capability,
        )
    }

    /// Create a new empty index with an explicit routing page fanout.
    pub fn create_with_routing_page_fanout(
        config: IndexConfig,
        routing_page_fanout: usize,
    ) -> Result<Self> {
        Self::create_with_cache_routing_page_fanout_and_graph_neighbors(
            config,
            None,
            routing_page_fanout,
            LOCAL_GRAPH_NEIGHBORS,
        )
    }

    /// Create a new empty index with an explicit segment-local graph neighbor count.
    pub fn create_with_graph_neighbors(
        config: IndexConfig,
        graph_neighbors: usize,
    ) -> Result<Self> {
        Self::create_with_cache_routing_page_fanout_graph_neighbors_and_leaf_capability(
            config,
            None,
            DEFAULT_ROUTING_PAGE_FANOUT,
            graph_neighbors,
            LeafCapability::GraphEnabled,
        )
    }

    /// Create a new empty index with an optional local read-through cache.
    pub fn create_with_cache(config: IndexConfig, cache_dir: Option<PathBuf>) -> Result<Self> {
        Self::create_with_cache_routing_page_fanout_and_graph_neighbors(
            config,
            cache_dir,
            DEFAULT_ROUTING_PAGE_FANOUT,
            LOCAL_GRAPH_NEIGHBORS,
        )
    }

    /// Create a new empty index with cache and explicit routing fanout options.
    pub fn create_with_cache_and_routing_page_fanout(
        config: IndexConfig,
        cache_dir: Option<PathBuf>,
        routing_page_fanout: usize,
    ) -> Result<Self> {
        Self::create_with_cache_routing_page_fanout_and_graph_neighbors(
            config,
            cache_dir,
            routing_page_fanout,
            LOCAL_GRAPH_NEIGHBORS,
        )
    }

    /// Create a new empty index with an explicit routing fanout and an opt-in WAL.
    pub fn create_with_wal_and_routing_page_fanout(
        config: IndexConfig,
        wal: WalConfig,
        routing_page_fanout: usize,
    ) -> Result<Self> {
        let storage = Storage::from_uri(&config.uri)?;
        Self::create_with_storage_and_wal(
            config,
            storage,
            routing_page_fanout,
            LOCAL_GRAPH_NEIGHBORS,
            wal,
        )
    }

    /// Create with an explicit WAL, routing fanout, and leaf capability.
    pub fn create_with_wal_routing_page_fanout_and_leaf_capability(
        config: IndexConfig,
        wal: WalConfig,
        routing_page_fanout: usize,
        leaf_capability: LeafCapability,
    ) -> Result<Self> {
        let storage = Storage::from_uri(&config.uri)?;
        Self::create_with_storage_wal_and_capability(
            config,
            storage,
            routing_page_fanout,
            LOCAL_GRAPH_NEIGHBORS,
            wal,
            leaf_capability,
        )
    }

    /// Create with an explicit WAL, routing fanout, leaf capability, and
    /// persisted scan-codec build configuration.
    pub fn create_with_wal_routing_page_fanout_leaf_capability_and_build_config(
        config: IndexConfig,
        wal: WalConfig,
        routing_page_fanout: usize,
        leaf_capability: LeafCapability,
        build_config: BuildConfig,
    ) -> Result<Self> {
        let storage = Storage::from_uri(&config.uri)?;
        Self::create_with_storage_wal_capability_and_build(
            config,
            storage,
            routing_page_fanout,
            LOCAL_GRAPH_NEIGHBORS,
            wal,
            leaf_capability,
            build_config,
        )
    }

    /// Create a new empty index with an optional local read-through cache and an
    /// opt-in WAL.
    pub fn create_with_cache_and_wal(
        config: IndexConfig,
        cache_dir: Option<PathBuf>,
        wal: WalConfig,
    ) -> Result<Self> {
        let storage = if let Some(cache_dir) = cache_dir {
            Storage::from_uri_with_cache(&config.uri, Some(cache_dir))?
        } else {
            Storage::from_uri(&config.uri)?
        };
        Self::create_with_storage_and_wal(
            config,
            storage,
            DEFAULT_ROUTING_PAGE_FANOUT,
            LOCAL_GRAPH_NEIGHBORS,
            wal,
        )
    }

    /// Create with cache, an explicit WAL, and a fixed leaf-search capability.
    pub fn create_with_cache_wal_and_leaf_capability(
        config: IndexConfig,
        cache_dir: Option<PathBuf>,
        wal: WalConfig,
        leaf_capability: LeafCapability,
    ) -> Result<Self> {
        let storage = if let Some(cache_dir) = cache_dir {
            Storage::from_uri_with_cache(&config.uri, Some(cache_dir))?
        } else {
            Storage::from_uri(&config.uri)?
        };
        Self::create_with_storage_wal_and_capability(
            config,
            storage,
            DEFAULT_ROUTING_PAGE_FANOUT,
            LOCAL_GRAPH_NEIGHBORS,
            wal,
            leaf_capability,
        )
    }

    /// Create a new empty index with cache, routing fanout, and graph neighbor options.
    pub fn create_with_cache_routing_page_fanout_and_graph_neighbors(
        config: IndexConfig,
        cache_dir: Option<PathBuf>,
        routing_page_fanout: usize,
        graph_neighbors: usize,
    ) -> Result<Self> {
        let storage = if let Some(cache_dir) = cache_dir {
            Storage::from_uri_with_cache(&config.uri, Some(cache_dir))?
        } else {
            Storage::from_uri(&config.uri)?
        };
        Self::create_with_storage(config, storage, routing_page_fanout, graph_neighbors)
    }

    /// Create with cache/routing controls and an explicit leaf-search capability.
    ///
    /// This is the binding-friendly counterpart to
    /// [`BorsukIndex::create_with_leaf_capability`]. Ordinary creation uses
    /// [`LeafCapability::PqScanOnly`]; graph experiments must opt into
    /// [`LeafCapability::GraphEnabled`].
    pub fn create_with_cache_routing_page_fanout_graph_neighbors_and_leaf_capability(
        config: IndexConfig,
        cache_dir: Option<PathBuf>,
        routing_page_fanout: usize,
        graph_neighbors: usize,
        leaf_capability: LeafCapability,
    ) -> Result<Self> {
        let storage = if let Some(cache_dir) = cache_dir {
            Storage::from_uri_with_cache(&config.uri, Some(cache_dir))?
        } else {
            Storage::from_uri(&config.uri)?
        };
        Self::create_with_storage_wal_and_capability(
            config,
            storage,
            routing_page_fanout,
            graph_neighbors,
            WalConfig::default(),
            leaf_capability,
        )
    }

    /// Binding-friendly creation with cache/routing/leaf controls and a
    /// persisted global-PQ build layout.
    pub fn create_with_cache_routing_page_fanout_graph_neighbors_leaf_capability_and_build_config(
        config: IndexConfig,
        cache_dir: Option<PathBuf>,
        routing_page_fanout: usize,
        graph_neighbors: usize,
        leaf_capability: LeafCapability,
        build_config: BuildConfig,
    ) -> Result<Self> {
        let storage = if let Some(cache_dir) = cache_dir {
            Storage::from_uri_with_cache(&config.uri, Some(cache_dir))?
        } else {
            Storage::from_uri(&config.uri)?
        };
        Self::create_with_storage_wal_capability_and_build(
            config,
            storage,
            routing_page_fanout,
            graph_neighbors,
            WalConfig::default(),
            leaf_capability,
            build_config,
        )
    }

    #[doc(hidden)]
    pub fn create_with_object_store(
        store: Arc<dyn ObjectStore>,
        config: IndexConfig,
    ) -> Result<Self> {
        // Test seam: integration tests can share or wrap an ObjectStore without URI parsing.
        let storage = Storage::from_object_store(config.uri.clone(), store)?;
        Self::create_with_storage(
            config,
            storage,
            DEFAULT_ROUTING_PAGE_FANOUT,
            LOCAL_GRAPH_NEIGHBORS,
        )
    }

    fn create_with_storage(
        config: IndexConfig,
        storage: Storage,
        routing_page_fanout: usize,
        graph_neighbors: usize,
    ) -> Result<Self> {
        Self::create_with_storage_wal_and_capability(
            config,
            storage,
            routing_page_fanout,
            graph_neighbors,
            WalConfig::default(),
            LeafCapability::PqScanOnly,
        )
    }

    fn create_with_storage_and_wal(
        config: IndexConfig,
        storage: Storage,
        routing_page_fanout: usize,
        graph_neighbors: usize,
        wal: WalConfig,
    ) -> Result<Self> {
        Self::create_with_storage_wal_and_capability(
            config,
            storage,
            routing_page_fanout,
            graph_neighbors,
            wal,
            LeafCapability::PqScanOnly,
        )
    }

    fn create_with_storage_wal_and_capability(
        config: IndexConfig,
        storage: Storage,
        routing_page_fanout: usize,
        graph_neighbors: usize,
        wal: WalConfig,
        leaf_capability: LeafCapability,
    ) -> Result<Self> {
        Self::create_with_storage_wal_capability_and_build(
            config,
            storage,
            routing_page_fanout,
            graph_neighbors,
            wal,
            leaf_capability,
            BuildConfig::default(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn create_with_storage_wal_capability_and_build(
        config: IndexConfig,
        storage: Storage,
        routing_page_fanout: usize,
        graph_neighbors: usize,
        wal: WalConfig,
        leaf_capability: LeafCapability,
        build_config: BuildConfig,
    ) -> Result<Self> {
        Self::create_modality_with_storage_wal_capability_and_build(
            config,
            storage,
            routing_page_fanout,
            graph_neighbors,
            wal,
            leaf_capability,
            build_config,
            PRIMARY_MODALITY,
            true,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn create_modality_with_storage_wal_capability_and_build(
        config: IndexConfig,
        storage: Storage,
        routing_page_fanout: usize,
        graph_neighbors: usize,
        wal: WalConfig,
        leaf_capability: LeafCapability,
        mut build_config: BuildConfig,
        modality: &str,
        collection_root: bool,
    ) -> Result<Self> {
        // Translate the temporary language-binding alias once at construction.
        // The persisted policy is canonical and every writer/reader below uses
        // per-object resolutions and references only.
        if build_config.physical_layout == crate::PhysicalLayoutPolicy::production_baseline()
            && build_config.segment_table_format != crate::DurableTableFormat::default()
        {
            build_config.physical_layout = build_config.physical_layout.clone().with_role_format(
                crate::PhysicalObjectRole::NormalSegment,
                build_config.segment_table_format.into(),
            );
        }
        build_config.segment_table_format =
            crate::DurableTableFormat::try_from(build_config.physical_layout.resolve(
                crate::PhysicalObjectRole::NormalSegment,
                crate::PhysicalLayoutContext::default(),
            )?)?;
        validate_named_vector_config(&config.named_vectors)?;
        if config.dimensions == 0 {
            return Err(BorsukError::InvalidMetricInput(
                "index dimensions must be greater than zero".to_string(),
            ));
        }

        if config.segment_max_vectors == 0 {
            return Err(BorsukError::InvalidMetricInput(
                "segment_max_vectors must be greater than zero".to_string(),
            ));
        }
        if routing_page_fanout <= 1 {
            return Err(BorsukError::InvalidMetricInput(
                "routing_page_fanout must be greater than one".to_string(),
            ));
        }
        validate_graph_neighbors(graph_neighbors)?;
        validate_wal_config(&wal)?;
        validate_build_config(&build_config, config.dimensions)?;
        validate_vector_element_metric(
            "primary vector",
            build_config.vector_element_type,
            &config.metric,
        )?;

        if collection_root {
            storage.ensure_collection_absent()?;
        }
        storage.create_layout()?;

        let primary_uri = config.uri.clone();
        let named_specs = config.named_vectors.clone();
        let tokenizer = default_tokenizer();
        let mut manifest = Manifest::new_with_routing_page_fanout(
            config,
            routing_page_fanout,
            graph_neighbors,
            leaf_capability,
            build_config,
        );
        manifest.text_tokenizer = Some(tokenizer.fingerprint());
        manifest.wal_config = wal;
        enforce_ram_budget(&manifest, None)?;
        let staged = storage.stage_manifest(modality, &manifest, None)?;
        let manifest = staged.manifest;
        let manifest_reference = staged.reference;
        let lexical_admission = automatic_lexical_capacity_bytes(manifest.config.ram_budget_bytes)
            .map(|capacity| Arc::new(ByteAdmissionGate::new(capacity)));

        let mut index = Self {
            collection_storage: storage.clone(),
            storage,
            manifest,
            manifest_reference,
            collection_snapshot: None,
            writer_id: Uuid::new_v4().as_bytes().to_vec(),
            cell_wal_snapshot: Vec::new(),
            cell_wal_snapshot_retries: Arc::new(AtomicUsize::new(0)),
            cell_wal_claim_checkpoint: CellWalClaimCheckpoint::new(),
            named: BTreeMap::new(),
            tokenizer,
            runtime_ram_budget_bytes: None,
            lexical_admission,
            segment_cache: Arc::new(OnceLock::new()),
            resident_routing_summaries: Arc::new(Mutex::new(None)),
            coarse_quantizer: Arc::new(Mutex::new(None)),
            persisted_quantizer: Arc::new(Mutex::new(None)),
            resident_global_pq: Arc::new(Mutex::new(None)),
            resident_lexical_roots: Arc::new(Mutex::new(None)),
            admission: Some(Arc::new(AdmissionGate::new(
                DEFAULT_MAX_CONCURRENT_SEARCHES,
            ))),
            decode_admission: Some(Arc::new(AdmissionGate::new(
                DEFAULT_MAX_CONCURRENT_CELL_DECODES,
            ))),
            global_pq_rerank_admission: Arc::new(AdmissionGate::new(
                DEFAULT_GLOBAL_PQ_RERANK_READS,
            )),
            inflight_segment_reads: Arc::new(InFlightSegmentReads::default()),
            inflight_graph_reads: Arc::new(InFlightGraphReads::default()),
            inflight_lexical_reads: Arc::new(InFlightReads::default()),
            decoded_lexical_reads: Arc::new(DecodedObjectCache::new(
                DEFAULT_LEXICAL_RUN_CACHE_BYTES,
            )),
            inflight_lexical_pages: Arc::new(InFlightReads::default()),
            decoded_lexical_pages: Arc::new(DecodedObjectCache::new(
                DEFAULT_LEXICAL_TERM_PAGE_CACHE_BYTES,
            )),
            decoded_global_cell_graphs: Arc::new(DecodedObjectCache::new(
                DEFAULT_GLOBAL_CELL_GRAPH_CACHE_BYTES,
            )),
            inflight_global_cell_graph_reads: Arc::new(InFlightReads::default()),
            tombstone_cache: Arc::new(DecodedObjectCache::new(DEFAULT_TOMBSTONE_PAGE_CACHE_BYTES)),
            inflight_bm25_stats_pages: Arc::new(InFlightReads::default()),
            decoded_bm25_stats_pages: Arc::new(DecodedObjectCache::new(
                DEFAULT_BM25_STATS_PAGE_CACHE_BYTES,
            )),
            vector_sidecar_indexes: Arc::new(Mutex::new(SidecarIndexCache::default())),
            late_interaction_sidecar_indexes: Arc::new(Mutex::new(
                LateInteractionSidecarIndexCache::default(),
            )),
            inflight_late_interaction_batches: Arc::new(InFlightReads::default()),
            decoded_late_interaction_batches: Arc::new(DecodedObjectCache::new(
                DEFAULT_LATE_INTERACTION_BATCH_CACHE_BYTES,
            )),
            wal_tail_cache: Arc::new(Mutex::new(None)),
        };
        index.named = index.create_named_indexes(&primary_uri, &named_specs)?;
        if collection_root {
            let mut modalities = Vec::with_capacity(index.named.len() + 1);
            modalities.push(index.manifest_reference.clone());
            modalities.extend(
                index
                    .named
                    .values()
                    .map(|child| child.manifest_reference.clone()),
            );
            let snapshot = CollectionSnapshot {
                generation: 1,
                schema_fingerprint: collection_schema_fingerprint(&index.manifest),
                previous_snapshot_checksum: None,
                modalities,
            };
            let loaded = index.storage.create_collection_snapshot(&snapshot)?;
            for child in index.named.values_mut() {
                child.collection_storage = index.collection_storage.clone();
                child.collection_snapshot = Some(loaded.clone());
            }
            index.collection_snapshot = Some(loaded);
        }
        Ok(index)
    }

    /// Open an existing index from a local URI or path.
    pub fn open(uri: &str) -> Result<Self> {
        Self::open_with_options(uri, OpenOptions::default())
    }

    /// Open an existing index with an optional local read-through cache.
    pub fn open_with_cache(uri: &str, cache_dir: Option<PathBuf>) -> Result<Self> {
        Self::open_with_options(
            uri,
            OpenOptions {
                cache_dir,
                cache_max_bytes: None,
                ram_budget_bytes: Some(DEFAULT_RAM_BUDGET_BYTES),
                resident_routing: false,
                segment_cache_max_bytes: None,
                global_cell_graph_cache_max_bytes: DEFAULT_GLOBAL_CELL_GRAPH_CACHE_BYTES,
                tombstone_page_cache_max_bytes: DEFAULT_TOMBSTONE_PAGE_CACHE_BYTES,
                bm25_stats_page_cache_max_bytes: DEFAULT_BM25_STATS_PAGE_CACHE_BYTES,
                lexical_run_cache_max_bytes: DEFAULT_LEXICAL_RUN_CACHE_BYTES,
                lexical_term_page_cache_max_bytes: DEFAULT_LEXICAL_TERM_PAGE_CACHE_BYTES,
                late_interaction_batch_cache_max_bytes: DEFAULT_LATE_INTERACTION_BATCH_CACHE_BYTES,
                preload: false,
                max_concurrent_searches: Some(DEFAULT_MAX_CONCURRENT_SEARCHES),
                max_concurrent_cell_decodes: Some(DEFAULT_MAX_CONCURRENT_CELL_DECODES),
            },
        )
    }

    /// Open an existing index with cache and runtime budget options.
    pub fn open_with_options(uri: &str, options: OpenOptions) -> Result<Self> {
        let storage = if let Some(cache_dir) = &options.cache_dir {
            Storage::from_uri_with_cache_and_max(
                uri,
                Some(cache_dir.clone()),
                options.cache_max_bytes,
            )?
        } else {
            Storage::from_uri(uri)?
        };
        Self::open_with_storage(storage, options)
    }

    #[doc(hidden)]
    pub fn open_with_object_store(store: Arc<dyn ObjectStore>, uri: &str) -> Result<Self> {
        // Test seam: integration tests can share or wrap an ObjectStore without URI parsing.
        let storage = Storage::from_object_store(uri.to_string(), store)?;
        Self::open_with_storage(storage, OpenOptions::default())
    }

    fn open_with_storage(storage: Storage, options: OpenOptions) -> Result<Self> {
        let loaded_snapshot = storage.load_collection_snapshot()?;
        let primary_reference = loaded_snapshot
            .snapshot
            .modalities
            .first()
            .filter(|reference| reference.modality == PRIMARY_MODALITY)
            .ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "collection snapshot has no primary manifest reference".to_string(),
                )
            })?
            .clone();
        let manifest = storage.load_manifest_ref(&primary_reference, options.resident_routing)?;
        let schema_fingerprint = collection_schema_fingerprint(&manifest);
        if schema_fingerprint != loaded_snapshot.snapshot.schema_fingerprint {
            return Err(BorsukError::InvalidStorage(format!(
                "collection schema fingerprint mismatch: snapshot pins {}, primary manifest produces {schema_fingerprint}",
                loaded_snapshot.snapshot.schema_fingerprint
            )));
        }
        let expected_modalities = std::iter::once(PRIMARY_MODALITY.to_string())
            .chain(
                manifest
                    .config
                    .named_vectors
                    .iter()
                    .filter(|(_, spec)| spec.kind != VectorKind::Sparse)
                    .map(|(name, _)| name.clone()),
            )
            .collect::<Vec<_>>();
        let actual_modalities = loaded_snapshot
            .snapshot
            .modalities
            .iter()
            .map(|reference| reference.modality.clone())
            .collect::<Vec<_>>();
        if actual_modalities != expected_modalities {
            return Err(BorsukError::InvalidStorage(format!(
                "collection snapshot modalities {actual_modalities:?} do not match schema modalities {expected_modalities:?}"
            )));
        }
        let primary_uri = manifest.config.uri.clone();
        let named_specs = manifest.config.named_vectors.clone();
        let snapshot = loaded_snapshot.snapshot.clone();
        let mut index =
            Self::open_with_loaded_manifest(storage, manifest, primary_reference, options.clone())?;
        index.named = index.open_named_indexes(&primary_uri, &named_specs, &snapshot, &options)?;
        for child in index.named.values_mut() {
            child.collection_storage = index.collection_storage.clone();
            child.collection_snapshot = Some(loaded_snapshot.clone());
        }
        index.collection_snapshot = Some(loaded_snapshot);
        Ok(index)
    }

    fn open_with_loaded_manifest(
        storage: Storage,
        manifest: Manifest,
        manifest_reference: CollectionManifestRef,
        mut options: OpenOptions,
    ) -> Result<Self> {
        if options.preload {
            options.resident_routing = true;
        }
        let span = observability::open_span(options.resident_routing);
        let _entered = span.enter();
        validate_named_vector_config(&manifest.config.named_vectors)?;
        validate_build_config(&manifest.build_config, manifest.config.dimensions)?;
        validate_vector_element_metric(
            "primary vector",
            manifest.build_config.vector_element_type,
            &manifest.config.metric,
        )?;
        observability::record_open(&span, &manifest);
        enforce_ram_budget(&manifest, options.ram_budget_bytes)?;
        let effective_ram_budget =
            effective_ram_budget_bytes(manifest.config.ram_budget_bytes, options.ram_budget_bytes);
        let lexical_admission = automatic_lexical_capacity_bytes(effective_ram_budget)
            .map(|capacity| Arc::new(ByteAdmissionGate::new(capacity)));
        let segment_cache = options
            .segment_cache_max_bytes
            .or_else(|| {
                options
                    .preload
                    .then_some(effective_ram_budget.unwrap_or(u64::MAX))
            })
            .filter(|budget| *budget > 0)
            .map(|budget| Arc::new(DecodedSegmentCache::new(budget)));
        let segment_cache_cell = Arc::new(OnceLock::new());
        if let Some(segment_cache) = segment_cache {
            let _ = segment_cache_cell.set(segment_cache);
        }
        let admission = options
            .max_concurrent_searches
            .filter(|permits| *permits > 0)
            .map(|permits| Arc::new(AdmissionGate::new(permits)));
        let decode_admission = options
            .max_concurrent_cell_decodes
            .filter(|permits| *permits > 0)
            .map(|permits| Arc::new(AdmissionGate::new(permits)));
        let mut index = Self {
            collection_storage: storage.clone(),
            storage,
            manifest,
            manifest_reference,
            collection_snapshot: None,
            writer_id: Uuid::new_v4().as_bytes().to_vec(),
            cell_wal_snapshot: Vec::new(),
            cell_wal_snapshot_retries: Arc::new(AtomicUsize::new(0)),
            cell_wal_claim_checkpoint: CellWalClaimCheckpoint::new(),
            named: BTreeMap::new(),
            tokenizer: default_tokenizer(),
            runtime_ram_budget_bytes: options.ram_budget_bytes,
            lexical_admission,
            segment_cache: segment_cache_cell,
            resident_routing_summaries: Arc::new(Mutex::new(None)),
            coarse_quantizer: Arc::new(Mutex::new(None)),
            persisted_quantizer: Arc::new(Mutex::new(None)),
            resident_global_pq: Arc::new(Mutex::new(None)),
            resident_lexical_roots: Arc::new(Mutex::new(None)),
            admission,
            decode_admission,
            global_pq_rerank_admission: Arc::new(AdmissionGate::new(
                DEFAULT_GLOBAL_PQ_RERANK_READS,
            )),
            inflight_segment_reads: Arc::new(InFlightSegmentReads::default()),
            inflight_graph_reads: Arc::new(InFlightGraphReads::default()),
            inflight_lexical_reads: Arc::new(InFlightReads::default()),
            decoded_lexical_reads: Arc::new(DecodedObjectCache::new(
                options.lexical_run_cache_max_bytes,
            )),
            inflight_lexical_pages: Arc::new(InFlightReads::default()),
            decoded_lexical_pages: Arc::new(DecodedObjectCache::new(
                options.lexical_term_page_cache_max_bytes,
            )),
            decoded_global_cell_graphs: Arc::new(DecodedObjectCache::new(
                options.global_cell_graph_cache_max_bytes,
            )),
            inflight_global_cell_graph_reads: Arc::new(InFlightReads::default()),
            tombstone_cache: Arc::new(DecodedObjectCache::new(
                options.tombstone_page_cache_max_bytes,
            )),
            inflight_bm25_stats_pages: Arc::new(InFlightReads::default()),
            decoded_bm25_stats_pages: Arc::new(DecodedObjectCache::new(
                options.bm25_stats_page_cache_max_bytes,
            )),
            vector_sidecar_indexes: Arc::new(Mutex::new(SidecarIndexCache::default())),
            late_interaction_sidecar_indexes: Arc::new(Mutex::new(
                LateInteractionSidecarIndexCache::default(),
            )),
            inflight_late_interaction_batches: Arc::new(InFlightReads::default()),
            decoded_late_interaction_batches: Arc::new(DecodedObjectCache::new(
                options.late_interaction_batch_cache_max_bytes,
            )),
            wal_tail_cache: Arc::new(Mutex::new(None)),
        };
        index.cell_wal_snapshot = index.fetch_cell_wal_snapshot(&index.manifest)?;
        index.manifest.cell_wal_visible_runs = cell_wal_run_count(&index.cell_wal_snapshot);
        index.manifest.cell_wal_visible_tombstone_runs =
            cell_wal_tombstone_run_count(&index.cell_wal_snapshot);
        if options.resident_routing {
            // Modern manifests page routing summaries outside the manifest
            // table. Resolve the complete active set before marking it
            // resident; `manifest.segments` is empty for those indexes.
            let summaries = Arc::new(index.active_segment_summaries()?);
            let mut resident = index
                .resident_routing_summaries
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            *resident = Some((index.manifest.version, summaries));
            drop(resident);
            // Build the IVF centroid graph during open, so the first measured
            // query hides neither metadata I/O nor one-time construction.
            let _ = index.coarse_quantizer()?;
        }
        // The global PQ fast path defines uncached query latency after open:
        // load its compact codes and exact-sidecar metadata here, never on the
        // first measured request.
        let _ = index.load_resident_global_pq()?;
        let _ = index.load_resident_lexical_roots()?;
        index.prepare_mutation_frontier(&index.manifest)?;
        if options.preload {
            index.warm()?;
        }
        Ok(index)
    }

    fn create_named_indexes(
        &self,
        primary_uri: &str,
        named_specs: &BTreeMap<String, VectorSpec>,
    ) -> Result<BTreeMap<String, BorsukIndex>> {
        let mut named = BTreeMap::new();
        for (name, spec) in named_specs {
            if spec.kind == VectorKind::Sparse {
                continue;
            }
            let child_uri = named_vector_child_uri(primary_uri, name);
            let child_storage = self.storage.child(child_uri.clone(), name)?;
            let child_config = self.child_config(child_uri, spec);
            let mut child = Self::create_modality_with_storage_wal_capability_and_build(
                child_config,
                child_storage,
                self.manifest.routing_page_fanout,
                self.manifest.graph_neighbors,
                WalConfig::default(),
                self.manifest.leaf_capability,
                BuildConfig {
                    vector_element_type: spec.element_type,
                    ..self.manifest.build_config.clone()
                },
                name,
                false,
            )?;
            child.collection_storage = self.collection_storage.clone();
            named.insert(name.clone(), child);
        }
        Ok(named)
    }

    fn open_named_indexes(
        &self,
        primary_uri: &str,
        named_specs: &BTreeMap<String, VectorSpec>,
        snapshot: &CollectionSnapshot,
        options: &OpenOptions,
    ) -> Result<BTreeMap<String, BorsukIndex>> {
        validate_named_vector_config(named_specs)?;
        let mut named = BTreeMap::new();
        for (name, spec) in named_specs {
            if spec.kind == VectorKind::Sparse {
                continue;
            }
            let child_uri = named_vector_child_uri(primary_uri, name);
            let child_storage = self.storage.child(child_uri, name)?;
            let reference = snapshot
                .modalities
                .iter()
                .find(|reference| reference.modality == *name)
                .ok_or_else(|| {
                    BorsukError::InvalidStorage(format!(
                        "collection snapshot is missing named modality `{name}`"
                    ))
                })?
                .clone();
            let manifest = self
                .storage
                .load_manifest_ref(&reference, options.resident_routing)?;
            if manifest.config.dimensions != spec.dimensions
                || manifest.config.metric != spec.metric
                || manifest.build_config.vector_element_type != spec.element_type
                || !manifest.config.named_vectors.is_empty()
            {
                return Err(BorsukError::InvalidStorage(format!(
                    "named modality `{name}` manifest does not match its collection schema"
                )));
            }
            let mut child = Self::open_with_loaded_manifest(
                child_storage,
                manifest,
                reference,
                options.clone(),
            )?;
            child.collection_storage = self.collection_storage.clone();
            named.insert(name.clone(), child);
        }
        Ok(named)
    }

    fn child_config(&self, uri: String, spec: &VectorSpec) -> IndexConfig {
        IndexConfig {
            uri,
            metric: spec.metric.clone(),
            dimensions: spec.dimensions,
            segment_max_vectors: self.manifest.config.segment_max_vectors,
            ram_budget_bytes: self.manifest.config.ram_budget_bytes,
            text: false,
            named_vectors: BTreeMap::new(),
        }
    }

    /// Return the active manifest metadata.
    #[must_use]
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// Cumulative object-store request counts for this index handle, including
    /// dense named-vector child indexes. Snapshot before/after an operation and
    /// use [`RequestCounts::delta`] to attribute its physical requests.
    #[must_use]
    pub fn request_counts(&self) -> RequestCounts {
        self.named
            .values()
            .fold(self.storage.request_counts(), |mut total, child| {
                let child = child.request_counts();
                total.gets = total.gets.saturating_add(child.gets);
                total.puts = total.puts.saturating_add(child.puts);
                total.deletes = total.deletes.saturating_add(child.deletes);
                total.heads = total.heads.saturating_add(child.heads);
                total.lists = total.lists.saturating_add(child.lists);
                total
            })
    }

    /// Eagerly decode every active segment into the shared in-memory cache.
    /// Graph-enabled indexes also decode and validate their immutable segment
    /// graphs, so the first graph query performs no graph-object I/O or Parquet
    /// decode. Graph-free production indexes retain no graph memory.
    ///
    /// The active routing summaries are retained as a resident snapshot for
    /// this manifest version. Decoded entries remain byte-bounded and evictable:
    /// callers must inspect [`WarmReport::coverage_complete`] rather than assume
    /// all payloads fit. `Auto` selects the cache graph only for complete graph
    /// coverage and otherwise keeps using the configured storage scan.
    pub fn warm(&self) -> Result<WarmReport> {
        let summaries = self.active_segment_summaries()?;
        let summaries = {
            let mut resident = self
                .resident_routing_summaries
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            match resident.as_ref() {
                Some((version, summaries)) if *version == self.manifest.version => {
                    Arc::clone(summaries)
                }
                _ => {
                    let summaries = Arc::new(summaries);
                    *resident = Some((self.manifest.version, Arc::clone(&summaries)));
                    summaries
                }
            }
        };
        let cache = self.segment_cache.get_or_init(|| {
            Arc::new(DecodedSegmentCache::new(
                self.effective_ram_budget_bytes().unwrap_or(u64::MAX),
            ))
        });

        let mut segments_loaded = 0;
        for summary in summaries.iter() {
            let (segment, _, _, _, decoded_cache_hit) =
                self.read_segment_through_cache(summary, false)?;
            if !decoded_cache_hit {
                segments_loaded += 1;
            }
            if self.manifest.leaf_capability.builds_graph() {
                self.read_graph(summary, &segment)?;
            }
        }
        let segments_resident = summaries
            .iter()
            .filter(|summary| cache.contains(&summary.checksum))
            .count();
        let graphs_resident = if self.manifest.leaf_capability.builds_graph() {
            summaries
                .iter()
                .filter(|summary| cache.contains_graph(&summary.checksum, &summary.graph_checksum))
                .count()
        } else {
            0
        };
        let segments_total = summaries.len();
        let coverage_complete = segments_resident == segments_total
            && (!self.manifest.leaf_capability.builds_graph() || graphs_resident == segments_total);
        Ok(WarmReport {
            segments_loaded,
            segments_total,
            segments_resident,
            graphs_resident,
            coverage_complete,
            bytes_resident: cache.resident_bytes(),
        })
    }

    /// Prepare compact metadata needed by object-store quantized scans without
    /// loading segment payloads or dense vectors.
    ///
    /// This makes routing summaries, the IVF centroid graph, and the global PQ
    /// descriptor/codebook resident. Per-cell code payloads and exact-sidecar
    /// indexes remain paged and enter bounded caches on demand, so preparing a
    /// 100M-vector index does not allocate memory proportional to its rows.
    ///
    /// Returns the number of active routing cells prepared.
    pub fn prepare_serving_metadata(&self) -> Result<usize> {
        let summaries = if let Some(summaries) = self.resident_routing_summaries() {
            summaries
        } else {
            let summaries = Arc::new(self.active_segment_summaries()?);
            let mut resident = self
                .resident_routing_summaries
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            *resident = Some((self.manifest.version, Arc::clone(&summaries)));
            summaries
        };
        let _ = self.coarse_quantizer()?;
        let _ = self.load_resident_global_pq()?;
        let _ = self.load_resident_lexical_roots()?;
        self.prepare_mutation_frontier(&self.manifest)?;
        Ok(summaries.len())
    }

    /// Load the bounded live mutation frontier before the handle begins
    /// serving. Stable tombstone/statistics pages stay query-paged; only recent
    /// WAL deltas are prepared, so a reader refresh—not an arbitrary first
    /// query—pays the S3 metadata-overlay fetch.
    fn prepare_mutation_frontier(&self, manifest: &Manifest) -> Result<()> {
        self.prepare_manifest_mutation_frontier(manifest)?;
        self.prepare_cell_mutation_frontier(&self.cell_wal_snapshot)
    }

    fn prepare_manifest_mutation_frontier(&self, manifest: &Manifest) -> Result<()> {
        for tombstone in &manifest.tombstone_frontier {
            self.load_tombstone_run(tombstone)?;
        }
        for reference in &manifest.bm25_stats_delta_frontier {
            for page in &reference.pages {
                self.read_bm25_stats_delta_page(page)?;
            }
        }
        Ok(())
    }

    fn prepare_cell_mutation_frontier(
        &self,
        snapshot: &[CommittedCellWalTransaction],
    ) -> Result<()> {
        for transaction in snapshot {
            for run in &transaction.runs {
                if run.kind == CellWalRunKind::Tombstones {
                    self.load_tombstone_run(&Self::cell_wal_tombstone_summary(run)?)?;
                }
            }
            if let Some(reference) = Self::cell_wal_metadata(transaction)?.bm25_stats_delta {
                for page in &reference.pages {
                    self.read_bm25_stats_delta_page(page)?;
                }
            }
        }
        Ok(())
    }

    fn load_resident_lexical_roots(&self) -> Result<BTreeMap<(String, String), Arc<LexicalRoot>>> {
        {
            let cache = self
                .resident_lexical_roots
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if let Some((version, roots)) = cache.as_ref()
                && *version == self.manifest.version
            {
                return Ok(roots.clone());
            }
        }
        let mut roots = BTreeMap::new();
        for root_ref in &self.manifest.lexical_roots {
            let read = self
                .storage
                .read_known_size_with_cache_status_and_checksum(
                    &root_ref.path,
                    root_ref.encoded_bytes,
                    &root_ref.checksum,
                )?;
            let root = lexical_root_from_parquet(&read.bytes)?;
            if root.kind.as_str() != root_ref.kind {
                return Err(BorsukError::InvalidStorage(format!(
                    "lexical root `{}` kind differs from manifest",
                    root_ref.path
                )));
            }
            roots.insert(
                (root_ref.kind.clone(), root_ref.name.clone()),
                Arc::new(root),
            );
        }
        let mut cache = self
            .resident_lexical_roots
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        *cache = Some((self.manifest.version, roots.clone()));
        Ok(roots)
    }

    /// Populate the local disk/decoded graph caches only for cells selected by
    /// the supplied query working set. Cells without a built graph are skipped;
    /// later queries still scan every uncovered cell from the configured
    /// storage tier. This explicit preparation keeps network I/O outside the
    /// measured cached-query phase and avoids loading a corpus-wide snapshot.
    /// `nprobe == 0` uses the immutable artifact's production probe count.
    #[doc(hidden)]
    pub fn warm_global_cell_graphs_for_queries(
        &self,
        queries: &[Vec<f32>],
        nprobe: usize,
    ) -> Result<usize> {
        let Some((index, _, _)) = self.load_resident_global_pq()? else {
            return Ok(0);
        };
        let probe_count = if nprobe == 0 {
            self.manifest
                .global_pq_ref
                .as_ref()
                .map_or(1, |reference| reference.probes)
        } else {
            nprobe
        }
        .max(1)
        .min(index.cell_count());
        let mut selected = BTreeMap::<String, GlobalPqChunkRef>::new();
        for query in queries {
            if query.len() != self.manifest.config.dimensions {
                return Err(BorsukError::InvalidMetricInput(format!(
                    "query has {} dimensions but index requires {}",
                    query.len(),
                    self.manifest.config.dimensions
                )));
            }
            let query = if self
                .manifest
                .config
                .metric
                .uses_normalized_euclidean_geometry()
            {
                crate::metric::unit_l2_normalized(query)
            } else {
                query.clone()
            };
            let cells = index.nearest_cells(&query, probe_count)?;
            for chunk in index.chunks_for_cells(&cells) {
                if let Some(graph) = &chunk.graph {
                    selected.insert(graph.checksum.clone(), chunk);
                }
            }
        }
        let mut loaded = 0;
        for chunk in selected.into_values() {
            let graph = chunk.graph.as_ref().expect("selected graph reference");
            self.storage
                .read_bytes_with_cache_status_and_checksum(&graph.path, &graph.checksum)?;
            if self.cached_global_cell_graph(&chunk)?.is_some() {
                loaded += 1;
            }
        }
        Ok(loaded)
    }

    fn resident_routing_summaries(&self) -> Option<Arc<Vec<SegmentSummary>>> {
        let resident = self
            .resident_routing_summaries
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        resident.as_ref().and_then(|(version, summaries)| {
            (*version == self.manifest.version).then(|| Arc::clone(summaries))
        })
    }

    /// Return the configured maximum segment-local graph neighbors per source record.
    #[must_use]
    pub fn graph_neighbors(&self) -> usize {
        self.manifest.graph_neighbors
    }

    /// Advance this handle to the latest atomically published snapshot.
    ///
    /// Long-lived reader nodes call this at their preferred polling boundary to
    /// observe remote WAL/delta commits without reopening the library. Existing
    /// handles remain snapshot-isolated until they explicitly refresh. Returns
    /// `true` when the manifest advanced.
    pub fn refresh(&mut self) -> Result<bool> {
        let latest_collection = self.collection_storage.load_collection_snapshot()?;
        let own_modality = self.manifest_reference.modality.clone();
        let own_reference = latest_collection
            .snapshot
            .modalities
            .iter()
            .find(|reference| reference.modality == own_modality)
            .ok_or_else(|| {
                BorsukError::InvalidStorage(format!(
                    "collection snapshot has no `{own_modality}` manifest reference"
                ))
            })?
            .clone();
        let mut latest = self
            .collection_storage
            .load_manifest_ref(&own_reference, self.resident_routing_summaries().is_some())?;
        if own_modality == PRIMARY_MODALITY
            && collection_schema_fingerprint(&latest)
                != latest_collection.snapshot.schema_fingerprint
        {
            return Err(BorsukError::InvalidStorage(
                "collection schema fingerprint changed during refresh".to_string(),
            ));
        }
        if own_modality != PRIMARY_MODALITY
            && (latest.config.dimensions != self.manifest.config.dimensions
                || latest.config.metric != self.manifest.config.metric
                || latest.build_config.vector_element_type
                    != self.manifest.build_config.vector_element_type)
        {
            return Err(BorsukError::InvalidStorage(format!(
                "named modality `{own_modality}` schema changed during refresh"
            )));
        }
        let latest_cell_wal_snapshot = self.fetch_cell_wal_snapshot(&latest)?;
        self.prepare_manifest_mutation_frontier(&latest)?;
        self.prepare_cell_mutation_frontier(&latest_cell_wal_snapshot)?;
        latest.cell_wal_visible_runs = cell_wal_run_count(&latest_cell_wal_snapshot);
        latest.cell_wal_visible_tombstone_runs =
            cell_wal_tombstone_run_count(&latest_cell_wal_snapshot);

        let mut prepared_named = BTreeMap::new();
        for (name, child) in &self.named {
            let reference = latest_collection
                .snapshot
                .modalities
                .iter()
                .find(|reference| reference.modality == *name)
                .ok_or_else(|| {
                    BorsukError::InvalidStorage(format!(
                        "collection snapshot is missing named modality `{name}`"
                    ))
                })?
                .clone();
            let mut manifest = self
                .collection_storage
                .load_manifest_ref(&reference, child.resident_routing_summaries().is_some())?;
            let cell_wal_snapshot = child.fetch_cell_wal_snapshot(&manifest)?;
            child.prepare_manifest_mutation_frontier(&manifest)?;
            child.prepare_cell_mutation_frontier(&cell_wal_snapshot)?;
            manifest.cell_wal_visible_runs = cell_wal_run_count(&cell_wal_snapshot);
            manifest.cell_wal_visible_tombstone_runs =
                cell_wal_tombstone_run_count(&cell_wal_snapshot);
            prepared_named.insert(name.clone(), (manifest, reference, cell_wal_snapshot));
        }

        let collection_advanced = self
            .collection_snapshot
            .as_ref()
            .is_none_or(|current| current.checksum != latest_collection.checksum);
        let manifest_advanced = latest.version != self.manifest.version;
        let cell_wal_advanced = latest_cell_wal_snapshot != self.cell_wal_snapshot;
        let named_advanced = prepared_named
            .iter()
            .any(|(name, (manifest, _, snapshot))| {
                let child = &self.named[name];
                manifest.version != child.manifest.version || snapshot != &child.cell_wal_snapshot
            });
        if !collection_advanced && !manifest_advanced && !cell_wal_advanced && !named_advanced {
            return Ok(false);
        }

        self.manifest = latest;
        self.manifest_reference = own_reference;
        self.cell_wal_snapshot = latest_cell_wal_snapshot;
        self.invalidate_wal_tail_cache();
        for (name, (manifest, reference, cell_wal_snapshot)) in prepared_named {
            let child = self
                .named
                .get_mut(&name)
                .expect("prepared named modality belongs to the current schema");
            child.manifest = manifest;
            child.manifest_reference = reference;
            child.cell_wal_snapshot = cell_wal_snapshot;
            child.collection_snapshot = Some(latest_collection.clone());
            child.invalidate_wal_tail_cache();
        }
        self.collection_snapshot = Some(latest_collection);
        Ok(true)
    }

    /// Return active index statistics without scanning segment or graph payloads.
    #[must_use]
    pub fn stats(&self) -> IndexStats {
        self.try_stats().unwrap_or_else(|_| {
            let totals = self.manifest_stats_totals();
            self.index_stats_from_totals(totals)
        })
    }

    /// Return active index statistics or an error when required metadata is corrupt.
    pub fn try_stats(&self) -> Result<IndexStats> {
        let totals = self.stats_totals()?;
        Ok(self.index_stats_from_totals(totals))
    }

    fn index_stats_from_totals(&self, totals: StatsTotals) -> IndexStats {
        // Un-flushed WAL-tail records are live but not yet in any segment, so
        // fold their live count into the visible record total. Best-effort: a
        // failed tail read leaves the segment-only total (stats is advisory).
        let wal_records = self
            .live_wal_tail_records()
            .map(|records| records.len())
            .unwrap_or(0);
        let mut wal_record_runs = 0;
        let mut wal_record_bytes = 0_u64;
        let mut wal_parquet_record_runs = 0;
        let mut wal_parquet_record_bytes = 0_u64;
        let mut wal_vortex_record_runs = 0;
        let mut wal_vortex_record_bytes = 0_u64;
        for run in self
            .cell_wal_snapshot
            .iter()
            .flat_map(|transaction| &transaction.runs)
            .filter(|run| run.kind == CellWalRunKind::Records)
        {
            wal_record_runs += 1;
            wal_record_bytes = wal_record_bytes.saturating_add(run.byte_len);
            if run.path.ends_with(".parquet") {
                wal_parquet_record_runs += 1;
                wal_parquet_record_bytes = wal_parquet_record_bytes.saturating_add(run.byte_len);
            } else if run.path.ends_with(".vortex") {
                wal_vortex_record_runs += 1;
                wal_vortex_record_bytes = wal_vortex_record_bytes.saturating_add(run.byte_len);
            }
        }
        IndexStats {
            metric: self.manifest.config.metric.to_string(),
            dimensions: self.manifest.config.dimensions,
            segment_max_vectors: self.manifest.config.segment_max_vectors,
            ram_budget_bytes: self.effective_ram_budget_bytes(),
            text: self.manifest.config.text,
            named_vectors: self.named.keys().cloned().collect(),
            sparse_encoded_vectors: totals.sparse_encoded_vectors,
            dense_encoded_vectors: totals.dense_encoded_vectors,
            manifest_version: self.manifest.version,
            routing_max_level: self.manifest.routing_max_level,
            routing_page_fanout: self.manifest.routing_page_fanout,
            routing_leaf_pages: totals.routing_leaf_pages,
            routing_pages: totals.routing_pages,
            segments: totals.segments,
            records: totals.records + wal_records,
            segment_bytes: totals.segment_bytes,
            vector_bytes: totals.vector_bytes,
            graph_bytes: totals.graph_bytes,
            wal_record_runs,
            wal_record_bytes,
            wal_parquet_record_runs,
            wal_parquet_record_bytes,
            wal_vortex_record_runs,
            wal_vortex_record_bytes,
            global_scan_bytes: self
                .manifest
                .global_pq_ref
                .as_ref()
                .map_or(0, |reference| reference.storage_bytes),
            resident_bytes_estimate: self.manifest.resident_bytes_estimate(),
        }
    }

    fn stats_totals(&self) -> Result<StatsTotals> {
        if !self.manifest.segments.is_empty() {
            return Ok(self.manifest_stats_totals());
        }

        let page_refs = self.storage.read_routing_layer_page_index(
            self.manifest.version,
            self.manifest.routing_max_level,
        )?;
        let (routing_leaf_pages, routing_pages) =
            self.routing_topology_totals_from_top_page_refs(&page_refs)?;

        Ok(StatsTotals {
            routing_leaf_pages,
            routing_pages,
            segments: page_refs
                .iter()
                .map(|page_ref| page_ref.leaf_segments)
                .sum(),
            records: page_refs.iter().map(|page_ref| page_ref.page_records).sum(),
            segment_bytes: page_refs
                .iter()
                .map(|page_ref| page_ref.page_segment_bytes)
                .sum(),
            vector_bytes: page_refs
                .iter()
                .map(|page_ref| page_ref.page_vector_bytes)
                .sum(),
            graph_bytes: page_refs
                .iter()
                .map(|page_ref| page_ref.page_graph_bytes)
                .sum(),
            sparse_encoded_vectors: page_refs
                .iter()
                .map(|page_ref| page_ref.page_sparse_encoded_vectors)
                .sum(),
            dense_encoded_vectors: page_refs
                .iter()
                .map(|page_ref| page_ref.page_dense_encoded_vectors)
                .sum(),
        })
    }

    fn manifest_stats_totals(&self) -> StatsTotals {
        let segments = self.manifest.segments.len();
        StatsTotals {
            routing_leaf_pages: routing_leaf_page_count(
                segments,
                self.manifest.routing_page_fanout,
            ),
            routing_pages: routing_page_tree_content_page_count(
                segments,
                self.manifest.routing_page_fanout,
            ),
            segments,
            records: self
                .manifest
                .segments
                .iter()
                .map(|segment| segment.object_count)
                .sum(),
            segment_bytes: self
                .manifest
                .segments
                .iter()
                .map(|segment| segment.size_bytes)
                .sum(),
            vector_bytes: self
                .manifest
                .segments
                .iter()
                .map(|segment| segment.vector_size_bytes)
                .sum(),
            graph_bytes: self
                .manifest
                .segments
                .iter()
                .map(|segment| segment.graph_size_bytes)
                .sum(),
            sparse_encoded_vectors: self
                .manifest
                .segments
                .iter()
                .map(|segment| segment.sparse_encoded)
                .sum(),
            dense_encoded_vectors: self
                .manifest
                .segments
                .iter()
                .map(|segment| segment.dense_encoded)
                .sum(),
        }
    }

    fn routing_topology_totals_from_top_page_refs(
        &self,
        top_page_refs: &[RoutingLayerPageRef],
    ) -> Result<(usize, usize)> {
        let Some(first_page_ref) = top_page_refs.first() else {
            return Ok((0, 0));
        };
        let routing_level = first_page_ref.routing_level;
        if top_page_refs
            .iter()
            .any(|page_ref| page_ref.routing_level != routing_level)
        {
            return Err(BorsukError::InvalidStorage(
                "routing stats found mixed top routing levels".to_string(),
            ));
        }
        if routing_level == 0 {
            return Ok((top_page_refs.len(), top_page_refs.len()));
        }
        if top_page_refs
            .iter()
            .all(|page_ref| page_ref.leaf_pages > 0 && page_ref.routing_pages > 0)
        {
            return Ok((
                top_page_refs
                    .iter()
                    .map(|page_ref| page_ref.leaf_pages)
                    .sum(),
                top_page_refs
                    .iter()
                    .map(|page_ref| page_ref.routing_pages)
                    .sum(),
            ));
        }

        let leaf_read = self.routing_leaf_page_refs_for_filter_read(top_page_refs, |_| true)?;
        let routing_leaf_pages = leaf_read.page_refs.len();
        let routing_pages = leaf_read
            .routing_pages_read
            .saturating_add(routing_leaf_pages);
        Ok((routing_leaf_pages, routing_pages))
    }

    /// Add records by writing one or more immutable L0 segments and publishing a new manifest.
    pub fn add(&mut self, mut records: Vec<VectorRecord>) -> Result<()> {
        self.canonicalize_sparse_named_records(&mut records)?;
        self.canonicalize_late_interaction_records(&mut records)?;
        let named_records = self.named_records_for_add(&records)?;
        let next_generated_id = next_generated_id_after_explicit_records(
            self.cell_wal_next_generated_id_floor()?,
            &records,
        )?;
        self.add_records_with_report(records, true, next_generated_id)?;
        self.add_named_records(named_records)?;
        Ok(())
    }

    /// Insert or replace records by id (MVCC upsert). Unlike [`BorsukIndex::add`],
    /// which is insert-only and rejects existing ids, `upsert` stamps each record
    /// a strictly higher generation than the id's current live version and
    /// publishes that new version together with a tombstone-overlay bump in one
    /// manifest — so reads immediately see only the new record and the superseded
    /// generations are dropped by the next compaction. A previously deleted id is
    /// revived. Named and sparse-named vectors are replaced in lockstep.
    pub fn upsert(&mut self, mut records: Vec<VectorRecord>) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let entity_ids = records
            .iter()
            .map(|record| record.id.clone())
            .collect::<Vec<_>>();
        let previous_late_token_ids = self.late_interaction_token_ids_for_entities(&entity_ids)?;

        let mut minimums = BTreeMap::new();
        for record in &records {
            let key = record.id.as_bytes().to_vec();
            if !minimums.contains_key(&key) {
                minimums.insert(key.clone(), self.min_visible_generation(&key)?);
            }
        }

        // Persist only corrections for superseded versions that already live in
        // immutable segments. An old WAL version is removed from the live WAL
        // sidecar by the same tombstone and was never part of the persisted
        // lexical root, so subtracting it here would double-correct N/df.
        let mut bm25_stats_delta_change = Bm25StatsDelta::default();
        if self.manifest.config.text {
            let targets = minimums
                .iter()
                .map(|(key, minimum)| (key.clone(), minimum.unwrap_or(0)))
                .collect::<BTreeMap<_, _>>();
            for live in self.live_delete_records(&targets)?.into_values() {
                if live.persisted
                    && let Some(terms) = live.text_terms
                {
                    bm25_stats_delta_change.suppress_document(&terms)?;
                }
            }
        }
        let bm25_stats_delta = self.persist_bm25_stats_delta(&bm25_stats_delta_change)?;

        // Stamp a strictly higher generation per id and append only this
        // mutation batch to the tombstone WAL. Never clone/rewrite the
        // accumulated deleted-id set on the foreground path.
        let mut planned_overlay = BTreeMap::new();
        let mut generation_requests = Vec::with_capacity(records.len());
        let mut new_overlay_ids = 0_u64;
        for record in &records {
            let key = record.id.as_bytes().to_vec();
            let previous_generation = planned_overlay
                .get(&key)
                .copied()
                .or(minimums.get(&key).copied().flatten());
            if previous_generation.is_none() {
                new_overlay_ids = new_overlay_ids.checked_add(1).ok_or_else(|| {
                    BorsukError::InvalidStorage("tombstone id count exceeds u64".to_string())
                })?;
            }
            let minimum_generation =
                previous_generation
                    .unwrap_or(0)
                    .checked_add(1)
                    .ok_or_else(|| {
                        BorsukError::InvalidStorage("record generation exceeds u64".to_string())
                    })?;
            planned_overlay.insert(key.clone(), minimum_generation);
            generation_requests.push((key, minimum_generation));
        }
        let reserved_generations = self.reserve_record_generations(&generation_requests)?;
        let mut overlay_delta = BTreeMap::new();
        for (record, generation) in records.iter_mut().zip(reserved_generations) {
            record.generation = generation;
            overlay_delta.insert(record.id.as_bytes().to_vec(), generation);
        }

        self.canonicalize_sparse_named_records(&mut records)?;
        self.canonicalize_late_interaction_records(&mut records)?;
        let named_records = self.named_records_for_add(&records)?;
        let next_generated_id = next_generated_id_after_explicit_records(
            self.cell_wal_next_generated_id_floor()?,
            &records,
        )?;
        let tombstone = self
            .write_tombstone(overlay_delta)?
            .map(|summary| (summary, new_overlay_ids));
        self.add_records_with_report_and_tombstone(
            records,
            false,
            next_generated_id,
            tombstone,
            Some(bm25_stats_delta),
        )?;
        self.upsert_named_records(named_records)?;
        for (name, token_ids) in previous_late_token_ids {
            if token_ids.is_empty() {
                continue;
            }
            let child = self.named.get_mut(&name).ok_or_else(|| {
                BorsukError::InvalidStorage(format!(
                    "late-interaction token index `{name}` is not open"
                ))
            })?;
            child.delete_with_report(token_ids)?;
        }
        Ok(())
    }

    fn upsert_named_records(
        &mut self,
        named_records: BTreeMap<String, Vec<VectorRecord>>,
    ) -> Result<()> {
        for (name, records) in named_records {
            if records.is_empty() {
                continue;
            }
            let kind = self
                .manifest
                .config
                .named_vectors
                .get(&name)
                .map(|spec| spec.kind)
                .ok_or_else(|| {
                    BorsukError::InvalidRecordInput(format!(
                        "named vector `{name}` is not declared"
                    ))
                })?;
            let child = self.named.get_mut(&name).ok_or_else(|| {
                BorsukError::InvalidRecordInput(format!(
                    "named vector `{name}` is declared but its sub-index is not open"
                ))
            })?;
            if kind == VectorKind::LateInteraction {
                let next_generated_id = next_generated_id_after_explicit_records(
                    child.cell_wal_next_generated_id_floor()?,
                    &records,
                )?;
                child.add_records_with_report(records, true, next_generated_id)?;
            } else {
                child.upsert(records)?;
            }
        }
        Ok(())
    }

    /// Add vectors and return generated or supplied ids plus write counters.
    pub fn add_with_report(
        &mut self,
        vectors: Vec<Vec<f32>>,
        ids: Option<Vec<String>>,
    ) -> Result<(Vec<String>, AddReport)> {
        let Some(ids) = ids else {
            return self.add_vectors_with_report(vectors);
        };
        let records = records_from_ids_and_vectors(ids.clone(), vectors)?;
        let next_generated_id = next_generated_id_after_explicit_records(
            self.cell_wal_next_generated_id_floor()?,
            &records,
        )?;
        let report = self.add_records_with_report(records, true, next_generated_id)?;
        Ok((ids, report))
    }

    /// Add vectors with generated collision-free numeric ids.
    pub fn add_vectors(&mut self, vectors: Vec<Vec<f32>>) -> Result<Vec<String>> {
        let (ids, _) = self.add_vectors_with_report(vectors)?;
        Ok(ids)
    }

    /// Add vectors with generated collision-free numeric ids and return write counters.
    pub fn add_vectors_with_report(
        &mut self,
        vectors: Vec<Vec<f32>>,
    ) -> Result<(Vec<String>, AddReport)> {
        let ids = self.generate_ids(vectors.len())?;
        let records = records_from_ids_and_vectors(ids.clone(), vectors)?;
        let next_generated_id = next_generated_id_after_explicit_records(
            self.cell_wal_next_generated_id_floor()?,
            &records,
        )?;
        let report = self.add_records_with_report(records, false, next_generated_id)?;
        Ok((ids, report))
    }

    /// Add vectors with caller-supplied ids.
    pub fn add_vectors_with_ids(
        &mut self,
        vectors: Vec<Vec<f32>>,
        ids: Vec<String>,
    ) -> Result<Vec<String>> {
        let (ids, _) = self.add_with_report(vectors, Some(ids))?;
        Ok(ids)
    }

    /// Logically delete records by id and return how many were newly tombstoned.
    ///
    /// Deletes are soft: ids are appended to immutable tombstone deltas and
    /// consolidated into hash-routed pages, so search and `get_vector` skip them
    /// immediately while underlying rows remain in immutable segments until
    /// compaction or [`BorsukIndex::purge`] physically rewrites them. Re-adding a
    /// deleted id revives it.
    pub fn delete<I, R>(&mut self, ids: I) -> Result<usize>
    where
        I: IntoIterator<Item = R>,
        R: Into<RecordId>,
    {
        Ok(self.delete_with_report(ids)?.deleted)
    }

    /// Logically delete records by id and return a [`DeleteReport`].
    pub fn delete_with_report<I, R>(&mut self, ids: I) -> Result<DeleteReport>
    where
        I: IntoIterator<Item = R>,
        R: Into<RecordId>,
    {
        let ids = ids.into_iter().map(Into::into).collect::<Vec<_>>();
        let late_token_ids = self.late_interaction_token_ids_for_entities(&ids)?;
        let report = self.delete_primary_with_report(ids.iter().cloned())?;
        for (name, child) in &mut self.named {
            let kind = self
                .manifest
                .config
                .named_vectors
                .get(name)
                .map(|spec| spec.kind)
                .unwrap_or(VectorKind::Dense);
            if kind == VectorKind::LateInteraction {
                if let Some(token_ids) = late_token_ids.get(name) {
                    child.delete_with_report(token_ids.iter().cloned())?;
                }
            } else {
                child.delete_with_report(ids.iter().cloned())?;
            }
        }
        Ok(report)
    }

    fn delete_primary_with_report<I, R>(&mut self, ids: I) -> Result<DeleteReport>
    where
        I: IntoIterator<Item = R>,
        R: Into<RecordId>,
    {
        let requests_before = self.storage.request_counts();
        let before = usize::try_from(self.visible_tombstone_id_count()?).unwrap_or(usize::MAX);
        let ids = ids
            .into_iter()
            .map(|id| id.into().as_bytes().to_vec())
            .collect::<Vec<_>>();
        let mut minimums = BTreeMap::new();
        for key in &ids {
            if !minimums.contains_key(key) {
                minimums.insert(key.clone(), self.min_visible_generation(key)?);
            }
        }
        let live_targets = minimums
            .iter()
            .filter(|(_, minimum)| self.manifest.config.text || minimum.is_some())
            .map(|(key, minimum)| (key.clone(), minimum.unwrap_or(0)))
            .collect::<BTreeMap<_, _>>();
        let live_records = self.live_delete_records(&live_targets)?;
        let mut planned_delta = BTreeMap::new();
        let mut generation_requests = Vec::new();
        let mut newly = 0usize;
        let mut new_overlay_ids = 0_u64;
        let mut bm25_stats_delta_change = Bm25StatsDelta::default();
        for key in ids {
            let current = planned_delta
                .get(&key)
                .copied()
                .or(minimums.get(&key).copied().flatten());
            match current {
                // First tombstone for this id: any stored copy has generation 0
                // (an upsert would already have left an entry), so a minimum
                // visible generation of 1 suppresses it.
                None => {
                    if self.manifest.config.text
                        && let Some(live) = live_records.get(&key)
                        && live.persisted
                        && let Some(terms) = &live.text_terms
                    {
                        bm25_stats_delta_change.suppress_document(terms)?;
                    }
                    planned_delta.insert(key.clone(), 1);
                    generation_requests.push((key, 1));
                    newly += 1;
                    new_overlay_ids = new_overlay_ids.checked_add(1).ok_or_else(|| {
                        BorsukError::InvalidStorage("tombstone id count exceeds u64".to_string())
                    })?;
                }
                // Already tombstoned: only bump — and count — when a still-visible
                // copy exists (e.g. the id was re-upserted after a prior delete).
                // Re-deleting an already-deleted id is a no-op.
                Some(min_visible) => {
                    if let Some(live) = live_records.get(&key)
                        && live.generation >= min_visible
                    {
                        if self.manifest.config.text
                            && live.persisted
                            && let Some(terms) = &live.text_terms
                        {
                            bm25_stats_delta_change.suppress_document(terms)?;
                        }
                        let minimum = min_visible.checked_add(1).ok_or_else(|| {
                            BorsukError::InvalidStorage("record generation exceeds u64".to_string())
                        })?;
                        planned_delta.insert(key.clone(), minimum);
                        generation_requests.push((key, minimum));
                        newly += 1;
                    }
                }
            }
        }
        if newly == 0 {
            return Ok(DeleteReport {
                deleted: 0,
                total_tombstoned: before,
                published: false,
                requests: self.storage.request_counts().delta(&requests_before),
            });
        }
        let reserved_generations = self.reserve_record_generations(&generation_requests)?;
        let deleted_delta = generation_requests
            .into_iter()
            .zip(reserved_generations)
            .map(|((key, _), generation)| (key, generation))
            .collect();
        let bm25_stats_delta = self.persist_bm25_stats_delta(&bm25_stats_delta_change)?;
        let tombstone = self.write_tombstone(deleted_delta)?;
        let transaction_id = Uuid::new_v4().simple().to_string();
        self.append_wal_and_publish(
            Vec::new(),
            self.cell_wal_next_generated_id_floor()?,
            tombstone.map(|summary| (summary, new_overlay_ids)),
            Some(bm25_stats_delta),
            &requests_before,
            CellWalAppendTransaction {
                id: &transaction_id,
                claimed: false,
            },
        )?;
        self.maybe_flush_wal()?;
        Ok(DeleteReport {
            deleted: newly,
            total_tombstoned: usize::try_from(self.visible_tombstone_id_count()?)
                .unwrap_or(usize::MAX),
            published: true,
            requests: self.storage.request_counts().delta(&requests_before),
        })
    }

    /// Physically remove every tombstoned row and clear the stable/live tombstone
    /// state, reclaiming storage synchronously and re-enabling those ids for `add`.
    ///
    /// This is the heavy, on-demand counterpart to the lazy reclaim that ordinary
    /// compaction performs: it rewrites every active segment without the deleted
    /// rows. Prefer running it during maintenance windows on large indexes.
    pub fn purge(&mut self) -> Result<usize> {
        Ok(self.purge_with_report()?.records_purged)
    }

    /// Purge tombstoned rows and return a [`PurgeReport`].
    pub fn purge_with_report(&mut self) -> Result<PurgeReport> {
        // Materialize the WAL tail so a purge physically rewrites every live
        // record (purge only rewrites segments). Flush recurses into children.
        self.flush()?;
        let report = self.purge_primary_with_report()?;
        for child in self.named.values_mut() {
            child.purge_with_report()?;
        }
        Ok(report)
    }

    fn purge_primary_with_report(&mut self) -> Result<PurgeReport> {
        let span = observability::compact_span(
            &CompactionOptions {
                source_level: 0,
                target_level: 0,
                max_segments: None,
                min_segments: 0,
                target_segment_max_vectors: None,
                target_segment_max_radius: None,
            },
            self.manifest.version,
        );
        let _entered = span.enter();
        self.purge_impl()
    }

    fn purge_impl(&mut self) -> Result<PurgeReport> {
        let requests_before = self.storage.request_counts();
        if self.manifest.tombstone_id_count == 0 {
            return Ok(PurgeReport {
                requests: self.storage.request_counts().delta(&requests_before),
                ..PurgeReport::default()
            });
        }
        let tombstoned = usize::try_from(self.manifest.tombstone_id_count).unwrap_or(usize::MAX);

        // Inspect active segments one at a time. Unaffected immutable objects are
        // reused verbatim; a segment containing suppressed rows is filtered and
        // replaced before the next segment is decoded. This bounds the purge
        // working set by one dimension-sized cell instead of retaining the whole
        // corpus (and avoids rewriting clean cells).
        let active = self.active_segment_summaries()?;
        let previous = self.manifest.clone();
        let mut manifest = self.manifest.next_version();
        manifest.segments = Vec::with_capacity(active.len());
        let mut segments_rewritten = 0_usize;
        let mut records_purged = 0_usize;
        for summary in &active {
            let (segment, _, _, _) = self.read_segment_for_rewrite(summary)?;
            let before = segment.records.len();
            let mut kept = Vec::with_capacity(before);
            for record in segment.records {
                if self.is_suppressed(&record)? {
                    records_purged += 1;
                } else {
                    kept.push(record);
                }
            }
            if kept.len() == before {
                manifest.segments.push(summary.clone());
                continue;
            }
            segments_rewritten += 1;
            self.repopulate_sparse_named_records(&mut kept, std::slice::from_ref(summary))?;
            sort_records_by_vector_locality(
                &mut kept,
                self.manifest.config.dimensions,
                self.manifest.config.segment_max_vectors,
            );
            for chunk in kept.chunks(self.manifest.config.segment_max_vectors) {
                let segment = Segment::from_records_with_quantizer_and_geometry(
                    Uuid::new_v4().to_string(),
                    summary.level,
                    self.manifest.config.metric.clone(),
                    self.manifest.config.dimensions,
                    chunk.to_vec(),
                    self.manifest.build_config.quantizer,
                    self.manifest
                        .build_config
                        .normalized_angular_coarse_geometry,
                )?;
                manifest.segments.push(self.write_segment(segment)?);
            }
        }
        // Publish the reused/replacement summary set without a tombstone. Even
        // when no physical row was present, this version clears the logical
        // overlay so the deleted ids become addable again.
        manifest.rebuild_pivots();
        manifest.tombstone = None;
        manifest.tombstone_frontier.clear();
        manifest.tombstone_pages.clear();
        manifest.tombstone_id_count = 0;
        manifest.bm25_stats_delta = None;
        manifest.bm25_stats_delta_frontier.clear();
        let global_pq_summaries = manifest.segments.clone();
        manifest.global_pq_ref = self.persist_resident_global_pq(&global_pq_summaries)?;
        enforce_ram_budget(&manifest, self.runtime_ram_budget_bytes)?;
        // Generation shard counters are shared monotonic allocators and must
        // survive purge. They contain no record ownership or visibility state;
        // retaining them prevents a later upsert from reusing a generation that
        // a concurrent or delayed reader may already have observed.
        self.manifest =
            self.publish_manifest_reusing_routing_pages_with_recovery(manifest, Some(&previous))?;
        // Purge rebuilt the cell layout; refresh the persisted cold quantizer.
        self.refresh_persisted_quantizer()?;

        Ok(PurgeReport {
            segments_rewritten,
            records_purged,
            tombstones_cleared: tombstoned,
            published: true,
            requests: self.storage.request_counts().delta(&requests_before),
        })
    }

    /// Spawn a background thread that opens its own handle on `uri` and runs
    /// [`BorsukIndex::run_maintenance_once`] every `interval` until the returned
    /// [`MaintenanceHandle`] is stopped or dropped. Coordination with other
    /// instances is automatic through the S3 membership and lease objects. Errors
    /// in a pass are swallowed and retried on the next tick so a transient storage
    /// hiccup does not kill the loop.
    pub fn start_background_maintenance(
        uri: impl Into<String>,
        open_options: OpenOptions,
        config: MaintenanceConfig,
        interval: Duration,
    ) -> MaintenanceHandle {
        use std::sync::atomic::{AtomicBool, Ordering};
        let uri = uri.into();
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let join = std::thread::spawn(move || {
            let Ok(mut index) = Self::open_with_options(&uri, open_options) else {
                return;
            };
            while !thread_stop.load(Ordering::Relaxed) {
                let _ = index.run_maintenance_once(&config);
                let step = Duration::from_millis(100);
                let mut slept = Duration::ZERO;
                while slept < interval && !thread_stop.load(Ordering::Relaxed) {
                    let nap = step.min(interval - slept);
                    std::thread::sleep(nap);
                    slept += nap;
                }
            }
        });
        MaintenanceHandle::new(stop, join)
    }

    /// Run one coordinated maintenance pass, sharing compaction, purge, and GC
    /// with any other live instances of this index through S3 membership and lease
    /// objects. This instance heartbeats, learns the live membership, and runs
    /// only the maintenance units in its shard, each guarded by a lease so two
    /// instances do not duplicate the same work. Safe to call from a scheduler.
    pub fn run_maintenance_once(
        &mut self,
        config: &MaintenanceConfig,
    ) -> Result<MaintenanceReport> {
        let now = Utc::now().timestamp_millis();
        let ttl_ms = i64::try_from(config.lease_ttl.as_millis()).unwrap_or(i64::MAX);

        // Refresh to the current published version so sharded work builds on the
        // latest state instead of this handle's possibly stale manifest (another
        // instance may have published since this handle last read).
        self.refresh()?;
        maintenance::heartbeat(&self.storage, &config.instance_id, now)?;
        let active = maintenance::active_instances(&self.storage, ttl_ms, now)?;
        let (rank, count) = maintenance::shard_rank(&active, &config.instance_id)
            .unwrap_or((0, active.len().max(1)));

        let mut report = MaintenanceReport {
            active_instances: count,
            instance_rank: rank,
            ..MaintenanceReport::default()
        };

        // Each maintenance kind is one sharded, leased unit of work. With a single
        // live instance it runs all of them; with several, the S3 leases and shard
        // hashing spread the work and let a healthy instance take over a dead one's
        // share once its lease expires.
        // Incremental split/merge is sharded by segment, so every live instance
        // runs it in parallel on its own disjoint slice of bubbles — no single
        // "who compacts" lease. Rebase-safe publishing composes the concurrent
        // manifest updates.
        if config.incremental {
            report.incremental = self
                .run_incremental_maintenance_sharded(
                    IncrementalMaintenanceOptions::default(),
                    Some((rank, count)),
                )?
                .published;
        }
        if config.compaction && maintenance::owns_shard("compact", rank, count) {
            let compacted =
                self.run_leased_unit(config, "compact", ttl_ms, now, &mut report, |index| {
                    Ok(index
                        .compact_primary(CompactionOptions::default())?
                        .compacted)
                })?;
            report.compacted = compacted;
        }
        if config.purge
            && self.manifest.tombstone_id_count > 0
            && maintenance::owns_shard("purge", rank, count)
        {
            let purged =
                self.run_leased_unit(config, "purge", ttl_ms, now, &mut report, |index| {
                    Ok(index.purge_primary_with_report()?.published)
                })?;
            report.purged = purged;
        }
        if config.garbage_collection && maintenance::owns_shard("gc", rank, count) {
            let collected =
                self.run_leased_unit(config, "gc", ttl_ms, now, &mut report, |index| {
                    let gc = index.gc_obsolete_segments_primary(GarbageCollectionOptions {
                        dry_run: false,
                        min_age: config.lease_ttl,
                    })?;
                    Ok(!gc.dry_run)
                })?;
            report.garbage_collected = collected;
        }
        for child in self.named.values_mut() {
            child.run_maintenance_once(config)?;
        }
        Ok(report)
    }

    /// Acquire the lease for `key`, run `work`, and release the lease. Returns the
    /// work result, or `false` (recording contention) if another instance holds
    /// the lease.
    fn run_leased_unit(
        &mut self,
        config: &MaintenanceConfig,
        key: &str,
        ttl_ms: i64,
        now_ms: i64,
        report: &mut MaintenanceReport,
        work: impl FnOnce(&mut Self) -> Result<bool>,
    ) -> Result<bool> {
        if !maintenance::acquire_lease(&self.storage, key, &config.instance_id, ttl_ms, now_ms)? {
            report.leases_contended += 1;
            return Ok(false);
        }
        let outcome = work(self);
        let _ = maintenance::release_lease(&self.storage, key);
        outcome
    }

    /// Run one incremental-maintenance pass: split oversized bubbles and merge
    /// sparse ones locally, touching only the affected segments (SPFresh/LIRE
    /// style) rather than rewriting whole levels.
    ///
    /// Splitting turns a segment that holds too many vectors — or whose bubble
    /// radius grew too wide — into several tighter bubbles. Merging folds a
    /// segment whose live count fell below the threshold (typically from deletes)
    /// into its nearest neighbour, dropping tombstoned rows in the process so
    /// delete-driven reclaim is local too. The pass is bounded by
    /// `max_operations`, and republishing reuses every unchanged routing page by
    /// content address, so an incremental pass is O(touched), not O(index).
    pub fn run_incremental_maintenance(
        &mut self,
        options: IncrementalMaintenanceOptions,
    ) -> Result<IncrementalReport> {
        self.run_incremental_maintenance_sharded(options, None)
    }

    /// Run incremental maintenance on one shard of `count` — for schedulers that
    /// drive their own fixed pool of nodes and want each node to compact a
    /// disjoint slice of the bubbles in parallel. `rank` must be in `0..count`.
    /// Prefer [`BorsukIndex::start_background_maintenance`], which derives the
    /// shard from the live membership automatically.
    pub fn run_incremental_maintenance_shard(
        &mut self,
        options: IncrementalMaintenanceOptions,
        rank: usize,
        count: usize,
    ) -> Result<IncrementalReport> {
        let shard = (count > 1).then_some((rank.min(count.saturating_sub(1)), count));
        self.run_incremental_maintenance_sharded(options, shard)
    }

    /// Incremental maintenance restricted to this node's segment shard, so many
    /// instances can compact disjoint bubbles in parallel. `shard` is
    /// `(rank, active_instances)`; a segment is handled only when its id hashes to
    /// this rank, and merges pick a neighbour from the same shard so two nodes
    /// never rewrite the same segment. Changes are collected as a segment delta
    /// (ids removed, summaries added) and published with a rebase-safe retry loop,
    /// so concurrent publishes from other nodes compose instead of clobbering.
    pub(crate) fn run_incremental_maintenance_sharded(
        &mut self,
        options: IncrementalMaintenanceOptions,
        shard: Option<(usize, usize)>,
    ) -> Result<IncrementalReport> {
        let requests_before = self.storage.request_counts();
        self.refresh()?;
        let dimensions = self.manifest.config.dimensions;
        let metric = self.manifest.config.metric.clone();
        let in_shard =
            |id: &str| shard.is_none_or(|(rank, count)| maintenance::owns_shard(id, rank, count));

        // Paged manifests intentionally keep the full segment table out of
        // resident metadata. Maintenance must operate on the resolved active
        // set, not only on the (possibly empty) resident summary vector.
        let mut working = self.active_segment_summaries()?;
        let mut removed: HashSet<String> = HashSet::new();
        let mut added: Vec<SegmentSummary> = Vec::new();
        let mut report = IncrementalReport::default();
        let mut ops = 0_usize;

        // Split pass: oversized in-shard bubbles become tighter pieces.
        let mut index = 0;
        while index < working.len() && ops < options.max_operations {
            let summary = working[index].clone();
            let too_many = summary.object_count > options.max_segment_vectors;
            let too_wide = options
                .max_segment_radius
                .is_some_and(|max| summary.radius > max);
            if !in_shard(&summary.id) || !(too_many || too_wide) {
                index += 1;
                continue;
            }

            let (segment, _, _, _) = self.read_segment_for_rewrite(&summary)?;
            let mut records = segment.records;
            self.repopulate_sparse_named_records(&mut records, std::slice::from_ref(&summary))?;
            let records = self.retain_live_records(records)?;
            let pieces = if too_many {
                records.len().div_ceil(options.max_segment_vectors.max(1))
            } else {
                1
            };
            let effective_max = if pieces > 1 {
                records.len().div_ceil(pieces).max(1)
            } else {
                options.max_segment_vectors.max(1)
            };
            let chunks =
                adaptive_chunks(records, &metric, effective_max, options.max_segment_radius)?;
            if chunks.len() <= 1 {
                index += 1;
                continue;
            }

            working.remove(index);
            Self::stage_removal(&mut removed, &mut added, &summary.id);
            report.segments_removed += 1;
            for chunk in chunks {
                report.records_moved += chunk.len();
                let segment = Segment::from_records_with_quantizer_and_geometry(
                    Uuid::new_v4().to_string(),
                    summary.level,
                    metric.clone(),
                    dimensions,
                    chunk,
                    self.manifest.build_config.quantizer,
                    self.manifest
                        .build_config
                        .normalized_angular_coarse_geometry,
                )?;
                let written = self.write_segment(segment)?;
                added.push(written.clone());
                working.insert(index, written);
                report.segments_created += 1;
                index += 1;
            }
            report.splits += 1;
            ops += 1;
        }

        // Merge pass: sparse in-shard bubbles fold into an in-shard neighbour.
        if ops < options.max_operations {
            let mut sparse: Vec<String> = Vec::new();
            for summary in &working {
                if in_shard(&summary.id)
                    && summary.object_count <= options.min_segment_vectors.saturating_mul(2)
                {
                    let (segment, _, _, _) = self.read_segment_for_rewrite(summary)?;
                    if self.live_record_count(&segment)? < options.min_segment_vectors {
                        sparse.push(summary.id.clone());
                    }
                }
            }
            for id in sparse {
                if ops >= options.max_operations {
                    break;
                }
                let Some(pos) = working.iter().position(|summary| summary.id == id) else {
                    continue;
                };
                let level = working[pos].level;
                let centroid = working[pos].centroid.clone();
                // Only merge with a neighbour from the same shard so two nodes
                // never rewrite the same segment.
                let neighbour = working
                    .iter()
                    .enumerate()
                    .filter(|(other, summary)| {
                        *other != pos && summary.level == level && in_shard(&summary.id)
                    })
                    .filter_map(|(other, summary)| {
                        // Both centroids are stored, already-validated vectors —
                        // skip the finite/dim re-scan (degeneracy errors preserved).
                        metric
                            .centroid_geometry_distance_unchecked(&centroid, &summary.centroid)
                            .ok()
                            .map(|distance| (other, distance))
                    })
                    .min_by(|(_, a), (_, b)| a.total_cmp(b))
                    .map(|(other, _)| other);
                let Some(neighbour) = neighbour else {
                    continue;
                };

                let sparse_id = working[pos].id.clone();
                let neighbour_id = working[neighbour].id.clone();
                let (sparse_segment, _, _, _) = self.read_segment_for_rewrite(&working[pos])?;
                let (neighbour_segment, _, _, _) =
                    self.read_segment_for_rewrite(&working[neighbour])?;
                let source_summaries = [working[pos].clone(), working[neighbour].clone()];
                let mut combined = sparse_segment
                    .records
                    .into_iter()
                    .chain(neighbour_segment.records)
                    .collect::<Vec<_>>();
                self.repopulate_sparse_named_records(&mut combined, &source_summaries)?;
                let combined = self.retain_live_records(combined)?;
                let chunks = adaptive_chunks(
                    combined,
                    &metric,
                    options.max_segment_vectors.max(1),
                    options.max_segment_radius,
                )?;

                let (high, low) = if pos > neighbour {
                    (pos, neighbour)
                } else {
                    (neighbour, pos)
                };
                working.remove(high);
                working.remove(low);
                Self::stage_removal(&mut removed, &mut added, &sparse_id);
                Self::stage_removal(&mut removed, &mut added, &neighbour_id);
                report.segments_removed += 2;
                for chunk in chunks {
                    report.records_moved += chunk.len();
                    let segment = Segment::from_records_with_quantizer_and_geometry(
                        Uuid::new_v4().to_string(),
                        level,
                        metric.clone(),
                        dimensions,
                        chunk,
                        self.manifest.build_config.quantizer,
                        self.manifest
                            .build_config
                            .normalized_angular_coarse_geometry,
                    )?;
                    let written = self.write_segment(segment)?;
                    added.push(written.clone());
                    working.push(written);
                    report.segments_created += 1;
                }
                report.merges += 1;
                ops += 1;
            }
        }

        if !removed.is_empty() || !added.is_empty() {
            report.published = self.publish_segment_delta(&removed, &added)?;
        }
        report.requests = self.storage.request_counts().delta(&requests_before);
        Ok(report)
    }

    /// Publish a segment delta (`removed` ids dropped, `added` summaries appended)
    /// on top of the latest published manifest, retrying on a concurrent publish
    /// by re-reading `CURRENT` and re-applying the delta. Because the delta only
    /// touches this node's disjoint segments, re-applying it onto another node's
    /// concurrent change composes cleanly. Returns `false` if it could not win the
    /// compare-and-swap within the retry budget (the pass is retried next cycle).
    fn publish_segment_delta(
        &mut self,
        removed: &HashSet<String>,
        added: &[SegmentSummary],
    ) -> Result<bool> {
        const MAX_PUBLISH_ATTEMPTS: usize = 8;
        for _ in 0..MAX_PUBLISH_ATTEMPTS {
            self.refresh()?;
            let previous = self.manifest.clone();
            let active_segments = self.active_segment_summaries()?;
            let mut manifest = self.manifest.next_version();
            manifest.segments = active_segments
                .into_iter()
                .filter(|summary| !removed.contains(&summary.id))
                .collect();
            manifest.segments.extend(added.iter().cloned());
            manifest.rebuild_pivots();
            self.rebuild_lexical_roots(&mut manifest)?;
            enforce_ram_budget(&manifest, self.runtime_ram_budget_bytes)?;
            match self.publish_manifest_reusing_routing_pages_with_recovery_report(
                manifest,
                Some(&previous),
            ) {
                Ok((published, _report)) => {
                    self.manifest = published;
                    return Ok(true);
                }
                Err(BorsukError::ConcurrentModification { .. }) => continue,
                Err(err) => return Err(err),
            }
        }
        Ok(false)
    }

    /// Stage a segment id for removal from the base manifest. If the id names a
    /// segment created earlier in this same pass (a transient split/merge output
    /// that a later merge consumed), drop it from `added` instead so it never
    /// reaches the published manifest; otherwise record it in `removed`.
    fn stage_removal(removed: &mut HashSet<String>, added: &mut Vec<SegmentSummary>, id: &str) {
        if let Some(position) = added.iter().position(|summary| summary.id == id) {
            added.remove(position);
        } else {
            removed.insert(id.to_string());
        }
    }

    /// Keep only the records that are not tombstoned.
    fn retain_live_records(&self, records: Vec<VectorRecord>) -> Result<Vec<VectorRecord>> {
        let mut live = Vec::with_capacity(records.len());
        for record in records {
            if !self.is_suppressed(&record)? {
                live.push(record);
            }
        }
        Ok(live)
    }

    /// Count the live (non-tombstoned) records in a decoded segment.
    fn live_record_count(&self, segment: &Segment) -> Result<usize> {
        let mut live = 0;
        for record in &segment.records {
            if !self.is_suppressed(record)? {
                live += 1;
            }
        }
        Ok(live)
    }

    /// Write one sorted tombstone-delta `(id, min_visible_generation)` run and
    /// return its summary, or `None` when the batch is empty.
    fn write_tombstone(&self, deleted: BTreeMap<Vec<u8>, u64>) -> Result<Option<TombstoneSummary>> {
        if deleted.is_empty() {
            return Ok(None);
        }
        // BTreeMap already yields ids in sorted order.
        let entries: Vec<(Vec<u8>, u64)> = deleted.into_iter().collect();
        let bytes = tombstone_ids_to_parquet(&entries)?;
        let checksum = blake3::hash(&bytes).to_hex().to_string();
        let path = Manifest::tombstone_content_file_name(&checksum);
        self.storage.write_bytes(&path, &bytes)?;
        let decoded = Arc::new(entries.iter().cloned().collect::<TombstoneOverlay>());
        self.tombstone_cache.insert(
            checksum.clone(),
            Arc::clone(&decoded),
            decoded_tombstone_overlay_bytes(&decoded),
        );
        Ok(Some(TombstoneSummary {
            id_bloom: segment_id_bloom(entries.iter().map(|(id, _)| id)),
            count: entries.len() as u64,
            path,
            checksum,
            created_at: Utc::now(),
        }))
    }

    /// Load and cache the merged stable tombstone plus foreground delta runs.
    /// Runs are applied in manifest order and generations merge by maximum.
    fn tombstone_overlay_for_manifest(
        &self,
        manifest: &Manifest,
    ) -> Result<Option<Arc<TombstoneOverlay>>> {
        let cell_tombstones = self.cell_wal_tombstone_summaries()?;
        if manifest.tombstone.is_none()
            && manifest.tombstone_pages.is_empty()
            && manifest.tombstone_frontier.is_empty()
            && cell_tombstones.is_empty()
        {
            return Ok(None);
        }
        let tombstones = manifest
            .tombstone
            .iter()
            .chain(&manifest.tombstone_frontier)
            .chain(&cell_tombstones)
            .collect::<Vec<_>>();
        let mut merged = HashMap::new();
        for tombstone in tombstones {
            for (id, generation) in self.load_tombstone_run(tombstone)?.iter() {
                let entry = merged.entry(id.clone()).or_insert(0_u64);
                *entry = (*entry).max(*generation);
            }
        }
        for tombstone in &manifest.tombstone_pages {
            for (id, generation) in self.load_tombstone_page(tombstone)?.iter() {
                let entry = merged.entry(id.clone()).or_insert(0_u64);
                *entry = (*entry).max(*generation);
            }
        }
        Ok(Some(Arc::new(merged)))
    }

    fn load_tombstone_run(&self, tombstone: &TombstoneSummary) -> Result<Arc<TombstoneOverlay>> {
        self.load_tombstone_object(&tombstone.path, &tombstone.checksum)
    }

    fn load_tombstone_page(&self, tombstone: &TombstonePageRef) -> Result<Arc<TombstoneOverlay>> {
        self.load_tombstone_object(&tombstone.path, &tombstone.checksum)
    }

    fn load_tombstone_object(&self, path: &str, checksum: &str) -> Result<Arc<TombstoneOverlay>> {
        if let Some(run) = self.tombstone_cache.get(checksum) {
            return Ok(run);
        }
        let read = self
            .storage
            .read_bytes_with_cache_status_and_checksum(path, checksum)?;
        let run = Arc::new(
            tombstone_ids_from_parquet(&read.bytes)?
                .into_iter()
                .collect::<HashMap<_, _>>(),
        );
        self.tombstone_cache.insert(
            checksum.to_string(),
            Arc::clone(&run),
            decoded_tombstone_overlay_bytes(&run),
        );
        Ok(run)
    }

    fn load_bm25_stats_delta_for_terms(
        &self,
        terms: &BTreeSet<u32>,
    ) -> Result<(i64, i64, BTreeMap<u32, i64>, u64)> {
        let mut references = self
            .manifest
            .bm25_stats_delta
            .iter()
            .chain(&self.manifest.bm25_stats_delta_frontier)
            .cloned()
            .collect::<Vec<_>>();
        for transaction in &self.cell_wal_snapshot {
            if let Some(reference) = Self::cell_wal_metadata(transaction)?.bm25_stats_delta {
                references.push(reference);
            }
        }
        if references.is_empty() {
            return Ok((0, 0, BTreeMap::new(), 0));
        }
        let mut deltas = BTreeMap::new();
        let mut bytes_read = 0_u64;
        let mut document_count_delta = 0_i64;
        let mut total_document_length_delta = 0_i64;
        for reference in &references {
            document_count_delta = document_count_delta
                .checked_add(reference.document_count_delta)
                .ok_or_else(|| {
                    BorsukError::InvalidStorage("BM25 document-count delta exceeds i64".to_string())
                })?;
            total_document_length_delta = total_document_length_delta
                .checked_add(reference.total_document_length_delta)
                .ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "BM25 total-document-length delta exceeds i64".to_string(),
                    )
                })?;
            for page in &reference.pages {
                if terms
                    .range(page.first_term..=page.last_term)
                    .next()
                    .is_none()
                {
                    continue;
                }
                let (entries, physical_bytes, _) = self.read_bm25_stats_delta_page(page)?;
                bytes_read = bytes_read.saturating_add(physical_bytes);
                for (term, delta) in entries.iter().copied() {
                    if terms.contains(&term) {
                        let accumulated = deltas.entry(term).or_insert(0_i64);
                        *accumulated = accumulated.checked_add(delta).ok_or_else(|| {
                            BorsukError::InvalidStorage(
                                "BM25 document-frequency delta exceeds i64".to_string(),
                            )
                        })?;
                    }
                }
            }
        }
        Ok((
            document_count_delta,
            total_document_length_delta,
            deltas,
            bytes_read,
        ))
    }

    fn read_bm25_stats_delta_page(
        &self,
        page: &Bm25StatsDeltaPageRef,
    ) -> Result<(Arc<Bm25StatsPage>, u64, bool)> {
        if let Some(entries) = self.decoded_bm25_stats_pages.get(&page.checksum) {
            return Ok((entries, 0, true));
        }
        let result = self.inflight_bm25_stats_pages.load(&page.checksum, || {
            let read = self
                .storage
                .read_bytes_with_cache_status_and_checksum(&page.path, &page.checksum)?;
            let entries = bm25_stats_delta_page_from_parquet(&read.bytes)?;
            if entries.len() != page.term_count as usize
                || entries.first().map(|entry| entry.0) != Some(page.first_term)
                || entries.last().map(|entry| entry.0) != Some(page.last_term)
            {
                return Err(BorsukError::InvalidStorage(format!(
                    "BM25 statistics-delta page `{}` differs from its reference",
                    page.path
                )));
            }
            Ok((entries, read.bytes.len() as u64))
        })?;
        let decoded_bytes = result
            .0
            .capacity()
            .saturating_mul(std::mem::size_of::<(u32, i64)>()) as u64;
        self.decoded_bm25_stats_pages.insert(
            page.checksum.clone(),
            Arc::clone(&result.0),
            decoded_bytes,
        );
        Ok(result)
    }

    fn persist_bm25_stats_delta(
        &self,
        delta: &Bm25StatsDelta,
    ) -> Result<Option<Bm25StatsDeltaRef>> {
        if delta.is_empty() {
            return Ok(None);
        }
        if delta.document_count > 0
            || delta.total_document_length > 0
            || delta.document_frequencies.values().any(|value| *value > 0)
        {
            return Err(BorsukError::InvalidStorage(
                "BM25 statistics delta may only suppress physical generations".to_string(),
            ));
        }
        let entries = delta
            .document_frequencies
            .iter()
            .map(|(term, correction)| (*term, *correction))
            .collect::<Vec<_>>();
        let pages = self.write_bm25_stats_delta_pages(&entries)?;
        Ok(Some(Bm25StatsDeltaRef {
            document_count_delta: delta.document_count,
            total_document_length_delta: delta.total_document_length,
            pages,
        }))
    }

    fn load_bm25_stats_delta_ref(&self, reference: &Bm25StatsDeltaRef) -> Result<Bm25StatsDelta> {
        let mut document_frequencies = BTreeMap::new();
        for page in &reference.pages {
            let (entries, _, _) = self.read_bm25_stats_delta_page(page)?;
            for (term, correction) in entries.iter().copied() {
                let entry = document_frequencies.entry(term).or_insert(0_i64);
                *entry = entry.checked_add(correction).ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "BM25 document-frequency delta exceeds i64".to_string(),
                    )
                })?;
            }
        }
        Ok(Bm25StatsDelta {
            document_count: reference.document_count_delta,
            total_document_length: reference.total_document_length_delta,
            document_frequencies,
        })
    }

    fn consolidate_mutation_frontiers(
        &self,
        manifest: &mut Manifest,
        lexical_roots_will_rebuild: bool,
    ) -> Result<()> {
        if manifest.tombstone.is_some() || !manifest.tombstone_frontier.is_empty() {
            let mut updates = BTreeMap::<u16, BTreeMap<Vec<u8>, u64>>::new();
            for run in manifest
                .tombstone
                .iter()
                .chain(&manifest.tombstone_frontier)
            {
                for (id, generation) in self.load_tombstone_run(run)?.iter() {
                    let entry = updates
                        .entry(tombstone_bucket(id))
                        .or_default()
                        .entry(id.clone())
                        .or_insert(0);
                    *entry = (*entry).max(*generation);
                }
            }
            let mut pages = manifest
                .tombstone_pages
                .iter()
                .cloned()
                .map(|page| (page.bucket, page))
                .collect::<BTreeMap<_, _>>();
            for (bucket, mut changes) in updates {
                if let Some(previous) = pages.get(&bucket) {
                    for (id, generation) in self.load_tombstone_page(previous)?.iter() {
                        let entry = changes.entry(id.clone()).or_insert(0);
                        *entry = (*entry).max(*generation);
                    }
                }
                let summary = self.write_tombstone(changes)?.ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "non-empty tombstone bucket produced no page".to_string(),
                    )
                })?;
                pages.insert(
                    bucket,
                    TombstonePageRef {
                        bucket,
                        path: summary.path,
                        checksum: summary.checksum,
                        count: summary.count,
                        created_at: summary.created_at,
                    },
                );
            }
            manifest.tombstone = None;
            manifest.tombstone_frontier.clear();
            manifest.tombstone_pages = pages.into_values().collect();
        }
        if lexical_roots_will_rebuild {
            // The rebuild derives corrections from the now-consolidated
            // tombstone and the final physical segment set. This also catches
            // superseded generations that previously lived only in the WAL.
            manifest.bm25_stats_delta_frontier.clear();
            return Ok(());
        }
        if manifest.bm25_stats_delta_frontier.is_empty() {
            return Ok(());
        }
        let mut combined = Bm25StatsDelta::default();
        for reference in &manifest.bm25_stats_delta_frontier {
            let delta = self.load_bm25_stats_delta_ref(reference)?;
            combined.document_count = combined
                .document_count
                .checked_add(delta.document_count)
                .ok_or_else(|| {
                    BorsukError::InvalidStorage("BM25 document-count delta exceeds i64".to_string())
                })?;
            combined.total_document_length = combined
                .total_document_length
                .checked_add(delta.total_document_length)
                .ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "BM25 total-document-length delta exceeds i64".to_string(),
                    )
                })?;
            for (term, correction) in delta.document_frequencies {
                let entry = combined.document_frequencies.entry(term).or_insert(0_i64);
                *entry = entry.checked_add(correction).ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "BM25 document-frequency delta exceeds i64".to_string(),
                    )
                })?;
            }
        }
        manifest.bm25_stats_delta =
            self.update_bm25_stats_delta_from(manifest.bm25_stats_delta.as_ref(), &combined)?;
        manifest.bm25_stats_delta_frontier.clear();
        Ok(())
    }

    fn write_bm25_stats_delta_pages(
        &self,
        entries: &[(u32, i64)],
    ) -> Result<Vec<Bm25StatsDeltaPageRef>> {
        let mut pages = Vec::new();
        for chunk in entries.chunks(DEFAULT_LEXICAL_TERM_PAGE_ENTRIES) {
            let bytes = bm25_stats_delta_page_to_parquet(chunk)?;
            let checksum = blake3::hash(&bytes).to_hex().to_string();
            let first_term = chunk[0].0;
            let last_term = chunk[chunk.len() - 1].0;
            let path = format!(
                "lexical/stats-delta/{}/stats-{}-{}-{}.parquet",
                &checksum[..2],
                first_term,
                last_term,
                &checksum[..12],
            );
            self.storage.write_bytes_content_addressed(&path, &bytes)?;
            let decoded = Arc::new(chunk.to_vec());
            self.decoded_bm25_stats_pages.insert(
                checksum.clone(),
                decoded,
                chunk
                    .len()
                    .saturating_mul(std::mem::size_of::<(u32, i64)>()) as u64,
            );
            pages.push(Bm25StatsDeltaPageRef {
                first_term,
                last_term,
                path,
                checksum,
                encoded_bytes: bytes.len() as u64,
                term_count: u32::try_from(chunk.len()).map_err(|_| {
                    BorsukError::InvalidStorage(
                        "BM25 statistics-delta page term count exceeds u32".to_string(),
                    )
                })?,
            });
        }
        Ok(pages)
    }

    /// Apply one mutation batch by copy-on-writing only the bounded delta pages
    /// whose term ranges intersect the batch. Unaffected pages remain
    /// content-addressed references, so update/delete cost does not grow with
    /// accumulated vocabulary.
    fn update_bm25_stats_delta_from(
        &self,
        previous: Option<&Bm25StatsDeltaRef>,
        change: &Bm25StatsDelta,
    ) -> Result<Option<Bm25StatsDeltaRef>> {
        if change.is_empty() {
            return Ok(previous.cloned());
        }
        let document_count_delta = previous
            .map_or(0, |value| value.document_count_delta)
            .checked_add(change.document_count)
            .ok_or_else(|| {
                BorsukError::InvalidStorage("BM25 document-count delta exceeds i64".to_string())
            })?;
        let total_document_length_delta = previous
            .map_or(0, |value| value.total_document_length_delta)
            .checked_add(change.total_document_length)
            .ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "BM25 total-document-length delta exceeds i64".to_string(),
                )
            })?;
        let mut changes = change.document_frequencies.clone();
        let mut pages = Vec::new();
        for page in previous.into_iter().flat_map(|value| &value.pages) {
            let before = changes
                .range(..page.first_term)
                .map(|(term, delta)| (*term, *delta))
                .collect::<Vec<_>>();
            for (term, _) in &before {
                changes.remove(term);
            }
            pages.extend(self.write_bm25_stats_delta_pages(&before)?);

            let page_changes = changes
                .range(page.first_term..=page.last_term)
                .map(|(term, delta)| (*term, *delta))
                .collect::<Vec<_>>();
            if page_changes.is_empty() {
                pages.push(page.clone());
                continue;
            }
            for (term, _) in &page_changes {
                changes.remove(term);
            }
            let (old_entries, _, _) = self.read_bm25_stats_delta_page(page)?;
            let mut merged = old_entries.iter().copied().collect::<BTreeMap<_, _>>();
            for (term, correction) in page_changes {
                let updated = merged
                    .get(&term)
                    .copied()
                    .unwrap_or_default()
                    .checked_add(correction)
                    .ok_or_else(|| {
                        BorsukError::InvalidStorage(
                            "BM25 document-frequency delta exceeds i64".to_string(),
                        )
                    })?;
                if updated == 0 {
                    merged.remove(&term);
                } else {
                    merged.insert(term, updated);
                }
            }
            let merged = merged.into_iter().collect::<Vec<_>>();
            pages.extend(self.write_bm25_stats_delta_pages(&merged)?);
        }
        let trailing = changes.into_iter().collect::<Vec<_>>();
        pages.extend(self.write_bm25_stats_delta_pages(&trailing)?);
        pages.sort_by_key(|page| page.first_term);
        if document_count_delta == 0 && total_document_length_delta == 0 && pages.is_empty() {
            return Ok(None);
        }
        if document_count_delta > 0
            || total_document_length_delta > 0
            || pages.iter().any(|page| page.term_count == 0)
        {
            return Err(BorsukError::InvalidStorage(
                "invalid positive BM25 physical-statistics correction".to_string(),
            ));
        }
        Ok(Some(Bm25StatsDeltaRef {
            document_count_delta,
            total_document_length_delta,
            pages,
        }))
    }

    fn rebuild_bm25_stats_delta_from_segments(
        &self,
        manifest: &Manifest,
    ) -> Result<Option<Bm25StatsDeltaRef>> {
        if !manifest.config.text {
            return Ok(None);
        }
        let Some(overlay) = self.tombstone_overlay_for_manifest(manifest)? else {
            return Ok(None);
        };
        let mut delta = Bm25StatsDelta::default();
        for summary in &manifest.segments {
            if summary.text_doc_count == 0
                || !overlay.keys().any(|id| summary.might_contain_record_id(id))
            {
                continue;
            }
            // The lean segment projection carries ids, generations, and text
            // terms but not dense sidecars, so reconciliation cost scales with
            // tombstone-matching cells rather than vector dimensions.
            let (segment, _) = self.read_segment_lean_ranged(summary)?;
            for record in &segment.records {
                if overlay
                    .get(record.id.as_bytes())
                    .is_some_and(|minimum| record.generation < *minimum)
                    && let Some(terms) = record_text_terms(record)
                {
                    delta.suppress_document(&terms)?;
                }
            }
        }
        self.persist_bm25_stats_delta(&delta)
    }

    /// The minimum visible generation for `id`, or `None` when the id carries no
    /// tombstone entry. Bloom fast-path: an id absent from the tombstone bloom
    /// pays zero I/O.
    fn min_visible_generation(&self, id: &[u8]) -> Result<Option<u64>> {
        let mut minimum = None;
        let bucket = tombstone_bucket(id);
        if let Ok(index) = self
            .manifest
            .tombstone_pages
            .binary_search_by_key(&bucket, |page| page.bucket)
            && let Some(generation) = self
                .load_tombstone_page(&self.manifest.tombstone_pages[index])?
                .get(id)
                .copied()
        {
            minimum = Some(generation);
        }
        for tombstone in self
            .manifest
            .tombstone
            .iter()
            .chain(&self.manifest.tombstone_frontier)
        {
            if !tombstone.might_contain_record_id(id) {
                continue;
            }
            if let Some(generation) = self.load_tombstone_run(tombstone)?.get(id).copied() {
                minimum = Some(minimum.map_or(generation, |current: u64| current.max(generation)));
            }
        }
        for tombstone in self.cell_wal_tombstone_summaries()? {
            if !tombstone.might_contain_record_id(id) {
                continue;
            }
            if let Some(generation) = self.load_tombstone_run(&tombstone)?.get(id).copied() {
                minimum = Some(minimum.map_or(generation, |current: u64| current.max(generation)));
            }
        }
        Ok(minimum)
    }

    /// Whether `id` carries any tombstone entry (deleted or superseded by a
    /// newer upsert). Used where the caller only has an id, not a record.
    fn id_is_tombstoned(&self, id: &[u8]) -> Result<bool> {
        Ok(self.min_visible_generation(id)?.is_some())
    }

    /// Whether a stored record is suppressed: its id has a tombstone entry and
    /// the record's generation is below the id's minimum visible generation.
    /// The newest upsert (whose generation equals the entry) and untombstoned
    /// records stay visible.
    fn is_suppressed(&self, record: &VectorRecord) -> Result<bool> {
        match self.min_visible_generation(record.id.as_bytes())? {
            Some(min_visible) => Ok(record.generation < min_visible),
            None => Ok(false),
        }
    }

    /// Resolve the newest live copy of every requested id with one bounded pass
    /// over matching immutable segments plus one pass over the WAL tail.
    ///
    /// Delete batches use this instead of probing each id independently. Segment
    /// blooms still avoid unrelated cells, while every matching Parquet object is
    /// decoded at most once and only through the lean id/generation/text
    /// projection — dense sidecars are never materialized for membership checks.
    fn live_delete_records(
        &self,
        targets: &BTreeMap<Vec<u8>, u64>,
    ) -> Result<HashMap<Vec<u8>, LiveDeleteRecord>> {
        if targets.is_empty() {
            return Ok(HashMap::new());
        }
        let mut live = HashMap::with_capacity(targets.len());
        for summary in self.active_segment_summaries()? {
            if !targets.keys().any(|id| summary.might_contain_record_id(id)) {
                continue;
            }
            let (segment, _) = self.read_segment_lean_ranged(&summary)?;
            for record in &segment.records {
                let Some(threshold) = targets.get(record.id.as_bytes()) else {
                    continue;
                };
                if record.generation < *threshold {
                    continue;
                }
                let key = record.id.as_bytes().to_vec();
                let replace = live.get(&key).is_none_or(|current: &LiveDeleteRecord| {
                    record.generation >= current.generation
                });
                if replace {
                    live.insert(
                        key,
                        LiveDeleteRecord {
                            generation: record.generation,
                            text_terms: record_text_terms(record),
                            persisted: true,
                        },
                    );
                }
            }
        }
        // WAL entries are newer than immutable cells. Process them last so an
        // equal-generation tail record wins without entering persisted BM25
        // physical-statistics corrections.
        for record in self.wal_tail()?.iter() {
            let Some(threshold) = targets.get(record.id.as_bytes()) else {
                continue;
            };
            if record.generation < *threshold {
                continue;
            }
            let key = record.id.as_bytes().to_vec();
            let replace = live
                .get(&key)
                .is_none_or(|current| record.generation >= current.generation);
            if replace {
                live.insert(
                    key,
                    LiveDeleteRecord {
                        generation: record.generation,
                        text_terms: record_text_terms(record),
                        persisted: false,
                    },
                );
            }
        }
        Ok(live)
    }

    /// Remove logically deleted records from a compaction/purge input set,
    /// returning how many rows were dropped.
    fn drop_deleted_records(&self, records: &mut Vec<VectorRecord>) -> Result<usize> {
        if self.manifest.tombstone.is_none()
            && self.manifest.tombstone_pages.is_empty()
            && self.manifest.tombstone_frontier.is_empty()
        {
            return Ok(0);
        }
        let before = records.len();
        let mut kept = Vec::with_capacity(records.len());
        for record in records.drain(..) {
            if !self.is_suppressed(&record)? {
                kept.push(record);
            }
        }
        *records = kept;
        Ok(before - records.len())
    }

    fn named_records_for_add(
        &self,
        records: &[VectorRecord],
    ) -> Result<BTreeMap<String, Vec<VectorRecord>>> {
        let mut named_records = BTreeMap::<String, Vec<VectorRecord>>::new();
        for record in records {
            for (name, vector) in &record.extra_vectors {
                let Some(spec) = self.manifest.config.named_vectors.get(name) else {
                    return Err(BorsukError::InvalidRecordInput(format!(
                        "record `{}` carries undeclared named vector `{name}`",
                        record.id
                    )));
                };
                if spec.kind != VectorKind::Dense {
                    return Err(BorsukError::InvalidRecordInput(format!(
                        "record `{}` supplies a fixed dense vector for {:?} field `{name}`",
                        record.id, spec.kind
                    )));
                }
                if vector.len() != spec.dimensions {
                    return Err(BorsukError::InvalidRecordInput(format!(
                        "record `{}` named vector `{name}` has {} dimensions, expected {}",
                        record.id,
                        vector.len(),
                        spec.dimensions
                    )));
                }
                named_records.entry(name.clone()).or_default().push(
                    VectorRecord::new(record.id.clone(), vector.clone())
                        .with_metadata(record.metadata.clone()),
                );
            }
            for (name, matrix) in &record.extra_multi_vectors {
                let Some(spec) = self.manifest.config.named_vectors.get(name) else {
                    return Err(BorsukError::InvalidRecordInput(format!(
                        "record `{}` carries undeclared late-interaction vector `{name}`",
                        record.id
                    )));
                };
                if spec.kind != VectorKind::LateInteraction {
                    return Err(BorsukError::InvalidRecordInput(format!(
                        "record `{}` supplies late-interaction data for non-late vector `{name}`",
                        record.id
                    )));
                }
                for (token_index, token) in matrix.tokens().enumerate() {
                    named_records.entry(name.clone()).or_default().push(
                        VectorRecord::new_bytes(
                            encode_late_interaction_token_id(
                                record.id.as_bytes(),
                                record.generation,
                                token_index,
                            )?,
                            token.to_vec(),
                        )
                        .with_metadata(record.metadata.clone()),
                    );
                }
            }
        }
        Ok(named_records)
    }

    fn add_named_records(
        &mut self,
        named_records: BTreeMap<String, Vec<VectorRecord>>,
    ) -> Result<()> {
        for (name, records) in named_records {
            let child = self.named.get_mut(&name).ok_or_else(|| {
                BorsukError::InvalidRecordInput(format!(
                    "named vector `{name}` is declared but its sub-index is not open"
                ))
            })?;
            if records.is_empty() {
                continue;
            }
            let next_generated_id = next_generated_id_after_explicit_records(
                child.cell_wal_next_generated_id_floor()?,
                &records,
            )?;
            child.add_records_with_report(records, true, next_generated_id)?;
        }
        Ok(())
    }

    fn canonicalize_sparse_named_records(&self, records: &mut [VectorRecord]) -> Result<()> {
        for record in records {
            for (name, vector) in &mut record.extra_sparse {
                let Some(spec) = self.manifest.config.named_vectors.get(name) else {
                    return Err(BorsukError::InvalidRecordInput(format!(
                        "record `{}` carries undeclared named vector `{name}`",
                        record.id
                    )));
                };
                if spec.kind != VectorKind::Sparse {
                    return Err(BorsukError::InvalidRecordInput(format!(
                        "record `{}` supplies sparse data for dense named vector `{name}`",
                        record.id
                    )));
                }
                if let Some(&max) = vector.indices().iter().max()
                    && (max as usize) >= spec.dimensions
                {
                    return Err(BorsukError::InvalidRecordInput(format!(
                        "record `{}` sparse index {max} exceeds dimensionality {}",
                        record.id, spec.dimensions
                    )));
                }
                if let Some((value_index, value)) = vector
                    .values()
                    .iter()
                    .copied()
                    .enumerate()
                    .find(|(_, value)| *value < 0.0)
                {
                    return Err(BorsukError::InvalidRecordInput(format!(
                        "record `{}` sparse named vector `{name}` weights must be non-negative; \
                         value {value_index} was {value}",
                        record.id
                    )));
                }
                *vector = vector.canonicalize_values(spec.element_type)?;
            }
        }
        Ok(())
    }

    fn canonicalize_late_interaction_records(&self, records: &mut [VectorRecord]) -> Result<()> {
        for record in records {
            for (name, vector) in &mut record.extra_multi_vectors {
                let Some(spec) = self.manifest.config.named_vectors.get(name) else {
                    return Err(BorsukError::InvalidRecordInput(format!(
                        "record `{}` carries undeclared late-interaction vector `{name}`",
                        record.id
                    )));
                };
                if spec.kind != VectorKind::LateInteraction {
                    return Err(BorsukError::InvalidRecordInput(format!(
                        "record `{}` supplies late-interaction data for non-late vector `{name}`",
                        record.id
                    )));
                }
                if vector.dimensions() != spec.dimensions {
                    return Err(BorsukError::DimensionMismatch {
                        expected: spec.dimensions,
                        actual: vector.dimensions(),
                    });
                }
                *vector = vector.canonicalize_as(spec.element_type)?;
            }
        }
        Ok(())
    }

    /// Exact ColBERT-style late-interaction search.
    ///
    /// Token rows are read through the field's flattened child ANN index to
    /// discover entity ids, then each live entity is scored once with exact
    /// SIMD MaxSim against its persisted Arrow token matrix. This no-options
    /// entry point deliberately requests the complete token frontier, making
    /// recall deterministic. Use [`Self::search_late_interaction_with_report`]
    /// to sweep bounded token frontiers and record amplification/I/O evidence.
    pub fn search_late_interaction(
        &self,
        name: &str,
        query_tokens: Vec<Vec<f32>>,
        k: usize,
    ) -> Result<Vec<SearchHit>> {
        Ok(self
            .search_late_interaction_with_report(
                name,
                query_tokens,
                LateInteractionSearchOptions::exact(k),
            )?
            .hits)
    }

    /// Search a flattened token child index, then exact-rerank unique live
    /// entities with SIMD MaxSim over their persisted Arrow token matrices.
    ///
    /// `LateInteractionSearchOptions::exact` is the ground-truth reference.
    /// `bounded` caps token hits per query token and exposes the resulting
    /// candidate/latency curve without changing the exact entity reranker.
    pub fn search_late_interaction_with_report(
        &self,
        name: &str,
        query_tokens: Vec<Vec<f32>>,
        options: LateInteractionSearchOptions,
    ) -> Result<LateInteractionSearchReport> {
        if options.k == 0 {
            return Err(BorsukError::InvalidSearchOptions(
                "late-interaction k must be greater than zero".to_string(),
            ));
        }
        if options.candidates_per_query_token == Some(0) {
            return Err(BorsukError::InvalidSearchOptions(
                "late-interaction candidates_per_query_token must be greater than zero".to_string(),
            ));
        }
        let started = Instant::now();
        let requests_before = self.request_counts();
        let primary_reads_before = self.storage.cache_read_counts();
        let spec = self
            .manifest
            .config
            .named_vectors
            .get(name)
            .ok_or_else(|| {
                BorsukError::InvalidSearchOptions(format!(
                    "no late-interaction named vector `{name}` is declared"
                ))
            })?;
        if spec.kind != VectorKind::LateInteraction {
            return Err(BorsukError::InvalidSearchOptions(format!(
                "named vector `{name}` is not late-interaction"
            )));
        }
        let query =
            crate::LateInteractionVector::new(query_tokens, crate::VectorElementType::Float32)?;
        if query.dimensions() != spec.dimensions {
            return Err(BorsukError::DimensionMismatch {
                expected: spec.dimensions,
                actual: query.dimensions(),
            });
        }
        let child = self.named.get(name).ok_or_else(|| {
            BorsukError::InvalidStorage(format!(
                "late-interaction token index `{name}` is not open"
            ))
        })?;
        let token_limit = child.stats().records.max(1);
        let mut candidates = BTreeMap::<Vec<u8>, u64>::new();
        let mut token_hits_considered = 0_usize;
        let mut child_bytes_read = 0_u64;
        let mut child_disk_bytes_read = 0_u64;
        let mut child_backing_bytes_read = 0_u64;
        let mut child_wal_cells_examined = 0_usize;
        let mut child_wal_lanes_examined = 0_usize;
        let mut child_wal_runs_examined = 0_usize;
        let mut child_wal_records_examined = 0_usize;
        let mut child_wal_snapshot_retries = 0_usize;
        let token_started = Instant::now();
        for token in query.tokens() {
            let search_options = options.candidates_per_query_token.map_or_else(
                || SearchOptions::exact(token_limit),
                |limit| {
                    SearchOptions::approx(limit, child.build_config().global_scan_codec.leaf_mode())
                        .with_max_candidates_per_segment(limit)
                },
            );
            let report = child.search_with_report(token, search_options)?;
            token_hits_considered = token_hits_considered.saturating_add(report.hits.len());
            child_bytes_read = child_bytes_read.saturating_add(report.bytes_read);
            child_disk_bytes_read =
                child_disk_bytes_read.saturating_add(report.disk_cache_bytes_read);
            child_backing_bytes_read =
                child_backing_bytes_read.saturating_add(report.backing_bytes_read);
            // Every query token reads the same child-index WAL snapshot. Max
            // preserves the unique snapshot footprint instead of multiplying it
            // by the number of query tokens.
            child_wal_cells_examined = child_wal_cells_examined.max(report.wal_cells_examined);
            child_wal_lanes_examined = child_wal_lanes_examined.max(report.wal_lanes_examined);
            child_wal_runs_examined = child_wal_runs_examined.max(report.wal_runs_examined);
            child_wal_records_examined =
                child_wal_records_examined.max(report.wal_records_examined);
            child_wal_snapshot_retries =
                child_wal_snapshot_retries.max(report.wal_snapshot_retries);
            for hit in report.hits {
                let (entity_id, generation, _) =
                    decode_late_interaction_token_id(hit.id.as_bytes())?;
                if self
                    .min_visible_generation(entity_id)?
                    .is_some_and(|minimum| generation < minimum)
                {
                    continue;
                }
                candidates
                    .entry(entity_id.to_vec())
                    .and_modify(|current| *current = (*current).max(generation))
                    .or_insert(generation);
            }
        }
        let token_search_ms = token_started.elapsed().as_secs_f64() * 1_000.0;
        let rerank_started = Instant::now();
        let matrices = self.late_interaction_vectors_for_candidates(name, &candidates)?;
        let candidate_entities = matrices.len();
        let mut scored = matrices
            .into_iter()
            .map(|(id, matrix)| {
                Ok(SearchHit {
                    id: RecordId::from_bytes(id),
                    distance: -crate::late_interaction_maxsim(&query, &matrix)?,
                    metadata: None,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        scored.sort_by(|left, right| {
            left.distance
                .total_cmp(&right.distance)
                .then_with(|| left.id.as_bytes().cmp(right.id.as_bytes()))
        });
        scored.truncate(options.k);
        let rerank_ms = rerank_started.elapsed().as_secs_f64() * 1_000.0;
        let primary_reads = self
            .storage
            .cache_read_counts()
            .delta(&primary_reads_before);
        let (
            primary_wal_cells,
            primary_wal_lanes,
            primary_wal_runs,
            primary_wal_records,
            primary_wal_retries,
        ) = self.wal_search_observation();
        Ok(LateInteractionSearchReport {
            hits: scored,
            query_tokens: query.token_count(),
            candidates_per_query_token: options.candidates_per_query_token,
            token_hits_considered,
            candidate_entities,
            token_search_ms,
            rerank_ms,
            elapsed_ms: started.elapsed().as_secs_f64() * 1_000.0,
            bytes_read: child_bytes_read
                .saturating_add(primary_reads.disk_bytes)
                .saturating_add(primary_reads.backing_bytes),
            disk_cache_bytes_read: child_disk_bytes_read.saturating_add(primary_reads.disk_bytes),
            backing_bytes_read: child_backing_bytes_read
                .saturating_add(primary_reads.backing_bytes),
            requests: self.request_counts().delta(&requests_before),
            wal_cells_examined: primary_wal_cells.saturating_add(child_wal_cells_examined),
            wal_lanes_examined: primary_wal_lanes.saturating_add(child_wal_lanes_examined),
            wal_runs_examined: primary_wal_runs.saturating_add(child_wal_runs_examined),
            wal_records_examined: primary_wal_records.saturating_add(child_wal_records_examined),
            wal_snapshot_retries: primary_wal_retries.saturating_add(child_wal_snapshot_retries),
        })
    }

    fn late_interaction_vectors_for_candidates(
        &self,
        name: &str,
        candidates: &BTreeMap<Vec<u8>, u64>,
    ) -> Result<HashMap<Vec<u8>, crate::LateInteractionVector>> {
        let spec = self
            .manifest
            .config
            .named_vectors
            .get(name)
            .ok_or_else(|| {
                BorsukError::InvalidSearchOptions(format!(
                    "no late-interaction named vector `{name}` is declared"
                ))
            })?;
        let mut found = HashMap::with_capacity(candidates.len());
        for id in candidates.keys() {
            if let Some(record) = self.live_wal_tail_record_for_id(id)?
                && candidates.get(id) == Some(&record.generation)
                && let Some(vector) = record.extra_multi_vectors.get(name)
            {
                found.insert(id.clone(), vector.clone());
            }
        }
        let mut missing_ids = candidates
            .keys()
            .filter(|id| !found.contains_key(*id))
            .cloned()
            .collect::<BTreeSet<_>>();
        if missing_ids.is_empty() {
            return Ok(found);
        }
        for summary in self.active_segment_summaries()? {
            if !missing_ids
                .iter()
                .any(|id| summary.might_contain_record_id(id))
            {
                continue;
            }
            let (segment, _) = self.read_segment_lean_ranged(&summary)?;
            let rows = segment
                .records
                .iter()
                .enumerate()
                .filter_map(|(row, record)| {
                    let id = record.id.as_bytes();
                    (missing_ids.contains(id) && candidates.get(id) == Some(&record.generation))
                        .then_some(row)
                })
                .collect::<Vec<_>>();
            if rows.is_empty() {
                continue;
            }
            let vectors = self.segment_late_interaction_rows_ranged(&summary, name, spec, &rows)?;
            for row in rows {
                let record = &segment.records[row];
                if let Some(Some(vector)) = vectors.get(&row) {
                    found.insert(record.id.as_bytes().to_vec(), vector.clone());
                }
            }
            missing_ids = candidates
                .keys()
                .filter(|id| !found.contains_key(*id))
                .cloned()
                .collect();
            if missing_ids.is_empty() {
                break;
            }
        }
        Ok(found)
    }

    fn late_interaction_token_ids_for_entities(
        &self,
        ids: &[RecordId],
    ) -> Result<BTreeMap<String, Vec<RecordId>>> {
        let candidates = ids
            .iter()
            .map(|id| {
                Ok((
                    id.as_bytes().to_vec(),
                    self.min_visible_generation(id.as_bytes())?.unwrap_or(0),
                ))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        let mut token_ids = BTreeMap::new();
        for (name, spec) in &self.manifest.config.named_vectors {
            if spec.kind != VectorKind::LateInteraction {
                continue;
            }
            let matrices = self.late_interaction_vectors_for_candidates(name, &candidates)?;
            let mut field_ids = Vec::new();
            for (entity_id, matrix) in matrices {
                let generation = candidates.get(&entity_id).copied().unwrap_or(0);
                for token_index in 0..matrix.token_count() {
                    field_ids.push(RecordId::from_bytes(encode_late_interaction_token_id(
                        &entity_id,
                        generation,
                        token_index,
                    )?));
                }
            }
            token_ids.insert(name.clone(), field_ids);
        }
        Ok(token_ids)
    }

    fn segment_late_interaction_rows_ranged(
        &self,
        summary: &SegmentSummary,
        name: &str,
        spec: &VectorSpec,
        rows: &[usize],
    ) -> Result<HashMap<usize, Option<crate::LateInteractionVector>>> {
        let path = late_interaction_sidecar_relative_path(name, &summary.checksum);
        let cache_key = format!("{name}:{}", summary.checksum);
        let cached = self
            .late_interaction_sidecar_indexes
            .lock()
            .ok()
            .and_then(|mut cache| cache.get(&cache_key));
        let index = if let Some(index) = cached {
            index
        } else {
            let max_tail = crate::late_interaction_sidecar::max_index_tail_len(
                summary.object_count,
                spec.dimensions,
                spec.element_type,
            )?;
            let tail = self.storage.read_suffix(&path, max_tail)?;
            let index = Arc::new(crate::late_interaction_sidecar::parse_tail(
                &tail.bytes,
                summary.object_count,
            )?);
            if let Ok(mut cache) = self.late_interaction_sidecar_indexes.lock() {
                cache.insert(cache_key.clone(), Arc::clone(&index));
            }
            index
        };
        let mut sorted = rows.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        let mut groups = Vec::<(Range<u64>, Vec<usize>)>::new();
        for row in sorted {
            let range = index.row_range(row)?;
            if let Some((previous, grouped)) = groups.last_mut()
                && *previous == range
            {
                grouped.push(row);
            } else {
                groups.push((range, vec![row]));
            }
        }
        let mut decoded = HashMap::with_capacity(rows.len());
        for (range, requested_rows) in groups {
            let batch_key = format!(
                "{cache_key}:{}:{}",
                range.start,
                range.end.saturating_sub(range.start)
            );
            let batch = if let Some(batch) = self.decoded_late_interaction_batches.get(&batch_key) {
                batch
            } else {
                let first_row = *requested_rows.first().ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "late-interaction batch request has no rows".to_string(),
                    )
                })?;
                let all_rows = index.batch_rows_for(first_row)?.collect::<Vec<_>>();
                let logical_selection = format!("rows:{}", join_rows(&requested_rows));
                let logical_rows_requested = requested_rows.len();
                let result = self
                    .inflight_late_interaction_batches
                    .load(&batch_key, || {
                        let bytes = self.storage.read_range(&path, range.clone())?;
                        let bytes_read = bytes.len() as u64;
                        let decode_started = Instant::now();
                        let rows = index.decode_rows(&all_rows, &bytes)?.into_iter().collect();
                        self.storage
                            .record_access_event(StorageAccessEvent::decode(
                                &path,
                                physical_format_for_path(&path),
                                0,
                                "record_id|generation|token_matrix",
                                &logical_selection,
                                logical_rows_requested as u64,
                                all_rows.len() as u64,
                                elapsed_ns(decode_started),
                            ))?;
                        Ok((LateInteractionBatch { rows }, bytes_read))
                    })?;
                self.decoded_late_interaction_batches.insert(
                    batch_key,
                    Arc::clone(&result.0),
                    decoded_late_interaction_batch_bytes(&result.0),
                );
                result.0
            };
            for row in requested_rows {
                let vector = batch.rows.get(&row).ok_or_else(|| {
                    BorsukError::InvalidStorage(format!(
                        "decoded late-interaction Arrow batch has no row {row}"
                    ))
                })?;
                decoded.insert(row, vector.clone());
            }
        }
        Ok(decoded)
    }

    /// Search a sparse named vector for the top `k` records by inner-product
    /// similarity, scoring the query directly against the inverted index without
    /// densifying. Stored and query weights must be non-negative. Returns every
    /// strictly positive match in exact descending-score order, capped at `k`;
    /// zero-score records sharing no positively weighted term are outside the
    /// sparse-match result set.
    pub fn search_sparse_named(
        &self,
        name: &str,
        indices: Vec<u32>,
        values: Vec<f32>,
        k: usize,
    ) -> Result<Vec<SearchHit>> {
        Ok(self
            .search_sparse_named_with_report(name, indices, values, k)?
            .hits)
    }

    fn search_sparse_named_with_report(
        &self,
        name: &str,
        indices: Vec<u32>,
        values: Vec<f32>,
        k: usize,
    ) -> Result<SearchReport> {
        // Sparse named retrieval is a complete search leg, including inside a
        // hybrid query. Share the same whole-search cap as dense and text:
        // weighted byte admission bounds live buffers, while this count also
        // prevents many caller threads from retaining allocator arenas.
        let _admission = self.admission.as_ref().map(|gate| gate.acquire());
        let started = Instant::now();
        let spec = self
            .manifest
            .config
            .named_vectors
            .get(name)
            .ok_or_else(|| {
                BorsukError::InvalidMetricInput(format!(
                    "no sparse named vector `{name}` is declared"
                ))
            })?;
        if spec.kind != VectorKind::Sparse {
            return Err(BorsukError::InvalidMetricInput(format!(
                "no sparse named vector `{name}` is declared"
            )));
        }
        let query = SparseVector::new(indices, values)?;
        if let Some((value_index, value)) = query
            .values()
            .iter()
            .copied()
            .enumerate()
            .find(|(_, value)| *value < 0.0)
        {
            return Err(BorsukError::InvalidMetricInput(format!(
                "sparse query for named vector `{name}` must use non-negative weights; \
                 value {value_index} was {value}"
            )));
        }
        if let Some(&max) = query.indices().iter().max()
            && (max as usize) >= spec.dimensions
        {
            return Err(BorsukError::InvalidMetricInput(format!(
                "sparse query index {max} exceeds dimensionality {}",
                spec.dimensions
            )));
        }

        let summaries = self.active_segment_summaries()?;
        let segments_total = summaries.len();
        let query_terms = query.indices().iter().copied().collect::<BTreeSet<_>>();
        let (plans, mut bytes_read) =
            match self.load_lexical_query_plan(LexicalKind::Sparse, name, &query_terms)? {
                Some((root, pages, bytes)) => (
                    LexicalTermPage::plan_sparse(
                        &pages,
                        &root,
                        &query
                            .indices()
                            .iter()
                            .copied()
                            .zip(query.values().iter().copied())
                            .collect::<Vec<_>>(),
                    )?,
                    bytes,
                ),
                None => (Vec::new(), 0),
            };
        let weights = query
            .indices()
            .iter()
            .copied()
            .zip(query.values().iter().copied())
            .collect::<BTreeMap<_, _>>();
        let mut searched_segment_keys = HashSet::new();
        let mut records_considered = 0_usize;
        let mut records_scored = 0_usize;
        let mut shared_decodes = 0_usize;
        let mut shared_decoded_bytes = 0_u64;
        let mut best_by_id = HashMap::<Vec<u8>, (u64, f32)>::new();
        let mut next_plan = 0;
        while next_plan < plans.len() {
            if kth_largest_score(best_by_id.values().map(|(_, score)| f64::from(*score)), k)
                .is_some_and(|threshold| plans[next_plan].upper_bound < threshold)
            {
                break;
            }
            let wave_end = next_plan
                .saturating_add(DEFAULT_SEARCH_PREFETCH_DEPTH.max(1))
                .min(plans.len());
            let wave = &plans[next_plan..wave_end];
            let reads = self
                .read_lexical_wave(LexicalKind::Sparse, wave)
                .into_iter()
                .collect::<Result<Vec<_>>>()?;
            for (plan, (read, physical_bytes, shared_inflight)) in wave.iter().zip(&reads) {
                bytes_read = bytes_read.saturating_add(*physical_bytes);
                if *shared_inflight {
                    shared_decodes = shared_decodes.saturating_add(1);
                    shared_decoded_bytes =
                        shared_decoded_bytes.saturating_add(plan.run.decoded_bytes);
                }
                searched_segment_keys.insert(plan.run.segment_key.clone());
                records_considered = records_considered.saturating_add(read.rows.len());
                let mut scores = vec![0.0_f32; read.rows.len()];
                let mut touched = vec![false; read.rows.len()];
                let LexicalRunPostings::Sparse(postings) = &read.postings else {
                    unreachable!("sparse plan decoded BM25 postings")
                };
                crate::lexical_simd::accumulate_sparse(
                    postings,
                    &weights,
                    &mut scores,
                    &mut touched,
                );
                records_scored =
                    records_scored.saturating_add(touched.iter().filter(|seen| **seen).count());
                for (row, (metadata, score)) in read.rows.iter().zip(scores).enumerate() {
                    if !touched[row] || score <= 0.0 {
                        continue;
                    }
                    if self
                        .min_visible_generation(&metadata.record_id)?
                        .is_some_and(|min_visible| metadata.generation < min_visible)
                    {
                        continue;
                    }
                    match best_by_id.get_mut(&metadata.record_id) {
                        Some(existing) if existing.0 >= metadata.generation => {}
                        Some(existing) => *existing = (metadata.generation, score),
                        None => {
                            best_by_id
                                .insert(metadata.record_id.clone(), (metadata.generation, score));
                        }
                    }
                }
            }
            next_plan = wave_end;
        }
        let mut wal_sparse_searched = false;
        for record in self.live_wal_tail_records()? {
            let Some(vector) = record.extra_sparse.get(name) else {
                continue;
            };
            wal_sparse_searched = true;
            records_considered = records_considered.saturating_add(1);
            let score = sparse_dot(&query, vector);
            if score <= 0.0 {
                continue;
            }
            records_scored = records_scored.saturating_add(1);
            let key = record.id.as_bytes().to_vec();
            match best_by_id.get_mut(&key) {
                Some(existing) if existing.0 >= record.generation => {}
                Some(existing) => *existing = (record.generation, score),
                None => {
                    best_by_id.insert(key, (record.generation, score));
                }
            }
        }
        let segments_searched = searched_segment_keys.len();
        if wal_sparse_searched {
            bytes_read = bytes_read.saturating_add(self.cell_wal_record_bytes());
        }

        let mut scored = best_by_id
            .into_iter()
            .map(|(id, (_, score))| (RecordId::from_bytes(id), score))
            .collect::<Vec<_>>();
        scored.sort_by(|left, right| {
            right
                .1
                .total_cmp(&left.1)
                .then_with(|| left.0.cmp(&right.0))
        });
        scored.truncate(k);
        let hits = scored
            .into_iter()
            .map(|(id, score)| SearchHit {
                id,
                distance: -score,
                metadata: None,
            })
            .collect();

        let mut report = SearchReport {
            hits,
            leaf_mode: "sparse".to_string(),
            termination_reason: SearchTerminationReason::Complete,
            recall_guarantee: RecallGuarantee::Exact,
            segments_total,
            segments_searched,
            segments_skipped: segments_total.saturating_sub(segments_searched),
            routing_page_indexes_read: 0,
            routing_pages_read: 0,
            bytes_read,
            prefetched_bytes_unused: 0,
            graph_bytes_read: 0,
            decoded_cache_hits: shared_decodes,
            decoded_cache_bytes_read: shared_decoded_bytes,
            object_cache_hits: 0,
            object_cache_misses: 0,
            disk_cache_bytes_read: 0,
            backing_bytes_read: 0,
            disk_cache_reads: 0,
            backing_reads: 0,
            cache_repairs: 0,
            records_considered,
            records_scored,
            graph_candidates_added: 0,
            global_graph_chunks_searched: 0,
            global_scan_chunks_searched: 0,
            resident_bytes_estimate: self.manifest.resident_bytes_estimate(),
            elapsed_ms: started.elapsed().as_millis() as u64,
            requests: RequestCounts::default(),
            rows_evaluated: 0,
            rows_passed_filter: 0,
            segments_pruned_by_filter: 0,
            wal_cells_examined: 0,
            wal_lanes_examined: 0,
            wal_runs_examined: 0,
            wal_records_examined: 0,
            wal_snapshot_retries: 0,
        };
        self.apply_wal_search_observation(&mut report);
        Ok(report)
    }

    fn add_records_with_report(
        &mut self,
        records: Vec<VectorRecord>,
        scan_existing_ids: bool,
        next_generated_id: u64,
    ) -> Result<AddReport> {
        self.add_records_with_report_and_tombstone(
            records,
            scan_existing_ids,
            next_generated_id,
            None,
            None,
        )
    }

    /// Add records and, when `tombstone_update` is set, publish that tombstone
    /// overlay in the same manifest version — so an upsert's new record and the
    /// suppression of its superseded generations become visible atomically.
    fn add_records_with_report_and_tombstone(
        &mut self,
        mut records: Vec<VectorRecord>,
        scan_existing_ids: bool,
        next_generated_id: u64,
        tombstone_update: Option<(TombstoneSummary, u64)>,
        bm25_stats_delta_update: Option<Option<Bm25StatsDeltaRef>>,
    ) -> Result<AddReport> {
        let vectors_added = records.len();
        let span = observability::add_span(vectors_added, self.manifest.version);
        let _entered = span.enter();
        let requests_before = self.storage.request_counts();
        if records.is_empty() {
            let report = AddReport::default();
            observability::record_add_report(&span, &report, self.manifest.version);
            return Ok(report);
        }

        for record in &mut records {
            self.validate_vector(&record.vector)?;
            record.vector = self
                .manifest
                .build_config
                .vector_element_type
                .canonicalize(&record.vector)?;
        }
        self.validate_text_records(&mut records)?;
        let coordinated_insert = self.manifest.wal_config.enabled && scan_existing_ids;
        self.validate_record_ids_allowing_existing(
            &records,
            scan_existing_ids && !coordinated_insert,
            tombstone_update.is_some(),
        )?;
        let transaction_id = Uuid::new_v4().simple().to_string();
        let mut insert_claims = if coordinated_insert {
            let claims = self.cell_wal_store()?.claim_ids(
                &transaction_id,
                records.iter().map(|record| record.id.as_bytes()),
            )?;
            let refresh = if claims.matches_checkpoint(&self.cell_wal_claim_checkpoint) {
                Ok(false)
            } else {
                self.refresh()
            };
            if let Err(error) = refresh.and_then(|_| {
                self.validate_record_ids_allowing_existing(
                    &records,
                    true,
                    tombstone_update.is_some(),
                )
            }) {
                drop(claims);
                return Err(error);
            }
            Some(claims)
        } else {
            None
        };

        // WAL fast path: route records to immutable cell-local runs and publish
        // one atomic transaction commit marker without swapping the collection
        // manifest. The tail is flushed into a real segment once it crosses the
        // configured threshold (checked below).
        //
        // The WAL codec preserves forced storage and every named payload, so all
        // normal writes share the append-only path. Sparse/text reads union the
        // bounded live tail; flush/compaction builds their immutable posting
        // shards once.
        if self.manifest.wal_config.enabled {
            let mut report = self.append_wal_and_publish(
                records,
                next_generated_id,
                tombstone_update,
                bm25_stats_delta_update,
                &requests_before,
                CellWalAppendTransaction {
                    id: &transaction_id,
                    claimed: coordinated_insert,
                },
            )?;
            if let Some(claims) = &mut insert_claims {
                // The committed WAL transaction now owns these ids. Any later
                // flush error must not make them available to a second insert.
                self.cell_wal_claim_checkpoint.extend(claims.finish());
            }
            self.maybe_flush_wal()?;
            report.requests = self.storage.request_counts().delta(&requests_before);
            observability::record_add_report(&span, &report, self.manifest.version);
            return Ok(report);
        }

        if self.manifest.segments.is_empty() {
            let top_read = self.storage.read_routing_layer_page_index_with_status(
                self.manifest.version,
                self.manifest.routing_max_level,
            )?;
            if !top_read.page_refs.is_empty() {
                let mut report = self.add_records_to_top_routing_page_refs(
                    records,
                    next_generated_id,
                    self.manifest.routing_max_level,
                    top_read.page_refs,
                    tombstone_update,
                    bm25_stats_delta_update,
                )?;
                report.requests = self.storage.request_counts().delta(&requests_before);
                observability::record_add_report(&span, &report, self.manifest.version);
                return Ok(report);
            }
        }

        // A large direct/bulk checkpoint is already bounded by the caller's
        // ingest budget. Locality-order it before cutting immutable segments so
        // global-PQ neighbours share fewer exact sidecars without requiring a
        // later full-corpus, multi-GiB reclustering pass. IDs and generations
        // move with their vectors, so logical identity is unchanged.
        if records.len() > self.manifest.config.segment_max_vectors {
            sort_records_by_vector_locality(
                &mut records,
                self.manifest.config.dimensions,
                self.manifest.config.segment_max_vectors,
            );
        }
        let chunks = records.chunks(self.manifest.config.segment_max_vectors);
        let previous = self.manifest.clone();
        let mut manifest = self.manifest.next_version();
        manifest.next_generated_id = next_generated_id;
        if let Some((tombstone, new_tombstone_ids)) = tombstone_update {
            manifest.tombstone_frontier.push(tombstone);
            manifest.tombstone_id_count = manifest
                .tombstone_id_count
                .checked_add(new_tombstone_ids)
                .ok_or_else(|| {
                    BorsukError::InvalidStorage("tombstone id count exceeds u64".to_string())
                })?;
        }
        if let Some(Some(update)) = bm25_stats_delta_update {
            manifest.bm25_stats_delta_frontier.push(update);
        }
        let mut segments_written = 0_usize;
        let mut graph_payloads_written = 0_usize;
        let mut payload_bytes_written = 0_u64;

        for chunk in chunks {
            let segment_id = Uuid::new_v4().to_string();
            let segment = Segment::from_records_with_quantizer_and_geometry(
                segment_id.clone(),
                0,
                self.manifest.config.metric.clone(),
                self.manifest.config.dimensions,
                chunk.to_vec(),
                self.manifest.build_config.quantizer,
                self.manifest
                    .build_config
                    .normalized_angular_coarse_geometry,
            )?;
            let summary = self.write_segment(segment)?;
            segments_written += 1;
            graph_payloads_written += 1;
            payload_bytes_written +=
                summary.size_bytes + summary.vector_size_bytes + summary.graph_size_bytes;
            manifest.segments.push(summary);
        }

        manifest.rebuild_pivots();
        // Direct writes are an ingest path, not an index-finalization boundary.
        // Building a corpus-wide artifact after every batch makes a 10M-vector
        // load rescan the growing corpus repeatedly. If a finalized immutable
        // base already exists, retain it and expose these appended cells through
        // the same materialized-delta read path used by a WAL flush.
        enforce_ram_budget(&manifest, self.runtime_ram_budget_bytes)?;
        let (published, storage_report) = self
            .publish_manifest_reusing_routing_pages_with_recovery_report(
                manifest,
                Some(&previous),
            )?;
        self.manifest = published;
        let mut report = add_report_from_parts(
            segments_written,
            graph_payloads_written,
            payload_bytes_written,
            storage_report,
            vectors_added,
        );
        report.requests = self.storage.request_counts().delta(&requests_before);
        observability::record_add_report(&span, &report, self.manifest.version);
        Ok(report)
    }

    fn cell_wal_store(&self) -> Result<CellWalStore> {
        CellWalStore::from_storage(
            self.storage.clone(),
            self.manifest.cell_wal_config,
            self.writer_id.clone(),
        )
    }

    fn route_vector_to_logical_cell(&self, vector: &[f32]) -> Result<LogicalCellId> {
        let bootstrap = self
            .manifest
            .logical_cells
            .first()
            .copied()
            .unwrap_or_else(|| LogicalCellId::new(self.manifest.routing_epoch, 0));
        if self.manifest.logical_cell_centroids.len() != self.manifest.logical_cells.len()
            || self.manifest.logical_cell_centroids.is_empty()
        {
            return Ok(bootstrap);
        }
        let routed = if self
            .manifest
            .config
            .metric
            .uses_normalized_euclidean_geometry()
        {
            crate::metric::unit_l2_normalized(vector)
        } else {
            vector.to_vec()
        };
        self.manifest
            .logical_cell_centroids
            .iter()
            .enumerate()
            .map(|(ordinal, centroid)| {
                self.manifest
                    .config
                    .metric
                    .centroid_geometry_distance_unchecked(&routed, centroid)
                    .map(|distance| (ordinal, distance))
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .min_by(|left, right| left.1.total_cmp(&right.1))
            .and_then(|(ordinal, _)| self.manifest.logical_cells.get(ordinal).copied())
            .ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "logical cell routing catalog contains no usable centroid".to_string(),
                )
            })
    }

    fn id_directory_partition(&self, id: &[u8]) -> LogicalCellId {
        let cells = &self.manifest.logical_cells;
        if cells.is_empty() {
            return LogicalCellId::new(self.manifest.routing_epoch, 0);
        }
        let digest = blake3::hash(id);
        let ordinal = u64::from_le_bytes(
            digest.as_bytes()[..8]
                .try_into()
                .expect("BLAKE3 has at least eight bytes"),
        ) % cells.len() as u64;
        cells[ordinal as usize]
    }

    fn reserve_coordination_counter(
        &self,
        path: &str,
        minimum_start: u64,
        count: u64,
    ) -> Result<u64> {
        const MAX_ATTEMPTS: usize = 128;
        for _ in 0..MAX_ATTEMPTS {
            let current = self.storage.read_coordination_object(path)?;
            let (stored, expected) = match current {
                Some(current) => {
                    let stored = coordination_counter_from_slice(&current.bytes, path)?;
                    (stored, Some(current.version))
                }
                None => (0, None),
            };
            // A zero-width reservation is an ensure-at-least operation. Once
            // another allocator has already established that floor, do not
            // rewrite the shared coordination object: doing so would make
            // unrelated cell-local WAL appends contend on a collection-wide
            // CAS for no state change.
            if count == 0 && stored >= minimum_start {
                return Ok(stored);
            }
            let start = stored.max(minimum_start);
            let next = start.checked_add(count).ok_or_else(|| {
                BorsukError::InvalidStorage(format!("coordination counter `{path}` exceeds u64"))
            })?;
            match self.storage.write_coordination_object(
                path,
                &coordination_counter_bytes(next),
                expected,
            ) {
                Ok(_) => return Ok(start),
                Err(BorsukError::ConcurrentModification { .. }) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(BorsukError::ConcurrentModification {
            path: path.to_string(),
        })
    }

    fn reserve_record_generations(&self, requests: &[(Vec<u8>, u64)]) -> Result<Vec<u64>> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        let mut by_shard = BTreeMap::<u8, Vec<(usize, u64)>>::new();
        for (index, (id, minimum)) in requests.iter().enumerate() {
            by_shard
                .entry(id_claim_shard(id))
                .or_default()
                .push((index, *minimum));
        }
        let reservations = crate::parallel::install_io(|| {
            by_shard
                .into_par_iter()
                .map(|(shard, entries)| {
                    let minimum = entries
                        .iter()
                        .map(|(_, minimum)| *minimum)
                        .max()
                        .unwrap_or(0);
                    let count = u64::try_from(entries.len()).map_err(|_| {
                        BorsukError::InvalidStorage(
                            "generation reservation count exceeds u64".to_string(),
                        )
                    })?;
                    let start = self.reserve_coordination_counter(
                        &Self::record_generation_shard_path(shard),
                        minimum,
                        count,
                    )?;
                    Ok((entries, start))
                })
                .collect::<Result<Vec<_>>>()
        })?;
        let mut generations = vec![0; requests.len()];
        for (entries, start) in reservations {
            for (offset, (index, _)) in entries.into_iter().enumerate() {
                generations[index] = start
                    .checked_add(u64::try_from(offset).map_err(|_| {
                        BorsukError::InvalidStorage(
                            "generation reservation offset exceeds u64".to_string(),
                        )
                    })?)
                    .ok_or_else(|| {
                        BorsukError::InvalidStorage(
                            "record generation reservation exceeds u64".to_string(),
                        )
                    })?;
            }
        }
        Ok(generations)
    }

    fn record_generation_shard_path(shard: u8) -> String {
        debug_assert!(shard < CELL_WAL_CLAIM_SHARDS);
        format!("id-directory/generation-shards/{shard:02}/NEXT")
    }

    fn cell_wal_metadata(
        transaction: &CommittedCellWalTransaction,
    ) -> Result<CellWalMutationMetadata> {
        if transaction.metadata.is_empty() {
            return Ok(CellWalMutationMetadata::default());
        }
        cell_wal_mutation_metadata_from_slice(&transaction.metadata, &transaction.descriptor_path)
    }

    fn cell_wal_next_generated_id_floor(&self) -> Result<u64> {
        self.cell_wal_snapshot.iter().try_fold(
            self.manifest.next_generated_id,
            |floor, transaction| {
                Ok(floor.max(Self::cell_wal_metadata(transaction)?.next_generated_id_floor))
            },
        )
    }

    fn visible_tombstone_id_count(&self) -> Result<u64> {
        self.cell_wal_snapshot.iter().try_fold(
            self.manifest.tombstone_id_count,
            |total, transaction| {
                total
                    .checked_add(Self::cell_wal_metadata(transaction)?.new_tombstone_ids)
                    .ok_or_else(|| {
                        BorsukError::InvalidStorage("tombstone id count exceeds u64".to_string())
                    })
            },
        )
    }

    fn apply_cell_mutation_metadata_to_manifest(
        manifest: &mut Manifest,
        transactions: &[CommittedCellWalTransaction],
    ) -> Result<()> {
        for transaction in transactions {
            let metadata = Self::cell_wal_metadata(transaction)?;
            manifest.next_generated_id = manifest
                .next_generated_id
                .max(metadata.next_generated_id_floor);
            manifest.tombstone_id_count = manifest
                .tombstone_id_count
                .checked_add(metadata.new_tombstone_ids)
                .ok_or_else(|| {
                    BorsukError::InvalidStorage("tombstone id count exceeds u64".to_string())
                })?;
            if let Some(delta) = metadata.bm25_stats_delta {
                manifest.bm25_stats_delta_frontier.push(delta);
            }
            for run in &transaction.runs {
                if run.kind == CellWalRunKind::Tombstones {
                    manifest
                        .tombstone_frontier
                        .push(Self::cell_wal_tombstone_summary(run)?);
                }
            }
        }
        Ok(())
    }

    fn prune_consumed_cell_wal(&mut self) -> Result<()> {
        let consumed = self.manifest.cell_wal_consumed_runs.clone();
        if consumed.is_empty() {
            return Ok(());
        }
        self.cell_wal_store()?
            .prune_consumed_runs(&self.manifest.logical_cells, &consumed)?;
        let previous = self.manifest.clone();
        let mut manifest = self.manifest.next_version();
        manifest.cell_wal_consumed_runs.clear();
        self.manifest =
            self.publish_manifest_reusing_routing_pages_with_recovery(manifest, Some(&previous))?;
        Ok(())
    }

    fn cell_wal_tombstone_summary(run: &PreparedCellWalRun) -> Result<TombstoneSummary> {
        if run.kind != CellWalRunKind::Tombstones {
            return Err(BorsukError::InvalidStorage(format!(
                "cell WAL run `{}` is not a tombstone run",
                run.path
            )));
        }
        let metadata = cell_wal_tombstone_metadata_from_slice(&run.metadata, &run.path)?;
        Ok(TombstoneSummary {
            id_bloom: metadata.id_bloom,
            count: run.record_count as u64,
            path: run.path.clone(),
            checksum: run.checksum.clone(),
            created_at: metadata.created_at,
        })
    }

    fn fetch_cell_wal_snapshot(
        &self,
        manifest: &Manifest,
    ) -> Result<Vec<CommittedCellWalTransaction>> {
        let (transactions, retries) = CellWalStore::from_storage(
            self.storage.clone(),
            manifest.cell_wal_config,
            self.writer_id.clone(),
        )?
        .committed_transactions_snapshot_with_retries(&manifest.logical_cells)?;
        self.cell_wal_snapshot_retries
            .store(retries, AtomicOrdering::Relaxed);
        transactions
            .into_iter()
            .filter_map(|transaction| {
                let consumed = transaction
                    .runs
                    .iter()
                    .filter(|run| {
                        manifest
                            .cell_wal_consumed_runs
                            .contains(&cell_wal_run_identity(run))
                    })
                    .count();
                if consumed == 0 {
                    Some(Ok(transaction))
                } else if consumed == transaction.runs.len() {
                    None
                } else {
                    Some(Err(BorsukError::InvalidStorage(format!(
                        "cell WAL transaction `{}` is only partially consumed",
                        transaction.transaction_id
                    ))))
                }
            })
            .collect()
    }

    fn unconsumed_cell_wal_runs(&self) -> Vec<PreparedCellWalRun> {
        self.cell_wal_snapshot
            .iter()
            .flat_map(|transaction| transaction.runs.iter().cloned())
            .collect()
    }

    fn cell_wal_tombstone_summaries(&self) -> Result<Vec<TombstoneSummary>> {
        self.cell_wal_snapshot
            .iter()
            .flat_map(|transaction| transaction.runs.iter())
            .filter(|run| run.kind == CellWalRunKind::Tombstones)
            .map(Self::cell_wal_tombstone_summary)
            .collect()
    }

    fn cell_wal_record_bytes(&self) -> u64 {
        self.cell_wal_snapshot
            .iter()
            .flat_map(|transaction| &transaction.runs)
            .filter(|run| run.kind == CellWalRunKind::Records)
            .map(|run| run.byte_len)
            .sum()
    }

    fn cell_wal_id_directory_entries<'a, I>(
        &self,
        ids: I,
    ) -> Result<HashMap<Vec<u8>, CellWalIdDirectoryEntry>>
    where
        I: IntoIterator<Item = &'a [u8]>,
    {
        let mut ids_by_partition = BTreeMap::<LogicalCellId, HashSet<Vec<u8>>>::new();
        for id in ids {
            ids_by_partition
                .entry(self.id_directory_partition(id))
                .or_default()
                .insert(id.to_vec());
        }
        if ids_by_partition.is_empty() {
            return Ok(HashMap::new());
        }
        let runs = self
            .cell_wal_snapshot
            .iter()
            .flat_map(|transaction| &transaction.runs)
            .filter(|run| {
                run.kind == CellWalRunKind::IdDirectory && ids_by_partition.contains_key(&run.cell)
            })
            .cloned()
            .collect::<Vec<_>>();
        let decoded = crate::parallel::install_io(|| {
            runs.par_iter()
                .map(|run| {
                    let read = self
                        .storage
                        .read_bytes_with_cache_status_and_checksum(&run.path, &run.checksum)?;
                    Ok((
                        run.cell,
                        cell_wal_id_directory_from_slice(&read.bytes, &run.path)?,
                    ))
                })
                .collect::<Result<Vec<_>>>()
        })?;
        let mut newest = HashMap::<Vec<u8>, CellWalIdDirectoryEntry>::new();
        for (partition, entries) in decoded {
            let targets = &ids_by_partition[&partition];
            for candidate in entries {
                if !targets.contains(&candidate.id) {
                    continue;
                }
                match newest.entry(candidate.id.clone()) {
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        entry.insert(candidate);
                    }
                    std::collections::hash_map::Entry::Occupied(mut entry)
                        if candidate.generation > entry.get().generation =>
                    {
                        entry.insert(candidate);
                    }
                    std::collections::hash_map::Entry::Occupied(_) => {}
                }
            }
        }
        Ok(newest)
    }

    /// Serialize records into an immutable WAL object using the WAL record codec,
    /// which carries the dense `vector` column INLINE (unlike a normal segment,
    /// whose dense vectors live only in the Arrow sidecar). This keeps the
    /// un-flushed WAL tail fully searchable without building a sidecar. Round-trips
    /// id, dense vector, generation, metadata, text, and sparse encoding. This does
    /// NOT build a graph, sidecars, or routing summary — just one PUT.
    fn wal_object_bytes(&self, records: &[VectorRecord]) -> Result<(Vec<u8>, String)> {
        let format = self.manifest.build_config.physical_layout.resolve(
            crate::PhysicalObjectRole::WalRun,
            crate::PhysicalLayoutContext {
                rows: records.len(),
                dimensions: self.manifest.config.dimensions,
                vector_element_type: Some(self.manifest.build_config.vector_element_type),
            },
        )?;
        let bytes = wal_records_to_table(
            records,
            self.manifest.config.dimensions,
            self.manifest.build_config.vector_element_type,
            format,
        )?;
        Ok((bytes, format.extension().to_string()))
    }

    /// Route and commit one complete mutation through cell-local lanes. Records,
    /// tombstones, ownership updates, and lexical-statistics metadata become
    /// visible through one transaction commit marker; `CURRENT` is untouched.
    fn append_wal_and_publish(
        &mut self,
        records: Vec<VectorRecord>,
        next_generated_id: u64,
        tombstone_update: Option<(TombstoneSummary, u64)>,
        bm25_stats_delta_update: Option<Option<Bm25StatsDeltaRef>>,
        requests_before: &RequestCounts,
        transaction: CellWalAppendTransaction<'_>,
    ) -> Result<AddReport> {
        let vectors_added = records.len();
        let visible_generated_id_floor = self.cell_wal_next_generated_id_floor()?;
        if next_generated_id > visible_generated_id_floor {
            self.reserve_coordination_counter("id-directory/generated/NEXT", next_generated_id, 0)?;
        }
        let mut inputs = Vec::new();
        let mut records_by_cell = BTreeMap::<LogicalCellId, Vec<VectorRecord>>::new();
        let mut directory_by_partition =
            BTreeMap::<LogicalCellId, Vec<CellWalIdDirectoryEntry>>::new();
        let mut replaced_ids = HashSet::new();
        for record in records {
            let owner = self.route_vector_to_logical_cell(&record.vector)?;
            replaced_ids.insert(record.id.as_bytes().to_vec());
            directory_by_partition
                .entry(self.id_directory_partition(record.id.as_bytes()))
                .or_default()
                .push(CellWalIdDirectoryEntry {
                    id: record.id.as_bytes().to_vec(),
                    owner,
                    generation: record.generation,
                    deleted: false,
                });
            records_by_cell.entry(owner).or_default().push(record);
        }
        for (cell, records) in records_by_cell {
            let (bytes, extension) = self.wal_object_bytes(&records)?;
            inputs.push(CellWalRunInput {
                cell,
                kind: CellWalRunKind::Records,
                metadata: Vec::new(),
                bytes,
                record_count: records.len(),
                extension,
            });
        }

        let (tombstone, new_tombstone_ids) =
            tombstone_update.map_or((None, 0), |(summary, count)| (Some(summary), count));
        if let Some(tombstone) = tombstone {
            let read = self
                .storage
                .read_bytes_with_cache_status_and_checksum(&tombstone.path, &tombstone.checksum)?;
            let tombstone_entries = tombstone_ids_from_parquet(&read.bytes)?;
            let previous_entries = self.cell_wal_id_directory_entries(
                tombstone_entries.iter().map(|(id, _)| id.as_slice()),
            )?;
            let mut tombstones_by_cell = BTreeMap::<LogicalCellId, Vec<(Vec<u8>, u64)>>::new();
            for (id, generation) in tombstone_entries {
                let previous_owner = previous_entries
                    .get(&id)
                    .filter(|entry| !entry.deleted)
                    .map_or_else(|| self.id_directory_partition(&id), |entry| entry.owner);
                tombstones_by_cell
                    .entry(previous_owner)
                    .or_default()
                    .push((id.clone(), generation));
                if !replaced_ids.contains(&id) {
                    let partition = self.id_directory_partition(&id);
                    directory_by_partition.entry(partition).or_default().push(
                        CellWalIdDirectoryEntry {
                            id,
                            owner: previous_owner,
                            generation,
                            deleted: true,
                        },
                    );
                }
            }
            for (cell, entries) in tombstones_by_cell {
                let bytes = tombstone_ids_to_parquet(&entries)?;
                inputs.push(CellWalRunInput {
                    cell,
                    kind: CellWalRunKind::Tombstones,
                    metadata: cell_wal_tombstone_metadata_bytes(&CellWalTombstoneMetadata {
                        id_bloom: segment_id_bloom(entries.iter().map(|(id, _)| id)),
                        created_at: tombstone.created_at,
                    })?,
                    bytes,
                    record_count: entries.len(),
                    extension: "parquet".to_string(),
                });
            }
        }
        for (cell, mut entries) in directory_by_partition {
            entries.sort_by(|left, right| left.id.cmp(&right.id));
            let bytes = cell_wal_id_directory_bytes(&entries)?;
            inputs.push(CellWalRunInput {
                cell,
                kind: CellWalRunKind::IdDirectory,
                metadata: Vec::new(),
                record_count: entries.len(),
                bytes,
                extension: "bin".to_string(),
            });
        }
        let metadata = CellWalMutationMetadata {
            new_tombstone_ids,
            next_generated_id_floor: next_generated_id,
            bm25_stats_delta: bm25_stats_delta_update.flatten(),
        };
        let metadata = cell_wal_mutation_metadata_bytes(&metadata)?;
        let payload_bytes = inputs.iter().map(|input| input.bytes.len() as u64).sum();
        let cell_wal = self.cell_wal_store()?;
        let committed = if transaction.claimed {
            cell_wal.commit_claimed_with_metadata(transaction.id, &inputs, &metadata)?
        } else {
            cell_wal.commit_with_metadata(transaction.id, &inputs, &metadata)?
        };
        self.cell_wal_snapshot.push(committed);
        self.cell_wal_snapshot
            .sort_by(|left, right| left.transaction_id.cmp(&right.transaction_id));
        self.manifest.cell_wal_visible_runs = cell_wal_run_count(&self.cell_wal_snapshot);
        self.manifest.cell_wal_visible_tombstone_runs =
            cell_wal_tombstone_run_count(&self.cell_wal_snapshot);
        self.invalidate_wal_tail_cache();
        let mut report = add_report_from_parts(
            0,
            0,
            payload_bytes,
            StorageWriteReport::default(),
            vectors_added,
        );
        report.requests = self.storage.request_counts().delta(requests_before);
        Ok(report)
    }

    /// Flush the WAL tail into real segments when it crosses either threshold.
    fn maybe_flush_wal(&mut self) -> Result<()> {
        if !self.manifest.wal_config.enabled {
            return Ok(());
        }
        let threshold = &self.manifest.wal_config;
        let mut cell_totals = BTreeMap::<LogicalCellId, (usize, usize, usize, u64)>::new();
        for transaction in &self.cell_wal_snapshot {
            let mut touched_cells = BTreeSet::new();
            for run in &transaction.runs {
                let totals = cell_totals.entry(run.cell).or_default();
                match run.kind {
                    CellWalRunKind::Records => {
                        totals.1 = totals.1.saturating_add(run.record_count);
                    }
                    CellWalRunKind::Tombstones => {
                        totals.2 = totals.2.saturating_add(run.record_count);
                    }
                    CellWalRunKind::IdDirectory => {}
                }
                totals.3 = totals.3.saturating_add(run.byte_len);
                touched_cells.insert(run.cell);
            }
            for cell in touched_cells {
                cell_totals.entry(cell).or_default().0 += 1;
            }
        }
        let crossed_cells = cell_totals
            .iter()
            .filter_map(
                |(&cell, &(frontier_runs, record_count, mutation_count, byte_count))| {
                    ((threshold.flush_threshold_runs > 0
                        && frontier_runs >= threshold.flush_threshold_runs)
                        || (threshold.flush_threshold_records > 0
                            && record_count.max(mutation_count)
                                >= threshold.flush_threshold_records)
                        || (threshold.flush_threshold_bytes > 0
                            && byte_count >= threshold.flush_threshold_bytes))
                        .then_some(cell)
                },
            )
            .collect::<BTreeSet<_>>();
        let legacy_mutation_count = self
            .manifest
            .tombstone_frontier
            .iter()
            .map(|entry| entry.count as usize)
            .sum::<usize>();
        let legacy_crossed = (threshold.flush_threshold_runs > 0
            && self
                .manifest
                .tombstone_frontier
                .len()
                .max(self.manifest.bm25_stats_delta_frontier.len())
                >= threshold.flush_threshold_runs)
            || (threshold.flush_threshold_records > 0
                && legacy_mutation_count >= threshold.flush_threshold_records);
        let flush_result = if legacy_crossed {
            self.flush_wal()
        } else if !crossed_cells.is_empty() {
            let selected_transactions = self
                .cell_wal_snapshot
                .iter()
                .filter(|transaction| {
                    transaction
                        .runs
                        .iter()
                        .any(|run| crossed_cells.contains(&run.cell))
                })
                .map(|transaction| transaction.transaction_id.clone())
                .collect::<BTreeSet<_>>();
            self.flush_wal_transactions(&selected_transactions)
        } else {
            Ok(())
        };
        match flush_result {
            Ok(()) => {}
            Err(BorsukError::ConcurrentModification { .. }) => {
                // The public mutation committed before automatic maintenance
                // began. A concurrent cell flush may win the separate catalog
                // CAS; that must not turn an already-durable add/delete into a
                // reported failure. Refresh against the winning base and leave
                // any still-unconsumed transaction in its lane for the next
                // threshold check. Explicit flush() continues to surface CAS
                // conflicts to maintenance callers.
                self.cell_wal_snapshot = self.fetch_cell_wal_snapshot(&self.manifest)?;
                self.manifest.cell_wal_visible_runs = cell_wal_run_count(&self.cell_wal_snapshot);
                self.manifest.cell_wal_visible_tombstone_runs =
                    cell_wal_tombstone_run_count(&self.cell_wal_snapshot);
                self.invalidate_wal_tail_cache();
            }
            Err(error) => return Err(error),
        }
        Ok(())
    }

    /// Force the accumulated WAL tail into real, indexed segments and clear the
    /// frontier in one published manifest. A no-op when the WAL is disabled or
    /// the frontier is empty. Also drives the named/sparse sub-indexes: their
    /// WAL is flushed in lockstep so a flush is atomic across modalities from a
    /// reader's perspective (each sub-index publishes its own manifest).
    pub fn flush(&mut self) -> Result<()> {
        self.flush_wal()?;
        for child in self.named.values_mut() {
            child.flush()?;
        }
        Ok(())
    }

    fn flush_wal(&mut self) -> Result<()> {
        let selected_transactions = self
            .cell_wal_snapshot
            .iter()
            .map(|transaction| transaction.transaction_id.clone())
            .collect::<BTreeSet<_>>();
        self.flush_wal_transactions(&selected_transactions)
    }

    /// Materialize complete committed transactions selected by a hot cell.
    ///
    /// Selection is transaction-granular, rather than run-granular, so one
    /// public mutation that spans cells is never partially consumed. Independent
    /// cold-cell transactions remain in their lane frontiers.
    fn flush_wal_transactions(
        &mut self,
        selected_transaction_ids: &BTreeSet<String>,
    ) -> Result<()> {
        let selected_transactions = self
            .cell_wal_snapshot
            .iter()
            .filter(|transaction| selected_transaction_ids.contains(&transaction.transaction_id))
            .cloned()
            .collect::<Vec<_>>();
        let cell_runs = selected_transactions
            .iter()
            .flat_map(|transaction| transaction.runs.iter().cloned())
            .collect::<Vec<_>>();
        if cell_runs.is_empty()
            && self.manifest.tombstone_frontier.is_empty()
            && self.manifest.bm25_stats_delta_frontier.is_empty()
        {
            return Ok(());
        }
        // Resolve the CURRENT active segment set. For a paged index (e.g. after a
        // compaction) `manifest.segments` is empty and the real segments live in
        // routing pages, so seed the new manifest's segment list from the
        // resolved active summaries — otherwise the flush would republish routing
        // built from only the newly-flushed segments and silently drop every
        // pre-existing segment.
        let active_summaries = self.active_segment_summaries()?;
        let previous = self.manifest.clone();
        let lexical_roots_will_rebuild = cell_runs
            .iter()
            .any(|run| run.kind == CellWalRunKind::Records);
        let mut manifest = self.manifest.next_version();
        manifest.segments = active_summaries;
        // The frontier is now being materialized into segments; the consumed
        // identities published below let readers skip it and GC reclaim it.
        Self::apply_cell_mutation_metadata_to_manifest(&mut manifest, &selected_transactions)?;
        self.consolidate_mutation_frontiers(&mut manifest, lexical_roots_will_rebuild)?;

        // Build segments per cell record run. Different runs may contain
        // different generations of one id; the tombstone overlay suppresses
        // superseded copies until compaction physically drops them.
        for entry in &cell_runs {
            if entry.kind != CellWalRunKind::Records {
                manifest
                    .cell_wal_consumed_runs
                    .insert(cell_wal_run_identity(entry));
                continue;
            }
            let read = self
                .storage
                .read_bytes_with_cache_status_and_checksum(&entry.path, &entry.checksum)?;
            let mut records = wal_records_from_table(read.bytes, &entry.path)?;
            if records.len() > self.manifest.config.segment_max_vectors {
                sort_records_by_vector_locality(
                    &mut records,
                    self.manifest.config.dimensions,
                    self.manifest.config.segment_max_vectors,
                );
            }
            for chunk in records.chunks(self.manifest.config.segment_max_vectors.max(1)) {
                if chunk.is_empty() {
                    continue;
                }
                let segment = Segment::from_records_with_quantizer_and_geometry(
                    Uuid::new_v4().to_string(),
                    0,
                    self.manifest.config.metric.clone(),
                    self.manifest.config.dimensions,
                    chunk.to_vec(),
                    self.manifest.build_config.quantizer,
                    self.manifest
                        .build_config
                        .normalized_angular_coarse_geometry,
                )?;
                let summary = self.write_segment(segment)?;
                manifest.segments.push(summary);
            }
            manifest
                .cell_wal_consumed_runs
                .insert(cell_wal_run_identity(entry));
        }
        let remaining_transactions = self
            .cell_wal_snapshot
            .iter()
            .filter(|transaction| !selected_transaction_ids.contains(&transaction.transaction_id))
            .cloned()
            .collect::<Vec<_>>();
        manifest.cell_wal_visible_runs = cell_wal_run_count(&remaining_transactions);
        manifest.cell_wal_visible_tombstone_runs =
            cell_wal_tombstone_run_count(&remaining_transactions);
        manifest.rebuild_pivots();
        // Flushing is an online durability boundary, not a corpus-wide training
        // boundary. Existing cells and their row ordinals are unchanged, so
        // retain the immutable global base and expose the appended cells as a
        // bounded materialized delta. Search merges both layers; delta-only
        // compaction must never rewrite a base-covered segment.
        enforce_ram_budget(&manifest, self.runtime_ram_budget_bytes)?;
        let published =
            self.publish_manifest_reusing_routing_pages_with_recovery(manifest, Some(&previous))?;
        self.manifest = published;
        self.prune_consumed_cell_wal()?;
        self.cell_wal_snapshot = self.fetch_cell_wal_snapshot(&self.manifest)?;
        self.manifest.cell_wal_visible_runs = cell_wal_run_count(&self.cell_wal_snapshot);
        self.manifest.cell_wal_visible_tombstone_runs =
            cell_wal_tombstone_run_count(&self.cell_wal_snapshot);
        self.invalidate_wal_tail_cache();
        // The flush materialized the WAL tail into cells; refresh the persisted
        // cold quantizer so a cold/paged query routes over the current cell set.
        self.refresh_persisted_quantizer()?;
        Ok(())
    }

    /// Decode the published, un-flushed WAL objects into their records, in
    /// frontier order. Used by flush; reads go through [`Self::wal_tail`] which
    /// caches the result.
    fn load_wal_tail_records(&self, cell_runs: &[PreparedCellWalRun]) -> Result<Vec<VectorRecord>> {
        let mut records = Vec::new();
        for entry in cell_runs {
            if entry.kind != CellWalRunKind::Records {
                continue;
            }
            let read = self
                .storage
                .read_bytes_with_cache_status_and_checksum(&entry.path, &entry.checksum)?;
            records.extend(wal_records_from_table(read.bytes, &entry.path)?);
        }
        Ok(records)
    }

    fn wal_frontier_key(&self, cell_runs: &[PreparedCellWalRun]) -> WalFrontierKey {
        cell_runs.iter().map(cell_wal_run_identity).collect()
    }

    fn invalidate_wal_tail_cache(&self) {
        *self
            .wal_tail_cache
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = None;
    }

    fn wal_search_observation(&self) -> (usize, usize, usize, usize, usize) {
        let record_runs = self
            .unconsumed_cell_wal_runs()
            .into_iter()
            .filter(|run| run.kind == CellWalRunKind::Records)
            .collect::<Vec<_>>();
        let cells = record_runs
            .iter()
            .map(|run| (run.cell.routing_epoch, run.cell.cell_ordinal))
            .collect::<BTreeSet<_>>();
        let lanes = record_runs
            .iter()
            .map(|run| (run.cell.routing_epoch, run.cell.cell_ordinal, run.lane))
            .collect::<BTreeSet<_>>();
        (
            cells.len(),
            lanes.len(),
            record_runs.len(),
            record_runs.iter().map(|run| run.record_count).sum(),
            self.cell_wal_snapshot_retries.load(AtomicOrdering::Relaxed),
        )
    }

    fn apply_wal_search_observation(&self, report: &mut SearchReport) {
        (
            report.wal_cells_examined,
            report.wal_lanes_examined,
            report.wal_runs_examined,
            report.wal_records_examined,
            report.wal_snapshot_retries,
        ) = self.wal_search_observation();
    }

    /// The decoded, un-flushed WAL tail for this handle's manifest snapshot,
    /// cached by the frontier's ordered checksums. Empty (zero I/O) when the WAL
    /// is disabled or the frontier is empty.
    fn wal_tail(&self) -> Result<Arc<Vec<VectorRecord>>> {
        let cell_runs = self.unconsumed_cell_wal_runs();
        if cell_runs.is_empty() {
            return Ok(Arc::new(Vec::new()));
        }
        let key = self.wal_frontier_key(&cell_runs);
        {
            let cache = self
                .wal_tail_cache
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if let Some((cached_key, records)) = cache.as_ref()
                && *cached_key == key
            {
                return Ok(Arc::clone(records));
            }
        }
        let records = Arc::new(self.load_wal_tail_records(&cell_runs)?);
        let mut cache = self
            .wal_tail_cache
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        *cache = Some((key, Arc::clone(&records)));
        Ok(records)
    }

    /// The live WAL-tail records visible for a read: newest-generation-wins per
    /// id (so a later upsert in the tail supersedes an earlier add of the same
    /// id), with tombstone-suppressed records dropped. Empty when no WAL tail.
    fn live_wal_tail_records(&self) -> Result<Vec<VectorRecord>> {
        let tail = self.wal_tail()?;
        if tail.is_empty() {
            return Ok(Vec::new());
        }
        // Cell lanes have independent publication order, so generation—not a
        // collection-wide frontier position—selects the newest version.
        let mut newest: HashMap<Vec<u8>, VectorRecord> = HashMap::new();
        for record in tail.iter() {
            let key = record.id.as_bytes().to_vec();
            match newest.entry(key) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(record.clone());
                }
                std::collections::hash_map::Entry::Occupied(mut entry)
                    if record.generation > entry.get().generation =>
                {
                    entry.insert(record.clone());
                }
                std::collections::hash_map::Entry::Occupied(_) => {}
            }
        }
        let mut live = Vec::with_capacity(newest.len());
        for record in newest.into_values() {
            // Tombstone overlay (published in the same manifest) suppresses a
            // record whose generation is below the id's minimum visible
            // generation — so a delete/upsert supersedes a WAL-tail record too.
            if !self.is_suppressed(&record)? {
                live.push(record);
            }
        }
        // HashMap iteration order is intentionally unstable. A deterministic
        // id order is required because export/compatibility adapters paginate
        // `list_records` with an offset across separate calls.
        live.sort_by(|left, right| left.id.as_bytes().cmp(right.id.as_bytes()));
        Ok(live)
    }

    /// Build an in-memory BM25 index over live WAL-tail records that carry text, so
    /// text search can fold the un-flushed tail in as one extra virtual segment.
    /// Empty when the tail has no text-bearing live records.
    fn wal_bm25_sidecar(&self) -> Result<crate::bm25::Bm25IndexSidecar> {
        let rows = self
            .live_wal_tail_records()?
            .into_iter()
            .filter_map(|record| {
                record_text_terms(&record)
                    .map(|terms| (record.id.as_bytes().to_vec(), record.generation, terms))
            })
            .collect::<Vec<_>>();
        Ok(crate::bm25::Bm25IndexSidecar::from_text_rows(&rows))
    }

    /// The newest live WAL-tail copy of a single id, or `None` when the tail has
    /// no live copy (absent, or every copy tombstone-suppressed). Point-lookup
    /// variant of [`Self::live_wal_tail_records`].
    fn live_wal_tail_record_for_id(&self, id: &[u8]) -> Result<Option<VectorRecord>> {
        let tail = self.wal_tail()?;
        if tail.is_empty() {
            return Ok(None);
        }
        // Later frontier entries are newer; the first rev-scan match with the
        // highest generation is the visible copy.
        let mut best: Option<VectorRecord> = None;
        for record in tail.iter() {
            if record.id.as_bytes() != id {
                continue;
            }
            match &best {
                Some(current) if current.generation >= record.generation => {}
                _ => best = Some(record.clone()),
            }
        }
        match best {
            Some(record) if !self.is_suppressed(&record)? => Ok(Some(record)),
            _ => Ok(None),
        }
    }

    fn publish_manifest_reusing_routing_pages_with_recovery(
        &mut self,
        manifest: Manifest,
        previous: Option<&Manifest>,
    ) -> Result<Manifest> {
        Ok(self
            .publish_manifest_reusing_routing_pages_with_recovery_report(manifest, previous)?
            .0)
    }

    fn publish_manifest_reusing_routing_pages_with_recovery_report(
        &mut self,
        mut manifest: Manifest,
        previous: Option<&Manifest>,
    ) -> Result<(Manifest, StorageWriteReport)> {
        if !manifest.segments.is_empty() {
            self.rebuild_lexical_roots(&mut manifest)?;
        }
        let base_version = self.manifest.version;
        loop {
            match self.storage.stage_manifest_with_report(
                &self.manifest_reference.modality,
                &manifest,
                previous,
            ) {
                Ok((staged, mut report)) => {
                    match self.publish_staged_collection_manifest(staged, &mut report) {
                        Ok(published) => return Ok((published, report)),
                        Err(err) => self.advance_publish_version_after_conflict(
                            base_version,
                            &mut manifest,
                            err,
                        )?,
                    }
                }
                Err(err) => {
                    self.advance_publish_version_after_conflict(base_version, &mut manifest, err)?
                }
            }
        }
    }

    fn publish_manifest_with_routing_page_refs_with_recovery_report(
        &mut self,
        mut manifest: Manifest,
        page_refs: &[RoutingLayerPageRef],
        report: &mut StorageWriteReport,
    ) -> Result<Manifest> {
        if !manifest.segments.is_empty() {
            self.rebuild_lexical_roots(&mut manifest)?;
        }
        let base_version = self.manifest.version;
        loop {
            match self
                .storage
                .stage_manifest_with_routing_page_refs_with_report(
                    &self.manifest_reference.modality,
                    &manifest,
                    page_refs,
                    report,
                ) {
                Ok(staged) => match self.publish_staged_collection_manifest(staged, report) {
                    Ok(published) => return Ok(published),
                    Err(err) => self.advance_publish_version_after_conflict(
                        base_version,
                        &mut manifest,
                        err,
                    )?,
                },
                Err(err) => {
                    self.advance_publish_version_after_conflict(base_version, &mut manifest, err)?
                }
            }
        }
    }

    fn publish_manifest_with_top_routing_page_refs_with_recovery(
        &mut self,
        manifest: Manifest,
        routing_level: u8,
        page_refs: &[RoutingLayerPageRef],
    ) -> Result<Manifest> {
        let mut report = StorageWriteReport::default();
        self.publish_manifest_with_top_routing_page_refs_with_recovery_report(
            manifest,
            routing_level,
            page_refs,
            &mut report,
        )
    }

    fn publish_manifest_with_top_routing_page_refs_with_recovery_report(
        &mut self,
        mut manifest: Manifest,
        routing_level: u8,
        page_refs: &[RoutingLayerPageRef],
        report: &mut StorageWriteReport,
    ) -> Result<Manifest> {
        if !manifest.segments.is_empty() {
            self.rebuild_lexical_roots(&mut manifest)?;
        }
        let base_version = self.manifest.version;
        loop {
            match self
                .storage
                .stage_manifest_with_top_routing_page_refs_with_report(
                    &self.manifest_reference.modality,
                    &manifest,
                    routing_level,
                    page_refs,
                    report,
                ) {
                Ok(staged) => match self.publish_staged_collection_manifest(staged, report) {
                    Ok(published) => return Ok(published),
                    Err(err) => self.advance_publish_version_after_conflict(
                        base_version,
                        &mut manifest,
                        err,
                    )?,
                },
                Err(err) => {
                    self.advance_publish_version_after_conflict(base_version, &mut manifest, err)?
                }
            }
        }
    }

    fn publish_staged_collection_manifest(
        &mut self,
        staged: StagedManifest,
        report: &mut StorageWriteReport,
    ) -> Result<Manifest> {
        const MAX_COLLECTION_CAS_ATTEMPTS: usize = 128;
        let modality = self.manifest_reference.modality.clone();
        if staged.reference.modality != modality {
            return Err(BorsukError::InvalidStorage(format!(
                "staged modality `{}` does not match handle modality `{modality}`",
                staged.reference.modality
            )));
        }
        for _ in 0..MAX_COLLECTION_CAS_ATTEMPTS {
            let current = self.collection_storage.load_collection_snapshot()?;
            if modality == PRIMARY_MODALITY
                && collection_schema_fingerprint(&staged.manifest)
                    != current.snapshot.schema_fingerprint
            {
                return Err(BorsukError::InvalidStorage(
                    "primary manifest schema changed during collection publication".to_string(),
                ));
            }
            let position = current
                .snapshot
                .modalities
                .iter()
                .position(|reference| reference.modality == modality)
                .ok_or_else(|| {
                    BorsukError::InvalidStorage(format!(
                        "collection snapshot is missing modality `{modality}`"
                    ))
                })?;
            if current.snapshot.modalities[position] != self.manifest_reference {
                return Err(BorsukError::ConcurrentModification {
                    path: COLLECTION_CURRENT.to_string(),
                });
            }
            let mut next = current.snapshot.clone();
            next.generation = next.generation.checked_add(1).ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "collection snapshot generation exceeds u64".to_string(),
                )
            })?;
            next.previous_snapshot_checksum = Some(current.checksum.clone());
            next.modalities[position] = staged.reference.clone();
            match self
                .collection_storage
                .compare_and_swap_collection_snapshot_with_report(
                    current.current_version,
                    &next,
                    report,
                ) {
                Ok(loaded) => {
                    self.manifest_reference = staged.reference;
                    self.collection_snapshot = Some(loaded.clone());
                    for child in self.named.values_mut() {
                        child.collection_snapshot = Some(loaded.clone());
                    }
                    return Ok(staged.manifest);
                }
                Err(BorsukError::ConcurrentModification { .. }) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(BorsukError::ConcurrentModification {
            path: COLLECTION_CURRENT.to_string(),
        })
    }

    fn advance_publish_version_after_conflict(
        &mut self,
        base_version: u64,
        manifest: &mut Manifest,
        err: BorsukError,
    ) -> Result<()> {
        let conflict_path = match err {
            BorsukError::ConcurrentModification { path } => path,
            err => return Err(err),
        };
        let (refreshed_collection, refreshed_reference, refreshed) =
            self.load_latest_own_manifest()?;
        if refreshed.version != base_version {
            self.manifest = refreshed;
            self.manifest_reference = refreshed_reference;
            self.collection_snapshot = Some(refreshed_collection);
            return Err(BorsukError::ConcurrentModification {
                path: conflict_path,
            });
        }
        // Local filesystem storage cannot CAS the final CURRENT write and falls
        // back to a plain put. Re-check before treating an occupied future
        // namespace as orphaned so a slower in-flight writer can advance CURRENT.
        std::thread::sleep(VERSION_SKIP_CURRENT_RECHECK_DELAY);
        let (rechecked_collection, rechecked_reference, rechecked) =
            self.load_latest_own_manifest()?;
        if rechecked.version != base_version {
            self.manifest = rechecked;
            self.manifest_reference = rechecked_reference;
            self.collection_snapshot = Some(rechecked_collection);
            return Err(BorsukError::ConcurrentModification {
                path: conflict_path,
            });
        }
        manifest.version = manifest.version.checked_add(1).ok_or_else(|| {
            BorsukError::InvalidStorage("manifest version exceeds u64".to_string())
        })?;
        Ok(())
    }

    fn load_latest_own_manifest(
        &self,
    ) -> Result<(LoadedCollectionSnapshot, CollectionManifestRef, Manifest)> {
        let collection = self.collection_storage.load_collection_snapshot()?;
        let modality = &self.manifest_reference.modality;
        let reference = collection
            .snapshot
            .modalities
            .iter()
            .find(|reference| &reference.modality == modality)
            .ok_or_else(|| {
                BorsukError::InvalidStorage(format!(
                    "collection snapshot is missing modality `{modality}`"
                ))
            })?
            .clone();
        let manifest = self
            .collection_storage
            .load_manifest_ref(&reference, self.resident_routing_summaries().is_some())?;
        Ok((collection, reference, manifest))
    }

    fn add_records_to_top_routing_page_refs(
        &mut self,
        records: Vec<VectorRecord>,
        next_generated_id: u64,
        top_routing_level: u8,
        mut top_page_refs: Vec<RoutingLayerPageRef>,
        tombstone_update: Option<(TombstoneSummary, u64)>,
        bm25_stats_delta_update: Option<Option<Bm25StatsDeltaRef>>,
    ) -> Result<AddReport> {
        let vectors_added = records.len();
        if top_page_refs
            .iter()
            .any(|page_ref| page_ref.routing_level != top_routing_level)
        {
            return Err(BorsukError::InvalidStorage(
                "top routing page refs contain mixed routing levels".to_string(),
            ));
        }

        let chunks = records.chunks(self.manifest.config.segment_max_vectors);
        let mut manifest = self.manifest.next_version();
        manifest.segments.clear();
        manifest.pivots.clear();
        manifest.next_generated_id = next_generated_id;
        if let Some((tombstone, new_tombstone_ids)) = tombstone_update {
            manifest.tombstone_frontier.push(tombstone);
            manifest.tombstone_id_count = manifest
                .tombstone_id_count
                .checked_add(new_tombstone_ids)
                .ok_or_else(|| {
                    BorsukError::InvalidStorage("tombstone id count exceeds u64".to_string())
                })?;
        }
        if let Some(Some(update)) = bm25_stats_delta_update {
            manifest.bm25_stats_delta_frontier.push(update);
        }

        let mut new_summaries = Vec::<SegmentSummary>::new();
        let mut segments_written = 0_usize;
        let mut graph_payloads_written = 0_usize;
        let mut payload_bytes_written = 0_u64;
        for chunk in chunks {
            let segment_id = Uuid::new_v4().to_string();
            let segment = Segment::from_records_with_quantizer_and_geometry(
                segment_id,
                0,
                self.manifest.config.metric.clone(),
                self.manifest.config.dimensions,
                chunk.to_vec(),
                self.manifest.build_config.quantizer,
                self.manifest
                    .build_config
                    .normalized_angular_coarse_geometry,
            )?;
            let summary = self.write_segment(segment)?;
            segments_written += 1;
            graph_payloads_written += 1;
            payload_bytes_written +=
                summary.size_bytes + summary.vector_size_bytes + summary.graph_size_bytes;
            new_summaries.push(summary);
        }
        // Existing routing pages and their segment rows are unchanged. Keep a
        // finalized immutable global base, if present; the new right-edge pages
        // are a materialized delta until bounded delta-only compaction or an
        // explicit offline rebuild replaces the base.
        if self.manifest.config.text
            || self
                .manifest
                .config
                .named_vectors
                .values()
                .any(|spec| spec.kind == VectorKind::Sparse)
        {
            let mut lexical_summaries = self.active_segment_summaries()?;
            lexical_summaries.extend(new_summaries.iter().cloned());
            manifest.segments = lexical_summaries;
            self.rebuild_lexical_roots(&mut manifest)?;
            manifest.segments.clear();
        }

        let mut decoded_parent_pages = HashMap::new();
        if top_routing_level > 0
            && self
                .cache_rightmost_routing_branch(&top_page_refs, &mut decoded_parent_pages)
                .is_err()
        {
            decoded_parent_pages.clear();
        }

        let mut occupied_leaf_ranges = leaf_page_occupied_ranges_from_cached_tree(
            &top_page_refs,
            &decoded_parent_pages,
            self.manifest.routing_page_fanout,
        )?;
        let mut next_leaf_page_ordinal = 0_usize;
        let mut new_leaf_page_refs = Vec::new();
        let mut storage_report = StorageWriteReport::default();
        for summaries in new_summaries.chunks(self.manifest.routing_page_fanout) {
            let page_ordinal = next_available_leaf_page_ordinal(
                &mut next_leaf_page_ordinal,
                &mut occupied_leaf_ranges,
            )?;
            let page_ref = self.storage.write_routing_layer_page_with_report(
                &manifest,
                0,
                page_ordinal,
                summaries,
                &mut storage_report,
            )?;
            new_leaf_page_refs.push(page_ref);
        }

        if top_routing_level == 0 {
            top_page_refs.extend(new_leaf_page_refs);
            top_page_refs.sort_by_key(|page_ref| page_ref.page_ordinal);
            enforce_ram_budget(&manifest, self.runtime_ram_budget_bytes)?;
            let published = self.publish_manifest_with_routing_page_refs_with_recovery_report(
                manifest,
                &top_page_refs,
                &mut storage_report,
            )?;
            self.manifest = published;
            return Ok(add_report_from_parts(
                segments_written,
                graph_payloads_written,
                payload_bytes_written,
                storage_report,
                vectors_added,
            ));
        }

        let patch = self.routing_top_page_refs_with_leaf_updates_report(
            &manifest,
            top_routing_level,
            &top_page_refs,
            &new_leaf_page_refs,
            &mut decoded_parent_pages,
            Some(&mut storage_report),
        )?;
        let promoted_top_refs = self.promote_top_routing_page_refs_if_needed_with_report(
            &manifest,
            top_routing_level,
            patch.page_refs,
            Some(&mut storage_report),
        )?;
        enforce_ram_budget(&manifest, self.runtime_ram_budget_bytes)?;
        let published = self.publish_manifest_with_top_routing_page_refs_with_recovery_report(
            manifest,
            promoted_top_refs.routing_level,
            &promoted_top_refs.page_refs,
            &mut storage_report,
        )?;
        self.manifest = published;
        Ok(add_report_from_parts(
            segments_written,
            graph_payloads_written,
            payload_bytes_written,
            storage_report,
            vectors_added,
        ))
    }

    fn cache_rightmost_routing_branch(
        &self,
        top_page_refs: &[RoutingLayerPageRef],
        decoded_parent_pages: &mut HashMap<String, Vec<RoutingLayerPageRef>>,
    ) -> Result<()> {
        let Some(mut page_ref) = top_page_refs
            .iter()
            .max_by_key(|page_ref| page_ref.page_ordinal)
            .cloned()
        else {
            return Ok(());
        };

        while page_ref.routing_level > 0 {
            let child_read = self.routing_child_page_refs_read_from_parent_refs_with_cache(
                std::slice::from_ref(&page_ref),
                Some(decoded_parent_pages),
                None,
            )?;
            let Some(rightmost_child) = child_read
                .page_refs
                .into_iter()
                .max_by_key(|page_ref| page_ref.page_ordinal)
            else {
                return Ok(());
            };
            page_ref = rightmost_child;
        }

        Ok(())
    }

    /// Generate collision-free numeric string ids without scanning segment payloads.
    pub fn generate_ids(&self, count: usize) -> Result<Vec<String>> {
        let count_u64 = u64::try_from(count).map_err(|_| {
            BorsukError::InvalidRecordInput("generated id count does not fit u64".to_string())
        })?;
        let start = self.reserve_coordination_counter(
            "id-directory/generated/NEXT",
            self.cell_wal_next_generated_id_floor()?,
            count_u64,
        )?;
        let end = advance_generated_id(start, count)?;
        Ok((start..end).map(|id| id.to_string()).collect())
    }

    /// Load a stored vector by its identifier.
    pub fn get_vector(&self, id: &str) -> Result<Option<Vec<f32>>> {
        Ok(self.get_record(id)?.map(|(vector, _)| vector))
    }

    /// Load a stored vector by its byte identifier.
    pub fn get_vector_by_id(&self, id: impl AsRef<[u8]>) -> Result<Option<Vec<f32>>> {
        Ok(self.get_record_by_id(id)?.map(|(vector, _)| vector))
    }

    /// Load a stored vector together with its metadata by string id.
    pub fn get_record(&self, id: &str) -> Result<Option<(Vec<f32>, crate::Metadata)>> {
        if id.trim().is_empty() {
            return Err(BorsukError::InvalidRecordInput(
                "record ids must not be empty".to_string(),
            ));
        }

        self.get_record_by_id(id.as_bytes())
    }

    /// Load a stored vector together with its metadata by byte identifier.
    pub fn get_record_by_id(
        &self,
        id: impl AsRef<[u8]>,
    ) -> Result<Option<(Vec<f32>, crate::Metadata)>> {
        let id_bytes = id.as_ref();
        if id_bytes.is_empty() {
            return Err(BorsukError::InvalidRecordInput(
                "record ids must not be empty".to_string(),
            ));
        }

        // The WAL tail holds the newest un-flushed writes, so it supersedes any
        // published-segment copy of the same id (read-your-writes). Its records
        // are already MVCC-resolved (newest generation per id, suppressed
        // dropped) by `live_wal_tail_records`.
        if let Some(record) = self.live_wal_tail_record_for_id(id_bytes)? {
            return Ok(Some((record.vector.clone(), record.metadata.clone())));
        }

        // Scan newest segment first and return the first live (non-suppressed)
        // copy: an upsert writes the new version into a newer segment, so the
        // newest copy is the visible one and older generations are skipped.
        for summary in self.manifest.segments.iter().rev() {
            if !summary.might_contain_record_id(id_bytes) {
                continue;
            }
            let (segment, _, _, _) = self.read_segment(summary)?;
            for record in segment.records.iter().rev() {
                if record.id.as_bytes() == id_bytes && !self.is_suppressed(record)? {
                    return Ok(Some((record.vector.clone(), record.metadata.clone())));
                }
            }
        }

        if self.manifest.segments.is_empty() {
            return self.get_record_from_routing_pages(id_bytes);
        }

        Ok(None)
    }

    /// Load stored text term frequencies by record identifier.
    pub fn get_text_terms(&self, id: &RecordId) -> Result<Option<Vec<(u32, u32)>>> {
        let id_bytes = id.as_bytes();
        if id_bytes.is_empty() {
            return Err(BorsukError::InvalidRecordInput(
                "record ids must not be empty".to_string(),
            ));
        }

        // The WAL tail supersedes published segments (read-your-writes).
        if let Some(record) = self.live_wal_tail_record_for_id(id_bytes)? {
            return Ok(record_text_terms(&record));
        }

        for summary in self.manifest.segments.iter().rev() {
            if !summary.might_contain_record_id(id_bytes) {
                continue;
            }
            let (segment, _, _, _) = self.read_segment(summary)?;
            for record in segment.records.iter().rev() {
                if record.id.as_bytes() == id_bytes && !self.is_suppressed(record)? {
                    return Ok(record_text_terms(record));
                }
            }
        }

        if self.manifest.segments.is_empty() {
            return self.get_text_terms_from_routing_pages(id_bytes);
        }

        Ok(None)
    }

    /// A page of stored records for export/scroll use: `(id, vector, metadata)`
    /// for up to `limit` live records, skipping the first `offset`. Iterates
    /// active segments in manifest order and skips deleted records. This scans
    /// segment payloads, so it is an export/admin path (backing operations like
    /// a "scroll" or "get all" in the drop-in adapters), not a hot query path.
    pub fn list_records(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<(RecordId, Vec<f32>, crate::Metadata)>> {
        let mut out = Vec::new();
        if limit == 0 {
            return Ok(out);
        }
        let summaries = self.active_segment_summaries()?;
        let mut skipped = 0usize;
        for summary in &summaries {
            let (segment, _, _, _) = self.read_segment(summary)?;
            for record in &segment.records {
                if self.is_suppressed(record)? {
                    continue;
                }
                if skipped < offset {
                    skipped += 1;
                    continue;
                }
                out.push((
                    record.id.clone(),
                    record.vector.clone(),
                    record.metadata.clone(),
                ));
                if out.len() >= limit {
                    return Ok(out);
                }
            }
        }
        // Append the live WAL tail: un-flushed records that are not yet in any
        // segment. Suppressed/superseded copies are already dropped, and a WAL
        // record that supersedes a segment copy leaves that segment copy
        // tombstone-suppressed above, so no id is emitted twice.
        for record in self.live_wal_tail_records()? {
            if skipped < offset {
                skipped += 1;
                continue;
            }
            out.push((
                record.id.clone(),
                record.vector.clone(),
                record.metadata.clone(),
            ));
            if out.len() >= limit {
                return Ok(out);
            }
        }
        Ok(out)
    }

    fn get_record_from_routing_pages(
        &self,
        id_bytes: &[u8],
    ) -> Result<Option<(Vec<f32>, crate::Metadata)>> {
        let page_index_read = self.routing_layer_page_index_read_for_search()?;
        let page_refs = self
            .routing_leaf_page_refs_for_filter(&page_index_read.page_refs, |page_ref| {
                page_ref.might_contain_record_id(id_bytes)
            })?;

        for page_ref in page_refs.iter().rev() {
            let summaries =
                self.routing_summaries_from_page_refs(std::slice::from_ref(page_ref))?;
            for summary in summaries.iter().rev() {
                if !summary.might_contain_record_id(id_bytes) {
                    continue;
                }
                let (segment, _, _, _) = self.read_segment(summary)?;
                for record in segment.records.iter().rev() {
                    if record.id.as_bytes() == id_bytes && !self.is_suppressed(record)? {
                        return Ok(Some((record.vector.clone(), record.metadata.clone())));
                    }
                }
            }
        }

        Ok(None)
    }

    fn get_text_terms_from_routing_pages(
        &self,
        id_bytes: &[u8],
    ) -> Result<Option<Vec<(u32, u32)>>> {
        let page_index_read = self.routing_layer_page_index_read_for_search()?;
        let page_refs = self
            .routing_leaf_page_refs_for_filter(&page_index_read.page_refs, |page_ref| {
                page_ref.might_contain_record_id(id_bytes)
            })?;

        for page_ref in page_refs.iter().rev() {
            let summaries =
                self.routing_summaries_from_page_refs(std::slice::from_ref(page_ref))?;
            for summary in summaries.iter().rev() {
                if !summary.might_contain_record_id(id_bytes) {
                    continue;
                }
                let (segment, _, _, _) = self.read_segment(summary)?;
                for record in segment.records.iter().rev() {
                    if record.id.as_bytes() == id_bytes && !self.is_suppressed(record)? {
                        return Ok(record_text_terms(record));
                    }
                }
            }
        }

        Ok(None)
    }

    fn validate_text_records(&self, records: &mut [VectorRecord]) -> Result<()> {
        for record in records {
            if record.text.is_none()
                && record.text_term_ids.is_empty()
                && record.text_term_freqs.is_empty()
            {
                continue;
            }
            if !self.manifest.config.text {
                return Err(BorsukError::InvalidMetricInput(format!(
                    "record `{}` carries text data but this index was created with text=false",
                    record.id
                )));
            }

            if let Some(text) = record.text.take() {
                let terms = term_frequencies(self.tokenizer.as_ref(), &text);
                record.text_term_ids = terms.keys().copied().collect();
                record.text_term_freqs = terms.values().copied().collect();
            }
            validate_record_text_terms(record)?;
        }

        Ok(())
    }

    /// Validate ids for an add or upsert. `add` rejects ids that already exist
    /// or are tombstoned (insert-only); `upsert` (`allow_existing`) permits them,
    /// only enforcing non-empty ids and no duplicates within the batch.
    fn validate_record_ids_allowing_existing(
        &self,
        records: &[VectorRecord],
        scan_existing_ids: bool,
        allow_existing: bool,
    ) -> Result<()> {
        let mut batch_ids = HashSet::<&[u8]>::with_capacity(records.len());
        for record in records {
            if record.id.is_empty() {
                return Err(BorsukError::InvalidRecordInput(
                    "record ids must not be empty".to_string(),
                ));
            }
            if !batch_ids.insert(record.id.as_bytes()) {
                return Err(BorsukError::InvalidRecordInput(format!(
                    "duplicate record id `{}` in add batch",
                    record.id
                )));
            }
            // A tombstoned id (deleted or superseded) cannot be re-added through
            // `add`, which is insert-only; use `upsert` to replace an existing id.
            if !allow_existing && self.id_is_tombstoned(record.id.as_bytes())? {
                return Err(BorsukError::InvalidRecordInput(format!(
                    "record id `{}` is deleted; purge before re-adding it, or use upsert",
                    record.id
                )));
            }
        }

        if scan_existing_ids && !allow_existing {
            self.validate_record_ids_against_existing_segments(records)?;
        }

        Ok(())
    }

    fn validate_record_ids_against_existing_segments(
        &self,
        records: &[VectorRecord],
    ) -> Result<()> {
        // Reject re-adding an id that already lives in the un-flushed WAL tail:
        // `add` is insert-only, and a tail record is not yet in any segment, so
        // the segment scan below would miss it.
        let tail = self.wal_tail()?;
        if !tail.is_empty() {
            for record in records {
                let id = record.id.as_bytes();
                if tail.iter().any(|existing| {
                    existing.id.as_bytes() == id
                        // A tail copy that is tombstone-suppressed does not count
                        // as live; `id_is_tombstoned` already rejected those above.
                        && !self.is_suppressed(existing).unwrap_or(false)
                }) {
                    return Err(BorsukError::InvalidRecordInput(format!(
                        "duplicate record id `{}` already exists",
                        record.id
                    )));
                }
            }
        }

        if self.manifest.segments.is_empty() {
            return self.validate_record_ids_against_routing_pages(records);
        }

        for summary in &self.manifest.segments {
            if !records
                .iter()
                .any(|record| summary.might_contain_record_id(&record.id))
            {
                continue;
            }

            let (segment, _, _, _) = self.read_segment(summary)?;
            for record in records {
                if segment
                    .records
                    .iter()
                    .any(|existing| existing.id == record.id)
                {
                    return Err(BorsukError::InvalidRecordInput(format!(
                        "duplicate record id `{}` already exists",
                        record.id
                    )));
                }
            }
        }

        Ok(())
    }

    fn validate_record_ids_against_routing_pages(&self, records: &[VectorRecord]) -> Result<()> {
        let page_index_read = self.routing_layer_page_index_read_for_search()?;
        let page_refs =
            self.routing_leaf_page_refs_for_filter(&page_index_read.page_refs, |page_ref| {
                records
                    .iter()
                    .any(|record| page_ref.might_contain_record_id(&record.id))
            })?;
        for page_ref in page_refs.iter().rev() {
            let summaries =
                self.routing_summaries_from_page_refs(std::slice::from_ref(page_ref))?;
            for summary in summaries.iter().rev() {
                if !records
                    .iter()
                    .any(|record| summary.might_contain_record_id(&record.id))
                {
                    continue;
                }

                let (segment, _, _, _) = self.read_segment(summary)?;
                for record in records {
                    if segment
                        .records
                        .iter()
                        .any(|existing| existing.id == record.id)
                    {
                        return Err(BorsukError::InvalidRecordInput(format!(
                            "duplicate record id `{}` already exists",
                            record.id
                        )));
                    }
                }
            }
        }

        Ok(())
    }

    /// Compact immutable segments out-of-place into a higher target level.
    pub fn compact(&mut self, options: CompactionOptions) -> Result<CompactionReport> {
        // The un-flushed WAL tail must be materialized as part of compaction so
        // the built index holds every record. Rather than flush the tail into an
        // intermediate L0 segment (building its Parquet/dense-sidecar/graph/PQ)
        // that this very compaction would immediately read back, discard, and
        // rebuild into cells — a wasteful DOUBLE encode — compaction consumes the
        // tail records DIRECTLY: it folds them into its record set, builds the
        // final cell segments once, and clears the frontier in the same publish.
        //
        // This direct path is available only for the non-paged compaction (no
        // routing pages yet), which is exactly the first compaction after a bulk
        // WAL ingest — the case that matters. When routing pages already exist
        // (subsequent compactions of an already-organized index), the paged
        // rewrite keys off the specific source segments in dirty pages and has no
        // clean seam for loose tail records, so there we still flush the (small,
        // rare streaming) tail into L0 first. `flush()` recurses into children;
        // for the direct path the children are flushed explicitly below so a
        // compaction is atomic across modalities from a reader's perspective.
        let report = self.compact_primary(options.clone())?;
        for child in self.named.values_mut() {
            child.compact(options.clone())?;
        }
        Ok(report)
    }

    /// Whether the active index is organized into routing pages (a paged index),
    /// in which case compaction takes the routing-tree rewrite path rather than
    /// the flat non-paged path.
    fn compaction_is_paged(&self) -> Result<bool> {
        Ok(!self
            .routing_layer_page_index_read_for_compaction()?
            .page_refs
            .is_empty())
    }

    /// Compact this (primary) index, materializing the un-flushed WAL tail as part
    /// of the build. For the non-paged first compaction the tail records are folded
    /// directly into the record set and the frontier cleared in the same publish —
    /// no discarded intermediate L0 segment. For a paged index (no seam for loose
    /// tail records) the small/rare streaming tail is flushed to L0 first. Shared
    /// by [`Self::compact`] and the background maintenance loop.
    fn compact_primary(&mut self, options: CompactionOptions) -> Result<CompactionReport> {
        // Validate options BEFORE probing the routing tree so a malformed request
        // fails fast without any read (and never surfaces an unrelated read error
        // from a corrupt page index ahead of the input validation).
        validate_compaction_options(&options)?;
        if self.compaction_is_paged()? {
            self.flush_wal()?;
            self.compact_primary_impl(options, Vec::new(), false)
        } else {
            let tail = self.live_wal_tail_records()?;
            self.compact_primary_impl(options, tail, true)
        }
    }

    fn compact_primary_impl(
        &mut self,
        options: CompactionOptions,
        wal_tail_records: Vec<VectorRecord>,
        clear_frontier: bool,
    ) -> Result<CompactionReport> {
        let span = observability::compact_span(&options, self.manifest.version);
        let _entered = span.enter();
        let report = self.compact_impl(options, wal_tail_records, clear_frontier)?;
        observability::record_compaction_report(&span, &report);
        Ok(report)
    }

    /// `wal_tail_records` are the live un-flushed WAL-tail records folded directly
    /// into the (non-paged) compaction's record set so the tail is materialized by
    /// this single build instead of a discarded intermediate L0 segment; when
    /// `clear_frontier` is set the published manifest drops the frontier (the tail
    /// is now in the built cells). Both are empty/false for the paged path and for
    /// callers that pre-flushed the tail.
    fn compact_impl(
        &mut self,
        options: CompactionOptions,
        wal_tail_records: Vec<VectorRecord>,
        clear_frontier: bool,
    ) -> Result<CompactionReport> {
        validate_compaction_options(&options)?;

        let max_segments = options.max_segments.unwrap_or(usize::MAX);
        let page_index_read = self.routing_layer_page_index_read_for_compaction()?;
        if !page_index_read.page_refs.is_empty() {
            // Paged rewrite has no seam for loose tail records; callers route the
            // tail through a pre-flush, so nothing to fold in here.
            debug_assert!(wal_tail_records.is_empty() && !clear_frontier);
            return self.compact_from_routing_tree(options, max_segments, page_index_read);
        }

        let active_summaries = self.active_segment_summaries()?;
        let preserve_global_base =
            options.max_segments.is_some() && self.manifest.global_pq_ref.is_some();
        let global_base_segments = if preserve_global_base {
            self.manifest
                .global_pq_ref
                .as_ref()
                .expect("checked above")
                .segments
                .iter()
                .cloned()
                .collect::<HashSet<_>>()
        } else {
            HashSet::new()
        };
        let selected = active_summaries
            .iter()
            .filter(|summary| summary.level == options.source_level)
            .filter(|summary| !global_base_segments.contains(&summary.checksum))
            .take(max_segments)
            .cloned()
            .collect::<Vec<_>>();

        // The `min_segments` guard avoids churning when too few source segments
        // exist to be worth reorganizing — but an un-flushed tail folded in here
        // (`clear_frontier`) MUST be materialized, so a non-empty tail overrides
        // the guard. Without a tail this is the classic "not enough to compact"
        // no-op.
        if selected.len() < options.min_segments && wal_tail_records.is_empty() {
            return Ok(CompactionReport {
                compacted: false,
                source_level: options.source_level,
                target_level: options.target_level,
                segments_read: 0,
                segments_written: 0,
                records_rewritten: 0,
                routing_page_indexes_read: page_index_read.page_indexes_read,
                routing_pages_read: 0,
                routing_page_indexes_written: 0,
                routing_pages_written: 0,
                graph_payloads_read: 0,
                graph_bytes_read: 0,
                bytes_read: page_index_read.bytes_read,
                bytes_written: 0,
                object_cache_hits: page_index_read.object_cache_hits,
                object_cache_misses: page_index_read.object_cache_misses,
                manifest_version: self.manifest.version,
            });
        }

        let target_segment_max_vectors = options
            .target_segment_max_vectors
            .unwrap_or(self.manifest.config.segment_max_vectors);
        if target_segment_max_vectors == 0 {
            return Err(BorsukError::InvalidCompactionInput(
                "target_segment_max_vectors must be greater than zero".to_string(),
            ));
        }
        let mut records = Vec::<VectorRecord>::new();
        let mut bytes_read = page_index_read.bytes_read;
        let mut object_cache_hits = page_index_read.object_cache_hits;
        let mut object_cache_misses = page_index_read.object_cache_misses;

        crate::build_timing::timed(crate::build_timing::Phase::CompactionSourceRead, || {
            for summary in &selected {
                let (segment, segment_bytes_read, segment_cache_hit, _) =
                    self.read_segment_for_rewrite(summary)?;
                bytes_read += segment_bytes_read;
                count_cache_read(
                    segment_cache_hit,
                    &mut object_cache_hits,
                    &mut object_cache_misses,
                );
                records.extend(segment.records);
            }
            Ok::<_, BorsukError>(())
        })?;
        self.repopulate_sparse_named_records(&mut records, &selected)?;
        // Fold the un-flushed WAL tail directly into the record set (its dense
        // vectors are carried inline in the WAL codec, so no sidecar read). Tail
        // records are already MVCC-resolved (newest-generation-per-id, tombstone-
        // suppressed dropped) by `live_wal_tail_records`; appending them BEFORE
        // `drop_deleted_records` means any older-generation source-segment copy of
        // an id whose newest copy is in the tail (e.g. a cap-spilled L0 add later
        // upserted into the tail) is suppressed by the tombstone overlay, leaving
        // exactly one live copy per id in the built cells — identical to the
        // read-time overlay merge, and to flushing the tail first.
        records.extend(wal_tail_records);
        // Physically drop logically deleted rows so compaction reclaims their
        // storage. Tombstone entries are cleared only by purge(), which rewrites
        // every remaining occurrence.
        self.drop_deleted_records(&mut records)?;
        crate::build_timing::timed(crate::build_timing::Phase::LocalitySort, || {
            sort_records_by_vector_locality(
                &mut records,
                self.manifest.config.dimensions,
                target_segment_max_vectors,
            );
        });

        let selected_ids = selected
            .iter()
            .map(|summary| summary.id.as_str())
            .collect::<HashSet<_>>();
        let mut manifest = self.manifest.next_version();
        manifest.segments = active_summaries;
        manifest
            .segments
            .retain(|summary| !selected_ids.contains(summary.id.as_str()));
        // The tail is now materialized into the built cells; drop the frontier in
        // this same publish so the built version no longer references the WAL
        // objects (GC reclaims them) and reads no longer union the tail.
        if clear_frontier {
            manifest.cell_wal_consumed_runs.extend(
                self.unconsumed_cell_wal_runs()
                    .iter()
                    .map(cell_wal_run_identity),
            );
            Self::apply_cell_mutation_metadata_to_manifest(&mut manifest, &self.cell_wal_snapshot)?;
        }
        self.consolidate_mutation_frontiers(&mut manifest, true)?;

        let mut segments_written = 0_usize;
        let mut bytes_written = 0_u64;
        let records_rewritten = records.len();

        // Voronoi (k-means) cells, not axis-aligned locality slabs: tight
        // clusters whose centroids let approximate search probe only the few
        // nearest segments in high dimensions. Emitted in centroid-locality
        // order so the routing tree pages stay coherent.
        let kmeans_params = KmeansParams::from_build_config(&self.manifest.build_config);
        let chunks = crate::build_timing::timed(crate::build_timing::Phase::VoronoiChunks, || {
            voronoi_chunks(
                records,
                &self.manifest.config.metric,
                target_segment_max_vectors,
                options.target_segment_max_radius,
                &kmeans_params,
            )
        })?;
        for chunk in chunks {
            let segment_id = Uuid::new_v4().to_string();
            let segment = Segment::from_records_with_quantizer_and_geometry(
                segment_id,
                options.target_level,
                self.manifest.config.metric.clone(),
                self.manifest.config.dimensions,
                chunk,
                self.manifest.build_config.quantizer,
                self.manifest
                    .build_config
                    .normalized_angular_coarse_geometry,
            )?;
            let summary = self.write_segment(segment)?;
            bytes_written +=
                summary.size_bytes + summary.vector_size_bytes + summary.graph_size_bytes;
            segments_written += 1;
            manifest.segments.push(summary);
        }

        manifest.rebuild_pivots();
        if !preserve_global_base {
            let global_pq_summaries = manifest.segments.clone();
            manifest.global_pq_ref = self.persist_resident_global_pq(&global_pq_summaries)?;
        }
        let routing_pages_written = routing_page_tree_content_page_count(
            manifest.segments.len(),
            manifest.routing_page_fanout,
        );
        enforce_ram_budget(&manifest, self.runtime_ram_budget_bytes)?;
        let previous = self.manifest.clone();
        self.manifest =
            self.publish_manifest_reusing_routing_pages_with_recovery(manifest, Some(&previous))?;
        // Frontier just changed (tail folded in and cleared): drop any stale
        // decoded tail so the next read reloads against the new (empty) frontier.
        if clear_frontier {
            self.prune_consumed_cell_wal()?;
            self.cell_wal_snapshot.clear();
            self.manifest.cell_wal_visible_runs = 0;
            self.manifest.cell_wal_visible_tombstone_runs = 0;
            self.invalidate_wal_tail_cache();
        }
        let routing_page_indexes_written = usize::from(self.manifest.routing_max_level) + 1;
        // Compaction rebuilt the cell layout; refresh the persisted cold
        // quantizer so a cold/paged query routes through the IVF probe list.
        self.refresh_persisted_quantizer()?;

        Ok(CompactionReport {
            compacted: true,
            source_level: options.source_level,
            target_level: options.target_level,
            segments_read: selected.len(),
            segments_written,
            records_rewritten,
            routing_page_indexes_read: page_index_read.page_indexes_read,
            routing_pages_read: 0,
            routing_page_indexes_written,
            routing_pages_written,
            graph_payloads_read: 0,
            graph_bytes_read: 0,
            bytes_read,
            bytes_written,
            object_cache_hits,
            object_cache_misses,
            manifest_version: self.manifest.version,
        })
    }

    fn routing_layer_page_index_read_for_compaction(&self) -> Result<RoutingLayerPageIndexRead> {
        let top_read = self.storage.read_routing_layer_page_index_with_status(
            self.manifest.version,
            self.manifest.routing_max_level,
        )?;
        if !top_read.page_refs.is_empty() {
            return Ok(top_read);
        }

        if self.manifest.routing_max_level == 0 {
            return Ok(top_read);
        }

        let mut leaf_read = self
            .storage
            .read_routing_layer_page_index_with_status(self.manifest.version, 0)?;
        leaf_read.bytes_read += top_read.bytes_read;
        leaf_read.page_indexes_read += top_read.page_indexes_read;
        leaf_read.object_cache_hits += top_read.object_cache_hits;
        leaf_read.object_cache_misses += top_read.object_cache_misses;
        Ok(leaf_read)
    }

    fn compact_from_routing_tree(
        &mut self,
        options: CompactionOptions,
        max_segments: usize,
        page_index_read: RoutingLayerPageIndexRead,
    ) -> Result<CompactionReport> {
        let preserve_global_base =
            options.max_segments.is_some() && self.manifest.global_pq_ref.is_some();
        let global_base_segments = if preserve_global_base {
            self.manifest
                .global_pq_ref
                .as_ref()
                .expect("checked above")
                .segments
                .iter()
                .cloned()
                .collect::<HashSet<_>>()
        } else {
            HashSet::new()
        };
        let top_routing_level = page_index_read
            .page_refs
            .first()
            .map(|page_ref| page_ref.routing_level)
            .unwrap_or(0);
        let top_page_refs = page_index_read.page_refs.clone();
        let full_leaf_page_refs = page_index_read
            .page_refs
            .first()
            .is_some_and(|page_ref| page_ref.routing_level == 0)
            .then(|| page_index_read.page_refs.clone());
        let source_selection = self.compaction_source_selection_from_routing_tree(
            options.source_level,
            max_segments,
            page_index_read,
            &global_base_segments,
        )?;
        let selected = source_selection.selected;
        let dirty_pages = source_selection.dirty_pages;
        let mut decoded_parent_pages = source_selection.decoded_parent_pages;
        let routing_page_indexes_read = source_selection.routing_page_indexes_read;
        let routing_bytes_read = source_selection.bytes_read;
        let mut routing_pages_read = source_selection.routing_pages_read;
        let mut routing_pages_written = 0_usize;
        let routing_page_indexes_written;
        let routing_object_cache_hits = source_selection.object_cache_hits;
        let routing_object_cache_misses = source_selection.object_cache_misses;

        if selected.len() < options.min_segments {
            return Ok(CompactionReport {
                compacted: false,
                source_level: options.source_level,
                target_level: options.target_level,
                segments_read: 0,
                segments_written: 0,
                records_rewritten: 0,
                routing_page_indexes_read,
                routing_pages_read,
                routing_page_indexes_written: 0,
                routing_pages_written: 0,
                graph_payloads_read: 0,
                graph_bytes_read: 0,
                bytes_read: routing_bytes_read,
                bytes_written: 0,
                object_cache_hits: routing_object_cache_hits,
                object_cache_misses: routing_object_cache_misses,
                manifest_version: self.manifest.version,
            });
        }

        let target_segment_max_vectors = options
            .target_segment_max_vectors
            .unwrap_or(self.manifest.config.segment_max_vectors);
        if target_segment_max_vectors == 0 {
            return Err(BorsukError::InvalidCompactionInput(
                "target_segment_max_vectors must be greater than zero".to_string(),
            ));
        }

        let lexical_enabled = self.manifest.config.text
            || self
                .manifest
                .config
                .named_vectors
                .values()
                .any(|spec| spec.kind == VectorKind::Sparse);
        let mut lexical_active_summaries = lexical_enabled
            .then(|| self.active_segment_summaries())
            .transpose()?;
        let mut records = Vec::<VectorRecord>::new();
        let mut bytes_read = routing_bytes_read;
        let mut object_cache_hits = routing_object_cache_hits;
        let mut object_cache_misses = routing_object_cache_misses;

        crate::build_timing::timed(crate::build_timing::Phase::CompactionSourceRead, || {
            for summary in &selected {
                let (segment, segment_bytes_read, segment_cache_hit, _) =
                    self.read_segment_for_rewrite(summary)?;
                bytes_read += segment_bytes_read;
                count_cache_read(
                    segment_cache_hit,
                    &mut object_cache_hits,
                    &mut object_cache_misses,
                );
                records.extend(segment.records);
            }
            Ok::<_, BorsukError>(())
        })?;
        self.repopulate_sparse_named_records(&mut records, &selected)?;
        // Physically drop logically deleted rows so compaction reclaims their
        // storage. Tombstone entries are cleared only by purge(), which rewrites
        // every remaining occurrence.
        self.drop_deleted_records(&mut records)?;
        crate::build_timing::timed(crate::build_timing::Phase::LocalitySort, || {
            sort_records_by_vector_locality(
                &mut records,
                self.manifest.config.dimensions,
                target_segment_max_vectors,
            );
        });

        let selected_ids = selected
            .iter()
            .map(|summary| summary.id.as_str())
            .collect::<HashSet<_>>();
        let dirty_page_count = dirty_pages.len();
        let dirty_page_ordinals = dirty_pages
            .iter()
            .map(|(page_ordinal, _)| *page_ordinal)
            .collect::<Vec<_>>();
        let mut replacement_summaries = dirty_pages
            .into_iter()
            .flat_map(|(_, page_summaries)| page_summaries)
            .filter(|summary| !selected_ids.contains(summary.id.as_str()))
            .collect::<Vec<_>>();

        let mut manifest = self.manifest.next_version();
        manifest.segments.clear();
        manifest.pivots.clear();
        if !preserve_global_base {
            // An explicit unbounded/offline rebuild may rewrite base-covered
            // cells. Drop the old row ordinals before publishing and train the
            // replacement artifact from the complete resulting layout below.
            manifest.global_pq_ref = None;
        }

        let mut segments_written = 0_usize;
        let mut bytes_written = 0_u64;
        let mut new_lexical_summaries = Vec::new();
        let min_output_segments = dirty_page_count
            .saturating_sub(replacement_summaries.len())
            .max(1);
        let output_chunk_size = output_segment_chunk_size(
            records.len(),
            target_segment_max_vectors,
            min_output_segments,
        );

        let records_rewritten = records.len();
        // Voronoi (k-means) cells — see the sibling compaction path.
        let kmeans_params = KmeansParams::from_build_config(&self.manifest.build_config);
        let chunks = crate::build_timing::timed(crate::build_timing::Phase::VoronoiChunks, || {
            voronoi_chunks(
                records,
                &self.manifest.config.metric,
                output_chunk_size,
                options.target_segment_max_radius,
                &kmeans_params,
            )
        })?;
        for chunk in chunks {
            let segment_id = Uuid::new_v4().to_string();
            let segment = Segment::from_records_with_quantizer_and_geometry(
                segment_id,
                options.target_level,
                self.manifest.config.metric.clone(),
                self.manifest.config.dimensions,
                chunk,
                self.manifest.build_config.quantizer,
                self.manifest
                    .build_config
                    .normalized_angular_coarse_geometry,
            )?;
            let summary = self.write_segment(segment)?;
            bytes_written +=
                summary.size_bytes + summary.vector_size_bytes + summary.graph_size_bytes;
            segments_written += 1;
            new_lexical_summaries.push(summary.clone());
            replacement_summaries.push(summary);
        }

        if let Some(active) = lexical_active_summaries.as_mut() {
            active.retain(|summary| !selected_ids.contains(summary.id.as_str()));
            active.append(&mut new_lexical_summaries);
            manifest.segments = active.clone();
            self.rebuild_lexical_roots(&mut manifest)?;
            // Paged manifests route segment summaries through the immutable
            // routing tree; only the compact global lexical roots stay here.
            manifest.segments.clear();
        }

        let replacement_pages = split_summaries_for_routing_pages(
            replacement_summaries,
            dirty_page_count,
            manifest.routing_page_fanout,
        );
        let needs_leaf_page_append = replacement_pages.len() > dirty_page_count;
        if let Some(mut page_refs) = full_leaf_page_refs {
            let mut occupied_leaf_ranges = leaf_page_occupied_ranges_from_cached_tree(
                &page_refs,
                &HashMap::new(),
                manifest.routing_page_fanout,
            )?;
            let mut next_appended_leaf_ordinal = dirty_page_ordinals.first().copied().unwrap_or(0);

            for (chunk_index, summaries) in replacement_pages.iter().enumerate() {
                let target_page_ordinal = if chunk_index < dirty_page_count {
                    dirty_page_ordinals[chunk_index]
                } else {
                    next_available_leaf_page_ordinal(
                        &mut next_appended_leaf_ordinal,
                        &mut occupied_leaf_ranges,
                    )?
                };
                let page_ref = self.storage.write_routing_layer_page(
                    &manifest,
                    0,
                    target_page_ordinal,
                    summaries,
                )?;
                routing_pages_written += 1;
                upsert_leaf_page_ref_by_ordinal(&mut page_refs, page_ref)?;
            }
            let promoted_top_refs =
                self.promote_top_routing_page_refs_if_needed(&manifest, 0, page_refs)?;
            routing_pages_written += promoted_top_refs.routing_pages_written;
            enforce_ram_budget(&manifest, self.runtime_ram_budget_bytes)?;
            self.manifest = self.publish_manifest_with_top_routing_page_refs_with_recovery(
                manifest,
                promoted_top_refs.routing_level,
                &promoted_top_refs.page_refs,
            )?;
            routing_page_indexes_written = 1;
        } else if needs_leaf_page_append {
            let mut occupied_leaf_ranges = leaf_page_occupied_ranges_from_cached_tree(
                &top_page_refs,
                &decoded_parent_pages,
                manifest.routing_page_fanout,
            )?;
            let mut next_appended_leaf_ordinal = dirty_page_ordinals.first().copied().unwrap_or(0);
            let mut updated_leaf_page_refs = Vec::with_capacity(replacement_pages.len());

            for (chunk_index, summaries) in replacement_pages.iter().enumerate() {
                let target_page_ordinal = if chunk_index < dirty_page_count {
                    dirty_page_ordinals[chunk_index]
                } else {
                    next_available_leaf_page_ordinal(
                        &mut next_appended_leaf_ordinal,
                        &mut occupied_leaf_ranges,
                    )?
                };
                updated_leaf_page_refs.push(self.storage.write_routing_layer_page(
                    &manifest,
                    0,
                    target_page_ordinal,
                    summaries,
                )?);
                routing_pages_written += 1;
            }

            let patch = self.routing_top_page_refs_with_leaf_updates(
                &manifest,
                top_routing_level,
                &top_page_refs,
                &updated_leaf_page_refs,
                &mut decoded_parent_pages,
            )?;
            bytes_read += patch.bytes_read;
            routing_pages_read += patch.routing_pages_read;
            routing_pages_written += patch.routing_pages_written;
            object_cache_hits += patch.object_cache_hits;
            object_cache_misses += patch.object_cache_misses;
            let promoted_top_refs = self.promote_top_routing_page_refs_if_needed(
                &manifest,
                top_routing_level,
                patch.page_refs,
            )?;
            routing_pages_written += promoted_top_refs.routing_pages_written;
            enforce_ram_budget(&manifest, self.runtime_ram_budget_bytes)?;
            self.manifest = self.publish_manifest_with_top_routing_page_refs_with_recovery(
                manifest,
                promoted_top_refs.routing_level,
                &promoted_top_refs.page_refs,
            )?;
            routing_page_indexes_written = 1;
        } else {
            let mut replacement_leaf_page_refs = Vec::with_capacity(replacement_pages.len());
            for (chunk_index, summaries) in replacement_pages.iter().enumerate() {
                let target_page_ordinal = dirty_page_ordinals[chunk_index];
                replacement_leaf_page_refs.push(self.storage.write_routing_layer_page(
                    &manifest,
                    0,
                    target_page_ordinal,
                    summaries,
                )?);
                routing_pages_written += 1;
            }
            let patch = self.routing_top_page_refs_with_leaf_updates(
                &manifest,
                top_routing_level,
                &top_page_refs,
                &replacement_leaf_page_refs,
                &mut decoded_parent_pages,
            )?;
            bytes_read += patch.bytes_read;
            routing_pages_read += patch.routing_pages_read;
            routing_pages_written += patch.routing_pages_written;
            object_cache_hits += patch.object_cache_hits;
            object_cache_misses += patch.object_cache_misses;
            let promoted_top_refs = self.promote_top_routing_page_refs_if_needed(
                &manifest,
                top_routing_level,
                patch.page_refs,
            )?;
            routing_pages_written += promoted_top_refs.routing_pages_written;
            enforce_ram_budget(&manifest, self.runtime_ram_budget_bytes)?;
            self.manifest = self.publish_manifest_with_top_routing_page_refs_with_recovery(
                manifest,
                promoted_top_refs.routing_level,
                &promoted_top_refs.page_refs,
            )?;
            routing_page_indexes_written = 1;
        }

        // Compaction rebuilt the (paged) cell layout; refresh the persisted cold
        // quantizer from the full active summary set so a cold/paged query routes
        // through the IVF probe list instead of the degraded routing tree.
        self.refresh_persisted_quantizer()?;
        if options.max_segments.is_none() {
            self.refresh_resident_global_pq()?;
        }

        Ok(CompactionReport {
            compacted: true,
            source_level: options.source_level,
            target_level: options.target_level,
            segments_read: selected.len(),
            segments_written,
            records_rewritten,
            routing_page_indexes_read,
            routing_pages_read,
            routing_page_indexes_written,
            routing_pages_written,
            graph_payloads_read: 0,
            graph_bytes_read: 0,
            bytes_read,
            bytes_written,
            object_cache_hits,
            object_cache_misses,
            manifest_version: self.manifest.version,
        })
    }

    fn promote_top_routing_page_refs_if_needed(
        &self,
        manifest: &Manifest,
        routing_level: u8,
        page_refs: Vec<RoutingLayerPageRef>,
    ) -> Result<CompactionTopRoutingPageRefs> {
        self.promote_top_routing_page_refs_if_needed_with_report(
            manifest,
            routing_level,
            page_refs,
            None,
        )
    }

    fn promote_top_routing_page_refs_if_needed_with_report(
        &self,
        manifest: &Manifest,
        mut routing_level: u8,
        mut page_refs: Vec<RoutingLayerPageRef>,
        mut storage_report: Option<&mut StorageWriteReport>,
    ) -> Result<CompactionTopRoutingPageRefs> {
        let mut routing_pages_written = 0_usize;

        while page_refs.len() > manifest.routing_page_fanout {
            if page_refs
                .iter()
                .any(|page_ref| page_ref.routing_level != routing_level)
            {
                return Err(BorsukError::InvalidStorage(
                    "top routing page refs contain mixed routing levels".to_string(),
                ));
            }
            let parent_routing_level = routing_level.checked_add(1).ok_or_else(|| {
                BorsukError::InvalidStorage("routing layer depth exceeds u8".to_string())
            })?;
            let grouped_child_refs =
                routing_page_refs_by_parent_ordinal(&page_refs, manifest.routing_page_fanout);
            let mut promoted_page_refs = Vec::with_capacity(grouped_child_refs.len());
            for (page_ordinal, child_refs) in grouped_child_refs {
                let page_ref = if let Some(report) = storage_report.as_deref_mut() {
                    self.storage.write_parent_routing_layer_page_with_report(
                        manifest,
                        parent_routing_level,
                        page_ordinal,
                        &child_refs,
                        report,
                    )?
                } else {
                    self.storage.write_parent_routing_layer_page(
                        manifest,
                        parent_routing_level,
                        page_ordinal,
                        &child_refs,
                    )?
                };
                promoted_page_refs.push(page_ref);
                routing_pages_written += 1;
            }
            routing_level = parent_routing_level;
            page_refs = promoted_page_refs;
        }

        Ok(CompactionTopRoutingPageRefs {
            routing_level,
            page_refs,
            routing_pages_written,
        })
    }

    fn routing_top_page_refs_with_leaf_updates(
        &self,
        manifest: &Manifest,
        top_routing_level: u8,
        top_page_refs: &[RoutingLayerPageRef],
        updated_leaf_page_refs: &[RoutingLayerPageRef],
        decoded_parent_pages: &mut HashMap<String, Vec<RoutingLayerPageRef>>,
    ) -> Result<CompactionRoutingPatch> {
        self.routing_top_page_refs_with_leaf_updates_report(
            manifest,
            top_routing_level,
            top_page_refs,
            updated_leaf_page_refs,
            decoded_parent_pages,
            None,
        )
    }

    fn routing_top_page_refs_with_leaf_updates_report(
        &self,
        manifest: &Manifest,
        top_routing_level: u8,
        top_page_refs: &[RoutingLayerPageRef],
        updated_leaf_page_refs: &[RoutingLayerPageRef],
        decoded_parent_pages: &mut HashMap<String, Vec<RoutingLayerPageRef>>,
        mut storage_report: Option<&mut StorageWriteReport>,
    ) -> Result<CompactionRoutingPatch> {
        if top_routing_level == 0 {
            return Err(BorsukError::InvalidStorage(
                "top routing update without L0 page refs".to_string(),
            ));
        }
        let updates = leaf_page_ref_updates_by_ordinal(updated_leaf_page_refs)?;
        let mut rewritten_top_refs = Vec::with_capacity(top_page_refs.len());
        let mut patch = CompactionRoutingPatch::default();
        for page_ref in top_page_refs {
            if routing_subtree_contains_leaf_update(
                page_ref,
                &updates,
                manifest.routing_page_fanout,
            ) {
                let update = self.routing_parent_page_ref_with_leaf_updates_report(
                    manifest,
                    page_ref,
                    &updates,
                    decoded_parent_pages,
                    storage_report.as_deref_mut(),
                )?;
                patch.bytes_read += update.patch.bytes_read;
                patch.routing_pages_read += update.patch.routing_pages_read;
                patch.routing_pages_written += update.patch.routing_pages_written;
                patch.object_cache_hits += update.patch.object_cache_hits;
                patch.object_cache_misses += update.patch.object_cache_misses;
                rewritten_top_refs.push(update.page_ref);
            } else {
                rewritten_top_refs.push(page_ref.clone());
            }
        }

        let existing_top_page_ordinals = top_page_refs
            .iter()
            .map(|page_ref| page_ref.page_ordinal)
            .collect::<HashSet<_>>();
        let new_top_leaf_updates = leaf_page_ref_updates_by_parent_ordinal(
            top_routing_level,
            updated_leaf_page_refs.iter().filter(|page_ref| {
                !top_page_refs.iter().any(|top_page_ref| {
                    routing_subtree_contains_leaf_ordinal(
                        top_page_ref,
                        page_ref.page_ordinal,
                        manifest.routing_page_fanout,
                    )
                })
            }),
            manifest.routing_page_fanout,
        )?;
        for (top_page_ordinal, leaf_updates) in new_top_leaf_updates {
            if existing_top_page_ordinals.contains(&top_page_ordinal) {
                continue;
            }
            let update = self.routing_parent_page_ref_from_leaf_updates_report(
                manifest,
                top_routing_level,
                top_page_ordinal,
                &leaf_updates,
                storage_report.as_deref_mut(),
            )?;
            patch.routing_pages_written += update.patch.routing_pages_written;
            rewritten_top_refs.push(update.page_ref);
        }
        rewritten_top_refs.sort_by_key(|page_ref| page_ref.page_ordinal);
        patch.page_refs = rewritten_top_refs;
        Ok(patch)
    }

    fn routing_parent_page_ref_with_leaf_updates_report(
        &self,
        manifest: &Manifest,
        parent_ref: &RoutingLayerPageRef,
        updates: &HashMap<usize, RoutingLayerPageRef>,
        decoded_parent_pages: &mut HashMap<String, Vec<RoutingLayerPageRef>>,
        mut storage_report: Option<&mut StorageWriteReport>,
    ) -> Result<CompactionRoutingPageUpdate> {
        let child_routing_level = parent_ref.routing_level.checked_sub(1).ok_or_else(|| {
            BorsukError::InvalidStorage("cannot rewrite children below L0 routing page".to_string())
        })?;
        let child_read = self.routing_child_page_refs_read_from_parent_refs_with_cache(
            std::slice::from_ref(parent_ref),
            Some(decoded_parent_pages),
            None,
        )?;
        let mut patch = CompactionRoutingPatch {
            bytes_read: child_read.bytes_read,
            routing_pages_read: child_read.routing_pages_read,
            object_cache_hits: child_read.object_cache_hits,
            object_cache_misses: child_read.object_cache_misses,
            ..Default::default()
        };
        let mut child_refs = child_read.page_refs;
        let mut existing_child_ordinals = HashSet::with_capacity(child_refs.len());
        for child_ref in &mut child_refs {
            existing_child_ordinals.insert(child_ref.page_ordinal);
            if child_routing_level == 0 {
                if let Some(update) = updates.get(&child_ref.page_ordinal) {
                    *child_ref = update.clone();
                }
            } else if routing_subtree_contains_leaf_update(
                child_ref,
                updates,
                manifest.routing_page_fanout,
            ) {
                let update = self.routing_parent_page_ref_with_leaf_updates_report(
                    manifest,
                    child_ref,
                    updates,
                    decoded_parent_pages,
                    storage_report.as_deref_mut(),
                );
                let update = update?;
                patch.bytes_read += update.patch.bytes_read;
                patch.routing_pages_read += update.patch.routing_pages_read;
                patch.routing_pages_written += update.patch.routing_pages_written;
                patch.object_cache_hits += update.patch.object_cache_hits;
                patch.object_cache_misses += update.patch.object_cache_misses;
                *child_ref = update.page_ref;
            }
        }

        let new_child_updates = leaf_page_ref_updates_by_parent_ordinal(
            child_routing_level,
            updates
                .values()
                .filter(|page_ref| {
                    routing_subtree_contains_leaf_ordinal(
                        parent_ref,
                        page_ref.page_ordinal,
                        manifest.routing_page_fanout,
                    )
                })
                .filter(|page_ref| {
                    let child_ordinal = routing_parent_ordinal_for_leaf(
                        child_routing_level,
                        page_ref.page_ordinal,
                        manifest.routing_page_fanout,
                    )
                    .ok();
                    child_ordinal.is_some_and(|ordinal| !existing_child_ordinals.contains(&ordinal))
                }),
            manifest.routing_page_fanout,
        )?;
        for (child_page_ordinal, leaf_updates) in new_child_updates {
            if child_routing_level == 0 {
                child_refs.extend(leaf_updates);
            } else {
                let update = self.routing_parent_page_ref_from_leaf_updates_report(
                    manifest,
                    child_routing_level,
                    child_page_ordinal,
                    &leaf_updates,
                    storage_report.as_deref_mut(),
                )?;
                patch.routing_pages_written += update.patch.routing_pages_written;
                child_refs.push(update.page_ref);
            }
        }
        child_refs.sort_by_key(|page_ref| page_ref.page_ordinal);

        let page_ref = if let Some(report) = storage_report {
            self.storage.write_parent_routing_layer_page_with_report(
                manifest,
                parent_ref.routing_level,
                parent_ref.page_ordinal,
                &child_refs,
                report,
            )?
        } else {
            self.storage.write_parent_routing_layer_page(
                manifest,
                parent_ref.routing_level,
                parent_ref.page_ordinal,
                &child_refs,
            )?
        };
        patch.routing_pages_written += 1;
        Ok(CompactionRoutingPageUpdate { page_ref, patch })
    }

    fn routing_parent_page_ref_from_leaf_updates_report(
        &self,
        manifest: &Manifest,
        routing_level: u8,
        page_ordinal: usize,
        leaf_updates: &[RoutingLayerPageRef],
        mut storage_report: Option<&mut StorageWriteReport>,
    ) -> Result<CompactionRoutingPageUpdate> {
        if routing_level == 0 {
            return Err(BorsukError::InvalidStorage(
                "cannot build parent routing page at L0".to_string(),
            ));
        }
        for leaf_update in leaf_updates {
            let parent_ordinal = routing_parent_ordinal_for_leaf(
                routing_level,
                leaf_update.page_ordinal,
                manifest.routing_page_fanout,
            )?;
            if parent_ordinal != page_ordinal {
                return Err(BorsukError::InvalidStorage(format!(
                    "leaf routing page {} does not belong to L{} parent page {}",
                    leaf_update.page_ordinal, routing_level, page_ordinal
                )));
            }
        }
        let child_routing_level = routing_level.checked_sub(1).ok_or_else(|| {
            BorsukError::InvalidStorage("cannot build children below L0 routing page".to_string())
        })?;
        let grouped_updates = leaf_page_ref_updates_by_parent_ordinal(
            child_routing_level,
            leaf_updates.iter(),
            manifest.routing_page_fanout,
        )?;
        let mut child_refs = Vec::with_capacity(grouped_updates.len());
        let mut patch = CompactionRoutingPatch::default();
        for (child_page_ordinal, leaf_updates) in grouped_updates {
            if child_routing_level == 0 {
                child_refs.extend(leaf_updates);
            } else {
                let update = self.routing_parent_page_ref_from_leaf_updates_report(
                    manifest,
                    child_routing_level,
                    child_page_ordinal,
                    &leaf_updates,
                    storage_report.as_deref_mut(),
                )?;
                patch.routing_pages_written += update.patch.routing_pages_written;
                child_refs.push(update.page_ref);
            }
        }
        child_refs.sort_by_key(|page_ref| page_ref.page_ordinal);

        let page_ref = if let Some(report) = storage_report {
            self.storage.write_parent_routing_layer_page_with_report(
                manifest,
                routing_level,
                page_ordinal,
                &child_refs,
                report,
            )?
        } else {
            self.storage.write_parent_routing_layer_page(
                manifest,
                routing_level,
                page_ordinal,
                &child_refs,
            )?
        };
        patch.routing_pages_written += 1;
        Ok(CompactionRoutingPageUpdate { page_ref, patch })
    }

    /// Rebuild a full source level into a target level, then report or delete obsolete objects.
    ///
    /// When `delete_obsolete` is enabled, the cleanup pass uses `min_age = Duration::ZERO`.
    /// Callers must provide external quiescence: no concurrent readers or writers may depend on
    /// old objects while the rebuild cleanup runs. Use `compact` followed by
    /// `gc_obsolete_segments` with an explicit retention interval when concurrent handles may
    /// still be active.
    pub fn rebuild(&mut self, options: RebuildOptions) -> Result<RebuildReport> {
        let compaction = self.compact(CompactionOptions {
            source_level: options.source_level,
            target_level: options.target_level,
            max_segments: None,
            min_segments: options.min_segments,
            target_segment_max_vectors: options.target_segment_max_vectors,
            target_segment_max_radius: None,
        })?;
        let garbage_collection = self.gc_obsolete_segments(GarbageCollectionOptions {
            dry_run: !options.delete_obsolete,
            min_age: Duration::ZERO,
        })?;

        Ok(RebuildReport {
            compaction,
            garbage_collection,
        })
    }

    /// Delete inactive index objects that are no longer referenced by the current manifest.
    pub fn gc_obsolete_segments(
        &mut self,
        options: GarbageCollectionOptions,
    ) -> Result<GarbageCollectionReport> {
        let report = self.gc_obsolete_segments_primary(options.clone())?;
        for child in self.named.values_mut() {
            child.gc_obsolete_segments(options.clone())?;
        }
        Ok(report)
    }

    fn gc_obsolete_segments_primary(
        &mut self,
        options: GarbageCollectionOptions,
    ) -> Result<GarbageCollectionReport> {
        let span = observability::gc_span(&options, self.manifest.version);
        let _entered = span.enter();
        let report = self.gc_obsolete_segments_impl(options)?;
        observability::record_gc_report(&span, &report);
        Ok(report)
    }

    fn gc_obsolete_segments_impl(
        &mut self,
        options: GarbageCollectionOptions,
    ) -> Result<GarbageCollectionReport> {
        self.refresh()?;
        let now = Utc::now();
        let mut active_paths = self.active_segment_object_paths()?;
        // Retention is obsolescence-based: an object may be deleted only when no retained
        // manifest version references it. A version stays retained until the version that
        // superseded it is itself at least `min_age` old, so anything compacted out of the
        // active manifest keeps its references alive for `min_age` after obsolescence.
        for version in self.retained_manifest_versions(options.min_age, now)? {
            let Some(manifest) = self.storage.load_manifest_for_version(version)? else {
                continue;
            };
            let retained = self.object_paths_for_retained_manifest(manifest)?;
            active_paths.paths.extend(retained.paths);
            active_paths.bytes_read += retained.bytes_read;
            active_paths.routing_page_indexes_read += retained.routing_page_indexes_read;
            active_paths.routing_pages_read += retained.routing_pages_read;
            active_paths.object_cache_hits += retained.object_cache_hits;
            active_paths.object_cache_misses += retained.object_cache_misses;
        }
        let mut objects_scanned = 0_usize;
        let mut candidates = Vec::new();
        {
            let mut scan = GarbageCollectionCandidateScan {
                active_paths: &active_paths.paths,
                min_age: options.min_age,
                now,
                objects_scanned: &mut objects_scanned,
                candidates: &mut candidates,
            };
            self.collect_gc_candidates(
                "segments",
                is_segment_table_path,
                GarbageCollectionObjectKind::SegmentOrGraph,
                &mut scan,
            )?;
            self.collect_gc_candidates(
                "graphs",
                is_parquet_path,
                GarbageCollectionObjectKind::SegmentOrGraph,
                &mut scan,
            )?;
            // The content-addressed dense-vector Arrow sidecar (`vectors/<cs>.arrow`).
            // The keep-set retains it for every non-empty-dimension active/retained
            // summary, but without listing this prefix an orphaned sidecar left behind
            // by compaction/purge would never become a deletion candidate and would leak.
            self.collect_gc_candidates(
                "vectors",
                is_vector_sidecar_path,
                GarbageCollectionObjectKind::SegmentOrGraph,
                &mut scan,
            )?;
            self.collect_gc_candidates(
                "fidx",
                is_filter_index_path,
                GarbageCollectionObjectKind::SegmentOrGraph,
                &mut scan,
            )?;
            // The persisted coarse-quantizer object (`quantizer/<cs>.parquet`). The
            // keep-set retains the object each active/retained manifest
            // references; without listing this prefix a superseded quantizer
            // (from a prior compaction) would never become a deletion candidate.
            self.collect_gc_candidates(
                "quantizer",
                is_quantizer_path,
                GarbageCollectionObjectKind::SegmentOrGraph,
                &mut scan,
            )?;
            self.collect_gc_candidates(
                "global-pq",
                is_global_pq_path,
                GarbageCollectionObjectKind::SegmentOrGraph,
                &mut scan,
            )?;
            self.collect_gc_candidates(
                "lexical",
                is_parquet_path,
                GarbageCollectionObjectKind::SegmentOrGraph,
                &mut scan,
            )?;
            self.collect_gc_candidates(
                "routing/pages",
                is_parquet_path,
                GarbageCollectionObjectKind::Routing,
                &mut scan,
            )?;
            self.collect_gc_candidates(
                "routing/layers",
                is_parquet_path,
                GarbageCollectionObjectKind::Routing,
                &mut scan,
            )?;
            self.collect_gc_candidates(
                "manifests",
                is_manifest_table_path,
                GarbageCollectionObjectKind::Table,
                &mut scan,
            )?;
            self.collect_gc_candidates(
                "routing",
                is_routing_metadata_table_path,
                GarbageCollectionObjectKind::Table,
                &mut scan,
            )?;
            // Content-addressed tombstone overlays. The keep-set retains only the
            // tombstone referenced by each active/retained manifest, so a tombstone
            // superseded by a newer overlay would leak without listing this prefix.
            self.collect_gc_candidates(
                "tombstones",
                is_tombstone_table_path,
                GarbageCollectionObjectKind::Table,
                &mut scan,
            )?;
            self.collect_gc_candidates(
                "cells",
                is_cell_wal_immutable_path,
                GarbageCollectionObjectKind::SegmentOrGraph,
                &mut scan,
            )?;
            self.collect_gc_candidates(
                "transactions",
                is_cell_wal_transaction_path,
                GarbageCollectionObjectKind::Table,
                &mut scan,
            )?;
        }
        candidates.sort_by(|left, right| left.path.cmp(&right.path));
        let bytes_reclaimable = candidates.iter().map(|object| object.size).sum::<u64>();
        let candidate_paths = candidates
            .iter()
            .map(|object| object.path.clone())
            .collect::<Vec<_>>();

        if options.dry_run {
            return Ok(GarbageCollectionReport {
                dry_run: true,
                objects_scanned,
                objects_deleted: 0,
                routing_objects_deleted: 0,
                tables_deleted: 0,
                routing_page_indexes_read: active_paths.routing_page_indexes_read,
                routing_pages_read: active_paths.routing_pages_read,
                bytes_read: active_paths.bytes_read,
                bytes_reclaimable,
                bytes_reclaimed: 0,
                object_cache_hits: active_paths.object_cache_hits,
                object_cache_misses: active_paths.object_cache_misses,
                candidates: candidate_paths,
            });
        }

        // Concurrency guard against a live delete: between snapshotting the
        // keep-set above and deleting here, a concurrent writer may have
        // published a NEW `CURRENT` version whose active set (segments, routing
        // pages, tombstone, quantizer, and especially freshly appended WAL
        // objects) references an object this snapshot deemed obsolete. With a
        // positive `min_age` the per-object age check already spares such a
        // just-created object, but at `min_age == 0` nothing does — so re-load
        // the latest committed keep-set and drop any candidate it now protects.
        // This makes GC safe to run concurrently with writers at ANY `min_age`:
        // it never deletes an object a currently-committed manifest depends on.
        let (live_now, latest_manifest_created_at) = self.reload_live_keep_set()?;
        let candidates: Vec<GarbageCollectionCandidate> = candidates
            .into_iter()
            .filter(|candidate| {
                // Not referenced by the freshly re-loaded latest manifest, AND not
                // newer than that manifest. A content-addressed object a concurrent
                // writer PUT just before its CAS-publish (a tombstone, quantizer,
                // segment, or sidecar) exists on the store before the manifest that
                // references it is committed; such an object is strictly newer than
                // the latest committed manifest, so fencing on the manifest's own
                // commit time keeps GC from deleting an object whose referencing
                // publish is still in flight — closing the write-then-commit race
                // for every object kind, at any `min_age`.
                !live_now.contains(&candidate.path)
                    && candidate.last_modified <= latest_manifest_created_at
            })
            .collect();
        let bytes_reclaimable = candidates.iter().map(|object| object.size).sum::<u64>();
        let candidate_paths = candidates
            .iter()
            .map(|object| object.path.clone())
            .collect::<Vec<_>>();

        let mut objects_deleted = 0_usize;
        let mut routing_objects_deleted = 0_usize;
        let mut tables_deleted = 0_usize;
        let mut bytes_reclaimed = 0_u64;
        for object in &candidates {
            if self.storage.delete_object(&object.path)? {
                objects_deleted += 1;
                match object.kind {
                    GarbageCollectionObjectKind::SegmentOrGraph => {}
                    GarbageCollectionObjectKind::Routing => routing_objects_deleted += 1,
                    GarbageCollectionObjectKind::Table => tables_deleted += 1,
                }
                bytes_reclaimed += object.size;
            }
        }

        Ok(GarbageCollectionReport {
            dry_run: false,
            objects_scanned,
            objects_deleted,
            routing_objects_deleted,
            tables_deleted,
            routing_page_indexes_read: active_paths.routing_page_indexes_read,
            routing_pages_read: active_paths.routing_pages_read,
            bytes_read: active_paths.bytes_read,
            bytes_reclaimable,
            bytes_reclaimed,
            object_cache_hits: active_paths.object_cache_hits,
            object_cache_misses: active_paths.object_cache_misses,
            candidates: candidate_paths,
        })
    }

    /// Versions before `CURRENT` whose supersession is younger than `min_age`.
    ///
    /// A published version becomes obsolete when its successor version is created. Until
    /// that successor's manifest table is at least `min_age` old, concurrent readers that
    /// pinned the older version may still depend on every object it references, so the
    /// whole version remains part of the live set. Versions staged after `CURRENT` (crash
    /// orphans) are never readable and stay covered by the per-object age check alone.
    fn retained_manifest_versions(
        &self,
        min_age: Duration,
        now: DateTime<Utc>,
    ) -> Result<Vec<u64>> {
        let current_version = self.manifest.version;
        let mut manifest_tables = Vec::new();
        self.storage.for_each_object("manifests", |object| {
            if let Some(version) = manifest_table_version_from_path(&object.path) {
                manifest_tables.push((version, object.last_modified));
            }
            Ok(())
        })?;
        manifest_tables.sort_by_key(|(version, _)| *version);

        let mut retained = Vec::new();
        for (index, (version, _)) in manifest_tables.iter().enumerate() {
            if *version >= current_version {
                continue;
            }
            // The earliest surviving later version bounds when this version became
            // obsolete; missing intermediates only make the bound more conservative.
            let recently_superseded =
                manifest_tables
                    .get(index + 1)
                    .is_some_and(|(_, superseded_at)| {
                        !timestamp_is_at_least_min_age(*superseded_at, min_age, now)
                    });
            if recently_superseded {
                retained.push(*version);
            }
        }
        Ok(retained)
    }

    /// Walk a retained (non-current) manifest exactly as a reader pinned to it would.
    fn object_paths_for_retained_manifest(
        &mut self,
        manifest: Manifest,
    ) -> Result<ActiveGcObjectPathsRead> {
        let current = std::mem::replace(&mut self.manifest, manifest);
        let result = self.active_segment_object_paths();
        self.manifest = current;
        result
    }

    /// Re-load the LATEST committed `CURRENT` manifest and return the set of
    /// object paths it references (segments, graphs, sidecars, routing pages,
    /// tombstone, quantizer, and un-flushed WAL objects) plus that manifest's own
    /// commit time. Used as a delete-time re-validation so GC never deletes an
    /// object a concurrent writer just made live by publishing a newer version
    /// after the keep-set was snapshotted — the race that otherwise loses a
    /// freshly appended WAL object at `min_age == 0`. Any error re-reading
    /// `CURRENT` (a concurrent publish mid-swap, a moved metadata table) is
    /// propagated so GC aborts the delete pass rather than delete blindly; GC is
    /// idempotent and re-runnable, so aborting loses nothing.
    fn reload_live_keep_set(&mut self) -> Result<(HashSet<String>, DateTime<Utc>)> {
        let (_, _, latest) = self.load_latest_own_manifest()?;
        let latest_created_at = latest.created_at;
        let previous = std::mem::replace(&mut self.manifest, latest);
        let result = self.active_segment_object_paths();
        self.manifest = previous;
        Ok((result?.paths, latest_created_at))
    }

    fn active_segment_object_paths(&self) -> Result<ActiveGcObjectPathsRead> {
        let mut paths = HashSet::new();
        let mut read = ActiveGcObjectPathsRead::default();
        paths.insert(self.manifest.file_name());
        paths.insert(self.manifest.routing_file_name());
        paths.insert(self.manifest.pivots_file_name());
        if let Some(tombstone) = &self.manifest.tombstone {
            paths.insert(tombstone.path.clone());
        }
        for tombstone in &self.manifest.tombstone_frontier {
            paths.insert(tombstone.path.clone());
        }
        for tombstone in &self.manifest.tombstone_pages {
            paths.insert(tombstone.path.clone());
        }
        // The persisted coarse-quantizer object this manifest references is live
        // and must be retained; a superseded one is reclaimed once no active or
        // retained manifest references it.
        if let Some(quantizer_ref) = &self.manifest.quantizer_ref {
            paths.insert(quantizer_ref.path.clone());
        }
        if let Some(global_pq_ref) = &self.manifest.global_pq_ref {
            paths.insert(global_pq_ref.path.clone());
            let descriptor_read = self.storage.read_bytes_with_cache_status_and_checksum(
                &global_pq_ref.path,
                &global_pq_ref.checksum,
            )?;
            let descriptor = GlobalPqDescriptor::decode(&descriptor_read.bytes)?;
            read.bytes_read = read
                .bytes_read
                .saturating_add(descriptor_read.bytes.len() as u64);
            if descriptor_read.cache_hit {
                read.object_cache_hits += 1;
            } else {
                read.object_cache_misses += 1;
            }
            for chunk in descriptor.chunks() {
                paths.insert(chunk.path.clone());
                paths.insert(chunk.path.clone());
                if let Some(graph) = &chunk.graph {
                    paths.insert(graph.path.clone());
                }
            }
        }
        for root_ref in &self.manifest.lexical_roots {
            paths.insert(root_ref.path.clone());
            let root_read = self
                .storage
                .read_bytes_with_cache_status_and_checksum(&root_ref.path, &root_ref.checksum)?;
            let root = lexical_root_from_parquet(&root_read.bytes)?;
            read.bytes_read = read.bytes_read.saturating_add(root_read.bytes.len() as u64);
            for page in root.pages {
                paths.insert(page.path);
            }
        }
        if let Some(delta) = &self.manifest.bm25_stats_delta {
            for page in &delta.pages {
                paths.insert(page.path.clone());
            }
        }
        for delta in &self.manifest.bm25_stats_delta_frontier {
            for page in &delta.pages {
                paths.insert(page.path.clone());
            }
        }
        paths.extend(
            self.cell_wal_store()?
                .active_object_paths(&self.manifest.logical_cells)?,
        );

        for routing_level in 0..=self.manifest.routing_max_level {
            let index_path =
                Manifest::routing_layer_page_index_file_name(self.manifest.version, routing_level);
            paths.insert(index_path);
        }

        let top_read = self.storage.read_routing_layer_page_index_with_status(
            self.manifest.version,
            self.manifest.routing_max_level,
        )?;
        read.bytes_read += top_read.bytes_read;
        read.routing_page_indexes_read += top_read.page_indexes_read;
        read.object_cache_hits += top_read.object_cache_hits;
        read.object_cache_misses += top_read.object_cache_misses;

        let mut current_page_refs = top_read.page_refs;
        let l0_page_refs = loop {
            for page_ref in &current_page_refs {
                paths.insert(page_ref.path.clone());
            }
            let Some(first_page_ref) = current_page_refs.first() else {
                break Vec::new();
            };
            let routing_level = first_page_ref.routing_level;
            if current_page_refs
                .iter()
                .any(|page_ref| page_ref.routing_level != routing_level)
            {
                return Err(BorsukError::InvalidStorage(
                    "routing GC walk found mixed routing levels".to_string(),
                ));
            }
            if routing_level == 0 {
                break current_page_refs;
            }

            let child_read =
                self.routing_child_page_refs_read_from_parent_refs(&current_page_refs)?;
            read.bytes_read += child_read.bytes_read;
            read.routing_pages_read += child_read.routing_pages_read;
            read.object_cache_hits += child_read.object_cache_hits;
            read.object_cache_misses += child_read.object_cache_misses;
            current_page_refs = child_read.page_refs;
        };

        let active_summaries = if !self.manifest.segments.is_empty() {
            RoutingSummariesRead {
                summaries: self.manifest.segments.clone(),
                ..Default::default()
            }
        } else if l0_page_refs.is_empty() {
            RoutingSummariesRead::default()
        } else {
            self.routing_summaries_read_from_page_refs(&l0_page_refs)?
        };
        read.bytes_read += active_summaries.bytes_read;
        read.routing_page_indexes_read += active_summaries.routing_page_indexes_read;
        read.routing_pages_read += active_summaries.routing_pages_read;
        read.object_cache_hits += active_summaries.object_cache_hits;
        read.object_cache_misses += active_summaries.object_cache_misses;
        for summary in &active_summaries.summaries {
            paths.insert(summary.path.clone());
            paths.insert(summary.graph_path.clone());
            // The filter-index sidecar is content-addressed by the segment
            // checksum, so its path is derivable -- retain it for the segment.
            paths.insert(filter_index_relative_path(&summary.checksum));
            // The dense-vector sidecar is content-addressed by the segment
            // checksum and written for every non-empty-dimension segment; it
            // must be retained or projected rerank loses its range-read source.
            if summary.dimensions > 0 {
                paths.insert(vector_sidecar_relative_path(&summary.checksum));
            }
            for (name, spec) in &self.manifest.config.named_vectors {
                if spec.kind == VectorKind::LateInteraction {
                    paths.insert(late_interaction_sidecar_relative_path(
                        name,
                        &summary.checksum,
                    ));
                }
            }
            for shard in &summary.lexical_shards {
                paths.insert(shard.path.clone());
                let shard_read = self
                    .storage
                    .read_bytes_with_cache_status_and_checksum(&shard.path, &shard.checksum)?;
                let shard_root = LexicalRoot {
                    kind: LexicalKind::from_str(&shard.kind)?,
                    dimensions: shard.dimensions,
                    document_count: shard.document_count,
                    total_document_length: shard.total_document_length,
                    pages: Vec::new(),
                };
                let page = lexical_term_page_from_parquet(&shard_root, &shard_read.bytes)?;
                for entry in page.entries {
                    paths.insert(entry.run.postings_path);
                    paths.insert(entry.run.metadata_path);
                }
            }
        }
        read.paths = paths;
        Ok(read)
    }

    fn collect_gc_candidates(
        &self,
        relative_prefix: &str,
        path_filter: impl Fn(&str) -> bool + Sync,
        kind: GarbageCollectionObjectKind,
        scan: &mut GarbageCollectionCandidateScan<'_>,
    ) -> Result<()> {
        self.storage.for_each_object(relative_prefix, |object| {
            if !path_filter(&object.path) {
                return Ok(());
            }
            *scan.objects_scanned += 1;
            if !scan.active_paths.contains(&object.path)
                && object_is_at_least_min_age(&object, scan.min_age, scan.now)
            {
                scan.candidates.push(GarbageCollectionCandidate {
                    path: object.path,
                    size: object.size,
                    kind,
                    last_modified: object.last_modified,
                });
            }
            Ok(())
        })
    }

    fn active_segment_summaries(&self) -> Result<Vec<SegmentSummary>> {
        if let Some(summaries) = self.resident_routing_summaries() {
            return Ok(summaries.as_ref().clone());
        }
        if !self.manifest.segments.is_empty() {
            return Ok(self.manifest.segments.clone());
        }

        let page_refs = self
            .routing_leaf_page_refs_for_metadata_scan_with_report()?
            .page_refs;
        if page_refs.is_empty() {
            return Ok(Vec::new());
        }

        self.routing_summaries_from_page_refs(&page_refs)
    }

    /// The routing centroids (in the metric's routing geometry) for a set of
    /// cell summaries. Cosine/angular cells store the mean of unit-normalized
    /// vectors; unit normalizing the centroid makes squared-Euclidean rank
    /// identically to cosine distance, matching `segment_routing_rank_distance`.
    fn routing_centroids_for_summaries(&self, summaries: &[SegmentSummary]) -> Vec<Vec<f32>> {
        let normalize = self
            .manifest
            .config
            .metric
            .uses_normalized_euclidean_geometry();
        summaries
            .iter()
            .map(|summary| {
                if normalize {
                    crate::metric::unit_l2_normalized(&summary.centroid)
                } else {
                    summary.centroid.clone()
                }
            })
            .collect()
    }

    /// Build the HNSW coarse quantizer over a set of cell summaries, or `None`
    /// when there are too few cells to bother navigating.
    fn build_coarse_quantizer_for_summaries(
        &self,
        summaries: &[SegmentSummary],
    ) -> Option<CentroidHnsw> {
        if summaries.len() < COARSE_QUANTIZER_MIN_CELLS {
            return None;
        }
        let centroids = self.routing_centroids_for_summaries(summaries);
        CentroidHnsw::build(&centroids)
    }

    /// The HNSW coarse quantizer over cell centroids for the active manifest,
    /// built lazily and cached until the version changes.
    ///
    /// On a WARMED/resident index the graph is built in memory from the resident
    /// routing summaries. On a COLD/paged index (no resident summaries) it is
    /// LOADED with a single object read from the persisted quantizer object the
    /// manifest references — so cold approximate search routes through the same
    /// IVF probe list without pulling every centroid resident. Returns `None`
    /// when there are too few cells, when neither a resident snapshot nor a
    /// persisted object is available, or when the persisted object is corrupt
    /// (in which case the caller falls back to the routing tree).
    fn coarse_quantizer(&self) -> Result<Option<ResolvedCoarseQuantizer>> {
        {
            let cache = self
                .coarse_quantizer
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if let Some((version, hnsw, summaries)) = cache.as_ref()
                && *version == self.manifest.version
            {
                return Ok(Some((Arc::clone(hnsw), Arc::clone(summaries))));
            }
        }

        // Warm/resident path: build the graph from the resident summaries.
        if let Some(summaries) = self.resident_routing_summaries() {
            let Some(hnsw) = self.build_coarse_quantizer_for_summaries(&summaries) else {
                return Ok(None);
            };
            let hnsw = Arc::new(hnsw);
            let mut cache = self
                .coarse_quantizer
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            *cache = Some((
                self.manifest.version,
                Arc::clone(&hnsw),
                Arc::clone(&summaries),
            ));
            return Ok(Some((hnsw, summaries)));
        }

        // Cold/paged path: load the persisted quantizer object (one read, cached).
        self.load_persisted_quantizer()
    }

    /// Load the coarse quantizer the active manifest references from storage with
    /// a single read, caching it keyed by the object checksum. Returns `None`
    /// when the manifest references no persisted quantizer (older manifest,
    /// disabled, or too few cells at build time). A corrupt object is surfaced as
    /// an error by `decode`, which the search path treats as "no quantizer" and
    /// falls back to the routing tree.
    fn load_persisted_quantizer(&self) -> Result<Option<ResolvedCoarseQuantizer>> {
        let Some(quantizer_ref) = self.manifest.quantizer_ref.clone() else {
            return Ok(None);
        };
        {
            let cache = self
                .persisted_quantizer
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if let Some((checksum, hnsw, summaries)) = cache.as_ref()
                && *checksum == quantizer_ref.checksum
            {
                return Ok(Some((Arc::clone(hnsw), Arc::clone(summaries))));
            }
        }

        let read = self.storage.read_bytes_with_cache_status_and_checksum(
            &quantizer_ref.path,
            &quantizer_ref.checksum,
        )?;
        let persisted = PersistedQuantizer::decode(&read.bytes)?;
        let hnsw = Arc::new(persisted.graph);
        let summaries = Arc::new(persisted.summaries);
        let mut cache = self
            .persisted_quantizer
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        *cache = Some((
            quantizer_ref.checksum,
            Arc::clone(&hnsw),
            Arc::clone(&summaries),
        ));
        Ok(Some((hnsw, summaries)))
    }

    fn load_resident_global_pq(&self) -> Result<Option<LoadedResidentGlobalPq>> {
        let Some(global_ref) = self.manifest.global_pq_ref.clone() else {
            return Ok(None);
        };
        {
            let cache = self
                .resident_global_pq
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if let Some((version, checksum, index, summaries, delta_summaries)) = cache.as_ref()
                && *version == self.manifest.version
                && *checksum == global_ref.checksum
            {
                return Ok(Some((
                    Arc::clone(index),
                    Arc::clone(summaries),
                    Arc::clone(delta_summaries),
                )));
            }
        }

        let active_summaries = self.active_segment_summaries()?;
        let active_by_checksum = active_summaries
            .iter()
            .map(|summary| (summary.checksum.as_str(), summary))
            .collect::<HashMap<_, _>>();
        let mut base_summaries = Vec::with_capacity(global_ref.segments.len());
        for checksum in &global_ref.segments {
            let Some(summary) = active_by_checksum.get(checksum.as_str()) else {
                // A compaction replaced a segment covered by this artifact.
                // Its persisted row ordinal is no longer resolvable, so the
                // artifact is invalid for this snapshot.
                return Ok(None);
            };
            base_summaries.push((*summary).clone());
        }
        let covered = global_ref.segments.iter().collect::<HashSet<_>>();
        let delta_summaries = active_summaries
            .into_iter()
            .filter(|summary| !covered.contains(&summary.checksum))
            .collect::<Vec<_>>();
        let summaries = Arc::new(base_summaries);
        let delta_summaries = Arc::new(delta_summaries);
        let read = self
            .storage
            .read_bytes_with_cache_status_and_checksum(&global_ref.path, &global_ref.checksum)?;
        let descriptor = GlobalPqDescriptor::decode(&read.bytes)?;
        if descriptor.vectors() != global_ref.vectors
            || descriptor.subspaces() != global_ref.subspaces
            || descriptor.vector_element_type() != self.manifest.build_config.vector_element_type
        {
            return Err(BorsukError::InvalidStorage(
                "resident global PQ reference does not match its descriptor".to_string(),
            ));
        }
        let index = Arc::new(ResidentGlobalPq::load(descriptor)?);
        if index.len() != global_ref.vectors
            || index.code_bytes_per_vector() != global_ref.subspaces
        {
            return Err(BorsukError::InvalidStorage(
                "resident global PQ reference does not match its artifact".to_string(),
            ));
        }
        let mut cache = self
            .resident_global_pq
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        *cache = Some((
            self.manifest.version,
            global_ref.checksum,
            Arc::clone(&index),
            Arc::clone(&summaries),
            Arc::clone(&delta_summaries),
        ));
        Ok(Some((index, summaries, delta_summaries)))
    }

    fn cached_global_cell_graph(
        &self,
        chunk: &GlobalPqChunkRef,
    ) -> Result<Option<(Arc<GlobalCellGraph>, bool)>> {
        let Some(reference) = chunk.graph.as_ref() else {
            return Ok(None);
        };
        if !self.storage.has_cached_object(&reference.path) {
            return Ok(None);
        }
        if let Some(graph) = self.decoded_global_cell_graphs.get(&reference.checksum) {
            graph.validate_reference(chunk)?;
            return Ok(Some((graph, true)));
        }
        let loaded = self
            .inflight_global_cell_graph_reads
            .load(&reference.checksum, || {
                let Some(bytes) = self
                    .storage
                    .read_cached_bytes_with_checksum(&reference.path, &reference.checksum)?
                else {
                    return Err(BorsukError::InvalidStorage(
                        "global cell graph left the local cache before decode".to_string(),
                    ));
                };
                if bytes.len() != reference.size_bytes {
                    return Err(BorsukError::InvalidStorage(
                        "cached global cell graph size does not match its reference".to_string(),
                    ));
                }
                let graph = GlobalCellGraph::decode(&bytes)?;
                graph.validate_reference(chunk)?;
                Ok((graph, 0))
            });
        let Ok((graph, _, shared_inflight)) = loaded else {
            // A cache race or corrupt local graph must not fail the query or
            // fetch the graph from storage. The caller scans this cell.
            return Ok(None);
        };
        self.decoded_global_cell_graphs.insert(
            reference.checksum.clone(),
            Arc::clone(&graph),
            u64::try_from(graph.resident_bytes()).unwrap_or(u64::MAX),
        );
        Ok(Some((graph, shared_inflight)))
    }

    fn search_resident_global_pq(
        &self,
        query: &[f32],
        options: &SearchOptions,
        include_vectors: bool,
        started: Instant,
        requests_before: &RequestCounts,
    ) -> Result<Option<SearchExecution>> {
        let expected_leaf_mode = self.manifest.build_config.global_scan_codec.leaf_mode();
        let eligible = matches!(options.mode, SearchMode::Approx { .. })
            && options.mode.leaf_mode() == expected_leaf_mode
            && !options.guaranteed_recall
            && !options.disable_coarse_quantizer
            && options.filter.is_none()
            && !options.include_metadata;
        if !eligible {
            return Ok(None);
        }
        let Some(global_ref) = self.manifest.global_pq_ref.as_ref() else {
            return Ok(None);
        };
        let Some((index, summaries, delta_summaries)) = self.load_resident_global_pq()? else {
            return Ok(None);
        };

        // The cell-local candidate knob becomes the whole-index rerank budget
        // on the resident global path, where there is no per-cell scan. Leaving
        // it unset uses the persisted production default; an explicit value is
        // useful for recall/latency curves and dataset-specific tuning.
        let requested_candidates = match &options.mode {
            SearchMode::Approx {
                max_candidates_per_segment: Some(value),
                ..
            } => *value,
            _ => global_ref.candidates,
        };
        let pq_query = if self
            .manifest
            .config
            .metric
            .uses_normalized_euclidean_geometry()
        {
            crate::metric::unit_l2_normalized(query)
        } else {
            query.to_vec()
        };
        let requested_probes = match &options.mode {
            SearchMode::Approx {
                max_segments: Some(value),
                ..
            } => *value,
            _ => global_ref.probes,
        };
        let probe_count = requested_probes.max(1).min(index.cell_count());
        let selected_cells = index.nearest_cells(&pq_query, probe_count)?;
        let selected_chunks = index.chunks_for_cells(&selected_cells);
        let records_considered = selected_chunks
            .iter()
            .map(|chunk| chunk.rows)
            .sum::<usize>();
        let candidate_limit = requested_candidates.max(options.k).min(records_considered);
        let mut candidate_pages =
            Vec::with_capacity(selected_chunks.len().div_ceil(DEFAULT_GLOBAL_PQ_CODE_READS));
        let mut bytes_read = 0_u64;
        let mut wave_start = 0_usize;
        let mut graph_chunks_used = 0_usize;
        let mut scan_chunks_used = 0_usize;
        let mut graph_candidates_added = 0_usize;
        let mut decoded_cache_hits = 0_usize;
        let mut decoded_cache_bytes_read = 0_u64;
        let use_cached_graphs =
            !matches!(options.cache_execution, crate::CacheExecutionPolicy::Scan);
        while wave_start < selected_chunks.len() {
            let wave_end = global_pq_code_read_wave_end(
                &selected_chunks,
                wave_start,
                DEFAULT_GLOBAL_PQ_CODE_READS,
                DEFAULT_GLOBAL_PQ_CODE_WAVE_BYTES,
            );
            let page = &selected_chunks[wave_start..wave_end];
            let mut scan_page = Vec::with_capacity(page.len());
            for chunk in page {
                let graph = if use_cached_graphs {
                    self.cached_global_cell_graph(chunk)?
                } else {
                    None
                };
                if let Some((graph, decoded_hit)) = graph {
                    if decoded_hit {
                        decoded_cache_hits = decoded_cache_hits.saturating_add(1);
                        decoded_cache_bytes_read = decoded_cache_bytes_read.saturating_add(
                            u64::try_from(graph.resident_bytes()).unwrap_or(u64::MAX),
                        );
                    }
                    let candidates = index.candidates_in_graph(
                        &pq_query,
                        &graph,
                        candidate_limit,
                        candidate_limit.max(64),
                    )?;
                    graph_chunks_used = graph_chunks_used.saturating_add(1);
                    graph_candidates_added =
                        graph_candidates_added.saturating_add(candidates.len());
                    candidate_pages.push(candidates);
                } else {
                    scan_chunks_used = scan_chunks_used.saturating_add(1);
                    scan_page.push(chunk.clone());
                }
            }
            if scan_page.is_empty() {
                wave_start = wave_end;
                continue;
            }
            let code_groups = global_pq_code_read_groups(
                &scan_page,
                DEFAULT_GLOBAL_PQ_CODE_COALESCE_GAP_BYTES,
                DEFAULT_GLOBAL_PQ_CODE_REQUEST_WEIGHT_BYTES,
            )?;
            let code_reads = bounded_io_map_with_gate(
                &code_groups,
                DEFAULT_GLOBAL_PQ_CODE_READS.min(options.prefetch_depth.max(1)),
                self.decode_admission.as_deref(),
                |(path, chunks)| {
                    let start = chunks
                        .iter()
                        .map(|chunk| chunk.offset_bytes)
                        .min()
                        .unwrap_or(0);
                    let end = chunks
                        .iter()
                        .map(|chunk| chunk.offset_bytes.saturating_add(chunk.size_bytes))
                        .max()
                        .unwrap_or(start);
                    let bundled = self.storage.read_range(path, start as u64..end as u64)?;
                    let mut loaded = Vec::with_capacity(chunks.len());
                    for chunk in chunks {
                        let local_start =
                            chunk.offset_bytes.checked_sub(start).ok_or_else(|| {
                                BorsukError::InvalidStorage(
                                    "global PQ bundled code offset underflows".to_string(),
                                )
                            })?;
                        let local_end =
                            local_start.checked_add(chunk.size_bytes).ok_or_else(|| {
                                BorsukError::InvalidStorage(
                                    "global PQ bundled code range overflows".to_string(),
                                )
                            })?;
                        let bytes = bundled.get(local_start..local_end).ok_or_else(|| {
                            BorsukError::InvalidStorage(
                                "global PQ bundled code range is truncated".to_string(),
                            )
                        })?;
                        let actual = blake3::hash(bytes).to_hex().to_string();
                        if actual != chunk.checksum {
                            return Err(BorsukError::ChecksumMismatch {
                                path: path.clone(),
                                expected: chunk.checksum.clone(),
                                actual,
                            });
                        }
                        loaded.push((chunk.clone(), bytes::Bytes::copy_from_slice(bytes)));
                    }
                    Ok::<_, BorsukError>((loaded, bundled.len() as u64))
                },
            );
            let mut loaded = Vec::with_capacity(code_reads.len());
            for result in code_reads {
                let (mut chunks, count) = result?;
                loaded.append(&mut chunks);
                bytes_read = bytes_read.saturating_add(count);
            }
            candidate_pages.push(index.candidates_in_chunks(
                &pq_query,
                candidate_limit,
                &loaded,
                crate::configured_cpu_threads(),
            )?);
            wave_start = wave_end;
        }
        let nodes = crate::global_pq_sidecar::merge_candidates(candidate_pages, candidate_limit);
        let chunks_by_start = selected_chunks
            .iter()
            .map(|chunk| (chunk.row_start, chunk))
            .collect::<HashMap<_, _>>();
        let mut candidate_rows = HashMap::with_capacity(nodes.len());
        let mut grouped = BTreeMap::<usize, Vec<(usize, usize)>>::new();
        for candidate in nodes {
            grouped
                .entry(candidate.chunk_row_start)
                .or_default()
                .push((candidate.node, candidate.local_row));
            candidate_rows.insert(candidate.node, candidate.row);
        }
        let mut bundled_groups =
            BTreeMap::<String, Vec<(GlobalPqChunkRef, Vec<(usize, usize)>)>>::new();
        for (row_start, entries) in grouped {
            let chunk = chunks_by_start.get(&row_start).ok_or_else(|| {
                BorsukError::InvalidStorage("resident global PQ exact chunk is missing".to_string())
            })?;
            bundled_groups
                .entry(chunk.path.clone())
                .or_default()
                .push(((*chunk).clone(), entries));
        }
        let groups = bundled_groups.into_iter().collect::<Vec<_>>();
        let fetched = bounded_io_map_with_gate(
            &groups,
            DEFAULT_GLOBAL_PQ_RERANK_READS,
            Some(&self.global_pq_rerank_admission),
            |(path, chunks)| self.global_exact_vectors_bundled(path, chunks),
        );
        let mut vectors_by_node = HashMap::with_capacity(candidate_rows.len());
        for result in fetched {
            let (vectors, bytes) = result?;
            vectors_by_node.extend(vectors);
            bytes_read = bytes_read.saturating_add(bytes);
        }

        let metric = &self.manifest.config.metric;
        let mut scored_vectors = Vec::with_capacity(candidate_rows.len());
        for (node, row) in candidate_rows {
            let vector = vectors_by_node.remove(&node).ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "resident global PQ candidate vector is missing".to_string(),
                )
            })?;
            let distance = metric.distance_unchecked(query, &vector)?;
            scored_vectors.push((distance, node, row, vector));
        }
        scored_vectors.sort_by(|left, right| {
            left.0
                .total_cmp(&right.0)
                .then_with(|| left.1.cmp(&right.1))
        });
        let records_scored = scored_vectors.len();
        let materialize = options.k.min(scored_vectors.len());
        let boundary = materialize
            .checked_sub(1)
            .map(|index| scored_vectors[index].0);
        let materialize = boundary.map_or(0, |distance| {
            scored_vectors.partition_point(|entry| entry.0.total_cmp(&distance).is_le())
        });
        scored_vectors.truncate(materialize);

        let mut physical_groups = BTreeMap::<u32, Vec<(usize, usize)>>::new();
        for (_, node, row, _) in &scored_vectors {
            physical_groups
                .entry(row.segment_index)
                .or_default()
                .push((*node, row.row_index as usize));
        }
        let physical_groups = physical_groups.into_iter().collect::<Vec<_>>();
        let fetched_records = bounded_io_map_with_gate(
            &physical_groups,
            DEFAULT_GLOBAL_PQ_RERANK_READS,
            Some(&self.global_pq_rerank_admission),
            |(segment, entries)| {
                let summary = summaries.get(*segment as usize).ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "resident global PQ segment ordinal is invalid".to_string(),
                    )
                })?;
                let rows = entries.iter().map(|(_, row)| *row).collect::<Vec<_>>();
                let (records, bytes) = self.segment_exact_rows_ranged(summary, &rows)?;
                let records = entries
                    .iter()
                    .map(|(node, row)| {
                        records
                            .get(row)
                            .cloned()
                            .map(|record| (*node, record))
                            .ok_or_else(|| {
                                BorsukError::InvalidStorage(
                                    "resident global PQ final record is missing".to_string(),
                                )
                            })
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok::<_, BorsukError>((records, bytes))
            },
        );
        let mut records_by_node = HashMap::with_capacity(scored_vectors.len());
        for result in fetched_records {
            let (records, bytes) = result?;
            records_by_node.extend(records);
            bytes_read = bytes_read.saturating_add(bytes);
        }
        let mut scored = Vec::with_capacity(scored_vectors.len());
        for (distance, node, _row, vector) in scored_vectors {
            let exact = records_by_node.remove(&node).ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "resident global PQ final record is missing".to_string(),
                )
            })?;
            if self
                .min_visible_generation(exact.id.as_bytes())?
                .is_some_and(|minimum| exact.generation < minimum)
            {
                continue;
            }
            scored.push((distance, exact.id, vector));
        }
        scored.sort_by(|left, right| {
            left.0
                .total_cmp(&right.0)
                .then_with(|| left.1.cmp(&right.1))
        });
        let mut seen = HashSet::new();
        scored.retain(|(_, id, _)| seen.insert(id.clone()));
        scored.truncate(options.k);
        let hits = scored
            .iter()
            .map(|(distance, id, _)| SearchHit {
                id: id.clone(),
                distance: *distance,
                metadata: None,
            })
            .collect::<Vec<_>>();
        let vectors = if include_vectors {
            scored.into_iter().map(|(_, _, vector)| vector).collect()
        } else {
            Vec::new()
        };
        let segments_total = summaries.len();
        let segments_searched = selected_chunks.len();
        let execution_engine = match (graph_chunks_used, scan_chunks_used) {
            (0, _) => expected_leaf_mode.to_string(),
            (_, 0) => "global-cell-graph".to_string(),
            _ => format!("mixed-global-cell-graph+{expected_leaf_mode}"),
        };
        let mut execution = SearchExecution {
            report: SearchReport {
                hits,
                leaf_mode: execution_engine,
                termination_reason: SearchTerminationReason::Complete,
                recall_guarantee: RecallGuarantee::Degraded,
                segments_total,
                segments_searched,
                segments_skipped: segments_total.saturating_sub(segments_searched),
                routing_page_indexes_read: 0,
                routing_pages_read: 0,
                bytes_read,
                prefetched_bytes_unused: 0,
                graph_bytes_read: 0,
                decoded_cache_hits,
                decoded_cache_bytes_read,
                object_cache_hits: 0,
                object_cache_misses: 0,
                disk_cache_bytes_read: 0,
                backing_bytes_read: 0,
                disk_cache_reads: 0,
                backing_reads: 0,
                cache_repairs: 0,
                records_considered,
                records_scored,
                graph_candidates_added,
                global_graph_chunks_searched: graph_chunks_used,
                global_scan_chunks_searched: scan_chunks_used,
                resident_bytes_estimate: self.manifest.resident_bytes_estimate(),
                elapsed_ms: started.elapsed().as_millis() as u64,
                requests: self.storage.request_counts().delta(requests_before),
                rows_evaluated: records_considered,
                rows_passed_filter: records_considered,
                segments_pruned_by_filter: 0,
                wal_cells_examined: 0,
                wal_lanes_examined: 0,
                wal_runs_examined: 0,
                wal_records_examined: 0,
                wal_snapshot_retries: 0,
            },
            vectors,
        };
        self.merge_materialized_global_delta(
            query,
            options,
            include_vectors,
            &delta_summaries,
            started,
            requests_before,
            &mut execution,
        )?;
        Ok(Some(execution))
    }

    #[allow(clippy::too_many_arguments)]
    fn merge_materialized_global_delta(
        &self,
        query: &[f32],
        options: &SearchOptions,
        include_vectors: bool,
        delta_summaries: &[SegmentSummary],
        started: Instant,
        requests_before: &RequestCounts,
        execution: &mut SearchExecution,
    ) -> Result<()> {
        if delta_summaries.is_empty() {
            return Ok(());
        }

        // Build a query-local view over only the manifest-selected delta
        // segments. It shares immutable object caches, decode gates, and the
        // caller's storage read scope with the base handle, but must not
        // recursively enter the global artifact or acquire the query admission
        // permit a second time.
        let mut delta = self.clone();
        delta.named.clear();
        delta.manifest.global_pq_ref = None;
        delta.manifest.quantizer_ref = None;
        delta.manifest.segments = delta_summaries.to_vec();
        delta.manifest.rebuild_pivots();
        delta.resident_routing_summaries = Arc::new(Mutex::new(Some((
            delta.manifest.version,
            Arc::new(delta_summaries.to_vec()),
        ))));
        delta.coarse_quantizer = Arc::new(Mutex::new(None));
        delta.persisted_quantizer = Arc::new(Mutex::new(None));
        delta.resident_global_pq = Arc::new(Mutex::new(None));
        delta.admission = None;

        let mut delta_options = options.clone();
        delta_options.disable_coarse_quantizer = true;
        let delta_execution = delta.search_execution_with_routing_cache(
            query,
            delta_options,
            include_vectors,
            None,
        )?;
        merge_search_execution_hits(execution, delta_execution, options.k, include_vectors);

        execution.report.leaf_mode = format!("{}+materialized-delta", execution.report.leaf_mode);
        execution.report.elapsed_ms = started.elapsed().as_millis() as u64;
        execution.report.requests = self.storage.request_counts().delta(requests_before);
        Ok(())
    }

    /// Build and persist the coarse quantizer over `summaries` as a
    /// content-addressed object, returning its manifest reference. `None` when
    /// disabled by config or there are too few cells to build a graph. Called at
    /// compaction time before publishing so a cold/paged query can route with a
    /// single read. The object is small (centroids + adjacency + per-cell
    /// summaries), so writing it is cheap and preserves near-zero-RAM.
    fn persist_coarse_quantizer(
        &self,
        summaries: &[SegmentSummary],
    ) -> Result<Option<QuantizerRef>> {
        if !self.manifest.build_config.persist_coarse_quantizer {
            return Ok(None);
        }
        let Some(graph) = self.build_coarse_quantizer_for_summaries(summaries) else {
            return Ok(None);
        };
        let cells = graph.node_count();
        let persisted = PersistedQuantizer {
            summaries: summaries.to_vec(),
            graph,
        };
        let bytes = persisted.encode()?;
        let checksum = blake3::hash(&bytes).to_hex().to_string();
        let path = quantizer_relative_path(&checksum);
        self.storage.write_bytes_content_addressed(&path, &bytes)?;
        Ok(Some(QuantizerRef {
            path,
            checksum,
            cells,
        }))
    }

    /// Refresh the persisted coarse quantizer to match the ACTIVE cell set, then
    /// republish the manifest metadata if the reference changed.
    ///
    /// Called after a compaction/flush/purge that rebuilt the cell layout. It
    /// reads the full active summary set (`active_segment_summaries`, one routing
    /// tree walk for a paged index — a one-time maintenance cost), builds the
    /// HNSW over those centroids, writes the content-addressed object, and — if
    /// the resulting reference differs from the one the just-published manifest
    /// carries — republishes the manifest metadata (reusing the routing pages,
    /// like a tombstone-only publish) so the reference is durable. Idempotent:
    /// when the reference already matches (or the quantizer is disabled / the
    /// cell count is below threshold and no reference exists), it is a no-op and
    /// publishes nothing.
    ///
    /// This is the single write site for the persisted quantizer; every
    /// segment-changing operation funnels through it, so a cold/paged query never
    /// routes on a stale or missing reference for an index big enough to want one.
    fn refresh_persisted_quantizer(&mut self) -> Result<()> {
        if !self.manifest.build_config.persist_coarse_quantizer {
            // Disabled: ensure no stale reference lingers.
            if self.manifest.quantizer_ref.is_some() {
                self.republish_manifest_metadata_with_quantizer_ref(None)?;
            }
            return Ok(());
        }
        // Gathering the full active summary set (a routing tree walk for a paged
        // index) and building the object are OPTIMIZATIONS, never correctness
        // requirements: a cold query falls back to the routing tree without a
        // reference. So a failure here (e.g. a summary object temporarily
        // unreadable) must NOT fail the compaction that just published; skip the
        // refresh and leave the reference untouched for a later pass to update.
        let summaries = match self.active_segment_summaries() {
            Ok(summaries) => summaries,
            Err(_) => return Ok(()),
        };
        let desired = match self.persist_coarse_quantizer(&summaries) {
            Ok(desired) => desired,
            Err(_) => return Ok(()),
        };
        if desired == self.manifest.quantizer_ref {
            return Ok(());
        }
        self.republish_manifest_metadata_with_quantizer_ref(desired)?;
        Ok(())
    }

    /// Publish a new manifest version identical to the current one except for its
    /// `quantizer_ref`, reusing the existing routing pages (no segment or routing
    /// rewrite). Models the metadata-only republish `publish_tombstone` uses for
    /// paged indexes.
    fn republish_manifest_metadata_with_quantizer_ref(
        &mut self,
        quantizer_ref: Option<QuantizerRef>,
    ) -> Result<()> {
        let previous = self.manifest.clone();
        let mut manifest = self.manifest.next_version();
        manifest.quantizer_ref = quantizer_ref;
        enforce_ram_budget(&manifest, self.runtime_ram_budget_bytes)?;

        // A quantizer-ref-only publish changes no segments or routing pages. When
        // the index has PAGED (segments live in routing pages, `manifest.segments`
        // is empty), rebuilding routing pages from the empty segment list would
        // publish an empty index; re-publish referencing the existing routing
        // pages instead (only the manifest metadata is rewritten).
        if manifest.segments.is_empty() {
            let top_read = self.storage.read_routing_layer_page_index_with_status(
                previous.version,
                previous.routing_max_level,
            )?;
            if !top_read.page_refs.is_empty() {
                self.manifest = self.publish_manifest_with_top_routing_page_refs_with_recovery(
                    manifest,
                    previous.routing_max_level,
                    &top_read.page_refs,
                )?;
                return Ok(());
            }
        }
        self.manifest =
            self.publish_manifest_reusing_routing_pages_with_recovery(manifest, Some(&previous))?;
        Ok(())
    }

    fn persist_resident_global_pq(
        &self,
        summaries: &[SegmentSummary],
    ) -> Result<Option<crate::manifest::GlobalPqRef>> {
        if summaries.is_empty() {
            return Ok(None);
        }
        const PQ_TRAINING_SAMPLE_LIMIT: usize = 4_096;
        let training_sample_limit =
            global_pq_training_sample_limit(self.manifest.config.dimensions);
        let normalize = self
            .manifest
            .config
            .metric
            .uses_normalized_euclidean_geometry();
        let mut training_sample = Vec::with_capacity(training_sample_limit);
        let mut vectors_seen = 0_usize;
        let mut reservoir_state = crate::DEFAULT_TURBOQUANT_SEED;

        // First pass retains only a bounded, deterministic reservoir for fitting.
        // In particular, a 1M x 960-d GIST build no longer holds ~3.8 GiB of
        // dense vectors while constructing its compact serving artifact.
        for summary in summaries {
            let (segment, _, _, _) = self.read_segment(summary)?;
            for record in &segment.records {
                if self.is_suppressed(record)? {
                    continue;
                }
                let vector = if normalize {
                    crate::metric::unit_l2_normalized(&record.vector)
                } else {
                    record.vector.clone()
                };
                vectors_seen = vectors_seen.saturating_add(1);
                if training_sample.len() < training_sample_limit {
                    training_sample.push(vector);
                } else {
                    let replacement = splitmix_index(&mut reservoir_state, vectors_seen);
                    if replacement < training_sample_limit {
                        training_sample[replacement] = vector;
                    }
                }
            }
        }
        if vectors_seen == 0 {
            return Ok(None);
        }
        let dimensions = self.manifest.config.dimensions;
        let subspaces = resident_global_pq_subspaces(
            dimensions,
            vectors_seen,
            self.manifest.build_config.global_pq_code_bytes,
        );
        let quantizer = match self.manifest.build_config.global_scan_codec {
            GlobalScanCodec::Pq | GlobalScanCodec::SrhtPq => {
                let rotation = match self.manifest.build_config.global_scan_codec {
                    GlobalScanCodec::Pq => {
                        crate::rotated_product_quantizer::ProductRotation::Identity
                    }
                    GlobalScanCodec::SrhtPq => {
                        crate::rotated_product_quantizer::ProductRotation::Srht
                    }
                    _ => unreachable!("matched above"),
                };
                GlobalScanQuantizer::from(RotatedProductQuantizer::fit(
                    ProductQuantizerConfig {
                        rotation,
                        seed: crate::DEFAULT_TURBOQUANT_SEED,
                        dimensions,
                        subspaces,
                        centroids: training_sample.len().min(256),
                        sample_limit: training_sample.len().min(PQ_TRAINING_SAMPLE_LIMIT),
                        iterations: 4,
                    },
                    &training_sample,
                )?)
            }
            GlobalScanCodec::FastTurboQuantMse => {
                GlobalScanQuantizer::from(crate::turboquant::FastTurboQuantMseScanQuantizer::new(
                    crate::DEFAULT_TURBOQUANT_SEED,
                    dimensions,
                    self.manifest.build_config.global_turboquant_bits,
                    self.manifest.build_config.global_turboquant_shards,
                )?)
            }
            GlobalScanCodec::FastTurboQuantProd => {
                GlobalScanQuantizer::from(crate::turboquant::FastTurboQuantProdScanQuantizer::new(
                    crate::DEFAULT_TURBOQUANT_SEED,
                    dimensions,
                    self.manifest.build_config.global_turboquant_bits,
                )?)
            }
        };

        // The IVF layer is fitted from actual corpus vectors and every encoded
        // row is assigned independently. Physical segments are only bounded
        // ingest/rerank storage units; they must not define query routing or the
        // same semantic region gets fragmented across ingest checkpoints.
        let coarse_layout = resolved_global_pq_layout(
            &self.manifest.build_config.global_pq_layout,
            &self.manifest.config.metric,
            dimensions,
            vectors_seen,
        );
        let (coarse_subspaces, requested_parent_centroids) = match coarse_layout {
            ResolvedGlobalPqLayout::Product {
                subspaces,
                centroids,
            } => (subspaces, centroids),
            ResolvedGlobalPqLayout::Hierarchical { .. } => (1, 64),
        };
        let coarse_parent_centroids = requested_parent_centroids.min(training_sample.len());
        let coarse_parent = RotatedProductQuantizer::fit(
            ProductQuantizerConfig {
                rotation: crate::rotated_product_quantizer::ProductRotation::Srht,
                seed: crate::DEFAULT_TURBOQUANT_SEED ^ 0xA076_1D64_78BD_642F,
                dimensions,
                subspaces: coarse_subspaces,
                centroids: coarse_parent_centroids,
                sample_limit: training_sample.len(),
                iterations: 8,
            },
            &training_sample,
        )?;
        let coarse_quantizer = match coarse_layout {
            ResolvedGlobalPqLayout::Product { .. } => GlobalCoarseQuantizer::Product(coarse_parent),
            ResolvedGlobalPqLayout::Hierarchical {
                children_per_parent,
            } => GlobalCoarseQuantizer::Hierarchical(HierarchicalCoarseQuantizer::fit(
                coarse_parent,
                &training_sample,
                children_per_parent,
                6,
            )?),
        };
        let coarse_quantizer_state = coarse_quantizer.state();
        let global_code_width = quantizer.code_bytes_per_vector();
        let quantizer_state = quantizer.state();
        drop(training_sample);

        // The external cell spool writes compact PQ-code/location rows to local
        // temporary storage and emits at most one 32 MiB chunk in RAM. This is
        // the bounded-memory equivalent of a global IVF sort and remains stable
        // at 100M vectors.
        let location = LocationEncoding::for_layout(
            summaries.len(),
            summaries
                .iter()
                .map(|summary| summary.object_count)
                .max()
                .unwrap_or(1),
        )?;
        let mut spool = GlobalPqCellSpool::new(
            quantizer,
            coarse_quantizer,
            location,
            DEFAULT_GLOBAL_PQ_CHUNK_BYTES,
            dimensions,
            self.manifest.build_config.vector_element_type,
        )?;
        let mut chunk_refs = Vec::new();
        let mut row_start = 0_usize;
        let mut pending_chunks = Vec::<PendingGlobalPqChunk>::new();
        let mut pending_code_bytes = 0_usize;
        let mut pending_total_bytes = 0_usize;
        let persist_bundle = |pending: &mut Vec<PendingGlobalPqChunk>,
                              chunk_refs: &mut Vec<GlobalPqChunkRef>,
                              storage_bytes: &mut u64|
         -> Result<()> {
            if pending.is_empty() {
                return Ok(());
            }
            let encoded = encode_global_pq_arrow_bundle(
                pending,
                global_code_width,
                location,
                dimensions,
                self.manifest.build_config.vector_element_type,
            )?;
            *storage_bytes = storage_bytes.saturating_add(encoded.bytes.len() as u64);
            let bundle_checksum = blake3::hash(&encoded.bytes).to_hex().to_string();
            let path = format!(
                "global-pq/bundles/{}/bundle-{bundle_checksum}.arrow",
                &bundle_checksum[..2]
            );
            self.storage
                .write_bytes_content_addressed(&path, &encoded.bytes)?;
            for (entry, slice) in pending.iter().zip(&encoded.slices) {
                let code = &encoded.bytes[slice.code_range.clone()];
                let exact = &encoded.bytes[slice.exact_range.clone()];
                let graph = self
                    .manifest
                    .build_config
                    .global_cell_graph
                    .as_ref()
                    .filter(|_| entry.chunk.rows >= 2)
                    .map(|config| {
                        let graph = GlobalCellGraph::build(
                            &GlobalPqChunkRef {
                                path: path.clone(),
                                checksum: blake3::hash(code).to_hex().to_string(),
                                offset_bytes: slice.code_range.start,
                                exact_checksum: blake3::hash(exact)
                                    .to_hex()
                                    .to_string()
                                    .into_boxed_str(),
                                exact_offset_bytes: slice.exact_range.start,
                                exact_size_bytes: exact.len(),
                                cell_index: entry.cell_index,
                                row_start: entry.row_start,
                                rows: entry.chunk.rows,
                                size_bytes: code.len(),
                                graph: None,
                            },
                            code.to_vec(),
                            exact,
                            dimensions,
                            self.manifest.build_config.vector_element_type,
                            global_code_width,
                            location,
                            config.degree,
                            config.construction_ef,
                            normalize,
                        )?;
                        let graph_bytes = graph.encode()?;
                        let checksum = blake3::hash(&graph_bytes).to_hex().to_string();
                        let graph_path = format!(
                            "global-pq/cell-graphs/{}/graph-{checksum}.bin",
                            &checksum[..2]
                        );
                        self.storage
                            .write_bytes_content_addressed(&graph_path, &graph_bytes)?;
                        *storage_bytes = storage_bytes.saturating_add(graph_bytes.len() as u64);
                        Ok::<_, BorsukError>(GlobalCellGraphRef {
                            path: graph_path,
                            checksum,
                            size_bytes: graph_bytes.len(),
                        })
                    })
                    .transpose()?;
                chunk_refs.push(GlobalPqChunkRef {
                    path: path.clone(),
                    checksum: blake3::hash(code).to_hex().to_string(),
                    offset_bytes: slice.code_range.start,
                    exact_checksum: blake3::hash(exact).to_hex().to_string().into_boxed_str(),
                    exact_offset_bytes: slice.exact_range.start,
                    exact_size_bytes: exact.len(),
                    cell_index: entry.cell_index,
                    row_start: entry.row_start,
                    rows: entry.chunk.rows,
                    size_bytes: code.len(),
                    graph,
                });
            }
            pending.clear();
            Ok(())
        };
        let mut storage_bytes = 0_u64;
        for (segment_index, summary) in summaries.iter().enumerate() {
            let segment_index = u32::try_from(segment_index).map_err(|_| {
                BorsukError::InvalidStorage(
                    "resident global PQ has more than u32 segments".to_string(),
                )
            })?;
            let (segment, _, _, _) = self.read_segment(summary)?;
            let active = segment
                .records
                .iter()
                .enumerate()
                .filter_map(|(row_index, record)| match self.is_suppressed(record) {
                    Ok(true) => None,
                    Ok(false) => Some(Ok((
                        row_index,
                        if normalize {
                            crate::metric::unit_l2_normalized(&record.vector)
                        } else {
                            record.vector.clone()
                        },
                    ))),
                    Err(error) => Some(Err(error)),
                })
                .collect::<Result<Vec<_>>>()?;
            let encoded = crate::parallel::install(|| {
                active
                    .par_iter()
                    .map(|(_, vector)| spool.encode_vector(vector))
                    .collect::<Result<Vec<_>>>()
            })?;
            for ((row_index, _vector), (cell, code)) in active.into_iter().zip(encoded) {
                let record = &segment.records[row_index];
                spool.push_encoded(
                    cell,
                    &code,
                    GlobalPqRow {
                        segment_index,
                        row_index: u32::try_from(row_index).map_err(|_| {
                            BorsukError::InvalidStorage(
                                "resident global PQ segment has more than u32 rows".to_string(),
                            )
                        })?,
                    },
                    &record.vector,
                )?;
            }
        }
        let parent_contiguous_bundles = matches!(
            &coarse_quantizer_state,
            crate::global_pq_sidecar::GlobalCoarseQuantizerState::Hierarchical(_)
        );
        let spooled_rows = spool.finish(|cell_index, chunk| {
            let next_code_bytes = pending_code_bytes.saturating_add(chunk.bytes.len());
            let next_total_bytes = pending_total_bytes
                .saturating_add(chunk.bytes.len())
                .saturating_add(chunk.exact_bytes.len());
            if should_flush_global_pq_bundle(
                pending_chunks.last().map(|entry| entry.cell_index),
                cell_index,
                parent_contiguous_bundles,
                next_code_bytes,
                next_total_bytes,
            ) {
                persist_bundle(&mut pending_chunks, &mut chunk_refs, &mut storage_bytes)?;
                pending_code_bytes = 0;
                pending_total_bytes = 0;
            }
            let chunk_row_start = row_start;
            row_start = row_start.checked_add(chunk.rows).ok_or_else(|| {
                BorsukError::InvalidStorage("resident global PQ row count overflows".to_string())
            })?;
            pending_code_bytes = pending_code_bytes.saturating_add(chunk.bytes.len());
            pending_total_bytes = pending_total_bytes
                .saturating_add(chunk.bytes.len())
                .saturating_add(chunk.exact_bytes.len());
            pending_chunks.push(PendingGlobalPqChunk {
                cell_index,
                row_start: chunk_row_start,
                chunk,
            });
            Ok(())
        })?;
        persist_bundle(&mut pending_chunks, &mut chunk_refs, &mut storage_bytes)?;
        if spooled_rows != vectors_seen || row_start != vectors_seen {
            return Err(BorsukError::InvalidStorage(format!(
                "resident global PQ row count changed during build: sampled {vectors_seen}, spooled {spooled_rows}, encoded {row_start}"
            )));
        }
        let coarse_cell_count = chunk_refs
            .iter()
            .map(|chunk| chunk.cell_index)
            .collect::<HashSet<_>>()
            .len();
        let descriptor = GlobalPqDescriptor::new(
            quantizer_state,
            coarse_quantizer_state,
            vectors_seen,
            self.manifest.build_config.vector_element_type,
            location,
            chunk_refs,
        )?;
        let resident_bytes = u64::try_from(descriptor.resident_bytes()).map_err(|_| {
            BorsukError::InvalidStorage("global PQ resident bytes exceed u64".to_string())
        })?;
        let code_bytes_per_vector = descriptor.subspaces();
        // Fixed-width cell-aligned exact vectors need no resident offset table
        // or compression dictionary: row byte ranges are computed directly.
        let sidecar_index_bytes = 0;
        let bytes = descriptor.encode()?;
        storage_bytes = storage_bytes.saturating_add(bytes.len() as u64);
        let checksum = blake3::hash(&bytes).to_hex().to_string();
        let path = format!(
            "global-pq/descriptors/{}/descriptor-{checksum}.parquet",
            &checksum[..2]
        );
        self.storage.write_bytes_content_addressed(&path, &bytes)?;
        Ok(Some(crate::manifest::GlobalPqRef {
            path,
            checksum,
            vectors: vectors_seen,
            subspaces: code_bytes_per_vector,
            candidates: resident_global_pq_candidates(
                &self.manifest.config.metric,
                dimensions,
                code_bytes_per_vector,
                vectors_seen,
            ),
            probes: resident_global_pq_probes(
                &self.manifest.config.metric,
                dimensions,
                coarse_cell_count,
            ),
            resident_bytes,
            sidecar_index_bytes,
            storage_bytes,
            segments: summaries
                .iter()
                .map(|summary| summary.checksum.clone())
                .collect(),
        }))
    }

    fn refresh_resident_global_pq(&mut self) -> Result<()> {
        let summaries = self.active_segment_summaries()?;
        self.refresh_resident_global_pq_from_summaries(&summaries)
    }

    fn refresh_resident_global_pq_from_summaries(
        &mut self,
        summaries: &[SegmentSummary],
    ) -> Result<()> {
        let desired = self.persist_resident_global_pq(summaries)?;
        if desired == self.manifest.global_pq_ref {
            return Ok(());
        }
        let previous = self.manifest.clone();
        let mut manifest = self.manifest.next_version();
        manifest.global_pq_ref = desired;
        enforce_ram_budget(&manifest, self.runtime_ram_budget_bytes)?;
        if manifest.segments.is_empty() {
            let top_read = self.storage.read_routing_layer_page_index_with_status(
                previous.version,
                previous.routing_max_level,
            )?;
            if !top_read.page_refs.is_empty() {
                self.manifest = self.publish_manifest_with_top_routing_page_refs_with_recovery(
                    manifest,
                    previous.routing_max_level,
                    &top_read.page_refs,
                )?;
            } else {
                self.manifest = self.publish_manifest_reusing_routing_pages_with_recovery(
                    manifest,
                    Some(&previous),
                )?;
            }
        } else {
            self.manifest = self
                .publish_manifest_reusing_routing_pages_with_recovery(manifest, Some(&previous))?;
        }
        let mut cache = self
            .resident_global_pq
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        *cache = None;
        Ok(())
    }

    /// For a bounded approximate search over enough cells, navigate the centroid
    /// HNSW to the nearest cells (the IVF probe list) instead of ranking every
    /// cell. Returns `None` to fall back to the routing-tree summaries (exact
    /// search, unbounded probes, or too few cells).
    fn coarse_quantizer_candidates(
        &self,
        query: &[f32],
        options: &SearchOptions,
    ) -> Result<Option<Vec<SegmentSummary>>> {
        if options.guaranteed_recall || options.disable_coarse_quantizer {
            return Ok(None);
        }
        let SearchMode::Approx {
            max_segments: Some(max_segments),
            ..
        } = &options.mode
        else {
            return Ok(None);
        };
        let max_segments = *max_segments;
        if max_segments == 0 {
            return Ok(None);
        }
        // A corrupt/missing persisted quantizer object must not fail the query:
        // fall back to the routing tree instead. (A resident-build error cannot
        // occur here — `build` is infallible past the cell threshold.)
        let Some((hnsw, summaries)) = self.coarse_quantizer().unwrap_or_default() else {
            return Ok(None);
        };
        let normalize = self
            .manifest
            .config
            .metric
            .uses_normalized_euclidean_geometry();
        let probe_query = if normalize {
            crate::metric::unit_l2_normalized(query)
        } else {
            query.to_vec()
        };
        let budget = max_segments
            .saturating_mul(COARSE_QUANTIZER_OVERFETCH)
            .min(summaries.len());
        let selected = hnsw.nearest(&probe_query, budget);
        Ok(Some(
            selected
                .into_iter()
                .map(|node| summaries[node as usize].clone())
                .collect(),
        ))
    }

    fn search_hits(&self, query: &[f32], options: SearchOptions) -> Result<Vec<SearchHit>> {
        Ok(self.search_with_report(query, options)?.hits)
    }

    /// Search the index and return only matching identifiers.
    pub fn search_ids(&self, query: &[f32], options: SearchOptions) -> Result<Vec<String>> {
        self.search_hits(query, options)?
            .into_iter()
            .map(|hit| hit.id.to_utf8_string())
            .collect()
    }

    /// Search the index and return matching byte identifiers.
    pub fn search_id_bytes(&self, query: &[f32], options: SearchOptions) -> Result<Vec<Vec<u8>>> {
        Ok(self
            .search_hits(query, options)?
            .into_iter()
            .map(|hit| hit.id.as_bytes().to_vec())
            .collect())
    }

    /// Search the index and return stored vectors for the nearest neighbors.
    pub fn search_vectors(&self, query: &[f32], options: SearchOptions) -> Result<Vec<Vec<f32>>> {
        Ok(self.search_execution(query, options, true)?.vectors)
    }

    fn search_hits_batch(
        &self,
        queries: &[Vec<f32>],
        options: SearchOptions,
    ) -> Result<Vec<Vec<SearchHit>>> {
        queries
            .iter()
            .map(|query| self.search_hits(query, options.clone()))
            .collect()
    }

    /// Search multiple queries and return only matching identifiers for each query.
    pub fn search_ids_batch(
        &self,
        queries: &[Vec<f32>],
        options: SearchOptions,
    ) -> Result<Vec<Vec<String>>> {
        self.search_hits_batch(queries, options)?
            .into_iter()
            .map(|hits| {
                hits.into_iter()
                    .map(|hit| hit.id.to_utf8_string())
                    .collect()
            })
            .collect()
    }

    /// Search multiple queries and return matching byte identifiers for each query.
    pub fn search_id_bytes_batch(
        &self,
        queries: &[Vec<f32>],
        options: SearchOptions,
    ) -> Result<Vec<Vec<Vec<u8>>>> {
        Ok(self
            .search_hits_batch(queries, options)?
            .into_iter()
            .map(|hits| {
                hits.into_iter()
                    .map(|hit| hit.id.as_bytes().to_vec())
                    .collect()
            })
            .collect())
    }

    /// Search multiple queries and return stored vectors for each query's nearest neighbors.
    pub fn search_vectors_batch(
        &self,
        queries: &[Vec<f32>],
        options: SearchOptions,
    ) -> Result<Vec<Vec<Vec<f32>>>> {
        queries
            .iter()
            .map(|query| self.search_vectors(query, options.clone()))
            .collect()
    }

    /// Search multiple queries and return execution measurements for each query in input order.
    pub fn search_batch_with_report(
        &self,
        queries: &[Vec<f32>],
        options: SearchOptions,
    ) -> Result<Vec<SearchReport>> {
        let mut routing_page_cache = RoutingPageReadCache::default();
        queries
            .iter()
            .map(|query| {
                self.search_execution_with_routing_cache(
                    query,
                    options.clone(),
                    false,
                    Some(&mut routing_page_cache),
                )
                .map(|execution| execution.report)
            })
            .collect()
    }

    /// Search the index and return execution measurements along with the hits.
    pub fn search_with_report(
        &self,
        query: &[f32],
        options: SearchOptions,
    ) -> Result<SearchReport> {
        Ok(self.search_execution(query, options, false)?.report)
    }

    /// Execute a query and return its plan and estimated cost: the object-store
    /// requests and bytes it touched, how routing pruned the segment set, cache
    /// effectiveness, measured latency, and a dollar estimate under `cost`.
    ///
    /// Object-storage engines make cost legible in a way RAM-first engines can't;
    /// `explain` surfaces it directly so callers can reason about `$`/query
    /// before scaling. Pass [`QueryCostModel::default`] for AWS S3 list pricing.
    pub fn explain(
        &self,
        query: &[f32],
        options: SearchOptions,
        cost: QueryCostModel,
    ) -> Result<ExplainReport> {
        let report = self.search_with_report(query, options)?;
        Ok(explain_from_report(report, cost))
    }

    /// Run the retrieve → rerank → top-k pipeline every RAG stack uses, as one
    /// call: retrieve the candidates described by `candidate_options` (include
    /// metadata there if the reranker needs it), rescore them with `rerank`, and
    /// return the top `final_k` by the new score (descending). Each returned
    /// hit's `distance` is set to `-score` so the rest of the API's
    /// lower-is-better ordering still holds.
    ///
    /// `rerank` receives the candidate hits in retrieval order and returns one
    /// score per hit (e.g. from a cross-encoder keyed by `hit.id`, or a function
    /// of `hit.metadata`). A score-count mismatch is rejected.
    pub fn search_rerank<F>(
        &self,
        query: &[f32],
        candidate_options: SearchOptions,
        final_k: usize,
        mut rerank: F,
    ) -> Result<Vec<SearchHit>>
    where
        F: FnMut(&[SearchHit]) -> Vec<f32>,
    {
        let hits = self.search_with_report(query, candidate_options)?.hits;
        let scores = rerank(&hits);
        if scores.len() != hits.len() {
            return Err(BorsukError::InvalidSearchOptions(format!(
                "reranker returned {} scores for {} candidates",
                scores.len(),
                hits.len()
            )));
        }
        let mut scored: Vec<(SearchHit, f32)> = hits.into_iter().zip(scores).collect();
        scored.sort_by(|left, right| {
            right
                .1
                .total_cmp(&left.1)
                .then_with(|| left.0.id.as_bytes().cmp(right.0.id.as_bytes()))
        });
        scored.truncate(final_k);
        Ok(scored
            .into_iter()
            .map(|(mut hit, score)| {
                hit.distance = -score;
                hit
            })
            .collect())
    }

    /// Search using any combination of vector and text queries, then fuse the ranked lists.
    pub fn search_hybrid(
        &self,
        query: &HybridQuery,
        options: HybridOptions,
    ) -> Result<SearchReport> {
        let scoped = self.isolated_search_handle();
        let mut report = scoped.search_hybrid_scoped(query, options)?;
        let cache = scoped.storage.cache_read_counts();
        report.disk_cache_bytes_read = cache.disk_bytes;
        report.backing_bytes_read = cache.backing_bytes;
        report.disk_cache_reads = cache.disk_reads;
        report.backing_reads = cache.backing_reads;
        report.requests = scoped.storage.request_counts();
        Ok(report)
    }

    fn search_hybrid_scoped(
        &self,
        query: &HybridQuery,
        options: HybridOptions,
    ) -> Result<SearchReport> {
        let started = Instant::now();
        if options.k == 0 {
            return Err(BorsukError::InvalidSearchOptions(
                "k must be greater than zero".to_string(),
            ));
        }
        if query.vectors.is_empty() && query.sparse_vectors.is_empty() && query.text.is_none() {
            return Err(BorsukError::InvalidSearchOptions(
                "hybrid query must set at least one vector or text query".to_string(),
            ));
        }

        let candidate_depth = options.candidate_depth.max(options.k);
        enum HybridLeg<'a> {
            Dense(&'a str, &'a [f32]),
            Sparse(&'a str, &'a [u32], &'a [f32]),
            Text(&'a str),
        }
        let mut legs = Vec::with_capacity(
            query.vectors.len() + query.sparse_vectors.len() + usize::from(query.text.is_some()),
        );
        legs.extend(
            query
                .vectors
                .iter()
                .map(|(name, vector)| HybridLeg::Dense(name, vector)),
        );
        legs.extend(
            query
                .sparse_vectors
                .iter()
                .map(|(name, (indices, values))| HybridLeg::Sparse(name, indices, values)),
        );
        if let Some(text) = &query.text {
            legs.push(HybridLeg::Text(text));
        }
        let reports = crate::parallel::install_io(|| {
            legs.par_iter()
                .map(|leg| match leg {
                    HybridLeg::Dense(name, vector) => Ok((
                        (*name).to_string(),
                        self.search_execution_with_routing_cache(
                            vector,
                            options
                                .dense_options
                                .clone()
                                .with_k(candidate_depth)
                                .with_vector_name(*name),
                            false,
                            None,
                        )?
                        .report,
                    )),
                    HybridLeg::Sparse(name, indices, values) => Ok((
                        (*name).to_string(),
                        self.search_sparse_named_with_report(
                            name,
                            indices.to_vec(),
                            values.to_vec(),
                            candidate_depth,
                        )?,
                    )),
                    HybridLeg::Text(text) => Ok((
                        HYBRID_TEXT_MODALITY.to_string(),
                        self.search_text_scoped(text, candidate_depth)?,
                    )),
                })
                .collect::<Result<Vec<_>>>()
        })?;

        let hits = fuse_hybrid_hits(&reports, &options.fusion, options.k);

        Ok(SearchReport {
            hits,
            leaf_mode: "hybrid".to_string(),
            termination_reason: SearchTerminationReason::Complete,
            recall_guarantee: RecallGuarantee::Approximate,
            segments_total: reports
                .iter()
                .map(|(_, report)| report.segments_total)
                .sum(),
            segments_searched: reports
                .iter()
                .map(|(_, report)| report.segments_searched)
                .sum(),
            segments_skipped: reports
                .iter()
                .map(|(_, report)| report.segments_skipped)
                .sum(),
            routing_page_indexes_read: reports
                .iter()
                .map(|(_, report)| report.routing_page_indexes_read)
                .sum(),
            routing_pages_read: reports
                .iter()
                .map(|(_, report)| report.routing_pages_read)
                .sum(),
            bytes_read: reports.iter().map(|(_, report)| report.bytes_read).sum(),
            prefetched_bytes_unused: reports
                .iter()
                .map(|(_, report)| report.prefetched_bytes_unused)
                .sum(),
            graph_bytes_read: reports
                .iter()
                .map(|(_, report)| report.graph_bytes_read)
                .sum(),
            decoded_cache_hits: reports
                .iter()
                .map(|(_, report)| report.decoded_cache_hits)
                .sum(),
            decoded_cache_bytes_read: reports
                .iter()
                .map(|(_, report)| report.decoded_cache_bytes_read)
                .sum(),
            object_cache_hits: reports
                .iter()
                .map(|(_, report)| report.object_cache_hits)
                .sum(),
            object_cache_misses: reports
                .iter()
                .map(|(_, report)| report.object_cache_misses)
                .sum(),
            disk_cache_bytes_read: reports
                .iter()
                .map(|(_, report)| report.disk_cache_bytes_read)
                .sum(),
            backing_bytes_read: reports
                .iter()
                .map(|(_, report)| report.backing_bytes_read)
                .sum(),
            disk_cache_reads: reports
                .iter()
                .map(|(_, report)| report.disk_cache_reads)
                .sum(),
            backing_reads: reports.iter().map(|(_, report)| report.backing_reads).sum(),
            cache_repairs: reports.iter().map(|(_, report)| report.cache_repairs).sum(),
            records_considered: reports
                .iter()
                .map(|(_, report)| report.records_considered)
                .sum(),
            records_scored: reports
                .iter()
                .map(|(_, report)| report.records_scored)
                .sum(),
            graph_candidates_added: reports
                .iter()
                .map(|(_, report)| report.graph_candidates_added)
                .sum(),
            global_graph_chunks_searched: reports
                .iter()
                .map(|(_, report)| report.global_graph_chunks_searched)
                .sum(),
            global_scan_chunks_searched: reports
                .iter()
                .map(|(_, report)| report.global_scan_chunks_searched)
                .sum(),
            resident_bytes_estimate: reports
                .iter()
                .map(|(_, report)| report.resident_bytes_estimate)
                .max()
                .unwrap_or(0),
            elapsed_ms: started.elapsed().as_millis() as u64,
            requests: sum_hybrid_requests(&reports),
            rows_evaluated: reports
                .iter()
                .map(|(_, report)| report.rows_evaluated)
                .sum(),
            rows_passed_filter: reports
                .iter()
                .map(|(_, report)| report.rows_passed_filter)
                .sum(),
            segments_pruned_by_filter: reports
                .iter()
                .map(|(_, report)| report.segments_pruned_by_filter)
                .sum(),
            wal_cells_examined: reports
                .iter()
                .map(|(_, report)| report.wal_cells_examined)
                .sum(),
            wal_lanes_examined: reports
                .iter()
                .map(|(_, report)| report.wal_lanes_examined)
                .sum(),
            wal_runs_examined: reports
                .iter()
                .map(|(_, report)| report.wal_runs_examined)
                .sum(),
            wal_records_examined: reports
                .iter()
                .map(|(_, report)| report.wal_records_examined)
                .sum(),
            wal_snapshot_retries: reports
                .iter()
                .map(|(_, report)| report.wal_snapshot_retries)
                .sum(),
        })
    }

    /// Search text by BM25 over hierarchical Parquet posting blocks.
    pub fn search_text(&self, text: &str, k: usize) -> Result<SearchReport> {
        let scoped = self.isolated_search_handle();
        let mut report = scoped.search_text_scoped(text, k)?;
        let cache = scoped.storage.cache_read_counts();
        report.disk_cache_bytes_read = cache.disk_bytes;
        report.backing_bytes_read = cache.backing_bytes;
        report.disk_cache_reads = cache.disk_reads;
        report.backing_reads = cache.backing_reads;
        report.requests = scoped.storage.request_counts();
        Ok(report)
    }

    fn search_text_scoped(&self, text: &str, k: usize) -> Result<SearchReport> {
        if k == 0 {
            return Err(BorsukError::InvalidSearchOptions(
                "k must be greater than zero".to_string(),
            ));
        }
        if !self.manifest.config.text {
            return Err(BorsukError::InvalidMetricInput(
                "text search requires an index created with text=true; this index has text=false"
                    .to_string(),
            ));
        }

        let _admission = self.admission.as_ref().map(|gate| gate.acquire());
        let started = Instant::now();
        let query_terms = term_frequencies(self.tokenizer.as_ref(), text)
            .keys()
            .copied()
            .collect::<BTreeSet<_>>();
        let summaries = self.active_segment_summaries()?;
        let segments_total = summaries.len();
        let resident_bytes_estimate = self.manifest.resident_bytes_estimate();
        let (document_count_delta, total_document_length_delta, df_deltas, delta_bytes) =
            self.load_bm25_stats_delta_for_terms(&query_terms)?;
        // Fold the un-flushed WAL tail into BM25 as one extra virtual segment so
        // text search is read-your-writes for WAL-buffered documents. The tail
        // records are already MVCC-resolved (newest generation per id, suppressed
        // dropped), so their generations sit at or above any segment copy's.
        let wal_text_sidecar = self.wal_bm25_sidecar()?;
        let physical_docs = summaries
            .iter()
            .map(|summary| u64::from(summary.text_doc_count))
            .sum::<u64>();
        let physical_doc_length = summaries
            .iter()
            .map(|summary| summary.text_total_doc_length)
            .sum::<u64>();
        let total_docs =
            apply_i64_delta(physical_docs, document_count_delta, "BM25 document count")?
                .checked_add(u64::from(wal_text_sidecar.doc_count()))
                .ok_or_else(|| {
                    BorsukError::InvalidStorage("live BM25 document count exceeds u64".to_string())
                })?;
        let total_doc_length = apply_i64_delta(
            physical_doc_length,
            total_document_length_delta,
            "BM25 total document length",
        )?
        .checked_add(wal_text_sidecar.total_doc_length())
        .ok_or_else(|| {
            BorsukError::InvalidStorage("live BM25 total document length exceeds u64".to_string())
        })?;

        if query_terms.is_empty() || total_docs == 0 {
            let mut report = SearchReport {
                hits: Vec::new(),
                leaf_mode: "bm25".to_string(),
                termination_reason: SearchTerminationReason::Complete,
                recall_guarantee: RecallGuarantee::Exact,
                segments_total,
                segments_searched: 0,
                segments_skipped: segments_total,
                routing_page_indexes_read: 0,
                routing_pages_read: 0,
                bytes_read: 0,
                prefetched_bytes_unused: 0,
                graph_bytes_read: 0,
                decoded_cache_hits: 0,
                decoded_cache_bytes_read: 0,
                object_cache_hits: 0,
                object_cache_misses: 0,
                disk_cache_bytes_read: 0,
                backing_bytes_read: 0,
                disk_cache_reads: 0,
                backing_reads: 0,
                cache_repairs: 0,
                records_considered: 0,
                records_scored: 0,
                graph_candidates_added: 0,
                global_graph_chunks_searched: 0,
                global_scan_chunks_searched: 0,
                resident_bytes_estimate,
                elapsed_ms: started.elapsed().as_millis() as u64,
                requests: RequestCounts::default(),
                rows_evaluated: 0,
                rows_passed_filter: 0,
                segments_pruned_by_filter: 0,
                wal_cells_examined: 0,
                wal_lanes_examined: 0,
                wal_runs_examined: 0,
                wal_records_examined: 0,
                wal_snapshot_retries: 0,
            };
            self.apply_wal_search_observation(&mut report);
            return Ok(report);
        }

        let avgdl = total_doc_length as f64 / total_docs as f64;
        let loaded = self.load_lexical_query_plan(LexicalKind::Bm25, "text", &query_terms)?;
        let (plans, mut dfs, mut bytes_read) = match loaded {
            Some((root, pages, bytes)) => {
                let mut dfs = query_terms
                    .iter()
                    .map(|term| (*term, 0_u64))
                    .collect::<BTreeMap<_, _>>();
                for entry in pages.iter().flat_map(|page| &page.entries) {
                    let current = dfs.entry(entry.term).or_default();
                    if *current != 0 && *current != entry.document_frequency {
                        return Err(BorsukError::InvalidStorage(format!(
                            "global BM25 df differs across term pages for {}",
                            entry.term
                        )));
                    }
                    *current = entry.document_frequency;
                }
                (
                    LexicalTermPage::plan_bm25(&pages, &root, &query_terms)?,
                    dfs,
                    bytes,
                )
            }
            None => (
                Vec::new(),
                query_terms.iter().map(|term| (*term, 0_u64)).collect(),
                0,
            ),
        };
        bytes_read = bytes_read.saturating_add(delta_bytes);
        if !wal_text_sidecar.is_empty() {
            bytes_read = bytes_read.saturating_add(self.cell_wal_record_bytes());
        }
        for (term, delta) in df_deltas {
            let current = dfs.entry(term).or_default();
            *current = apply_i64_delta(*current, delta, "BM25 document frequency")?;
        }
        // The WAL tail contributes to global document frequencies but never
        // consumes the persisted-sidecar working-set budget.
        if !wal_text_sidecar.is_empty() {
            for term in &query_terms {
                if let Some(df) = dfs.get_mut(term) {
                    *df += u64::from(wal_text_sidecar.df(*term));
                }
            }
        }

        // Generation-aware MVCC visibility, matching the dense leg: the sidecar
        // stores each row's generation, so a row is visible unless its generation
        // is below the id's minimum visible generation (a plain delete maps above
        // every generation; an upsert maps to the new generation, hiding older
        // copies but keeping the fresh one). A re-upserted document is therefore
        // searchable in the lexical leg immediately, not only after compaction.
        // When a still-live id appears in more than one segment we keep its
        // highest-generation copy so each id contributes a single hit.
        let mut best_by_id = HashMap::<Vec<u8>, (u64, f64)>::new();
        let mut searched_segment_keys = HashSet::new();
        let mut shared_decodes = 0_usize;
        let mut shared_decoded_bytes = 0_u64;
        let mut next_plan = 0;
        while next_plan < plans.len() {
            // The WAL changes corpus statistics relative to the persisted root,
            // so its conservative path evaluates every persisted block.
            if wal_text_sidecar.is_empty()
                && kth_largest_score(best_by_id.values().map(|(_, score)| *score), k)
                    .is_some_and(|threshold| plans[next_plan].upper_bound < threshold)
            {
                break;
            }
            let wave_end = next_plan
                .saturating_add(DEFAULT_SEARCH_PREFETCH_DEPTH.max(1))
                .min(plans.len());
            let wave = &plans[next_plan..wave_end];
            let reads = self
                .read_lexical_wave(LexicalKind::Bm25, wave)
                .into_iter()
                .collect::<Result<Vec<_>>>()?;
            for (plan, (read, physical_bytes, shared_inflight)) in wave.iter().zip(&reads) {
                bytes_read = bytes_read.saturating_add(*physical_bytes);
                if *shared_inflight {
                    shared_decodes = shared_decodes.saturating_add(1);
                    shared_decoded_bytes =
                        shared_decoded_bytes.saturating_add(plan.run.decoded_bytes);
                }
                searched_segment_keys.insert(plan.run.segment_key.clone());
                let mut scores = vec![0.0_f64; read.rows.len()];
                let LexicalRunPostings::Bm25(postings) = &read.postings else {
                    unreachable!("BM25 plan decoded sparse postings")
                };
                crate::lexical_simd::accumulate_bm25(
                    postings,
                    &read.rows,
                    &dfs,
                    total_docs,
                    avgdl,
                    BM25_K1,
                    BM25_B,
                    &mut scores,
                );
                for (metadata, score) in read.rows.iter().zip(scores) {
                    if score <= 0.0
                        || self
                            .min_visible_generation(&metadata.record_id)?
                            .is_some_and(|min_visible| metadata.generation < min_visible)
                    {
                        continue;
                    }
                    match best_by_id.get_mut(&metadata.record_id) {
                        Some(existing) if existing.0 >= metadata.generation => {}
                        Some(existing) => *existing = (metadata.generation, score),
                        None => {
                            best_by_id
                                .insert(metadata.record_id.clone(), (metadata.generation, score));
                        }
                    }
                }
            }
            next_plan = wave_end;
        }
        let segments_searched = searched_segment_keys.len();
        if !wal_text_sidecar.is_empty() {
            self.merge_bm25_sidecar_scores(
                &wal_text_sidecar,
                &query_terms,
                &dfs,
                total_docs,
                avgdl,
                &mut best_by_id,
            )?;
        }

        let mut scored = best_by_id
            .into_iter()
            .map(|(id, (_, score))| (RecordId::from_bytes(id), score))
            .collect::<Vec<_>>();
        scored.sort_by(|left, right| {
            right
                .1
                .total_cmp(&left.1)
                .then_with(|| left.0.cmp(&right.0))
        });
        scored.truncate(k);
        let hits = scored
            .into_iter()
            .map(|(id, score)| SearchHit {
                id,
                distance: -(score as f32),
                metadata: None,
            })
            .collect();

        let mut report = SearchReport {
            hits,
            leaf_mode: "bm25".to_string(),
            termination_reason: SearchTerminationReason::Complete,
            recall_guarantee: RecallGuarantee::Exact,
            segments_total,
            segments_searched,
            segments_skipped: segments_total.saturating_sub(segments_searched),
            routing_page_indexes_read: 0,
            routing_pages_read: 0,
            bytes_read,
            prefetched_bytes_unused: 0,
            graph_bytes_read: 0,
            decoded_cache_hits: shared_decodes,
            decoded_cache_bytes_read: shared_decoded_bytes,
            object_cache_hits: 0,
            object_cache_misses: 0,
            disk_cache_bytes_read: 0,
            backing_bytes_read: 0,
            disk_cache_reads: 0,
            backing_reads: 0,
            cache_repairs: 0,
            records_considered: 0,
            records_scored: 0,
            graph_candidates_added: 0,
            global_graph_chunks_searched: 0,
            global_scan_chunks_searched: 0,
            resident_bytes_estimate,
            elapsed_ms: started.elapsed().as_millis() as u64,
            requests: RequestCounts::default(),
            rows_evaluated: 0,
            rows_passed_filter: 0,
            segments_pruned_by_filter: 0,
            wal_cells_examined: 0,
            wal_lanes_examined: 0,
            wal_runs_examined: 0,
            wal_records_examined: 0,
            wal_snapshot_retries: 0,
        };
        self.apply_wal_search_observation(&mut report);
        Ok(report)
    }

    fn merge_bm25_sidecar_scores(
        &self,
        sidecar: &crate::bm25::Bm25IndexSidecar,
        query_terms: &BTreeSet<u32>,
        dfs: &BTreeMap<u32, u64>,
        total_docs: u64,
        avgdl: f64,
        best_by_id: &mut HashMap<Vec<u8>, (u64, f64)>,
    ) -> Result<()> {
        let mut scores = vec![0.0_f64; sidecar.doc_count() as usize];
        let mut touched = vec![false; scores.len()];
        for term in query_terms {
            let df = dfs[term];
            if df == 0 {
                continue;
            }
            let idf = (1.0 + (total_docs as f64 - df as f64 + 0.5) / (df as f64 + 0.5)).ln();
            crate::lexical_simd::accumulate_bm25_term(
                sidecar.postings(*term),
                sidecar.doc_lengths(),
                idf,
                avgdl,
                BM25_K1,
                BM25_B,
                &mut scores,
                &mut touched,
            );
        }

        for (row, (score, touched)) in scores.into_iter().zip(touched).enumerate() {
            if !touched || score <= 0.0 {
                continue;
            }
            let row = u32::try_from(row).map_err(|_| {
                BorsukError::InvalidStorage("bm25 index row exceeds u32".to_string())
            })?;
            let id_bytes = sidecar.row_id(row).ok_or_else(|| {
                BorsukError::InvalidStorage(format!(
                    "bm25 index row {row} has no record-id mapping"
                ))
            })?;
            let generation = sidecar.row_generation(row).ok_or_else(|| {
                BorsukError::InvalidStorage(format!(
                    "bm25 index row {row} has no generation mapping"
                ))
            })?;
            if self
                .min_visible_generation(id_bytes)?
                .is_some_and(|min_visible| generation < min_visible)
            {
                continue;
            }
            match best_by_id.get_mut(id_bytes) {
                Some(existing) if existing.0 >= generation => {}
                Some(existing) => *existing = (generation, score),
                None => {
                    best_by_id.insert(id_bytes.to_vec(), (generation, score));
                }
            }
        }
        Ok(())
    }

    fn search_execution(
        &self,
        query: &[f32],
        options: SearchOptions,
        include_vectors: bool,
    ) -> Result<SearchExecution> {
        let scoped = self.isolated_search_handle();
        let mut execution =
            scoped.search_execution_with_routing_cache(query, options, include_vectors, None)?;
        let cache = scoped.storage.cache_read_counts();
        execution.report.disk_cache_bytes_read = cache.disk_bytes;
        execution.report.backing_bytes_read = cache.backing_bytes;
        execution.report.disk_cache_reads = cache.disk_reads;
        execution.report.backing_reads = cache.backing_reads;
        Ok(execution)
    }

    fn isolated_search_handle(&self) -> Self {
        let mut scoped = self.clone();
        scoped.storage = self.storage.isolated_read_scope();
        let read_scope = scoped.storage.clone();
        for child in scoped.named.values_mut() {
            child.bind_read_scope(&read_scope);
        }
        scoped
    }

    fn bind_read_scope(&mut self, read_scope: &Storage) {
        self.storage = self.storage.with_read_scope_of(read_scope);
        for child in self.named.values_mut() {
            child.bind_read_scope(read_scope);
        }
    }

    fn search_execution_with_routing_cache(
        &self,
        query: &[f32],
        mut options: SearchOptions,
        include_vectors: bool,
        routing_page_cache: Option<&mut RoutingPageReadCache>,
    ) -> Result<SearchExecution> {
        if !options.vector_name.is_empty() {
            let name = std::mem::take(&mut options.vector_name);
            let child = self.named.get(&name).ok_or_else(|| {
                BorsukError::InvalidSearchOptions(format!(
                    "named vector `{name}` is not declared for this index"
                ))
            })?;
            return child.search_execution_with_routing_cache(
                query,
                options,
                include_vectors,
                routing_page_cache,
            );
        }
        let span = observability::search_span(query.len(), &options, self.manifest.version);
        let _entered = span.enter();
        self.validate_vector(query)?;
        let canonical_query = self
            .manifest
            .build_config
            .vector_element_type
            .canonicalize(query)?;
        let query = canonical_query.as_slice();
        validate_search_options(&options)?;
        self.resolve_cache_execution(&mut options)?;
        self.validate_leaf_capability(options.mode.leaf_mode())?;
        let _admission = self.admission.as_ref().map(|gate| gate.acquire());

        let requests_before = self.storage.request_counts();
        let started = Instant::now();
        let live_wal_tail = if options.k == 0 {
            Vec::new()
        } else {
            self.live_wal_tail_records()?
        };
        if options.k > 0
            && let Some(mut execution) = self.search_resident_global_pq(
                query,
                &options,
                include_vectors,
                started,
                &requests_before,
            )?
        {
            self.merge_wal_tail_into_execution(
                query,
                &options,
                include_vectors,
                &live_wal_tail,
                started,
                &requests_before,
                &mut execution,
            )?;
            observability::record_search_report(&span, &execution.report);
            return Ok(execution);
        }
        let page_index_read = self.routing_layer_page_index_read_for_search()?;
        let segments_total = self.routing_segments_total(&page_index_read.page_refs);
        let resident_bytes_estimate = self.manifest.resident_bytes_estimate();

        if options.k == 0 {
            let execution = SearchExecution {
                report: SearchReport {
                    hits: Vec::new(),
                    leaf_mode: options.mode.leaf_mode().to_string(),
                    termination_reason: SearchTerminationReason::Complete,
                    recall_guarantee: recall_guarantee_for_search(
                        &options.mode,
                        SearchTerminationReason::Complete,
                        segments_total,
                        false,
                    ),
                    segments_total,
                    segments_searched: 0,
                    segments_skipped: segments_total,
                    routing_page_indexes_read: 0,
                    routing_pages_read: 0,
                    bytes_read: 0,
                    prefetched_bytes_unused: 0,
                    graph_bytes_read: 0,
                    decoded_cache_hits: 0,
                    decoded_cache_bytes_read: 0,
                    object_cache_hits: 0,
                    object_cache_misses: 0,
                    disk_cache_bytes_read: 0,
                    backing_bytes_read: 0,
                    disk_cache_reads: 0,
                    backing_reads: 0,
                    cache_repairs: 0,
                    records_considered: 0,
                    records_scored: 0,
                    graph_candidates_added: 0,
                    global_graph_chunks_searched: 0,
                    global_scan_chunks_searched: 0,
                    resident_bytes_estimate,
                    elapsed_ms: started.elapsed().as_millis() as u64,
                    requests: self.storage.request_counts().delta(&requests_before),
                    rows_evaluated: 0,
                    rows_passed_filter: 0,
                    segments_pruned_by_filter: 0,
                    wal_cells_examined: 0,
                    wal_lanes_examined: 0,
                    wal_runs_examined: 0,
                    wal_records_examined: 0,
                    wal_snapshot_retries: 0,
                },
                vectors: Vec::new(),
            };
            observability::record_search_report(&span, &execution.report);
            return Ok(execution);
        }

        // Coarse quantizer (IVF probe list): for a bounded approximate search,
        // navigate the centroid HNSW to the nearest cells rather than ranking
        // every cell through the routing tree. When it fires we skip the tree
        // traversal entirely (only the top-level page-index read already paid
        // for is accounted); otherwise we fall back to the routing summaries.
        let quantizer_candidates = self.coarse_quantizer_candidates(query, &options)?;
        let routing_read = if quantizer_candidates.is_some() {
            RoutingSummariesRead {
                bytes_read: page_index_read.bytes_read,
                routing_page_indexes_read: page_index_read.page_indexes_read,
                object_cache_hits: page_index_read.object_cache_hits,
                object_cache_misses: page_index_read.object_cache_misses,
                ..Default::default()
            }
        } else {
            self.routing_summaries_for_search(query, &options, page_index_read, routing_page_cache)?
        };
        let candidate_summaries: &[SegmentSummary] = match &quantizer_candidates {
            Some(selected) => selected.as_slice(),
            None => routing_read.summaries.as_slice(),
        };
        let metric = &self.manifest.config.metric;
        // Signature prioritization is a heuristic for the routing-tree path. The
        // HNSW coarse quantizer is a proper IVF probe list, so it ranks cells
        // purely by centroid distance; layering signature preference on top would
        // pull spuriously-matching cells ahead of the true-nearest ones and wreck
        // recall at low nprobe. Only prefer signature matches for proximity
        // metrics (under inner product the best match is the highest-magnitude
        // vector, not the most similar, so a signature hit would mislead).
        let prioritize_signature = quantizer_candidates.is_none()
            && should_prioritize_vector_signature(&options.mode)
            && metric.supports_centroid_lower_bound();
        let query_signature = prioritize_signature.then(|| vector_signature(query));
        let candidate_mode = candidate_selection_mode(&options);
        // Prune candidate segments whose metadata stats prove no row can satisfy
        // the filter -- they are never fetched (fewer object reads on selective
        // filters). Pruning is sound: a pruned segment cannot contain a match.
        let mut segments_pruned_by_filter = 0_usize;
        let mut candidates = Vec::with_capacity(candidate_summaries.len());
        for summary in candidate_summaries.iter() {
            if let Some(filter) = &options.filter
                && !summary.metadata_stats.can_match(filter)
            {
                segments_pruned_by_filter += 1;
                continue;
            }
            let lower_bound = summary.lower_bound(query, metric).unwrap_or(0.0);
            let rank_distance =
                segment_routing_rank_distance(summary, query, metric).unwrap_or(lower_bound);
            let signature_miss = query_signature
                .is_some_and(|signature| !summary.might_contain_vector_signature(signature));
            candidates.push((summary, signature_miss, lower_bound, rank_distance));
        }

        // Exact search must visit segments in lower-bound order: its pruning
        // stops as soon as a segment's lower bound exceeds the k-th best, which
        // is only sound when every later segment has an equal-or-larger lower
        // bound. Approximate search instead ranks by centroid distance (the IVF
        // probe order), which recovers recall in high dimensions where the
        // bounding-box lower bound cannot separate cells.
        // Segments proven to hold a vector matching the query's signature come
        // first: a signature hit means the exact/near neighbour is very likely
        // inside, regardless of how the centroids compare. On ordinary queries
        // no segment matches (the query is not an indexed vector), so this is a
        // no-op and the distance key drives ordering. Within a signature tier we
        // rank by lower bound for exact search (its pruning needs that order) and
        // by centroid distance for approximate search (the IVF probe order).
        let rank_by_lower_bound = matches!(candidate_mode, SearchMode::Exact);
        candidates.sort_by(
            |(_, left_signature_miss, left_lower, left_rank),
             (_, right_signature_miss, right_lower, right_rank)| {
                let (left_key, right_key) = if rank_by_lower_bound {
                    (left_lower, right_lower)
                } else {
                    (left_rank, right_rank)
                };
                left_signature_miss
                    .cmp(right_signature_miss)
                    .then_with(|| left_key.partial_cmp(right_key).unwrap_or(Ordering::Equal))
            },
        );

        // Dynamically-loaded filter index: for a filtered query, fetch each
        // candidate's small on-demand filter-index sidecar and drop any segment
        // it proves holds no matching row -- refining the coarse resident stats
        // with an exact index without keeping that index in RAM. Bounded to the
        // segment budget so we never fetch more sidecars than segments we might
        // otherwise read.
        let mut filter_index_bytes_read = 0_u64;
        let mut filter_index_cache_hits = 0_usize;
        let mut filter_index_cache_misses = 0_usize;
        let mut filter_index_cache_repairs = 0_usize;
        if let Some(filter) = &options.filter
            && filter_may_use_index(filter)
        {
            let segment_budget = match &candidate_mode {
                SearchMode::Approx {
                    max_segments: Some(limit),
                    ..
                } => *limit,
                _ => candidates.len(),
            };
            let mut kept = Vec::with_capacity(candidates.len());
            for (position, candidate) in candidates.into_iter().enumerate() {
                if position < segment_budget
                    && let Some(read) = self.read_filter_index(candidate.0)?
                {
                    filter_index_bytes_read += read.bytes_read;
                    if read.cache_hit {
                        filter_index_cache_hits += 1;
                    } else {
                        filter_index_cache_misses += 1;
                    }
                    if read.cache_repaired {
                        filter_index_cache_repairs += 1;
                    }
                    // Prune only when the index can answer the filter exactly and
                    // proves zero matches -- otherwise fall back to reading the
                    // segment. This never drops a real match.
                    if read
                        .index
                        .matching_rows(filter)
                        .is_some_and(|rows| rows.is_empty())
                    {
                        segments_pruned_by_filter += 1;
                        continue;
                    }
                }
                kept.push(candidate);
            }
            candidates = kept;
        }

        let mut hits = Vec::<SearchHitWithVector>::new();
        let mut segments_searched = 0_usize;
        let candidates_total = candidates.len();
        let mut segments_skipped = segments_total.saturating_sub(candidates_total);
        let mut bytes_read = routing_read.bytes_read + filter_index_bytes_read;
        if !live_wal_tail.is_empty() {
            bytes_read = bytes_read.saturating_add(self.cell_wal_record_bytes());
        }
        let mut graph_bytes_read = 0_u64;
        let mut decoded_cache_hits = 0_usize;
        let mut object_cache_hits = routing_read.object_cache_hits + filter_index_cache_hits;
        let mut object_cache_misses = routing_read.object_cache_misses + filter_index_cache_misses;
        let mut cache_repairs = routing_read.cache_repairs + filter_index_cache_repairs;
        let mut records_considered = 0_usize;
        let mut records_scored = 0_usize;
        let mut graph_candidates_added = 0_usize;
        let mut rows_evaluated = 0_usize;
        let mut rows_passed_filter = 0_usize;
        let mut termination_reason = SearchTerminationReason::Complete;
        let mut candidate_truncated = false;
        let mut prefetched_bytes_unused = 0_u64;
        // pq-scan/sq-scan with a candidate budget can score only the chosen
        // candidates, so decode the vector column-projected and fetch just those
        // rows -- bounding per-query decode memory on large segments. Prefetch is
        // disabled for these queries because the projected path reads on its own
        // schedule.
        let projected_reads_override = match candidate_mode {
            SearchMode::Approx {
                projected_reads, ..
            } => projected_reads,
            _ => None,
        };
        // Type-safe `with_projected_reads(..)` wins; falling back to the legacy
        // env kill-switch keeps existing debug workflows working.
        let projected_reads_enabled = projected_reads_override
            .unwrap_or_else(|| std::env::var("BORSUK_DISABLE_PROJECTED_SCORING").is_err());
        let decoded_working_set_is_resident = self.segment_cache.get().is_some_and(|cache| {
            candidates
                .iter()
                .all(|(summary, _, _, _)| cache.contains(&summary.checksum))
        });
        // PQ/SQ projection is a per-query I/O strategy, not a cache policy.
        // Prefer it when an optional decoded cache is empty/incomplete: forcing
        // full-cell reads merely to populate that cache can multiply bytes,
        // decode work, and latency for a small shortlist. An explicitly warmed
        // complete working set still takes the zero-I/O decoded-cache path.
        // Graph and flat modes always take the full-segment/cache path below.
        let query_projectable = projected_reads_enabled
            && !decoded_working_set_is_resident
            && matches!(
                candidate_mode,
                SearchMode::Approx {
                    leaf_mode: LeafMode::PqScan
                        | LeafMode::SrhtPqScan
                        | LeafMode::FastTurboQuantMseScan
                        | LeafMode::FastTurboQuantProdScan
                        | LeafMode::SqScan,
                    max_candidates_per_segment: Some(_),
                    ..
                }
            );
        let prefetch_depth = if self.segment_cache.get().is_some() || query_projectable {
            1
        } else {
            options.prefetch_depth
        };
        // The production graph-free profile has independent immutable objects
        // per selected cell. Fetch and decode those cells concurrently; doing
        // this work serially multiplies S3 round-trip latency by `nprobe`.
        // Restrict eager parallel work to a fixed segment budget with no
        // adaptive/latency/byte stop, so it cannot over-read past a dynamic stop.
        let parallel_projected_budget = if query_projectable && options.filter.is_none() {
            parallel_projected_segment_budget(&candidate_mode, candidates_total)
        } else {
            0
        };
        let mut parallel_projected_reads = bounded_parallel_map_with_gate(
            &candidates[..parallel_projected_budget],
            options.prefetch_depth,
            self.decode_admission.as_deref(),
            |(summary, _, _, _)| {
                self.read_projected_segment(summary, query, &candidate_mode, options.k)
            },
        )
        .into_iter();
        let mut segment_prefetches = VecDeque::<SegmentPrefetch>::new();
        let mut next_prefetch_candidate = 0_usize;
        let mut prefetch_reserved_bytes = bytes_read;
        let mut prefetch_reserved_segments = segments_searched;
        let prefetch_semaphore = Arc::new(Semaphore::new(prefetch_depth.max(1)));
        // Adaptive early-stop bookkeeping: count consecutive segments that did not
        // improve the running top-k (its length grew or its k-th distance fell).
        let mut stale_segments = 0_usize;
        let mut previous_hits_len = 0_usize;
        let mut previous_kth_distance = f32::INFINITY;

        for candidate_index in 0..candidates_total {
            let (summary, _, lower_bound, _) = candidates[candidate_index];
            let current_kth_distance = hits
                .get(options.k.saturating_sub(1))
                .map_or(f32::INFINITY, |hit| hit.hit.distance);
            if hits.len() > previous_hits_len || current_kth_distance < previous_kth_distance {
                stale_segments = 0;
            } else {
                stale_segments += 1;
            }
            previous_hits_len = hits.len();
            previous_kth_distance = current_kth_distance;
            if let Some(stop_reason) = search_stop_reason_before_segment(
                &hits,
                options.k,
                &options.mode,
                segments_searched,
                stale_segments,
                bytes_read,
                lower_bound,
                started.elapsed().as_millis() as u64,
            ) {
                if options.guaranteed_recall && !matches!(options.mode, SearchMode::Exact) {
                    return Err(BorsukError::RecallGuaranteeViolated {
                        reason: stop_reason,
                    });
                }
                termination_reason = stop_reason;
                segments_skipped += candidates_total - candidate_index;
                observability::segment_skip_event(stop_reason, candidates_total - candidate_index);
                for prefetch in segment_prefetches.drain(..) {
                    prefetched_bytes_unused =
                        prefetched_bytes_unused.saturating_add(prefetch.reserved_bytes);
                    prefetch.read.abort();
                }
                break;
            }

            if prefetch_depth > 1 {
                while next_prefetch_candidate < candidates_total
                    && segment_prefetches.len() < prefetch_depth
                    && !search_prefetch_byte_budget_exhausted(
                        &options.mode,
                        prefetch_reserved_bytes,
                    )
                    && !search_prefetch_segment_budget_exhausted(
                        &options.mode,
                        prefetch_reserved_segments,
                    )
                {
                    let (prefetch_summary, _, _, _) = candidates[next_prefetch_candidate];
                    prefetch_reserved_bytes =
                        prefetch_reserved_bytes.saturating_add(prefetch_summary.size_bytes);
                    prefetch_reserved_segments = prefetch_reserved_segments.saturating_add(1);
                    let read = self
                        .storage
                        .prefetch_read_bytes_with_cache_status_and_checksum(
                            prefetch_summary.path.clone(),
                            prefetch_summary.checksum.clone(),
                            Arc::clone(&prefetch_semaphore),
                        );
                    segment_prefetches.push_back(SegmentPrefetch {
                        candidate_index: next_prefetch_candidate,
                        reserved_bytes: prefetch_summary.size_bytes,
                        read,
                    });
                    next_prefetch_candidate += 1;
                }
            }

            let use_projection = query_projectable
                && matches!(
                    max_candidates_per_segment(&candidate_mode),
                    Some(limit) if limit < summary.object_count
                );
            let prepared_projection = if candidate_index < parallel_projected_budget {
                Some(parallel_projected_reads.next().ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "parallel projected read result was missing".to_string(),
                    )
                })??)
            } else {
                None
            };
            let (
                segment,
                segment_bytes_read,
                segment_cache_hit,
                segment_cache_repaired,
                decoded_segment_cache_hit,
                segment_records_considered,
                prepared_candidates,
                prepared_vectors,
            ): SearchSegmentRead = if let Some(prepared) = prepared_projection {
                (
                    prepared.segment,
                    prepared.bytes_read,
                    false,
                    false,
                    false,
                    prepared.records_considered,
                    Some(prepared.candidates),
                    Some(prepared.vectors),
                )
            } else if self.segment_cache.get().is_some() {
                let (segment, bytes, cache_hit, repaired, decoded_cache_hit) =
                    self.read_segment_through_cache(summary, false)?;
                let records = segment.records.len();
                (
                    segment,
                    bytes,
                    cache_hit,
                    repaired,
                    decoded_cache_hit,
                    records,
                    None,
                    None,
                )
            } else if use_projection {
                // Object-store-native: range-read only the non-vector columns to
                // score; the chosen rows' full vectors are range-read at rerank.
                let (segment, bytes_fetched) = self.read_segment_lean_ranged(summary)?;
                let records = segment.records.len();
                (
                    Arc::new(segment),
                    bytes_fetched,
                    false,
                    false,
                    false,
                    records,
                    None,
                    None,
                )
            } else if prefetch_depth > 1 {
                let prefetch = segment_prefetches.pop_front().ok_or_else(|| {
                    BorsukError::InvalidStorage(format!(
                        "segment prefetch for candidate {candidate_index} was not scheduled"
                    ))
                })?;
                if prefetch.candidate_index != candidate_index {
                    return Err(BorsukError::InvalidStorage(format!(
                        "segment prefetch consumed candidate {}, expected {candidate_index}",
                        prefetch.candidate_index
                    )));
                }
                let (decoded, bytes, byte_hit, repaired) =
                    self.read_prefetched_segment(summary, prefetch.read)?;
                let records = decoded.records.len();
                (
                    Arc::new(decoded),
                    bytes,
                    byte_hit,
                    repaired,
                    false,
                    records,
                    None,
                    None,
                )
            } else {
                let (decoded, bytes, byte_hit, repaired) = self.read_segment(summary)?;
                let records = decoded.records.len();
                (
                    Arc::new(decoded),
                    bytes,
                    byte_hit,
                    repaired,
                    false,
                    records,
                    None,
                    None,
                )
            };
            segments_searched += 1;
            bytes_read += segment_bytes_read;
            if decoded_segment_cache_hit {
                decoded_cache_hits += 1;
            } else {
                count_cache_read(
                    segment_cache_hit,
                    &mut object_cache_hits,
                    &mut object_cache_misses,
                );
            }
            count_cache_repair(segment_cache_repaired, &mut cache_repairs);
            records_considered += segment_records_considered;

            // Prefilter: in a budgeted (approx) search with a metadata filter,
            // rank the rows that actually match instead of ranking vector-nearest
            // candidates and discarding the ones that fail the filter. This finds
            // every in-segment match (so filtered recall does not depend on the
            // matches landing in the vector-proximity window), needs no graph
            // read, and does not spend the candidate budget on non-matching rows
            // -- which lets the query reach k sooner and fetch fewer segments.
            // It only replaces the budgeted path when the match set fits the
            // per-segment budget; a broad filter whose matches exceed the budget
            // falls back to the budgeted candidate path. Exact search keeps its
            // existing path (it already scores only matching rows).
            let prefilter_rows = options.filter.as_ref().and_then(|filter| {
                let limit = max_candidates_per_segment(&candidate_mode)?;
                let matches = segment_filter_match_rows(&segment, filter);
                if matches.len() > limit {
                    None
                } else {
                    Some(matches)
                }
            });
            let prefiltered = prefilter_rows.is_some();
            let candidates = if let Some(candidates) = prepared_candidates {
                candidates
            } else if let Some(rows) = prefilter_rows {
                rows_evaluated += segment.records.len();
                rows_passed_filter += rows.len();
                CandidateRecordSelection {
                    indices: rows,
                    graph_candidates_added: 0,
                    truncated: false,
                }
            } else {
                let graph = if should_expand_segment_graph(
                    &candidate_mode,
                    options.k,
                    summary.leaf_mode,
                    segment.records.len(),
                ) {
                    let (
                        graph,
                        graph_bytes,
                        graph_cache_hit,
                        graph_cache_repaired,
                        decoded_graph_cache_hit,
                    ) = self.read_graph(summary, &segment)?;
                    graph_bytes_read += graph_bytes;
                    if decoded_graph_cache_hit {
                        decoded_cache_hits += 1;
                    } else {
                        count_cache_read(
                            graph_cache_hit,
                            &mut object_cache_hits,
                            &mut object_cache_misses,
                        );
                    }
                    count_cache_repair(graph_cache_repaired, &mut cache_repairs);
                    Some(graph)
                } else {
                    None
                };
                candidate_record_indices(
                    &segment,
                    graph.as_deref(),
                    query,
                    &candidate_mode,
                    effective_leaf_mode(&candidate_mode, summary.leaf_mode),
                    options.k,
                    &self.manifest.build_config,
                )?
            };
            candidate_truncated |= candidates.truncated;
            graph_candidates_added += candidates.graph_candidates_added;

            // In the projected path the lean segment has no vectors; fetch only
            // the chosen candidates' vectors from the raw bytes for re-ranking.
            let candidate_vectors = if let Some(vectors) = prepared_vectors {
                Some(vectors)
            } else if use_projection {
                let (vectors, rerank_bytes) =
                    self.segment_vectors_for_rows_ranged(summary, &candidates.indices)?;
                bytes_read += rerank_bytes;
                Some(vectors)
            } else {
                None
            };

            for record_index in candidates.indices {
                let record = &segment.records[record_index];
                // Skip suppressed records (deleted, or an older upsert
                // generation) so top-k is computed over the live version only.
                // The bloom fast-path makes this ~free when nothing is tombstoned.
                if self.is_suppressed(record)? {
                    continue;
                }
                // Filter: a record only competes for top-k if its metadata
                // matches. When the candidates came from the prefilter they are
                // already exactly the matching rows (counted above), so re-check
                // only on the budgeted candidate path. Filtered kNN fills up to k,
                // never fewer.
                if let Some(filter) = &options.filter
                    && !prefiltered
                {
                    rows_evaluated += 1;
                    if !filter.matches(&record.metadata) {
                        continue;
                    }
                    rows_passed_filter += 1;
                }
                let vector = match &candidate_vectors {
                    Some(vectors) => vectors.get(&record_index).ok_or_else(|| {
                        BorsukError::InvalidStorage(format!(
                            "projected vector for candidate row {record_index} was not read"
                        ))
                    })?,
                    None => &record.vector,
                };
                // The query was validated once at the search entry
                // (`validate_vector`) and candidate vectors are stored,
                // already-validated rows, so scoring skips the finite/dim re-scan.
                // Norm-dependent metrics (cosine/angular) score a stored zero
                // vector at their maximum distance (it ranks last) rather than
                // erroring, so a zero-vector corpus never aborts the search.
                let distance = metric.distance_unchecked(query, vector)?;
                records_scored += 1;
                push_hit_with_vector(
                    &mut hits,
                    SearchHit {
                        id: record.id.clone(),
                        distance,
                        metadata: options.include_metadata.then(|| record.metadata.clone()),
                    },
                    include_vectors.then(|| vector.clone()),
                    options.k,
                );
            }
        }
        for prefetch in segment_prefetches.drain(..) {
            prefetched_bytes_unused =
                prefetched_bytes_unused.saturating_add(prefetch.reserved_bytes);
            prefetch.read.abort();
        }

        // WAL tail: brute-force score the un-flushed, un-indexed records and
        // merge them into the same top-k buffer. The tail is bounded by the
        // flush threshold, and its records already respect MVCC (newest
        // generation per id, tombstone-suppressed dropped) via
        // `live_wal_tail_records`, so a just-added record is visible immediately
        // and a later upsert/delete supersedes it. Read-your-writes and
        // snapshot isolation both hold: the tail is exactly this handle's
        // published frontier.
        for record in &live_wal_tail {
            records_considered += 1;
            if let Some(filter) = &options.filter {
                rows_evaluated += 1;
                if !filter.matches(&record.metadata) {
                    continue;
                }
                rows_passed_filter += 1;
            }
            // Query validated once at entry; WAL-tail record vectors were
            // validated at insertion (`add_records_with_report_and_tombstone`).
            let distance = metric.distance_unchecked(query, &record.vector)?;
            records_scored += 1;
            push_hit_with_vector(
                &mut hits,
                SearchHit {
                    id: record.id.clone(),
                    distance,
                    metadata: options.include_metadata.then(|| record.metadata.clone()),
                },
                include_vectors.then(|| record.vector.clone()),
                options.k,
            );
        }

        let vectors = hits
            .iter()
            .filter_map(|hit| hit.vector.clone())
            .collect::<Vec<_>>();
        let hits = hits.into_iter().map(|hit| hit.hit).collect::<Vec<_>>();

        let mut execution = SearchExecution {
            report: SearchReport {
                hits,
                leaf_mode: options.mode.leaf_mode().to_string(),
                termination_reason,
                recall_guarantee: recall_guarantee_for_search(
                    &options.mode,
                    termination_reason,
                    segments_skipped,
                    candidate_truncated,
                ),
                segments_total,
                segments_searched,
                segments_skipped,
                routing_page_indexes_read: routing_read.routing_page_indexes_read,
                routing_pages_read: routing_read.routing_pages_read,
                bytes_read,
                prefetched_bytes_unused,
                graph_bytes_read,
                decoded_cache_hits,
                decoded_cache_bytes_read: 0,
                object_cache_hits,
                object_cache_misses,
                disk_cache_bytes_read: 0,
                backing_bytes_read: 0,
                disk_cache_reads: 0,
                backing_reads: 0,
                cache_repairs,
                records_considered,
                records_scored,
                graph_candidates_added,
                global_graph_chunks_searched: 0,
                global_scan_chunks_searched: 0,
                resident_bytes_estimate,
                elapsed_ms: started.elapsed().as_millis() as u64,
                requests: self.storage.request_counts().delta(&requests_before),
                rows_evaluated,
                rows_passed_filter,
                segments_pruned_by_filter,
                wal_cells_examined: 0,
                wal_lanes_examined: 0,
                wal_runs_examined: 0,
                wal_records_examined: 0,
                wal_snapshot_retries: 0,
            },
            vectors,
        };
        self.apply_wal_search_observation(&mut execution.report);
        observability::record_search_report(&span, &execution.report);
        Ok(execution)
    }

    #[allow(clippy::too_many_arguments)]
    fn merge_wal_tail_into_execution(
        &self,
        query: &[f32],
        options: &SearchOptions,
        include_vectors: bool,
        live_wal_tail: &[VectorRecord],
        started: Instant,
        requests_before: &RequestCounts,
        execution: &mut SearchExecution,
    ) -> Result<()> {
        self.apply_wal_search_observation(&mut execution.report);
        if live_wal_tail.is_empty() {
            return Ok(());
        }
        let existing_vectors = std::mem::take(&mut execution.vectors);
        let mut ranked = std::mem::take(&mut execution.report.hits)
            .into_iter()
            .enumerate()
            .map(|(index, hit)| SearchHitWithVector {
                hit,
                vector: include_vectors
                    .then(|| existing_vectors.get(index).cloned())
                    .flatten(),
            })
            .collect::<Vec<_>>();
        for record in live_wal_tail {
            execution.report.records_considered =
                execution.report.records_considered.saturating_add(1);
            if let Some(filter) = &options.filter {
                execution.report.rows_evaluated = execution.report.rows_evaluated.saturating_add(1);
                if !filter.matches(&record.metadata) {
                    continue;
                }
                execution.report.rows_passed_filter =
                    execution.report.rows_passed_filter.saturating_add(1);
            }
            let distance = self
                .manifest
                .config
                .metric
                .distance_unchecked(query, &record.vector)?;
            execution.report.records_scored = execution.report.records_scored.saturating_add(1);
            push_hit_with_vector(
                &mut ranked,
                SearchHit {
                    id: record.id.clone(),
                    distance,
                    metadata: options.include_metadata.then(|| record.metadata.clone()),
                },
                include_vectors.then(|| record.vector.clone()),
                options.k,
            );
        }
        execution.report.hits = ranked.iter().map(|entry| entry.hit.clone()).collect();
        execution.vectors = ranked
            .iter()
            .filter_map(|entry| entry.vector.clone())
            .collect();
        execution.report.bytes_read = execution
            .report
            .bytes_read
            .saturating_add(self.cell_wal_record_bytes());
        execution.report.elapsed_ms = started.elapsed().as_millis() as u64;
        execution.report.requests = self.storage.request_counts().delta(requests_before);
        Ok(())
    }

    fn resolve_cache_execution(&self, options: &mut SearchOptions) -> Result<()> {
        use crate::CacheExecutionPolicy;

        if matches!(options.cache_execution, CacheExecutionPolicy::Scan)
            || !matches!(options.mode, SearchMode::Approx { .. })
        {
            return Ok(());
        }
        if self.manifest.global_pq_ref.is_some() {
            // The global path selects graph versus scan independently for each
            // probed cell. Rewriting the leaf mode here would make that path
            // ineligible and restore the old all-or-nothing segment graph.
            return Ok(());
        }
        let covered = self
            .resident_routing_summaries()
            .zip(self.segment_cache.get().cloned())
            .is_some_and(|(summaries, cache)| {
                !summaries.is_empty()
                    && summaries.iter().all(|summary| {
                        cache.contains(&summary.checksum)
                            && cache.contains_graph(&summary.checksum, &summary.graph_checksum)
                    })
            });
        if !covered {
            return Ok(());
        }
        if let SearchMode::Approx { leaf_mode, .. } = &mut options.mode {
            *leaf_mode = LeafMode::Graph;
        }
        Ok(())
    }

    fn routing_summaries_for_search(
        &self,
        query: &[f32],
        options: &SearchOptions,
        page_index_read: RoutingLayerPageIndexRead,
        mut routing_page_cache: Option<&mut RoutingPageReadCache>,
    ) -> Result<RoutingSummariesRead> {
        let mut routing_read = RoutingSummariesRead {
            bytes_read: page_index_read.bytes_read,
            routing_page_indexes_read: page_index_read.page_indexes_read,
            object_cache_hits: page_index_read.object_cache_hits,
            object_cache_misses: page_index_read.object_cache_misses,
            ..Default::default()
        };

        if let Some(summaries) = self.resident_routing_summaries() {
            routing_read.summaries = summaries.as_ref().clone();
            return Ok(routing_read);
        }

        if !page_index_read.page_refs.is_empty() {
            let selected_leaf_page_refs_read = self.routing_leaf_page_refs_for_search(
                query,
                options,
                &page_index_read.page_refs,
                routing_page_cache.as_deref_mut(),
            )?;
            routing_read.bytes_read += selected_leaf_page_refs_read.bytes_read;
            routing_read.routing_pages_read += selected_leaf_page_refs_read.routing_pages_read;
            routing_read.object_cache_hits += selected_leaf_page_refs_read.object_cache_hits;
            routing_read.object_cache_misses += selected_leaf_page_refs_read.object_cache_misses;
            routing_read.cache_repairs += selected_leaf_page_refs_read.cache_repairs;
            let selected_pages_read = self.routing_summaries_read_from_page_refs_with_cache(
                &selected_leaf_page_refs_read.page_refs,
                routing_page_cache,
            )?;
            routing_read.bytes_read += selected_pages_read.bytes_read;
            routing_read.routing_pages_read += selected_pages_read.routing_pages_read;
            routing_read.object_cache_hits += selected_pages_read.object_cache_hits;
            routing_read.object_cache_misses += selected_pages_read.object_cache_misses;
            routing_read.cache_repairs += selected_pages_read.cache_repairs;
            routing_read.summaries = selected_pages_read.summaries;
            return Ok(routing_read);
        }

        if self.manifest.segments.is_empty() {
            return Ok(routing_read);
        }

        Err(BorsukError::InvalidStorage(
            "active index has segments but no routing page index".to_string(),
        ))
    }

    fn routing_layer_page_index_read_for_search(&self) -> Result<RoutingLayerPageIndexRead> {
        if self.resident_routing_summaries().is_some() {
            return Ok(RoutingLayerPageIndexRead {
                page_refs: Vec::new(),
                bytes_read: 0,
                page_indexes_read: 0,
                object_cache_hits: 0,
                object_cache_misses: 0,
            });
        }
        if self.manifest.segments.is_empty() {
            let top_read = self.storage.read_routing_layer_page_index_with_status(
                self.manifest.version,
                self.manifest.routing_max_level,
            )?;
            if !top_read.page_refs.is_empty() || self.manifest.routing_max_level == 0 {
                return Ok(top_read);
            }
        }

        self.storage
            .read_routing_layer_page_index_with_status(self.manifest.version, 0)
    }

    fn routing_segments_total(&self, page_refs: &[RoutingLayerPageRef]) -> usize {
        if let Some(summaries) = self.resident_routing_summaries() {
            return summaries.len();
        }
        if !self.manifest.segments.is_empty() {
            return self.manifest.segments.len();
        }

        page_refs
            .iter()
            .map(|page_ref| page_ref.leaf_segments)
            .sum()
    }

    fn routing_leaf_page_refs_for_metadata_scan_with_report(&self) -> Result<RoutingPageRefsRead> {
        let top_read = self.storage.read_routing_layer_page_index_with_status(
            self.manifest.version,
            self.manifest.routing_max_level,
        )?;
        let mut read_result = RoutingPageRefsRead {
            bytes_read: top_read.bytes_read,
            object_cache_hits: top_read.object_cache_hits,
            object_cache_misses: top_read.object_cache_misses,
            ..Default::default()
        };
        if top_read.page_refs.is_empty() {
            return Ok(read_result);
        }
        if self.manifest.routing_max_level == 0 {
            read_result.page_refs = top_read.page_refs;
            return Ok(read_result);
        }
        let leaf_read =
            self.routing_leaf_page_refs_for_filter_read(&top_read.page_refs, |_| true)?;
        read_result.bytes_read += leaf_read.bytes_read;
        read_result.routing_pages_read += leaf_read.routing_pages_read;
        read_result.object_cache_hits += leaf_read.object_cache_hits;
        read_result.object_cache_misses += leaf_read.object_cache_misses;
        read_result.page_refs = leaf_read.page_refs;
        Ok(read_result)
    }

    fn routing_layer_page_refs_for_search(
        &self,
        query: &[f32],
        options: &SearchOptions,
        page_refs: &[RoutingLayerPageRef],
    ) -> Result<Vec<RoutingLayerPageRef>> {
        if options.guaranteed_recall {
            return Ok(page_refs.to_vec());
        }

        let SearchMode::Approx {
            max_segments: Some(max_segments),
            ..
        } = &options.mode
        else {
            return Ok(page_refs.to_vec());
        };
        if page_refs
            .iter()
            .any(|page_ref| page_ref.centroid.len() != self.manifest.config.dimensions)
        {
            return Ok(page_refs.to_vec());
        }

        let prioritize_signature = should_prioritize_vector_signature(&options.mode);
        let query_signature = prioritize_signature.then(|| vector_signature(query));
        let mut ranked_pages = page_refs
            .iter()
            .map(|page_ref| {
                let rank_distance =
                    page_ref_routing_rank_distance(page_ref, query, &self.manifest.config.metric)?;
                let signature_miss = query_signature
                    .is_some_and(|signature| !page_ref.might_contain_vector_signature(signature));
                Ok((
                    rank_distance,
                    signature_miss,
                    page_ref.page_ordinal,
                    page_ref.clone(),
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        ranked_pages.sort_by(|left, right| {
            left.0
                .total_cmp(&right.0)
                .then_with(|| left.1.cmp(&right.1))
                .then_with(|| left.2.cmp(&right.2))
        });
        if page_refs
            .first()
            .is_some_and(|page_ref| page_ref.routing_level == 0)
        {
            let target_leaf_segments = (*max_segments).max(1);
            let target_page_overfetch = routing_page_overfetch(&options.mode);
            let mut selected_leaf_segments = ranked_pages[0].3.leaf_segments.max(1);
            let target_overfetch_leaf_segments =
                target_leaf_segments.saturating_mul(target_page_overfetch);
            // Stop once the probe budget is covered and the next page is beyond
            // the nearest page's centroid distance plus a margin. The cutoff is
            // keyed on centroid rank distance (ranked_pages[..].0), not a
            // bounding-box lower bound, so it holds up in high dimensions where
            // box bounds collapse. This routing path serves the small/paged
            // cases; large indexes take the HNSW coarse-quantizer path instead.
            let cutoff = ranked_pages[0].0;
            let cutoff_margin = routing_lower_bound_overfetch_margin(query, ranked_pages.len());
            let mut pages_to_read = 1_usize;
            while pages_to_read < ranked_pages.len()
                && (pages_to_read < target_page_overfetch
                    || selected_leaf_segments < target_overfetch_leaf_segments)
                && ranked_pages[pages_to_read].0 <= cutoff + cutoff_margin
            {
                selected_leaf_segments = selected_leaf_segments
                    .saturating_add(ranked_pages[pages_to_read].3.leaf_segments.max(1));
                pages_to_read += 1;
            }
            ranked_pages.truncate(pages_to_read);
            ranked_pages.sort_by_key(|(_, _, ordinal, _)| *ordinal);
            return Ok(ranked_pages
                .into_iter()
                .map(|(_, _, _, page_ref)| page_ref)
                .collect());
        }

        // Parent level: descend through the nearest pages by centroid until they
        // cover the probe budget, stopping once the next page is beyond the
        // budget cutoff plus a margin (keyed on centroid rank distance).
        let mut selected = Vec::new();
        let mut selected_leaf_segments = 0_usize;
        let mut cutoff = None::<f32>;
        let cutoff_margin = routing_lower_bound_overfetch_margin(query, ranked_pages.len());
        let target_page_overfetch = routing_page_overfetch(&options.mode);
        let target_leaf_segments = max_segments.saturating_mul(target_page_overfetch);
        for (rank_distance, _, ordinal, page_ref) in ranked_pages {
            if let Some(cutoff) = cutoff
                && rank_distance > cutoff + cutoff_margin
            {
                break;
            }
            selected_leaf_segments = selected_leaf_segments.saturating_add(page_ref.leaf_segments);
            selected.push((ordinal, page_ref));
            if *max_segments != usize::MAX && selected_leaf_segments >= *max_segments {
                if cutoff.is_none() {
                    cutoff = Some(rank_distance);
                }
                if selected.len() >= target_page_overfetch
                    && selected_leaf_segments >= target_leaf_segments
                {
                    break;
                }
            }
        }
        selected.sort_by_key(|(ordinal, _)| *ordinal);

        Ok(selected.into_iter().map(|(_, page_ref)| page_ref).collect())
    }

    fn routing_leaf_page_refs_for_search(
        &self,
        query: &[f32],
        options: &SearchOptions,
        page_refs: &[RoutingLayerPageRef],
        mut routing_page_cache: Option<&mut RoutingPageReadCache>,
    ) -> Result<RoutingPageRefsRead> {
        let mut read_result = RoutingPageRefsRead::default();
        let mut current_page_refs =
            self.routing_layer_page_refs_for_search(query, options, page_refs)?;

        loop {
            let Some(first_page_ref) = current_page_refs.first() else {
                return Ok(read_result);
            };
            let routing_level = first_page_ref.routing_level;
            if current_page_refs
                .iter()
                .any(|page_ref| page_ref.routing_level != routing_level)
            {
                return Err(BorsukError::InvalidStorage(
                    "routing page walk found mixed routing levels".to_string(),
                ));
            }
            if routing_level == 0 {
                read_result.page_refs = current_page_refs;
                return Ok(read_result);
            }

            let child_read = self.routing_child_page_refs_read_from_parent_refs_with_cache(
                &current_page_refs,
                None,
                routing_page_cache.as_deref_mut(),
            )?;
            read_result.bytes_read += child_read.bytes_read;
            read_result.routing_pages_read += child_read.routing_pages_read;
            read_result.object_cache_hits += child_read.object_cache_hits;
            read_result.object_cache_misses += child_read.object_cache_misses;
            read_result.cache_repairs += child_read.cache_repairs;
            current_page_refs =
                self.routing_layer_page_refs_for_search(query, options, &child_read.page_refs)?;
        }
    }

    fn compaction_source_selection_from_routing_tree(
        &self,
        source_level: u8,
        max_segments: usize,
        page_index_read: RoutingLayerPageIndexRead,
        excluded_checksums: &HashSet<String>,
    ) -> Result<CompactionSourceSelectionRead> {
        let mut read_result = CompactionSourceSelectionRead {
            bytes_read: page_index_read.bytes_read,
            routing_page_indexes_read: page_index_read.page_indexes_read,
            object_cache_hits: page_index_read.object_cache_hits,
            object_cache_misses: page_index_read.object_cache_misses,
            ..Default::default()
        };
        let mut pending = page_index_read
            .page_refs
            .into_iter()
            .filter(|page_ref| page_ref.might_contain_level(source_level))
            .collect::<VecDeque<_>>();

        while let Some(page_ref) = pending.pop_front() {
            if read_result.selected.len() >= max_segments {
                break;
            }
            if !page_ref.might_contain_level(source_level) {
                continue;
            }

            if page_ref.routing_level == 0 {
                let page_read =
                    self.routing_summaries_read_from_page_refs(std::slice::from_ref(&page_ref))?;
                read_result.bytes_read += page_read.bytes_read;
                read_result.routing_pages_read += page_read.routing_pages_read;
                read_result.object_cache_hits += page_read.object_cache_hits;
                read_result.object_cache_misses += page_read.object_cache_misses;
                let page_summaries = page_read.summaries;

                let selected_before_page = read_result.selected.len();
                for summary in page_summaries
                    .iter()
                    .filter(|summary| summary.level == source_level)
                    .filter(|summary| !excluded_checksums.contains(&summary.checksum))
                {
                    if read_result.selected.len() >= max_segments {
                        break;
                    }
                    read_result.selected.push(summary.clone());
                }

                if read_result.selected.len() > selected_before_page {
                    read_result
                        .dirty_pages
                        .push((page_ref.page_ordinal, page_summaries));
                }
                continue;
            }

            let child_read = self.routing_child_page_refs_read_from_parent_refs_with_cache(
                std::slice::from_ref(&page_ref),
                Some(&mut read_result.decoded_parent_pages),
                None,
            )?;
            read_result.bytes_read += child_read.bytes_read;
            read_result.routing_pages_read += child_read.routing_pages_read;
            read_result.object_cache_hits += child_read.object_cache_hits;
            read_result.object_cache_misses += child_read.object_cache_misses;

            let mut children = child_read
                .page_refs
                .into_iter()
                .filter(|page_ref| page_ref.might_contain_level(source_level))
                .collect::<Vec<_>>();
            children.sort_by_key(|page_ref| page_ref.page_ordinal);
            for child in children.into_iter().rev() {
                pending.push_front(child);
            }
        }

        read_result
            .dirty_pages
            .sort_by_key(|(page_ordinal, _)| *page_ordinal);

        Ok(read_result)
    }

    fn routing_leaf_page_refs_for_filter<F>(
        &self,
        page_refs: &[RoutingLayerPageRef],
        page_filter: F,
    ) -> Result<Vec<RoutingLayerPageRef>>
    where
        F: FnMut(&RoutingLayerPageRef) -> bool,
    {
        Ok(self
            .routing_leaf_page_refs_for_filter_read(page_refs, page_filter)?
            .page_refs)
    }

    fn routing_leaf_page_refs_for_filter_read<F>(
        &self,
        page_refs: &[RoutingLayerPageRef],
        mut page_filter: F,
    ) -> Result<RoutingPageRefsRead>
    where
        F: FnMut(&RoutingLayerPageRef) -> bool,
    {
        let mut current_page_refs = page_refs
            .iter()
            .filter(|page_ref| page_filter(page_ref))
            .cloned()
            .collect::<Vec<_>>();
        let mut read_result = RoutingPageRefsRead::default();

        loop {
            let Some(first_page_ref) = current_page_refs.first() else {
                return Ok(read_result);
            };
            let routing_level = first_page_ref.routing_level;
            if current_page_refs
                .iter()
                .any(|page_ref| page_ref.routing_level != routing_level)
            {
                return Err(BorsukError::InvalidStorage(
                    "routing page filter found mixed routing levels".to_string(),
                ));
            }
            if routing_level == 0 {
                read_result.page_refs = current_page_refs;
                return Ok(read_result);
            }

            let child_read =
                self.routing_child_page_refs_read_from_parent_refs(&current_page_refs)?;
            read_result.bytes_read += child_read.bytes_read;
            read_result.routing_pages_read += child_read.routing_pages_read;
            read_result.object_cache_hits += child_read.object_cache_hits;
            read_result.object_cache_misses += child_read.object_cache_misses;
            read_result.cache_repairs += child_read.cache_repairs;
            current_page_refs = child_read
                .page_refs
                .into_iter()
                .filter(|page_ref| page_filter(page_ref))
                .collect();
        }
    }

    fn routing_child_page_refs_read_from_parent_refs(
        &self,
        parent_refs: &[RoutingLayerPageRef],
    ) -> Result<RoutingPageRefsRead> {
        self.routing_child_page_refs_read_from_parent_refs_with_cache(parent_refs, None, None)
    }

    fn routing_child_page_refs_read_from_parent_refs_with_cache(
        &self,
        parent_refs: &[RoutingLayerPageRef],
        mut decoded_parent_pages: Option<&mut HashMap<String, Vec<RoutingLayerPageRef>>>,
        mut routing_page_cache: Option<&mut RoutingPageReadCache>,
    ) -> Result<RoutingPageRefsRead> {
        let expected_page_refs = parent_refs
            .iter()
            .map(|page_ref| page_ref.page_segments)
            .sum::<usize>();
        let mut read_result = RoutingPageRefsRead {
            page_refs: Vec::with_capacity(expected_page_refs),
            ..Default::default()
        };

        for parent_ref in parent_refs {
            if let Some(cache) = decoded_parent_pages.as_deref_mut()
                && let Some(cached_page_refs) = cache.get(&parent_ref.path)
            {
                if cached_page_refs.len() != parent_ref.page_segments {
                    return Err(BorsukError::InvalidStorage(format!(
                        "cached routing parent page `{}` yielded {} child page refs, expected {}",
                        parent_ref.path,
                        cached_page_refs.len(),
                        parent_ref.page_segments
                    )));
                }
                read_result
                    .page_refs
                    .extend(cached_page_refs.iter().cloned());
                continue;
            }

            let child_routing_level = parent_ref.routing_level.checked_sub(1).ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "routing parent page read requested for L0 page".to_string(),
                )
            })?;
            let page_read = self
                .read_routing_page_with_cache(
                    &parent_ref.path,
                    &parent_ref.checksum,
                    routing_page_cache.as_deref_mut(),
                )
                .map_err(|err| {
                    BorsukError::InvalidStorage(format!(
                        "routing parent page `{}` could not be read: {err}",
                        parent_ref.path
                    ))
                })?;
            let read = page_read.read;
            read_result.bytes_read += read.bytes.len() as u64;
            read_result.routing_pages_read += 1;
            if !page_read.request_cache_hit {
                count_cache_read(
                    read.cache_hit,
                    &mut read_result.object_cache_hits,
                    &mut read_result.object_cache_misses,
                );
                count_cache_repair(read.cache_repaired, &mut read_result.cache_repairs);
            }
            let mut child_page_refs =
                routing_layer_page_index_from_parquet_relaxed_manifest_version(
                    &read.bytes,
                    self.manifest.version,
                    child_routing_level,
                )
                .map_err(|err| {
                    BorsukError::InvalidStorage(format!(
                        "routing parent page `{}` could not be decoded: {err}",
                        parent_ref.path
                    ))
                })?;
            if child_page_refs.len() != parent_ref.page_segments {
                return Err(BorsukError::InvalidStorage(format!(
                    "routing parent page `{}` yielded {} child page refs, expected {}",
                    parent_ref.path,
                    child_page_refs.len(),
                    parent_ref.page_segments
                )));
            }
            if let Some(cache) = decoded_parent_pages.as_deref_mut() {
                cache.insert(parent_ref.path.clone(), child_page_refs.clone());
            }
            read_result.page_refs.append(&mut child_page_refs);
        }

        if read_result.page_refs.len() != expected_page_refs {
            return Err(BorsukError::InvalidStorage(format!(
                "routing parent pages yielded {} child page refs, expected {}",
                read_result.page_refs.len(),
                expected_page_refs
            )));
        }
        read_result
            .page_refs
            .sort_by_key(|page_ref| page_ref.page_ordinal);

        Ok(read_result)
    }

    fn routing_summaries_from_page_refs(
        &self,
        page_refs: &[RoutingLayerPageRef],
    ) -> Result<Vec<SegmentSummary>> {
        Ok(self
            .routing_summaries_read_from_page_refs(page_refs)?
            .summaries)
    }

    fn read_routing_page_with_cache(
        &self,
        path: &str,
        checksum: &str,
        routing_page_cache: Option<&mut RoutingPageReadCache>,
    ) -> Result<RoutingPageRead> {
        let Some(routing_page_cache) = routing_page_cache else {
            let read = self
                .storage
                .read_bytes_with_cache_status_and_checksum(path, checksum)?;
            return Ok(RoutingPageRead {
                read,
                request_cache_hit: false,
            });
        };

        if let Some(read) = routing_page_cache.reads.get(path) {
            return Ok(RoutingPageRead {
                read: read.clone(),
                request_cache_hit: true,
            });
        }

        let read = self
            .storage
            .read_bytes_with_cache_status_and_checksum(path, checksum)?;
        routing_page_cache
            .reads
            .insert(path.to_string(), read.clone());
        Ok(RoutingPageRead {
            read,
            request_cache_hit: false,
        })
    }

    fn routing_summaries_read_from_page_refs(
        &self,
        page_refs: &[RoutingLayerPageRef],
    ) -> Result<RoutingSummariesRead> {
        self.routing_summaries_read_from_page_refs_with_cache(page_refs, None)
    }

    fn routing_summaries_read_from_page_refs_with_cache(
        &self,
        page_refs: &[RoutingLayerPageRef],
        mut routing_page_cache: Option<&mut RoutingPageReadCache>,
    ) -> Result<RoutingSummariesRead> {
        let expected_summaries = page_refs
            .iter()
            .map(|page_ref| page_ref.page_segments)
            .sum::<usize>();
        let mut read_result = RoutingSummariesRead {
            summaries: Vec::with_capacity(expected_summaries),
            ..Default::default()
        };

        for page_ref in page_refs {
            let page_read = self
                .read_routing_page_with_cache(
                    &page_ref.path,
                    &page_ref.checksum,
                    routing_page_cache.as_deref_mut(),
                )
                .map_err(|err| {
                    BorsukError::InvalidStorage(format!(
                        "routing layer page `{}` could not be read: {err}",
                        page_ref.path
                    ))
                })?;
            let read = page_read.read;
            read_result.bytes_read += read.bytes.len() as u64;
            read_result.routing_pages_read += 1;
            if !page_read.request_cache_hit {
                count_cache_read(
                    read.cache_hit,
                    &mut read_result.object_cache_hits,
                    &mut read_result.object_cache_misses,
                );
                count_cache_repair(read.cache_repaired, &mut read_result.cache_repairs);
            }
            let mut page_summaries = routing_layer_page_from_parquet(
                &read.bytes,
                self.manifest.version,
                page_ref.routing_level,
                page_ref.page_ordinal,
                self.manifest.config.dimensions,
            )
            .map_err(|err| {
                BorsukError::InvalidStorage(format!(
                    "routing layer page `{}` could not be decoded: {err}",
                    page_ref.path
                ))
            })?;
            if page_summaries.len() != page_ref.page_segments {
                return Err(BorsukError::InvalidStorage(format!(
                    "routing layer page `{}` yielded {} segment summaries, expected {}",
                    page_ref.path,
                    page_summaries.len(),
                    page_ref.page_segments
                )));
            }
            read_result.summaries.append(&mut page_summaries);
        }

        if read_result.summaries.len() != expected_summaries {
            return Err(BorsukError::InvalidStorage(format!(
                "routing layer pages yielded {} segment summaries, expected {}",
                read_result.summaries.len(),
                expected_summaries
            )));
        }

        Ok(read_result)
    }

    fn write_segment(&self, segment: Segment) -> Result<SegmentSummary> {
        let layout = crate::PhysicalLayoutRef::resolve(
            &self.manifest.build_config.physical_layout,
            crate::PhysicalObjectRole::NormalSegment,
            crate::PhysicalLayoutContext {
                rows: segment.records.len(),
                dimensions: segment.dimensions,
                vector_element_type: Some(self.manifest.build_config.vector_element_type),
            },
        )?;
        let table_format = crate::DurableTableFormat::try_from(layout.physical_format)?;
        let bytes = crate::build_timing::timed(crate::build_timing::Phase::SegmentTable, || {
            segment_to_table(&segment, table_format)
        })?;
        let layout = layout.with_integrity(&bytes);
        let checksum = blake3::hash(&bytes).to_hex().to_string();
        let prefix = &checksum[..2];
        let path = format!(
            "segments/L{}/{prefix}/seg-{}.{}",
            segment.level,
            segment.id,
            table_format.extension()
        );

        // Build and write the per-segment graph only when the index's leaf
        // capability allows a graph-backed leaf mode. A `PqScanOnly` index never
        // reads a graph, so building and persisting one is pure waste; the
        // segment records/search/compact/GC correctly with the graph triple left
        // empty (validation treats an empty triple as "no graph").
        let (graph_path, graph_checksum, graph_size_bytes) =
            if self.manifest.leaf_capability.builds_graph() {
                let graph = SegmentGraph::from_segment(&segment, self.manifest.graph_neighbors)?;
                let graph_bytes =
                    crate::build_timing::timed(crate::build_timing::Phase::SegmentTable, || {
                        graph_to_parquet(&graph)
                    })?;
                let graph_checksum = blake3::hash(&graph_bytes).to_hex().to_string();
                let graph_prefix = &graph_checksum[..2];
                let graph_path = format!(
                    "graphs/L{}/{graph_prefix}/graph-{}.parquet",
                    segment.level, segment.id
                );
                let graph_size_bytes = graph_bytes.len() as u64;
                crate::build_timing::timed(crate::build_timing::Phase::ObjectPuts, || {
                    self.storage.write_bytes(&path, &bytes)?;
                    self.storage.write_bytes(&graph_path, &graph_bytes)
                })?;
                (graph_path, graph_checksum, graph_size_bytes)
            } else {
                crate::build_timing::timed(crate::build_timing::Phase::ObjectPuts, || {
                    self.storage.write_bytes(&path, &bytes)
                })?;
                (String::new(), String::new(), 0)
            };
        // Persist the on-demand filter-index sidecar (always, so filtered reads
        // never miss it). It rides object storage, not RAM.
        crate::build_timing::timed(crate::build_timing::Phase::FilterIndex, || {
            let filter_index = crate::MetadataIndex::from_rows(
                segment.records.iter().map(|record| &record.metadata),
            );
            self.storage.write_bytes(
                &filter_index_relative_path(&checksum),
                &encode_filter_index(&checksum, &filter_index),
            )
        })?;
        // Persist the per-segment dense-vector sidecar (Arrow IPC) so projected
        // rerank can range-read one candidate row instead of re-reading a
        // Parquet row group. Records store a full-width dense vector even when
        // their Parquet encoding is sparse; a defensively empty vector is
        // written as zeros so every sidecar row is dimension-consistent.
        let vector_size_bytes = if segment.dimensions > 0 {
            crate::build_timing::timed(crate::build_timing::Phase::VectorSidecar, || {
                let mut sidecar_records = segment.records.clone();
                for record in &mut sidecar_records {
                    if record.vector.len() != segment.dimensions {
                        record.vector = vec![0.0f32; segment.dimensions];
                    }
                }
                let vector_bytes = crate::arrow_vector_sidecar::encode_record_sidecar_typed_with(
                    &sidecar_records,
                    segment.dimensions,
                    self.manifest.build_config.vector_element_type,
                    self.manifest.build_config.sidecar_compression,
                )?;
                self.storage
                    .write_bytes(&vector_sidecar_relative_path(&checksum), &vector_bytes)?;
                Ok::<_, BorsukError>(vector_bytes.len() as u64)
            })?
        } else {
            0
        };
        for (name, spec) in &self.manifest.config.named_vectors {
            if spec.kind != VectorKind::LateInteraction {
                continue;
            }
            crate::build_timing::timed(crate::build_timing::Phase::VectorSidecar, || {
                let bytes = crate::late_interaction_sidecar::encode(
                    &segment.records,
                    name,
                    spec.dimensions,
                    spec.element_type,
                    self.manifest.build_config.sidecar_compression,
                )?;
                self.storage.write_bytes(
                    &late_interaction_sidecar_relative_path(name, &checksum),
                    &bytes,
                )
            })?;
        }
        let sparse_encoded = segment
            .records
            .iter()
            .filter(|record| {
                record.storage.resolve_for_vector(&record.vector) == StorageEncoding::Sparse
            })
            .count();
        let dense_encoded = segment.records.len().saturating_sub(sparse_encoded);
        let mut lexical_shards = Vec::new();
        let (text_doc_count, text_total_doc_length, text_lexical_decoded_bytes) =
            if self.manifest.config.text {
                let text_rows = segment
                    .records
                    .iter()
                    .filter_map(|record| {
                        record_text_terms(record)
                            .map(|terms| (record.id.as_bytes().to_vec(), record.generation, terms))
                    })
                    .collect::<Vec<_>>();
                let lexical_rows = text_rows
                    .iter()
                    .map(|(record_id, generation, terms)| LexicalInputRow {
                        record_id: record_id.clone(),
                        generation: *generation,
                        terms: terms.iter().map(|(term, tf)| (*term, *tf as f32)).collect(),
                        document_length: terms.iter().map(|(_, tf)| *tf).sum(),
                    })
                    .collect::<Vec<_>>();
                let lexical_build = build_lexical_segment(
                    LexicalKind::Bm25,
                    crate::VectorElementType::Float32,
                    0,
                    "text",
                    &checksum,
                    &lexical_rows,
                    DEFAULT_LEXICAL_BLOCK_BYTES,
                )?;
                if let Some(shard) =
                    self.persist_lexical_segment_build("text", &checksum, &lexical_build)?
                {
                    lexical_shards.push(shard);
                }
                (
                    u32::try_from(lexical_build.document_count).unwrap_or(u32::MAX),
                    lexical_build.total_document_length,
                    lexical_build
                        .term_page
                        .entries
                        .iter()
                        .map(|entry| entry.run.decoded_bytes)
                        .max()
                        .unwrap_or(0),
                )
            } else {
                (0, 0, 0)
            };
        let mut sparse_lexical_max_decoded_bytes = 0_u64;
        for (name, spec) in &self.manifest.config.named_vectors {
            if spec.kind != VectorKind::Sparse {
                continue;
            }
            let rows = segment
                .records
                .iter()
                .filter_map(|record| {
                    record.extra_sparse.get(name).map(|vector| {
                        (
                            record.id.as_bytes().to_vec(),
                            record.generation,
                            vector.clone(),
                        )
                    })
                })
                .collect::<Vec<_>>();
            let lexical_rows = rows
                .iter()
                .map(|(record_id, generation, vector)| LexicalInputRow {
                    record_id: record_id.clone(),
                    generation: *generation,
                    terms: vector
                        .indices()
                        .iter()
                        .copied()
                        .zip(vector.values().iter().copied())
                        .collect(),
                    document_length: 0,
                })
                .collect::<Vec<_>>();
            let lexical_build = build_lexical_segment(
                LexicalKind::Sparse,
                spec.element_type,
                u32::try_from(spec.dimensions).map_err(|_| {
                    BorsukError::InvalidStorage(format!(
                        "named sparse vector `{name}` dimensions exceed u32"
                    ))
                })?,
                name,
                &checksum,
                &lexical_rows,
                DEFAULT_LEXICAL_BLOCK_BYTES,
            )?;
            if let Some(shard) =
                self.persist_lexical_segment_build(name, &checksum, &lexical_build)?
            {
                lexical_shards.push(shard);
            }
            sparse_lexical_max_decoded_bytes = sparse_lexical_max_decoded_bytes.max(
                lexical_build
                    .term_page
                    .entries
                    .iter()
                    .map(|entry| entry.run.decoded_bytes)
                    .max()
                    .unwrap_or(0),
            );
        }
        let id_bloom = segment_id_bloom(segment.records.iter().map(|record| record.id.as_bytes()));
        let vector_signature_bloom = segment_vector_signature_bloom(
            segment
                .records
                .iter()
                .map(|record| record.vector.as_slice()),
        );
        let (bounds_min, bounds_max) =
            vector_bounds(&segment.records, segment.dimensions, &segment.metric)?;
        let metadata_stats =
            crate::MetadataStats::from_rows(segment.records.iter().map(|record| &record.metadata));

        Ok(SegmentSummary {
            id: segment.id,
            level: segment.level,
            path,
            layout,
            object_count: segment.records.len(),
            dimensions: segment.dimensions,
            centroid: segment.centroid,
            radius: segment.radius,
            bounds_min,
            bounds_max,
            checksum,
            size_bytes: bytes.len() as u64,
            vector_size_bytes,
            graph_path,
            graph_checksum,
            graph_size_bytes,
            leaf_mode: leaf_mode_for_segment_level(segment.level),
            id_bloom,
            vector_signature_bloom,
            metadata_stats,
            sparse_encoded,
            dense_encoded,
            text_doc_count,
            text_total_doc_length,
            text_lexical_decoded_bytes,
            sparse_lexical_max_decoded_bytes,
            lexical_shards,
            created_at: segment.created_at,
        })
    }

    fn persist_lexical_segment_build(
        &self,
        field_name: &str,
        segment_key: &str,
        build: &LexicalSegmentBuild,
    ) -> Result<Option<SegmentLexicalShardRef>> {
        if build.document_count == 0 {
            return Ok(None);
        }
        for object in &build.objects {
            self.storage.write_bytes(&object.path, &object.bytes)?;
        }
        let root = LexicalRoot {
            kind: build.kind,
            dimensions: build.dimensions,
            document_count: build.document_count,
            total_document_length: build.total_document_length,
            pages: Vec::new(),
        };
        let shard_bytes = lexical_term_page_to_parquet(&root, &build.term_page)?;
        let shard_checksum = blake3::hash(&shard_bytes).to_hex().to_string();
        let field_key = blake3::hash(field_name.as_bytes()).to_hex().to_string();
        let shard_path = format!(
            "lexical/shards/{}/{}/{}/shard-{}-{}.parquet",
            build.kind.as_str(),
            &field_key[..12],
            &segment_key[..2],
            segment_key,
            &shard_checksum[..12],
        );
        self.storage.write_bytes(&shard_path, &shard_bytes)?;
        Ok(Some(SegmentLexicalShardRef {
            kind: build.kind.as_str().to_string(),
            name: field_name.to_string(),
            path: shard_path,
            checksum: shard_checksum,
            encoded_bytes: shard_bytes.len() as u64,
            document_count: build.document_count,
            total_document_length: build.total_document_length,
            dimensions: build.dimensions,
        }))
    }

    fn write_lexical_term_pages(
        &self,
        root: &LexicalRoot,
        field_name: &str,
        entries: &[crate::lexical_root::LexicalTermBlock],
    ) -> Result<Vec<LexicalTermPageRef>> {
        let field_key = blake3::hash(field_name.as_bytes()).to_hex().to_string();
        let mut page_refs = Vec::new();
        let mut start = 0;
        while start < entries.len() {
            let first_term = entries[start].term;
            let mut end = start;
            let mut estimated_bytes = 0_usize;
            while end < entries.len()
                && end.saturating_sub(start) < DEFAULT_LEXICAL_TERM_PAGE_ENTRIES
            {
                let entry_bytes = estimated_lexical_term_block_bytes(&entries[end]);
                if end > start
                    && estimated_bytes.saturating_add(entry_bytes) > DEFAULT_LEXICAL_TERM_PAGE_BYTES
                {
                    break;
                }
                estimated_bytes = estimated_bytes.saturating_add(entry_bytes);
                end += 1;
            }
            let last_term = entries[end - 1].term;
            let term_count = entries[start..end]
                .iter()
                .map(|entry| entry.term)
                .collect::<BTreeSet<_>>()
                .len();
            let page = LexicalTermPage {
                kind: root.kind,
                entries: entries[start..end].to_vec(),
            };
            page.validate(root)?;
            let page_bytes = lexical_term_page_to_parquet(root, &page)?;
            let content_checksum = term_page_content_checksum(&page);
            let page_checksum = blake3::hash(&page_bytes).to_hex().to_string();
            let page_path = format!(
                "lexical/terms/{}/{}/{}/terms-{}-{}-{}.parquet",
                root.kind.as_str(),
                &field_key[..12],
                &page_checksum[..2],
                first_term,
                last_term,
                &page_checksum[..12],
            );
            self.storage
                .write_bytes_content_addressed(&page_path, &page_bytes)?;
            page_refs.push(LexicalTermPageRef {
                first_term,
                last_term,
                path: page_path,
                checksum: page_checksum,
                content_checksum,
                encoded_bytes: page_bytes.len() as u64,
                term_count: u32::try_from(term_count).map_err(|_| {
                    BorsukError::InvalidStorage(
                        "lexical term-page term count exceeds u32".to_string(),
                    )
                })?,
            });
            start = end;
        }
        Ok(page_refs)
    }

    fn apply_lexical_shard_change(
        &self,
        mut root: LexicalRoot,
        field_name: &str,
        shard: &SegmentLexicalShardRef,
        adding: bool,
    ) -> Result<LexicalRoot> {
        let kind = LexicalKind::from_str(&shard.kind)?;
        if root.kind != kind || root.dimensions != shard.dimensions || shard.name != field_name {
            return Err(BorsukError::InvalidStorage(format!(
                "lexical shard `{}` metadata differs from root `{field_name}`",
                shard.path
            )));
        }
        let read = self
            .storage
            .read_bytes_with_cache_status_and_checksum(&shard.path, &shard.checksum)?;
        let shard_root = LexicalRoot {
            kind,
            dimensions: shard.dimensions,
            document_count: shard.document_count,
            total_document_length: shard.total_document_length,
            pages: Vec::new(),
        };
        let shard_page = lexical_term_page_from_parquet(&shard_root, &read.bytes)?;
        let decode_root = root.clone();
        let mut mutations = BTreeMap::<u32, LexicalShardTermMutation>::new();
        for entry in shard_page.entries {
            let mutation = mutations.entry(entry.term).or_default();
            let local_df = i64::try_from(entry.document_frequency).map_err(|_| {
                BorsukError::InvalidStorage(
                    "lexical shard document frequency exceeds i64".to_string(),
                )
            })?;
            let signed_df = if adding { local_df } else { -local_df };
            if mutation.document_frequency_delta == 0 {
                mutation.document_frequency_delta = signed_df;
            } else if mutation.document_frequency_delta != signed_df {
                return Err(BorsukError::InvalidStorage(format!(
                    "lexical shard `{}` has inconsistent term df",
                    shard.path
                )));
            }
            if adding {
                mutation.additions.push(entry);
            } else {
                mutation.removal_segment_key = Some(entry.run.segment_key);
            }
        }

        root.document_count = if adding {
            root.document_count
                .checked_add(shard.document_count)
                .ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "lexical global document count exceeds u64".to_string(),
                    )
                })?
        } else {
            root.document_count
                .checked_sub(shard.document_count)
                .ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "lexical shard removal exceeds global document count".to_string(),
                    )
                })?
        };
        root.total_document_length = if adding {
            root.total_document_length
                .checked_add(shard.total_document_length)
                .ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "lexical total document length exceeds u64".to_string(),
                    )
                })?
        } else {
            root.total_document_length
                .checked_sub(shard.total_document_length)
                .ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "lexical shard removal exceeds total document length".to_string(),
                    )
                })?
        };

        let old_pages = std::mem::take(&mut root.pages);
        let mut page_refs = Vec::new();
        let mut resolved_dfs = BTreeMap::<u32, u64>::new();
        let mut additions_written = HashSet::<u32>::new();
        let mut removals_seen = HashSet::<u32>::new();
        for page_ref in old_pages {
            let affected = mutations
                .range(page_ref.first_term..=page_ref.last_term)
                .map(|(term, _)| *term)
                .collect::<Vec<_>>();
            if affected.is_empty() {
                page_refs.push(page_ref);
                continue;
            }
            let read = self
                .storage
                .read_bytes_with_cache_status_and_checksum(&page_ref.path, &page_ref.checksum)?;
            let old_page = lexical_term_page_from_parquet(
                &LexicalRoot {
                    pages: Vec::new(),
                    ..decode_root.clone()
                },
                &read.bytes,
            )?;
            let mut entries = Vec::with_capacity(
                old_page.entries.len()
                    + affected
                        .iter()
                        .map(|term| mutations[term].additions.len())
                        .sum::<usize>(),
            );
            for mut entry in old_page.entries {
                let Some(mutation) = mutations.get(&entry.term) else {
                    entries.push(entry);
                    continue;
                };
                let new_df = match resolved_dfs.get(&entry.term).copied() {
                    Some(value) => value,
                    None => {
                        let value = apply_i64_delta(
                            entry.document_frequency,
                            mutation.document_frequency_delta,
                            "lexical global document frequency",
                        )?;
                        resolved_dfs.insert(entry.term, value);
                        value
                    }
                };
                if mutation
                    .removal_segment_key
                    .as_ref()
                    .is_some_and(|segment_key| segment_key == &entry.run.segment_key)
                {
                    removals_seen.insert(entry.term);
                    continue;
                }
                if new_df == 0 {
                    return Err(BorsukError::InvalidStorage(format!(
                        "term {} retains blocks with zero global df",
                        entry.term
                    )));
                }
                entry.document_frequency = new_df;
                entries.push(entry);
            }
            for term in affected {
                let mutation = &mutations[&term];
                if mutation.additions.is_empty() || !additions_written.insert(term) {
                    continue;
                }
                let new_df = match resolved_dfs.get(&term).copied() {
                    Some(value) => value,
                    None => {
                        let value = apply_i64_delta(
                            0,
                            mutation.document_frequency_delta,
                            "lexical new-term document frequency",
                        )?;
                        resolved_dfs.insert(term, value);
                        value
                    }
                };
                for mut entry in mutation.additions.clone() {
                    entry.document_frequency = new_df;
                    entries.push(entry);
                }
            }
            entries.sort_by(|left, right| {
                left.term
                    .cmp(&right.term)
                    .then_with(|| left.run.segment_key.cmp(&right.run.segment_key))
                    .then_with(|| left.run.row_start.cmp(&right.run.row_start))
            });
            page_refs.extend(self.write_lexical_term_pages(&root, field_name, &entries)?);
        }

        let mut new_entries = Vec::new();
        for (term, mutation) in &mutations {
            if !mutation.additions.is_empty() && !additions_written.contains(term) {
                let new_df = apply_i64_delta(
                    0,
                    mutation.document_frequency_delta,
                    "lexical new-term document frequency",
                )?;
                for mut entry in mutation.additions.clone() {
                    entry.document_frequency = new_df;
                    new_entries.push(entry);
                }
            }
            if mutation.removal_segment_key.is_some() && !removals_seen.contains(term) {
                return Err(BorsukError::InvalidStorage(format!(
                    "lexical shard removal could not find term {term} in the global root"
                )));
            }
        }
        new_entries.sort_by(|left, right| {
            left.term
                .cmp(&right.term)
                .then_with(|| left.run.segment_key.cmp(&right.run.segment_key))
                .then_with(|| left.run.row_start.cmp(&right.run.row_start))
        });
        page_refs.sort_by(|left, right| {
            left.first_term
                .cmp(&right.first_term)
                .then_with(|| left.last_term.cmp(&right.last_term))
                .then_with(|| left.path.cmp(&right.path))
        });
        // Terms outside every existing page can occupy several disjoint gaps.
        // Never combine entries across an intervening page: doing so would make
        // the new page's [first,last] range overlap that retained page.
        let mut gap_entries = BTreeMap::<usize, Vec<crate::lexical_root::LexicalTermBlock>>::new();
        for entry in new_entries {
            let slot = page_refs.partition_point(|page| page.last_term < entry.term);
            if page_refs
                .get(slot)
                .is_some_and(|page| page.first_term <= entry.term)
            {
                return Err(BorsukError::InvalidStorage(format!(
                    "new lexical term {} unexpectedly falls inside a retained page",
                    entry.term
                )));
            }
            gap_entries.entry(slot).or_default().push(entry);
        }
        for entries in gap_entries.into_values() {
            page_refs.extend(self.write_lexical_term_pages(&root, field_name, &entries)?);
        }
        page_refs.sort_by(|left, right| {
            left.first_term
                .cmp(&right.first_term)
                .then_with(|| left.last_term.cmp(&right.last_term))
                .then_with(|| left.path.cmp(&right.path))
        });
        root.pages = page_refs;
        root.validate()?;
        Ok(root)
    }

    fn rebuild_lexical_roots(&self, manifest: &mut Manifest) -> Result<()> {
        let current_summaries = self.active_segment_summaries()?;
        let desired_shards = lexical_shard_identity(&manifest.segments);
        let current_shards = lexical_shard_identity(&current_summaries);
        if desired_shards == current_shards
            && (desired_shards.is_empty() || !self.manifest.lexical_roots.is_empty())
        {
            manifest.lexical_roots = self.manifest.lexical_roots.clone();
            return Ok(());
        }

        let group = |segments: &[SegmentSummary]| {
            let mut grouped = BTreeMap::<(String, String), Vec<SegmentLexicalShardRef>>::new();
            for segment in segments {
                for shard in &segment.lexical_shards {
                    grouped
                        .entry((shard.kind.clone(), shard.name.clone()))
                        .or_default()
                        .push(shard.clone());
                }
            }
            for shards in grouped.values_mut() {
                shards.sort_by(|left, right| {
                    left.path
                        .cmp(&right.path)
                        .then_with(|| left.checksum.cmp(&right.checksum))
                });
            }
            grouped
        };
        let current = group(&current_summaries);
        let desired = group(&manifest.segments);
        let current_roots = self.load_resident_lexical_roots()?;
        let root_refs_by_key = self
            .manifest
            .lexical_roots
            .iter()
            .map(|reference| {
                (
                    (reference.kind.clone(), reference.name.clone()),
                    reference.clone(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let keys = current
            .keys()
            .chain(desired.keys())
            .cloned()
            .collect::<BTreeSet<_>>();
        let mut root_refs = Vec::new();
        for (kind_name, field_name) in keys {
            let old_shards = current
                .get(&(kind_name.clone(), field_name.clone()))
                .cloned()
                .unwrap_or_default();
            let new_shards = desired
                .get(&(kind_name.clone(), field_name.clone()))
                .cloned()
                .unwrap_or_default();
            if old_shards == new_shards {
                if let Some(reference) =
                    root_refs_by_key.get(&(kind_name.clone(), field_name.clone()))
                {
                    root_refs.push(reference.clone());
                }
                continue;
            }
            let kind = LexicalKind::from_str(&kind_name)?;
            let dimensions = new_shards
                .first()
                .or_else(|| old_shards.first())
                .map(|shard| shard.dimensions)
                .ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "lexical shard group unexpectedly empty".to_string(),
                    )
                })?;
            let mut root = current_roots
                .get(&(kind_name.clone(), field_name.clone()))
                .map(|root| root.as_ref().clone())
                .unwrap_or(LexicalRoot {
                    kind,
                    dimensions,
                    document_count: 0,
                    total_document_length: 0,
                    pages: Vec::new(),
                });
            let old_identity = old_shards
                .iter()
                .map(|shard| (shard.path.clone(), shard.checksum.clone()))
                .collect::<HashSet<_>>();
            let new_identity = new_shards
                .iter()
                .map(|shard| (shard.path.clone(), shard.checksum.clone()))
                .collect::<HashSet<_>>();
            for shard in old_shards.iter().filter(|shard| {
                !new_identity.contains(&(shard.path.clone(), shard.checksum.clone()))
            }) {
                root = self.apply_lexical_shard_change(root, &field_name, shard, false)?;
            }
            for shard in new_shards.iter().filter(|shard| {
                !old_identity.contains(&(shard.path.clone(), shard.checksum.clone()))
            }) {
                root = self.apply_lexical_shard_change(root, &field_name, shard, true)?;
            }
            if root.document_count == 0 {
                if !root.pages.is_empty() || root.total_document_length != 0 {
                    return Err(BorsukError::InvalidStorage(format!(
                        "empty lexical root `{field_name}` retains data"
                    )));
                }
                continue;
            }
            root.validate()?;
            let root_bytes = lexical_root_to_parquet(&root)?;
            let root_checksum = blake3::hash(&root_bytes).to_hex().to_string();
            let field_key = blake3::hash(field_name.as_bytes()).to_hex().to_string();
            let root_path = format!(
                "lexical/roots/{}/{}/{}/root-{}.parquet",
                kind.as_str(),
                &field_key[..12],
                &root_checksum[..2],
                root_checksum,
            );
            self.storage
                .write_bytes_content_addressed(&root_path, &root_bytes)?;
            root_refs.push(LexicalRootRef {
                kind: kind.as_str().to_string(),
                name: field_name,
                path: root_path,
                checksum: root_checksum,
                encoded_bytes: root_bytes.len() as u64,
            });
        }
        root_refs.sort_by(|left, right| {
            left.kind
                .cmp(&right.kind)
                .then_with(|| left.name.cmp(&right.name))
        });
        manifest.lexical_roots = root_refs;
        manifest.bm25_stats_delta = self.rebuild_bm25_stats_delta_from_segments(manifest)?;
        Ok(())
    }

    fn load_lexical_query_plan(
        &self,
        kind: LexicalKind,
        field_name: &str,
        terms: &BTreeSet<u32>,
    ) -> Result<Option<(LexicalRoot, Vec<LexicalTermPage>, u64)>> {
        let roots = self.load_resident_lexical_roots()?;
        let Some(root) = roots
            .get(&(kind.as_str().to_string(), field_name.to_string()))
            .cloned()
        else {
            return Ok(None);
        };
        if root.kind != kind {
            return Err(BorsukError::InvalidStorage(
                "resident lexical root kind differs from manifest".to_string(),
            ));
        }
        let mut bytes_read = 0_u64;
        const TERM_PAGE_COLUMNS: &[&str] = &[
            "kind",
            "term",
            "document_frequency",
            "segment_key",
            "row_start",
            "row_count",
            "decoded_bytes",
            "postings_path",
            "postings_checksum",
            "postings_bytes",
            "postings_row_group",
            "postings_group_checksum",
            "metadata_path",
            "metadata_checksum",
            "metadata_bytes",
            "metadata_row_group",
            "metadata_group_checksum",
            "posting_count",
            "min_value",
            "max_value",
            "min_doc_length",
        ];
        let page_refs = root.pages_for_terms(terms);
        let reads = bounded_io_map_with_gate(
            &page_refs,
            DEFAULT_SEARCH_PREFETCH_DEPTH,
            None,
            |page_ref| self.read_lexical_term_page(&root, page_ref, TERM_PAGE_COLUMNS),
        );
        let mut pages = Vec::with_capacity(reads.len());
        for read in reads {
            let (page, physical_bytes, _) = read?;
            bytes_read = bytes_read.saturating_add(physical_bytes);
            pages.push(LexicalTermPage {
                kind: page.kind,
                entries: page
                    .entries
                    .iter()
                    .filter(|entry| terms.contains(&entry.term))
                    .cloned()
                    .collect(),
            });
        }
        Ok(Some((root.as_ref().clone(), pages, bytes_read)))
    }

    fn read_lexical_term_page(
        &self,
        root: &LexicalRoot,
        page_ref: &LexicalTermPageRef,
        columns: &[&str],
    ) -> Result<(Arc<LexicalTermPage>, u64, bool)> {
        let flight_key = format!("term-page:{}", page_ref.content_checksum);
        if let Some(page) = self.decoded_lexical_pages.get(&flight_key) {
            return Ok((page, 0, true));
        }
        let result = self.inflight_lexical_pages.load(&flight_key, || {
            let read = self.storage.read_parquet_row_groups_ranged(
                &page_ref.path,
                page_ref.encoded_bytes,
                RangedColumns::Keep(columns),
                &[0],
            )?;
            let page = lexical_term_page_from_batches(root, &read.batches)?;
            let actual = term_page_content_checksum(&page);
            if actual != page_ref.content_checksum {
                return Err(BorsukError::ChecksumMismatch {
                    path: page_ref.path.clone(),
                    expected: page_ref.content_checksum.clone(),
                    actual,
                });
            }
            Ok((page, read.bytes_fetched))
        })?;
        self.decoded_lexical_pages.insert(
            flight_key,
            Arc::clone(&result.0),
            decoded_lexical_term_page_bytes(&result.0),
        );
        Ok(result)
    }

    fn read_lexical_run(
        &self,
        kind: LexicalKind,
        plan: &PlannedRun,
    ) -> Result<(Arc<LexicalRunRead>, u64, bool)> {
        let flight_key = format!(
            "{}:{}:{}:{}:{}",
            kind.as_str(),
            plan.run.postings_group_checksum,
            plan.run.postings_row_group,
            plan.run.metadata_group_checksum,
            plan.run.metadata_row_group
        );
        if let Some(read) = self.decoded_lexical_reads.get(&flight_key) {
            return Ok((read, 0, true));
        }
        let result = self.inflight_lexical_reads.load(&flight_key, || {
            let (read, bytes_read) = self.read_lexical_run_uncached(kind, plan)?;
            Ok((read, bytes_read))
        })?;
        self.decoded_lexical_reads.insert(
            flight_key,
            Arc::clone(&result.0),
            plan.run.decoded_bytes,
        );
        Ok(result)
    }

    fn read_lexical_wave(
        &self,
        kind: LexicalKind,
        plans: &[PlannedRun],
    ) -> Vec<Result<(Arc<LexicalRunRead>, u64, bool)>> {
        crate::parallel::install_io(|| {
            plans
                .par_iter()
                .map(|plan| {
                    let _bytes = self
                        .lexical_admission
                        .as_ref()
                        .map(|gate| gate.acquire(plan.run.decoded_bytes));
                    self.read_lexical_run(kind, plan)
                })
                .collect()
        })
    }

    fn read_lexical_run_uncached(
        &self,
        kind: LexicalKind,
        plan: &PlannedRun,
    ) -> Result<(LexicalRunRead, u64)> {
        let row_group = usize::try_from(plan.run.postings_row_group).map_err(|_| {
            BorsukError::InvalidStorage("postings row-group ordinal exceeds usize".to_string())
        })?;
        let posting_columns = match kind {
            LexicalKind::Bm25 => &["term", "row", "term_frequency"][..],
            LexicalKind::Sparse => &["term", "row", "value"][..],
        };
        let metadata_group = usize::try_from(plan.run.metadata_row_group).map_err(|_| {
            BorsukError::InvalidStorage("metadata row-group ordinal exceeds usize".to_string())
        })?;
        let (postings_read, metadata_read) = rayon::join(
            || {
                self.storage.read_parquet_row_groups_ranged(
                    &plan.run.postings_path,
                    plan.run.postings_bytes,
                    RangedColumns::Keep(posting_columns),
                    &[row_group],
                )
            },
            || {
                self.storage.read_parquet_row_groups_ranged(
                    &plan.run.metadata_path,
                    plan.run.metadata_bytes,
                    RangedColumns::Keep(&["row", "record_id", "generation", "document_length"]),
                    &[metadata_group],
                )
            },
        );
        let postings_read = postings_read?;
        let metadata_read = metadata_read?;
        let postings = match kind {
            LexicalKind::Bm25 => {
                let postings =
                    bm25_postings_from_batches(&postings_read.batches, plan.run.row_count)?;
                if crate::lexical_root::bm25_postings_checksum(&postings)
                    != plan.run.postings_group_checksum
                {
                    return Err(BorsukError::ChecksumMismatch {
                        path: plan.run.postings_path.clone(),
                        expected: plan.run.postings_group_checksum.clone(),
                        actual: crate::lexical_root::bm25_postings_checksum(&postings),
                    });
                }
                LexicalRunPostings::Bm25(postings)
            }
            LexicalKind::Sparse => {
                let postings =
                    sparse_postings_from_batches(&postings_read.batches, plan.run.row_count)?;
                if crate::lexical_root::sparse_postings_checksum(&postings)
                    != plan.run.postings_group_checksum
                {
                    return Err(BorsukError::ChecksumMismatch {
                        path: plan.run.postings_path.clone(),
                        expected: plan.run.postings_group_checksum.clone(),
                        actual: crate::lexical_root::sparse_postings_checksum(&postings),
                    });
                }
                LexicalRunPostings::Sparse(postings)
            }
        };
        let rows = lexical_row_metadata_from_batches(kind, &metadata_read.batches)?;
        if rows.len() != plan.run.row_count as usize {
            return Err(BorsukError::InvalidStorage(format!(
                "lexical metadata row group has {} rows, expected {}",
                rows.len(),
                plan.run.row_count
            )));
        }
        let actual_metadata_checksum = crate::lexical_root::row_metadata_checksum(&rows);
        if actual_metadata_checksum != plan.run.metadata_group_checksum {
            return Err(BorsukError::ChecksumMismatch {
                path: plan.run.metadata_path.clone(),
                expected: plan.run.metadata_group_checksum.clone(),
                actual: actual_metadata_checksum,
            });
        }
        let bytes_read = postings_read
            .bytes_fetched
            .saturating_add(metadata_read.bytes_fetched);
        Ok((LexicalRunRead { postings, rows }, bytes_read))
    }

    fn read_segment(&self, summary: &SegmentSummary) -> Result<(Segment, u64, bool, bool)> {
        let read = self
            .storage
            .read_bytes_with_cache_status_and_checksum(&summary.path, &summary.checksum)?;
        self.segment_from_read(summary, read)
    }

    /// Full source read for an operation that will rewrite records. Normal
    /// dense searches, point reads, warmup, and global-PQ construction do not
    /// load unrelated late-interaction matrices; compaction/purge/maintenance
    /// call this path so every declared field survives the rewrite.
    fn read_segment_for_rewrite(
        &self,
        summary: &SegmentSummary,
    ) -> Result<(Segment, u64, bool, bool)> {
        let (mut segment, bytes_read, cache_hit, repaired) = self.read_segment(summary)?;
        let late_bytes = self.reconstruct_segment_late_interaction(summary, &mut segment)?;
        Ok((
            segment,
            bytes_read.saturating_add(late_bytes),
            cache_hit,
            repaired,
        ))
    }

    /// Use the same decoded-cache get-or-load path for searches and warming.
    /// The final flag reports whether the decoded segment was already cached.
    fn read_segment_through_cache(
        &self,
        summary: &SegmentSummary,
        pin: bool,
    ) -> Result<(Arc<Segment>, u64, bool, bool, bool)> {
        let cache = self.segment_cache.get().ok_or_else(|| {
            BorsukError::InvalidStorage(
                "decoded segment cache was not initialized before use".to_string(),
            )
        })?;
        if let Some(cached) = cache.get_with_pin(&summary.checksum, pin) {
            return Ok((cached, 0, true, false, true));
        }

        let (decoded, bytes, byte_hit, repaired) = self.read_segment(summary)?;
        let decoded = Arc::new(decoded);
        let decoded_bytes = decoded_segment_bytes(&decoded);
        if pin {
            cache.insert_with_pin(
                summary.checksum.clone(),
                Arc::clone(&decoded),
                decoded_bytes,
                true,
            );
        } else {
            cache.insert(
                summary.checksum.clone(),
                Arc::clone(&decoded),
                decoded_bytes,
            );
        }
        Ok((decoded, bytes, byte_hit, repaired, false))
    }

    /// Object-store-native lean read. Normal segment tables contain no dense
    /// vector column (vectors live in the Arrow sidecar). Parquet currently
    /// fetches its compact table in one known-size GET; Vortex supplies its own
    /// projection/range plan through `StorageVortexReadAt`.
    fn read_segment_lean_ranged(&self, summary: &SegmentSummary) -> Result<(Segment, u64)> {
        let decode_started = Instant::now();
        summary
            .layout
            .validate_for(crate::PhysicalObjectRole::NormalSegment)?;
        let (segment, bytes_fetched) = match summary.layout.physical_format {
            crate::PhysicalFormat::Parquet => {
                let read = self
                    .storage
                    .read_known_size_with_cache_status_and_checksum(
                        &summary.path,
                        summary.size_bytes,
                        &summary.checksum,
                    )?;
                let bytes_fetched = if read.cache_hit {
                    0
                } else {
                    read.bytes.len() as u64
                };
                (
                    lean_segment_from_table(read.bytes, crate::DurableTableFormat::Parquet)?,
                    bytes_fetched,
                )
            }
            crate::PhysicalFormat::Vortex => {
                if self.manifest.build_config.vortex_range_reads {
                    let cache_before = self.storage.cache_read_counts();
                    let segment = crate::format::lean_segment_from_vortex_storage(
                        self.storage.clone(),
                        summary.path.clone(),
                        summary.size_bytes,
                        summary.layout.clone(),
                    )?;
                    let cache_delta = self.storage.cache_read_counts().delta(&cache_before);
                    (segment, cache_delta.backing_bytes)
                } else {
                    let read = self
                        .storage
                        .read_known_size_with_cache_status_and_checksum(
                            &summary.path,
                            summary.size_bytes,
                            &summary.checksum,
                        )?;
                    let bytes_fetched = if read.cache_hit {
                        0
                    } else {
                        read.bytes.len() as u64
                    };
                    (
                        lean_segment_from_table(read.bytes, crate::DurableTableFormat::Vortex)?,
                        bytes_fetched,
                    )
                }
            }
            other => {
                return Err(BorsukError::InvalidStorage(format!(
                    "normal segment cannot use physical format `{other}`"
                )));
            }
        };
        validate_segment_metadata(summary, &segment, &self.manifest.config.metric)?;
        self.storage
            .record_access_event(StorageAccessEvent::decode(
                &summary.path,
                physical_format_for_path(&summary.path),
                summary.size_bytes,
                "record_id|generation|metadata|text|routing_codes|pq_codes",
                "all",
                summary.object_count as u64,
                segment.records.len() as u64,
                elapsed_ns(decode_started),
            ))?;
        Ok((segment, bytes_fetched))
    }

    fn read_projected_segment(
        &self,
        summary: &SegmentSummary,
        query: &[f32],
        mode: &SearchMode,
        k: usize,
    ) -> Result<ProjectedSegmentRead> {
        let (segment, segment_bytes, _shared_inflight) = self
            .inflight_segment_reads
            .load(&summary.checksum, || self.read_segment_lean_ranged(summary))?;
        let records_considered = segment.records.len();
        let mut candidates = candidate_record_indices(
            &segment,
            None,
            query,
            mode,
            effective_leaf_mode(mode, summary.leaf_mode),
            k,
            &self.manifest.build_config,
        )?;
        let (mut vectors, rerank_bytes) =
            self.segment_vectors_for_rows_ranged(summary, &candidates.indices)?;
        let selected = std::mem::take(&mut candidates.indices);
        let mut compact_records = Vec::with_capacity(selected.len());
        let mut compact_vectors = HashMap::with_capacity(selected.len());
        for (compact_index, record_index) in selected.into_iter().enumerate() {
            compact_records.push(segment.records[record_index].clone());
            let vector = vectors.remove(&record_index).ok_or_else(|| {
                BorsukError::InvalidStorage(format!(
                    "projected vector for candidate row {record_index} was not read"
                ))
            })?;
            compact_vectors.insert(compact_index, vector);
        }
        // Keep only query-selected records in the returned segment. Constructing
        // it explicitly avoids cloning the shared cell's full coarse-code matrix.
        let segment = Segment {
            id: segment.id.clone(),
            level: segment.level,
            metric: segment.metric.clone(),
            dimensions: segment.dimensions,
            centroid: segment.centroid.clone(),
            radius: segment.radius,
            records: compact_records,
            routing_codes: Vec::new(),
            pq_codes: Vec::new(),
            pq_min: Vec::new(),
            pq_max: Vec::new(),
            created_at: segment.created_at,
        };
        candidates.indices = (0..segment.records.len()).collect();
        Ok(ProjectedSegmentRead {
            segment: Arc::new(segment),
            bytes_read: segment_bytes.saturating_add(rerank_bytes),
            records_considered,
            candidates,
            vectors: compact_vectors,
        })
    }

    /// Obtain the parsed standard Arrow IPC footer and record-batch table for a
    /// segment, memoized by checksum.
    ///
    /// The index is small — it holds no compressed row payloads, only the tail
    /// metadata — so building it once per segment and reusing it across reranks
    /// is cheap. Segment checksums are content-addressed, so a cached entry is
    /// always valid for its checksum. Reads one bounded Arrow IPC footer suffix;
    /// subsequent candidate reads fetch bounded record-batch byte ranges.
    fn vector_sidecar_index(
        &self,
        checksum: &str,
        row_count: usize,
        dimensions: usize,
    ) -> Result<(Arc<crate::arrow_vector_sidecar::SidecarIndex>, u64)> {
        let path = vector_sidecar_relative_path(checksum);
        self.vector_sidecar_index_at(&path, checksum, row_count, dimensions)
    }

    fn vector_sidecar_index_at(
        &self,
        path: &str,
        checksum: &str,
        row_count: usize,
        dimensions: usize,
    ) -> Result<(Arc<crate::arrow_vector_sidecar::SidecarIndex>, u64)> {
        if let Ok(mut cache) = self.vector_sidecar_indexes.lock()
            && let Some(index) = cache.get(checksum)
        {
            return Ok((index, 0));
        }
        let max_tail = crate::arrow_vector_sidecar::max_index_tail_len(
            row_count,
            dimensions,
            self.manifest.build_config.vector_element_type,
        )?;
        let tail = self.storage.read_suffix(path, max_tail)?;
        let index = Arc::new(crate::arrow_vector_sidecar::parse_tail(
            &tail.bytes,
            row_count,
        )?);
        let bytes_fetched = if tail.cache_hit {
            0
        } else {
            tail.bytes.len() as u64
        };
        if let Ok(mut cache) = self.vector_sidecar_indexes.lock() {
            cache.insert(checksum.to_string(), Arc::clone(&index));
        }
        Ok((index, bytes_fetched))
    }

    /// Rerank read: range-fetch full vectors for exactly the chosen candidate
    /// rows, decoded in ascending row order. Returns the row→vector map and the
    /// bytes fetched.
    ///
    /// Candidate rows in the same Arrow IPC record batch share one fetched and
    /// decoded immutable block. The fetched-byte total includes the complete
    /// physical spans transferred from object storage.
    fn segment_vectors_for_rows_ranged(
        &self,
        summary: &SegmentSummary,
        rows: &[usize],
    ) -> Result<(std::collections::HashMap<usize, Vec<f32>>, u64)> {
        let mut unique_rows = rows.to_vec();
        unique_rows.sort_unstable();
        unique_rows.dedup();
        let batch_rows = crate::arrow_vector_sidecar::recommended_batch_rows(
            summary.dimensions,
            self.manifest.build_config.vector_element_type,
        )?;
        let batch_count = summary.object_count.div_ceil(batch_rows);
        if !unique_rows.is_empty() && unique_rows.len() >= batch_count {
            // At this candidate density a ranged `take` can touch every Arrow
            // record batch. Fetching the footer plus all batch ranges would be
            // no smaller than the complete sidecar and can be larger because
            // the footer is transferred twice. Select the full immutable object
            // before issuing any range read, then retain only requested rows.
            let path = vector_sidecar_relative_path(&summary.checksum);
            let read = self.storage.read_bytes_with_cache_status(&path)?;
            let bytes = if read.cache_hit {
                0
            } else {
                read.bytes.len() as u64
            };
            let requested_rows = unique_rows.len();
            let decode_started = Instant::now();
            let vectors = crate::arrow_vector_sidecar::decode_all(&read.bytes, summary.dimensions)?;
            self.storage
                .record_access_event(StorageAccessEvent::decode(
                    &path,
                    physical_format_for_path(&path),
                    summary.vector_size_bytes,
                    "vector",
                    format!("rows:{}", join_rows(&unique_rows)),
                    requested_rows as u64,
                    vectors.len() as u64,
                    elapsed_ns(decode_started),
                ))?;
            let selected = unique_rows
                .into_iter()
                .map(|row| {
                    vectors
                        .get(row)
                        .cloned()
                        .map(|vector| (row, vector))
                        .ok_or_else(|| {
                            BorsukError::InvalidStorage(format!(
                                "projected vector row {row} exceeds {} sidecar rows",
                                vectors.len()
                            ))
                        })
                })
                .collect::<Result<std::collections::HashMap<_, _>>>()?;
            return Ok((selected, bytes));
        }
        let (records, bytes) = self.segment_exact_rows_ranged(summary, rows)?;
        Ok((
            records
                .into_iter()
                .map(|(row, record)| (row, record.vector))
                .collect(),
            bytes,
        ))
    }

    fn segment_exact_rows_ranged(
        &self,
        summary: &SegmentSummary,
        rows: &[usize],
    ) -> Result<(
        std::collections::HashMap<usize, crate::arrow_vector_sidecar::ExactSidecarRow>,
        u64,
    )> {
        let mut sorted = rows.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        if sorted.is_empty() {
            return Ok((std::collections::HashMap::new(), 0));
        }
        let (index, index_bytes) =
            self.vector_sidecar_index(&summary.checksum, summary.object_count, summary.dimensions)?;
        let path = vector_sidecar_relative_path(&summary.checksum);
        let mut groups = Vec::<(std::ops::Range<u64>, Vec<usize>)>::with_capacity(sorted.len());
        for row in sorted {
            let range = index.row_range(row)?;
            if let Some((last_range, group_rows)) = groups.last_mut()
                && *last_range == range
            {
                group_rows.push(row);
            } else {
                groups.push((range, vec![row]));
            }
        }
        let ranges = groups
            .iter()
            .map(|(range, _)| range.clone())
            .collect::<Vec<_>>();
        let chunks = self.storage.read_ranges(&path, &ranges)?;
        let mut map = std::collections::HashMap::with_capacity(rows.len());
        let bytes = index_bytes.saturating_add(chunks.bytes_fetched);
        let decoded_rows = groups.iter().try_fold(0_usize, |total, (_, rows)| {
            index
                .batch_rows_for(rows[0])
                .map(|range| total.saturating_add(range.len()))
        })?;
        let decode_started = Instant::now();
        for ((_, rows), bytes) in groups.iter().zip(&chunks.chunks) {
            for (row, record) in index.decode_records(rows, bytes)? {
                map.insert(row, record);
            }
        }
        self.storage
            .record_access_event(StorageAccessEvent::decode(
                &path,
                physical_format_for_path(&path),
                summary.vector_size_bytes,
                "record_id|generation|vector",
                format!("rows:{}", join_rows(rows)),
                map.len() as u64,
                decoded_rows as u64,
                elapsed_ns(decode_started),
            ))?;
        Ok((map, bytes))
    }

    #[allow(clippy::type_complexity)]
    fn global_exact_vectors_bundled(
        &self,
        path: &str,
        chunks: &[(GlobalPqChunkRef, Vec<(usize, usize)>)],
    ) -> Result<(Vec<(usize, Vec<f32>)>, u64)> {
        let dimensions = self.manifest.config.dimensions;
        let vector_element_type = self.manifest.build_config.vector_element_type;
        let row_bytes = vector_element_type.fixed_width_bytes(dimensions)?;
        let mut requested = Vec::<(Range<u64>, usize)>::new();
        for (chunk, entries) in chunks {
            if chunk.path != path {
                return Err(BorsukError::InvalidStorage(
                    "global exact-vector bundle path mismatch".to_string(),
                ));
            }
            for &(node, row) in entries {
                if row >= chunk.rows {
                    return Err(BorsukError::InvalidStorage(
                        "global exact-vector row exceeds its chunk".to_string(),
                    ));
                }
                let local_start = row.checked_mul(row_bytes).ok_or_else(|| {
                    BorsukError::InvalidStorage("global exact-vector range overflows".to_string())
                })?;
                let start = chunk
                    .exact_offset_bytes
                    .checked_add(local_start)
                    .ok_or_else(|| {
                        BorsukError::InvalidStorage(
                            "global exact-vector bundle offset overflows".to_string(),
                        )
                    })?;
                let end = start.checked_add(row_bytes).ok_or_else(|| {
                    BorsukError::InvalidStorage("global exact-vector range overflows".to_string())
                })?;
                requested.push((start as u64..end as u64, node));
            }
        }
        requested.sort_unstable_by(|left, right| {
            left.0
                .start
                .cmp(&right.0.start)
                .then_with(|| left.1.cmp(&right.1))
        });
        let ranges = requested
            .iter()
            .map(|(range, _)| range.clone())
            .collect::<Vec<_>>();
        let fetched = self.storage.read_ranges(path, &ranges)?;
        let mut vectors = Vec::with_capacity(requested.len());
        let bytes_fetched = fetched.bytes_fetched;
        for ((_, node), bytes) in requested.into_iter().zip(&fetched.chunks) {
            if bytes.len() != row_bytes {
                return Err(BorsukError::InvalidStorage(
                    "global exact-vector row is truncated".to_string(),
                ));
            }
            let vector = vector_element_type.decode_fixed_width(bytes, dimensions)?;
            vectors.push((node, vector));
        }
        Ok((vectors, bytes_fetched))
    }

    fn read_prefetched_segment(
        &self,
        summary: &SegmentSummary,
        prefetched: PrefetchedRead,
    ) -> Result<(Segment, u64, bool, bool)> {
        let relative = prefetched.relative().to_string();
        let read = self.storage.consume_prefetched_read(prefetched)?;
        if relative != summary.path {
            return Err(BorsukError::InvalidStorage(format!(
                "prefetched segment path `{relative}` does not match summary path `{}`",
                summary.path
            )));
        }
        self.segment_from_read(summary, read)
    }

    fn segment_from_read(
        &self,
        summary: &SegmentSummary,
        read: ReadBytes,
    ) -> Result<(Segment, u64, bool, bool)> {
        let bytes_read = read.bytes.len() as u64;
        let cache_hit = read.cache_hit;
        let cache_repaired = read.cache_repaired;
        validate_object_size("segment", &summary.path, summary.size_bytes, bytes_read)?;

        // The segment table no longer carries the dense `vector` column, so it
        // decodes with empty dense vectors. Reconstruct each record's dense
        // vector from the per-segment Arrow IPC sidecar, which is the sole home
        // of dense vectors and stores them in the same row order as the durable
        // segment rows (both were written from `segment.records` in order). The whole
        // sidecar is read here, so its bytes are charged to the read total.
        let decode_started = Instant::now();
        summary
            .layout
            .validate_for(crate::PhysicalObjectRole::NormalSegment)?;
        let mut segment = segment_from_table(
            read.bytes,
            crate::DurableTableFormat::try_from(summary.layout.physical_format)?,
        )?;
        self.storage
            .record_access_event(StorageAccessEvent::decode(
                &summary.path,
                physical_format_for_path(&summary.path),
                summary.size_bytes,
                "*|-vector",
                "all",
                summary.object_count as u64,
                segment.records.len() as u64,
                elapsed_ns(decode_started),
            ))?;
        let sidecar_bytes = self.reconstruct_segment_vectors(summary, &mut segment)?;
        validate_segment_metadata(summary, &segment, &self.manifest.config.metric)?;

        Ok((
            segment,
            bytes_read + sidecar_bytes,
            cache_hit,
            cache_repaired,
        ))
    }

    /// Populate a freshly-decoded segment's per-record dense vectors from the
    /// per-segment Arrow IPC dense-vector sidecar.
    ///
    /// The segment table stores no dense `vector` column, so the format-neutral
    /// decoder returns records with empty dense vectors. The
    /// sidecar holds every row's full-width dense vector (a sparse-encoded
    /// record still carries its densified vector at write time, so its sidecar
    /// row is that densified vector — matching the pre-sidecar behavior where a
    /// full decode returned the densified vector for sparse records). Row `i` of
    /// the sidecar corresponds to `segment.records[i]`.
    fn reconstruct_segment_vectors(
        &self,
        summary: &SegmentSummary,
        segment: &mut Segment,
    ) -> Result<u64> {
        let dim = segment.dimensions;
        if dim == 0 || segment.records.is_empty() {
            return Ok(0);
        }
        let path = vector_sidecar_relative_path(&summary.checksum);
        let sidecar = self.storage.read_bytes_with_cache_status(&path)?.bytes;
        let sidecar_bytes = sidecar.len() as u64;
        let decode_started = Instant::now();
        let vectors = crate::arrow_vector_sidecar::decode_all(&sidecar, dim)?;
        self.storage
            .record_access_event(StorageAccessEvent::decode(
                &path,
                physical_format_for_path(&path),
                sidecar.len() as u64,
                "vector",
                "all",
                segment.records.len() as u64,
                vectors.len() as u64,
                elapsed_ns(decode_started),
            ))?;
        if vectors.len() != segment.records.len() {
            return Err(BorsukError::InvalidStorage(format!(
                "vector sidecar `{path}` holds {} rows but the segment has {} records",
                vectors.len(),
                segment.records.len()
            )));
        }
        for (record, vector) in segment.records.iter_mut().zip(vectors) {
            record.vector = vector;
        }
        Ok(sidecar_bytes)
    }

    fn reconstruct_segment_late_interaction(
        &self,
        summary: &SegmentSummary,
        segment: &mut Segment,
    ) -> Result<u64> {
        let mut bytes_read = 0_u64;
        for (name, spec) in &self.manifest.config.named_vectors {
            if spec.kind != VectorKind::LateInteraction {
                continue;
            }
            let path = late_interaction_sidecar_relative_path(name, &summary.checksum);
            let read = self.storage.read_bytes_with_cache_status(&path)?;
            bytes_read = bytes_read.saturating_add(read.bytes.len() as u64);
            let decode_started = Instant::now();
            let vectors =
                crate::late_interaction_sidecar::decode_all(&read.bytes, segment.records.len())?;
            self.storage
                .record_access_event(StorageAccessEvent::decode(
                    &path,
                    physical_format_for_path(&path),
                    read.bytes.len() as u64,
                    "record_id|generation|token_matrix",
                    "all",
                    segment.records.len() as u64,
                    vectors.len() as u64,
                    elapsed_ns(decode_started),
                ))?;
            for (record, vector) in segment.records.iter_mut().zip(vectors) {
                if let Some(vector) = vector {
                    record.extra_multi_vectors.insert(name.clone(), vector);
                }
            }
        }
        Ok(bytes_read)
    }

    /// Read the per-segment filter-index sidecar on demand. Returns `None` when
    /// the sidecar is absent, unreadable, or fails self-validation -- in every
    /// such case the caller falls back to reading the segment payload, so a bad
    /// sidecar only forgoes an I/O saving, never changes results.
    fn read_filter_index(&self, summary: &SegmentSummary) -> Result<Option<FilterIndexRead>> {
        let path = filter_index_relative_path(&summary.checksum);
        match self.storage.read_bytes_with_cache_status(&path) {
            Ok(read) => {
                let decode_started = Instant::now();
                let decoded = decode_filter_index(&read.bytes, &summary.checksum);
                self.storage
                    .record_access_event(StorageAccessEvent::decode(
                        &path,
                        physical_format_for_path(&path),
                        read.bytes.len() as u64,
                        "metadata_filter_index",
                        "all",
                        summary.object_count as u64,
                        summary.object_count as u64,
                        elapsed_ns(decode_started),
                    ))?;
                Ok(decoded.map(|index| FilterIndexRead {
                    index,
                    bytes_read: read.bytes.len() as u64,
                    cache_hit: read.cache_hit,
                    cache_repaired: read.cache_repaired,
                }))
            }
            // Best-effort accelerator: any read failure just means "fall back".
            Err(_) => Ok(None),
        }
    }

    /// Restore sparse named-vector payloads stripped by primary segment decode.
    /// Source rows are keyed by both id and generation so an output record can
    /// never inherit a superseded version's sparse vector.
    fn repopulate_sparse_named_records(
        &self,
        records: &mut [VectorRecord],
        source_summaries: &[SegmentSummary],
    ) -> Result<()> {
        for (name, spec) in &self.manifest.config.named_vectors {
            if spec.kind != VectorKind::Sparse {
                continue;
            }
            let mut vectors = HashMap::<(Vec<u8>, u64), SparseVector>::new();
            for summary in source_summaries {
                let Some(shard) = summary
                    .lexical_shards
                    .iter()
                    .find(|shard| shard.kind == "sparse" && shard.name == *name)
                else {
                    continue;
                };
                let read = self
                    .storage
                    .read_bytes_with_cache_status_and_checksum(&shard.path, &shard.checksum)?;
                let shard_root = LexicalRoot {
                    kind: LexicalKind::Sparse,
                    dimensions: shard.dimensions,
                    document_count: shard.document_count,
                    total_document_length: 0,
                    pages: Vec::new(),
                };
                let page = lexical_term_page_from_parquet(&shard_root, &read.bytes)?;
                let mut runs = BTreeMap::<(String, u32), PlannedRun>::new();
                for entry in page.entries {
                    let key = (entry.run.segment_key.clone(), entry.run.row_start);
                    let plan = runs.entry(key).or_insert_with(|| PlannedRun {
                        run: entry.run.clone(),
                        upper_bound: 0.0,
                        terms: Vec::new(),
                    });
                    plan.terms.push(entry.term);
                }
                for mut plan in runs.into_values() {
                    plan.terms.sort_unstable();
                    plan.terms.dedup();
                    let (decoded, _, _) = self.read_lexical_run(LexicalKind::Sparse, &plan)?;
                    let LexicalRunPostings::Sparse(postings) = &decoded.postings else {
                        unreachable!("sparse compaction decoded BM25 postings")
                    };
                    let mut row_terms = vec![Vec::new(); decoded.rows.len()];
                    for posting in postings {
                        row_terms[posting.row as usize].push((posting.term, posting.value));
                    }
                    for (metadata, terms) in decoded.rows.iter().zip(row_terms) {
                        if terms.is_empty() {
                            continue;
                        }
                        let (indices, values): (Vec<_>, Vec<_>) = terms.into_iter().unzip();
                        vectors.insert(
                            (metadata.record_id.clone(), metadata.generation),
                            SparseVector::new(indices, values)?,
                        );
                    }
                }
            }
            for record in records.iter_mut() {
                let key = (record.id.as_bytes().to_vec(), record.generation);
                if let Some(vector) = vectors.get(&key) {
                    record.extra_sparse.insert(name.clone(), vector.clone());
                }
            }
        }
        Ok(())
    }

    fn read_graph(
        &self,
        summary: &SegmentSummary,
        segment: &Segment,
    ) -> Result<(Arc<SegmentGraph>, u64, bool, bool, bool)> {
        if let Some(graph) = self
            .segment_cache
            .get()
            .and_then(|cache| cache.get_graph(&summary.checksum, &summary.graph_checksum))
        {
            return Ok((graph, 0, false, false, true));
        }

        let mut cache_hit = false;
        let mut cache_repaired = false;
        let flight_key = format!("{}:{}", summary.checksum, summary.graph_checksum);
        let (graph, bytes_read, shared_inflight) =
            self.inflight_graph_reads.load(&flight_key, || {
                let read = self.storage.read_bytes_with_cache_status_and_checksum(
                    &summary.graph_path,
                    &summary.graph_checksum,
                )?;
                let bytes_read = read.bytes.len() as u64;
                cache_hit = read.cache_hit;
                cache_repaired = read.cache_repaired;
                validate_object_size(
                    "graph",
                    &summary.graph_path,
                    summary.graph_size_bytes,
                    bytes_read,
                )?;

                let graph =
                    graph_from_parquet(&read.bytes, &summary.id, summary.level, &segment.records)?;
                validate_graph_record_references(
                    &summary.graph_path,
                    segment,
                    &graph,
                    self.manifest.graph_neighbors,
                )?;
                Ok((graph, bytes_read))
            })?;
        // An overlapping follower performed no storage read. Treat its shared
        // immutable graph as a memory hit rather than a second object miss.
        cache_hit |= shared_inflight;
        if shared_inflight {
            cache_repaired = false;
        }
        if let Some(cache) = self.segment_cache.get() {
            cache.insert_graph(
                &summary.checksum,
                summary.graph_checksum.clone(),
                Arc::clone(&graph),
                decoded_graph_bytes(&graph),
            );
        }

        Ok((graph, bytes_read, cache_hit, cache_repaired, false))
    }

    fn validate_vector(&self, vector: &[f32]) -> Result<()> {
        if vector.len() != self.manifest.config.dimensions {
            return Err(BorsukError::DimensionMismatch {
                expected: self.manifest.config.dimensions,
                actual: vector.len(),
            });
        }

        if let Some((coordinate_index, value)) = vector
            .iter()
            .copied()
            .enumerate()
            .find(|(_, value)| !value.is_finite())
        {
            return Err(BorsukError::InvalidMetricInput(format!(
                "vectors must contain only finite f32 values; coordinate {coordinate_index} was {value}"
            )));
        }

        Ok(())
    }

    /// Reject a search that requests a leaf mode the index was not built for.
    ///
    /// A `PqScanOnly` index skips graph construction, so a graph-backed leaf
    /// mode has no graph to read; fail fast with a typed error rather than
    /// silently degrading or reading a missing object.
    fn validate_leaf_capability(&self, leaf_mode: LeafMode) -> Result<()> {
        let capability = self.manifest.leaf_capability;
        if capability.allows_leaf_mode(leaf_mode) {
            Ok(())
        } else {
            Err(BorsukError::LeafModeNotConfigured {
                requested: leaf_mode,
                capability,
            })
        }
    }

    fn effective_ram_budget_bytes(&self) -> Option<u64> {
        effective_ram_budget_bytes(
            self.manifest.config.ram_budget_bytes,
            self.runtime_ram_budget_bytes,
        )
    }
}

/// Split locality-ordered records into output segments. Without a radius cap this
/// is a plain count chunker. With a radius cap it is spread-aware: it closes a
/// segment as soon as the next record would sit farther than `max_radius` from the
/// running centroid, so a dispersed cluster becomes several tight, small-radius
/// bubbles that prune far better than one large bubble. The count cap still bounds
/// each segment.
fn adaptive_chunks(
    records: Vec<VectorRecord>,
    metric: &VectorMetric,
    max_vectors: usize,
    max_radius: Option<f32>,
) -> Result<Vec<Vec<VectorRecord>>> {
    let Some(max_radius) = max_radius else {
        return Ok(records
            .chunks(max_vectors)
            .map(<[VectorRecord]>::to_vec)
            .collect());
    };

    let mut chunks: Vec<Vec<VectorRecord>> = Vec::new();
    let mut current: Vec<VectorRecord> = Vec::new();
    let mut centroid: Vec<f32> = Vec::new();
    for record in records {
        let exceeds_count = current.len() >= max_vectors;
        let normalized;
        let geometry_vector = if metric.uses_normalized_euclidean_geometry() {
            normalized = crate::metric::unit_l2_normalized(&record.vector);
            normalized.as_slice()
        } else {
            &record.vector
        };
        // `centroid` is derived from and `geometry_vector` normalized from stored,
        // already-validated record vectors — skip the finite/dim re-scan here.
        let exceeds_radius = !current.is_empty()
            && metric.centroid_geometry_distance_unchecked(&centroid, geometry_vector)?
                > max_radius;
        if !current.is_empty() && (exceeds_count || exceeds_radius) {
            chunks.push(std::mem::take(&mut current));
            centroid.clear();
        }
        if centroid.is_empty() {
            centroid = geometry_vector.to_vec();
        } else {
            let count = current.len() as f32;
            crate::metric::online_mean_assign_simd(&mut centroid, geometry_vector, count);
        }
        current.push(record);
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    Ok(chunks)
}

/// Lloyd iterations for the k-means Voronoi partition. A handful converges the
/// coarse cell shapes; more barely moves recall.
const VORONOI_KMEANS_ITERS: usize = 20;

/// Branching factor per clustering level. Instead of one flat k-means into
/// `n / max_vectors` cells — whose assignment step is O(n·k) ≈ O(n²/max_vectors),
/// quadratic in the corpus and the reason full compaction crawled — we split
/// into at most `VORONOI_FANOUT` cells and recurse. That makes each level O(n·F)
/// and the whole partition O(n·log_F(n)) — near-linear (hierarchical k-means,
/// the FAISS IMI approach). A wider fanout keeps cell quality (and recall) close
/// to flat k-means while staying near-linear; 32 is the recall/speed sweet spot.
const VORONOI_FANOUT: usize = 32;

/// Stop Lloyd iterations once the summed squared centroid movement drops below
/// this — k-means++ init usually converges well before the iteration cap.
const VORONOI_KMEANS_CONVERGENCE: f32 = 1.0e-5;

/// The clustering knobs from [`BuildConfig`], resolved into the concrete values
/// `voronoi_chunks` uses. Carried by reference through the recursion so every
/// level uses the same policy.
#[derive(Debug, Clone, Copy)]
struct KmeansParams {
    /// Fraction of points used to FIT centroids, in `(0, 1]`. Below `1.0` the
    /// Lloyd iterations run on a deterministic uniform subsample and then ALL
    /// points are assigned to the fitted centroids.
    sample_fraction: f32,
    /// Lloyd iteration cap per clustering level.
    max_iterations: usize,
}

impl KmeansParams {
    fn from_build_config(build: &BuildConfig) -> Self {
        Self {
            sample_fraction: build.effective_kmeans_sample_fraction(),
            max_iterations: build.kmeans_max_iterations.unwrap_or(VORONOI_KMEANS_ITERS),
        }
    }
}

impl Default for KmeansParams {
    fn default() -> Self {
        Self {
            sample_fraction: 1.0,
            max_iterations: VORONOI_KMEANS_ITERS,
        }
    }
}

/// Deterministically pick a uniform subsample of `input_len` point indices whose
/// centroids will be FIT (then all points are assigned). Seeded on `input_len`
/// so a fixed corpus and fraction always select the same fit set — clustering,
/// and therefore the whole compaction, stays reproducible.
///
/// Returns `None` when the fraction keeps every point (fit on all — the
/// historical path), or when the subsample would be too small to seed `k`
/// centroids, so tiny cells never lose fit quality.
fn kmeans_fit_subsample(input_len: usize, k: usize, fraction: f32) -> Option<Vec<usize>> {
    if fraction >= 1.0 || input_len == 0 {
        return None;
    }
    let target = ((input_len as f64) * (fraction as f64)).ceil() as usize;
    let target = target.clamp(1, input_len);
    // Need at least `k` points to seed `k` distinct centroids; below that, fit on
    // everything (the subsample would just degrade an already-small cell).
    if target >= input_len || target < k {
        return None;
    }
    // Deterministic Fisher–Yates partial shuffle over the index space, seeded on
    // the input length so the same corpus+fraction always yields the same set.
    let mut indices: Vec<usize> = (0..input_len).collect();
    let mut state = 0x243F_6A88_85A3_08D3_u64 ^ (input_len as u64).wrapping_mul(0x9E37_79B9);
    for slot in 0..target {
        let pick = slot + splitmix_index(&mut state, input_len - slot);
        indices.swap(slot, pick);
    }
    indices.truncate(target);
    // Sort so the fit set is presented in the original point order, keeping the
    // downstream k-means++ seeding (which is order-sensitive) deterministic.
    indices.sort_unstable();
    Some(indices)
}

/// Partition records into Voronoi cells by k-means, so each output segment is a
/// tight cluster whose centroid is representative.
///
/// This is what makes approximate search cheap in high dimensions. Locality
/// chunking (`adaptive_chunks`) slices vectors into axis-aligned slabs; in 100+
/// dimensions those slabs scatter a query's true neighbours across many cells,
/// so probing the nearest few misses most of them and the query ends up reading
/// most of the index. k-means cells instead concentrate a query's neighbours in
/// its few nearest cells, so `nprobe` (max_segments) can read a small fixed
/// number of segments and still recover them.
///
/// Cells are emitted in centroid-locality order so the routing tree groups
/// neighbouring cells into the same page and its per-page bounds stay tight —
/// the paged-routing path depends on that ordering. Deterministic: k-means
/// seeding is a splitmix stream keyed on the record count, so compaction is
/// reproducible.
fn voronoi_chunks(
    records: Vec<VectorRecord>,
    metric: &VectorMetric,
    max_vectors: usize,
    max_radius: Option<f32>,
    kmeans: &KmeansParams,
) -> Result<Vec<Vec<VectorRecord>>> {
    let max_vectors = max_vectors.max(1);
    // Cosine/angular cluster on unit-L2-normalized vectors (spherical k-means);
    // other metrics cluster on the raw vector. This matches the geometry the
    // segment centroid and the coarse quantizer use.
    let normalize = metric.uses_normalized_euclidean_geometry();
    let geometry: Vec<Vec<f32>> = records
        .iter()
        .map(|record| {
            if normalize {
                crate::metric::unit_l2_normalized(&record.vector)
            } else {
                record.vector.clone()
            }
        })
        .collect();
    // No records to cluster (e.g. every record in the compaction source was
    // deleted): emit no cells rather than one empty cell, which would fail the
    // "segments must contain at least one record" invariant downstream.
    if records.is_empty() {
        return Ok(Vec::new());
    }
    // A cell small enough by count and tight enough by radius is emitted whole.
    if records.len() <= max_vectors
        && max_radius.is_none_or(|cap| geometry_radius(&geometry) <= cap)
    {
        return Ok(vec![records]);
    }
    let input_len = geometry.len();
    let dimensions = geometry[0].len();
    // Bounded branching factor: split into at most VORONOI_FANOUT cells here and
    // let the recursion below reach `max_vectors`-sized leaves — hierarchical
    // k-means, near-linear in `input_len` (see VORONOI_FANOUT).
    let k = input_len
        .div_ceil(max_vectors)
        .clamp(2, VORONOI_FANOUT.max(2));

    // When a sub-1.0 sample fraction is configured, FIT the centroids on a
    // deterministic uniform subsample; then ALL points are assigned to those
    // centroids below. On the default (fraction 1.0) the fit set IS the full
    // geometry, so the Lloyd loop is byte-identical to the historical path.
    let fit_indices = kmeans_fit_subsample(input_len, k, kmeans.sample_fraction);
    let fit_geometry: Vec<Vec<f32>> = match &fit_indices {
        Some(indices) => indices.iter().map(|&i| geometry[i].clone()).collect(),
        None => geometry.clone(),
    };
    let fit_len = fit_geometry.len();

    let mut centroids = kmeans_plus_plus_init(&fit_geometry, k);
    let mut assignment = vec![0_usize; fit_len];
    let mut nearest_distance = vec![0.0_f32; fit_len];
    for _ in 0..kmeans.max_iterations {
        // The nearest-centroid assignment is independent per point and writes
        // disjoint index-keyed slots, so it parallelizes deterministically; the
        // reseed/update step below stays serial.
        assign_nearest_centroids(
            &fit_geometry,
            &centroids,
            &mut assignment,
            &mut nearest_distance,
        );
        let mut sums = vec![vec![0.0_f32; dimensions]; k];
        let mut counts = vec![0_usize; k];
        for (index, vector) in fit_geometry.iter().enumerate() {
            let cluster = assignment[index];
            counts[cluster] += 1;
            crate::metric::add_assign_simd(&mut sums[cluster], vector);
        }
        let mut movement = 0.0_f32;
        for cluster in 0..k {
            if counts[cluster] == 0 {
                // Reseed an empty cluster on the worst-served point so k-means
                // does not collapse to fewer cells than requested.
                if let Some(farthest) = (0..fit_len)
                    .max_by(|&a, &b| nearest_distance[a].total_cmp(&nearest_distance[b]))
                {
                    centroids[cluster] = fit_geometry[farthest].clone();
                    nearest_distance[farthest] = 0.0;
                    movement = f32::INFINITY;
                }
            } else {
                let count = counts[cluster] as f32;
                for (value, sum) in centroids[cluster].iter_mut().zip(&sums[cluster]) {
                    let updated = sum / count;
                    movement += (updated - *value) * (updated - *value);
                    *value = updated;
                }
            }
        }
        if movement <= VORONOI_KMEANS_CONVERGENCE {
            break;
        }
    }

    let mut groups: Vec<Vec<VectorRecord>> = vec![Vec::new(); k];
    for (index, record) in records.into_iter().enumerate() {
        let (nearest, _) = nearest_centroid(&geometry[index], &centroids);
        groups[nearest].push(record);
    }

    let mut output: Vec<Vec<VectorRecord>> = Vec::new();
    for group in groups {
        if group.is_empty() {
            continue;
        }
        let over_count = group.len() > max_vectors;
        let over_radius = max_radius.is_some_and(|cap| group_radius(&group, normalize) > cap);
        if group.len() > 1 && (over_count || over_radius) {
            if group.len() == input_len {
                // No spatial progress (e.g. identical vectors landed in one
                // cell) — slice sequentially so recursion terminates.
                for slice in group.chunks(max_vectors) {
                    output.push(slice.to_vec());
                }
            } else {
                output.extend(voronoi_chunks(
                    group,
                    metric,
                    max_vectors,
                    max_radius,
                    kmeans,
                )?);
            }
        } else {
            output.push(group);
        }
    }

    // Order cells by their centroid's locality key so the routing tree pages
    // group neighbouring cells (tight page bounds).
    let mut keyed: Vec<_> = output
        .into_iter()
        .map(|cell| {
            let key = cell_centroid_locality_key(&cell, normalize, dimensions);
            (key, cell)
        })
        .collect();
    keyed.sort_by_key(|(key, _)| *key);
    Ok(keyed.into_iter().map(|(_, cell)| cell).collect())
}

/// The locality key of a cell's centroid (mean vector), in the same normalized
/// geometry the cell was clustered in. Used to order cells so nearby cells sit
/// next to each other for routing-page grouping.
fn cell_centroid_locality_key(
    cell: &[VectorRecord],
    normalize: bool,
    dimensions: usize,
) -> [i32; VECTOR_LOCALITY_KEY_LEN] {
    let mut centroid = vec![0.0_f32; dimensions];
    for record in cell {
        if normalize {
            let normalized = crate::metric::unit_l2_normalized(&record.vector);
            crate::metric::add_assign_simd(&mut centroid, &normalized);
        } else {
            crate::metric::add_assign_simd(&mut centroid, &record.vector);
        }
    }
    let count = cell.len().max(1) as f32;
    crate::metric::divide_assign_simd(&mut centroid, count);
    vector_locality_key(&centroid)
}

/// The radius of a cell in geometry space: the largest distance from any point
/// to the cell centroid. Used to honour `target_segment_max_radius`.
fn geometry_radius(geometry: &[Vec<f32>]) -> f32 {
    if geometry.is_empty() {
        return 0.0;
    }
    let dimensions = geometry[0].len();
    let mut centroid = vec![0.0_f32; dimensions];
    for vector in geometry {
        crate::metric::add_assign_simd(&mut centroid, vector);
    }
    let count = geometry.len() as f32;
    crate::metric::divide_assign_simd(&mut centroid, count);
    geometry
        .iter()
        .map(|vector| squared_distance(vector, &centroid).sqrt())
        .fold(0.0_f32, f32::max)
}

/// The radius of a cell of records, normalizing the same way clustering did.
fn group_radius(cell: &[VectorRecord], normalize: bool) -> f32 {
    let geometry: Vec<Vec<f32>> = cell
        .iter()
        .map(|record| {
            if normalize {
                crate::metric::unit_l2_normalized(&record.vector)
            } else {
                record.vector.clone()
            }
        })
        .collect();
    geometry_radius(&geometry)
}

/// Squared Euclidean distance between two equal-length vectors.
///
/// Routes k-means clustering (`voronoi_chunks`: seeding, Lloyd assignment, cell
/// radii) through the shared SIMD kernel (`f32x8` bulk + scalar tail) so every
/// squared-Euclidean computation in the engine reduces in the same lane+tail
/// order. Deterministic per target — the reduction order is fixed, so a fixed
/// config+data still partitions identically build-to-build. Clustering inputs
/// are always equal-length, satisfying the kernel's contract.
fn squared_distance(a: &[f32], b: &[f32]) -> f32 {
    crate::metric::squared_euclidean_simd(a, b)
}

/// Below this point count, a serial nearest-centroid pass is cheaper than paying
/// thread-spawn overhead. Above it, the independent per-point work is split
/// across threads. Only affects scheduling, never the produced assignment.
const KMEANS_ASSIGN_PARALLEL_THRESHOLD: usize = 4096;

/// Assign every point in `geometry` to its nearest centroid, writing the chosen
/// cluster and squared distance into `assignment`/`nearest_distance` at the
/// point's index. Each point is an independent, pure function of the read-only
/// `geometry` and `centroids`, and every output slot is written by exactly one
/// worker keyed on the point index — so the result is byte-for-byte identical to
/// a serial loop regardless of thread scheduling.
fn assign_nearest_centroids(
    geometry: &[Vec<f32>],
    centroids: &[Vec<f32>],
    assignment: &mut [usize],
    nearest_distance: &mut [f32],
) {
    let point_count = geometry.len();
    let thread_count = if point_count < KMEANS_ASSIGN_PARALLEL_THRESHOLD {
        1
    } else {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
            .min(crate::configured_cpu_threads())
            .min(point_count)
            .max(1)
    };

    if thread_count == 1 {
        for (index, vector) in geometry.iter().enumerate() {
            let (nearest, distance) = nearest_centroid(vector, centroids);
            assignment[index] = nearest;
            nearest_distance[index] = distance;
        }
        return;
    }

    let chunk_len = point_count.div_ceil(thread_count);
    std::thread::scope(|scope| {
        let mut geo_rest = geometry;
        let mut assign_rest = assignment;
        let mut dist_rest = nearest_distance;
        while !geo_rest.is_empty() {
            let take = chunk_len.min(geo_rest.len());
            let (geo_chunk, geo_next) = geo_rest.split_at(take);
            let (assign_chunk, assign_next) = assign_rest.split_at_mut(take);
            let (dist_chunk, dist_next) = dist_rest.split_at_mut(take);
            geo_rest = geo_next;
            assign_rest = assign_next;
            dist_rest = dist_next;
            scope.spawn(move || {
                for ((vector, assign_slot), dist_slot) in geo_chunk
                    .iter()
                    .zip(assign_chunk.iter_mut())
                    .zip(dist_chunk.iter_mut())
                {
                    let (nearest, distance) = nearest_centroid(vector, centroids);
                    *assign_slot = nearest;
                    *dist_slot = distance;
                }
            });
        }
    });
}

/// The nearest centroid to `vector` and its squared distance.
fn nearest_centroid(vector: &[f32], centroids: &[Vec<f32>]) -> (usize, f32) {
    let mut best = 0_usize;
    let mut best_distance = f32::INFINITY;
    for (index, centroid) in centroids.iter().enumerate() {
        let distance = squared_distance(vector, centroid);
        if distance < best_distance {
            best_distance = distance;
            best = index;
        }
    }
    (best, best_distance)
}

/// k-means++ seeding: pick `k` initial centroids spread across the data by
/// distance-weighted sampling. Uses a splitmix64 stream keyed on the point count
/// so the same data always seeds the same centroids (deterministic compaction).
fn kmeans_plus_plus_init(geometry: &[Vec<f32>], k: usize) -> Vec<Vec<f32>> {
    let mut state = 0x9E37_79B9_7F4A_7C15_u64 ^ (geometry.len() as u64);
    let first = splitmix_index(&mut state, geometry.len());
    let mut centroids = vec![geometry[first].clone()];
    let mut distances: Vec<f32> = geometry
        .iter()
        .map(|vector| squared_distance(vector, &centroids[0]))
        .collect();
    while centroids.len() < k {
        let total: f32 = distances.iter().sum();
        let chosen = if total <= 0.0 {
            splitmix_index(&mut state, geometry.len())
        } else {
            let mut target = splitmix_unit(&mut state) as f32 * total;
            let mut picked = geometry.len() - 1;
            for (index, distance) in distances.iter().enumerate() {
                target -= distance;
                if target <= 0.0 {
                    picked = index;
                    break;
                }
            }
            picked
        };
        let latest = geometry[chosen].clone();
        for (distance, vector) in distances.iter_mut().zip(geometry) {
            *distance = distance.min(squared_distance(vector, &latest));
        }
        centroids.push(latest);
    }
    centroids
}

fn splitmix_next(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn splitmix_index(state: &mut u64, len: usize) -> usize {
    (splitmix_next(state) % len as u64) as usize
}

struct PendingGlobalPqChunk {
    cell_index: u16,
    row_start: usize,
    chunk: crate::global_pq_sidecar::GlobalPqChunkBytes,
}

struct GlobalPqBundleSlice {
    code_range: Range<usize>,
    exact_range: Range<usize>,
}

struct EncodedGlobalPqBundle {
    bytes: Vec<u8>,
    slices: Vec<GlobalPqBundleSlice>,
}

fn encode_global_pq_arrow_bundle(
    pending: &[PendingGlobalPqChunk],
    code_width: usize,
    location: LocationEncoding,
    dimensions: usize,
    element_type: crate::record::VectorElementType,
) -> Result<EncodedGlobalPqBundle> {
    if pending.is_empty() {
        return Err(BorsukError::InvalidStorage(
            "global PQ bundle cannot be empty".to_string(),
        ));
    }
    let scan_row_width = code_width
        .checked_add(location.width_bytes())
        .ok_or_else(|| {
            BorsukError::InvalidStorage("global PQ Arrow scan width overflows".to_string())
        })?;
    let scan_type =
        arrow_schema::DataType::FixedSizeBinary(i32::try_from(scan_row_width).map_err(|_| {
            BorsukError::InvalidStorage("global PQ Arrow scan width exceeds i32".to_string())
        })?);
    let exact_type = crate::arrow_vector_sidecar::vector_data_type(element_type, dimensions)?;
    let schema = Arc::new(arrow_schema::Schema::new_with_metadata(
        vec![
            arrow_schema::Field::new("scan_payload", scan_type, false),
            arrow_schema::Field::new("exact_vector", exact_type, false),
        ],
        HashMap::from([
            ("borsuk.ann.code_width".to_string(), code_width.to_string()),
            (
                "borsuk.ann.location_width".to_string(),
                location.width_bytes().to_string(),
            ),
            (
                "borsuk.vector.dimensions".to_string(),
                dimensions.to_string(),
            ),
            (
                "borsuk.vector.element_type".to_string(),
                element_type.as_str().to_string(),
            ),
        ]),
    ));
    let mut bytes = Vec::new();
    {
        let mut writer = arrow_ipc::writer::FileWriter::try_new_with_options(
            &mut bytes,
            &schema,
            arrow_ipc::writer::IpcWriteOptions::default(),
        )?;
        for entry in pending {
            let expected_scan = entry
                .chunk
                .rows
                .checked_mul(scan_row_width)
                .ok_or_else(|| {
                    BorsukError::InvalidStorage("global PQ Arrow scan size overflows".to_string())
                })?;
            if entry.chunk.bytes.len() != expected_scan {
                return Err(BorsukError::InvalidStorage(format!(
                    "global PQ scan payload has {} bytes, expected {expected_scan}",
                    entry.chunk.bytes.len()
                )));
            }
            let scan = arrow_array::FixedSizeBinaryArray::try_from_iter(
                entry.chunk.bytes.chunks_exact(scan_row_width),
            )?;
            let exact = global_pq_exact_arrow_array(
                &entry.chunk.exact_bytes,
                entry.chunk.rows,
                dimensions,
                element_type,
            )?;
            let batch = arrow_array::RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(scan), exact],
            )?;
            writer.write(&batch)?;
        }
        writer.finish()?;
    }
    let slices = global_pq_arrow_buffer_ranges(&bytes, pending.len())?;
    Ok(EncodedGlobalPqBundle { bytes, slices })
}

fn global_pq_exact_arrow_array(
    bytes: &[u8],
    rows: usize,
    dimensions: usize,
    element_type: crate::record::VectorElementType,
) -> Result<Arc<dyn arrow_array::Array>> {
    use arrow_array::types::{Float16Type, Float32Type, Int8Type, UInt8Type, UInt16Type};

    let row_bytes = element_type.fixed_width_bytes(dimensions)?;
    let expected = rows.checked_mul(row_bytes).ok_or_else(|| {
        BorsukError::InvalidStorage("global PQ exact Arrow size overflows".to_string())
    })?;
    if bytes.len() != expected {
        return Err(BorsukError::InvalidStorage(format!(
            "global PQ exact payload has {} bytes, expected {expected}",
            bytes.len()
        )));
    }
    let list_size = i32::try_from(dimensions).map_err(|_| {
        BorsukError::InvalidStorage("global PQ exact dimensions exceed i32".to_string())
    })?;
    let array: Arc<dyn arrow_array::Array> = match element_type {
        crate::record::VectorElementType::Float32 => {
            Arc::new(arrow_array::FixedSizeListArray::from_iter_primitive::<
                Float32Type,
                _,
                _,
            >(
                bytes.chunks_exact(row_bytes).map(|row| {
                    Some(
                        row.chunks_exact(4)
                            .map(|value| {
                                Some(f32::from_le_bytes(value.try_into().expect("four bytes")))
                            })
                            .collect::<Vec<_>>(),
                    )
                }),
                list_size,
            ))
        }
        crate::record::VectorElementType::Float16 => {
            Arc::new(arrow_array::FixedSizeListArray::from_iter_primitive::<
                Float16Type,
                _,
                _,
            >(
                bytes.chunks_exact(row_bytes).map(|row| {
                    Some(
                        row.chunks_exact(2)
                            .map(|value| {
                                Some(half::f16::from_bits(u16::from_le_bytes(
                                    value.try_into().expect("two bytes"),
                                )))
                            })
                            .collect::<Vec<_>>(),
                    )
                }),
                list_size,
            ))
        }
        crate::record::VectorElementType::BFloat16 => {
            Arc::new(arrow_array::FixedSizeListArray::from_iter_primitive::<
                UInt16Type,
                _,
                _,
            >(
                bytes.chunks_exact(row_bytes).map(|row| {
                    Some(
                        row.chunks_exact(2)
                            .map(|value| {
                                Some(u16::from_le_bytes(value.try_into().expect("two bytes")))
                            })
                            .collect::<Vec<_>>(),
                    )
                }),
                list_size,
            ))
        }
        crate::record::VectorElementType::Float8E4M3Fn
        | crate::record::VectorElementType::Float8E5M2 => {
            Arc::new(arrow_array::FixedSizeListArray::from_iter_primitive::<
                UInt8Type,
                _,
                _,
            >(
                bytes
                    .chunks_exact(row_bytes)
                    .map(|row| Some(row.iter().copied().map(Some).collect::<Vec<_>>())),
                list_size,
            ))
        }
        crate::record::VectorElementType::Int8 => {
            Arc::new(arrow_array::FixedSizeListArray::from_iter_primitive::<
                Int8Type,
                _,
                _,
            >(
                bytes.chunks_exact(row_bytes).map(|row| {
                    Some(
                        row.iter()
                            .copied()
                            .map(|value| Some(value as i8))
                            .collect::<Vec<_>>(),
                    )
                }),
                list_size,
            ))
        }
        crate::record::VectorElementType::Binary => Arc::new(
            arrow_array::FixedSizeBinaryArray::try_from_iter(bytes.chunks_exact(row_bytes))?,
        ),
    };
    Ok(array)
}

fn global_pq_arrow_buffer_ranges(
    bytes: &[u8],
    expected_batches: usize,
) -> Result<Vec<GlobalPqBundleSlice>> {
    if bytes.len() < 10 {
        return Err(BorsukError::InvalidStorage(
            "global PQ Arrow bundle is shorter than its trailer".to_string(),
        ));
    }
    let trailer: [u8; 10] = bytes[bytes.len() - 10..].try_into().map_err(|_| {
        BorsukError::InvalidStorage("global PQ Arrow trailer is truncated".to_string())
    })?;
    let footer_len = arrow_ipc::reader::read_footer_length(trailer)?;
    let footer_end = bytes.len() - 10;
    let footer_start = footer_end.checked_sub(footer_len).ok_or_else(|| {
        BorsukError::InvalidStorage("global PQ Arrow footer is truncated".to_string())
    })?;
    let footer = arrow_ipc::root_as_footer(&bytes[footer_start..footer_end]).map_err(|error| {
        BorsukError::InvalidStorage(format!("global PQ Arrow footer is invalid: {error}"))
    })?;
    let blocks = footer.recordBatches().ok_or_else(|| {
        BorsukError::InvalidStorage("global PQ Arrow footer has no batches".to_string())
    })?;
    if blocks.len() != expected_batches {
        return Err(BorsukError::InvalidStorage(format!(
            "global PQ Arrow has {} batches, expected {expected_batches}",
            blocks.len()
        )));
    }
    blocks
        .iter()
        .map(|block| {
            let block_start = usize::try_from(block.offset()).map_err(|_| {
                BorsukError::InvalidStorage(
                    "global PQ Arrow batch has a negative offset".to_string(),
                )
            })?;
            let metadata_len = usize::try_from(block.metaDataLength()).map_err(|_| {
                BorsukError::InvalidStorage(
                    "global PQ Arrow batch has a negative metadata length".to_string(),
                )
            })?;
            let metadata_end = block_start.checked_add(metadata_len).ok_or_else(|| {
                BorsukError::InvalidStorage("global PQ Arrow metadata range overflows".to_string())
            })?;
            let metadata = bytes.get(block_start..metadata_end).ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "global PQ Arrow metadata range is outside the file".to_string(),
                )
            })?;
            let message_bytes = if metadata.starts_with(&[0xff, 0xff, 0xff, 0xff]) {
                metadata.get(8..)
            } else {
                metadata.get(4..)
            }
            .ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "global PQ Arrow metadata prefix is truncated".to_string(),
                )
            })?;
            let message = arrow_ipc::root_as_message(message_bytes).map_err(|error| {
                BorsukError::InvalidStorage(format!(
                    "global PQ Arrow record batch metadata is invalid: {error}"
                ))
            })?;
            let batch = message.header_as_record_batch().ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "global PQ Arrow block is not a record batch".to_string(),
                )
            })?;
            if batch.compression().is_some() {
                return Err(BorsukError::InvalidStorage(
                    "global PQ Arrow range layout must be uncompressed".to_string(),
                ));
            }
            let buffers = batch.buffers().ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "global PQ Arrow record batch has no buffers".to_string(),
                )
            })?;
            if buffers.len() < 4 {
                return Err(BorsukError::InvalidStorage(
                    "global PQ Arrow record batch has too few buffers".to_string(),
                ));
            }
            let body_len = usize::try_from(block.bodyLength()).map_err(|_| {
                BorsukError::InvalidStorage(
                    "global PQ Arrow batch has a negative body length".to_string(),
                )
            })?;
            let body_end = metadata_end.checked_add(body_len).ok_or_else(|| {
                BorsukError::InvalidStorage("global PQ Arrow body range overflows".to_string())
            })?;
            if body_end > bytes.len() {
                return Err(BorsukError::InvalidStorage(
                    "global PQ Arrow body range is outside the file".to_string(),
                ));
            }
            let buffer_range = |buffer: &arrow_ipc::Buffer| -> Result<Range<usize>> {
                let offset = usize::try_from(buffer.offset()).map_err(|_| {
                    BorsukError::InvalidStorage(
                        "global PQ Arrow buffer has a negative offset".to_string(),
                    )
                })?;
                let length = usize::try_from(buffer.length()).map_err(|_| {
                    BorsukError::InvalidStorage(
                        "global PQ Arrow buffer has a negative length".to_string(),
                    )
                })?;
                let start = metadata_end.checked_add(offset).ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "global PQ Arrow buffer range overflows".to_string(),
                    )
                })?;
                let end = start.checked_add(length).ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "global PQ Arrow buffer range overflows".to_string(),
                    )
                })?;
                if end > body_end {
                    return Err(BorsukError::InvalidStorage(
                        "global PQ Arrow buffer exceeds its batch body".to_string(),
                    ));
                }
                Ok(start..end)
            };
            Ok(GlobalPqBundleSlice {
                code_range: buffer_range(buffers.get(1))?,
                exact_range: buffer_range(buffers.get(buffers.len() - 1))?,
            })
        })
        .collect()
}

fn global_pq_code_read_wave_end(
    chunks: &[GlobalPqChunkRef],
    start: usize,
    max_chunks: usize,
    max_bytes: usize,
) -> usize {
    debug_assert!(start < chunks.len());
    let hard_end = start.saturating_add(max_chunks.max(1)).min(chunks.len());
    let mut bytes = 0_usize;
    let mut end = start;
    while end < hard_end {
        let next = bytes.saturating_add(chunks[end].size_bytes);
        if end > start && next > max_bytes.max(1) {
            break;
        }
        bytes = next;
        end += 1;
    }
    end.max(start + 1)
}

fn should_flush_global_pq_bundle(
    previous_cell: Option<u16>,
    next_cell: u16,
    parent_contiguous: bool,
    next_code_bytes: usize,
    next_total_bytes: usize,
) -> bool {
    previous_cell.is_some_and(|previous| {
        (parent_contiguous && previous.to_be_bytes()[0] != next_cell.to_be_bytes()[0])
            || next_code_bytes > DEFAULT_GLOBAL_PQ_BUNDLE_CODE_BYTES
            || next_total_bytes > DEFAULT_GLOBAL_PQ_BUNDLE_BYTES
    })
}

fn global_pq_code_read_groups(
    chunks: &[GlobalPqChunkRef],
    max_gap_bytes: usize,
    request_weight_bytes: usize,
) -> Result<Vec<(String, Vec<GlobalPqChunkRef>)>> {
    let mut chunks_by_path = BTreeMap::<String, Vec<GlobalPqChunkRef>>::new();
    for chunk in chunks {
        chunks_by_path
            .entry(chunk.path.clone())
            .or_default()
            .push(chunk.clone());
    }

    let mut groups = Vec::new();
    for (path, mut path_chunks) in chunks_by_path {
        path_chunks.sort_unstable_by(|left, right| {
            left.offset_bytes
                .cmp(&right.offset_bytes)
                .then_with(|| left.cell_index.cmp(&right.cell_index))
                .then_with(|| left.row_start.cmp(&right.row_start))
        });
        let requested = path_chunks
            .iter()
            .map(|chunk| {
                let end = chunk
                    .offset_bytes
                    .checked_add(chunk.size_bytes)
                    .ok_or_else(|| {
                        BorsukError::InvalidStorage(
                            "global PQ bundled code range overflows".to_string(),
                        )
                    })?;
                Ok(chunk.offset_bytes..end)
            })
            .collect::<Result<Vec<_>>>()?;
        let plan = crate::global_read_planner::plan_byte_ranges(
            &requested,
            max_gap_bytes,
            request_weight_bytes,
        )?;
        let mut chunk_index = 0usize;
        for span in plan.ranges {
            let start = chunk_index;
            while chunk_index < path_chunks.len()
                && path_chunks[chunk_index].offset_bytes < span.end
            {
                chunk_index += 1;
            }
            if start < chunk_index {
                groups.push((path.clone(), path_chunks[start..chunk_index].to_vec()));
            }
        }
    }
    Ok(groups)
}

fn resident_global_pq_subspaces(
    dimensions: usize,
    vectors: usize,
    configured_code_bytes: Option<usize>,
) -> usize {
    let padded = dimensions.next_power_of_two();
    if let Some(code_bytes) = configured_code_bytes {
        return code_bytes.min(padded);
    }
    // 96–128D corpora use two rotated coordinates per codeword. A measured
    // one-coordinate/128-byte GloVe layout doubled scan bytes/CPU without a
    // quality need, so those corpora remain at 64 bytes. At 256–512D and at
    // least 100K rows, the fresh NYTimes curve selects 128 bytes: its explicit
    // 256-byte control adds CPU/bytes without recall. GIST is different: at
    // 960D, code128 needs 32 probes and 608 exact rows merely to reach 0.985,
    // while code256 reaches 0.995 at 24 probes / 96 rows with unchanged build
    // RSS and only 1.5% more index bytes. Select that measured high-dimensional
    // regime without widening ordinary or low-dimensional corpora.
    let cap = if dimensions >= 768 && vectors >= 100_000 {
        256
    } else if dimensions >= 256 && vectors >= 100_000 {
        128
    } else {
        64
    };
    let coordinates_per_codeword = if dimensions <= 512 { 2 } else { 4 };
    padded
        .div_ceil(coordinates_per_codeword)
        .clamp(1, cap)
        .min(padded)
}

/// Select the second-level fan-out of the 64-way full-dimensional hierarchy.
/// The leaf-centroid table is capped at 32 MiB regardless of corpus size or
/// dimension, while ordinary million-row corpora get 1,024 cells, Deep-scale
/// corpora get 4,096, and 50M+ corpora get up to 16,384. This keeps cells small
/// enough for object-store reads without turning resident routing metadata into
/// a 100M-scale table.
fn resident_global_pq_coarse_children(dimensions: usize, vectors: usize) -> usize {
    let desired = if vectors < 250_000 {
        4
    } else if vectors < 5_000_000 {
        16
    } else if vectors < 50_000_000 {
        64
    } else {
        // Preserve roughly Deep-Image-scale rows/cell at 100M instead of
        // allowing corpus growth to multiply the bytes and ADC work per probe.
        // The 32 MiB centroid ceiling below remains authoritative for very wide
        // vectors, so this scaling does not become a resident-RAM hazard.
        256
    };
    bounded_global_pq_coarse_children(dimensions, desired)
}

fn bounded_global_pq_coarse_children(dimensions: usize, desired: usize) -> usize {
    const PARENTS: usize = 64;
    const MAX_RESIDENT_CENTROID_BYTES: usize = 32 * 1024 * 1024;
    let max_by_bytes =
        MAX_RESIDENT_CENTROID_BYTES / PARENTS / dimensions.max(1) / std::mem::size_of::<f32>();
    desired.min(max_by_bytes.max(1)).clamp(1, 256)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResolvedGlobalPqLayout {
    Product { subspaces: usize, centroids: usize },
    Hierarchical { children_per_parent: usize },
}

fn resolved_global_pq_layout(
    configured: &crate::GlobalPqLayout,
    metric: &VectorMetric,
    dimensions: usize,
    vectors: usize,
) -> ResolvedGlobalPqLayout {
    match configured {
        crate::GlobalPqLayout::Adaptive => {
            if let Some(subspaces) = resident_global_pq_product_coarse_subspaces(metric, vectors) {
                ResolvedGlobalPqLayout::Product {
                    subspaces,
                    centroids: if subspaces == 1 { 256 } else { 64 },
                }
            } else {
                ResolvedGlobalPqLayout::Hierarchical {
                    children_per_parent: resident_global_pq_coarse_children(dimensions, vectors),
                }
            }
        }
        crate::GlobalPqLayout::Flat256 => ResolvedGlobalPqLayout::Product {
            subspaces: 1,
            centroids: 256,
        },
        crate::GlobalPqLayout::Product2x64 => ResolvedGlobalPqLayout::Product {
            subspaces: 2,
            centroids: 64,
        },
        crate::GlobalPqLayout::Hierarchical {
            children_per_parent,
        } => ResolvedGlobalPqLayout::Hierarchical {
            children_per_parent: bounded_global_pq_coarse_children(
                dimensions,
                *children_per_parent,
            ),
        },
    }
}

/// Normalized/angular corpora retain flat full-dimensional 256 cells below 5M.
/// At larger scale the fresh Deep-Image curve qualifies the full-dimensional
/// hierarchy and rejects 2x64 product routing. Euclidean corpora use the same
/// bounded hierarchy, qualified independently by Fashion/SIFT.
#[cfg(test)]
fn resident_global_pq_uses_flat_coarse(
    metric: &VectorMetric,
    _dimensions: usize,
    vectors: usize,
) -> bool {
    resident_global_pq_product_coarse_subspaces(metric, vectors) == Some(1)
}

fn resident_global_pq_product_coarse_subspaces(
    metric: &VectorMetric,
    vectors: usize,
) -> Option<usize> {
    metric
        .uses_normalized_euclidean_geometry()
        .then_some(1)
        .filter(|_| vectors < 5_000_000)
}

/// Bound the dense coarse-training reservoir by both rows and bytes.
///
/// The 65,536-row ceiling preserves enough samples for low-dimensional
/// hierarchical cells. The 16 MiB byte ceiling leaves room for the rotated and
/// clustered working copies created while fitting, so an already-warm serving
/// process stays inside the default 512 MiB envelope. One vector is the
/// irreducible minimum; fitted codebooks reduce their centroid count when fewer
/// than 256 samples fit the budget.
fn global_pq_training_sample_limit(dimensions: usize) -> usize {
    const MAX_ROWS: usize = 65_536;
    const TARGET_BYTES: usize = 16 * 1024 * 1024;
    let bytes_per_vector = dimensions.max(1).saturating_mul(std::mem::size_of::<f32>());
    (TARGET_BYTES / bytes_per_vector).clamp(1, MAX_ROWS)
}

fn resident_global_pq_candidates(
    metric: &VectorMetric,
    dimensions: usize,
    subspaces: usize,
    vectors: usize,
) -> usize {
    let linear = subspaces.saturating_mul(3).saturating_sub(8).max(32);
    if dimensions >= 768 && subspaces >= 256 && vectors >= 100_000 {
        // The fresh GIST code256 sweep reaches 0.995 at 24 probes with only 96
        // lossless rows. 128..256 rows plateau at 0.996; 384 first reaches
        // 0.997 but crosses the production latency/request envelope. Keep that
        // wider point as the documented max-recall profile, not the default.
        96
    } else if metric.uses_normalized_euclidean_geometry()
        && (192..512).contains(&dimensions)
        && subspaces >= 128
    {
        // The fresh NYTimes-256 code128 sweep reached its 0.993 ceiling at
        // 288 candidates. 320..768 returned the identical neighbors while
        // increasing lossless page GETs and p95, so do not extrapolate the
        // lower-fidelity 5*m rule after doubling code fidelity.
        subspaces.saturating_mul(2).saturating_add(32)
    } else if dimensions >= 512 {
        // High-dimensional Euclidean corpora need substantially more exact
        // rerank headroom than the ordinary 3*m frontier. Fashion-MNIST first
        // matches the measured S3 Vectors recall around 5*m; million-row GIST
        // needs 6*m. This changes only bounded sidecar row reads, not resident
        // memory or the number of product codes scanned.
        let multiplier = if vectors >= 100_000 { 6 } else { 5 };
        linear.max(subspaces.saturating_mul(multiplier))
    } else if metric.uses_normalized_euclidean_geometry() && dimensions >= 192 {
        // Higher-dimensional angular corpora need a wider ADC shortlist than
        // Euclidean corpora at the same code width. Five rows per subspace
        // selects 320 rows for the measured 256-D/64-subspace NYTimes profile.
        linear.max(subspaces.saturating_mul(5))
    } else if metric.uses_normalized_euclidean_geometry() && vectors >= 5_000_000 {
        // Deep-Image-scale angular corpora need modest extra headroom even at
        // 96 dimensions. AWS sweeps restored the old recall at 100 rows; +8
        // over the ordinary 3*m rule gives 104 and a stable publication default.
        linear.max(subspaces.saturating_mul(3).saturating_add(8))
    } else {
        linear
    }
}

fn resident_global_pq_probes(metric: &VectorMetric, dimensions: usize, segments: usize) -> usize {
    if segments == 0 {
        return 0;
    }
    if metric.uses_normalized_euclidean_geometry() && dimensions == 256 && segments <= 256 {
        // The fine-grained NYTimes-256 code128 boundary sweep first reaches its
        // 0.993 ceiling at 223/256 cells. 221..222 remain at 0.989; all 256 add
        // latency and bytes without changing neighbors. Preserve that measured
        // fraction when a small build has fewer populated flat cells.
        return segments.saturating_mul(223).div_ceil(256).max(1);
    }
    if segments <= 256 && dimensions >= 512 {
        // A single, full-dimensional coarse codebook is sharply selective.
        // Fresh Fashion-MNIST v8 vector-level IVF measurements reached 0.990
        // recall at 8 probes; 16..128 added latency without recall.
        return 8.min(segments);
    }
    if dimensions >= 768 {
        // The fresh GIST code256 curve reaches 0.995 at 24 probes / 96 rows.
        // 32 probes add 25% disk-cached p95 for only +0.003 recall; 48 probes
        // do not improve recall at all. Keep routing work independent of the
        // square-root fallback for very wide, many-cell corpora.
        return 24.min(segments);
    }
    let base = if metric.uses_normalized_euclidean_geometry() && dimensions <= 128 {
        // GloVe's 256-cell full-dimensional layout needs roughly half the
        // cells for S3-Vectors-class recall. Deep-Image independently selects
        // 128 probes over its 4,096 full-dimensional hierarchical cells.
        128
    } else if dimensions >= 512 {
        32
    } else {
        24
    };
    let scale = ((segments as f64).sqrt().ceil() as usize).saturating_mul(2);
    base.max(scale).min(256).min(segments)
}

fn splitmix_unit(state: &mut u64) -> f64 {
    (splitmix_next(state) >> 11) as f64 / (1_u64 << 53) as f64
}

/// A record paired with its precomputed locality key. The kd-ordering sort uses
/// the key as a tie-breaker on every comparison; recomputing it (an O(dim *
/// projections) pass) inside the comparator dominated compaction on
/// high-dimensional data, so it is computed exactly once per record up front and
/// carried alongside the record as it is reordered. This is a pure hoist — the
/// keys and therefore the final ordering are identical to the recomputing path.
struct KeyedRecord {
    record: VectorRecord,
    locality_key: [i32; VECTOR_LOCALITY_KEY_LEN],
}

fn sort_records_by_vector_locality(
    records: &mut Vec<VectorRecord>,
    dimensions: usize,
    target_segment_max_vectors: usize,
) {
    let taken = std::mem::take(records);
    let mut keyed = keyed_records(taken);
    kd_order_records(&mut keyed, dimensions, target_segment_max_vectors.max(1));
    records.extend(keyed.into_iter().map(|entry| entry.record));
}

/// Pair every record with its locality key, computing the keys in parallel above
/// a size threshold (each key is an independent, index-keyed pure function of its
/// vector, so the result is identical regardless of scheduling).
fn keyed_records(records: Vec<VectorRecord>) -> Vec<KeyedRecord> {
    let mut keys = vec![[0_i32; VECTOR_LOCALITY_KEY_LEN]; records.len()];

    let thread_count = if records.len() < LOCALITY_KEY_PARALLEL_THRESHOLD {
        1
    } else {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
            .min(crate::configured_cpu_threads())
            .min(records.len())
            .max(1)
    };

    if thread_count == 1 {
        for (record, slot) in records.iter().zip(keys.iter_mut()) {
            *slot = vector_locality_key(&record.vector);
        }
    } else {
        let chunk_len = records.len().div_ceil(thread_count);
        std::thread::scope(|scope| {
            let mut record_rest = records.as_slice();
            let mut key_rest = keys.as_mut_slice();
            while !record_rest.is_empty() {
                let take = chunk_len.min(record_rest.len());
                let (record_chunk, record_next) = record_rest.split_at(take);
                let (key_chunk, key_next) = key_rest.split_at_mut(take);
                record_rest = record_next;
                key_rest = key_next;
                scope.spawn(move || {
                    for (record, slot) in record_chunk.iter().zip(key_chunk.iter_mut()) {
                        *slot = vector_locality_key(&record.vector);
                    }
                });
            }
        });
    }

    records
        .into_iter()
        .zip(keys)
        .map(|(record, locality_key)| KeyedRecord {
            record,
            locality_key,
        })
        .collect()
}

/// Below this record count a serial key pass is cheaper than thread-spawn cost.
const LOCALITY_KEY_PARALLEL_THRESHOLD: usize = 4096;

#[cfg(test)]
thread_local! {
    static KD_ORDER_COMPARISONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn compare_kd_entries(left: &KeyedRecord, right: &KeyedRecord, split_dimension: usize) -> Ordering {
    #[cfg(test)]
    KD_ORDER_COMPARISONS.with(|count| count.set(count.get() + 1));

    left.record.vector[split_dimension]
        .partial_cmp(&right.record.vector[split_dimension])
        .unwrap_or(Ordering::Equal)
        .then_with(|| {
            left.locality_key
                .cmp(&right.locality_key)
                .then_with(|| left.record.id.cmp(&right.record.id))
        })
}

fn kd_order_records(records: &mut [KeyedRecord], dimensions: usize, leaf_size: usize) {
    if records.len() <= leaf_size {
        sort_leaf_records(records);
        return;
    }

    let split_dimension = widest_dimension(records, dimensions);
    let split = aligned_split(records.len(), leaf_size);
    // Only the partition membership matters at an internal KD node; both
    // children are partitioned again and leaves receive the final total sort.
    // Selecting the exact split element preserves the same deterministic leaf
    // membership as a full sort while avoiding O(n log n) work at every level.
    records.select_nth_unstable_by(split, |left, right| {
        compare_kd_entries(left, right, split_dimension)
    });

    let (left, right) = records.split_at_mut(split);
    kd_order_records(left, dimensions, leaf_size);
    kd_order_records(right, dimensions, leaf_size);
}

fn sort_leaf_records(records: &mut [KeyedRecord]) {
    records.sort_by(|left, right| {
        left.locality_key
            .cmp(&right.locality_key)
            .then_with(|| left.record.id.cmp(&right.record.id))
    });
}

fn widest_dimension(records: &[KeyedRecord], dimensions: usize) -> usize {
    let mut best_dimension = 0_usize;
    let mut best_width = f32::NEG_INFINITY;
    for dimension in 0..dimensions {
        let mut min = f32::INFINITY;
        let mut max = f32::NEG_INFINITY;
        for entry in records {
            let value = entry.record.vector[dimension];
            min = min.min(value);
            max = max.max(value);
        }
        let width = max - min;
        if width > best_width {
            best_width = width;
            best_dimension = dimension;
        }
    }
    best_dimension
}

fn aligned_split(len: usize, leaf_size: usize) -> usize {
    let midpoint = len / 2;
    let lower = (midpoint / leaf_size) * leaf_size;
    let upper = lower.saturating_add(leaf_size);
    let mut split = if midpoint.saturating_sub(lower) <= upper.saturating_sub(midpoint) {
        lower
    } else {
        upper
    };
    if split == 0 {
        split = midpoint.max(1);
    }
    if split >= len {
        split = len - 1;
    }
    split
}

fn leaf_mode_for_segment_level(level: u8) -> LeafMode {
    if level == 0 {
        LeafMode::Graph
    } else {
        LeafMode::VamanaPq
    }
}

fn validate_object_size(kind: &str, path: &str, expected: u64, actual: u64) -> Result<()> {
    if actual == expected {
        return Ok(());
    }

    Err(BorsukError::InvalidStorage(format!(
        "{kind} object size mismatch for `{path}`: expected {expected} bytes, got {actual}"
    )))
}

fn validate_segment_metadata(
    summary: &SegmentSummary,
    segment: &Segment,
    expected_metric: &VectorMetric,
) -> Result<()> {
    validate_segment_metadata_field("id", &summary.path, &summary.id, &segment.id)?;
    validate_segment_metadata_field("level", &summary.path, summary.level, segment.level)?;
    validate_segment_metadata_field(
        "dimensions",
        &summary.path,
        summary.dimensions,
        segment.dimensions,
    )?;
    validate_segment_metadata_field("metric", &summary.path, expected_metric, &segment.metric)?;
    validate_segment_metadata_field(
        "centroid",
        &summary.path,
        summary.centroid.as_slice(),
        segment.centroid.as_slice(),
    )?;
    validate_segment_metadata_field("radius", &summary.path, summary.radius, segment.radius)?;
    validate_segment_object_count(&summary.path, summary.object_count, segment.records.len())?;

    Ok(())
}

fn validate_segment_metadata_field<T>(field: &str, path: &str, expected: T, actual: T) -> Result<()>
where
    T: PartialEq + std::fmt::Debug,
{
    if actual == expected {
        return Ok(());
    }

    Err(BorsukError::InvalidStorage(format!(
        "segment metadata {field} mismatch for `{path}`: expected {expected:?}, got {actual:?}"
    )))
}

fn validate_segment_object_count(path: &str, expected: usize, actual: usize) -> Result<()> {
    if actual == expected {
        return Ok(());
    }

    Err(BorsukError::InvalidStorage(format!(
        "segment object_count mismatch for `{path}`: expected {expected}, got {actual}"
    )))
}

fn validate_graph_record_references(
    path: &str,
    segment: &Segment,
    graph: &SegmentGraph,
    max_neighbors: usize,
) -> Result<()> {
    validate_graph_has_edges_for_multi_record_segment(path, segment, graph)?;

    let mut graph_edges = HashSet::with_capacity(graph.edges.len());
    let mut source_out_degree = HashMap::<usize, usize>::new();
    for edge in &graph.edges {
        validate_graph_edge_not_self_referential(path, edge)?;
        validate_graph_edge_not_duplicate(path, edge, &mut graph_edges)?;
        validate_graph_source_out_degree(path, edge, &mut source_out_degree, max_neighbors)?;
        let source = graph_edge_record(path, "source", edge.source_record_index, segment)?;
        let neighbor = graph_edge_record(path, "neighbor", edge.neighbor_record_index, segment)?;
        // Both operands are stored, already-validated segment vectors; this
        // recomputed edge distance must match the graph build, which now scores
        // through the unchecked kernel — use the same path so they stay identical.
        let expected_distance = segment
            .metric
            .distance_unchecked(&source.vector, &neighbor.vector)?;
        validate_graph_edge_distance(path, edge, expected_distance)?;
    }

    Ok(())
}

fn validate_graph_has_edges_for_multi_record_segment(
    path: &str,
    segment: &Segment,
    graph: &SegmentGraph,
) -> Result<()> {
    if segment.records.len() <= 1 || !graph.edges.is_empty() {
        return Ok(());
    }

    Err(BorsukError::InvalidStorage(format!(
        "graph table must contain at least one edge for multi-record segment in `{path}`"
    )))
}

fn validate_graph_source_out_degree(
    path: &str,
    edge: &crate::segment::GraphEdge,
    source_out_degree: &mut HashMap<usize, usize>,
    max_neighbors: usize,
) -> Result<()> {
    let count = source_out_degree
        .entry(edge.source_record_index)
        .or_default();
    *count += 1;
    if *count <= max_neighbors {
        return Ok(());
    }

    Err(BorsukError::InvalidStorage(format!(
        "graph source out-degree exceeds local limit in `{path}`: source index {} has {} edges, limit is {max_neighbors}",
        edge.source_record_index, *count
    )))
}

fn validate_graph_edge_not_duplicate(
    path: &str,
    edge: &crate::segment::GraphEdge,
    graph_edges: &mut HashSet<(usize, usize)>,
) -> Result<()> {
    if graph_edges.insert((edge.source_record_index, edge.neighbor_record_index)) {
        return Ok(());
    }

    Err(BorsukError::InvalidStorage(format!(
        "duplicate graph edge in `{path}`: {} -> {}",
        edge.source_record_index, edge.neighbor_record_index
    )))
}

fn validate_graph_edge_not_self_referential(
    path: &str,
    edge: &crate::segment::GraphEdge,
) -> Result<()> {
    if edge.source_record_index != edge.neighbor_record_index {
        return Ok(());
    }

    Err(BorsukError::InvalidStorage(format!(
        "graph edge self-reference in `{path}`: record index {}",
        edge.source_record_index
    )))
}

fn graph_edge_record<'a>(
    path: &str,
    role: &str,
    record_index: usize,
    segment: &'a Segment,
) -> Result<&'a VectorRecord> {
    if let Some(record) = segment.records.get(record_index) {
        return Ok(record);
    }

    Err(BorsukError::InvalidStorage(format!(
        "graph edge references missing segment record in `{path}`: {role} record index {record_index}"
    )))
}

fn validate_graph_edge_distance(
    path: &str,
    edge: &crate::segment::GraphEdge,
    expected: f32,
) -> Result<()> {
    let actual = edge.distance;
    let tolerance = 1e-5_f32 * expected.abs().max(actual.abs()).max(1.0);
    if (actual - expected).abs() <= tolerance {
        return Ok(());
    }

    Err(BorsukError::InvalidStorage(format!(
        "graph edge distance mismatch in `{path}`: edge {} -> {} expected {expected}, got {actual}",
        edge.source_record_index, edge.neighbor_record_index
    )))
}

fn records_from_ids_and_vectors(
    ids: Vec<String>,
    vectors: Vec<Vec<f32>>,
) -> Result<Vec<VectorRecord>> {
    if ids.len() != vectors.len() {
        return Err(BorsukError::InvalidRecordInput(format!(
            "ids length {} must match vectors length {}",
            ids.len(),
            vectors.len()
        )));
    }

    Ok(ids
        .into_iter()
        .zip(vectors)
        .map(|(id, vector)| VectorRecord::new(id, vector))
        .collect())
}

fn record_text_terms(record: &VectorRecord) -> Option<Vec<(u32, u32)>> {
    if record.text_term_ids.is_empty() {
        None
    } else {
        Some(
            record
                .text_term_ids
                .iter()
                .copied()
                .zip(record.text_term_freqs.iter().copied())
                .collect(),
        )
    }
}

fn validate_record_text_terms(record: &VectorRecord) -> Result<()> {
    if record.text_term_ids.is_empty() && record.text_term_freqs.is_empty() {
        return Ok(());
    }
    if record.text_term_ids.len() != record.text_term_freqs.len() {
        return Err(BorsukError::InvalidMetricInput(format!(
            "record `{}` text term ids length {} must match text term freqs length {}",
            record.id,
            record.text_term_ids.len(),
            record.text_term_freqs.len()
        )));
    }
    if let Some(position) = record.text_term_freqs.iter().position(|freq| *freq == 0) {
        return Err(BorsukError::InvalidMetricInput(format!(
            "record `{}` text term frequency at position {position} must be greater than zero",
            record.id
        )));
    }
    if let Some(position) = record
        .text_term_ids
        .windows(2)
        .position(|window| window[0] >= window[1])
    {
        return Err(BorsukError::InvalidMetricInput(format!(
            "record `{}` text term ids must be strictly increasing; positions {position} and {} are out of order",
            record.id,
            position + 1
        )));
    }
    Ok(())
}

fn default_tokenizer() -> Arc<dyn Tokenizer> {
    Arc::new(UnicodeWordLowercase)
}

fn add_report_from_parts(
    segments_written: usize,
    graph_payloads_written: usize,
    payload_bytes_written: u64,
    storage_report: StorageWriteReport,
    vectors_added: usize,
) -> AddReport {
    let total_bytes_written = payload_bytes_written + storage_report.bytes_written;
    AddReport {
        segments_written,
        graph_payloads_written,
        manifest_tables_written: storage_report.metadata_tables_written,
        routing_pages_written: storage_report.routing_pages_written,
        total_bytes_written,
        bytes_per_vector: if vectors_added == 0 {
            0.0
        } else {
            total_bytes_written as f64 / vectors_added as f64
        },
        requests: RequestCounts::default(),
    }
}

fn validate_wal_config(wal: &WalConfig) -> Result<()> {
    if wal.enabled
        && wal.flush_threshold_runs == 0
        && wal.flush_threshold_records == 0
        && wal.flush_threshold_bytes == 0
    {
        return Err(BorsukError::InvalidMetricInput(
            "an enabled WAL must set a non-zero run, record, or byte flush threshold".to_string(),
        ));
    }
    Ok(())
}

fn validate_graph_neighbors(graph_neighbors: usize) -> Result<()> {
    if graph_neighbors == 0 {
        return Err(BorsukError::InvalidMetricInput(
            "graph_neighbors must be greater than zero".to_string(),
        ));
    }
    Ok(())
}

fn validate_build_config(build: &BuildConfig, dimensions: usize) -> Result<()> {
    build.physical_layout.validate()?;
    let normal_segment_format = build.physical_layout.resolve(
        crate::PhysicalObjectRole::NormalSegment,
        crate::PhysicalLayoutContext::default(),
    )?;
    crate::DurableTableFormat::try_from(normal_segment_format)?;
    let fraction = build.kmeans_sample_fraction;
    if !fraction.is_finite() || fraction <= 0.0 || fraction > 1.0 {
        return Err(BorsukError::InvalidMetricInput(format!(
            "kmeans_sample_fraction must be in (0, 1], got {fraction}"
        )));
    }
    if let Some(iters) = build.kmeans_max_iterations
        && iters == 0
    {
        return Err(BorsukError::InvalidMetricInput(
            "kmeans_max_iterations must be greater than zero when set".to_string(),
        ));
    }
    if let Some(sample) = build.pq_codebook_sample
        && sample == 0
    {
        return Err(BorsukError::InvalidMetricInput(
            "pq_codebook_sample must be greater than zero when set".to_string(),
        ));
    }
    if let crate::GlobalPqLayout::Hierarchical {
        children_per_parent,
    } = build.global_pq_layout
        && !(1..=256).contains(&children_per_parent)
    {
        return Err(BorsukError::InvalidMetricInput(format!(
            "global PQ children_per_parent must be in 1..=256, got {children_per_parent}"
        )));
    }
    if let Some(code_bytes) = build.global_pq_code_bytes {
        if !matches!(
            build.global_scan_codec,
            GlobalScanCodec::Pq | GlobalScanCodec::SrhtPq
        ) {
            return Err(BorsukError::InvalidMetricInput(
                "global_pq_code_bytes does not apply to TurboQuant codecs; configure global_turboquant_bits"
                    .to_string(),
            ));
        }
        let max_width = dimensions.next_power_of_two().min(256);
        if !(1..=max_width).contains(&code_bytes) || !code_bytes.is_power_of_two() {
            return Err(BorsukError::InvalidMetricInput(format!(
                "global_pq_code_bytes must be a power of two in 1..={max_width}, got {code_bytes}"
            )));
        }
    }
    if !(1..=8).contains(&build.global_turboquant_bits) {
        return Err(BorsukError::InvalidMetricInput(format!(
            "global_turboquant_bits must be in 1..=8, got {}",
            build.global_turboquant_bits
        )));
    }
    if build.global_turboquant_shards == 0 {
        return Err(BorsukError::InvalidMetricInput(
            "global_turboquant_shards must be greater than zero".to_string(),
        ));
    }
    if build.global_scan_codec == GlobalScanCodec::FastTurboQuantMse
        && build.global_turboquant_qjl_bits != 0
    {
        return Err(BorsukError::InvalidMetricInput(
            "fast-turboquant-mse-scan does not accept a QJL residual stage; choose fast-turboquant-scan"
                .to_string(),
        ));
    }
    if build.global_scan_codec == GlobalScanCodec::FastTurboQuantProd {
        if build.global_turboquant_bits < 2 {
            return Err(BorsukError::InvalidMetricInput(
                "fast-turboquant-scan requires at least two total bits".to_string(),
            ));
        }
        if build.global_turboquant_shards != 1 {
            return Err(BorsukError::InvalidMetricInput(
                "fast-turboquant-scan is a whole-vector codec and requires one shard".to_string(),
            ));
        }
    }
    if let Some(graph) = &build.global_cell_graph {
        if !(4..=128).contains(&graph.degree) {
            return Err(BorsukError::InvalidMetricInput(format!(
                "global cell graph degree must be in 4..=128, got {}",
                graph.degree
            )));
        }
        if graph.construction_ef < graph.degree || graph.construction_ef > 4_096 {
            return Err(BorsukError::InvalidMetricInput(format!(
                "global cell graph construction_ef must be in degree..=4096, got {}",
                graph.construction_ef
            )));
        }
    }
    Ok(())
}

fn next_generated_id_after_explicit_records(current: u64, records: &[VectorRecord]) -> Result<u64> {
    let mut next = current;
    for record in records {
        if let Some(id) = record
            .id
            .try_as_str()
            .ok()
            .and_then(|id| id.parse::<u64>().ok())
        {
            let after_id = id.checked_add(1).ok_or_else(|| {
                BorsukError::InvalidRecordInput(format!(
                    "numeric record id `{}` leaves no generated id range",
                    record.id
                ))
            })?;
            next = next.max(after_id);
        }
    }
    Ok(next)
}

fn advance_generated_id(current: u64, count: usize) -> Result<u64> {
    let count = u64::try_from(count).map_err(|_| {
        BorsukError::InvalidRecordInput("generated id count does not fit u64".to_string())
    })?;
    current.checked_add(count).ok_or_else(|| {
        BorsukError::InvalidRecordInput("generated id exceeds u64 range".to_string())
    })
}

fn count_cache_read(cache_hit: bool, hits: &mut usize, misses: &mut usize) {
    if cache_hit {
        *hits += 1;
    } else {
        *misses += 1;
    }
}

fn count_cache_repair(cache_repaired: bool, repairs: &mut usize) {
    if cache_repaired {
        *repairs += 1;
    }
}

fn object_is_at_least_min_age(
    object: &StoredObject,
    min_age: Duration,
    now: DateTime<Utc>,
) -> bool {
    timestamp_is_at_least_min_age(object.last_modified, min_age, now)
}

fn timestamp_is_at_least_min_age(
    last_modified: DateTime<Utc>,
    min_age: Duration,
    now: DateTime<Utc>,
) -> bool {
    now.signed_duration_since(last_modified)
        .to_std()
        .is_ok_and(|age| age >= min_age)
}

fn manifest_table_version_from_path(path: &str) -> Option<u64> {
    path.strip_prefix("manifests/manifest-")?
        .strip_suffix(".parquet")?
        .parse::<u64>()
        .ok()
}

fn is_parquet_path(path: &str) -> bool {
    path.ends_with(".parquet")
}

fn is_segment_table_path(path: &str) -> bool {
    path.ends_with(".parquet") || path.ends_with(".vortex")
}

fn is_filter_index_path(path: &str) -> bool {
    path.ends_with(".fidx")
}

fn is_vector_sidecar_path(path: &str) -> bool {
    path.ends_with(".arrow")
}

fn is_global_pq_path(path: &str) -> bool {
    path.starts_with("global-pq/")
        && (path.ends_with(".bin") || path.ends_with(".arrow") || path.ends_with(".parquet"))
}

/// Whether the filter's shape could ever be answered by the per-segment index
/// (every comparison is an equality-class op; no ranges or existence tests). If
/// not, the on-demand sidecars are skipped -- the index would decline anyway, so
/// there is no point paying for the reads (e.g. a numeric `year >= 2000` filter).
fn filter_may_use_index(filter: &crate::Filter) -> bool {
    use crate::{Filter, Op};
    match filter {
        Filter::And(children) | Filter::Or(children) => children.iter().all(filter_may_use_index),
        Filter::Not(child) => filter_may_use_index(child),
        Filter::Exists { .. } | Filter::GeoRadius { .. } => false,
        Filter::Cmp { op, .. } => {
            matches!(op, Op::Eq | Op::Ne | Op::In | Op::Nin | Op::Contains)
        }
    }
}

fn is_manifest_table_path(path: &str) -> bool {
    path.starts_with("manifests/manifest-") && is_parquet_path(path)
}

fn is_cell_wal_immutable_path(path: &str) -> bool {
    path.starts_with("cells/")
        && path.contains("/wal/")
        && (path.contains("/runs/") || path.contains("/frontier/"))
}

fn is_cell_wal_transaction_path(path: &str) -> bool {
    path.starts_with("transactions/")
        && (path.ends_with("/STATE")
            || path.ends_with("/COMMIT")
            || (path.contains("/descriptors/") && path.ends_with(".bin")))
}

fn is_routing_metadata_table_path(path: &str) -> bool {
    (path.starts_with("routing/segments-") || path.starts_with("routing/pivots-"))
        && is_parquet_path(path)
}

fn is_tombstone_table_path(path: &str) -> bool {
    path.starts_with("tombstones/") && is_parquet_path(path)
}

fn output_segment_chunk_size(
    record_count: usize,
    target_segment_max_vectors: usize,
    min_output_segments: usize,
) -> usize {
    let min_output_segments = min_output_segments.max(1).min(record_count.max(1));
    record_count
        .div_ceil(min_output_segments)
        .min(target_segment_max_vectors)
        .max(1)
}

fn split_summaries_for_routing_pages(
    summaries: Vec<SegmentSummary>,
    min_pages: usize,
    routing_page_fanout: usize,
) -> Vec<Vec<SegmentSummary>> {
    if summaries.is_empty() {
        return Vec::new();
    }

    let min_pages = min_pages.max(1).min(summaries.len());
    let mut pages = Vec::new();
    let mut start = 0_usize;

    for page_index in 0..min_pages {
        let remaining = summaries.len() - start;
        let remaining_pages = min_pages - page_index;
        let reserved_for_later_pages = remaining_pages - 1;
        let page_len = (remaining - reserved_for_later_pages).clamp(1, routing_page_fanout);
        pages.push(summaries[start..start + page_len].to_vec());
        start += page_len;
    }

    while start < summaries.len() {
        let page_len = (summaries.len() - start).min(routing_page_fanout);
        pages.push(summaries[start..start + page_len].to_vec());
        start += page_len;
    }

    pages
}

fn routing_page_tree_content_page_count(segment_count: usize, routing_page_fanout: usize) -> usize {
    if segment_count == 0 {
        return 0;
    }

    let mut page_count = segment_count.div_ceil(routing_page_fanout);
    let mut total = 0_usize;
    loop {
        total += page_count;
        if page_count <= 1 {
            return total;
        }
        page_count = page_count.div_ceil(routing_page_fanout);
    }
}

fn routing_leaf_page_count(segment_count: usize, routing_page_fanout: usize) -> usize {
    if segment_count == 0 {
        0
    } else {
        segment_count.div_ceil(routing_page_fanout)
    }
}

fn leaf_page_occupied_ranges_from_cached_tree(
    top_page_refs: &[RoutingLayerPageRef],
    decoded_parent_pages: &HashMap<String, Vec<RoutingLayerPageRef>>,
    routing_page_fanout: usize,
) -> Result<Vec<Range<usize>>> {
    let mut ranges = Vec::new();
    for page_ref in top_page_refs {
        reserve_leaf_page_range(
            page_ref,
            decoded_parent_pages,
            routing_page_fanout,
            &mut ranges,
        )?;
    }
    Ok(ranges)
}

fn reserve_leaf_page_range(
    page_ref: &RoutingLayerPageRef,
    decoded_parent_pages: &HashMap<String, Vec<RoutingLayerPageRef>>,
    routing_page_fanout: usize,
    ranges: &mut Vec<Range<usize>>,
) -> Result<()> {
    if page_ref.routing_level == 0 {
        let end = page_ref.page_ordinal.checked_add(1).ok_or_else(|| {
            BorsukError::InvalidStorage("routing leaf page ordinal overflow".to_string())
        })?;
        ranges.push(page_ref.page_ordinal..end);
        return Ok(());
    }

    if let Some(child_refs) = decoded_parent_pages.get(&page_ref.path) {
        for child_ref in child_refs {
            reserve_leaf_page_range(child_ref, decoded_parent_pages, routing_page_fanout, ranges)?;
        }
        return Ok(());
    }

    let span =
        routing_leaf_page_span(page_ref.routing_level, routing_page_fanout).ok_or_else(|| {
            BorsukError::InvalidStorage("routing leaf page span overflow".to_string())
        })?;
    let start = page_ref.page_ordinal.checked_mul(span).ok_or_else(|| {
        BorsukError::InvalidStorage("routing leaf page range overflow".to_string())
    })?;
    let end = start.checked_add(span).ok_or_else(|| {
        BorsukError::InvalidStorage("routing leaf page range overflow".to_string())
    })?;
    ranges.push(start..end);
    Ok(())
}

fn next_available_leaf_page_ordinal(
    cursor: &mut usize,
    occupied_ranges: &mut Vec<Range<usize>>,
) -> Result<usize> {
    loop {
        let mut advanced = false;
        for range in occupied_ranges.iter() {
            if range.contains(cursor) {
                *cursor = range.end;
                advanced = true;
                break;
            }
        }
        if advanced {
            continue;
        }

        let ordinal = *cursor;
        let end = ordinal.checked_add(1).ok_or_else(|| {
            BorsukError::InvalidStorage("routing leaf page ordinal overflow".to_string())
        })?;
        occupied_ranges.push(ordinal..end);
        *cursor = end;
        return Ok(ordinal);
    }
}

fn validate_compaction_options(options: &CompactionOptions) -> Result<()> {
    if options.source_level == options.target_level {
        return Err(BorsukError::InvalidCompactionInput(
            "source_level and target_level must differ".to_string(),
        ));
    }

    if options.min_segments == 0 {
        return Err(BorsukError::InvalidCompactionInput(
            "min_segments must be greater than zero".to_string(),
        ));
    }

    if options.max_segments == Some(0) {
        return Err(BorsukError::InvalidCompactionInput(
            "max_segments must be greater than zero when set".to_string(),
        ));
    }

    if let Some(max_segments) = options.max_segments
        && options.min_segments > max_segments
    {
        return Err(BorsukError::InvalidCompactionInput(
            "min_segments must be less than or equal to max_segments when max_segments is set"
                .to_string(),
        ));
    }

    if options.target_segment_max_vectors == Some(0) {
        return Err(BorsukError::InvalidCompactionInput(
            "target_segment_max_vectors must be greater than zero when set".to_string(),
        ));
    }

    if let Some(radius) = options.target_segment_max_radius
        && (!radius.is_finite() || radius <= 0.0)
    {
        return Err(BorsukError::InvalidCompactionInput(
            "target_segment_max_radius must be a finite value greater than zero when set"
                .to_string(),
        ));
    }

    Ok(())
}

fn validate_search_options(options: &SearchOptions) -> Result<()> {
    if options.k == 0 {
        return Err(BorsukError::InvalidSearchOptions(
            "k must be greater than zero".to_string(),
        ));
    }
    if options.prefetch_depth == 0 {
        return Err(BorsukError::InvalidSearchOptions(
            "prefetch_depth must be greater than zero".to_string(),
        ));
    }

    let SearchMode::Approx {
        leaf_mode: _,
        eps,
        max_segments,
        max_bytes,
        max_latency_ms,
        routing_page_overfetch,
        max_candidates_per_segment,
        adaptive_stop: _,
        projected_reads: _,
    } = &options.mode
    else {
        return Ok(());
    };

    if let Some(eps) = eps
        && (!eps.is_finite() || *eps < 0.0)
    {
        return Err(BorsukError::InvalidSearchOptions(
            "eps must be finite and non-negative when set".to_string(),
        ));
    }

    if *max_segments == Some(0) {
        return Err(BorsukError::InvalidSearchOptions(
            "max_segments must be greater than zero when set".to_string(),
        ));
    }

    if *max_bytes == Some(0) {
        return Err(BorsukError::InvalidSearchOptions(
            "max_bytes must be greater than zero when set".to_string(),
        ));
    }

    if *max_latency_ms == Some(0) {
        return Err(BorsukError::InvalidSearchOptions(
            "max_latency_ms must be greater than zero when set".to_string(),
        ));
    }

    if *routing_page_overfetch == Some(0) {
        return Err(BorsukError::InvalidSearchOptions(
            "routing_page_overfetch must be greater than zero when set".to_string(),
        ));
    }

    if *max_candidates_per_segment == Some(0) {
        return Err(BorsukError::InvalidSearchOptions(
            "max_candidates_per_segment must be greater than zero when set".to_string(),
        ));
    }

    Ok(())
}

fn enforce_ram_budget(manifest: &Manifest, runtime_budget_bytes: Option<u64>) -> Result<()> {
    let Some(budget_bytes) =
        effective_ram_budget_bytes(manifest.config.ram_budget_bytes, runtime_budget_bytes)
    else {
        return Ok(());
    };

    let resident_bytes = manifest.resident_bytes_estimate();
    if resident_bytes > budget_bytes {
        return Err(BorsukError::RamBudgetExceeded {
            resident_bytes,
            budget_bytes,
        });
    }

    Ok(())
}

fn apply_i64_delta(base: u64, delta: i64, label: &str) -> Result<u64> {
    if delta >= 0 {
        return base.checked_add(delta as u64).ok_or_else(|| {
            BorsukError::InvalidStorage(format!("{label} plus persisted delta exceeds u64"))
        });
    }
    base.checked_sub(delta.unsigned_abs()).ok_or_else(|| {
        BorsukError::InvalidStorage(format!(
            "{label} persisted delta suppresses more than the physical total"
        ))
    })
}

fn effective_ram_budget_bytes(
    persisted_budget_bytes: Option<u64>,
    runtime_budget_bytes: Option<u64>,
) -> Option<u64> {
    [persisted_budget_bytes, runtime_budget_bytes]
        .into_iter()
        .flatten()
        .min()
}

fn automatic_lexical_capacity_bytes(effective_ram_budget_bytes: Option<u64>) -> Option<u64> {
    effective_ram_budget_bytes.map(|total| {
        total
            .checked_div(LEXICAL_RAM_BUDGET_DIVISOR)
            .unwrap_or(0)
            .max(1)
    })
}

fn estimated_lexical_term_block_bytes(entry: &crate::lexical_root::LexicalTermBlock) -> usize {
    std::mem::size_of::<crate::lexical_root::LexicalTermBlock>()
        .saturating_add(entry.run.segment_key.len())
        .saturating_add(entry.run.postings_path.len())
        .saturating_add(entry.run.postings_checksum.len())
        .saturating_add(entry.run.postings_group_checksum.len())
        .saturating_add(entry.run.metadata_path.len())
        .saturating_add(entry.run.metadata_checksum.len())
        .saturating_add(entry.run.metadata_group_checksum.len())
}

fn lexical_shard_identity(segments: &[SegmentSummary]) -> Vec<(String, String, String, String)> {
    let mut shards = segments
        .iter()
        .flat_map(|segment| {
            segment.lexical_shards.iter().map(|shard| {
                (
                    shard.kind.clone(),
                    shard.name.clone(),
                    shard.path.clone(),
                    shard.checksum.clone(),
                )
            })
        })
        .collect::<Vec<_>>();
    shards.sort();
    shards
}

struct CandidateRecordSelection {
    indices: Vec<usize>,
    graph_candidates_added: usize,
    truncated: bool,
}

/// One discovered-but-not-yet-expanded graph row. [`BinaryHeap`] is a max
/// heap, so the comparisons are reversed to pop the nearest row first while
/// retaining the previous deterministic record-id tie break.
struct GraphFrontierEntry<'a> {
    record_index: usize,
    distance: f32,
    record_id: &'a RecordId,
}

impl PartialEq for GraphFrontierEntry<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.distance.total_cmp(&other.distance).is_eq()
            && self.record_id == other.record_id
            && self.record_index == other.record_index
    }
}

impl Eq for GraphFrontierEntry<'_> {}

impl Ord for GraphFrontierEntry<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .distance
            .total_cmp(&self.distance)
            .then_with(|| other.record_id.cmp(self.record_id))
            .then_with(|| other.record_index.cmp(&self.record_index))
    }
}

impl PartialOrd for GraphFrontierEntry<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

struct ProjectedSegmentRead {
    segment: Arc<Segment>,
    bytes_read: u64,
    records_considered: usize,
    candidates: CandidateRecordSelection,
    vectors: HashMap<usize, Vec<f32>>,
}

type SearchSegmentRead = (
    Arc<Segment>,
    u64,
    bool,
    bool,
    bool,
    usize,
    Option<CandidateRecordSelection>,
    Option<HashMap<usize, Vec<f32>>>,
);

// ---- Per-segment filter-index sidecar -----------------------------------
//
// A per-segment exact metadata index ([`crate::MetadataIndex`]) is persisted as
// a small sidecar object next to the segment and fetched ONLY when a query
// carries a filter -- never held resident, so it does not grow RAM. It lets a
// filtered query prove a segment holds no matching row and skip its (large)
// payload fetch entirely, refining the coarse resident stats without their bloom
// false positives.
//
// The sidecar is content-addressed by the segment checksum and self-validating:
// its bytes are `segment-checksum (64 ascii) || blake3(index-bytes) (32) ||
// index-bytes`. A corrupt, stale, or missing sidecar fails validation and the
// query simply falls back to reading the segment -- so it can never change
// results, only save I/O.

const FILTER_INDEX_CHECKSUM_LEN: usize = 64;
const FILTER_INDEX_CONTENT_HASH_LEN: usize = 32;

struct FilterIndexRead {
    index: crate::MetadataIndex,
    bytes_read: u64,
    cache_hit: bool,
    cache_repaired: bool,
}

enum LexicalRunPostings {
    Bm25(Vec<Bm25Posting>),
    Sparse(Vec<SparsePosting>),
}

struct LexicalRunRead {
    postings: LexicalRunPostings,
    rows: Vec<LexicalRowMetadata>,
}

fn decoded_lexical_term_page_bytes(page: &LexicalTermPage) -> u64 {
    let fixed = std::mem::size_of::<LexicalTermPage>().saturating_add(
        page.entries
            .capacity()
            .saturating_mul(std::mem::size_of::<crate::lexical_root::LexicalTermBlock>()),
    );
    let strings = page.entries.iter().fold(0_usize, |bytes, entry| {
        [
            &entry.run.segment_key,
            &entry.run.postings_path,
            &entry.run.postings_checksum,
            &entry.run.postings_group_checksum,
            &entry.run.metadata_path,
            &entry.run.metadata_checksum,
            &entry.run.metadata_group_checksum,
        ]
        .iter()
        .fold(bytes, |bytes, value| bytes.saturating_add(value.capacity()))
    });
    u64::try_from(fixed.saturating_add(strings)).unwrap_or(u64::MAX)
}

fn filter_index_relative_path(segment_checksum: &str) -> String {
    format!("fidx/{}/{}.fidx", &segment_checksum[..2], segment_checksum)
}

fn vector_sidecar_relative_path(segment_checksum: &str) -> String {
    format!(
        "vectors/{}/{}.arrow",
        &segment_checksum[..2],
        segment_checksum
    )
}

fn late_interaction_sidecar_relative_path(name: &str, segment_checksum: &str) -> String {
    format!(
        "late-interaction/{name}/{}/{}.arrow",
        &segment_checksum[..2],
        segment_checksum
    )
}

fn encode_filter_index(segment_checksum: &str, index: &crate::MetadataIndex) -> Vec<u8> {
    let index_bytes = index.to_bytes();
    let content_hash = blake3::hash(&index_bytes);
    let mut out = Vec::with_capacity(
        FILTER_INDEX_CHECKSUM_LEN + FILTER_INDEX_CONTENT_HASH_LEN + index_bytes.len(),
    );
    out.extend_from_slice(segment_checksum.as_bytes());
    out.extend_from_slice(content_hash.as_bytes());
    out.extend_from_slice(&index_bytes);
    out
}

fn decode_filter_index(bytes: &[u8], expected_checksum: &str) -> Option<crate::MetadataIndex> {
    let header = FILTER_INDEX_CHECKSUM_LEN + FILTER_INDEX_CONTENT_HASH_LEN;
    if bytes.len() < header || expected_checksum.len() != FILTER_INDEX_CHECKSUM_LEN {
        return None;
    }
    if &bytes[..FILTER_INDEX_CHECKSUM_LEN] != expected_checksum.as_bytes() {
        return None;
    }
    let content_hash = &bytes[FILTER_INDEX_CHECKSUM_LEN..header];
    let index_bytes = &bytes[header..];
    if blake3::hash(index_bytes).as_bytes() != content_hash {
        return None;
    }
    crate::MetadataIndex::from_bytes(index_bytes).ok()
}

/// Row positions in a segment whose metadata satisfies the filter, used to
/// prefilter a segment during a budgeted filtered search. Uses the exact
/// per-segment [`crate::MetadataIndex`] when it can answer the filter, and
/// otherwise evaluates the predicate row by row. Either way the result is the
/// exact match set, so it never changes which records a filter accepts.
fn segment_filter_match_rows(segment: &Segment, filter: &crate::Filter) -> Vec<usize> {
    let index =
        crate::MetadataIndex::from_rows(segment.records.iter().map(|record| &record.metadata));
    if let Some(rows) = index.matching_rows(filter) {
        return rows.into_iter().map(|row| row as usize).collect();
    }
    segment
        .records
        .iter()
        .enumerate()
        .filter(|(_, record)| filter.matches(&record.metadata))
        .map(|(index, _)| index)
        .collect()
}

/// Apply independent work with bounded cross-item concurrency while retaining
/// input order. Object-store search uses this for selected cells: each cell has
/// its own immutable objects, so serial round trips only add latency and cannot
/// improve correctness.
#[cfg(test)]
fn bounded_parallel_map<T, U, F>(values: &[T], width: usize, work: F) -> Vec<U>
where
    T: Sync,
    U: Send,
    F: Fn(&T) -> U + Sync,
{
    bounded_parallel_map_with_gate(values, width, None, work)
}

fn bounded_parallel_map_with_gate<T, U, F>(
    values: &[T],
    width: usize,
    gate: Option<&AdmissionGate>,
    work: F,
) -> Vec<U>
where
    T: Sync,
    U: Send,
    F: Fn(&T) -> U + Sync,
{
    let width = width.max(1);
    let mut output = Vec::with_capacity(values.len());
    for chunk in values.chunks(width) {
        let mapped = crate::parallel::install(|| {
            chunk
                .par_iter()
                .map(|value| {
                    let _permit = gate.map(AdmissionGate::acquire);
                    work(value)
                })
                .collect::<Vec<_>>()
        });
        output.extend(mapped);
    }
    output
}

fn bounded_io_map_with_gate<T, U, F>(
    values: &[T],
    width: usize,
    gate: Option<&AdmissionGate>,
    work: F,
) -> Vec<U>
where
    T: Sync,
    U: Send,
    F: Fn(&T) -> U + Sync,
{
    let width = width.max(1);
    let mut output = Vec::with_capacity(values.len());
    for chunk in values.chunks(width) {
        let mapped = crate::parallel::install_io(|| {
            chunk
                .par_iter()
                .map(|value| {
                    let _permit = gate.map(AdmissionGate::acquire);
                    work(value)
                })
                .collect::<Vec<_>>()
        });
        output.extend(mapped);
    }
    output
}

fn kth_largest_score(scores: impl Iterator<Item = f64>, k: usize) -> Option<f64> {
    if k == 0 {
        return None;
    }
    let mut scores = scores.collect::<Vec<_>>();
    if scores.len() < k {
        return None;
    }
    let (_, kth, _) = scores.select_nth_unstable_by(k - 1, |left, right| right.total_cmp(left));
    Some(*kth)
}

fn rank_candidate_indices<Distance, TieBreak>(
    length: usize,
    limit: usize,
    mut distance: Distance,
    mut tie_break: TieBreak,
) -> Vec<usize>
where
    Distance: FnMut(usize) -> f32,
    TieBreak: FnMut(&usize, &usize) -> Ordering,
{
    if length == 0 || limit == 0 {
        return Vec::new();
    }

    // Coarse scoring is the expensive part for high-dimensional leaves. Cache
    // one score per row, then partially select only the requested prefix instead
    // of recomputing scores throughout a full O(n log n) comparison sort.
    let mut scored = (0..length)
        .map(|index| (index, distance(index)))
        .collect::<Vec<_>>();
    let mut compare = |left: &(usize, f32), right: &(usize, f32)| {
        left.1
            .partial_cmp(&right.1)
            .unwrap_or(Ordering::Equal)
            .then_with(|| tie_break(&left.0, &right.0))
    };
    if limit < scored.len() {
        scored.select_nth_unstable_by(limit, &mut compare);
        scored.truncate(limit);
    }
    scored.sort_by(&mut compare);
    scored.into_iter().map(|(index, _)| index).collect()
}

fn enqueue_graph_neighbors<'a, Distance>(
    source: usize,
    records: &'a [VectorRecord],
    graph: &SegmentGraph,
    state: &mut [u8],
    frontier: &mut BinaryHeap<GraphFrontierEntry<'a>>,
    distance: &mut Distance,
) -> Result<()>
where
    Distance: FnMut(usize) -> Result<f32>,
{
    for edge in graph.outgoing_edges(source) {
        let record_index = edge.neighbor_record_index;
        if record_index >= records.len() || state[record_index] != 0 {
            continue;
        }
        state[record_index] = 1;
        frontier.push(GraphFrontierEntry {
            record_index,
            distance: distance(record_index)?,
            record_id: &records[record_index].id,
        });
    }
    Ok(())
}

/// Traverse a segment graph in exact-distance best-first order. A dense state
/// table prevents duplicate queue entries, so every discovered row is scored
/// exactly once rather than once per scan of a growing frontier.
fn best_first_graph_candidates<Distance>(
    records: &[VectorRecord],
    graph: &SegmentGraph,
    initial: &[usize],
    limit: usize,
    mut distance: Distance,
) -> Result<Vec<usize>>
where
    Distance: FnMut(usize) -> Result<f32>,
{
    let limit = limit.min(records.len());
    let mut selected = Vec::with_capacity(limit);
    let mut state = vec![0_u8; records.len()];
    for &record_index in initial {
        if selected.len() >= limit {
            break;
        }
        if record_index < records.len() && state[record_index] == 0 {
            state[record_index] = 2;
            selected.push(record_index);
        }
    }

    let mut frontier = BinaryHeap::new();
    for &record_index in &selected {
        enqueue_graph_neighbors(
            record_index,
            records,
            graph,
            &mut state,
            &mut frontier,
            &mut distance,
        )?;
    }

    while selected.len() < limit {
        let Some(next) = frontier.pop() else {
            break;
        };
        if state[next.record_index] != 1 {
            continue;
        }
        state[next.record_index] = 2;
        selected.push(next.record_index);
        enqueue_graph_neighbors(
            next.record_index,
            records,
            graph,
            &mut state,
            &mut frontier,
            &mut distance,
        )?;
    }

    Ok(selected)
}

fn candidate_record_indices(
    segment: &Segment,
    graph: Option<&SegmentGraph>,
    query: &[f32],
    mode: &SearchMode,
    leaf_mode: LeafMode,
    k: usize,
    build_config: &BuildConfig,
) -> Result<CandidateRecordSelection> {
    let Some(max_candidates_per_segment) = max_candidates_per_segment(mode) else {
        return Ok(CandidateRecordSelection {
            indices: (0..segment.records.len()).collect(),
            graph_candidates_added: 0,
            truncated: false,
        });
    };

    let limit = max_candidates_per_segment.min(segment.records.len());
    let truncated = limit < segment.records.len();
    let normalized_query;
    let coarse_query = if build_config.normalized_angular_coarse_geometry
        && segment.metric.uses_normalized_euclidean_geometry()
    {
        normalized_query = crate::metric::unit_l2_normalized(query);
        normalized_query.as_slice()
    } else {
        query
    };
    let query_code = routing_code(coarse_query);
    let use_pq_leaf = matches!(
        leaf_mode,
        LeafMode::PqScan
            | LeafMode::SrhtPqScan
            | LeafMode::FastTurboQuantMseScan
            | LeafMode::FastTurboQuantProdScan
            | LeafMode::VamanaPq
    );
    let scorer = CoarseScorer::for_query(
        segment,
        coarse_query,
        build_config.quantizer,
        use_pq_leaf,
        query_code,
    )?;
    let indices = rank_candidate_indices(
        segment.records.len(),
        limit,
        |index| scorer.distance(segment, index),
        |left, right| segment.records[*left].id.cmp(&segment.records[*right].id),
    );

    let Some(graph) = graph else {
        return Ok(CandidateRecordSelection {
            indices,
            graph_candidates_added: 0,
            truncated,
        });
    };

    let entry_count = k.max(1).min(limit).min(indices.len());
    let initial = indices
        .iter()
        .copied()
        .take(entry_count)
        .collect::<Vec<_>>();
    let mut selected =
        best_first_graph_candidates(&segment.records, graph, &initial, limit, |record_index| {
            // Query validated once at the search entry; segment record vectors
            // are stored, already-validated rows. Score through the unchecked
            // kernel while still surfacing degeneracy errors.
            segment
                .metric
                .distance_unchecked(query, &segment.records[record_index].vector)
        })?;
    let graph_candidates_added = selected.len().saturating_sub(initial.len());
    let mut selected_set = selected.iter().copied().collect::<HashSet<_>>();

    for record_index in indices {
        if selected.len() >= limit {
            break;
        }
        if selected_set.insert(record_index) {
            selected.push(record_index);
        }
    }

    Ok(CandidateRecordSelection {
        indices: selected,
        graph_candidates_added,
        truncated,
    })
}

fn effective_leaf_mode(mode: &SearchMode, stored_leaf_mode: LeafMode) -> LeafMode {
    match mode {
        SearchMode::Approx {
            leaf_mode: LeafMode::Hybrid,
            ..
        } => stored_leaf_mode,
        _ => mode.leaf_mode(),
    }
}

fn should_expand_segment_graph(
    mode: &SearchMode,
    k: usize,
    stored_leaf_mode: LeafMode,
    segment_len: usize,
) -> bool {
    let SearchMode::Approx {
        leaf_mode,
        max_candidates_per_segment: Some(max_candidates_per_segment),
        ..
    } = mode
    else {
        return false;
    };
    let candidate_limit = (*max_candidates_per_segment).min(segment_len);
    if candidate_limit <= k.max(1) || candidate_limit >= segment_len {
        return false;
    }

    match leaf_mode {
        LeafMode::Graph | LeafMode::VamanaPq => true,
        LeafMode::Hybrid => matches!(stored_leaf_mode, LeafMode::Graph | LeafMode::VamanaPq),
        LeafMode::FlatScan
        | LeafMode::SqScan
        | LeafMode::PqScan
        | LeafMode::SrhtPqScan
        | LeafMode::FastTurboQuantMseScan
        | LeafMode::FastTurboQuantProdScan => false,
    }
}

fn should_prioritize_vector_signature(mode: &SearchMode) -> bool {
    matches!(
        mode,
        SearchMode::Approx {
            eps: None,
            max_segments: Some(_),
            ..
        }
    )
}

fn candidate_selection_mode(options: &SearchOptions) -> SearchMode {
    if !options.guaranteed_recall {
        return options.mode.clone();
    }

    match &options.mode {
        SearchMode::Exact => SearchMode::Exact,
        SearchMode::Approx {
            leaf_mode,
            eps,
            max_segments,
            max_bytes,
            max_latency_ms,
            routing_page_overfetch,
            max_candidates_per_segment: _,
            adaptive_stop,
            projected_reads,
        } => SearchMode::Approx {
            leaf_mode: *leaf_mode,
            eps: *eps,
            max_segments: *max_segments,
            max_bytes: *max_bytes,
            max_latency_ms: *max_latency_ms,
            routing_page_overfetch: *routing_page_overfetch,
            max_candidates_per_segment: None,
            adaptive_stop: *adaptive_stop,
            projected_reads: *projected_reads,
        },
    }
}

fn recall_guarantee_for_search(
    mode: &SearchMode,
    termination_reason: SearchTerminationReason,
    segments_skipped: usize,
    candidate_truncated: bool,
) -> RecallGuarantee {
    if matches!(mode, SearchMode::Exact) {
        return RecallGuarantee::Exact;
    }

    if termination_reason == SearchTerminationReason::Complete
        && segments_skipped == 0
        && !candidate_truncated
    {
        RecallGuarantee::BudgetComplete
    } else {
        RecallGuarantee::Degraded
    }
}

fn max_candidates_per_segment(mode: &SearchMode) -> Option<usize> {
    match mode {
        SearchMode::Exact => None,
        SearchMode::Approx {
            leaf_mode: _,
            max_candidates_per_segment,
            ..
        } => *max_candidates_per_segment,
    }
}

fn parallel_projected_segment_budget(mode: &SearchMode, available: usize) -> usize {
    match mode {
        SearchMode::Approx {
            eps: None,
            max_segments: Some(limit),
            max_bytes: None,
            max_latency_ms: None,
            adaptive_stop: None,
            ..
        } => (*limit).min(available),
        _ => 0,
    }
}

fn routing_page_overfetch(mode: &SearchMode) -> usize {
    match mode {
        SearchMode::Exact => ROUTING_SEARCH_PAGE_OVERFETCH,
        SearchMode::Approx {
            routing_page_overfetch,
            ..
        } => routing_page_overfetch.unwrap_or(ROUTING_SEARCH_PAGE_OVERFETCH),
    }
}

// Rank cells for the approximate probe by distance to the cell CENTROID, not by
// the per-dimension bounding-box lower bound. The bounding box is a conservative
// exact-pruning bound that rewards axis-aligned cells and, in high dimensions,
// collapses toward zero for every cell — so it cannot order cells by how likely
// they are to hold the query's neighbours. Centroid distance is the IVF ranking:
// with tight Voronoi cells it puts a query's neighbours in its few nearest cells,
// which is what lets `nprobe` read a small, fixed number of segments. (Exact
// search still prunes on the true lower bound; only the visit ORDER changes.)
fn segment_routing_rank_distance(
    summary: &SegmentSummary,
    query: &[f32],
    metric: &VectorMetric,
) -> Result<f32> {
    // Query validated once at the search entry; the segment centroid is a stored,
    // already-validated vector — score through the unchecked SIMD kernel.
    metric.distance_unchecked(query, &summary.centroid)
}

fn page_ref_routing_rank_distance(
    page_ref: &RoutingLayerPageRef,
    query: &[f32],
    metric: &VectorMetric,
) -> Result<f32> {
    // Query validated once at the search entry; the page-ref centroid is stored.
    metric.distance_unchecked(query, &page_ref.centroid)
}

fn leaf_page_ref_updates_by_ordinal(
    page_refs: &[RoutingLayerPageRef],
) -> Result<HashMap<usize, RoutingLayerPageRef>> {
    let mut updates = HashMap::with_capacity(page_refs.len());
    for page_ref in page_refs {
        if page_ref.routing_level != 0 {
            return Err(BorsukError::InvalidStorage(format!(
                "routing leaf update must be an L0 page ref, got L{}",
                page_ref.routing_level
            )));
        }
        if updates
            .insert(page_ref.page_ordinal, page_ref.clone())
            .is_some()
        {
            return Err(BorsukError::InvalidStorage(format!(
                "duplicate routing leaf update for page {}",
                page_ref.page_ordinal
            )));
        }
    }
    Ok(updates)
}

fn upsert_leaf_page_ref_by_ordinal(
    page_refs: &mut Vec<RoutingLayerPageRef>,
    page_ref: RoutingLayerPageRef,
) -> Result<()> {
    if page_ref.routing_level != 0 {
        return Err(BorsukError::InvalidStorage(format!(
            "routing leaf update must be an L0 page ref, got L{}",
            page_ref.routing_level
        )));
    }
    page_refs.retain(|existing| existing.page_ordinal != page_ref.page_ordinal);
    page_refs.push(page_ref);
    page_refs.sort_by_key(|page_ref| page_ref.page_ordinal);
    Ok(())
}

fn routing_page_refs_by_parent_ordinal(
    page_refs: &[RoutingLayerPageRef],
    routing_page_fanout: usize,
) -> BTreeMap<usize, Vec<RoutingLayerPageRef>> {
    let mut grouped = BTreeMap::<usize, Vec<RoutingLayerPageRef>>::new();
    for page_ref in page_refs {
        grouped
            .entry(page_ref.page_ordinal / routing_page_fanout)
            .or_default()
            .push(page_ref.clone());
    }
    for refs in grouped.values_mut() {
        refs.sort_by_key(|page_ref| page_ref.page_ordinal);
    }
    grouped
}

fn leaf_page_ref_updates_by_parent_ordinal<'a>(
    routing_level: u8,
    page_refs: impl IntoIterator<Item = &'a RoutingLayerPageRef>,
    routing_page_fanout: usize,
) -> Result<BTreeMap<usize, Vec<RoutingLayerPageRef>>> {
    let mut grouped = BTreeMap::<usize, Vec<RoutingLayerPageRef>>::new();
    for page_ref in page_refs {
        if page_ref.routing_level != 0 {
            return Err(BorsukError::InvalidStorage(format!(
                "routing leaf update must be an L0 page ref, got L{}",
                page_ref.routing_level
            )));
        }
        grouped
            .entry(routing_parent_ordinal_for_leaf(
                routing_level,
                page_ref.page_ordinal,
                routing_page_fanout,
            )?)
            .or_default()
            .push(page_ref.clone());
    }
    for updates in grouped.values_mut() {
        updates.sort_by_key(|page_ref| page_ref.page_ordinal);
    }
    Ok(grouped)
}

fn routing_subtree_contains_leaf_update(
    page_ref: &RoutingLayerPageRef,
    updates: &HashMap<usize, RoutingLayerPageRef>,
    routing_page_fanout: usize,
) -> bool {
    updates.keys().any(|leaf_ordinal| {
        routing_subtree_contains_leaf_ordinal(page_ref, *leaf_ordinal, routing_page_fanout)
    })
}

fn routing_subtree_contains_leaf_ordinal(
    page_ref: &RoutingLayerPageRef,
    leaf_ordinal: usize,
    routing_page_fanout: usize,
) -> bool {
    let Some(span) = routing_leaf_page_span(page_ref.routing_level, routing_page_fanout) else {
        return true;
    };
    let Some(start) = page_ref.page_ordinal.checked_mul(span) else {
        return true;
    };
    let end = start.saturating_add(span);
    leaf_ordinal >= start && leaf_ordinal < end
}

fn routing_parent_ordinal_for_leaf(
    routing_level: u8,
    leaf_page_ordinal: usize,
    routing_page_fanout: usize,
) -> Result<usize> {
    let Some(span) = routing_leaf_page_span(routing_level, routing_page_fanout) else {
        return Err(BorsukError::InvalidStorage(
            "routing leaf page span overflow".to_string(),
        ));
    };
    Ok(leaf_page_ordinal / span)
}

fn routing_leaf_page_span(routing_level: u8, routing_page_fanout: usize) -> Option<usize> {
    let mut span = 1_usize;
    for _ in 0..routing_level {
        span = span.checked_mul(routing_page_fanout)?;
    }
    Some(span)
}

fn routing_code_distance(segment: &Segment, record_index: usize, query_code: f32) -> f32 {
    let code = segment
        .routing_codes
        .get(record_index)
        .copied()
        .unwrap_or_else(|| routing_code(&segment.records[record_index].vector));
    (code - query_code).abs()
}

/// Per-query coarse-scoring state, built once per segment. The variant is chosen
/// by the persisted [`QuantizerKind`] and the active leaf mode: a PQ/Vamana leaf
/// scores against the coarse codes, everything else against the scalar routing
/// code.
enum CoarseScorer {
    /// Cheap 1-D routing-code fallback (no PQ leaf).
    RoutingCode { query_code: f32 },
    /// Default symmetric scalar-bounds scoring: the query is quantized to a code
    /// and scored by squared distance between codes.
    ScalarBounds { query_pq_code: Vec<u8> },
    /// Asymmetric TurboQuant scoring: the query is rotated (not quantized) and
    /// each candidate is scored by dequantize-and-dot against its rotated code.
    TurboQuant {
        quantizer: crate::turboquant::TurboQuantizer,
        rotated_query: Vec<f32>,
    },
}

impl CoarseScorer {
    fn for_query(
        segment: &Segment,
        query: &[f32],
        quantizer: QuantizerKind,
        use_pq_leaf: bool,
        query_code: f32,
    ) -> Result<Self> {
        if !use_pq_leaf {
            return Ok(Self::RoutingCode { query_code });
        }
        // The build side downgrades TurboQuant to ScalarBounds for non-pow2 dims;
        // resolve the same way so the query interprets the stored codes correctly.
        match quantizer.effective_for_dimensions(segment.dimensions) {
            QuantizerKind::ScalarBounds => Ok(Self::ScalarBounds {
                query_pq_code: pq_code_for_query(segment, query)?,
            }),
            QuantizerKind::TurboQuant {
                seed,
                bits,
                qjl_bits,
                shards,
            } => {
                // Rebuild the quantizer from the segment's persisted rotated
                // bounds + the manifest seed/bits/qjl_bits/shards; rotate the query
                // once.
                let quantizer = crate::turboquant::TurboQuantizer::from_bounds(
                    seed,
                    segment.dimensions,
                    bits,
                    qjl_bits,
                    shards,
                    segment.pq_min.clone(),
                    segment.pq_max.clone(),
                );
                let rotated_query = quantizer.rotate_query(query);
                Ok(Self::TurboQuant {
                    quantizer,
                    rotated_query,
                })
            }
        }
    }

    fn distance(&self, segment: &Segment, record_index: usize) -> f32 {
        match self {
            Self::RoutingCode { query_code } => {
                routing_code_distance(segment, record_index, *query_code)
            }
            Self::ScalarBounds { query_pq_code } => {
                pq_code_distance(segment, record_index, query_pq_code)
            }
            Self::TurboQuant {
                quantizer,
                rotated_query,
            } => {
                let Some(code) = segment.pq_codes.get(record_index) else {
                    return f32::INFINITY;
                };
                quantizer.coarse_distance(rotated_query, code)
            }
        }
    }
}

fn pq_code_distance(segment: &Segment, record_index: usize, query_code: &[u8]) -> f32 {
    let Some(code) = segment.pq_codes.get(record_index) else {
        return f32::INFINITY;
    };

    crate::metric::squared_u8_euclidean_simd(code, query_code)
}

fn push_hit_with_vector(
    hits: &mut Vec<SearchHitWithVector>,
    hit: SearchHit,
    vector: Option<Vec<f32>>,
    k: usize,
) {
    hits.push(SearchHitWithVector { hit, vector });
    hits.sort_by(|left, right| {
        left.hit
            .distance
            .partial_cmp(&right.hit.distance)
            .unwrap_or(Ordering::Equal)
            .then_with(|| left.hit.id.cmp(&right.hit.id))
    });
    hits.truncate(k);
}

fn merge_search_execution_hits(
    base: &mut SearchExecution,
    delta: SearchExecution,
    k: usize,
    include_vectors: bool,
) {
    let SearchExecution {
        report: mut delta_report,
        vectors: delta_vectors,
    } = delta;
    let base_vectors = std::mem::take(&mut base.vectors);
    let mut newest = HashMap::<RecordId, SearchHitWithVector>::new();
    for (index, hit) in std::mem::take(&mut base.report.hits)
        .into_iter()
        .enumerate()
    {
        newest.insert(
            hit.id.clone(),
            SearchHitWithVector {
                hit,
                vector: include_vectors
                    .then(|| base_vectors.get(index).cloned())
                    .flatten(),
            },
        );
    }
    for (index, hit) in std::mem::take(&mut delta_report.hits)
        .into_iter()
        .enumerate()
    {
        newest.insert(
            hit.id.clone(),
            SearchHitWithVector {
                hit,
                vector: include_vectors
                    .then(|| delta_vectors.get(index).cloned())
                    .flatten(),
            },
        );
    }
    let mut ranked = newest.into_values().collect::<Vec<_>>();
    ranked.sort_by(|left, right| {
        left.hit
            .distance
            .total_cmp(&right.hit.distance)
            .then_with(|| left.hit.id.cmp(&right.hit.id))
    });
    ranked.truncate(k);
    base.report.hits = ranked.iter().map(|entry| entry.hit.clone()).collect();
    base.vectors = ranked
        .into_iter()
        .filter_map(|entry| entry.vector)
        .collect();

    base.report.segments_total = base
        .report
        .segments_total
        .saturating_add(delta_report.segments_total);
    base.report.segments_searched = base
        .report
        .segments_searched
        .saturating_add(delta_report.segments_searched);
    base.report.segments_skipped = base
        .report
        .segments_skipped
        .saturating_add(delta_report.segments_skipped);
    base.report.routing_page_indexes_read = base
        .report
        .routing_page_indexes_read
        .saturating_add(delta_report.routing_page_indexes_read);
    base.report.routing_pages_read = base
        .report
        .routing_pages_read
        .saturating_add(delta_report.routing_pages_read);
    base.report.bytes_read = base
        .report
        .bytes_read
        .saturating_add(delta_report.bytes_read);
    base.report.prefetched_bytes_unused = base
        .report
        .prefetched_bytes_unused
        .saturating_add(delta_report.prefetched_bytes_unused);
    base.report.graph_bytes_read = base
        .report
        .graph_bytes_read
        .saturating_add(delta_report.graph_bytes_read);
    base.report.decoded_cache_hits = base
        .report
        .decoded_cache_hits
        .saturating_add(delta_report.decoded_cache_hits);
    base.report.decoded_cache_bytes_read = base
        .report
        .decoded_cache_bytes_read
        .saturating_add(delta_report.decoded_cache_bytes_read);
    base.report.object_cache_hits = base
        .report
        .object_cache_hits
        .saturating_add(delta_report.object_cache_hits);
    base.report.object_cache_misses = base
        .report
        .object_cache_misses
        .saturating_add(delta_report.object_cache_misses);
    base.report.cache_repairs = base
        .report
        .cache_repairs
        .saturating_add(delta_report.cache_repairs);
    base.report.records_considered = base
        .report
        .records_considered
        .saturating_add(delta_report.records_considered);
    base.report.records_scored = base
        .report
        .records_scored
        .saturating_add(delta_report.records_scored);
    base.report.graph_candidates_added = base
        .report
        .graph_candidates_added
        .saturating_add(delta_report.graph_candidates_added);
    base.report.global_graph_chunks_searched = base
        .report
        .global_graph_chunks_searched
        .saturating_add(delta_report.global_graph_chunks_searched);
    base.report.global_scan_chunks_searched = base
        .report
        .global_scan_chunks_searched
        .saturating_add(delta_report.global_scan_chunks_searched);
    base.report.resident_bytes_estimate = base
        .report
        .resident_bytes_estimate
        .max(delta_report.resident_bytes_estimate);
    base.report.rows_evaluated = base
        .report
        .rows_evaluated
        .saturating_add(delta_report.rows_evaluated);
    base.report.rows_passed_filter = base
        .report
        .rows_passed_filter
        .saturating_add(delta_report.rows_passed_filter);
    base.report.segments_pruned_by_filter = base
        .report
        .segments_pruned_by_filter
        .saturating_add(delta_report.segments_pruned_by_filter);
}

/// Derive an [`ExplainReport`] (plan + estimated cost) from a measured search.
fn explain_from_report(report: SearchReport, cost: QueryCostModel) -> ExplainReport {
    let get_requests = report.requests.gets.saturating_add(report.requests.heads);
    let cache_lookups = report.object_cache_hits + report.object_cache_misses;
    let cache_hit_ratio = if cache_lookups == 0 {
        1.0
    } else {
        report.object_cache_hits as f64 / cache_lookups as f64
    };
    ExplainReport {
        hits: report.hits.clone(),
        leaf_mode: report.leaf_mode.clone(),
        segments_total: report.segments_total,
        segments_searched: report.segments_searched,
        segments_skipped: report.segments_skipped,
        segments_pruned_by_filter: report.segments_pruned_by_filter,
        get_requests,
        bytes_read: report.bytes_read,
        cache_hit_ratio,
        elapsed_ms: report.elapsed_ms,
        estimated_cost_usd: cost.estimate_usd(get_requests, report.bytes_read),
        report,
    }
}

fn fuse_hybrid_hits(
    reports: &[(String, SearchReport)],
    fusion: &Fusion,
    k: usize,
) -> Vec<SearchHit> {
    let mut candidates = BTreeMap::<Vec<u8>, HybridCandidate>::new();
    match fusion {
        Fusion::Rrf { k: rank_constant } => {
            for (modality, report) in reports {
                for (rank, hit) in report.hits.iter().enumerate() {
                    let denominator = *rank_constant as f32 + rank as f32;
                    let score = if denominator == 0.0 {
                        f32::INFINITY
                    } else {
                        1.0 / denominator
                    };
                    add_hybrid_score(&mut candidates, modality, hit, score);
                }
            }
        }
        Fusion::Weighted { weights } => {
            for (modality, report) in reports {
                let weight = weights.get(modality).copied().unwrap_or(1.0);
                let Some((min_distance, max_distance)) = distance_range(&report.hits) else {
                    continue;
                };
                for hit in &report.hits {
                    let similarity =
                        normalized_similarity(hit.distance, min_distance, max_distance);
                    add_hybrid_score(&mut candidates, modality, hit, weight * similarity);
                }
            }
        }
    }

    let mut fused = candidates.into_values().collect::<Vec<_>>();
    fused.sort_by(|left, right| {
        right
            .combined_score
            .total_cmp(&left.combined_score)
            .then_with(|| left.id.as_bytes().cmp(right.id.as_bytes()))
    });
    fused.truncate(k);
    fused
        .into_iter()
        .map(|candidate| SearchHit {
            id: candidate.id,
            distance: -candidate.combined_score,
            metadata: candidate.metadata,
        })
        .collect()
}

fn add_hybrid_score(
    candidates: &mut BTreeMap<Vec<u8>, HybridCandidate>,
    modality: &str,
    hit: &SearchHit,
    score: f32,
) {
    let candidate = candidates
        .entry(hit.id.as_bytes().to_vec())
        .or_insert_with(|| HybridCandidate {
            id: hit.id.clone(),
            combined_score: 0.0,
            metadata: None,
        });
    candidate.combined_score += score;
    if modality == HYBRID_TEXT_MODALITY {
        if candidate.metadata.is_none() {
            candidate.metadata = hit.metadata.clone();
        }
    } else if hit.metadata.is_some() {
        candidate.metadata = hit.metadata.clone();
    }
}

fn distance_range(hits: &[SearchHit]) -> Option<(f32, f32)> {
    let first = hits.first()?;
    let mut min_distance = first.distance;
    let mut max_distance = first.distance;
    for hit in &hits[1..] {
        min_distance = min_distance.min(hit.distance);
        max_distance = max_distance.max(hit.distance);
    }
    Some((min_distance, max_distance))
}

fn normalized_similarity(distance: f32, min_distance: f32, max_distance: f32) -> f32 {
    if min_distance == max_distance {
        1.0
    } else {
        1.0 - (distance - min_distance) / (max_distance - min_distance)
    }
}

fn sum_hybrid_requests(reports: &[(String, SearchReport)]) -> RequestCounts {
    reports
        .iter()
        .fold(RequestCounts::default(), |mut total, (_, report)| {
            total.gets = total.gets.saturating_add(report.requests.gets);
            total.puts = total.puts.saturating_add(report.requests.puts);
            total.deletes = total.deletes.saturating_add(report.requests.deletes);
            total.heads = total.heads.saturating_add(report.requests.heads);
            total.lists = total.lists.saturating_add(report.requests.lists);
            total
        })
}

fn validate_named_vector_config(named_vectors: &BTreeMap<String, VectorSpec>) -> Result<()> {
    for (name, spec) in named_vectors {
        validate_named_vector_name(name)?;
        if spec.dimensions == 0 {
            return Err(BorsukError::InvalidMetricInput(format!(
                "named vector `{name}` dimensions must be greater than zero"
            )));
        }
        if spec.kind == VectorKind::Sparse && spec.metric != VectorMetric::InnerProduct {
            return Err(BorsukError::InvalidMetricInput(format!(
                "sparse named vectors support the inner-product metric only, got {:?}",
                spec.metric
            )));
        }
        if spec.kind == VectorKind::Sparse
            && !matches!(
                spec.element_type,
                crate::VectorElementType::Float32 | crate::VectorElementType::Float16
            )
        {
            return Err(BorsukError::InvalidMetricInput(format!(
                "sparse named vector `{name}` supports float32 or float16 values, got {}",
                spec.element_type
            )));
        }
        if spec.kind == VectorKind::LateInteraction && spec.metric != VectorMetric::InnerProduct {
            return Err(BorsukError::InvalidMetricInput(format!(
                "late-interaction named vector `{name}` requires inner-product MaxSim, got {:?}",
                spec.metric
            )));
        }
        if spec.kind == VectorKind::LateInteraction
            && !matches!(
                spec.element_type,
                crate::VectorElementType::Float32 | crate::VectorElementType::Float16
            )
        {
            return Err(BorsukError::InvalidMetricInput(format!(
                "late-interaction named vector `{name}` supports float32 or float16 values, got {}",
                spec.element_type
            )));
        }
        if spec.kind == VectorKind::Dense {
            validate_vector_element_metric(
                &format!("named vector `{name}`"),
                spec.element_type,
                &spec.metric,
            )?;
        }
    }
    Ok(())
}

fn validate_vector_element_metric(
    label: &str,
    element_type: crate::VectorElementType,
    metric: &VectorMetric,
) -> Result<()> {
    if element_type == crate::VectorElementType::Binary
        && !matches!(metric, VectorMetric::Hamming | VectorMetric::Jaccard)
    {
        return Err(BorsukError::InvalidMetricInput(format!(
            "{label} with binary elements requires hamming or jaccard, got {metric}"
        )));
    }
    Ok(())
}

fn validate_named_vector_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(BorsukError::InvalidMetricInput(
            "named vector name must not be empty; the empty name is reserved for the primary vector"
                .to_string(),
        ));
    }
    if name.contains('/') || name.contains('\\') {
        return Err(BorsukError::InvalidMetricInput(format!(
            "named vector `{name}` must be a single path component"
        )));
    }
    Ok(())
}

fn named_vector_child_uri(primary_uri: &str, name: &str) -> String {
    if let Ok(mut url) = Url::parse(primary_uri) {
        let base = url.path().trim_end_matches('/');
        let path = if base.is_empty() {
            format!("/vectors/{name}")
        } else {
            format!("{base}/vectors/{name}")
        };
        url.set_path(&path);
        return url.to_string();
    }

    let mut path = PathBuf::from(primary_uri);
    path.push("vectors");
    path.push(name);
    path.to_string_lossy().into_owned()
}

const LATE_INTERACTION_TOKEN_ID_MAGIC: &[u8; 4] = b"BLI1";

fn encode_late_interaction_token_id(
    entity_id: &[u8],
    generation: u64,
    token_index: usize,
) -> Result<Vec<u8>> {
    let id_len = u32::try_from(entity_id.len()).map_err(|_| {
        BorsukError::InvalidRecordInput("late-interaction entity id exceeds u32 bytes".to_string())
    })?;
    let token_index = u32::try_from(token_index).map_err(|_| {
        BorsukError::InvalidRecordInput("late-interaction token index exceeds u32".to_string())
    })?;
    let mut encoded =
        Vec::with_capacity(LATE_INTERACTION_TOKEN_ID_MAGIC.len() + 4 + entity_id.len() + 8 + 4);
    encoded.extend_from_slice(LATE_INTERACTION_TOKEN_ID_MAGIC);
    encoded.extend_from_slice(&id_len.to_le_bytes());
    encoded.extend_from_slice(entity_id);
    encoded.extend_from_slice(&generation.to_le_bytes());
    encoded.extend_from_slice(&token_index.to_le_bytes());
    Ok(encoded)
}

fn decode_late_interaction_token_id(encoded: &[u8]) -> Result<(&[u8], u64, u32)> {
    let header = LATE_INTERACTION_TOKEN_ID_MAGIC.len() + 4;
    if encoded.len() < header + 8 + 4
        || &encoded[..LATE_INTERACTION_TOKEN_ID_MAGIC.len()] != LATE_INTERACTION_TOKEN_ID_MAGIC
    {
        return Err(BorsukError::InvalidStorage(
            "late-interaction token id has invalid framing".to_string(),
        ));
    }
    let id_len = u32::from_le_bytes(
        encoded[LATE_INTERACTION_TOKEN_ID_MAGIC.len()..header]
            .try_into()
            .expect("four-byte late-interaction id length"),
    ) as usize;
    let entity_end = header.checked_add(id_len).ok_or_else(|| {
        BorsukError::InvalidStorage("late-interaction token id length overflows".to_string())
    })?;
    let expected_end = entity_end.checked_add(12).ok_or_else(|| {
        BorsukError::InvalidStorage("late-interaction token id length overflows".to_string())
    })?;
    if expected_end != encoded.len() {
        return Err(BorsukError::InvalidStorage(
            "late-interaction token id has invalid length".to_string(),
        ));
    }
    let generation = u64::from_le_bytes(
        encoded[entity_end..entity_end + 8]
            .try_into()
            .expect("eight-byte late-interaction generation"),
    );
    let token_index = u32::from_le_bytes(
        encoded[entity_end + 8..expected_end]
            .try_into()
            .expect("four-byte late-interaction token index"),
    );
    Ok((&encoded[header..entity_end], generation, token_index))
}

#[allow(clippy::too_many_arguments)]
fn search_stop_reason_before_segment(
    hits: &[SearchHitWithVector],
    k: usize,
    mode: &SearchMode,
    searched_segments: usize,
    stale_segments: usize,
    bytes_read: u64,
    lower_bound: f32,
    elapsed_ms: u64,
) -> Option<SearchTerminationReason> {
    match mode {
        SearchMode::Exact => hits
            .get(k.saturating_sub(1))
            .is_some_and(|best_k| lower_bound > best_k.hit.distance)
            .then_some(SearchTerminationReason::ExactPruned),
        SearchMode::Approx {
            leaf_mode: _,
            eps,
            max_segments,
            max_bytes,
            max_latency_ms,
            routing_page_overfetch: _,
            max_candidates_per_segment: _,
            adaptive_stop,
            projected_reads: _,
        } => {
            if max_segments.is_some_and(|limit| searched_segments >= limit) {
                return Some(SearchTerminationReason::MaxSegments);
            }

            if max_bytes.is_some_and(|limit| bytes_read >= limit) {
                return Some(SearchTerminationReason::MaxBytes);
            }

            if max_latency_ms.is_some_and(|limit| elapsed_ms >= limit) {
                return Some(SearchTerminationReason::MaxLatency);
            }

            // Adaptive early-stop: the running top-k is full and has not improved
            // for `patience` consecutive segments, so the query has almost
            // certainly converged — stop before paying for more segment reads.
            if let Some(patience) = adaptive_stop
                && hits.len() >= k
                && stale_segments >= *patience
            {
                return Some(SearchTerminationReason::AdaptiveStop);
            }

            if let (Some(eps), Some(best_k)) = (eps, hits.get(k.saturating_sub(1))) {
                return (lower_bound >= best_k.hit.distance / (1.0 + eps))
                    .then_some(SearchTerminationReason::Epsilon);
            }

            None
        }
    }
}

fn search_prefetch_segment_budget_exhausted(mode: &SearchMode, reserved_segments: usize) -> bool {
    match mode {
        SearchMode::Exact => false,
        SearchMode::Approx { max_segments, .. } => {
            max_segments.is_some_and(|limit| reserved_segments >= limit)
        }
    }
}

fn search_prefetch_byte_budget_exhausted(mode: &SearchMode, reserved_bytes: u64) -> bool {
    match mode {
        SearchMode::Exact => false,
        SearchMode::Approx { max_bytes, .. } => {
            max_bytes.is_some_and(|limit| reserved_bytes >= limit)
        }
    }
}

/// Margin added to the probe-budget cutoff so pages whose centroid sits within
/// a query-scaled tolerance of the budget boundary are still read (boundary
/// overfetch). Tight for few pages, scaled by the query magnitude otherwise.
fn routing_lower_bound_overfetch_margin(query: &[f32], ranked_page_count: usize) -> f32 {
    if ranked_page_count <= ROUTING_SEARCH_PAGE_OVERFETCH * 2 {
        return 1.0e-6;
    }

    query
        .iter()
        .map(|value| value.abs())
        .fold(1.0_f32, f32::max)
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use chrono::Utc;

    use super::*;

    #[test]
    fn packed_id_directory_and_coordination_counters_round_trip_and_reject_corruption() {
        let entries = vec![
            CellWalIdDirectoryEntry {
                id: vec![0, 255, 1],
                owner: LogicalCellId::new(4, 8),
                generation: 12,
                deleted: true,
            },
            CellWalIdDirectoryEntry {
                id: b"alpha".to_vec(),
                owner: LogicalCellId::new(3, 7),
                generation: 11,
                deleted: false,
            },
        ];
        let encoded = cell_wal_id_directory_bytes(&entries).unwrap();
        assert!(encoded.starts_with(ID_DIRECTORY_MAGIC));
        assert_eq!(
            cell_wal_id_directory_from_slice(&encoded, "id-directory.bin").unwrap(),
            entries
        );

        let counter = coordination_counter_bytes(42);
        assert!(counter.starts_with(COORDINATION_COUNTER_MAGIC));
        assert_eq!(
            coordination_counter_from_slice(&counter, "NEXT").unwrap(),
            42
        );

        let mut corrupted = encoded;
        corrupted[9] ^= 1;
        let error = cell_wal_id_directory_from_slice(&corrupted, "id-directory.bin").unwrap_err();
        assert!(error.to_string().contains("checksum mismatch"), "{error}");
    }

    #[test]
    fn packed_cell_wal_metadata_round_trips_and_rejects_corruption() {
        let mutation = CellWalMutationMetadata {
            new_tombstone_ids: 9,
            next_generated_id_floor: 42,
            bm25_stats_delta: Some(Bm25StatsDeltaRef {
                document_count_delta: -2,
                total_document_length_delta: -17,
                pages: vec![Bm25StatsDeltaPageRef {
                    first_term: 3,
                    last_term: 11,
                    path: "lexical/bm25-stats-delta/page.parquet".to_string(),
                    checksum: "ab".repeat(32),
                    encoded_bytes: 1234,
                    term_count: 5,
                }],
            }),
        };
        let mutation_bytes = cell_wal_mutation_metadata_bytes(&mutation).unwrap();
        assert!(mutation_bytes.starts_with(CELL_WAL_MUTATION_METADATA_MAGIC));
        assert_eq!(
            cell_wal_mutation_metadata_from_slice(&mutation_bytes, "descriptor").unwrap(),
            mutation
        );

        let tombstone = CellWalTombstoneMetadata {
            id_bloom: vec![0, 1, 255, 7],
            created_at: DateTime::<Utc>::from_timestamp(1_724_321_234, 987_654_321).unwrap(),
        };
        let tombstone_bytes = cell_wal_tombstone_metadata_bytes(&tombstone).unwrap();
        assert!(tombstone_bytes.starts_with(CELL_WAL_TOMBSTONE_METADATA_MAGIC));
        assert_eq!(
            cell_wal_tombstone_metadata_from_slice(&tombstone_bytes, "tombstones.parquet").unwrap(),
            tombstone
        );

        let mut corrupted = mutation_bytes;
        corrupted[12] ^= 1;
        let error = cell_wal_mutation_metadata_from_slice(&corrupted, "descriptor").unwrap_err();
        assert!(error.to_string().contains("checksum mismatch"), "{error}");
    }

    #[test]
    fn cell_wal_id_directory_lookup_reads_only_its_hash_partition() {
        let directory = tempfile::tempdir().unwrap();
        let mut index = BorsukIndex::create(IndexConfig {
            uri: directory.path().to_string_lossy().into_owned(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 4,
            ram_budget_bytes: None,
            text: false,
            named_vectors: BTreeMap::new(),
        })
        .unwrap();
        let left = LogicalCellId::new(1, 0);
        let right = LogicalCellId::new(1, 1);
        index.manifest.logical_cells = vec![left, right];
        let (left_id, right_id) = (0_u64..10_000)
            .map(|ordinal| format!("partition-{ordinal}").into_bytes())
            .fold((None, None), |(left_id, right_id), id| {
                match index.id_directory_partition(&id) {
                    cell if cell == left && left_id.is_none() => (Some(id), right_id),
                    cell if cell == right && right_id.is_none() => (left_id, Some(id)),
                    _ => (left_id, right_id),
                }
            });
        let left_id = left_id.unwrap();
        let right_id = right_id.unwrap();
        let run = |id: Vec<u8>, owner| {
            cell_wal_id_directory_bytes(&[CellWalIdDirectoryEntry {
                id,
                owner,
                generation: 1,
                deleted: false,
            }])
            .unwrap()
        };
        let metadata =
            cell_wal_mutation_metadata_bytes(&CellWalMutationMetadata::default()).unwrap();
        let committed = index
            .cell_wal_store()
            .unwrap()
            .commit_with_metadata(
                "partitioned-directory",
                &[
                    CellWalRunInput {
                        cell: left,
                        kind: CellWalRunKind::IdDirectory,
                        metadata: Vec::new(),
                        bytes: run(left_id.clone(), left),
                        record_count: 1,
                        extension: "bin".to_string(),
                    },
                    CellWalRunInput {
                        cell: right,
                        kind: CellWalRunKind::IdDirectory,
                        metadata: Vec::new(),
                        bytes: run(right_id, right),
                        record_count: 1,
                        extension: "bin".to_string(),
                    },
                ],
                &metadata,
            )
            .unwrap();
        index.cell_wal_snapshot = vec![committed];

        let before = index.storage.request_counts();
        let found = index
            .cell_wal_id_directory_entries(std::iter::once(left_id.as_slice()))
            .unwrap()
            .remove(&left_id)
            .unwrap();
        let requests = index.storage.request_counts().delta(&before);

        assert_eq!(found.owner, left);
        assert_eq!(
            requests.gets, 1,
            "lookup fetched an unrelated ID-directory partition"
        );
    }

    #[test]
    fn default_open_options_bound_concurrent_searches() {
        assert_eq!(crate::DEFAULT_BUILD_THREADS, 4);
        assert_eq!(
            OpenOptions::default().ram_budget_bytes,
            Some(512 * 1024 * 1024)
        );
        assert_eq!(
            OpenOptions::default().max_concurrent_searches,
            Some(DEFAULT_MAX_CONCURRENT_SEARCHES)
        );
        assert_eq!(OpenOptions::default().max_concurrent_cell_decodes, Some(24));
        assert_eq!(
            SearchOptions::default().prefetch_depth,
            16,
            "the production query default should overlap S3 waits up to the bounded per-query width"
        );
        assert!(matches!(
            SearchOptions::default().mode,
            SearchMode::Approx {
                leaf_mode: LeafMode::SrhtPqScan,
                ..
            }
        ));
    }

    #[test]
    fn mse_only_turboquant_rejects_a_partial_qjl_stage() {
        let error = validate_build_config(
            &BuildConfig {
                global_scan_codec: GlobalScanCodec::FastTurboQuantMse,
                global_turboquant_qjl_bits: 16,
                ..BuildConfig::default()
            },
            96,
        )
        .unwrap_err();
        assert!(error.to_string().contains("choose fast-turboquant-scan"));
    }

    #[test]
    fn sidecar_index_cache_never_retains_more_than_its_byte_cap() {
        let bytes =
            crate::arrow_vector_sidecar::encode_vector_sidecar(&vec![vec![1.0_f32, 2.0]; 32], 2)
                .unwrap();
        let index = Arc::new(crate::arrow_vector_sidecar::parse(&bytes).unwrap());
        let one_index = index.resident_bytes();
        let mut cache = SidecarIndexCache::with_max_bytes(one_index);
        cache.insert("first".to_string(), Arc::clone(&index));
        cache.insert("second".to_string(), Arc::clone(&index));
        assert!(cache.bytes <= one_index);
        assert!(cache.get("first").is_none());
        assert!(cache.get("second").is_some());

        let mut too_small = SidecarIndexCache::with_max_bytes(one_index - 1);
        too_small.insert("oversized".to_string(), index);
        assert_eq!(too_small.bytes, 0);
        assert!(too_small.entries.is_empty());
    }

    #[test]
    fn global_pq_layout_scales_code_width_and_angular_rerank_budget() {
        assert_eq!(
            resolved_global_pq_layout(
                &crate::GlobalPqLayout::Product2x64,
                &VectorMetric::Cosine,
                100,
                1_183_514,
            ),
            ResolvedGlobalPqLayout::Product {
                subspaces: 2,
                centroids: 64,
            }
        );
        assert_eq!(
            resolved_global_pq_layout(
                &crate::GlobalPqLayout::Hierarchical {
                    children_per_parent: 8,
                },
                &VectorMetric::Cosine,
                100,
                1_183_514,
            ),
            ResolvedGlobalPqLayout::Hierarchical {
                children_per_parent: 8,
            }
        );
        assert!(resident_global_pq_uses_flat_coarse(
            &VectorMetric::Cosine,
            100,
            1_183_514
        ));
        assert!(!resident_global_pq_uses_flat_coarse(
            &VectorMetric::Euclidean,
            128,
            1_000_000
        ));
        assert!(!resident_global_pq_uses_flat_coarse(
            &VectorMetric::Cosine,
            96,
            9_990_000
        ));
        assert_eq!(
            resident_global_pq_product_coarse_subspaces(&VectorMetric::Cosine, 1_183_514),
            Some(1)
        );
        assert_eq!(
            resident_global_pq_product_coarse_subspaces(&VectorMetric::Cosine, 9_990_000),
            None,
            "the fresh Deep hierarchy dominates the rejected 2x64 product router"
        );
        assert_eq!(
            resident_global_pq_product_coarse_subspaces(&VectorMetric::Euclidean, 1_000_000),
            None
        );
        assert_eq!(resident_global_pq_coarse_children(100, 1_183_514), 16);
        assert_eq!(resident_global_pq_coarse_children(128, 1_000_000), 16);
        assert_eq!(resident_global_pq_coarse_children(960, 1_000_000), 16);
        assert_eq!(resident_global_pq_coarse_children(96, 9_990_000), 64);
        assert_eq!(resident_global_pq_coarse_children(96, 100_000_000), 256);
        assert_eq!(resident_global_pq_coarse_children(4_096, 100_000_000), 32);
        assert_eq!(resident_global_pq_subspaces(100, 1_183_514, None), 64);
        assert_eq!(resident_global_pq_subspaces(96, 9_990_000, None), 64);
        assert_eq!(resident_global_pq_subspaces(256, 290_000, None), 128);
        assert_eq!(resident_global_pq_subspaces(256, 290_000, Some(256)), 256);
        assert_eq!(
            resident_global_pq_subspaces(96, 100_000_000, None),
            64,
            "100M defaults must not double sequential scan bytes and ADC CPU"
        );
        assert_eq!(resident_global_pq_subspaces(784, 60_000, None), 64);
        assert_eq!(
            resident_global_pq_subspaces(960, 1_000_000, None),
            256,
            "fresh GIST evidence selects the wider code without increasing build RSS"
        );

        assert_eq!(
            resident_global_pq_candidates(&VectorMetric::Cosine, 100, 64, 1_183_514),
            184
        );
        assert_eq!(
            resident_global_pq_candidates(&VectorMetric::Cosine, 96, 64, 9_990_000),
            200,
            "large angular corpora need enough shortlist headroom to retain recall"
        );
        assert_eq!(
            resident_global_pq_candidates(&VectorMetric::Cosine, 96, 64, 100_000_000),
            200,
            "100M uses the narrower code with a still-bounded lossless shortlist"
        );
        assert_eq!(
            resident_global_pq_candidates(&VectorMetric::Cosine, 256, 64, 290_000),
            320
        );
        assert_eq!(
            resident_global_pq_candidates(&VectorMetric::Cosine, 256, 128, 290_000),
            288,
            "NYTimes code128 candidate sweep plateaus at 288; larger reranks waste exact I/O"
        );
        assert_eq!(
            resident_global_pq_candidates(&VectorMetric::Euclidean, 960, 128, 1_000_000),
            768,
            "the code128 GIST control needs a wide lossless shortlist"
        );
        assert_eq!(
            resident_global_pq_candidates(&VectorMetric::Euclidean, 960, 256, 1_000_000),
            96,
            "code256 GIST reaches 0.995 below 400 ms with a 96-row lossless shortlist"
        );
        assert_eq!(
            resident_global_pq_candidates(&VectorMetric::Euclidean, 784, 64, 60_000),
            320,
            "Fashion-MNIST default should meet the directly measured S3 Vectors recall target"
        );
        assert_eq!(
            resident_global_pq_probes(&VectorMetric::Euclidean, 784, 256),
            8
        );
        assert_eq!(
            resident_global_pq_probes(&VectorMetric::Euclidean, 960, 1_024),
            24,
            "code256 GIST reaches 0.997 at 24 probes; wider routing is dominated"
        );
        assert_eq!(
            resident_global_pq_probes(&VectorMetric::Cosine, 256, 256),
            223,
            "NYTimes code128 first reaches its 0.993 ceiling at 223 flat cells"
        );
        assert_eq!(
            resident_global_pq_probes(&VectorMetric::Cosine, 100, 1_024),
            128,
            "the hierarchy stays conservative until its AWS curve selects a lower probe count"
        );
        let deep_100m_segments = 100_000_000_usize.div_ceil(recommended_segment_max_vectors(96));
        // At this scale the adaptive angular layout uses 64 full-dimensional
        // parents with 256 children each. Physical segments remain bounded
        // ingest and object-store units; they are not the query-routing fan-out.
        let deep_100m_coarse_cells = 16_384;
        let deep_100m_probes =
            resident_global_pq_probes(&VectorMetric::Cosine, 96, deep_100m_coarse_cells);
        assert_eq!(recommended_segment_max_vectors(96), 43_690);
        assert_eq!(deep_100m_segments, 2_289);
        assert_eq!(deep_100m_probes, 256);
        assert!(
            deep_100m_probes * 64 <= deep_100m_coarse_cells,
            "100M Deep-Image probes no more than 1/64 of the coarse routing space by default"
        );
    }

    #[test]
    fn global_pq_code_read_waves_are_bounded_by_count_and_bytes() {
        let chunk = |size_bytes| GlobalPqChunkRef {
            path: format!("chunk-{size_bytes}"),
            checksum: "checksum".to_string(),
            offset_bytes: 0,
            exact_checksum: "exact-checksum".into(),
            exact_offset_bytes: 0,
            exact_size_bytes: 0,
            cell_index: 0,
            row_start: 0,
            rows: 1,
            size_bytes,
            graph: None,
        };
        let chunks = vec![chunk(20), chunk(20), chunk(20), chunk(20)];
        assert_eq!(global_pq_code_read_wave_end(&chunks, 0, 32, 55), 2);
        assert_eq!(global_pq_code_read_wave_end(&chunks, 2, 32, 55), 4);
        assert_eq!(global_pq_code_read_wave_end(&chunks, 0, 1, 1_000), 1);

        // One unusually large object is irreducible, but the next object must
        // wait for a later wave instead of multiplying the oversize allocation.
        let oversized = vec![chunk(80), chunk(10)];
        assert_eq!(global_pq_code_read_wave_end(&oversized, 0, 32, 55), 1);
    }

    #[test]
    fn global_pq_code_reads_do_not_span_distant_unselected_bundle_slices() {
        let chunk = |cell_index, offset_bytes, size_bytes| GlobalPqChunkRef {
            path: "packed-bundle".to_string(),
            checksum: format!("checksum-{cell_index}"),
            offset_bytes,
            exact_checksum: "exact-checksum".into(),
            exact_offset_bytes: 0,
            exact_size_bytes: 0,
            cell_index,
            row_start: 0,
            rows: 1,
            size_bytes,
            graph: None,
        };
        let selected = vec![chunk(1, 0, 100), chunk(2, 120, 100), chunk(3, 200_000, 100)];

        let groups = global_pq_code_read_groups(&selected, 64 * 1024, 64 * 1024).unwrap();

        assert_eq!(groups.len(), 2);
        assert_eq!(
            groups[0]
                .1
                .iter()
                .map(|chunk| chunk.cell_index)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(
            groups[1]
                .1
                .iter()
                .map(|chunk| chunk.cell_index)
                .collect::<Vec<_>>(),
            vec![3]
        );
    }

    #[test]
    fn global_pq_code_reads_merge_parent_local_bundle_gaps() {
        let chunk = |cell_index, offset_bytes| GlobalPqChunkRef {
            path: "parent-bundle".to_string(),
            checksum: format!("checksum-{cell_index}"),
            offset_bytes,
            exact_checksum: "exact-checksum".into(),
            exact_offset_bytes: 1_000_000,
            exact_size_bytes: 4,
            cell_index,
            row_start: cell_index as usize,
            rows: 1,
            size_bytes: 100,
            graph: None,
        };
        let selected = vec![chunk(1, 0), chunk(2, 200_000), chunk(3, 400_000)];

        let groups = global_pq_code_read_groups(&selected, 1024 * 1024, 1024 * 1024).unwrap();

        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].1.len(), 3);
    }

    #[test]
    fn global_pq_bundle_is_standard_arrow_with_independent_scan_and_exact_ranges() {
        let chunk = |codes: &[u8], locations: &[u8], exact_bytes: Vec<u8>| {
            let mut bytes = Vec::new();
            bytes.extend_from_slice(codes);
            bytes.extend_from_slice(locations);
            crate::global_pq_sidecar::GlobalPqChunkBytes {
                bytes,
                exact_bytes,
                rows: 1,
            }
        };
        let pending = vec![
            PendingGlobalPqChunk {
                cell_index: 3,
                row_start: 0,
                chunk: chunk(
                    &[1, 2],
                    &7_u32.to_le_bytes(),
                    [1.0_f32, 2.0]
                        .into_iter()
                        .flat_map(f32::to_le_bytes)
                        .collect(),
                ),
            },
            PendingGlobalPqChunk {
                cell_index: 4,
                row_start: 1,
                chunk: chunk(
                    &[3, 4],
                    &8_u32.to_le_bytes(),
                    [3.0_f32, 4.0]
                        .into_iter()
                        .flat_map(f32::to_le_bytes)
                        .collect(),
                ),
            },
        ];
        let encoded = encode_global_pq_arrow_bundle(
            &pending,
            2,
            LocationEncoding::for_layout(1, 65_536).unwrap(),
            2,
            crate::VectorElementType::Float32,
        )
        .unwrap();
        assert!(encoded.bytes.starts_with(b"ARROW1"));
        assert!(encoded.bytes.ends_with(b"ARROW1"));
        let batches = arrow_ipc::reader::FileReader::try_new(
            std::io::Cursor::new(encoded.bytes.clone()),
            None,
        )
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].schema().field(0).name(), "scan_payload");
        assert_eq!(batches[0].schema().field(1).name(), "exact_vector");
        assert_eq!(
            &encoded.bytes[encoded.slices[0].code_range.clone()],
            &[1, 2, 7, 0, 0, 0]
        );
        assert_eq!(
            &encoded.bytes[encoded.slices[1].code_range.clone()],
            &[3, 4, 8, 0, 0, 0]
        );
        assert_eq!(
            &encoded.bytes[encoded.slices[0].exact_range.clone()],
            [1.0_f32, 2.0]
                .into_iter()
                .flat_map(f32::to_le_bytes)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            &encoded.bytes[encoded.slices[1].exact_range.clone()],
            [3.0_f32, 4.0]
                .into_iter()
                .flat_map(f32::to_le_bytes)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn hierarchical_global_pq_bundles_flush_at_parent_boundaries() {
        let parent_two_child_four = u16::from_be_bytes([2, 4]);
        let parent_two_child_seven = u16::from_be_bytes([2, 7]);
        let parent_three_child_one = u16::from_be_bytes([3, 1]);

        assert!(!should_flush_global_pq_bundle(
            Some(parent_two_child_four),
            parent_two_child_seven,
            true,
            1,
            1,
        ));
        assert!(should_flush_global_pq_bundle(
            Some(parent_two_child_seven),
            parent_three_child_one,
            true,
            1,
            1,
        ));
        assert!(!should_flush_global_pq_bundle(
            Some(parent_two_child_seven),
            parent_three_child_one,
            false,
            1,
            1,
        ));
    }

    #[test]
    fn global_pq_ram_budget_accounts_for_artifact_and_sidecar_indexes() {
        let reference = crate::manifest::GlobalPqRef {
            path: "global-pq/descriptor".to_string(),
            checksum: "ab".repeat(32),
            vectors: 9_990_000,
            subspaces: 32,
            candidates: 104,
            probes: 100,
            resident_bytes: 360 * 1024 * 1024,
            sidecar_index_bytes: 120 * 1024 * 1024,
            storage_bytes: 720 * 1024 * 1024,
            segments: vec!["cd".repeat(32)],
        };
        assert!(reference.resident_bytes_estimate() >= 480 * 1024 * 1024);
    }

    #[test]
    fn create_refuses_to_replace_an_existing_index() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_string_lossy().into_owned();
        let config = IndexConfig {
            uri: uri.clone(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 16,
            ram_budget_bytes: None,
            text: false,
            named_vectors: Default::default(),
        };
        let mut original = BorsukIndex::create(config.clone()).unwrap();
        original
            .add(vec![VectorRecord::new("kept", vec![1.0, 2.0])])
            .unwrap();
        drop(original);

        let error = BorsukIndex::create(config).unwrap_err();
        assert!(matches!(
            error,
            BorsukError::InvalidStorage(message)
                if message.contains("already contains a collection")
        ));

        let reopened = BorsukIndex::open(&uri).unwrap();
        assert_eq!(reopened.stats().records, 1);
    }

    #[test]
    fn resident_global_pq_search_skips_routing_and_exactly_reranks_sidecar_rows() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_string_lossy().into_owned();
        let mut index = BorsukIndex::create(IndexConfig {
            uri: uri.clone(),
            metric: VectorMetric::Angular,
            dimensions: 8,
            segment_max_vectors: 256,
            ram_budget_bytes: None,
            text: false,
            named_vectors: Default::default(),
        })
        .unwrap();
        let vectors = (0..128)
            .map(|row| {
                (0..8)
                    .map(|dimension| {
                        (((row + 1) * (dimension + 3) * 17) % 101) as f32 / 100.0 + 0.01
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let records = vectors
            .iter()
            .enumerate()
            .map(|(row, vector)| VectorRecord::new(format!("row-{row}"), vector.clone()))
            .collect::<Vec<_>>();
        index.add(records).unwrap();
        index.finish_bulk_load().unwrap();
        assert!(
            index.manifest.global_pq_ref.is_some(),
            "version={} leaf={:?} active={} resident={}",
            index.manifest.version,
            index.manifest.leaf_capability,
            index.active_segment_summaries().unwrap().len(),
            index.manifest.segments.len()
        );
        drop(index);
        let index = BorsukIndex::open(&uri).unwrap();

        let query = vectors[37]
            .iter()
            .map(|value| value * 3.0)
            .collect::<Vec<_>>();
        let report = index
            .search_with_report(
                &query,
                SearchOptions::approx(5, LeafMode::SrhtPqScan).with_max_segments(8),
            )
            .unwrap();

        assert_eq!(report.hits[0].id, RecordId::from("row-37"));
        assert_eq!(report.routing_page_indexes_read, 0);
        assert_eq!(report.routing_pages_read, 0);
        assert!(report.records_scored <= 64);
        assert!(report.bytes_read > 0);
        assert!(
            report.requests.gets
                <= (report.segments_searched as u64)
                    .saturating_mul(3)
                    .saturating_add(8),
            "exact reads must scale with selected chunks, not exact-scored candidates: {:?}",
            report.requests
        );

        let limited = index
            .search_with_report(
                &query,
                SearchOptions::approx(5, LeafMode::SrhtPqScan)
                    .with_max_segments(8)
                    .with_max_candidates_per_segment(7),
            )
            .unwrap();
        assert!(limited.records_considered >= 7);
        assert!(limited.records_considered < vectors.len());
        assert_eq!(limited.records_scored, 7);
    }

    #[test]
    fn resident_global_pq_remains_the_base_when_a_published_wal_tail_is_visible() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_string_lossy().into_owned();
        let mut writer = BorsukIndex::create(IndexConfig {
            uri: uri.clone(),
            metric: VectorMetric::Euclidean,
            dimensions: 8,
            segment_max_vectors: 256,
            ram_budget_bytes: None,
            text: false,
            named_vectors: Default::default(),
        })
        .unwrap();
        let base = (0..128)
            .map(|row| {
                VectorRecord::new(
                    format!("base-{row}"),
                    (0..8)
                        .map(|dimension| ((row * 17 + dimension * 11) % 101) as f32 / 101.0)
                        .collect(),
                )
            })
            .collect::<Vec<_>>();
        writer.add(base).unwrap();
        writer.finish_bulk_load().unwrap();
        let stable_global_checksum = writer
            .manifest
            .global_pq_ref
            .as_ref()
            .unwrap()
            .checksum
            .clone();

        let tail_vector = vec![10.0; 8];
        writer
            .add(vec![VectorRecord::new("wal-tail", tail_vector.clone())])
            .unwrap();
        assert!(!writer.manifest.wal_frontier_is_empty());
        assert_eq!(
            writer.manifest.global_pq_ref.as_ref().unwrap().checksum,
            stable_global_checksum
        );

        // A separately opened node must use the immutable global artifact for
        // the stable corpus and exact-score the manifest-selected WAL overlay.
        let reader = BorsukIndex::open(&uri).unwrap();
        let report = reader
            .search_with_report(
                &tail_vector,
                SearchOptions::approx(5, LeafMode::SrhtPqScan)
                    .with_max_segments(8)
                    .with_max_candidates_per_segment(64),
            )
            .unwrap();
        assert_eq!(report.hits[0].id, RecordId::from("wal-tail"));
        assert_eq!(report.routing_page_indexes_read, 0);
        assert_eq!(report.routing_pages_read, 0);
        assert!(
            report.global_scan_chunks_searched > 0,
            "the WAL overlay must not disable the immutable global PQ base: {report:?}"
        );

        drop(reader);
        writer.flush().unwrap();
        assert!(writer.manifest.wal_frontier_is_empty());
        assert_eq!(
            writer
                .manifest
                .global_pq_ref
                .as_ref()
                .map(|reference| reference.checksum.as_str()),
            Some(stable_global_checksum.as_str()),
            "flushing a bounded delta must not retrain or discard the stable base"
        );

        let reader = BorsukIndex::open(&uri).unwrap();
        let report = reader
            .search_with_report(
                &tail_vector,
                SearchOptions::approx(5, LeafMode::SrhtPqScan)
                    .with_max_segments(8)
                    .with_max_candidates_per_segment(64),
            )
            .unwrap();
        assert_eq!(report.hits[0].id, RecordId::from("wal-tail"));
        assert!(
            report.global_scan_chunks_searched > 0,
            "the materialized delta must be merged without abandoning the stable base: {report:?}"
        );

        drop(reader);
        let second_tail_vector = vec![20.0; 8];
        writer
            .add(vec![VectorRecord::new(
                "wal-tail-2",
                second_tail_vector.clone(),
            )])
            .unwrap();
        writer.flush().unwrap();
        let compaction = writer.compact(CompactionOptions::default()).unwrap();
        assert!(compaction.compacted);
        assert_eq!(compaction.segments_read, 2);
        assert_eq!(
            writer
                .manifest
                .global_pq_ref
                .as_ref()
                .map(|reference| reference.checksum.as_str()),
            Some(stable_global_checksum.as_str()),
            "bounded online compaction must rewrite only delta cells"
        );

        let reader = BorsukIndex::open(&uri).unwrap();
        let report = reader
            .search_with_report(
                &second_tail_vector,
                SearchOptions::approx(5, LeafMode::SrhtPqScan)
                    .with_max_segments(8)
                    .with_max_candidates_per_segment(64),
            )
            .unwrap();
        assert_eq!(report.hits[0].id, RecordId::from("wal-tail-2"));
        assert!(report.global_scan_chunks_searched > 0);

        let mut pinned_reader = reader;
        let old_base_zero = (0..8)
            .map(|dimension| ((dimension * 11) % 101) as f32 / 101.0)
            .collect::<Vec<_>>();
        assert_eq!(
            pinned_reader
                .search_ids(&old_base_zero, SearchOptions::exact(1))
                .unwrap(),
            ["base-0"]
        );
        let updated_base_zero = vec![30.0; 8];
        writer
            .upsert(vec![VectorRecord::new("base-0", updated_base_zero.clone())])
            .unwrap();
        writer.delete(["base-1"]).unwrap();

        assert!(
            pinned_reader.get_vector("base-1").unwrap().is_some(),
            "an already-open reader must remain pinned until refresh"
        );
        assert!(pinned_reader.refresh().unwrap());
        assert!(pinned_reader.get_vector("base-1").unwrap().is_none());
        let report = pinned_reader
            .search_with_report(
                &updated_base_zero,
                SearchOptions::approx(3, LeafMode::SrhtPqScan)
                    .with_max_segments(8)
                    .with_max_candidates_per_segment(64),
            )
            .unwrap();
        assert_eq!(report.hits[0].id, RecordId::from("base-0"));
        assert!(
            report.global_scan_chunks_searched > 0,
            "upsert/delete overlays observed after refresh must retain the global base: {report:?}"
        );
    }

    #[test]
    fn direct_add_after_finalization_becomes_a_delta_without_discarding_the_base() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_string_lossy().into_owned();
        let mut index = BorsukIndex::create_with_wal(
            IndexConfig {
                uri: uri.clone(),
                metric: VectorMetric::Euclidean,
                dimensions: 8,
                segment_max_vectors: 256,
                ram_budget_bytes: None,
                text: false,
                named_vectors: Default::default(),
            },
            WalConfig::disabled(),
        )
        .unwrap();
        index
            .add(
                (0..128)
                    .map(|row| {
                        VectorRecord::new(
                            format!("base-{row}"),
                            (0..8)
                                .map(|dimension| ((row * 17 + dimension * 11) % 101) as f32 / 101.0)
                                .collect(),
                        )
                    })
                    .collect(),
            )
            .unwrap();
        index.finish_bulk_load().unwrap();
        let stable_checksum = index
            .manifest
            .global_pq_ref
            .as_ref()
            .unwrap()
            .checksum
            .clone();

        let delta_vector = vec![40.0; 8];
        index
            .add(vec![VectorRecord::new(
                "direct-delta",
                delta_vector.clone(),
            )])
            .unwrap();
        assert_eq!(
            index
                .manifest
                .global_pq_ref
                .as_ref()
                .map(|reference| reference.checksum.as_str()),
            Some(stable_checksum.as_str())
        );

        let report = BorsukIndex::open(&uri)
            .unwrap()
            .search_with_report(
                &delta_vector,
                SearchOptions::approx(3, LeafMode::SrhtPqScan)
                    .with_max_segments(8)
                    .with_max_candidates_per_segment(64),
            )
            .unwrap();
        assert_eq!(report.hits[0].id, RecordId::from("direct-delta"));
        assert!(report.global_scan_chunks_searched > 0);
    }

    #[test]
    fn every_named_global_scan_codec_builds_loads_and_searches_its_own_artifact() {
        let vectors = (0..128)
            .map(|row| {
                (0..16)
                    .map(|dimension| {
                        (((row + 3) * (dimension + 5) * 19) % 127) as f32 / 126.0 + 0.01
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        for codec in [
            GlobalScanCodec::Pq,
            GlobalScanCodec::SrhtPq,
            GlobalScanCodec::FastTurboQuantMse,
            GlobalScanCodec::FastTurboQuantProd,
        ] {
            let dir = tempfile::tempdir().unwrap();
            let uri = dir.path().to_string_lossy().into_owned();
            let mut index = BorsukIndex::create_with_build_config(
                IndexConfig {
                    uri: uri.clone(),
                    metric: VectorMetric::Angular,
                    dimensions: 16,
                    segment_max_vectors: 256,
                    ram_budget_bytes: None,
                    text: false,
                    named_vectors: Default::default(),
                },
                BuildConfig {
                    global_scan_codec: codec,
                    global_turboquant_bits: 4,
                    global_turboquant_qjl_bits: 0,
                    global_turboquant_shards: 1,
                    ..BuildConfig::default()
                },
            )
            .unwrap();
            index
                .add(
                    vectors
                        .iter()
                        .enumerate()
                        .map(|(row, vector)| {
                            VectorRecord::new(format!("{codec}-{row}"), vector.clone())
                        })
                        .collect(),
                )
                .unwrap();
            index.finish_bulk_load().unwrap();
            drop(index);

            let index = BorsukIndex::open(&uri).unwrap();
            let report = index
                .search_with_report(
                    &vectors[37],
                    SearchOptions::approx(5, codec.leaf_mode()).with_max_segments(8),
                )
                .unwrap();
            assert_eq!(report.leaf_mode, codec.to_string());
            assert_eq!(report.hits[0].id, RecordId::from(format!("{codec}-37")));
            assert_eq!(report.routing_pages_read, 0);
        }
    }

    #[test]
    fn full_paged_compaction_rebuilds_global_pq_for_the_new_segment_set() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_string_lossy().into_owned();
        let mut index = BorsukIndex::create_with_wal_routing_page_fanout_and_leaf_capability(
            IndexConfig {
                uri: uri.clone(),
                metric: VectorMetric::Euclidean,
                dimensions: 8,
                segment_max_vectors: 4,
                ram_budget_bytes: None,
                text: false,
                named_vectors: Default::default(),
            },
            WalConfig::disabled(),
            2,
            LeafCapability::PqScanOnly,
        )
        .unwrap();
        let records = (0..32)
            .map(|row| {
                VectorRecord::new(
                    format!("row-{row}"),
                    (0..8)
                        .map(|dimension| ((row * 13 + dimension * 7) % 97) as f32 / 97.0)
                        .collect(),
                )
            })
            .collect();
        index.add(records).unwrap();
        assert!(index.manifest.global_pq_ref.is_none());

        let report = index
            .compact(CompactionOptions {
                max_segments: None,
                ..CompactionOptions::default()
            })
            .unwrap();
        assert!(report.compacted);
        let active = index.active_segment_summaries().unwrap();
        let after = index.manifest.global_pq_ref.clone().unwrap();
        assert_eq!(
            after.segments,
            active
                .iter()
                .map(|summary| summary.checksum.clone())
                .collect::<Vec<_>>()
        );

        drop(index);
        let reopened = BorsukIndex::open(&uri).unwrap();
        assert!(reopened.load_resident_global_pq().unwrap().is_some());
    }

    #[test]
    fn finish_bulk_load_builds_global_pq_without_rewriting_ingest_segments() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_string_lossy().into_owned();
        let mut index = BorsukIndex::create_with_wal_and_leaf_capability(
            IndexConfig {
                uri: uri.clone(),
                metric: VectorMetric::Angular,
                dimensions: 8,
                segment_max_vectors: 8,
                ram_budget_bytes: None,
                text: false,
                named_vectors: Default::default(),
            },
            WalConfig::disabled(),
            LeafCapability::PqScanOnly,
        )
        .unwrap();
        let vectors = (0..32)
            .map(|row| {
                (0..8)
                    .map(|dimension| ((row * 17 + dimension * 11) % 101) as f32 / 101.0 + 0.01)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        index.add_vectors(vectors.clone()).unwrap();
        let before = index
            .active_segment_summaries()
            .unwrap()
            .into_iter()
            .map(|summary| summary.checksum)
            .collect::<Vec<_>>();
        assert!(index.manifest.global_pq_ref.is_none());

        index.finish_bulk_load().unwrap();

        let after = index
            .active_segment_summaries()
            .unwrap()
            .into_iter()
            .map(|summary| summary.checksum)
            .collect::<Vec<_>>();
        assert_eq!(after, before);
        assert_eq!(
            index.manifest.global_pq_ref.as_ref().unwrap().segments,
            before
        );
        let global_ref = index.manifest.global_pq_ref.as_ref().unwrap();
        let descriptor_bytes = index
            .storage
            .read_bytes_with_cache_status_and_checksum(&global_ref.path, &global_ref.checksum)
            .unwrap()
            .bytes;
        let descriptor = GlobalPqDescriptor::decode(&descriptor_bytes).unwrap();
        assert!(descriptor.chunks().len() > 1);
        assert!(
            descriptor
                .chunks()
                .iter()
                .map(|chunk| chunk.path.as_str())
                .collect::<HashSet<_>>()
                .len()
                < descriptor.chunks().len(),
            "small cell chunks should share immutable bundle objects"
        );
        drop(index);

        let reopened = BorsukIndex::open(&uri).unwrap();
        let report = reopened
            .search_with_report(&vectors[7], SearchOptions::approx(5, LeafMode::SrhtPqScan))
            .unwrap();
        assert_eq!(report.hits[0].id, RecordId::from("7"));
        assert_eq!(report.routing_pages_read, 0);
    }

    #[test]
    fn bulk_direct_add_locality_orders_records_before_segmenting() {
        let dir = tempfile::tempdir().unwrap();
        let mut index = BorsukIndex::create_with_wal_and_leaf_capability(
            IndexConfig {
                uri: dir.path().to_string_lossy().into_owned(),
                metric: VectorMetric::Euclidean,
                dimensions: 2,
                segment_max_vectors: 4,
                ram_budget_bytes: None,
                text: false,
                named_vectors: Default::default(),
            },
            WalConfig::disabled(),
            LeafCapability::PqScanOnly,
        )
        .unwrap();
        let records = (0..8)
            .map(|row| {
                let cluster = if row % 2 == 0 { -10.0 } else { 10.0 };
                VectorRecord::new(
                    format!("row-{row}"),
                    vec![cluster, cluster + row as f32 * 0.01],
                )
            })
            .collect();

        index.add(records).unwrap();

        let summaries = index.active_segment_summaries().unwrap();
        assert_eq!(summaries.len(), 2);
        for summary in summaries {
            let (segment, _, _, _) = index.read_segment(&summary).unwrap();
            let negative = segment.records[0].vector[0].is_sign_negative();
            assert!(
                segment
                    .records
                    .iter()
                    .all(|record| record.vector[0].is_sign_negative() == negative)
            );
        }
    }

    #[test]
    fn recommended_segment_size_targets_bounded_float32_working_sets() {
        assert_eq!(MIN_RECOMMENDED_SEGMENT_MAX_VECTORS, 64);
        assert_eq!(MAX_RECOMMENDED_SEGMENT_MAX_VECTORS, 131_072);
        assert_eq!(recommended_segment_max_vectors(1), 131_072);
        assert_eq!(recommended_segment_max_vectors(16), 131_072);
        assert_eq!(recommended_segment_max_vectors(96), 43_690);
        assert_eq!(recommended_segment_max_vectors(100), 41_943);
        assert_eq!(recommended_segment_max_vectors(128), 32_768);
        assert_eq!(recommended_segment_max_vectors(129), 32_513);
        assert_eq!(recommended_segment_max_vectors(256), 16_384);
        assert_eq!(recommended_segment_max_vectors(512), 8_192);
        assert_eq!(recommended_segment_max_vectors(784), 5_349);
        assert_eq!(recommended_segment_max_vectors(1_024), 4_096);
        assert_eq!(recommended_segment_max_vectors(2_048), 2_048);
        assert_eq!(recommended_segment_max_vectors(4_096), 1_024);
        assert_eq!(recommended_segment_max_vectors(8_192), 512);
        assert_eq!(recommended_segment_max_vectors(16_384), 256);
        assert_eq!(recommended_segment_max_vectors(65_536), 64);
    }

    #[test]
    fn global_pq_training_reservoir_is_dimension_byte_bounded() {
        assert_eq!(global_pq_training_sample_limit(96), 43_690);
        assert_eq!(global_pq_training_sample_limit(960), 4_369);
        assert_eq!(global_pq_training_sample_limit(1_024), 4_096);
        assert_eq!(global_pq_training_sample_limit(8_192), 512);
        assert_eq!(global_pq_training_sample_limit(65_536), 64);
        assert_eq!(global_pq_training_sample_limit(1_048_576), 4);
    }

    #[test]
    fn kd_locality_order_uses_linear_partition_work_per_level() {
        let mut state = 0xD1B5_4A32_D192_ED03_u64;
        let records = (0..8192)
            .map(|index| {
                let vector = (0..8)
                    .map(|_| (splitmix_next(&mut state) >> 32) as f32 / u32::MAX as f32)
                    .collect();
                VectorRecord::new(format!("row-{index:05}"), vector)
            })
            .collect::<Vec<_>>();
        let mut keyed = keyed_records(records);

        KD_ORDER_COMPARISONS.with(|count| count.set(0));
        kd_order_records(&mut keyed, 8, 32);
        let comparisons = KD_ORDER_COMPARISONS.with(Cell::get);

        assert!(
            comparisons < 200_000,
            "KD locality ordering repeated too much comparison work: {comparisons}"
        );
    }

    #[test]
    fn kd_median_partition_matches_full_sort_reference_order() {
        fn reference(records: &mut [KeyedRecord], dimensions: usize, leaf_size: usize) {
            if records.len() <= leaf_size {
                sort_leaf_records(records);
                return;
            }
            let split_dimension = widest_dimension(records, dimensions);
            records.sort_by(|left, right| compare_kd_entries(left, right, split_dimension));
            let split = aligned_split(records.len(), leaf_size);
            let (left, right) = records.split_at_mut(split);
            reference(left, dimensions, leaf_size);
            reference(right, dimensions, leaf_size);
        }

        let mut state = 0x94D0_49BB_1331_11EB_u64;
        let records = (0..2053)
            .map(|index| {
                let vector = (0..7)
                    .map(|_| (splitmix_next(&mut state) >> 32) as f32 / u32::MAX as f32)
                    .collect();
                VectorRecord::new(format!("row-{index:05}"), vector)
            })
            .collect::<Vec<_>>();
        let mut actual = keyed_records(records.clone());
        let mut expected = keyed_records(records);

        kd_order_records(&mut actual, 7, 32);
        reference(&mut expected, 7, 32);

        assert_eq!(
            actual
                .iter()
                .map(|entry| entry.record.id.as_str())
                .collect::<Vec<_>>(),
            expected
                .iter()
                .map(|entry| entry.record.id.as_str())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn bounded_candidate_ranking_scores_each_row_once() {
        let distances = [3.0_f32, 1.0, 1.0, 2.0];
        let calls = Cell::new(0_usize);

        let ranked = rank_candidate_indices(
            distances.len(),
            3,
            |index| {
                calls.set(calls.get() + 1);
                distances[index]
            },
            usize::cmp,
        );

        assert_eq!(ranked, vec![1, 2, 3]);
        assert_eq!(calls.get(), distances.len());
    }

    #[test]
    fn best_first_graph_frontier_scores_each_discovered_row_once() {
        let records = ["entry", "far", "near", "middle", "best", "next"]
            .into_iter()
            .map(|id| VectorRecord::new(id, vec![0.0]))
            .collect::<Vec<_>>();
        let adjacency = [
            vec![1, 2, 3],
            vec![],
            vec![1, 3, 4],
            vec![],
            vec![1, 5],
            vec![],
        ];
        let mut graph = SegmentGraph {
            segment_id: "test".to_string(),
            level: 0,
            edges: adjacency
                .iter()
                .enumerate()
                .flat_map(|(source_record_index, neighbors)| {
                    neighbors
                        .iter()
                        .map(move |&neighbor_record_index| crate::segment::GraphEdge {
                            source_record_index,
                            neighbor_record_index,
                            distance: 0.0,
                        })
                })
                .collect(),
            adjacency_offsets: Vec::new(),
            created_at: Utc::now(),
        };
        graph.prepare_adjacency(records.len());
        let distances = [0.0_f32, 3.0, 1.0, 2.0, 0.5, 1.5];
        let calls = (0..records.len())
            .map(|_| Cell::new(0_usize))
            .collect::<Vec<_>>();

        let selected = best_first_graph_candidates(&records, &graph, &[0], 5, |record_index| {
            calls[record_index].set(calls[record_index].get() + 1);
            Ok(distances[record_index])
        })
        .unwrap();

        assert_eq!(selected, vec![0, 2, 4, 5, 3]);
        assert_eq!(calls[0].get(), 0, "the selected entry is not rescored");
        for (record_index, call) in calls.iter().enumerate().skip(1) {
            assert_eq!(
                call.get(),
                1,
                "discovered row {record_index} was scored more than once"
            );
        }
    }

    #[test]
    fn bounded_parallel_map_preserves_order_and_uses_multiple_workers() {
        let active = std::sync::atomic::AtomicUsize::new(0);
        let peak = std::sync::atomic::AtomicUsize::new(0);
        let values = (0..8).collect::<Vec<_>>();

        let mapped = bounded_parallel_map(&values, 4, |value| {
            let now = active.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            peak.fetch_max(now, std::sync::atomic::Ordering::SeqCst);
            std::thread::sleep(std::time::Duration::from_millis(5));
            active.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            value * 2
        });

        assert_eq!(mapped, vec![0, 2, 4, 6, 8, 10, 12, 14]);
        assert!(peak.load(std::sync::atomic::Ordering::SeqCst) > 1);
        assert!(peak.load(std::sync::atomic::Ordering::SeqCst) <= 4);
    }

    #[test]
    fn bounded_io_map_is_not_limited_by_the_cpu_worker_count() {
        let active = std::sync::atomic::AtomicUsize::new(0);
        let peak = std::sync::atomic::AtomicUsize::new(0);
        let values = (0..12).collect::<Vec<_>>();
        let mapped = bounded_io_map_with_gate(&values, 12, None, |value| {
            let now = active.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            peak.fetch_max(now, std::sync::atomic::Ordering::SeqCst);
            std::thread::sleep(std::time::Duration::from_millis(20));
            active.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            value * 2
        });
        assert_eq!(mapped, vec![0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22]);
        let peak = peak.load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(peak, crate::configured_io_threads().min(12));
        if crate::configured_io_threads() > crate::configured_cpu_threads() {
            assert!(
                peak > crate::configured_cpu_threads().min(11),
                "blocking I/O must not be serialized by the CPU compute cap"
            );
        }
    }

    #[test]
    fn repeated_parallel_maps_reuse_a_bounded_worker_set() {
        let worker_ids = Arc::new(Mutex::new(HashSet::new()));
        let values = (0..16).collect::<Vec<_>>();
        for _ in 0..32 {
            let worker_ids = Arc::clone(&worker_ids);
            bounded_parallel_map(&values, 16, move |_| {
                worker_ids
                    .lock()
                    .unwrap()
                    .insert(std::thread::current().id());
            });
        }
        assert!(
            worker_ids.lock().unwrap().len() <= crate::configured_cpu_threads(),
            "query parallelism must reuse one fixed worker pool"
        );
    }

    #[test]
    fn global_decode_gate_bounds_parallel_maps_across_queries() {
        let gate = Arc::new(AdmissionGate::new(3));
        let active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let peak = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let start = Arc::new(std::sync::Barrier::new(3));

        let handles = (0..2)
            .map(|_| {
                let gate = Arc::clone(&gate);
                let active = Arc::clone(&active);
                let peak = Arc::clone(&peak);
                let start = Arc::clone(&start);
                std::thread::spawn(move || {
                    let values = (0..8).collect::<Vec<_>>();
                    start.wait();
                    bounded_parallel_map_with_gate(&values, 8, Some(&gate), |value| {
                        let now = active.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                        peak.fetch_max(now, std::sync::atomic::Ordering::SeqCst);
                        std::thread::sleep(std::time::Duration::from_millis(5));
                        active.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                        value * 2
                    })
                })
            })
            .collect::<Vec<_>>();
        start.wait();

        for handle in handles {
            assert_eq!(handle.join().unwrap(), vec![0, 2, 4, 6, 8, 10, 12, 14]);
        }
        assert_eq!(peak.load(std::sync::atomic::Ordering::SeqCst), 3);
    }

    #[test]
    fn projected_rerank_reads_only_the_bounded_sidecar_tail_for_its_index() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_string_lossy().into_owned();
        let index = BorsukIndex::create(IndexConfig {
            uri,
            metric: VectorMetric::Euclidean,
            dimensions: 128,
            segment_max_vectors: 512,
            ram_budget_bytes: None,
            text: false,
            named_vectors: Default::default(),
        })
        .unwrap();
        let mut state = 0xA076_1D64_78BD_642F_u64;
        let records = (0..512)
            .map(|row| {
                let vector = (0..128)
                    .map(|_| (splitmix_next(&mut state) >> 32) as f32 / u32::MAX as f32)
                    .collect();
                VectorRecord::new(format!("row-{row:04}"), vector)
            })
            .collect();
        let segment = Segment::from_records(
            "tail-read".to_string(),
            1,
            VectorMetric::Euclidean,
            128,
            records,
        )
        .unwrap();
        let summary = index.write_segment(segment).unwrap();
        let sidecar_path = vector_sidecar_relative_path(&summary.checksum);
        let full_bytes = std::fs::metadata(dir.path().join(sidecar_path))
            .unwrap()
            .len();

        let requests_before = index.storage.request_counts();
        let (_sidecar_index, bytes_fetched) = index
            .vector_sidecar_index(&summary.checksum, summary.object_count, summary.dimensions)
            .unwrap();
        let requests = index.storage.request_counts().delta(&requests_before);

        assert!(
            bytes_fetched * 4 < full_bytes,
            "sidecar index fetched {bytes_fetched} of {full_bytes} bytes"
        );
        assert_eq!(requests.gets, 1);
        assert_eq!(requests.heads, 0);
    }

    #[test]
    fn l0_page_routing_uses_leaf_segment_counts_for_sparse_pages() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_string_lossy().into_owned();
        let index = BorsukIndex::create_with_routing_page_fanout(
            IndexConfig {
                uri,
                metric: VectorMetric::Euclidean,
                dimensions: 2,
                segment_max_vectors: 1,
                ram_budget_bytes: None,
                text: false,
                named_vectors: Default::default(),
            },
            8,
        )
        .unwrap();
        let page_refs = (0..5)
            .map(|ordinal| {
                let centroid = if ordinal < 3 {
                    vec![0.0, 0.0]
                } else {
                    vec![100.0 + ordinal as f32, 0.0]
                };
                fake_l0_page_ref(ordinal, centroid, 1)
            })
            .collect::<Vec<_>>();

        let selected = index
            .routing_layer_page_refs_for_search(
                &[0.0, 0.0],
                &SearchOptions::approx(3, LeafMode::SrhtPqScan).with_max_segments(3),
                &page_refs,
            )
            .unwrap();

        assert_eq!(
            selected
                .iter()
                .map(|page_ref| page_ref.page_ordinal)
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert_eq!(
            selected
                .iter()
                .map(|page_ref| page_ref.leaf_segments)
                .sum::<usize>(),
            3
        );
    }

    #[test]
    fn l0_page_routing_overfetch_is_search_option() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_string_lossy().into_owned();
        let index = BorsukIndex::create_with_routing_page_fanout(
            IndexConfig {
                uri,
                metric: VectorMetric::Euclidean,
                dimensions: 2,
                segment_max_vectors: 1,
                ram_budget_bytes: None,
                text: false,
                named_vectors: Default::default(),
            },
            8,
        )
        .unwrap();
        let page_refs = (0..8)
            .map(|ordinal| fake_l0_page_ref(ordinal, vec![0.0, 0.0], 1))
            .collect::<Vec<_>>();

        let selected = index
            .routing_layer_page_refs_for_search(
                &[0.0, 0.0],
                &SearchOptions::approx(1, LeafMode::SrhtPqScan)
                    .with_max_segments(1)
                    .with_routing_page_overfetch(2),
                &page_refs,
            )
            .unwrap();

        assert_eq!(
            selected
                .iter()
                .map(|page_ref| page_ref.page_ordinal)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
    }

    #[test]
    fn l0_page_routing_overfetch_reads_sibling_pages_when_first_page_is_dense() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_string_lossy().into_owned();
        let index = BorsukIndex::create_with_routing_page_fanout(
            IndexConfig {
                uri,
                metric: VectorMetric::Euclidean,
                dimensions: 2,
                segment_max_vectors: 1,
                ram_budget_bytes: None,
                text: false,
                named_vectors: Default::default(),
            },
            8,
        )
        .unwrap();
        let page_refs = (0..4)
            .map(|ordinal| fake_l0_page_ref(ordinal, vec![0.0, 0.0], 4))
            .collect::<Vec<_>>();

        let selected = index
            .routing_layer_page_refs_for_search(
                &[0.0, 0.0],
                &SearchOptions::approx(2, LeafMode::SrhtPqScan)
                    .with_max_segments(2)
                    .with_routing_page_overfetch(2),
                &page_refs,
            )
            .unwrap();

        assert_eq!(
            selected
                .iter()
                .map(|page_ref| page_ref.page_ordinal)
                .collect::<Vec<_>>(),
            vec![0, 1],
            "routing overfetch should decode sibling L0 metadata pages even when one dense page already covers the segment-count target"
        );
    }

    #[test]
    fn parent_page_routing_overfetch_reads_sibling_branches_when_first_branch_is_dense() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_string_lossy().into_owned();
        let index = BorsukIndex::create_with_routing_page_fanout(
            IndexConfig {
                uri,
                metric: VectorMetric::Euclidean,
                dimensions: 2,
                segment_max_vectors: 1,
                ram_budget_bytes: None,
                text: false,
                named_vectors: Default::default(),
            },
            8,
        )
        .unwrap();
        let page_refs = (0..4)
            .map(|ordinal| {
                let mut page_ref = fake_l0_page_ref(ordinal, vec![0.0, 0.0], 4);
                page_ref.routing_level = 1;
                page_ref.path = format!("routing/pages/L1/fake-{ordinal}.parquet");
                page_ref
            })
            .collect::<Vec<_>>();

        let selected = index
            .routing_layer_page_refs_for_search(
                &[0.0, 0.0],
                &SearchOptions::approx(2, LeafMode::SrhtPqScan)
                    .with_max_segments(2)
                    .with_routing_page_overfetch(2),
                &page_refs,
            )
            .unwrap();

        assert_eq!(
            selected
                .iter()
                .map(|page_ref| page_ref.page_ordinal)
                .collect::<Vec<_>>(),
            vec![0, 1],
            "routing overfetch should keep sibling parent branches eligible even when one dense branch already covers the segment-count target"
        );
    }

    #[test]
    fn compact_overflow_does_not_read_unrelated_parent_routing_branches() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_string_lossy().into_owned();
        let mut index = BorsukIndex::create(IndexConfig {
            uri,
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 1,
            ram_budget_bytes: None,
            text: false,
            named_vectors: Default::default(),
        })
        .unwrap();

        let selected_segment = Segment::from_records(
            "selected".to_string(),
            1,
            VectorMetric::Euclidean,
            2,
            vec![
                VectorRecord::new("selected-a", vec![0.0, 0.0]),
                VectorRecord::new("selected-b", vec![1.0, 0.0]),
            ],
        )
        .unwrap();
        let selected_summary = index.write_segment(selected_segment).unwrap();

        let mut manifest = index.manifest.next_version();
        manifest.segments.clear();
        manifest.pivots.clear();
        manifest.routing_max_level = 2;

        let mut dirty_summaries = Vec::with_capacity(DEFAULT_ROUTING_PAGE_FANOUT);
        dirty_summaries.push(selected_summary);
        dirty_summaries.extend(
            (1..DEFAULT_ROUTING_PAGE_FANOUT)
                .map(|ordinal| fake_segment_summary(format!("dirty-{ordinal}"), 1, ordinal)),
        );

        let dirty_leaf = index
            .storage
            .write_routing_layer_page(&manifest, 0, 0, &dirty_summaries)
            .unwrap();
        let unrelated_middle_leaf = index
            .storage
            .write_routing_layer_page(
                &manifest,
                0,
                DEFAULT_ROUTING_PAGE_FANOUT,
                &[fake_segment_summary(
                    "middle",
                    0,
                    DEFAULT_ROUTING_PAGE_FANOUT,
                )],
            )
            .unwrap();
        let append_parent_leaf = index
            .storage
            .write_routing_layer_page(
                &manifest,
                0,
                DEFAULT_ROUTING_PAGE_FANOUT * 2,
                &[fake_segment_summary(
                    "append",
                    0,
                    DEFAULT_ROUTING_PAGE_FANOUT * 2,
                )],
            )
            .unwrap();

        let l1_dirty = index
            .storage
            .write_parent_routing_layer_page(&manifest, 1, 0, &[dirty_leaf])
            .unwrap();
        let l1_middle = index
            .storage
            .write_parent_routing_layer_page(&manifest, 1, 1, &[unrelated_middle_leaf])
            .unwrap();
        let l1_append = index
            .storage
            .write_parent_routing_layer_page(&manifest, 1, 2, &[append_parent_leaf])
            .unwrap();
        let l2_root = index
            .storage
            .write_parent_routing_layer_page(&manifest, 2, 0, &[l1_dirty, l1_middle, l1_append])
            .unwrap();

        index.manifest = index
            .publish_manifest_with_top_routing_page_refs_with_recovery(manifest, 2, &[l2_root])
            .unwrap();
        let top_page_paths = index
            .storage
            .read_routing_layer_page_index(index.manifest.version, 2)
            .unwrap();
        let root_children = index
            .routing_child_page_refs_read_from_parent_refs(&top_page_paths)
            .unwrap();
        let middle_path = root_children.page_refs[1].path.clone();
        let append_path = root_children.page_refs[2].path.clone();
        index
            .storage
            .write_bytes(&middle_path, b"corrupt unrelated parent routing page")
            .unwrap();
        index
            .storage
            .write_bytes(&append_path, b"corrupt append parent routing page")
            .unwrap();

        let compaction = index
            .compact(CompactionOptions {
                source_level: 1,
                target_level: 2,
                max_segments: Some(1),
                min_segments: 1,
                target_segment_max_vectors: Some(1),
                target_segment_max_radius: None,
            })
            .unwrap();

        assert!(compaction.compacted);
        assert_eq!(compaction.segments_read, 1);
        assert_eq!(compaction.segments_written, 2);
        assert_eq!(compaction.records_rewritten, 2);
        assert_eq!(compaction.routing_page_indexes_read, 1);
        assert_eq!(
            compaction.routing_pages_read, 3,
            "overflow compaction should read only the selected root, parent, and leaf pages"
        );
        assert_eq!(compaction.routing_page_indexes_written, 1);
        assert_eq!(
            compaction.routing_pages_written, 4,
            "overflow compaction should write two leaf pages and the two dirty parent pages"
        );
        assert_eq!(compaction.graph_payloads_read, 0);
        assert_eq!(compaction.graph_bytes_read, 0);
        assert!(index.manifest.segments.is_empty());
    }

    #[test]
    fn compact_max_segments_does_not_read_unneeded_source_parent_branches() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_string_lossy().into_owned();
        let mut index = BorsukIndex::create(IndexConfig {
            uri,
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 1,
            ram_budget_bytes: None,
            text: false,
            named_vectors: Default::default(),
        })
        .unwrap();

        let selected_segment = Segment::from_records(
            "selected".to_string(),
            1,
            VectorMetric::Euclidean,
            2,
            vec![VectorRecord::new("selected", vec![0.0, 0.0])],
        )
        .unwrap();
        let selected_summary = index.write_segment(selected_segment).unwrap();

        let unneeded_segment = Segment::from_records(
            "unneeded".to_string(),
            1,
            VectorMetric::Euclidean,
            2,
            vec![VectorRecord::new("unneeded", vec![1000.0, 0.0])],
        )
        .unwrap();
        let unneeded_summary = index.write_segment(unneeded_segment).unwrap();

        let mut manifest = index.manifest.next_version();
        manifest.segments.clear();
        manifest.pivots.clear();
        manifest.routing_max_level = 2;

        let dirty_leaf = index
            .storage
            .write_routing_layer_page(&manifest, 0, 0, &[selected_summary])
            .unwrap();
        let unneeded_leaf = index
            .storage
            .write_routing_layer_page(
                &manifest,
                0,
                DEFAULT_ROUTING_PAGE_FANOUT,
                &[unneeded_summary],
            )
            .unwrap();

        let l1_dirty = index
            .storage
            .write_parent_routing_layer_page(&manifest, 1, 0, &[dirty_leaf])
            .unwrap();
        let l1_unneeded = index
            .storage
            .write_parent_routing_layer_page(&manifest, 1, 1, &[unneeded_leaf])
            .unwrap();
        let l2_root = index
            .storage
            .write_parent_routing_layer_page(&manifest, 2, 0, &[l1_dirty, l1_unneeded])
            .unwrap();

        index.manifest = index
            .publish_manifest_with_top_routing_page_refs_with_recovery(manifest, 2, &[l2_root])
            .unwrap();
        let top_page_paths = index
            .storage
            .read_routing_layer_page_index(index.manifest.version, 2)
            .unwrap();
        let root_children = index
            .routing_child_page_refs_read_from_parent_refs(&top_page_paths)
            .unwrap();
        let unneeded_parent_path = root_children.page_refs[1].path.clone();
        index
            .storage
            .write_bytes(
                &unneeded_parent_path,
                b"corrupt unneeded source-level parent branch",
            )
            .unwrap();

        let compaction = index
            .compact(CompactionOptions {
                source_level: 1,
                target_level: 2,
                max_segments: Some(1),
                min_segments: 1,
                target_segment_max_vectors: Some(1),
                target_segment_max_radius: None,
            })
            .unwrap();

        assert!(compaction.compacted);
        assert_eq!(compaction.segments_read, 1);
        assert_eq!(compaction.records_rewritten, 1);
        assert_eq!(index.get_vector("selected").unwrap(), Some(vec![0.0, 0.0]));
    }

    #[test]
    fn compact_stops_parent_branch_reads_once_source_batch_is_covered() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_string_lossy().into_owned();
        let mut index = BorsukIndex::create(IndexConfig {
            uri,
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 1,
            ram_budget_bytes: None,
            text: false,
            named_vectors: Default::default(),
        })
        .unwrap();

        let selected_summaries = (0..32)
            .map(|ordinal| {
                let segment = Segment::from_records(
                    format!("selected-{ordinal}"),
                    1,
                    VectorMetric::Euclidean,
                    2,
                    vec![VectorRecord::new(
                        format!("selected-{ordinal}"),
                        vec![ordinal as f32, 0.0],
                    )],
                )
                .unwrap();
                index.write_segment(segment).unwrap()
            })
            .collect::<Vec<_>>();

        let mut manifest = index.manifest.next_version();
        manifest.segments.clear();
        manifest.pivots.clear();
        manifest.routing_max_level = 2;

        let selected_leaf = index
            .storage
            .write_routing_layer_page(&manifest, 0, 0, &selected_summaries)
            .unwrap();
        let unneeded_leaf = index
            .storage
            .write_routing_layer_page(
                &manifest,
                0,
                DEFAULT_ROUTING_PAGE_FANOUT,
                &[fake_segment_summary(
                    "unneeded",
                    1,
                    DEFAULT_ROUTING_PAGE_FANOUT,
                )],
            )
            .unwrap();
        let l1_selected = index
            .storage
            .write_parent_routing_layer_page(&manifest, 1, 0, &[selected_leaf])
            .unwrap();
        let l1_unneeded = index
            .storage
            .write_parent_routing_layer_page(&manifest, 1, 1, &[unneeded_leaf])
            .unwrap();
        let l2_root = index
            .storage
            .write_parent_routing_layer_page(&manifest, 2, 0, &[l1_selected, l1_unneeded])
            .unwrap();

        index.manifest = index
            .publish_manifest_with_top_routing_page_refs_with_recovery(manifest, 2, &[l2_root])
            .unwrap();
        let top_page_paths = index
            .storage
            .read_routing_layer_page_index(index.manifest.version, 2)
            .unwrap();
        let root_children = index
            .routing_child_page_refs_read_from_parent_refs(&top_page_paths)
            .unwrap();
        let unneeded_parent_path = root_children.page_refs[1].path.clone();
        index
            .storage
            .write_bytes(
                &unneeded_parent_path,
                b"corrupt sibling parent branch that compact must not read",
            )
            .unwrap();

        let compaction = index
            .compact(CompactionOptions {
                source_level: 1,
                target_level: 2,
                max_segments: Some(32),
                min_segments: 32,
                target_segment_max_vectors: Some(1),
                target_segment_max_radius: None,
            })
            .unwrap();

        assert!(compaction.compacted);
        assert_eq!(compaction.segments_read, 32);
        assert_eq!(compaction.records_rewritten, 32);
        assert_eq!(compaction.routing_page_indexes_read, 1);
        assert_eq!(compaction.routing_pages_read, 3);
        assert_eq!(compaction.routing_page_indexes_written, 1);
        assert_eq!(compaction.routing_pages_written, 3);
        assert_eq!(compaction.graph_payloads_read, 0);
        assert_eq!(compaction.graph_bytes_read, 0);
        assert_eq!(
            index.get_vector("selected-31").unwrap(),
            Some(vec![31.0, 0.0])
        );
    }

    #[test]
    fn compact_promotes_oversized_top_routing_index_without_reading_unrelated_parents() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_string_lossy().into_owned();
        let mut index = BorsukIndex::create(IndexConfig {
            uri,
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 1,
            ram_budget_bytes: None,
            text: false,
            named_vectors: Default::default(),
        })
        .unwrap();

        let selected_segment = Segment::from_records(
            "selected".to_string(),
            1,
            VectorMetric::Euclidean,
            2,
            vec![VectorRecord::new("selected", vec![0.0, 0.0])],
        )
        .unwrap();
        let selected_summary = index.write_segment(selected_segment).unwrap();

        let mut manifest = index.manifest.next_version();
        manifest.segments.clear();
        manifest.pivots.clear();
        manifest.routing_max_level = 1;

        let dirty_leaf = index
            .storage
            .write_routing_layer_page(&manifest, 0, 0, &[selected_summary])
            .unwrap();
        let mut top_refs = vec![
            index
                .storage
                .write_parent_routing_layer_page(&manifest, 1, 0, &[dirty_leaf])
                .unwrap(),
        ];

        for ordinal in 1..=DEFAULT_ROUTING_PAGE_FANOUT {
            let leaf_ordinal = ordinal * DEFAULT_ROUTING_PAGE_FANOUT;
            let cold_leaf = index
                .storage
                .write_routing_layer_page(
                    &manifest,
                    0,
                    leaf_ordinal,
                    &[fake_segment_summary(
                        format!("cold-{ordinal}"),
                        0,
                        leaf_ordinal,
                    )],
                )
                .unwrap();
            top_refs.push(
                index
                    .storage
                    .write_parent_routing_layer_page(&manifest, 1, ordinal, &[cold_leaf])
                    .unwrap(),
            );
        }

        let unrelated_parent_path = top_refs[1].path.clone();
        index.manifest = index
            .publish_manifest_with_top_routing_page_refs_with_recovery(manifest, 1, &top_refs)
            .unwrap();
        index
            .storage
            .write_bytes(
                &unrelated_parent_path,
                b"corrupt unrelated parent page that compaction must not read",
            )
            .unwrap();

        let compaction = index
            .compact(CompactionOptions {
                source_level: 1,
                target_level: 2,
                max_segments: Some(1),
                min_segments: 1,
                target_segment_max_vectors: Some(1),
                target_segment_max_radius: None,
            })
            .unwrap();

        assert!(compaction.compacted);
        assert_eq!(compaction.segments_read, 1);
        assert_eq!(compaction.records_rewritten, 1);
        assert_eq!(compaction.graph_payloads_read, 0);
        assert_eq!(compaction.graph_bytes_read, 0);
        assert_eq!(
            index.manifest.routing_max_level, 2,
            "scoped compaction should add a routing layer once the top page index exceeds fanout"
        );
        let promoted_top_refs = index
            .storage
            .read_routing_layer_page_index(index.manifest.version, 2)
            .unwrap();
        assert_eq!(promoted_top_refs.len(), 2);
        assert_eq!(index.get_vector("selected").unwrap(), Some(vec![0.0, 0.0]));
    }

    #[test]
    fn compact_updates_sparse_top_l0_page_refs_by_ordinal() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_string_lossy().into_owned();
        let mut index = BorsukIndex::create(IndexConfig {
            uri,
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 1,
            ram_budget_bytes: None,
            text: false,
            named_vectors: Default::default(),
        })
        .unwrap();

        let selected_segment = Segment::from_records(
            "selected".to_string(),
            1,
            VectorMetric::Euclidean,
            2,
            vec![VectorRecord::new("selected", vec![0.0, 0.0])],
        )
        .unwrap();
        let selected_summary = index.write_segment(selected_segment).unwrap();

        let mut manifest = index.manifest.next_version();
        manifest.segments.clear();
        manifest.pivots.clear();
        manifest.routing_max_level = 0;
        let sparse_leaf_ordinal = DEFAULT_ROUTING_PAGE_FANOUT;
        let sparse_leaf = index
            .storage
            .write_routing_layer_page(&manifest, 0, sparse_leaf_ordinal, &[selected_summary])
            .unwrap();

        index.manifest = index
            .publish_manifest_with_top_routing_page_refs_with_recovery(manifest, 0, &[sparse_leaf])
            .unwrap();

        let compaction = index
            .compact(CompactionOptions {
                source_level: 1,
                target_level: 2,
                max_segments: Some(1),
                min_segments: 1,
                target_segment_max_vectors: Some(1),
                target_segment_max_radius: None,
            })
            .unwrap();

        assert!(compaction.compacted);
        assert_eq!(compaction.segments_read, 1);
        assert_eq!(compaction.records_rewritten, 1);
        assert_eq!(compaction.routing_pages_read, 1);
        assert_eq!(compaction.routing_pages_written, 1);
        assert_eq!(compaction.graph_payloads_read, 0);
        assert_eq!(compaction.graph_bytes_read, 0);
        let page_refs = index
            .storage
            .read_routing_layer_page_index(index.manifest.version, 0)
            .unwrap();
        assert_eq!(page_refs.len(), 1);
        assert_eq!(page_refs[0].page_ordinal, sparse_leaf_ordinal);
        assert_eq!(index.get_vector("selected").unwrap(), Some(vec![0.0, 0.0]));
    }

    #[test]
    fn stats_reports_actual_sparse_page_backed_routing_topology() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_string_lossy().into_owned();
        let mut index = BorsukIndex::create_with_routing_page_fanout(
            IndexConfig {
                uri,
                metric: VectorMetric::Euclidean,
                dimensions: 2,
                segment_max_vectors: 1,
                ram_budget_bytes: None,
                text: false,
                named_vectors: Default::default(),
            },
            2,
        )
        .unwrap();

        let mut manifest = index.manifest.next_version();
        manifest.segments.clear();
        manifest.pivots.clear();
        manifest.routing_max_level = 1;

        let first_leaf = index
            .storage
            .write_routing_layer_page(&manifest, 0, 0, &[fake_segment_summary("first", 0, 0)])
            .unwrap();
        let sparse_leaf = index
            .storage
            .write_routing_layer_page(
                &manifest,
                0,
                DEFAULT_ROUTING_PAGE_FANOUT,
                &[fake_segment_summary(
                    "sparse",
                    0,
                    DEFAULT_ROUTING_PAGE_FANOUT,
                )],
            )
            .unwrap();
        let first_parent = index
            .storage
            .write_parent_routing_layer_page(&manifest, 1, 0, &[first_leaf])
            .unwrap();
        let sparse_parent = index
            .storage
            .write_parent_routing_layer_page(
                &manifest,
                1,
                DEFAULT_ROUTING_PAGE_FANOUT / 2,
                &[sparse_leaf],
            )
            .unwrap();

        index.manifest = index
            .publish_manifest_with_top_routing_page_refs_with_recovery(
                manifest,
                1,
                &[first_parent, sparse_parent],
            )
            .unwrap();

        let stats = index.try_stats().unwrap();

        assert_eq!(stats.segments, 2);
        assert_eq!(
            stats.routing_leaf_pages, 2,
            "stats should report actual L0 page refs for sparse page-backed routing"
        );
        assert_eq!(
            stats.routing_pages, 4,
            "stats should count the two L0 leaf pages plus the two L1 parent pages"
        );
    }

    #[test]
    fn stats_uses_top_index_page_count_aggregates_without_parent_reads() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_string_lossy().into_owned();
        let mut index = BorsukIndex::create_with_routing_page_fanout(
            IndexConfig {
                uri,
                metric: VectorMetric::Euclidean,
                dimensions: 2,
                segment_max_vectors: 1,
                ram_budget_bytes: None,
                text: false,
                named_vectors: Default::default(),
            },
            2,
        )
        .unwrap();

        let mut manifest = index.manifest.next_version();
        manifest.segments.clear();
        manifest.pivots.clear();
        manifest.routing_max_level = 1;

        let first_leaf = index
            .storage
            .write_routing_layer_page(&manifest, 0, 0, &[fake_segment_summary("first", 0, 0)])
            .unwrap();
        let second_leaf = index
            .storage
            .write_routing_layer_page(&manifest, 0, 1, &[fake_segment_summary("second", 0, 1)])
            .unwrap();
        let first_parent = index
            .storage
            .write_parent_routing_layer_page(&manifest, 1, 0, &[first_leaf])
            .unwrap();
        let second_parent = index
            .storage
            .write_parent_routing_layer_page(&manifest, 1, 1, &[second_leaf])
            .unwrap();
        let second_parent_path = second_parent.path.clone();

        index.manifest = index
            .publish_manifest_with_top_routing_page_refs_with_recovery(
                manifest,
                1,
                &[first_parent, second_parent],
            )
            .unwrap();
        index
            .storage
            .write_bytes(
                &second_parent_path,
                b"corrupt parent body stats must not read",
            )
            .unwrap();

        let stats = index.try_stats().unwrap();

        assert_eq!(stats.segments, 2);
        assert_eq!(stats.routing_leaf_pages, 2);
        assert_eq!(stats.routing_pages, 4);
    }

    fn fake_l0_page_ref(
        page_ordinal: usize,
        vector: Vec<f32>,
        leaf_segments: usize,
    ) -> RoutingLayerPageRef {
        RoutingLayerPageRef {
            routing_level: 0,
            page_ordinal,
            path: format!("routing/pages/L0/fake-{page_ordinal}.parquet"),
            checksum: format!("{page_ordinal:064x}"),
            page_segments: leaf_segments,
            leaf_segments,
            leaf_pages: 1,
            routing_pages: 1,
            dimensions: vector.len(),
            centroid: vector.clone(),
            radius: 0.0,
            bounds_min: vector.clone(),
            bounds_max: vector.clone(),
            id_bloom: Vec::new(),
            vector_signature_bloom: segment_vector_signature_bloom([vector.as_slice()]),
            level_mask: u64::MAX,
            page_records: leaf_segments,
            page_segment_bytes: leaf_segments as u64,
            page_vector_bytes: leaf_segments as u64,
            page_graph_bytes: 0,
            page_sparse_encoded_vectors: 0,
            page_dense_encoded_vectors: leaf_segments,
        }
    }

    fn fake_segment_summary(id: impl Into<String>, level: u8, ordinal: usize) -> SegmentSummary {
        let id = id.into();
        let vector = vec![ordinal as f32, 0.0];
        SegmentSummary {
            id: id.clone(),
            level,
            path: format!("segments/L{level}/fake-{ordinal}.parquet"),
            layout: crate::PhysicalLayoutRef {
                object_role: crate::PhysicalObjectRole::NormalSegment,
                physical_format: crate::PhysicalFormat::Parquet,
                layout_policy_version: crate::CURRENT_LAYOUT_POLICY_VERSION,
                integrity_chunk_bytes: 0,
                integrity_checksums: Vec::new(),
            }
            .with_integrity(b"fixture"),
            object_count: 1,
            dimensions: 2,
            centroid: vector.clone(),
            radius: 0.0,
            bounds_min: vector.clone(),
            bounds_max: vector.clone(),
            checksum: format!("{ordinal:064x}"),
            size_bytes: 1,
            vector_size_bytes: 1,
            graph_path: format!("graphs/L{level}/fake-{ordinal}.parquet"),
            graph_checksum: format!("{:064x}", ordinal + 1),
            graph_size_bytes: 1,
            leaf_mode: LeafMode::FlatScan,
            id_bloom: segment_id_bloom([id.as_str()]),
            vector_signature_bloom: segment_vector_signature_bloom([vector.as_slice()]),
            metadata_stats: crate::MetadataStats::default(),
            sparse_encoded: 0,
            dense_encoded: 1,
            text_doc_count: 0,
            text_total_doc_length: 0,
            text_lexical_decoded_bytes: 0,
            sparse_lexical_max_decoded_bytes: 0,
            lexical_shards: Vec::new(),
            created_at: Utc::now(),
        }
    }

    #[test]
    fn lexical_capacity_reserves_half_the_ram_envelope() {
        assert_eq!(
            automatic_lexical_capacity_bytes(Some(512 * 1024 * 1024)),
            Some(256 * 1024 * 1024)
        );
        assert_eq!(automatic_lexical_capacity_bytes(None), None);
    }

    #[test]
    fn bm25_delta_update_copy_on_writes_only_the_affected_page() {
        let dir = tempfile::tempdir().unwrap();
        let mut index = BorsukIndex::create(IndexConfig {
            uri: dir.path().to_string_lossy().into_owned(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 8,
            ram_budget_bytes: None,
            text: true,
            named_vectors: BTreeMap::new(),
        })
        .unwrap();
        let original = Bm25StatsDelta {
            document_count: -9_000,
            total_document_length: -9_000,
            document_frequencies: (0..9_000).map(|term| (term, -1)).collect(),
        };
        index.manifest.bm25_stats_delta = index.persist_bm25_stats_delta(&original).unwrap();
        assert_eq!(
            index
                .manifest
                .bm25_stats_delta
                .as_ref()
                .unwrap()
                .pages
                .len(),
            3
        );
        index.decoded_bm25_stats_pages =
            Arc::new(DecodedObjectCache::new(DEFAULT_BM25_STATS_PAGE_CACHE_BYTES));

        let before = index.storage.request_counts();
        let mut change = Bm25StatsDelta::default();
        change.suppress_document(&[(5_000, 1)]).unwrap();
        let updated = index
            .update_bm25_stats_delta_from(index.manifest.bm25_stats_delta.as_ref(), &change)
            .unwrap()
            .unwrap();
        let requests = index.storage.request_counts().delta(&before);

        assert_eq!(requests.gets, 1, "only the intersecting page may be read");
        assert_eq!(
            requests.puts, 1,
            "only the intersecting page may be rewritten"
        );
        assert_eq!(updated.pages[0], original_page(&index, 0));
        assert_eq!(updated.pages[2], original_page(&index, 2));

        let before_cached = index.storage.request_counts();
        let _ = index
            .update_bm25_stats_delta_from(index.manifest.bm25_stats_delta.as_ref(), &change)
            .unwrap();
        let cached_requests = index.storage.request_counts().delta(&before_cached);
        assert_eq!(
            cached_requests.gets, 0,
            "a decoded immutable statistics page must be reused in-process"
        );
    }

    #[test]
    fn tombstone_flush_copy_on_writes_only_the_affected_hash_bucket() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_string_lossy().into_owned();
        let index = BorsukIndex::create(IndexConfig {
            uri,
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 16,
            ram_budget_bytes: None,
            text: false,
            named_vectors: Default::default(),
        })
        .unwrap();
        let first_id = b"first".to_vec();
        let first_bucket = tombstone_bucket(&first_id);
        let second_id = (0_u64..)
            .map(|value| format!("second-{value}").into_bytes())
            .find(|id| tombstone_bucket(id) != first_bucket)
            .unwrap();
        let mut initial = BTreeMap::new();
        initial.insert(first_id.clone(), 1);
        initial.insert(second_id, 1);
        let mut manifest = index.manifest.next_version();
        manifest
            .tombstone_frontier
            .push(index.write_tombstone(initial).unwrap().unwrap());
        manifest.tombstone_id_count = 2;
        index
            .consolidate_mutation_frontiers(&mut manifest, false)
            .unwrap();
        assert_eq!(manifest.tombstone_pages.len(), 2);
        let before = manifest
            .tombstone_pages
            .iter()
            .map(|page| (page.bucket, page.checksum.clone()))
            .collect::<BTreeMap<_, _>>();

        let mut update = BTreeMap::new();
        update.insert(first_id, 2);
        manifest
            .tombstone_frontier
            .push(index.write_tombstone(update).unwrap().unwrap());
        index
            .consolidate_mutation_frontiers(&mut manifest, false)
            .unwrap();
        let after = manifest
            .tombstone_pages
            .iter()
            .map(|page| (page.bucket, page.checksum.clone()))
            .collect::<BTreeMap<_, _>>();

        assert_ne!(after[&first_bucket], before[&first_bucket]);
        for (bucket, checksum) in before {
            if bucket != first_bucket {
                assert_eq!(after[&bucket], checksum);
            }
        }

        let page = manifest
            .tombstone_pages
            .iter()
            .find(|page| page.bucket == first_bucket)
            .unwrap();
        index.load_tombstone_page(page).unwrap();
        let before_second_read = index.storage.request_counts();
        index.load_tombstone_page(page).unwrap();
        let second_read = index.storage.request_counts().delta(&before_second_read);
        assert_eq!(
            second_read.gets, 0,
            "a decoded immutable tombstone page must be reused in-process"
        );
    }

    #[test]
    fn lexical_insert_copy_on_writes_only_intersecting_global_term_pages() {
        let dir = tempfile::tempdir().unwrap();
        let mut index = BorsukIndex::create(IndexConfig {
            uri: dir.path().to_string_lossy().into_owned(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 1,
            ram_budget_bytes: None,
            text: true,
            named_vectors: BTreeMap::new(),
        })
        .unwrap();
        let vocabulary = (0..9_000)
            .map(|term| format!("token{term}"))
            .collect::<Vec<_>>()
            .join(" ");
        index
            .add(vec![
                VectorRecord::new("large-vocabulary", vec![0.0, 0.0]).with_text(vocabulary),
            ])
            .unwrap();
        index.flush().unwrap();
        let old_root = index
            .load_resident_lexical_roots()
            .unwrap()
            .remove(&("bm25".to_string(), "text".to_string()))
            .unwrap();
        assert!(old_root.pages.len() >= 2);
        let old_paths = old_root
            .pages
            .iter()
            .map(|page| page.path.clone())
            .collect::<HashSet<_>>();

        index
            .add(vec![
                VectorRecord::new("small-insert", vec![1.0, 0.0]).with_text("token5000"),
            ])
            .unwrap();
        index.flush().unwrap();
        let new_root = index
            .load_resident_lexical_roots()
            .unwrap()
            .remove(&("bm25".to_string(), "text".to_string()))
            .unwrap();
        let retained = new_root
            .pages
            .iter()
            .filter(|page| old_paths.contains(&page.path))
            .count();

        assert!(
            retained >= old_root.pages.len().saturating_sub(1),
            "one-term insert rewrote more than one global term page: old={}, retained={retained}",
            old_root.pages.len()
        );
    }

    fn original_page(index: &BorsukIndex, ordinal: usize) -> Bm25StatsDeltaPageRef {
        index.manifest.bm25_stats_delta.as_ref().unwrap().pages[ordinal].clone()
    }

    #[test]
    fn gc_path_filters_cover_both_segment_table_formats_and_all_global_pq_objects() {
        assert!(is_segment_table_path("segments/L0/aa/seg-1.parquet"));
        assert!(is_segment_table_path("segments/L0/aa/seg-1.vortex"));
        assert!(!is_segment_table_path("segments/L0/aa/seg-1.arrow"));

        assert!(is_global_pq_path("global-pq/cell-graphs/a.bin"));
        assert!(is_global_pq_path("global-pq/bundles/a.arrow"));
        assert!(is_global_pq_path("global-pq/descriptors/a.parquet"));
        assert!(!is_global_pq_path("vectors/a.arrow"));

        assert!(is_cell_wal_transaction_path("transactions/tx/STATE"));
        assert!(is_cell_wal_transaction_path("transactions/tx/COMMIT"));
        assert!(is_cell_wal_transaction_path(
            "transactions/tx/descriptors/abc.bin"
        ));
    }

    #[test]
    fn vortex_segment_format_writes_reads_and_reopens_through_required_summary_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_string_lossy().into_owned();
        let mut index = BorsukIndex::create_with_build_config(
            IndexConfig {
                uri: uri.clone(),
                metric: VectorMetric::Euclidean,
                dimensions: 2,
                segment_max_vectors: 2,
                ram_budget_bytes: None,
                text: false,
                named_vectors: BTreeMap::new(),
            },
            BuildConfig {
                physical_layout: crate::PhysicalLayoutPolicy::production_baseline()
                    .with_role_format(
                        crate::PhysicalObjectRole::NormalSegment,
                        crate::PhysicalFormat::Vortex,
                    ),
                ..BuildConfig::default()
            },
        )
        .unwrap();
        index
            .add(vec![
                VectorRecord::new("a", vec![0.0, 1.0]),
                VectorRecord::new("b", vec![2.0, 3.0]),
            ])
            .unwrap();
        index.flush().unwrap();

        let summary = index.manifest.segments.first().unwrap().clone();
        assert_eq!(
            summary.layout.physical_format,
            crate::PhysicalFormat::Vortex
        );
        assert!(summary.path.ends_with(".vortex"), "{}", summary.path);
        assert!(dir.path().join(&summary.path).is_file());
        let (decoded, _, _, _) = index.read_segment(&summary).unwrap();
        assert_eq!(decoded.records[0].vector, vec![0.0, 1.0]);
        assert_eq!(decoded.records[1].vector, vec![2.0, 3.0]);

        drop(index);
        let reopened = BorsukIndex::open(&uri).unwrap();
        let summaries = reopened.active_segment_summaries().unwrap();
        assert_eq!(
            summaries[0].layout.physical_format,
            crate::PhysicalFormat::Vortex
        );
        let (decoded, _, _, _) = reopened.read_segment(&summaries[0]).unwrap();
        assert_eq!(decoded.records.len(), 2);
    }
}
