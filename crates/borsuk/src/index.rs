use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    fmt,
    ops::Range,
    path::PathBuf,
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use object_store::ObjectStore;
use tokio::sync::Semaphore;
use url::Url;
use uuid::Uuid;

use crate::{
    centroid_hnsw::CentroidHnsw,
    error::{BorsukError, Result},
    format::{
        graph_from_parquet, graph_to_parquet, lean_segment_from_parquet,
        routing_layer_page_from_parquet,
        routing_layer_page_index_from_parquet_relaxed_manifest_version, segment_from_parquet,
        segment_has_persisted_pq_bounds, segment_to_parquet, segment_vectors_for_rows,
        tombstone_ids_from_parquet, tombstone_ids_to_parquet,
    },
    maintenance::{self, MaintenanceConfig, MaintenanceHandle, MaintenanceReport},
    manifest::{
        DEFAULT_GRAPH_NEIGHBORS, DEFAULT_ROUTING_PAGE_FANOUT, Manifest, RoutingLayerPageRef,
        SegmentSummary, TombstoneSummary, segment_id_bloom, segment_vector_signature_bloom,
    },
    metric::VectorMetric,
    observability,
    record::{
        AddReport, CompactionOptions, CompactionReport, DeleteReport, ExplainReport, Fusion,
        GarbageCollectionOptions, GarbageCollectionReport, HybridOptions, HybridQuery,
        IncrementalMaintenanceOptions, IncrementalReport, IndexStats, LeafMode, PurgeReport,
        QueryCostModel, RebuildOptions, RebuildReport, RecallGuarantee, RecordId, RequestCounts,
        SearchHit, SearchMode, SearchOptions, SearchReport, SearchTerminationReason,
        StorageEncoding, VectorKind, VectorRecord, VectorSpec,
    },
    segment::{
        Segment, SegmentGraph, VECTOR_LOCALITY_KEY_LEN, pq_code_for_query, routing_code,
        vector_bounds, vector_locality_key, vector_signature,
    },
    segment_cache::{AdmissionGate, DecodedSegmentCache, decoded_segment_bytes},
    sparse::SparseVector,
    sparse_named_sidecar::SparseNamedSidecar,
    storage::{
        PrefetchedRead, ReadBytes, RoutingLayerPageIndexRead, Storage, StorageWriteReport,
        StoredObject,
    },
    text::{Tokenizer, UnicodeWordLowercase, term_frequencies},
};

const LOCAL_GRAPH_NEIGHBORS: usize = DEFAULT_GRAPH_NEIGHBORS;
const ROUTING_SEARCH_PAGE_OVERFETCH: usize = 8;
/// Below this many cells a flat centroid scan is already cheap, so the HNSW
/// coarse quantizer stays off (building a graph would not pay for itself).
const COARSE_QUANTIZER_MIN_CELLS: usize = 64;
/// Cells the coarse quantizer returns per unit of the segment budget, so filter
/// pruning still leaves the full nprobe cells to read.
const COARSE_QUANTIZER_OVERFETCH: usize = 4;
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

/// Options used when opening an existing BORSUK index.
///
/// The derived defaults are the max-performance / minimal-RAM configuration:
/// paged routing (`resident_routing: false`), no local cache, no decoded-segment
/// cache, and unbounded search concurrency. Opt into resident routing or the
/// caches only for small, hot indexes that trade RAM for lower latency.
#[derive(Debug, Clone, Default)]
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
    /// Eagerly load every active decoded segment into RAM before open returns.
    ///
    /// Preload also makes routing summaries resident, overriding
    /// `resident_routing: false`. It requires the decoded-segment cache; when
    /// `segment_cache_max_bytes` is `None`, the cache is created with an
    /// effectively unbounded budget. Warmed entries are pinned so an explicit
    /// smaller cache budget cannot evict them and force later payload reads.
    pub preload: bool,
    /// Optional cap on how many searches run their decode/score phase at once.
    /// With `Some(n)`, additional concurrent searches wait for a permit, so
    /// peak working memory scales with `n` rather than the caller thread count.
    /// `None` leaves search concurrency unbounded.
    pub max_concurrent_searches: Option<usize>,
}

/// Result of eagerly loading active decoded segments into RAM.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WarmReport {
    /// Active segments newly decoded and inserted into the RAM cache.
    pub segments_loaded: usize,
    /// Estimated decoded bytes resident for all active segments.
    pub bytes_resident: u64,
}

/// A BORSUK index handle.
#[derive(Clone)]
pub struct BorsukIndex {
    storage: Storage,
    manifest: Manifest,
    named: BTreeMap<String, BorsukIndex>,
    tokenizer: Arc<dyn Tokenizer>,
    runtime_ram_budget_bytes: Option<u64>,
    segment_cache: Arc<OnceLock<Arc<DecodedSegmentCache>>>,
    resident_routing_summaries: ResidentRoutingSummaries,
    /// Lazily built HNSW coarse quantizer over cell centroids — the IVF probe
    /// list. Navigates to the nprobe nearest cells in ~O(log cells) instead of
    /// a flat centroid scan; rebuilt whenever the manifest version changes.
    coarse_quantizer: CoarseQuantizerCache,
    admission: Option<Arc<AdmissionGate>>,
    /// Lazily loaded deleted-id set, keyed by the active tombstone checksum so it
    /// reloads whenever deletions change. Loaded only when a bloom hit needs
    /// confirmation, so undeleted reads never pay for it.
    tombstone_cache: TombstoneCache,
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
    graph_bytes: u64,
    sparse_encoded_vectors: usize,
    dense_encoded_vectors: usize,
}

/// (lean segment, raw bytes when projected, bytes read, cache hit, cache repaired)
type LeanSegmentRead = (Segment, Option<Vec<u8>>, u64, bool, bool);

/// Lazily loaded deleted-id set keyed by the active tombstone checksum.
/// Tombstone overlay: `id -> minimum visible generation`. A stored record of
/// that id is suppressed when its generation is below the mapped value (a plain
/// delete maps it above every stored generation; an upsert maps it to the newest
/// generation, suppressing the older copies).
type TombstoneOverlay = HashMap<Vec<u8>, u64>;

/// Lazily loaded [`TombstoneOverlay`] keyed by the active tombstone checksum.
type TombstoneCache = Arc<Mutex<Option<(String, Arc<TombstoneOverlay>)>>>;

/// Resident active summaries keyed by the manifest version they describe.
type ResidentRoutingSummaries = Arc<Mutex<Option<(u64, Arc<Vec<SegmentSummary>>)>>>;

/// The coarse-quantizer HNSW over cell centroids plus the summaries it indexes
/// (node `i` is `summaries[i]`).
type ResolvedCoarseQuantizer = (Arc<CentroidHnsw>, Arc<Vec<SegmentSummary>>);

/// [`ResolvedCoarseQuantizer`] keyed by the manifest version it describes.
type CoarseQuantizerCache = Arc<Mutex<Option<(u64, Arc<CentroidHnsw>, Arc<Vec<SegmentSummary>>)>>>;

impl BorsukIndex {
    /// Create a new empty index and publish its first manifest.
    pub fn create(config: IndexConfig) -> Result<Self> {
        Self::create_with_cache(config, None)
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
        Self::create_with_cache_routing_page_fanout_and_graph_neighbors(
            config,
            None,
            DEFAULT_ROUTING_PAGE_FANOUT,
            graph_neighbors,
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

        storage.create_layout()?;

        let primary_uri = config.uri.clone();
        let named_specs = config.named_vectors.clone();
        let tokenizer = default_tokenizer();
        let mut manifest =
            Manifest::new_with_routing_page_fanout(config, routing_page_fanout, graph_neighbors);
        manifest.text_tokenizer = Some(tokenizer.fingerprint());
        enforce_ram_budget(&manifest, None)?;
        let manifest = storage.publish_manifest(&manifest)?;

        let mut index = Self {
            storage,
            manifest,
            named: BTreeMap::new(),
            tokenizer,
            runtime_ram_budget_bytes: None,
            segment_cache: Arc::new(OnceLock::new()),
            resident_routing_summaries: Arc::new(Mutex::new(None)),
            coarse_quantizer: Arc::new(Mutex::new(None)),
            admission: None,
            tombstone_cache: Arc::new(Mutex::new(None)),
        };
        index.named = index.create_named_indexes(&primary_uri, &named_specs)?;
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
                ram_budget_bytes: None,
                resident_routing: false,
                segment_cache_max_bytes: None,
                preload: false,
                max_concurrent_searches: None,
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

    fn open_with_storage(storage: Storage, mut options: OpenOptions) -> Result<Self> {
        if options.preload {
            options.resident_routing = true;
        }
        let span = observability::open_span(options.resident_routing);
        let _entered = span.enter();
        // Paged open reads only the manifest metadata table; it never fetches the
        // full routing/pivots tables or the routing page index. A corrupt or missing
        // page index surfaces lazily at search/stats time, keeping open O(1) in RAM.
        let manifest = if options.resident_routing {
            storage.load_current_manifest()?
        } else {
            storage.load_current_manifest_metadata()?
        };
        observability::record_open(&span, &manifest);
        enforce_ram_budget(&manifest, options.ram_budget_bytes)?;
        let segment_cache = options
            .segment_cache_max_bytes
            .or_else(|| options.preload.then_some(u64::MAX))
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
        let primary_uri = manifest.config.uri.clone();
        let named_specs = manifest.config.named_vectors.clone();
        let mut index = Self {
            storage,
            manifest,
            named: BTreeMap::new(),
            tokenizer: default_tokenizer(),
            runtime_ram_budget_bytes: options.ram_budget_bytes,
            segment_cache: segment_cache_cell,
            resident_routing_summaries: Arc::new(Mutex::new(None)),
            coarse_quantizer: Arc::new(Mutex::new(None)),
            admission,
            tombstone_cache: Arc::new(Mutex::new(None)),
        };
        index.named = index.open_named_indexes(&primary_uri, &named_specs, &options)?;
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
            let child = Self::create_with_storage(
                child_config,
                child_storage,
                self.manifest.routing_page_fanout,
                self.manifest.graph_neighbors,
            )?;
            named.insert(name.clone(), child);
        }
        Ok(named)
    }

    fn open_named_indexes(
        &self,
        primary_uri: &str,
        named_specs: &BTreeMap<String, VectorSpec>,
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
            let child = Self::open_with_storage(child_storage, options.clone())?;
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

    /// Eagerly decode every active segment into the shared in-memory cache.
    ///
    /// The active routing summaries are retained as a resident snapshot for
    /// this manifest version, so subsequent searches need neither routing-page
    /// reads nor segment-payload reads. Calling `warm` again is idempotent.
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
        self.segment_cache
            .get_or_init(|| Arc::new(DecodedSegmentCache::new(u64::MAX)));

        let mut report = WarmReport::default();
        for summary in summaries.iter() {
            let (segment, _, _, _, decoded_cache_hit) =
                self.read_segment_through_cache(summary, true)?;
            if !decoded_cache_hit {
                report.segments_loaded += 1;
            }
            report.bytes_resident = report
                .bytes_resident
                .saturating_add(decoded_segment_bytes(&segment));
        }
        Ok(report)
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
            records: totals.records,
            segment_bytes: totals.segment_bytes,
            graph_bytes: totals.graph_bytes,
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
    pub fn add(&mut self, records: Vec<VectorRecord>) -> Result<()> {
        let named_records = self.named_records_for_add(&records)?;
        self.validate_sparse_named_records(&records)?;
        let next_generated_id =
            next_generated_id_after_explicit_records(self.manifest.next_generated_id, &records)?;
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

        // Stamp a strictly higher generation per id and grow the tombstone
        // overlay so older generations of each upserted id become suppressed.
        let mut overlay: BTreeMap<Vec<u8>, u64> = match self.deleted_ids()? {
            Some(map) => map.as_ref().clone().into_iter().collect(),
            None => BTreeMap::new(),
        };
        for record in &mut records {
            let key = record.id.as_bytes().to_vec();
            let new_generation = overlay.get(&key).copied().unwrap_or(0) + 1;
            record.generation = new_generation;
            overlay.insert(key, new_generation);
        }

        let named_records = self.named_records_for_add(&records)?;
        self.validate_sparse_named_records(&records)?;
        let next_generated_id =
            next_generated_id_after_explicit_records(self.manifest.next_generated_id, &records)?;
        let tombstone = self.write_tombstone(overlay)?;
        self.add_records_with_report_and_tombstone(records, false, next_generated_id, tombstone)?;
        self.upsert_named_records(named_records)?;
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
            let child = self.named.get_mut(&name).ok_or_else(|| {
                BorsukError::InvalidRecordInput(format!(
                    "named vector `{name}` is declared but its sub-index is not open"
                ))
            })?;
            child.upsert(records)?;
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
        let next_generated_id =
            next_generated_id_after_explicit_records(self.manifest.next_generated_id, &records)?;
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
        let next_generated_id = advance_generated_id(self.manifest.next_generated_id, ids.len())?;
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
    /// Deletes are soft: the ids are recorded in a cumulative tombstone so search
    /// and `get_vector` skip them immediately, but the underlying rows stay in
    /// their immutable segments until a compaction or [`BorsukIndex::purge`]
    /// physically rewrites them. Re-adding a deleted id revives it.
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
        let report = self.delete_primary_with_report(ids.iter().cloned())?;
        for child in self.named.values_mut() {
            child.delete_with_report(ids.iter().cloned())?;
        }
        Ok(report)
    }

    fn delete_primary_with_report<I, R>(&mut self, ids: I) -> Result<DeleteReport>
    where
        I: IntoIterator<Item = R>,
        R: Into<RecordId>,
    {
        let requests_before = self.storage.request_counts();
        let mut deleted: BTreeMap<Vec<u8>, u64> = match self.deleted_ids()? {
            Some(map) => map.as_ref().clone().into_iter().collect(),
            None => BTreeMap::new(),
        };
        let before = deleted.len();
        let mut newly = 0usize;
        for id in ids {
            let key = id.into().as_bytes().to_vec();
            match deleted.get(&key).copied() {
                // First tombstone for this id: any stored copy has generation 0
                // (an upsert would already have left an entry), so a minimum
                // visible generation of 1 suppresses it.
                None => {
                    deleted.insert(key, 1);
                    newly += 1;
                }
                // Already tombstoned: only bump — and count — when a still-visible
                // copy exists (e.g. the id was re-upserted after a prior delete).
                // Re-deleting an already-deleted id is a no-op.
                Some(min_visible) => {
                    if self.has_live_record(&key, min_visible)? {
                        deleted.insert(key, min_visible + 1);
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
        self.publish_tombstone(deleted)?;
        Ok(DeleteReport {
            deleted: newly,
            total_tombstoned: self
                .manifest
                .tombstone
                .as_ref()
                .map_or(0, |tombstone| tombstone.count as usize),
            published: true,
            requests: self.storage.request_counts().delta(&requests_before),
        })
    }

    /// Physically remove every tombstoned row and clear the cumulative tombstone,
    /// reclaiming storage synchronously and re-enabling those ids for `add`.
    ///
    /// This is the heavy, on-demand counterpart to the lazy reclaim that ordinary
    /// compaction performs: it rewrites every active segment without the deleted
    /// rows. Prefer running it during maintenance windows on large indexes.
    pub fn purge(&mut self) -> Result<usize> {
        Ok(self.purge_with_report()?.records_purged)
    }

    /// Purge tombstoned rows and return a [`PurgeReport`].
    pub fn purge_with_report(&mut self) -> Result<PurgeReport> {
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
        let Some(tombstone) = self.manifest.tombstone.clone() else {
            return Ok(PurgeReport {
                requests: self.storage.request_counts().delta(&requests_before),
                ..PurgeReport::default()
            });
        };
        let tombstoned = tombstone.count as usize;

        // Read every active segment, dropping tombstoned rows, grouping survivors
        // by their original level so the rewrite preserves the level structure.
        let active = self.active_segment_summaries()?;
        let mut by_level: BTreeMap<u8, Vec<VectorRecord>> = BTreeMap::new();
        let mut segments_rewritten = 0_usize;
        let mut records_purged = 0_usize;
        for summary in &active {
            let (segment, _, _, _) = self.read_segment(summary)?;
            let before = segment.records.len();
            let mut kept = Vec::with_capacity(before);
            for record in segment.records {
                if self.is_suppressed(&record)? {
                    records_purged += 1;
                } else {
                    kept.push(record);
                }
            }
            if kept.len() != before {
                segments_rewritten += 1;
            }
            by_level.entry(summary.level).or_default().extend(kept);
        }

        for records in by_level.values_mut() {
            self.repopulate_sparse_named_records(records, &active)?;
        }

        // Rebuild the manifest with the surviving records and no tombstone. Even
        // when no row was physically present, publish a version that clears the
        // tombstone so the deleted ids become addable again.
        let previous = self.manifest.clone();
        let mut manifest = self.manifest.next_version();
        manifest.segments.clear();
        let dimensions = self.manifest.config.dimensions;
        let segment_max_vectors = self.manifest.config.segment_max_vectors;
        for (level, mut records) in by_level {
            sort_records_by_vector_locality(&mut records, dimensions, segment_max_vectors);
            for chunk in records.chunks(segment_max_vectors) {
                let segment = Segment::from_records(
                    Uuid::new_v4().to_string(),
                    level,
                    self.manifest.config.metric.clone(),
                    dimensions,
                    chunk.to_vec(),
                )?;
                manifest.segments.push(self.write_segment(segment)?);
            }
        }
        manifest.rebuild_pivots();
        manifest.tombstone = None;
        enforce_ram_budget(&manifest, self.runtime_ram_budget_bytes)?;
        self.manifest =
            self.publish_manifest_reusing_routing_pages_with_recovery(manifest, Some(&previous))?;

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
        self.manifest = self.storage.load_current_manifest()?;
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
            && self.manifest.tombstone.is_some()
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
        self.manifest = self.storage.load_current_manifest()?;
        let dimensions = self.manifest.config.dimensions;
        let metric = self.manifest.config.metric.clone();
        let in_shard =
            |id: &str| shard.is_none_or(|(rank, count)| maintenance::owns_shard(id, rank, count));

        let mut working = self.manifest.segments.clone();
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

            let (segment, _, _, _) = self.read_segment(&summary)?;
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
                let segment = Segment::from_records(
                    Uuid::new_v4().to_string(),
                    summary.level,
                    metric.clone(),
                    dimensions,
                    chunk,
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
                    let (segment, _, _, _) = self.read_segment(summary)?;
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
                        metric
                            .centroid_geometry_distance(&centroid, &summary.centroid)
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
                let (sparse_segment, _, _, _) = self.read_segment(&working[pos])?;
                let (neighbour_segment, _, _, _) = self.read_segment(&working[neighbour])?;
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
                    let segment = Segment::from_records(
                        Uuid::new_v4().to_string(),
                        level,
                        metric.clone(),
                        dimensions,
                        chunk,
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
            self.manifest = self.storage.load_current_manifest()?;
            let previous = self.manifest.clone();
            let mut manifest = self.manifest.next_version();
            manifest
                .segments
                .retain(|summary| !removed.contains(&summary.id));
            manifest.segments.extend(added.iter().cloned());
            manifest.rebuild_pivots();
            enforce_ram_budget(&manifest, self.runtime_ram_budget_bytes)?;
            match self
                .storage
                .publish_manifest_reusing_routing_pages_with_report(&manifest, Some(&previous))
            {
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

    /// Publish a new manifest version whose cumulative tombstone is `deleted`.
    /// Writes the content-addressed tombstone id-list object, then republishes
    /// reusing the unchanged routing pages.
    fn publish_tombstone(&mut self, deleted: BTreeMap<Vec<u8>, u64>) -> Result<()> {
        let previous = self.manifest.clone();
        let mut manifest = self.manifest.next_version();
        manifest.tombstone = self.write_tombstone(deleted)?;
        enforce_ram_budget(&manifest, self.runtime_ram_budget_bytes)?;

        // A tombstone-only publish changes no segments or routing pages. When the
        // index has paged (segments live in routing pages and `manifest.segments`
        // is empty), rebuilding routing pages from the empty segment list would
        // publish an empty index and lose every record. Re-publish referencing the
        // existing routing pages instead; only the manifest metadata (with the new
        // tombstone) is rewritten.
        if manifest.segments.is_empty() {
            let top_read = self.storage.read_routing_layer_page_index_with_status(
                previous.version,
                previous.routing_max_level,
            )?;
            if !top_read.page_refs.is_empty() {
                let published = self.publish_manifest_with_top_routing_page_refs_with_recovery(
                    manifest,
                    previous.routing_max_level,
                    &top_read.page_refs,
                )?;
                self.manifest = published;
                return Ok(());
            }
        }

        let published =
            self.publish_manifest_reusing_routing_pages_with_recovery(manifest, Some(&previous))?;
        self.manifest = published;
        Ok(())
    }

    /// Write the cumulative tombstone `(id, min_visible_generation)` object and
    /// return its summary, or `None` when the overlay is empty.
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
        Ok(Some(TombstoneSummary {
            id_bloom: segment_id_bloom(entries.iter().map(|(id, _)| id)),
            count: entries.len() as u64,
            path,
            checksum,
            created_at: Utc::now(),
        }))
    }

    /// Load and cache the tombstone overlay (`id -> min visible generation`),
    /// keyed by the active tombstone checksum. Returns `None` when nothing is
    /// tombstoned. Called only after a bloom hit.
    fn deleted_ids(&self) -> Result<Option<Arc<TombstoneOverlay>>> {
        let Some(tombstone) = self.manifest.tombstone.as_ref() else {
            return Ok(None);
        };
        let mut cache = self
            .tombstone_cache
            .lock()
            .expect("tombstone cache poisoned");
        if let Some((checksum, map)) = cache.as_ref()
            && checksum == &tombstone.checksum
        {
            return Ok(Some(Arc::clone(map)));
        }
        let read = self.storage.read_bytes_with_cache_status(&tombstone.path)?;
        let entries = tombstone_ids_from_parquet(&read.bytes)?;
        let map = Arc::new(entries.into_iter().collect::<HashMap<_, _>>());
        *cache = Some((tombstone.checksum.clone(), Arc::clone(&map)));
        Ok(Some(map))
    }

    /// The minimum visible generation for `id`, or `None` when the id carries no
    /// tombstone entry. Bloom fast-path: an id absent from the tombstone bloom
    /// pays zero I/O.
    fn min_visible_generation(&self, id: &[u8]) -> Result<Option<u64>> {
        let Some(tombstone) = self.manifest.tombstone.as_ref() else {
            return Ok(None);
        };
        if !tombstone.might_contain_record_id(id) {
            return Ok(None);
        }
        match self.deleted_ids()? {
            Some(map) => Ok(map.get(id).copied()),
            None => Ok(None),
        }
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

    /// Whether any stored record of `id` has a generation at or above
    /// `threshold` — i.e. a still-visible copy exists. Bloom-gated per segment.
    /// Uses the active segment set (which resolves segments from routing pages
    /// for paged indexes, where `manifest.segments` is empty), so a delete of an
    /// upserted id is correctly detected and suppressed at scale.
    fn has_live_record(&self, id: &[u8], threshold: u64) -> Result<bool> {
        for summary in self.active_segment_summaries()? {
            if !summary.might_contain_record_id(id) {
                continue;
            }
            let (segment, _, _, _) = self.read_segment(&summary)?;
            if segment
                .records
                .iter()
                .any(|record| record.id.as_bytes() == id && record.generation >= threshold)
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Remove logically deleted records from a compaction/purge input set,
    /// returning how many rows were dropped.
    fn drop_deleted_records(&self, records: &mut Vec<VectorRecord>) -> Result<usize> {
        if self.manifest.tombstone.is_none() {
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
                child.manifest.next_generated_id,
                &records,
            )?;
            child.add_records_with_report(records, true, next_generated_id)?;
        }
        Ok(())
    }

    fn validate_sparse_named_records(&self, records: &[VectorRecord]) -> Result<()> {
        for record in records {
            for (name, vector) in &record.extra_sparse {
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
            }
        }
        Ok(())
    }

    /// Search a sparse named vector for the top `k` records by inner-product
    /// similarity, scoring the query directly against the inverted index without
    /// densifying. Returns hits ordered by ascending inner-product distance
    /// (`-dot`); records sharing no term with the query are never scored.
    pub fn search_sparse_named(
        &self,
        name: &str,
        indices: Vec<u32>,
        values: Vec<f32>,
        k: usize,
    ) -> Result<Vec<SearchHit>> {
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
        if let Some(&max) = query.indices().iter().max()
            && (max as usize) >= spec.dimensions
        {
            return Err(BorsukError::InvalidMetricInput(format!(
                "sparse query index {max} exceeds dimensionality {}",
                spec.dimensions
            )));
        }

        let mut best_by_id = HashMap::<Vec<u8>, (u64, f32)>::new();
        for summary in self.active_segment_summaries()? {
            let Some(sidecar) = self.read_sparse_named_sidecar(name, &summary) else {
                continue;
            };
            for (row, score) in sidecar.score(&query, k) {
                let id = sidecar.row_id(row).ok_or_else(|| {
                    BorsukError::InvalidStorage(format!(
                        "sparse named sidecar row {row} has no record-id mapping"
                    ))
                })?;
                let generation = sidecar.row_generation(row).ok_or_else(|| {
                    BorsukError::InvalidStorage(format!(
                        "sparse named sidecar row {row} has no generation mapping"
                    ))
                })?;
                if self
                    .min_visible_generation(id)?
                    .is_some_and(|min_visible| generation < min_visible)
                {
                    continue;
                }
                match best_by_id.get_mut(id) {
                    Some(existing) if existing.0 >= generation => {}
                    Some(existing) => *existing = (generation, score),
                    None => {
                        best_by_id.insert(id.to_vec(), (generation, score));
                    }
                }
            }
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
        Ok(scored
            .into_iter()
            .map(|(id, score)| SearchHit {
                id,
                distance: -score,
                metadata: None,
            })
            .collect())
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
        tombstone_update: Option<TombstoneSummary>,
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

        for record in &records {
            self.validate_vector(&record.vector)?;
        }
        self.validate_text_records(&mut records)?;
        self.validate_record_ids_allowing_existing(
            &records,
            scan_existing_ids,
            tombstone_update.is_some(),
        )?;

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
                )?;
                report.requests = self.storage.request_counts().delta(&requests_before);
                observability::record_add_report(&span, &report, self.manifest.version);
                return Ok(report);
            }
        }

        let chunks = records.chunks(self.manifest.config.segment_max_vectors);
        let previous = self.manifest.clone();
        let mut manifest = self.manifest.next_version();
        manifest.next_generated_id = next_generated_id;
        if let Some(tombstone) = tombstone_update {
            manifest.tombstone = Some(tombstone);
        }
        let mut segments_written = 0_usize;
        let mut graph_payloads_written = 0_usize;
        let mut payload_bytes_written = 0_u64;

        for chunk in chunks {
            let segment_id = Uuid::new_v4().to_string();
            let segment = Segment::from_records(
                segment_id.clone(),
                0,
                self.manifest.config.metric.clone(),
                self.manifest.config.dimensions,
                chunk.to_vec(),
            )?;
            let summary = self.write_segment(segment)?;
            segments_written += 1;
            graph_payloads_written += 1;
            payload_bytes_written += summary.size_bytes + summary.graph_size_bytes;
            manifest.segments.push(summary);
        }

        manifest.rebuild_pivots();
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
        let base_version = self.manifest.version;
        loop {
            match self
                .storage
                .publish_manifest_reusing_routing_pages_with_report(&manifest, previous)
            {
                Ok(published) => return Ok(published),
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
        let base_version = self.manifest.version;
        loop {
            match self
                .storage
                .publish_manifest_with_routing_page_refs_with_report(&manifest, page_refs, report)
            {
                Ok(published) => return Ok(published),
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
        let base_version = self.manifest.version;
        loop {
            match self
                .storage
                .publish_manifest_with_top_routing_page_refs_with_report(
                    &manifest,
                    routing_level,
                    page_refs,
                    report,
                ) {
                Ok(published) => return Ok(published),
                Err(err) => {
                    self.advance_publish_version_after_conflict(base_version, &mut manifest, err)?
                }
            }
        }
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
        let refreshed = self.storage.load_current_manifest()?;
        if refreshed.version != base_version {
            self.manifest = refreshed;
            return Err(BorsukError::ConcurrentModification {
                path: conflict_path,
            });
        }
        // Local filesystem storage cannot CAS the final CURRENT write and falls
        // back to a plain put. Re-check before treating an occupied future
        // namespace as orphaned so a slower in-flight writer can advance CURRENT.
        std::thread::sleep(VERSION_SKIP_CURRENT_RECHECK_DELAY);
        let rechecked = self.storage.load_current_manifest()?;
        if rechecked.version != base_version {
            self.manifest = rechecked;
            return Err(BorsukError::ConcurrentModification {
                path: conflict_path,
            });
        }
        manifest.version = manifest.version.checked_add(1).ok_or_else(|| {
            BorsukError::InvalidStorage("manifest version exceeds u64".to_string())
        })?;
        Ok(())
    }

    fn add_records_to_top_routing_page_refs(
        &mut self,
        records: Vec<VectorRecord>,
        next_generated_id: u64,
        top_routing_level: u8,
        mut top_page_refs: Vec<RoutingLayerPageRef>,
        tombstone_update: Option<TombstoneSummary>,
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
        if let Some(tombstone) = tombstone_update {
            manifest.tombstone = Some(tombstone);
        }

        let mut new_summaries = Vec::<SegmentSummary>::new();
        let mut segments_written = 0_usize;
        let mut graph_payloads_written = 0_usize;
        let mut payload_bytes_written = 0_u64;
        for chunk in chunks {
            let segment_id = Uuid::new_v4().to_string();
            let segment = Segment::from_records(
                segment_id,
                0,
                self.manifest.config.metric.clone(),
                self.manifest.config.dimensions,
                chunk.to_vec(),
            )?;
            let summary = self.write_segment(segment)?;
            segments_written += 1;
            graph_payloads_written += 1;
            payload_bytes_written += summary.size_bytes + summary.graph_size_bytes;
            new_summaries.push(summary);
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
        let start = self.manifest.next_generated_id;
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
        let report = self.compact_primary(options.clone())?;
        for child in self.named.values_mut() {
            child.compact(options.clone())?;
        }
        Ok(report)
    }

    fn compact_primary(&mut self, options: CompactionOptions) -> Result<CompactionReport> {
        let span = observability::compact_span(&options, self.manifest.version);
        let _entered = span.enter();
        let report = self.compact_impl(options)?;
        observability::record_compaction_report(&span, &report);
        Ok(report)
    }

    fn compact_impl(&mut self, options: CompactionOptions) -> Result<CompactionReport> {
        validate_compaction_options(&options)?;

        let max_segments = options.max_segments.unwrap_or(usize::MAX);
        let page_index_read = self.routing_layer_page_index_read_for_compaction()?;
        if !page_index_read.page_refs.is_empty() {
            return self.compact_from_routing_tree(options, max_segments, page_index_read);
        }

        let active_summaries = self.active_segment_summaries()?;
        let selected = active_summaries
            .iter()
            .filter(|summary| summary.level == options.source_level)
            .take(max_segments)
            .cloned()
            .collect::<Vec<_>>();

        if selected.len() < options.min_segments {
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

        for summary in &selected {
            let (segment, segment_bytes_read, segment_cache_hit, _) = self.read_segment(summary)?;
            bytes_read += segment_bytes_read;
            count_cache_read(
                segment_cache_hit,
                &mut object_cache_hits,
                &mut object_cache_misses,
            );
            records.extend(segment.records);
        }
        self.repopulate_sparse_named_records(&mut records, &selected)?;
        // Physically drop logically deleted rows so compaction reclaims their
        // storage. Tombstone entries are cleared only by purge(), which rewrites
        // every remaining occurrence.
        self.drop_deleted_records(&mut records)?;
        sort_records_by_vector_locality(
            &mut records,
            self.manifest.config.dimensions,
            target_segment_max_vectors,
        );

        let selected_ids = selected
            .iter()
            .map(|summary| summary.id.as_str())
            .collect::<HashSet<_>>();
        let mut manifest = self.manifest.next_version();
        manifest.segments = active_summaries;
        manifest
            .segments
            .retain(|summary| !selected_ids.contains(summary.id.as_str()));

        let mut segments_written = 0_usize;
        let mut bytes_written = 0_u64;
        let records_rewritten = records.len();

        // Voronoi (k-means) cells, not axis-aligned locality slabs: tight
        // clusters whose centroids let approximate search probe only the few
        // nearest segments in high dimensions. Emitted in centroid-locality
        // order so the routing tree pages stay coherent.
        let chunks = voronoi_chunks(
            records,
            &self.manifest.config.metric,
            target_segment_max_vectors,
            options.target_segment_max_radius,
        )?;
        for chunk in chunks {
            let segment_id = Uuid::new_v4().to_string();
            let segment = Segment::from_records(
                segment_id,
                options.target_level,
                self.manifest.config.metric.clone(),
                self.manifest.config.dimensions,
                chunk,
            )?;
            let summary = self.write_segment(segment)?;
            bytes_written += summary.size_bytes + summary.graph_size_bytes;
            segments_written += 1;
            manifest.segments.push(summary);
        }

        manifest.rebuild_pivots();
        let routing_pages_written = routing_page_tree_content_page_count(
            manifest.segments.len(),
            manifest.routing_page_fanout,
        );
        enforce_ram_budget(&manifest, self.runtime_ram_budget_bytes)?;
        let previous = self.manifest.clone();
        self.manifest =
            self.publish_manifest_reusing_routing_pages_with_recovery(manifest, Some(&previous))?;
        let routing_page_indexes_written = usize::from(self.manifest.routing_max_level) + 1;

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

        let mut records = Vec::<VectorRecord>::new();
        let mut bytes_read = routing_bytes_read;
        let mut object_cache_hits = routing_object_cache_hits;
        let mut object_cache_misses = routing_object_cache_misses;

        for summary in &selected {
            let (segment, segment_bytes_read, segment_cache_hit, _) = self.read_segment(summary)?;
            bytes_read += segment_bytes_read;
            count_cache_read(
                segment_cache_hit,
                &mut object_cache_hits,
                &mut object_cache_misses,
            );
            records.extend(segment.records);
        }
        self.repopulate_sparse_named_records(&mut records, &selected)?;
        // Physically drop logically deleted rows so compaction reclaims their
        // storage. Tombstone entries are cleared only by purge(), which rewrites
        // every remaining occurrence.
        self.drop_deleted_records(&mut records)?;
        sort_records_by_vector_locality(
            &mut records,
            self.manifest.config.dimensions,
            target_segment_max_vectors,
        );

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

        let mut segments_written = 0_usize;
        let mut bytes_written = 0_u64;
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
        let chunks = voronoi_chunks(
            records,
            &self.manifest.config.metric,
            output_chunk_size,
            options.target_segment_max_radius,
        )?;
        for chunk in chunks {
            let segment_id = Uuid::new_v4().to_string();
            let segment = Segment::from_records(
                segment_id,
                options.target_level,
                self.manifest.config.metric.clone(),
                self.manifest.config.dimensions,
                chunk,
            )?;
            let summary = self.write_segment(segment)?;
            bytes_written += summary.size_bytes + summary.graph_size_bytes;
            segments_written += 1;
            replacement_summaries.push(summary);
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
        self.manifest = self.storage.load_current_manifest()?;
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
                is_parquet_path,
                GarbageCollectionObjectKind::SegmentOrGraph,
                &mut scan,
            )?;
            self.collect_gc_candidates(
                "graphs",
                is_parquet_path,
                GarbageCollectionObjectKind::SegmentOrGraph,
                &mut scan,
            )?;
            self.collect_gc_candidates(
                "fidx",
                is_filter_index_path,
                GarbageCollectionObjectKind::SegmentOrGraph,
                &mut scan,
            )?;
            self.collect_gc_candidates(
                "bidx",
                is_bm25_index_path,
                GarbageCollectionObjectKind::SegmentOrGraph,
                &mut scan,
            )?;
            for (name, spec) in &self.manifest.config.named_vectors {
                if spec.kind == VectorKind::Sparse {
                    self.collect_gc_candidates(
                        &format!("svidx/{name}"),
                        is_sparse_named_sidecar_path,
                        GarbageCollectionObjectKind::SegmentOrGraph,
                        &mut scan,
                    )?;
                }
            }
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

    fn active_segment_object_paths(&self) -> Result<ActiveGcObjectPathsRead> {
        let mut paths = HashSet::new();
        paths.insert(self.manifest.file_name());
        paths.insert(self.manifest.routing_file_name());
        paths.insert(self.manifest.pivots_file_name());
        if let Some(tombstone) = &self.manifest.tombstone {
            paths.insert(tombstone.path.clone());
        }

        let mut read = ActiveGcObjectPathsRead::default();
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
            if self.manifest.config.text {
                // The BM25 sidecar is also content-addressed by the segment
                // checksum and is present only for text-bearing segments.
                paths.insert(bm25_index_relative_path(&summary.checksum));
            }
            for (name, spec) in &self.manifest.config.named_vectors {
                if spec.kind == VectorKind::Sparse {
                    paths.insert(sparse_named_sidecar_relative_path(name, &summary.checksum));
                }
            }
        }
        read.paths = paths;
        Ok(read)
    }

    fn collect_gc_candidates(
        &self,
        relative_prefix: &str,
        path_filter: impl Fn(&str) -> bool,
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

    /// The HNSW coarse quantizer over cell centroids for the active manifest,
    /// built lazily and cached until the version changes. Returns `None` when
    /// there are too few cells to bother, or when the routing summaries are not
    /// already resident: the quantizer indexes every cell centroid, so building
    /// it from a paged index would pull all summaries into RAM and defeat the
    /// near-zero-resident-memory design. It therefore rides on the resident
    /// snapshot that `warm()` / `resident_routing` already keep in memory.
    /// (Cold/paged activation via a persisted quantizer object is future work.)
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

        let Some(summaries) = self.resident_routing_summaries() else {
            return Ok(None);
        };
        if summaries.len() < COARSE_QUANTIZER_MIN_CELLS {
            return Ok(None);
        }
        // Cosine/angular cells store the mean of unit-normalized vectors; unit
        // normalizing the centroid makes squared-Euclidean rank identically to
        // cosine distance, matching `segment_routing_rank_distance`.
        let normalize = self
            .manifest
            .config
            .metric
            .uses_normalized_euclidean_geometry();
        let centroids: Vec<Vec<f32>> = summaries
            .iter()
            .map(|summary| {
                if normalize {
                    crate::metric::unit_l2_normalized(&summary.centroid)
                } else {
                    summary.centroid.clone()
                }
            })
            .collect();
        let Some(hnsw) = CentroidHnsw::build(&centroids) else {
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
        Ok(Some((hnsw, summaries)))
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
        if options.guaranteed_recall {
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
        let Some((hnsw, summaries)) = self.coarse_quantizer()? else {
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
        let mut reports = Vec::<(String, SearchReport)>::new();

        for (name, vector) in &query.vectors {
            reports.push((
                name.clone(),
                self.search_with_report(
                    vector,
                    options
                        .dense_options
                        .clone()
                        .with_k(candidate_depth)
                        .with_vector_name(name.clone()),
                )?,
            ));
        }
        for (name, (indices, values)) in &query.sparse_vectors {
            let hits =
                self.search_sparse_named(name, indices.clone(), values.clone(), candidate_depth)?;
            reports.push((name.clone(), sparse_leg_report(hits)));
        }
        if let Some(text) = &query.text {
            reports.push((
                HYBRID_TEXT_MODALITY.to_string(),
                self.search_text(text, candidate_depth)?,
            ));
        }

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
            object_cache_hits: reports
                .iter()
                .map(|(_, report)| report.object_cache_hits)
                .sum(),
            object_cache_misses: reports
                .iter()
                .map(|(_, report)| report.object_cache_misses)
                .sum(),
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
        })
    }

    /// Search text by BM25 over the per-segment text sidecars.
    pub fn search_text(&self, text: &str, k: usize) -> Result<SearchReport> {
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
        let requests_before = self.storage.request_counts();
        let started = Instant::now();
        let query_terms = term_frequencies(self.tokenizer.as_ref(), text)
            .keys()
            .copied()
            .collect::<BTreeSet<_>>();
        let summaries = self.active_segment_summaries()?;
        let segments_total = summaries.len();
        let resident_bytes_estimate = self.manifest.resident_bytes_estimate();
        let total_docs = summaries
            .iter()
            .map(|summary| u64::from(summary.text_doc_count))
            .sum::<u64>();
        let total_doc_length = summaries
            .iter()
            .map(|summary| summary.text_total_doc_length)
            .sum::<u64>();

        if query_terms.is_empty() || total_docs == 0 {
            return Ok(SearchReport {
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
                object_cache_hits: 0,
                object_cache_misses: 0,
                cache_repairs: 0,
                records_considered: 0,
                records_scored: 0,
                graph_candidates_added: 0,
                resident_bytes_estimate,
                elapsed_ms: started.elapsed().as_millis() as u64,
                requests: self.storage.request_counts().delta(&requests_before),
                rows_evaluated: 0,
                rows_passed_filter: 0,
                segments_pruned_by_filter: 0,
            });
        }

        let avgdl = total_doc_length as f64 / total_docs as f64;
        let mut dfs = query_terms
            .iter()
            .map(|term| (*term, 0_u64))
            .collect::<BTreeMap<_, _>>();
        let mut reads = Vec::<Bm25IndexRead>::new();
        let mut bytes_read = 0_u64;
        for summary in &summaries {
            let Some(read) = self.read_bm25_index(summary) else {
                continue;
            };
            bytes_read += read.bytes_read;
            for term in &query_terms {
                if let Some(df) = dfs.get_mut(term) {
                    *df += u64::from(read.sidecar.df(*term));
                }
            }
            reads.push(read);
        }

        let segments_searched = reads.len();
        let mut scores = HashMap::<(usize, u32), f64>::new();
        for (segment_index, read) in reads.iter().enumerate() {
            for term in &query_terms {
                let df = dfs[term];
                if df == 0 {
                    continue;
                }
                let idf = (1.0 + (total_docs as f64 - df as f64 + 0.5) / (df as f64 + 0.5)).ln();
                for &(row, tf) in read.sidecar.postings(*term) {
                    let doc_length = read.sidecar.doc_length(row).ok_or_else(|| {
                        BorsukError::InvalidStorage(format!(
                            "bm25 index row {row} has no document length"
                        ))
                    })?;
                    let tf = f64::from(tf);
                    let dl = f64::from(doc_length);
                    let denominator = tf + BM25_K1 * (1.0 - BM25_B + BM25_B * dl / avgdl);
                    *scores.entry((segment_index, row)).or_default() +=
                        idf * (tf * (BM25_K1 + 1.0)) / denominator;
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
        for ((segment_index, row), score) in scores {
            if score <= 0.0 {
                continue;
            }
            let read = &reads[segment_index];
            let id_bytes = read.sidecar.row_id(row).ok_or_else(|| {
                BorsukError::InvalidStorage(format!(
                    "bm25 index row {row} has no record-id mapping"
                ))
            })?;
            let generation = read.sidecar.row_generation(row).ok_or_else(|| {
                BorsukError::InvalidStorage(format!(
                    "bm25 index row {row} has no generation mapping"
                ))
            })?;
            let suppressed = self
                .min_visible_generation(id_bytes)?
                .is_some_and(|min_visible| generation < min_visible);
            if suppressed {
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

        Ok(SearchReport {
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
            object_cache_hits: 0,
            object_cache_misses: 0,
            cache_repairs: 0,
            records_considered: 0,
            records_scored: 0,
            graph_candidates_added: 0,
            resident_bytes_estimate,
            elapsed_ms: started.elapsed().as_millis() as u64,
            requests: self.storage.request_counts().delta(&requests_before),
            rows_evaluated: 0,
            rows_passed_filter: 0,
            segments_pruned_by_filter: 0,
        })
    }

    fn search_execution(
        &self,
        query: &[f32],
        options: SearchOptions,
        include_vectors: bool,
    ) -> Result<SearchExecution> {
        self.search_execution_with_routing_cache(query, options, include_vectors, None)
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
        validate_search_options(&options)?;
        let _admission = self.admission.as_ref().map(|gate| gate.acquire());

        let requests_before = self.storage.request_counts();
        let started = Instant::now();
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
                    object_cache_hits: 0,
                    object_cache_misses: 0,
                    cache_repairs: 0,
                    records_considered: 0,
                    records_scored: 0,
                    graph_candidates_added: 0,
                    resident_bytes_estimate,
                    elapsed_ms: started.elapsed().as_millis() as u64,
                    requests: self.storage.request_counts().delta(&requests_before),
                    rows_evaluated: 0,
                    rows_passed_filter: 0,
                    segments_pruned_by_filter: 0,
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
        let mut graph_bytes_read = 0_u64;
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
        let query_projectable = self.segment_cache.get().is_none()
            && std::env::var("BORSUK_DISABLE_PROJECTED_SCORING").is_err()
            && matches!(
                candidate_mode,
                SearchMode::Approx {
                    leaf_mode: LeafMode::PqScan | LeafMode::SqScan,
                    max_candidates_per_segment: Some(_),
                    ..
                }
            );
        let prefetch_depth = if self.segment_cache.get().is_some() || query_projectable {
            1
        } else {
            options.prefetch_depth
        };
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
            let mut projected_bytes: Option<Vec<u8>> = None;
            let (segment, segment_bytes_read, segment_cache_hit, segment_cache_repaired): (
                Arc<Segment>,
                u64,
                bool,
                bool,
            ) = if self.segment_cache.get().is_some() {
                let (segment, bytes, cache_hit, repaired, _) =
                    self.read_segment_through_cache(summary, false)?;
                (segment, bytes, cache_hit, repaired)
            } else if use_projection {
                let (segment, bytes, bytes_read, byte_hit, repaired) =
                    self.read_segment_lean(summary)?;
                projected_bytes = bytes;
                (Arc::new(segment), bytes_read, byte_hit, repaired)
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
                (Arc::new(decoded), bytes, byte_hit, repaired)
            } else {
                let (decoded, bytes, byte_hit, repaired) = self.read_segment(summary)?;
                (Arc::new(decoded), bytes, byte_hit, repaired)
            };
            segments_searched += 1;
            bytes_read += segment_bytes_read;
            count_cache_read(
                segment_cache_hit,
                &mut object_cache_hits,
                &mut object_cache_misses,
            );
            count_cache_repair(segment_cache_repaired, &mut cache_repairs);
            records_considered += segment.records.len();

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
            let candidates = if let Some(rows) = prefilter_rows {
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
                    let (graph, graph_bytes, graph_cache_hit, graph_cache_repaired) =
                        self.read_graph(summary, &segment)?;
                    graph_bytes_read += graph_bytes;
                    count_cache_read(
                        graph_cache_hit,
                        &mut object_cache_hits,
                        &mut object_cache_misses,
                    );
                    count_cache_repair(graph_cache_repaired, &mut cache_repairs);
                    Some(graph)
                } else {
                    None
                };
                candidate_record_indices(
                    &segment,
                    graph.as_ref(),
                    query,
                    &candidate_mode,
                    effective_leaf_mode(&candidate_mode, summary.leaf_mode),
                    options.k,
                )?
            };
            candidate_truncated |= candidates.truncated;
            graph_candidates_added += candidates.graph_candidates_added;

            // In the projected path the lean segment has no vectors; fetch only
            // the chosen candidates' vectors from the raw bytes for re-ranking.
            let candidate_vectors = match &projected_bytes {
                Some(bytes) => Some(segment_vectors_for_rows(
                    bytes,
                    &candidates.indices,
                    self.manifest.config.dimensions,
                )?),
                None => None,
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
                let distance = metric.distance(query, vector)?;
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

        let vectors = hits
            .iter()
            .filter_map(|hit| hit.vector.clone())
            .collect::<Vec<_>>();
        let hits = hits.into_iter().map(|hit| hit.hit).collect::<Vec<_>>();

        let execution = SearchExecution {
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
                object_cache_hits,
                object_cache_misses,
                cache_repairs,
                records_considered,
                records_scored,
                graph_candidates_added,
                resident_bytes_estimate,
                elapsed_ms: started.elapsed().as_millis() as u64,
                requests: self.storage.request_counts().delta(&requests_before),
                rows_evaluated,
                rows_passed_filter,
                segments_pruned_by_filter,
            },
            vectors,
        };
        observability::record_search_report(&span, &execution.report);
        Ok(execution)
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
        let bytes = segment_to_parquet(&segment)?;
        let checksum = blake3::hash(&bytes).to_hex().to_string();
        let prefix = &checksum[..2];
        let path = format!(
            "segments/L{}/{prefix}/seg-{}.parquet",
            segment.level, segment.id
        );

        let graph = SegmentGraph::from_segment(&segment, self.manifest.graph_neighbors)?;
        let graph_bytes = graph_to_parquet(&graph)?;
        let graph_checksum = blake3::hash(&graph_bytes).to_hex().to_string();
        let graph_prefix = &graph_checksum[..2];
        let graph_path = format!(
            "graphs/L{}/{graph_prefix}/graph-{}.parquet",
            segment.level, segment.id
        );

        self.storage.write_bytes(&path, &bytes)?;
        self.storage.write_bytes(&graph_path, &graph_bytes)?;
        // Persist the on-demand filter-index sidecar (always, so filtered reads
        // never miss it). It rides object storage, not RAM.
        let filter_index =
            crate::MetadataIndex::from_rows(segment.records.iter().map(|record| &record.metadata));
        self.storage.write_bytes(
            &filter_index_relative_path(&checksum),
            &encode_filter_index(&checksum, &filter_index),
        )?;
        let sparse_encoded = segment
            .records
            .iter()
            .filter(|record| {
                record.storage.resolve_for_vector(&record.vector) == StorageEncoding::Sparse
            })
            .count();
        let dense_encoded = segment.records.len().saturating_sub(sparse_encoded);
        let (text_doc_count, text_total_doc_length) = if self.manifest.config.text {
            let text_rows = segment
                .records
                .iter()
                .filter_map(|record| {
                    record_text_terms(record)
                        .map(|terms| (record.id.as_bytes().to_vec(), record.generation, terms))
                })
                .collect::<Vec<_>>();
            let bm25_index = crate::bm25::Bm25IndexSidecar::from_text_rows(&text_rows);
            if !bm25_index.is_empty() {
                self.storage.write_bytes(
                    &bm25_index_relative_path(&checksum),
                    &encode_bm25_index(&checksum, &bm25_index),
                )?;
            }
            (bm25_index.doc_count(), bm25_index.total_doc_length())
        } else {
            (0, 0)
        };
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
            let sidecar = SparseNamedSidecar::from_rows(spec.dimensions, &rows);
            if !sidecar.is_empty() {
                self.storage.write_bytes(
                    &sparse_named_sidecar_relative_path(name, &checksum),
                    &encode_sparse_named_sidecar(&checksum, &sidecar),
                )?;
            }
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
            object_count: segment.records.len(),
            dimensions: segment.dimensions,
            centroid: segment.centroid,
            radius: segment.radius,
            bounds_min,
            bounds_max,
            checksum,
            size_bytes: bytes.len() as u64,
            graph_path,
            graph_checksum,
            graph_size_bytes: graph_bytes.len() as u64,
            leaf_mode: leaf_mode_for_segment_level(segment.level),
            id_bloom,
            vector_signature_bloom,
            metadata_stats,
            sparse_encoded,
            dense_encoded,
            text_doc_count,
            text_total_doc_length,
            created_at: segment.created_at,
        })
    }

    fn read_segment(&self, summary: &SegmentSummary) -> Result<(Segment, u64, bool, bool)> {
        let read = self
            .storage
            .read_bytes_with_cache_status_and_checksum(&summary.path, &summary.checksum)?;
        self.segment_from_read(summary, read)
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

    /// Read a segment for pq/sq candidate selection. When the segment carries
    /// persisted PQ bounds it is decoded lean (no vector column) and the raw
    /// bytes are returned so only chosen candidates' vectors are decoded later.
    /// Segments without persisted bounds fall back to a full decode.
    fn read_segment_lean(&self, summary: &SegmentSummary) -> Result<LeanSegmentRead> {
        let read = self
            .storage
            .read_bytes_with_cache_status_and_checksum(&summary.path, &summary.checksum)?;
        let bytes_read = read.bytes.len() as u64;
        let cache_hit = read.cache_hit;
        let cache_repaired = read.cache_repaired;
        validate_object_size("segment", &summary.path, summary.size_bytes, bytes_read)?;
        if segment_has_persisted_pq_bounds(&read.bytes)? {
            let segment = lean_segment_from_parquet(&read.bytes)?;
            validate_segment_metadata(summary, &segment, &self.manifest.config.metric)?;
            Ok((
                segment,
                Some(read.bytes),
                bytes_read,
                cache_hit,
                cache_repaired,
            ))
        } else {
            let segment = segment_from_parquet(&read.bytes)?;
            validate_segment_metadata(summary, &segment, &self.manifest.config.metric)?;
            Ok((segment, None, bytes_read, cache_hit, cache_repaired))
        }
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

        let segment = segment_from_parquet(&read.bytes)?;
        validate_segment_metadata(summary, &segment, &self.manifest.config.metric)?;

        Ok((segment, bytes_read, cache_hit, cache_repaired))
    }

    /// Read the per-segment filter-index sidecar on demand. Returns `None` when
    /// the sidecar is absent, unreadable, or fails self-validation -- in every
    /// such case the caller falls back to reading the segment payload, so a bad
    /// sidecar only forgoes an I/O saving, never changes results.
    fn read_filter_index(&self, summary: &SegmentSummary) -> Result<Option<FilterIndexRead>> {
        let path = filter_index_relative_path(&summary.checksum);
        match self.storage.read_bytes_with_cache_status(&path) {
            Ok(read) => Ok(
                decode_filter_index(&read.bytes, &summary.checksum).map(|index| FilterIndexRead {
                    index,
                    bytes_read: read.bytes.len() as u64,
                    cache_hit: read.cache_hit,
                    cache_repaired: read.cache_repaired,
                }),
            ),
            // Best-effort accelerator: any read failure just means "fall back".
            Err(_) => Ok(None),
        }
    }

    /// Read the per-segment BM25 sidecar on demand. Missing, unreadable,
    /// corrupt, or stale sidecars are skipped; text search simply ignores
    /// segments that have no valid BM25 sidecar.
    fn read_bm25_index(&self, summary: &SegmentSummary) -> Option<Bm25IndexRead> {
        let path = bm25_index_relative_path(&summary.checksum);
        match self.storage.read_bytes_with_cache_status(&path) {
            Ok(read) => {
                decode_bm25_index(&read.bytes, &summary.checksum).map(|sidecar| Bm25IndexRead {
                    sidecar,
                    bytes_read: read.bytes.len() as u64,
                })
            }
            Err(_) => None,
        }
    }

    /// Read a per-segment sparse named-vector sidecar on demand. Missing,
    /// unreadable, corrupt, or stale sidecars are skipped.
    fn read_sparse_named_sidecar(
        &self,
        name: &str,
        summary: &SegmentSummary,
    ) -> Option<SparseNamedSidecar> {
        let path = sparse_named_sidecar_relative_path(name, &summary.checksum);
        match self.storage.read_bytes_with_cache_status(&path) {
            Ok(read) => decode_sparse_named_sidecar(&read.bytes, &summary.checksum).ok(),
            Err(_) => None,
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
                let Some(sidecar) = self.read_sparse_named_sidecar(name, summary) else {
                    continue;
                };
                for row in 0..sidecar.row_count() {
                    let id = sidecar.row_id(row).ok_or_else(|| {
                        BorsukError::InvalidStorage(format!(
                            "sparse named sidecar row {row} has no record-id mapping"
                        ))
                    })?;
                    let generation = sidecar.row_generation(row).ok_or_else(|| {
                        BorsukError::InvalidStorage(format!(
                            "sparse named sidecar row {row} has no generation mapping"
                        ))
                    })?;
                    let vector = sidecar.row_vector(row).ok_or_else(|| {
                        BorsukError::InvalidStorage(format!(
                            "sparse named sidecar row {row} has no vector mapping"
                        ))
                    })?;
                    vectors.insert((id.to_vec(), generation), vector.clone());
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
    ) -> Result<(SegmentGraph, u64, bool, bool)> {
        let read = self.storage.read_bytes_with_cache_status_and_checksum(
            &summary.graph_path,
            &summary.graph_checksum,
        )?;
        let bytes_read = read.bytes.len() as u64;
        let cache_hit = read.cache_hit;
        let cache_repaired = read.cache_repaired;
        validate_object_size(
            "graph",
            &summary.graph_path,
            summary.graph_size_bytes,
            bytes_read,
        )?;

        let graph = graph_from_parquet(&read.bytes, &summary.id, summary.level, &segment.records)?;
        validate_graph_record_references(
            &summary.graph_path,
            segment,
            &graph,
            self.manifest.graph_neighbors,
        )?;

        Ok((graph, bytes_read, cache_hit, cache_repaired))
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

    fn effective_ram_budget_bytes(&self) -> Option<u64> {
        [
            self.manifest.config.ram_budget_bytes,
            self.runtime_ram_budget_bytes,
        ]
        .into_iter()
        .flatten()
        .min()
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
        let exceeds_radius = !current.is_empty()
            && metric.centroid_geometry_distance(&centroid, geometry_vector)? > max_radius;
        if !current.is_empty() && (exceeds_count || exceeds_radius) {
            chunks.push(std::mem::take(&mut current));
            centroid.clear();
        }
        if centroid.is_empty() {
            centroid = geometry_vector.to_vec();
        } else {
            let count = current.len() as f32;
            for (mean, value) in centroid.iter_mut().zip(geometry_vector) {
                *mean = (*mean * count + value) / (count + 1.0);
            }
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
    let mut centroids = kmeans_plus_plus_init(&geometry, k);
    let mut assignment = vec![0_usize; input_len];
    let mut nearest_distance = vec![0.0_f32; input_len];
    for _ in 0..VORONOI_KMEANS_ITERS {
        for (index, vector) in geometry.iter().enumerate() {
            let (nearest, distance) = nearest_centroid(vector, &centroids);
            assignment[index] = nearest;
            nearest_distance[index] = distance;
        }
        let mut sums = vec![vec![0.0_f32; dimensions]; k];
        let mut counts = vec![0_usize; k];
        for (index, vector) in geometry.iter().enumerate() {
            let cluster = assignment[index];
            counts[cluster] += 1;
            for (sum, value) in sums[cluster].iter_mut().zip(vector) {
                *sum += value;
            }
        }
        let mut movement = 0.0_f32;
        for cluster in 0..k {
            if counts[cluster] == 0 {
                // Reseed an empty cluster on the worst-served point so k-means
                // does not collapse to fewer cells than requested.
                if let Some(farthest) = (0..input_len)
                    .max_by(|&a, &b| nearest_distance[a].total_cmp(&nearest_distance[b]))
                {
                    centroids[cluster] = geometry[farthest].clone();
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
                output.extend(voronoi_chunks(group, metric, max_vectors, max_radius)?);
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
            for (mean, value) in centroid
                .iter_mut()
                .zip(crate::metric::unit_l2_normalized(&record.vector))
            {
                *mean += value;
            }
        } else {
            for (mean, value) in centroid.iter_mut().zip(&record.vector) {
                *mean += value;
            }
        }
    }
    let count = cell.len().max(1) as f32;
    for mean in centroid.iter_mut() {
        *mean /= count;
    }
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
        for (mean, value) in centroid.iter_mut().zip(vector) {
            *mean += value;
        }
    }
    let count = geometry.len() as f32;
    for mean in centroid.iter_mut() {
        *mean /= count;
    }
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
fn squared_distance(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum()
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

fn splitmix_unit(state: &mut u64) -> f64 {
    (splitmix_next(state) >> 11) as f64 / (1_u64 << 53) as f64
}

fn sort_records_by_vector_locality(
    records: &mut Vec<VectorRecord>,
    dimensions: usize,
    target_segment_max_vectors: usize,
) {
    let mut reordered = std::mem::take(records);
    kd_order_records(
        &mut reordered,
        dimensions,
        target_segment_max_vectors.max(1),
    );
    records.extend(reordered);
}

fn kd_order_records(records: &mut [VectorRecord], dimensions: usize, leaf_size: usize) {
    if records.len() <= leaf_size {
        sort_leaf_records(records);
        return;
    }

    let split_dimension = widest_dimension(records, dimensions);
    records.sort_by(|left, right| {
        left.vector[split_dimension]
            .partial_cmp(&right.vector[split_dimension])
            .unwrap_or(Ordering::Equal)
            .then_with(|| {
                vector_locality_key(&left.vector)
                    .cmp(&vector_locality_key(&right.vector))
                    .then_with(|| left.id.cmp(&right.id))
            })
    });

    let split = aligned_split(records.len(), leaf_size);
    let (left, right) = records.split_at_mut(split);
    kd_order_records(left, dimensions, leaf_size);
    kd_order_records(right, dimensions, leaf_size);
}

fn sort_leaf_records(records: &mut [VectorRecord]) {
    records.sort_by(|left, right| {
        vector_locality_key(&left.vector)
            .cmp(&vector_locality_key(&right.vector))
            .then_with(|| left.id.cmp(&right.id))
    });
}

fn widest_dimension(records: &[VectorRecord], dimensions: usize) -> usize {
    let mut best_dimension = 0_usize;
    let mut best_width = f32::NEG_INFINITY;
    for dimension in 0..dimensions {
        let mut min = f32::INFINITY;
        let mut max = f32::NEG_INFINITY;
        for record in records {
            let value = record.vector[dimension];
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
        let expected_distance = segment.metric.distance(&source.vector, &neighbor.vector)?;
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

fn validate_graph_neighbors(graph_neighbors: usize) -> Result<()> {
    if graph_neighbors == 0 {
        return Err(BorsukError::InvalidMetricInput(
            "graph_neighbors must be greater than zero".to_string(),
        ));
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

fn is_filter_index_path(path: &str) -> bool {
    path.ends_with(".fidx")
}

fn is_bm25_index_path(path: &str) -> bool {
    path.ends_with(".bidx")
}

fn is_sparse_named_sidecar_path(path: &str) -> bool {
    path.ends_with(".svidx")
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

fn is_routing_metadata_table_path(path: &str) -> bool {
    (path.starts_with("routing/segments-") || path.starts_with("routing/pivots-"))
        && is_parquet_path(path)
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
    let Some(budget_bytes) = [manifest.config.ram_budget_bytes, runtime_budget_bytes]
        .into_iter()
        .flatten()
        .min()
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

struct CandidateRecordSelection {
    indices: Vec<usize>,
    graph_candidates_added: usize,
    truncated: bool,
}

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

struct Bm25IndexRead {
    sidecar: crate::bm25::Bm25IndexSidecar,
    bytes_read: u64,
}

fn filter_index_relative_path(segment_checksum: &str) -> String {
    format!("fidx/{}/{}.fidx", &segment_checksum[..2], segment_checksum)
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

const BM25_INDEX_CHECKSUM_LEN: usize = 64;
const BM25_INDEX_CONTENT_HASH_LEN: usize = 32;

fn bm25_index_relative_path(segment_checksum: &str) -> String {
    format!("bidx/{}/{}.bidx", &segment_checksum[..2], segment_checksum)
}

fn encode_bm25_index(segment_checksum: &str, sidecar: &crate::bm25::Bm25IndexSidecar) -> Vec<u8> {
    let sidecar_bytes = sidecar.to_bytes();
    let content_hash = blake3::hash(&sidecar_bytes);
    let mut out = Vec::with_capacity(
        BM25_INDEX_CHECKSUM_LEN + BM25_INDEX_CONTENT_HASH_LEN + sidecar_bytes.len(),
    );
    out.extend_from_slice(segment_checksum.as_bytes());
    out.extend_from_slice(content_hash.as_bytes());
    out.extend_from_slice(&sidecar_bytes);
    out
}

fn decode_bm25_index(
    bytes: &[u8],
    expected_checksum: &str,
) -> Option<crate::bm25::Bm25IndexSidecar> {
    let header = BM25_INDEX_CHECKSUM_LEN + BM25_INDEX_CONTENT_HASH_LEN;
    if bytes.len() < header || expected_checksum.len() != BM25_INDEX_CHECKSUM_LEN {
        return None;
    }
    if &bytes[..BM25_INDEX_CHECKSUM_LEN] != expected_checksum.as_bytes() {
        return None;
    }
    let content_hash = &bytes[BM25_INDEX_CHECKSUM_LEN..header];
    let sidecar_bytes = &bytes[header..];
    if blake3::hash(sidecar_bytes).as_bytes() != content_hash {
        return None;
    }
    crate::bm25::Bm25IndexSidecar::from_bytes(sidecar_bytes).ok()
}

const SPARSE_NAMED_SIDECAR_CHECKSUM_LEN: usize = 64;
const SPARSE_NAMED_SIDECAR_CONTENT_HASH_LEN: usize = 32;

fn sparse_named_sidecar_relative_path(name: &str, segment_checksum: &str) -> String {
    format!(
        "svidx/{name}/{}/{}.svidx",
        &segment_checksum[..2],
        segment_checksum
    )
}

fn encode_sparse_named_sidecar(segment_checksum: &str, sidecar: &SparseNamedSidecar) -> Vec<u8> {
    let sidecar_bytes = sidecar.to_bytes();
    let content_hash = blake3::hash(&sidecar_bytes);
    let mut out = Vec::with_capacity(
        SPARSE_NAMED_SIDECAR_CHECKSUM_LEN
            + SPARSE_NAMED_SIDECAR_CONTENT_HASH_LEN
            + sidecar_bytes.len(),
    );
    out.extend_from_slice(segment_checksum.as_bytes());
    out.extend_from_slice(content_hash.as_bytes());
    out.extend_from_slice(&sidecar_bytes);
    out
}

fn decode_sparse_named_sidecar(
    bytes: &[u8],
    expected_checksum: &str,
) -> Result<SparseNamedSidecar> {
    let header = SPARSE_NAMED_SIDECAR_CHECKSUM_LEN + SPARSE_NAMED_SIDECAR_CONTENT_HASH_LEN;
    if bytes.len() < header || expected_checksum.len() != SPARSE_NAMED_SIDECAR_CHECKSUM_LEN {
        return Err(BorsukError::InvalidStorage(
            "sparse named sidecar header is truncated or has an invalid checksum length"
                .to_string(),
        ));
    }
    if &bytes[..SPARSE_NAMED_SIDECAR_CHECKSUM_LEN] != expected_checksum.as_bytes() {
        return Err(BorsukError::InvalidStorage(
            "sparse named sidecar segment checksum mismatch".to_string(),
        ));
    }
    let content_hash = &bytes[SPARSE_NAMED_SIDECAR_CHECKSUM_LEN..header];
    let sidecar_bytes = &bytes[header..];
    if blake3::hash(sidecar_bytes).as_bytes() != content_hash {
        return Err(BorsukError::InvalidStorage(
            "sparse named sidecar content hash mismatch".to_string(),
        ));
    }
    SparseNamedSidecar::from_bytes(sidecar_bytes)
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

fn candidate_record_indices(
    segment: &Segment,
    graph: Option<&SegmentGraph>,
    query: &[f32],
    mode: &SearchMode,
    leaf_mode: LeafMode,
    k: usize,
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
    let query_code = routing_code(query);
    let query_pq_code = if matches!(leaf_mode, LeafMode::PqScan | LeafMode::VamanaPq) {
        Some(pq_code_for_query(segment, query)?)
    } else {
        None
    };
    let mut indices = (0..segment.records.len()).collect::<Vec<_>>();
    indices.sort_by(|left, right| {
        let left_distance =
            candidate_code_distance(segment, *left, query_code, query_pq_code.as_deref());
        let right_distance =
            candidate_code_distance(segment, *right, query_code, query_pq_code.as_deref());
        left_distance
            .partial_cmp(&right_distance)
            .unwrap_or(Ordering::Equal)
            .then_with(|| segment.records[*left].id.cmp(&segment.records[*right].id))
    });

    let Some(graph) = graph else {
        indices.truncate(limit);
        return Ok(CandidateRecordSelection {
            indices,
            graph_candidates_added: 0,
            truncated,
        });
    };

    let mut selected = Vec::with_capacity(limit);
    let mut selected_set = HashSet::with_capacity(limit);
    let entry_count = k.max(1).min(limit).min(indices.len());
    for record_index in indices.iter().copied().take(entry_count) {
        selected.push(record_index);
        selected_set.insert(record_index);
    }

    let mut adjacency = HashMap::<usize, Vec<usize>>::new();
    for edge in &graph.edges {
        if edge.source_record_index >= segment.records.len()
            || edge.neighbor_record_index >= segment.records.len()
        {
            continue;
        }
        adjacency
            .entry(edge.source_record_index)
            .or_default()
            .push(edge.neighbor_record_index);
    }

    let mut graph_candidates_added = 0_usize;
    let mut frontier = selected
        .iter()
        .filter_map(|index| adjacency.get(index))
        .flatten()
        .copied()
        .collect::<Vec<_>>();

    while selected.len() < limit {
        let Some(frontier_position) =
            best_frontier_position(segment, query, &frontier, &selected_set)?
        else {
            break;
        };
        let neighbor_index = frontier.swap_remove(frontier_position);
        if selected_set.insert(neighbor_index) {
            selected.push(neighbor_index);
            graph_candidates_added += 1;
            if let Some(neighbors) = adjacency.get(&neighbor_index) {
                frontier.extend(neighbors.iter().copied());
            }
        }
    }

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

fn best_frontier_position(
    segment: &Segment,
    query: &[f32],
    frontier: &[usize],
    selected: &HashSet<usize>,
) -> Result<Option<usize>> {
    let mut best = None::<(usize, f32)>;
    for (position, record_index) in frontier.iter().copied().enumerate() {
        if selected.contains(&record_index) {
            continue;
        }

        let distance = segment
            .metric
            .distance(query, &segment.records[record_index].vector)?;
        let is_better = best.is_none_or(|(best_position, best_distance)| {
            distance
                .partial_cmp(&best_distance)
                .unwrap_or(Ordering::Equal)
                .then_with(|| {
                    segment.records[record_index]
                        .id
                        .cmp(&segment.records[frontier[best_position]].id)
                })
                .is_lt()
        });
        if is_better {
            best = Some((position, distance));
        }
    }

    Ok(best.map(|(position, _)| position))
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
        LeafMode::FlatScan | LeafMode::SqScan | LeafMode::PqScan => false,
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
        } => SearchMode::Approx {
            leaf_mode: *leaf_mode,
            eps: *eps,
            max_segments: *max_segments,
            max_bytes: *max_bytes,
            max_latency_ms: *max_latency_ms,
            routing_page_overfetch: *routing_page_overfetch,
            max_candidates_per_segment: None,
            adaptive_stop: *adaptive_stop,
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
    metric.distance(query, &summary.centroid)
}

fn page_ref_routing_rank_distance(
    page_ref: &RoutingLayerPageRef,
    query: &[f32],
    metric: &VectorMetric,
) -> Result<f32> {
    metric.distance(query, &page_ref.centroid)
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

fn candidate_code_distance(
    segment: &Segment,
    record_index: usize,
    query_code: f32,
    query_pq_code: Option<&[u8]>,
) -> f32 {
    if let Some(query_pq_code) = query_pq_code {
        return pq_code_distance(segment, record_index, query_pq_code);
    }

    routing_code_distance(segment, record_index, query_code)
}

fn pq_code_distance(segment: &Segment, record_index: usize, query_code: &[u8]) -> f32 {
    let Some(code) = segment.pq_codes.get(record_index) else {
        return f32::INFINITY;
    };

    code.iter()
        .zip(query_code)
        .map(|(left, right)| {
            let diff = f32::from(*left) - f32::from(*right);
            diff * diff
        })
        .sum()
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

/// Wrap sparse inverted-index hits in a `SearchReport` so a sparse named vector
/// can participate in hybrid fusion. Only `hits` drives fusion; the counters
/// stay zero because the sparse leg reads its single object outside the
/// segment/routing machinery the other counters measure.
fn sparse_leg_report(hits: Vec<SearchHit>) -> SearchReport {
    SearchReport {
        hits,
        leaf_mode: "sparse".to_string(),
        termination_reason: SearchTerminationReason::Complete,
        recall_guarantee: RecallGuarantee::Exact,
        segments_total: 0,
        segments_searched: 0,
        segments_skipped: 0,
        routing_page_indexes_read: 0,
        routing_pages_read: 0,
        bytes_read: 0,
        prefetched_bytes_unused: 0,
        graph_bytes_read: 0,
        object_cache_hits: 0,
        object_cache_misses: 0,
        cache_repairs: 0,
        records_considered: 0,
        records_scored: 0,
        graph_candidates_added: 0,
        resident_bytes_estimate: 0,
        elapsed_ms: 0,
        requests: RequestCounts::default(),
        rows_evaluated: 0,
        rows_passed_filter: 0,
        segments_pruned_by_filter: 0,
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
    use chrono::Utc;

    use super::*;

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
                &SearchOptions::approx(3, LeafMode::PqScan).with_max_segments(3),
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
                &SearchOptions::approx(1, LeafMode::PqScan)
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
                &SearchOptions::approx(2, LeafMode::PqScan)
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
                &SearchOptions::approx(2, LeafMode::PqScan)
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
            .storage
            .publish_manifest_with_top_routing_page_refs(&manifest, 2, &[l2_root])
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
            .storage
            .publish_manifest_with_top_routing_page_refs(&manifest, 2, &[l2_root])
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
            .storage
            .publish_manifest_with_top_routing_page_refs(&manifest, 2, &[l2_root])
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
            .storage
            .publish_manifest_with_top_routing_page_refs(&manifest, 1, &top_refs)
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
            .storage
            .publish_manifest_with_top_routing_page_refs(&manifest, 0, &[sparse_leaf])
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
            .storage
            .publish_manifest_with_top_routing_page_refs(
                &manifest,
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
            .storage
            .publish_manifest_with_top_routing_page_refs(
                &manifest,
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
            object_count: 1,
            dimensions: 2,
            centroid: vector.clone(),
            radius: 0.0,
            bounds_min: vector.clone(),
            bounds_max: vector.clone(),
            checksum: format!("{ordinal:064x}"),
            size_bytes: 1,
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
            created_at: Utc::now(),
        }
    }
}
