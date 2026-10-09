//! A single authenticated root for frozen two-bit nomination and on-demand SQ8.
use crate::semantic_unit_router::{SemanticProfile, SemanticUnitRouter, SourceIdentity};
#[cfg(test)]
use crate::sq8_s3_range::rank_verified_sq8_pages_inner;
use crate::{
    budgeted_page_rank::{
        BudgetedPageError, BudgetedPagePlan, choose_budgeted_pages_sparse, cover_pages,
    },
    canonical_source::CanonicalSource,
    object_native_generation::{
        MetadataReadStats, ObjectNativeOpenError, metadata_location, stage_two_bit_metadata,
    },
    rotated_two_bit::PreparedTwoBit,
    sq8_page_authority::{PageAuthority, PageError},
    sq8_s3_range::{
        OneAttemptS3, RankedSq8, RankedSq8Failure, Sq8RangeTrace, Sq8ReadStats, VerifiedRange,
        fetch_verified_ranges_inner, rank_verified_sq8_pages_traced,
    },
    two_bit_mutations::{TwoBitMutationHit, TwoBitMutationSnapshot},
    two_bit_source::{SourceBuildError, SourcePlaneReceipt, TwoBitPlane, read_authenticated},
    unit_centroid_graph::{UnitCentroidGraph, UnitCentroidGraphError},
    unit_centroid_pages::{UnitCentroidError, UnitCentroidPages},
};
use object_store::{ObjectStore, ObjectStoreExt, path::Path as ObjectPath};
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, collections::BTreeSet, fs, path::Path};
use tokio::sync::Semaphore;

/// Generation identity, admission, planning or authenticated range-read failure.
#[derive(Debug)]
pub enum TwoBitGenerationError {
    /// Invalid root, binding or budget.
    Invalid(&'static str),
    /// Local metadata I/O.
    Io(std::io::Error),
    /// Remote metadata staging identity, transport or scratch failure.
    Stage(ObjectNativeOpenError),
    /// Source-plane identity or codec.
    Plane(SourceBuildError),
    /// Centroid metadata/query.
    Centroid(UnitCentroidError),
    /// Graph metadata/query.
    Graph(UnitCentroidGraphError),
    /// Page authority.
    Page(PageError),
    /// Physical page admission.
    Budget(BudgetedPageError),
    /// Remote failure with physical request accounting.
    Read(RankedSq8Failure),
    /// Source range failure, before SQ8 reads.
    SourceRead(RankedSq8Failure),
    /// SQ8 failure after successfully charged source reads.
    PagedRead {
        /// Source charges already incurred before SQ8 ranking.
        source: Sq8ReadStats,
        /// SQ8 error and its charges.
        sq8: RankedSq8Failure,
    },
    /// Source nomination failed after authenticated payload was charged.
    SourcePlanning {
        /// Authenticated source charges before nomination failed.
        stats: Sq8ReadStats,
        /// Original nomination or physical-admission error.
        error: Box<TwoBitGenerationError>,
    },
    /// Semantic router authentication or policy failure.
    Router(String),
    /// Incurred leaf charges, retained through later failures.
    RouterCharged {
        /// Logical leaf reads and verified bytes already incurred.
        stats: Sq8ReadStats,
        /// Failure in leaf validation or a downstream stage.
        error: Box<TwoBitGenerationError>,
    },
    /// Query intervals retained even when an admitted stage fails.
    Query {
        /// Observed intervals, including failed admitted stages.
        stages: QueryStages,
        /// Underlying error and incurred I/O charges.
        error: Box<TwoBitGenerationError>,
    },
    /// Source HEAD transport failure during startup.
    SourceHead(object_store::Error),
}
impl TwoBitGenerationError {
    /// Source and SQ8 logical request/verified-byte charges for this failure.
    pub fn read_stats(&self) -> (Sq8ReadStats, Sq8ReadStats) {
        match self {
            Self::RouterCharged { error, .. } | Self::Query { error, .. } => error.read_stats(),
            Self::Read(failure) => (Sq8ReadStats::default(), failure.stats),
            Self::SourceRead(failure) => (failure.stats, Sq8ReadStats::default()),
            Self::PagedRead { source, sq8 } => (*source, sq8.stats),
            Self::SourcePlanning { stats, .. } => (*stats, Sq8ReadStats::default()),
            _ => (Sq8ReadStats::default(), Sq8ReadStats::default()),
        }
    }
    /// Selected-leaf logical requests and authenticated bytes, including failures.
    pub fn router_stats(&self) -> Sq8ReadStats {
        match self {
            Self::RouterCharged { stats, .. } => *stats,
            Self::Query { error, .. } => error.router_stats(),
            _ => Sq8ReadStats::default(),
        }
    }
    /// Monotonic intervals since the admitted query began, if search reached a stage.
    pub fn stages(&self) -> Option<&QueryStages> {
        match self {
            Self::Query { stages, .. } => Some(stages),
            Self::RouterCharged { error, .. } => error.stages(),
            _ => None,
        }
    }
    /// Machine-readable reason when a valid direct-closure query was refused by an
    /// enforced memory/byte cap before any payload I/O; `None` for every other error.
    pub fn direct_resource_rejection(&self) -> Option<DirectClosureRejection> {
        match self {
            Self::Query { error, .. } | Self::RouterCharged { error, .. } => {
                error.direct_resource_rejection()
            }
            Self::Invalid(reason) if *reason == DIRECT_MEMORY_CAP => {
                Some(DirectClosureRejection::ModeledMemory)
            }
            Self::Invalid(reason) if *reason == DIRECT_BYTE_CAP => {
                Some(DirectClosureRejection::PlannedBytes)
            }
            _ => None,
        }
    }
    fn with_router(self, stats: Sq8ReadStats) -> Self {
        if stats.submitted_gets == 0 {
            self
        } else {
            Self::RouterCharged {
                stats,
                error: Box::new(self),
            }
        }
    }
    fn charged_read(source: Sq8ReadStats, sq8: RankedSq8Failure) -> Self {
        if source.submitted_gets == 0 {
            Self::Read(sq8)
        } else {
            Self::PagedRead { source, sq8 }
        }
    }
}
impl std::fmt::Display for TwoBitGenerationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "two-bit generation: {self:?}")
    }
}
impl std::error::Error for TwoBitGenerationError {}
type Result<T> = std::result::Result<T, TwoBitGenerationError>;

/// Payload admission including retired pins and concurrent planner/read buffers.
/// Runtime, allocator and transport overhead require separate RSS measurement.
#[derive(Clone, Copy)]
pub struct TwoBitGenerationLimits {
    /// Total modeled payload bytes.
    pub max_memory_bytes: u64,
    /// Concurrent searches, from planning through rank completion.
    pub max_active_queries: usize,
    /// Physical SQ8 bytes per query.
    pub max_query_bytes: usize,
    /// Physical range GETs per query.
    pub max_query_gets: usize,
    /// Concurrent GETs per admitted query, bounded by the admitted batch length.
    /// Retained payload is charged in full; transport overhead needs an RSS gate.
    pub max_parallel_gets: usize,
    /// Maximum authenticated source bytes per query, separate from SQ8.
    pub max_source_bytes: usize,
    /// Maximum source range GETs per query.
    pub max_source_gets: usize,
    /// Concurrent source GETs per query, bounded by the admitted batch length.
    pub max_parallel_source_gets: usize,
    /// Codec lookup scratch per query.
    pub max_query_scratch_bytes: usize,
    /// Payload charged to other pinned generations and immutable mutation snapshots.
    pub already_pinned_bytes: u64,
}

/// Explicit positive SQ8 caps for one opt-in direct-closure query. They are
/// independent of the historical `max_query_*` limits, which direct search never
/// reads or relaxes. The library carries no dataset, dimension or benchmark pin.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DirectClosureLimits {
    /// Physical SQ8 bytes, including every bridge page, per query.
    pub max_sq8_bytes: usize,
    /// Physical SQ8 range GETs per query.
    pub max_sq8_gets: usize,
}

/// Exact modeled memory for direct-closure search. The generation's existing
/// admitted model is kept whole (no SOURCE or baseline-query credit) and one full
/// direct query budget per active query is added.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct DirectClosureMemory {
    /// Existing model: metadata copies, baseline query/source budgets and
    /// `already_pinned_bytes`.
    pub resident_bytes: u64,
    /// Planner term inside `direct_query_bytes`, per active query: 1 MiB
    /// discovery/closure working set, full trace capacity, normalized query and
    /// 512 bytes per page of closure/cover roster.
    pub direct_planner_bytes: u64,
    /// `(3 * max_sq8_bytes + planner + ranking) * max_active_queries`, where ranking
    /// is `256 * min(rows, max_sq8_bytes / (D + 12)) + 4 * D`.
    pub direct_query_bytes: u64,
    /// `resident_bytes + direct_query_bytes`.
    pub total_bytes: u64,
    /// The generation's `max_memory_bytes` admission cap.
    pub cap_bytes: u64,
}

/// Valid-input resource refusals enforced by direct-closure admission before any
/// payload I/O. Host OOM, deadline or kill outcomes are never reported here.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DirectClosureRejection {
    /// `DirectClosureMemory::total_bytes` exceeded the generation memory cap.
    ModeledMemory,
    /// The deterministic cover needed more SQ8 bytes than `max_sq8_bytes`.
    PlannedBytes,
}
const DIRECT_MEMORY_CAP: &str = "direct closure memory cap";
const DIRECT_BYTE_CAP: &str = "direct closure byte cap";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Manifest {
    pub(crate) schema: String,
    pub(crate) generation: u64,
    pub(crate) base_epoch: u64,
    pub(crate) plane_manifest_sha256: String,
    pub(crate) page_manifest_sha256: String,
    pub(crate) discovery: Discovery,
    pub(crate) sq8_object_sha256: String,
    pub(crate) sq8_object_key: String,
    pub(crate) sq8_etag: String,
    pub(crate) canonical: CanonicalSource,
    pub(crate) low: Vec<f32>,
    pub(crate) step: Vec<f32>,
}
pub(crate) const SCHEMA: &str = "borsuk-two-bit-generation-v8";
pub(crate) const METADATA_FILES: [&str; 10] = [
    "manifest.json",
    "page_manifest.json",
    "page_digests.bin",
    "centroids.bin",
    "graph.bin",
    "diverse_graph.bin",
    "plane/manifest.json",
    "plane/mean.bin",
    "plane/records.bin",
    "plane/page_digests.bin",
];
pub(crate) const ROUTER_FILES: [&str; 3] = [
    "router/root.bin",
    "router/membership.bin",
    "router/leaves.bin",
];
pub(crate) const ROUTER_ROOT_CAP: usize = 4 * 1024 * 1024;

/// Concrete discovery policy. Compaction inherits it unless explicitly changed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiscoveryMode {
    /// Contemporaneous two-graph control.
    Graph,
    /// Fixed semantic unit-leaf discovery.
    Semantic,
}
#[derive(Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Discovery {
    Graph {
        centroids_sha256: String,
        graph_sha256: String,
        graph_resident_bytes: usize,
        diverse_graph_sha256: String,
        diverse_graph_resident_bytes: usize,
    },
    Semantic {
        profile: SemanticProfile,
        root_sha256: String,
        root_bytes: usize,
        membership_sha256: String,
        membership_bytes: usize,
        leaves_sha256: String,
        leaves_bytes: usize,
        // Construction provenance, independent of the new generation root.
        input_schema: String,
        input_root_sha256: String,
        centroids_sha256: String,
        source_sha256: String,
        source_order_sha256: String,
        mean_sha256: String,
        records_sha256: String,
        sq8_sha256: String,
    },
}
impl Discovery {
    pub(crate) fn mode(&self) -> DiscoveryMode {
        match self {
            Self::Graph { .. } => DiscoveryMode::Graph,
            Self::Semantic { .. } => DiscoveryMode::Semantic,
        }
    }
    pub(crate) fn valid(&self, rows: usize, dimensions: usize) -> bool {
        use crate::resident_graph_generation::valid_sha256 as valid;
        match self {
            Self::Graph {
                centroids_sha256,
                graph_sha256,
                graph_resident_bytes,
                diverse_graph_sha256,
                diverse_graph_resident_bytes,
            } => {
                valid(centroids_sha256)
                    && valid(graph_sha256)
                    && valid(diverse_graph_sha256)
                    && *graph_resident_bytes > 0
                    && *diverse_graph_resident_bytes > 0
            }
            Self::Semantic {
                profile,
                root_sha256,
                root_bytes,
                membership_sha256,
                membership_bytes,
                leaves_sha256,
                leaves_bytes,
                input_schema,
                input_root_sha256,
                centroids_sha256,
                source_sha256,
                source_order_sha256,
                mean_sha256,
                records_sha256,
                sq8_sha256,
            } => {
                profile.valid_geometry(rows, dimensions)
                    && (512..=profile.root_cap()).contains(root_bytes)
                    && (*root_bytes - 512) % (64 + 4 * dimensions) == 0
                    && (1..=2 * rows.div_ceil(32).div_ceil(64) - 1)
                        .contains(&((*root_bytes - 512) / (64 + 4 * dimensions)))
                    && (1..=256).contains(&input_schema.len())
                    && *membership_bytes == rows.div_ceil(32) * 4
                    && *leaves_bytes == rows.div_ceil(32) * (4 + dimensions * 2)
                    && [
                        root_sha256,
                        membership_sha256,
                        leaves_sha256,
                        input_root_sha256,
                        centroids_sha256,
                        source_sha256,
                        source_order_sha256,
                        mean_sha256,
                        records_sha256,
                        sq8_sha256,
                    ]
                    .into_iter()
                    .all(|s| valid(s))
            }
        }
    }
    pub(crate) fn files(&self, startup: bool) -> Vec<&'static str> {
        let mut files = METADATA_FILES
            .into_iter()
            .filter(|name| {
                !(startup && *name == "plane/records.bin")
                    && (self.mode() == DiscoveryMode::Graph
                        || !["centroids.bin", "graph.bin", "diverse_graph.bin"].contains(name))
            })
            .collect::<Vec<_>>();
        if self.mode() == DiscoveryMode::Semantic {
            files.extend(
                ROUTER_FILES
                    .into_iter()
                    .filter(|name| !startup || *name != "router/leaves.bin"),
            );
        }
        files
    }
    fn graph_memory(&self) -> Option<u64> {
        match self {
            Self::Graph {
                graph_resident_bytes,
                diverse_graph_resident_bytes,
                ..
            } => (*graph_resident_bytes as u64).checked_add(*diverse_graph_resident_bytes as u64),
            Self::Semantic { .. } => Some(0),
        }
    }
    pub(crate) fn input<'a>(&'a self, plane: &SourcePlaneReceipt) -> Result<SourceIdentity<'a>> {
        let Self::Semantic {
            profile,
            input_schema,
            input_root_sha256,
            centroids_sha256,
            source_sha256,
            source_order_sha256,
            mean_sha256,
            records_sha256,
            sq8_sha256,
            ..
        } = self
        else {
            return Err(TwoBitGenerationError::Invalid("semantic descriptor"));
        };
        if source_sha256 != &plane.source_sha256
            || source_order_sha256 != &plane.source_order_sha256
            || mean_sha256 != &plane.mean_sha256
            || records_sha256 != &plane.records_sha256
            || sq8_sha256 != &plane.sq8_sha256
        {
            return Err(TwoBitGenerationError::Invalid("router source binding"));
        }
        Ok(SourceIdentity {
            profile: *profile,
            schema: input_schema,
            root_sha256: input_root_sha256,
            centroids_sha256,
            rows: plane.rows,
            dimensions: plane.dimensions,
        })
    }
}
enum LoadedDiscovery {
    Graph {
        centroids: UnitCentroidPages,
        graphs: [UnitCentroidGraph; 2],
    },
    Semantic {
        router: SemanticUnitRouter,
    },
}
struct RemoteSource {
    authority: PageAuthority,
    location: ObjectPath,
    etag: String,
}

/// Cumulative generation-local cache observations, separate from transport reads.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SourceCacheStats {
    /// Modeled cache storage, tags/control and additional concurrent query metadata.
    pub charged_bytes: usize,
    /// Preallocated compressed-source payload capacity (including tail padding).
    pub capacity_bytes: usize,
    /// Authenticated payload currently held, excluding unused slots/tail padding.
    pub occupied_bytes: usize,
    /// Original planned ranges served entirely from the cache.
    pub hits: u64,
    /// Original planned ranges requiring their unchanged full fetch.
    pub misses: u64,
    /// Authenticated blocks replaced by another block in the same slot.
    pub evictions: u64,
}

// Owned only by its immutable generation: root/source location/ETag/authority
// cannot change while any query borrows the cache. Reopening always starts empty.
struct SourceCache {
    block_bytes: usize,
    object_bytes: usize,
    tags: Box<[usize]>,
    bytes: Box<[u8]>,
    stats: SourceCacheStats,
}
impl SourceCache {
    fn block_len(&self, block: usize) -> usize {
        self.block_bytes
            .min(self.object_bytes - block * self.block_bytes)
    }

    fn lookup(&mut self, range: &std::ops::Range<usize>) -> Option<VerifiedRange> {
        let first = range.start / self.block_bytes;
        let end = range.end.div_ceil(self.block_bytes);
        if !range.start.is_multiple_of(self.block_bytes)
            || (range.end != self.object_bytes && !range.end.is_multiple_of(self.block_bytes))
            || (first..end).any(|block| self.tags[block % self.tags.len()] != block)
        {
            self.stats.misses = self.stats.misses.saturating_add(1);
            return None;
        }
        // One lock covers all tags AND the copy. No borrowed/sliced cache Bytes
        // survives it, and the existing full-cover query payload charge applies.
        let mut bytes = bytes::BytesMut::with_capacity(range.len());
        for block in first..end {
            let offset = (block % self.tags.len()) * self.block_bytes;
            bytes.extend_from_slice(&self.bytes[offset..offset + self.block_len(block)]);
        }
        self.stats.hits = self.stats.hits.saturating_add(1);
        Some(VerifiedRange {
            start: range.start,
            bytes: bytes.freeze(),
        })
    }

    fn insert(&mut self, range: &VerifiedRange) {
        if !range.start.is_multiple_of(self.block_bytes) {
            return;
        }
        for (index, bytes) in range.bytes.chunks(self.block_bytes).enumerate() {
            let block = range.start / self.block_bytes + index;
            if bytes.len() != self.block_len(block) {
                continue;
            }
            let slot = block % self.tags.len();
            let old = self.tags[slot];
            if old == block {
                continue;
            }
            if old != usize::MAX {
                self.stats.evictions = self.stats.evictions.saturating_add(1);
                self.stats.occupied_bytes -= self.block_len(old);
            }
            // Invalidate before copying; poison recovery cannot expose a partial block.
            self.tags[slot] = usize::MAX;
            let offset = slot * self.block_bytes;
            self.bytes[offset..offset + bytes.len()].copy_from_slice(bytes);
            self.tags[slot] = block;
            self.stats.occupied_bytes += bytes.len();
        }
    }
}
/// Monotonic stage boundaries relative to the admitted query's start.
#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct StageInterval {
    /// Nanoseconds from query start.
    pub start_ns: u128,
    /// Nanoseconds from query start, including failed attempts.
    pub end_ns: u128,
}
/// Query phase intervals, not inferred network waves or wire accounting.
#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct QueryStages {
    /// Query preparation and graph/semantic discovery from authenticated metadata.
    pub discovery: StageInterval,
    /// Authenticated source fetch plus cache lookup/copy/insertion, after discovery.
    pub source: StageInterval,
    /// Shared source scoring and physical SQ8 plan.
    pub planning: StageInterval,
    /// Native authenticated SQ8 fetch/rank, after releasing source bodies.
    pub sq8: StageInterval,
    /// Selected-leaf read concurrency; zero for membership discovery.
    pub leaf_peak_inflight: usize,
}
/// A bounded generation query with independently charged source and SQ8 reads.
pub struct TwoBitSearchResult {
    /// SQ8 physical admission after source nomination.
    pub plan: BudgetedPagePlan,
    /// Exact SQ8 candidates and read charges.
    pub ranked: RankedSq8,
    /// Source nomination reads; zero for the local resident reference.
    pub source_stats: Sq8ReadStats,
    /// Router payload charges; zero for membership discovery.
    pub router_stats: Sq8ReadStats,
    /// Stage intervals from the admitted query.
    pub stages: QueryStages,
    /// True only for direct-closure search: no SOURCE nomination ran, so a trace's
    /// `primary_page`, `ranked_candidate_pages` and `nomination_evaluated_units` are
    /// unset (not zero-page evidence) and `plan.selected_pages` is the required closure.
    pub source_nomination_skipped: bool,
}
/// Remote namespace startup accounting; query measurements remain separate.
#[derive(Debug, serde::Serialize)]
pub struct RemoteOpenStats {
    /// Fixed, bounded roster of metadata-object transfers.
    pub metadata: Vec<MetadataReadStats>,
    /// All metadata staging, including scratch creation and transfers.
    pub staging_wall_ns: u128,
    /// Authenticated local decoding and admission after staging.
    pub decode_wall_ns: u128,
    /// Logical source HEAD requests, separate from metadata-object staging.
    pub source_head_requests: u64,
    /// Source HEAD wall time, excluded from local decoding time.
    pub source_head_wall_ns: u128,
    /// Logical whole-leaf object HEAD; zero for membership discovery.
    pub router_head_requests: u64,
    /// Whole-leaf HEAD interval, separate from local decode.
    pub router_head_wall_ns: u128,
}

/// Immutable metadata; SQ8 rows are fetched conditionally and never cached here.
pub struct TwoBitGeneration {
    root_sha256: String,
    modeled_memory_bytes: u64,
    plane: TwoBitPlane,
    pages: PageAuthority,
    discovery: LoadedDiscovery,
    manifest: Manifest,
    limits: TwoBitGenerationLimits,
    slots: Semaphore,
    remote_open_stats: Option<RemoteOpenStats>,
    source: Option<RemoteSource>,
    source_cache: Option<std::sync::Mutex<SourceCache>>,
}

/// One generation-pinned base fetch merged with a bounded immutable delta.
pub struct TwoBitMutationSearchResult {
    /// Physical base-page admission, unchanged by mutation visibility.
    pub plan: BudgetedPagePlan,
    /// Up to k visible logical candidates ordered by score then ID.
    pub candidates: Vec<TwoBitMutationHit>,
    /// Base-query GETs/bytes/failures; earlier snapshot recovery is lifecycle I/O.
    pub stats: Sq8ReadStats,
    /// Authenticated source reads before base SQ8 ranking.
    pub source_stats: Sq8ReadStats,
    /// Selected whole-leaf charges; separate from source/SQ8.
    pub router_stats: Sq8ReadStats,
    /// Stage intervals from the admitted query.
    pub stages: QueryStages,
    /// Latest-state rows visited, including tombstones.
    pub mutation_rows_scanned: usize,
    /// Normalized pending puts scored in this query.
    pub mutation_put_rows_scored: usize,
    /// Pinned snapshot revision, independent of subsequent publications.
    pub mutation_revision: u64,
    /// Pinned authenticated mutation-body identity.
    pub mutation_sha256: String,
}

/// One bounded source graph discovery, in nearest/diversity order.
#[doc(hidden)]
#[derive(Debug, Default, serde::Serialize)]
pub struct TwoBitDiscoveryTrace {
    /// Page of the initial best centroid.
    pub seed_page: usize,
    /// Distinct unit IDs evaluated by this graph's128-budget seed search.
    pub seed_evaluated_units: Vec<usize>,
    /// Distinct unit IDs evaluated by this graph's1272-budget page walk.
    pub walk_evaluated_units: Vec<usize>,
    /// Seed search exhausted its fixed allowance.
    pub seed_work_exhausted: bool,
    /// Page walk exhausted its fixed allowance.
    pub walk_work_exhausted: bool,
}
/// Offline union trace; retained payload is caller-owned after planning.
#[doc(hidden)]
#[derive(Debug, Default, serde::Serialize)]
pub struct TwoBitPlanTrace {
    /// Source nomination order: up to318 graph pages or512 Fresh1m closure pages.
    pub ranked_candidate_pages: Vec<usize>,
    /// Globally distinct32-row units source-scored, including bounded completion.
    pub nomination_evaluated_units: Vec<usize>,
    /// First ranked page, required by physical admission.
    pub primary_page: usize,
    /// Nearest then diversity discovery; identical graph identities run once.
    pub discoveries: Vec<TwoBitDiscoveryTrace>,
    /// Authenticated semantic nomination, absent for graph mode.
    pub semantic_leaves: Vec<usize>,
    /// Original semantic units before seed completion.
    pub semantic_units: Vec<usize>,
    /// Units added within the selected seed page.
    pub semantic_seed_additions: Vec<usize>,
}
impl TwoBitPlanTrace {
    /// Conservative retained ID/record payload excluding allocator overhead.
    pub fn scratch_bytes(rows: usize) -> usize {
        let units = rows.div_ceil(32);
        // Graph discoveries and semantic IDs are mutually exclusive. Charge the
        // larger retained capacity, including nomination's full completion reserve
        // and the discovery Vec's minimum four-record growth allocation.
        let graph_ids = 2 * rows.div_ceil(256).min(159)
            + 2 * (units.min(128) + units.min(1272))
            + units.min(2544);
        let semantic_ids = rows.div_ceil(256).min(512) + units.min(4096) + 48 + units.min(3072) + 8;
        let graph_bytes = graph_ids * std::mem::size_of::<usize>()
            + 4 * std::mem::size_of::<TwoBitDiscoveryTrace>();
        let semantic_bytes = semantic_ids * std::mem::size_of::<usize>();
        graph_bytes.max(semantic_bytes) + std::mem::size_of::<Self>()
    }
}
/// Offline source nomination and SQ8 page admission using the production planner.
/// The caller binds the prepared query and authenticated records to one source;
/// discovery, source-read charges and retained trace memory remain caller-owned.
/// The explicit profile admits semantic walk/completion bounds; `None` retains
/// graph limits. Semantic discovery supplies its entire admitted page closure.
#[doc(hidden)]
pub fn plan_two_bit_source_walks<'a>(
    rows: usize,
    dimensions: usize,
    nomination_page_limit: usize,
    semantic_profile: Option<SemanticProfile>,
    walks: &[(usize, Vec<usize>)],
    prepared: &PreparedTwoBit,
    mut record: impl FnMut(usize) -> Option<&'a [u8]>,
    max_query_gets: usize,
    max_query_bytes: usize,
    trace: Option<&mut TwoBitPlanTrace>,
) -> Result<BudgetedPagePlan> {
    // Reuse physical admission to reject overflowing geometry before walk/row
    // arithmetic. Actual query budgets are applied after nomination as before.
    choose_budgeted_pages_sparse(&[], &[0], rows, dimensions, 1, 1, usize::MAX)
        .map_err(TwoBitGenerationError::Budget)?;
    if semantic_profile.is_some_and(|profile| !profile.valid_geometry(rows, dimensions)) {
        return Err(TwoBitGenerationError::Invalid("source semantic profile"));
    }
    let source_unit_limit = semantic_profile.map_or(2544, SemanticProfile::source_unit_limit);
    let mut nomination_evaluated_units = Vec::with_capacity(if trace.is_some() {
        rows.div_ceil(32).min(source_unit_limit)
    } else {
        0
    });
    let score = |unit: usize| {
        if trace.is_some() {
            nomination_evaluated_units.push(unit);
        }
        let mut maximum = f64::NEG_INFINITY;
        for row in unit * 32..((unit + 1) * 32).min(rows) {
            maximum = maximum.max(
                prepared
                    .score(
                        record(row)
                            .ok_or(TwoBitGenerationError::Invalid("missing source record"))?,
                    )
                    .map_err(|e| TwoBitGenerationError::Plane(SourceBuildError::Codec(e)))?,
            );
        }
        Ok(maximum)
    };
    let ranked =
        rank_walked_source_with_limit(rows, walks, nomination_page_limit, semantic_profile, score)?;
    if let Some(trace) = trace {
        trace.primary_page = ranked[0].0;
        trace.nomination_evaluated_units = nomination_evaluated_units;
        trace.ranked_candidate_pages = ranked.iter().map(|&(page, _)| page).collect();
    }
    let primary = [ranked[0].0 * 256];
    let order = ranked
        .iter()
        .enumerate()
        .map(|(rank, &(page, _))| (page, rank as f32))
        .collect::<Vec<_>>();
    choose_budgeted_pages_sparse(
        &order,
        &primary,
        rows,
        dimensions,
        rows.div_ceil(256),
        max_query_gets,
        max_query_bytes,
    )
    .map_err(TwoBitGenerationError::Budget)
}

/// Exact production query normalization for offline discovery and SQ8 scoring.
/// Prepare two-bit source scoring from the original query before normalizing.
#[doc(hidden)]
pub fn normalize_two_bit_diagnostic_query(query: &[f32]) -> Result<Cow<'_, [f32]>> {
    crate::sq8_source::cosine_vector(query).map_err(TwoBitGenerationError::Plane)
}

/// Offline source-page closure with the production cover and byte admission.
#[doc(hidden)]
pub fn plan_two_bit_source_cover(
    closure: &BTreeSet<usize>,
    rows: usize,
    record_bytes: usize,
    max_source_gets: usize,
    max_source_bytes: usize,
) -> Result<(Vec<std::ops::Range<usize>>, usize)> {
    let (cover, bytes) = cover_pages(closure, rows, record_bytes, 256, max_source_gets)
        .map_err(TwoBitGenerationError::Budget)?;
    if bytes > max_source_bytes {
        return Err(TwoBitGenerationError::Budget(
            BudgetedPageError::InsufficientBudget,
        ));
    }
    Ok((cover, bytes))
}

// Source nomination over bounded graph walks; no corpus-sized query allocation.
#[cfg(test)]
fn rank_walked_source(
    rows: usize,
    walks: &[(usize, Vec<usize>)],
    score: impl FnMut(usize) -> Result<f64>,
) -> Result<Vec<(usize, f64)>> {
    rank_walked_source_with_limit(rows, walks, rows.div_ceil(256).min(159), None, score)
}

fn admit_source_walks(
    rows: usize,
    walks: &[(usize, Vec<usize>)],
    semantic_profile: Option<SemanticProfile>,
) -> Result<Vec<usize>> {
    let invalid = || TwoBitGenerationError::Invalid("walked source geometry");
    let walk_limit = semantic_profile.map_or(1272, SemanticProfile::walk_unit_limit);
    let walk_count = if semantic_profile.is_some() { 1 } else { 2 };
    if rows == 0 || walks.is_empty() || walks.len() > walk_count {
        return Err(invalid());
    }
    let mut units = Vec::with_capacity(walk_count * walk_limit);
    for (seed, evaluated) in walks {
        if *seed >= rows.div_ceil(256) || evaluated.is_empty() || evaluated.len() > walk_limit {
            return Err(invalid());
        }
        let mut sorted = evaluated.clone();
        sorted.sort_unstable();
        if sorted.iter().any(|&unit| unit >= rows.div_ceil(32))
            || sorted.windows(2).any(|pair| pair[0] == pair[1])
            || (seed * 8..((seed + 1) * 8).min(rows.div_ceil(32)))
                .any(|unit| sorted.binary_search(&unit).is_err())
        {
            return Err(invalid());
        }
        units.extend_from_slice(&sorted);
    }
    units.sort_unstable();
    units.dedup();
    if let Some(profile) = semantic_profile {
        let pages = units.iter().map(|unit| unit / 8).collect::<BTreeSet<_>>();
        if pages.len() > profile.closure_page_limit() {
            return Err(TwoBitGenerationError::Invalid("semantic closure page cap"));
        }
    }
    Ok(units)
}

fn rank_walked_source_with_limit(
    rows: usize,
    walks: &[(usize, Vec<usize>)],
    page_limit: usize,
    semantic_profile: Option<SemanticProfile>,
    mut score: impl FnMut(usize) -> Result<f64>,
) -> Result<Vec<(usize, f64)>> {
    let invalid = || TwoBitGenerationError::Invalid("walked source geometry");
    if page_limit == 0
        || page_limit > rows.div_ceil(256)
        || (semantic_profile.is_none() && page_limit != rows.div_ceil(256).min(159))
        || semantic_profile.is_some_and(|profile| page_limit > profile.closure_page_limit())
    {
        return Err(invalid());
    }
    let count = page_limit;
    let units = admit_source_walks(rows, walks, semantic_profile)?;
    if semantic_profile.is_some()
        && units
            .iter()
            .map(|unit| unit / 8)
            .collect::<BTreeSet<_>>()
            .len()
            != page_limit
    {
        return Err(invalid());
    }
    let source_unit_limit = semantic_profile.map_or(2544, SemanticProfile::source_unit_limit);
    let page_capacity = semantic_profile.map_or(2544, SemanticProfile::closure_page_limit);
    let mut memo = Vec::with_capacity(rows.div_ceil(32).min(source_unit_limit));
    for unit in units {
        let value = score(unit)?;
        if !value.is_finite() {
            return Err(TwoBitGenerationError::Invalid("nonfinite source score"));
        }
        memo.push((unit, value));
    }
    fn page_maxima(mut units: Vec<(usize, f64)>, capacity: usize) -> Vec<(usize, f64)> {
        units.sort_unstable_by_key(|&(page, _)| page);
        let mut pages = Vec::<(usize, f64)>::with_capacity(units.len().min(capacity));
        for (page, value) in units {
            if let Some(last) = pages.last_mut()
                && last.0 == page
            {
                last.1 = last.1.max(value);
            } else {
                pages.push((page, value));
            }
        }
        pages
    }
    let mut global = page_maxima(
        memo.iter()
            .map(|&(unit, value)| (unit / 8, value))
            .collect(),
        page_capacity,
    );
    let walked_count = memo.len();
    let mut completion = global.clone();
    completion.sort_unstable_by(|&(lp, l), &(rp, r)| r.total_cmp(&l).then(lp.cmp(&rp)));
    'complete: for (page, _) in completion {
        for unit in page * 8..((page + 1) * 8).min(rows.div_ceil(32)) {
            if memo.len() == source_unit_limit {
                break 'complete;
            }
            if memo[..walked_count]
                .binary_search_by_key(&unit, |&(id, _)| id)
                .is_ok()
            {
                continue;
            }
            let value = score(unit)?;
            if !value.is_finite() {
                return Err(TwoBitGenerationError::Invalid("nonfinite source score"));
            }
            memo.push((unit, value));
        }
    }
    global = page_maxima(
        memo.iter()
            .map(|&(unit, value)| (unit / 8, value))
            .collect(),
        page_capacity,
    );
    let mut selected = Vec::with_capacity(walks.len() * count);
    for (seed, evaluated) in walks {
        let mut pages = page_maxima(
            evaluated
                .iter()
                .map(|unit| {
                    let index = global
                        .binary_search_by_key(&(unit / 8), |&(page, _)| page)
                        .unwrap();
                    (unit / 8, global[index].1)
                })
                .collect(),
            page_capacity,
        );
        pages.sort_unstable_by(|&(lp, l), &(rp, r)| r.total_cmp(&l).then(lp.cmp(&rp)));
        if pages.len() < count {
            return Err(invalid());
        }
        selected.push(*seed);
        selected.extend(
            pages
                .iter()
                .filter(|&&(page, _)| page != *seed)
                .take(count - 1)
                .map(|&(page, _)| page),
        );
    }
    selected.sort_unstable();
    selected.dedup();
    let mut ranked = selected
        .into_iter()
        .map(|page| {
            let index = global.binary_search_by_key(&page, |&(id, _)| id).unwrap();
            (page, global[index].1)
        })
        .collect::<Vec<_>>();
    ranked.sort_unstable_by(|&(lp, l), &(rp, r)| r.total_cmp(&l).then(lp.cmp(&rp)));
    Ok(ranked)
}

impl TwoBitGeneration {
    /// Physical row count of this immutable nonempty base.
    pub fn rows(&self) -> usize {
        self.pages.rows()
    }
    /// Explicit semantic profile, absent for graph discovery.
    pub fn semantic_profile(&self) -> Option<SemanticProfile> {
        match &self.manifest.discovery {
            Discovery::Semantic { profile, .. } => Some(*profile),
            _ => None,
        }
    }
    pub(crate) fn modeled_memory_bytes(&self) -> u64 {
        self.modeled_memory_bytes
    }

    /// Opt in to a generation-local cache of authenticated 256-row source blocks.
    /// Zero disables it; positive budgets require a remote source and at least
    /// one slot. The cap includes tags/control and extra query metadata, not just
    /// payload. Existing full-cover admission and query concurrency remain in force.
    /// Configure before sharing the generation; replacement drops the old cache
    /// before allocation. Include this generation's model in subsequent pinned
    /// generation admission. A hit does not probe current object-store availability.
    pub fn with_source_cache(mut self, max_bytes: usize) -> Result<Self> {
        let bad = TwoBitGenerationError::Invalid;
        if let Some(old) = self.source_cache.take() {
            let old = old
                .into_inner()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            self.modeled_memory_bytes -= old.stats.charged_bytes as u64;
        }
        if max_bytes == 0 {
            return Ok(self);
        }
        let source = self
            .source
            .as_ref()
            .ok_or(bad("source cache requires remote source"))?;
        let block_bytes = self
            .plane
            .receipt()
            .record_bytes
            .checked_mul(256)
            .ok_or(bad("source cache geometry"))?;
        let slot_bytes = block_bytes
            .checked_add(std::mem::size_of::<usize>())
            .ok_or(bad("source cache geometry"))?;
        // Fixed boxed capacities never grow. Allow 64 bytes per allocation in
        // addition to control structures and both cache-path query Vec capacities.
        // Payload copies already fit the existing 2 * full source cover per query.
        let query_metadata = self
            .limits
            .max_source_gets
            .min(self.rows().div_ceil(256))
            .checked_mul(
                std::mem::size_of::<VerifiedRange>() + std::mem::size_of::<(usize, usize)>(),
            )
            .and_then(|n| n.checked_add(2 * (std::mem::size_of::<Vec<usize>>() + 64)))
            .and_then(|n| n.checked_mul(self.limits.max_active_queries))
            .ok_or(bad("source cache memory"))?;
        let fixed = std::mem::size_of::<Option<std::sync::Mutex<SourceCache>>>()
            .checked_add(2 * 64)
            .and_then(|n| n.checked_add(query_metadata))
            .ok_or(bad("source cache memory"))?;
        let slots = max_bytes
            .checked_sub(fixed)
            .ok_or(bad("source cache capacity"))?
            / slot_bytes;
        let slots = slots.min(self.rows().div_ceil(256));
        if slots == 0 {
            return Err(bad("source cache capacity"));
        }
        let charged_bytes = slots
            .checked_mul(slot_bytes)
            .and_then(|n| n.checked_add(fixed))
            .ok_or(bad("source cache memory"))?;
        let modeled = self
            .modeled_memory_bytes
            .checked_add(charged_bytes as u64)
            .filter(|&n| n <= self.limits.max_memory_bytes)
            .ok_or(bad("source cache memory cap"))?;
        // All cache and concurrent query capacity is admitted before either allocation.
        let capacity_bytes = slots * block_bytes;
        let mut tags = Vec::new();
        tags.try_reserve_exact(slots)
            .map_err(|_| bad("source cache allocation"))?;
        tags.resize(slots, usize::MAX);
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(capacity_bytes)
            .map_err(|_| bad("source cache allocation"))?;
        bytes.resize(capacity_bytes, 0);
        self.source_cache = Some(std::sync::Mutex::new(SourceCache {
            block_bytes,
            object_bytes: source.authority.object_bytes(),
            tags: tags.into_boxed_slice(),
            bytes: bytes.into_boxed_slice(),
            stats: SourceCacheStats {
                charged_bytes,
                capacity_bytes,
                ..SourceCacheStats::default()
            },
        }));
        self.modeled_memory_bytes = modeled;
        Ok(self)
    }

    /// Snapshot of cumulative range hits/misses and block occupancy/evictions.
    /// Disabled caches return zeros; these counters do not describe network billing.
    pub fn source_cache_stats(&self) -> SourceCacheStats {
        self.source_cache
            .as_ref()
            .map_or(SourceCacheStats::default(), |cache| {
                cache
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .stats
            })
    }

    /// Stream only generation metadata from an authorized immutable prefix,
    /// then reuse authenticated local open. No SQ8/source-vector GET is issued.
    /// Scratch is removed on success/failure/cancellation. The caller configures
    /// metadata transport retries; query range caps are a separate boundary.
    pub async fn open_remote(
        store: &dyn ObjectStore,
        prefix: &ObjectPath,
        trusted_sha256: &str,
        limits: TwoBitGenerationLimits,
        scratch_parent: &Path,
    ) -> Result<Self> {
        Self::open_remote_inner(store, prefix, trusted_sha256, limits, scratch_parent, None).await
    }

    /// Reuse the bounded immutable root authenticated by an authorized head.
    /// Its retained allocation is charged in addition to existing pins, even if
    /// the caller releases the head after this open. Child admission is unchanged.
    pub async fn open_remote_from_head(
        store: &dyn ObjectStore,
        head: &crate::two_bit_store::TwoBitHead,
        limits: TwoBitGenerationLimits,
        scratch_parent: &Path,
    ) -> Result<Self> {
        let caller_pins = limits.already_pinned_bytes;
        let limits = TwoBitGenerationLimits {
            already_pinned_bytes: limits
                .already_pinned_bytes
                .checked_add(head.retained_root_bytes())
                .ok_or(TwoBitGenerationError::Invalid("retained root memory"))?,
            ..limits
        };
        let mut generation = Self::open_remote_inner(
            store,
            &head.metadata_prefix(),
            head.root_sha256(),
            limits,
            scratch_parent,
            Some(head),
        )
        .await?;
        // The retained root is already in modeled_memory_bytes. It must not
        // authorize caller-owned mutation snapshots or exclusion rosters.
        generation.limits.already_pinned_bytes = caller_pins;
        Ok(generation)
    }

    async fn open_remote_inner(
        store: &dyn ObjectStore,
        prefix: &ObjectPath,
        trusted_sha256: &str,
        limits: TwoBitGenerationLimits,
        scratch_parent: &Path,
        head: Option<&crate::two_bit_store::TwoBitHead>,
    ) -> Result<Self> {
        let staging_started = std::time::Instant::now();
        let (scratch, metadata) = stage_two_bit_metadata(
            store,
            prefix,
            trusted_sha256,
            limits
                .max_memory_bytes
                .saturating_sub(limits.already_pinned_bytes),
            scratch_parent,
            head,
        )
        .await
        .map_err(TwoBitGenerationError::Stage)?;
        let staging_wall_ns = staging_started.elapsed().as_nanos();
        let location = metadata_location(prefix, "plane/records.bin");
        let head_started = std::time::Instant::now();
        let head = store
            .head(&location)
            .await
            .map_err(TwoBitGenerationError::SourceHead)?;
        let source_head_wall_ns = head_started.elapsed().as_nanos();
        let etag = head
            .e_tag
            .filter(|etag| {
                !etag.is_empty() && !etag.starts_with("W/") && !etag.chars().any(char::is_control)
            })
            .ok_or(TwoBitGenerationError::Invalid("source ETag"))?;
        let decode_started = std::time::Instant::now();
        let mut generation = Self::open_inner(
            scratch.path(),
            trusted_sha256,
            limits,
            Some((location, etag, head.size)),
            false,
        )?;
        let decode_wall_ns = decode_started.elapsed().as_nanos();
        generation.remote_open_stats = Some(RemoteOpenStats {
            metadata,
            staging_wall_ns,
            decode_wall_ns,
            source_head_requests: 1,
            source_head_wall_ns,
            router_head_requests: 0,
            router_head_wall_ns: 0,
        });
        Ok(generation)
    }

    /// Startup counters for a remote open; absent for local metadata opens.
    pub fn remote_open_stats(&self) -> Option<&RemoteOpenStats> {
        self.remote_open_stats.as_ref()
    }

    /// Open local metadata under a trusted root SHA. No PQ or SQ8 payload load.
    /// Artifact paths are fixed under `root`; caller supplies immutable metadata.
    pub fn open(root: &Path, trusted_sha256: &str, limits: TwoBitGenerationLimits) -> Result<Self> {
        Self::open_inner(root, trusted_sha256, limits, None, false)
    }
    /// Reuse serving admission/identity checks while streaming local records.
    /// No metadata-only local generation escapes or authorizes query snapshots.
    pub(crate) fn validate_local_publication(
        root: &Path,
        trusted_sha256: &str,
        limits: TwoBitGenerationLimits,
    ) -> Result<()> {
        Self::open_inner(root, trusted_sha256, limits, None, true).map(drop)
    }
    fn open_inner(
        root: &Path,
        trusted_sha256: &str,
        limits: TwoBitGenerationLimits,
        remote: Option<(ObjectPath, String, u64)>,
        validate_local_records: bool,
    ) -> Result<Self> {
        let paged = remote.is_some() || validate_local_records;
        let mut limits = limits;
        let bad = TwoBitGenerationError::Invalid;
        let size = |name: &str| {
            let path = root.join(name);
            let metadata = if validate_local_records {
                fs::symlink_metadata(path)
            } else {
                fs::metadata(path)
            }
            .map_err(TwoBitGenerationError::Io)?;
            if validate_local_records && !metadata.is_file() {
                return Err(bad("nonregular publication artifact"));
            }
            Ok(metadata.len())
        };
        let manifest_size = size("manifest.json")?;
        if manifest_size == 0
            || manifest_size > 65536
            || limits
                .max_memory_bytes
                .saturating_sub(limits.already_pinned_bytes)
                < 131072
            || limits.max_active_queries == 0
            || limits.max_query_bytes == 0
            || limits.max_query_gets == 0
            || limits.max_parallel_gets == 0
            || limits.max_query_scratch_bytes == 0
            || limits.max_source_bytes == 0
            || limits.max_source_gets == 0
            || limits.max_parallel_source_gets == 0
        {
            return Err(bad("root or admission"));
        }
        let body = read_authenticated(
            &root.join("manifest.json"),
            manifest_size as usize,
            trusted_sha256,
        )
        .map_err(TwoBitGenerationError::Plane)?;
        let manifest: Manifest = serde_json::from_slice(&body).map_err(|_| bad("root schema"))?;
        let key = manifest.sq8_object_key.split('/').collect::<Vec<_>>();
        if manifest.schema != SCHEMA
            || manifest.generation == 0
            || !manifest
                .discovery
                .valid(manifest.canonical.rows, manifest.canonical.dimensions)
            || manifest.sq8_etag.is_empty()
            || manifest.sq8_etag.starts_with("W/")
            || manifest.sq8_etag.chars().any(char::is_control)
            || key.len() < 2
            || key[key.len() - 2] != "objects"
            || key[key.len() - 1] != manifest.sq8_object_sha256
            || key.iter().any(|s| {
                s.is_empty()
                    || *s == "."
                    || *s == ".."
                    || !s
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
            })
            || manifest.low.is_empty()
            || manifest.low.len() != manifest.step.len()
            || manifest.low.iter().any(|v| !v.is_finite())
            || manifest.step.iter().any(|v| !v.is_finite() || *v <= 0.)
            || !manifest.canonical.valid()
        {
            return Err(bad("root identity"));
        }
        if validate_local_records {
            // These files are authenticated by the publisher's existing
            // semantic validation/uploads, before it can commit the head.
            size("canonical.bin")?;
            if manifest.discovery.mode() == DiscoveryMode::Semantic {
                size("router/leaves.bin")?;
                size("centroids.bin")?;
            }
        }
        if manifest.discovery.mode() == DiscoveryMode::Semantic {
            limits.max_source_bytes = limits.max_source_bytes.min(64 * 1024 * 1024);
            limits.max_source_gets = limits.max_source_gets.min(128);
            limits.max_query_bytes = limits.max_query_bytes.min(16_773_120);
            limits.max_query_gets = limits.max_query_gets.min(32);
        }
        let files = manifest.discovery.files(paged);
        let mut disk = manifest_size;
        let mut sizes = Vec::with_capacity(files.len());
        for name in files {
            if name == "manifest.json" || name == "router/leaves.bin" {
                continue;
            }
            let length = size(name)?;
            disk = disk.checked_add(length).ok_or(bad("metadata size"))?;
            sizes.push((name, length));
        }
        // Admit all encoded/decoded metadata copies before loading the plane
        // receipt, then refine the model with authenticated geometry below.
        if disk
            .checked_mul(3)
            .and_then(|n| n.checked_add(131072))
            .and_then(|n| n.checked_add(limits.already_pinned_bytes))
            .is_none_or(|n| n > limits.max_memory_bytes)
        {
            return Err(bad("metadata memory cap"));
        }
        // Keep the admitted lengths; never allocate from a later, larger stat.
        let admitted_size = |name: &str| -> Result<usize> {
            let length = sizes
                .iter()
                .find(|(n, _)| *n == name)
                .ok_or(bad("artifact name"))?
                .1;
            usize::try_from(length).map_err(|_| bad("artifact size"))
        };
        let plane_size = admitted_size("plane/manifest.json")?;
        if plane_size == 0 || plane_size > 65536 {
            return Err(bad("plane manifest size"));
        }
        let plane_body = read_authenticated(
            &root.join("plane/manifest.json"),
            plane_size,
            &manifest.plane_manifest_sha256,
        )
        .map_err(TwoBitGenerationError::Plane)?;
        let geometry: SourcePlaneReceipt =
            serde_json::from_slice(&plane_body).map_err(|_| bad("plane schema"))?;
        let padded =
            crate::rotated_two_bit::RotatedTwoBitCodec::padded_dimensions(geometry.dimensions)
                .map_err(|e| TwoBitGenerationError::Plane(SourceBuildError::Codec(e)))?;
        let expected_mean = geometry.dimensions.checked_mul(4).ok_or(bad("mean size"))?;
        let expected_records = geometry
            .rows
            .checked_mul(padded.div_ceil(4) + 8)
            .ok_or(bad("record size"))?;
        if geometry.dimensions != manifest.low.len()
            || geometry.dimensions != manifest.canonical.dimensions
            || geometry.rows != manifest.canonical.rows
            || admitted_size("plane/mean.bin")? != expected_mean
            || (!paged && admitted_size("plane/records.bin")? != expected_records)
            || (validate_local_records && size("plane/records.bin")? != expected_records as u64)
            || admitted_size("plane/page_digests.bin")?
                != geometry
                    .rows
                    .div_ceil(32)
                    .checked_mul(32)
                    .ok_or(bad("source digest size"))?
        {
            return Err(bad("plane file geometry"));
        }
        // Three copies cover reads/decoding, plus graph towers. Per-query 1MiB
        // covers sequential discovery/planner vectors through Fresh1m's3079 walk
        // and4096 SOURCE units. Retained diagnostic arrays use their own scratch charge.
        let planner_bytes = (limits.max_query_scratch_bytes as u64)
            .checked_add(1024 * 1024)
            .ok_or(bad("query memory"))?;
        let query_memory = crate::returned_sq8::query_payload_bytes(
            crate::exact_sq8_nominee::Sq8Geometry {
                rows: geometry.rows,
                dimensions: geometry.dimensions,
            },
            limits.max_query_bytes as u64,
            planner_bytes,
            limits.max_active_queries as u64,
        )
        .map_err(|_| bad("query memory"))?;
        // Mean is 4D bytes. 64 times its admitted length conservatively covers
        // padded codec metadata and its temporary copies, including tiny-N/high-D.
        let codec_memory = (admitted_size("plane/mean.bin")? as u64)
            .checked_mul(64)
            .ok_or(bad("codec memory"))?;
        let source_query_memory = if paged {
            limits
                .max_source_bytes
                .min(expected_records)
                .checked_mul(2)
                .and_then(|n| n.checked_mul(limits.max_active_queries))
                .ok_or(bad("source query memory"))? as u64
        } else {
            0
        };
        let validation_page_bytes = if validate_local_records {
            geometry
                .rows
                .min(32)
                .checked_mul(padded.div_ceil(4) + 8)
                .ok_or(bad("validation page memory"))? as u64
        } else {
            0
        };
        let modeled = disk
            .checked_mul(3)
            .and_then(|n| n.checked_add(codec_memory))
            .and_then(|n| n.checked_add(131072))
            .and_then(|n| n.checked_add(manifest.discovery.graph_memory()?))
            .and_then(|n| {
                n.checked_add(if manifest.discovery.mode() == DiscoveryMode::Semantic {
                    // Root geometry is authenticated by the generation descriptor.
                    // Charge directory capacities, including construction scratch,
                    // before open allocates them. Retired generations are added below.
                    let root = admitted_size("router/root.bin").ok()? as u64;
                    let Discovery::Semantic { root_bytes, .. } = &manifest.discovery else {
                        unreachable!()
                    };
                    let leaves = (*root_bytes - 512) / (64 + 4 * geometry.dimensions);
                    let (_, directory_peak) =
                        crate::semantic_unit_router::directory_allocation_bytes(
                            geometry.rows.div_ceil(32),
                            leaves,
                        )
                        .ok()?;
                    root.checked_mul(32)?
                        .checked_add(directory_peak as u64)?
                        .checked_add(
                            (1024 * 1024_u64).checked_mul(limits.max_active_queries as u64)?,
                        )?
                } else {
                    0
                })
            })
            .and_then(|n| n.checked_add(query_memory))
            .and_then(|n| n.checked_add(source_query_memory))
            .and_then(|n| n.checked_add(validation_page_bytes))
            .and_then(|n| n.checked_add(limits.already_pinned_bytes))
            .ok_or(bad("memory overflow"))?;
        if modeled > limits.max_memory_bytes {
            return Err(bad("memory cap"));
        }
        let memory = usize::try_from(limits.max_memory_bytes).map_err(|_| bad("memory width"))?;
        let (plane, source) = if paged {
            let (plane, authority) = TwoBitPlane::open_metadata(
                &root.join("plane"),
                &manifest.plane_manifest_sha256,
                &manifest.sq8_object_sha256,
                manifest.generation,
                memory,
            )
            .map_err(TwoBitGenerationError::Plane)?;
            if validate_local_records {
                plane
                    .validate_local_records(&root.join("plane/records.bin"), &authority)
                    .map_err(TwoBitGenerationError::Plane)?;
            }
            let source = if let Some((location, etag, bytes)) = remote {
                if u64::try_from(authority.object_bytes()).map_err(|_| bad("source size"))? != bytes
                {
                    return Err(bad("source HEAD geometry"));
                }
                Some(RemoteSource {
                    authority,
                    location,
                    etag,
                })
            } else {
                None
            };
            (plane, source)
        } else {
            (
                TwoBitPlane::open(
                    &root.join("plane"),
                    &manifest.plane_manifest_sha256,
                    &manifest.sq8_object_sha256,
                    memory,
                )
                .map_err(TwoBitGenerationError::Plane)?,
                None,
            )
        };
        let read = |name: &str, digest: &str| -> Result<Vec<u8>> {
            let length = admitted_size(name)?;
            read_authenticated(&root.join(name), length, digest)
                .map_err(TwoBitGenerationError::Plane)
        };
        let page_size = admitted_size("page_manifest.json")?;
        if page_size > 65536 {
            return Err(bad("page manifest size"));
        }
        let page_body = read("page_manifest.json", &manifest.page_manifest_sha256)?;
        let sidecar_size = admitted_size("page_digests.bin")?;
        // Sidecar identity is checked by PageAuthority against its pinned manifest.
        let mut sidecar = vec![0; sidecar_size];
        use std::io::Read;
        let mut file =
            fs::File::open(root.join("page_digests.bin")).map_err(TwoBitGenerationError::Io)?;
        file.read_exact(&mut sidecar)
            .map_err(TwoBitGenerationError::Io)?;
        if file.read(&mut [0]).map_err(TwoBitGenerationError::Io)? != 0 {
            return Err(bad("sidecar length"));
        }
        let pages = PageAuthority::load(&page_body, &manifest.page_manifest_sha256, &sidecar)
            .map_err(TwoBitGenerationError::Page)?;
        let discovery = match &manifest.discovery {
            Discovery::Graph {
                centroids_sha256,
                graph_sha256,
                graph_resident_bytes,
                diverse_graph_sha256,
                diverse_graph_resident_bytes,
            } => {
                let centroid_blob = read("centroids.bin", centroids_sha256)?;
                let centroids = UnitCentroidPages::decode(&centroid_blob)
                    .map_err(TwoBitGenerationError::Centroid)?;
                if centroids.rows() != geometry.rows
                    || centroids.dimensions() != geometry.dimensions
                    || centroids.page_rows() != 256
                    || centroids.unit_rows() != 32
                {
                    return Err(bad("centroid geometry"));
                }
                let mut graphs = Vec::with_capacity(2);
                for (name, digest, resident) in [
                    ("graph.bin", graph_sha256, *graph_resident_bytes),
                    (
                        "diverse_graph.bin",
                        diverse_graph_sha256,
                        *diverse_graph_resident_bytes,
                    ),
                ] {
                    let blob = read(name, digest)?;
                    if UnitCentroidGraph::preflight_resident_bytes(&blob, &centroids)
                        .map_err(TwoBitGenerationError::Graph)?
                        != resident
                    {
                        return Err(bad("graph memory declaration"));
                    }
                    graphs.push(
                        UnitCentroidGraph::decode_bounded(
                            &blob,
                            &centroid_blob,
                            &centroids,
                            resident,
                        )
                        .map_err(TwoBitGenerationError::Graph)?,
                    );
                }
                LoadedDiscovery::Graph {
                    centroids,
                    graphs: graphs.try_into().map_err(|_| bad("graph roster"))?,
                }
            }
            Discovery::Semantic {
                root_sha256,
                root_bytes,
                membership_sha256,
                membership_bytes,
                leaves_sha256,
                leaves_bytes,
                ..
            } => {
                if admitted_size("router/root.bin")? != *root_bytes
                    || admitted_size("router/membership.bin")? != *membership_bytes
                {
                    return Err(bad("router metadata length"));
                }
                let input = manifest.discovery.input(plane.receipt())?;
                let router = SemanticUnitRouter::open(
                    &read("router/root.bin", root_sha256)?,
                    &read("router/membership.bin", membership_sha256)?,
                    root_sha256,
                    &input,
                    ROUTER_ROOT_CAP,
                )
                .map_err(|e| TwoBitGenerationError::Router(e.to_string()))?;
                if router.manifest().leaf_payload.sha256 != *leaves_sha256
                    || router.manifest().leaf_payload.bytes != *leaves_bytes
                {
                    return Err(bad("router payload binding"));
                }
                LoadedDiscovery::Semantic { router }
            }
        };
        let receipt = plane.receipt();
        if pages.generation() != manifest.generation
            || pages.object_sha256() != manifest.sq8_object_sha256
            || pages.rows() != receipt.rows
            || pages.dimensions() != receipt.dimensions
            || pages.page_rows() != 256
            || manifest.low.len() != receipt.dimensions
        {
            return Err(bad("generation binding"));
        }
        Ok(Self {
            root_sha256: trusted_sha256.into(),
            modeled_memory_bytes: modeled,
            plane,
            pages,
            discovery,
            manifest,
            limits,
            slots: Semaphore::new(limits.max_active_queries),
            remote_open_stats: None,
            source,
            source_cache: None,
        })
    }
    /// Authenticated concrete discovery mode.
    pub fn discovery_mode(&self) -> DiscoveryMode {
        self.manifest.discovery.mode()
    }

    fn semantic_walks(
        router: &SemanticUnitRouter,
        ids: &[usize],
        trace: Option<&mut TwoBitPlanTrace>,
    ) -> Result<Vec<(usize, Vec<usize>)>> {
        let nomination = router
            .nominate_selected(ids)
            .map_err(|e| TwoBitGenerationError::Router(e.to_string()))?;
        if let Some(trace) = trace {
            trace.semantic_leaves = nomination.leaf_ids;
            trace.semantic_units = nomination.units.into_iter().collect();
            trace.semantic_seed_additions = nomination.seed_additions;
        }
        Ok(vec![(nomination.seed_page, nomination.walk_units)])
    }

    fn local_walks(
        &self,
        normalized: &[f32],
        trace: Option<&mut TwoBitPlanTrace>,
    ) -> Result<Vec<(usize, Vec<usize>)>> {
        let LoadedDiscovery::Semantic { router } = &self.discovery else {
            return self.discover_walks(normalized, trace);
        };
        let ids = router
            .select_leaves(normalized)
            .map_err(|e| TwoBitGenerationError::Router(e.to_string()))?;
        Self::semantic_walks(router, &ids, trace)
    }
    fn plan_inner<'a>(
        &self,
        query: &'a [f32],
        mut trace: Option<&mut TwoBitPlanTrace>,
    ) -> Result<(BudgetedPagePlan, Cow<'a, [f32]>)> {
        if self.source.is_some() {
            return Err(TwoBitGenerationError::Invalid("paged plan requires reader"));
        }
        let trace_bytes = if trace.is_some() {
            TwoBitPlanTrace::scratch_bytes(self.pages.rows())
        } else {
            0
        };
        let scratch = self
            .limits
            .max_query_scratch_bytes
            .checked_sub(trace_bytes)
            .ok_or(TwoBitGenerationError::Invalid("diagnostic scratch"))?;
        let prepared = self
            .plane
            .prepare_query(query, scratch)
            .map_err(|e| TwoBitGenerationError::Plane(SourceBuildError::Codec(e)))?;
        // The codec already validated finite, nonzero input. Normalize after its
        // temporary preparation buffer is released, within the same scratch cap.
        let normalized = normalize_two_bit_diagnostic_query(query)?;
        let walks = self.local_walks(normalized.as_ref(), trace.as_deref_mut())?;
        let plan = self.plan_walks(&walks, &prepared, |row| self.plane.record(row), trace)?;
        Ok((plan, normalized))
    }

    async fn plan_paged<'a>(
        &self,
        store: &dyn ObjectStore,
        query: &'a [f32],
        trace: Option<&mut TwoBitPlanTrace>,
    ) -> Result<(BudgetedPagePlan, Cow<'a, [f32]>, Sq8ReadStats, Sq8ReadStats)> {
        self.plan_paged_measured(
            store,
            query,
            trace,
            &mut QueryStages::default(),
            std::time::Instant::now(),
        )
        .await
    }

    async fn fetch_source_ranges(
        &self,
        store: &dyn ObjectStore,
        ranges: &[(usize, usize)],
    ) -> Result<(Vec<VerifiedRange>, Sq8ReadStats)> {
        let source = self
            .source
            .as_ref()
            .ok_or(TwoBitGenerationError::Invalid("source binding"))?;
        let Some(cache) = &self.source_cache else {
            return fetch_verified_ranges_inner(
                store,
                &source.location,
                &source.authority,
                ranges,
                &source.etag,
                self.limits.max_source_gets,
                self.limits.max_source_bytes,
                self.limits.max_parallel_source_gets,
            )
            .await
            .map_err(TwoBitGenerationError::SourceRead);
        };
        let mut verified = Vec::with_capacity(ranges.len());
        let mut misses = Vec::with_capacity(ranges.len());
        {
            // ponytail: one generation mutex serializes bounded copies; shard
            // only if measured concurrent copy contention warrants more state.
            let mut cache = cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for &(first, last) in ranges {
                let range = source
                    .authority
                    .byte_range(first, last)
                    .map_err(TwoBitGenerationError::Page)?;
                if let Some(hit) = cache.lookup(&range) {
                    verified.push(hit);
                } else {
                    misses.push((first, last));
                }
            }
        }
        let mut stats = Sq8ReadStats::default();
        if !misses.is_empty() {
            // The shared fetcher preserves ordered errors and drains every
            // admitted original miss. A failed batch inserts nothing.
            let (fetched, charged) = fetch_verified_ranges_inner(
                store,
                &source.location,
                &source.authority,
                &misses,
                &source.etag,
                self.limits.max_source_gets,
                self.limits.max_source_bytes,
                self.limits.max_parallel_source_gets,
            )
            .await
            .map_err(TwoBitGenerationError::SourceRead)?;
            stats = charged;
            let mut cache = cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for range in &fetched {
                cache.insert(range);
            }
            verified.extend(fetched);
        }
        verified.sort_unstable_by_key(|range| range.start);
        Ok((verified, stats))
    }

    async fn plan_paged_measured<'a>(
        &self,
        store: &dyn ObjectStore,
        query: &'a [f32],
        mut trace: Option<&mut TwoBitPlanTrace>,
        stages: &mut QueryStages,
        started: std::time::Instant,
    ) -> Result<(BudgetedPagePlan, Cow<'a, [f32]>, Sq8ReadStats, Sq8ReadStats)> {
        stages.discovery.start_ns = started.elapsed().as_nanos().max(1);
        self.source
            .as_ref()
            .ok_or(TwoBitGenerationError::Invalid("source binding"))?;
        let trace_bytes = if trace.is_some() {
            TwoBitPlanTrace::scratch_bytes(self.rows())
        } else {
            0
        };
        let scratch = self
            .limits
            .max_query_scratch_bytes
            .checked_sub(trace_bytes)
            .ok_or(TwoBitGenerationError::Invalid("diagnostic scratch"))?;
        let prepared = self
            .plane
            .prepare_query(query, scratch)
            .map_err(|e| TwoBitGenerationError::Plane(SourceBuildError::Codec(e)))?;
        let normalized = normalize_two_bit_diagnostic_query(query)?;
        let result = self.local_walks(normalized.as_ref(), trace.as_deref_mut());
        stages.discovery.end_ns = started.elapsed().as_nanos();
        let walks = result?;
        let router_stats = Sq8ReadStats::default();
        async {
            // Validate the same explicit discovery bounds before SOURCE cover/I/O.
            let closure = admit_source_walks(self.rows(), &walks, self.semantic_profile())?
                .into_iter()
                .map(|unit| unit / 8)
                .collect::<BTreeSet<_>>();
            let width = self.plane.receipt().record_bytes;
            let (cover, _) = plan_two_bit_source_cover(
                &closure,
                self.rows(),
                width,
                self.limits.max_source_gets,
                self.limits.max_source_bytes,
            )?;
            let unit_bytes = width
                .checked_mul(32)
                .ok_or(TwoBitGenerationError::Invalid("source unit geometry"))?;
            let ranges = cover
                .iter()
                .map(|r| (r.start / unit_bytes, (r.end - 1) / unit_bytes))
                .collect::<Vec<_>>();
            stages.source.start_ns = started.elapsed().as_nanos().max(1);
            let fetched = self.fetch_source_ranges(store, &ranges).await;
            stages.source.end_ns = started.elapsed().as_nanos();
            let (verified, stats) = fetched?;
            stages.planning.start_ns = started.elapsed().as_nanos().max(1);
            let plan = self
                .plan_walks(
                    &walks,
                    &prepared,
                    |row| {
                        let offset = row.checked_mul(width)?;
                        let index = verified
                            .partition_point(|range| range.start <= offset)
                            .checked_sub(1)?;
                        let relative = offset.checked_sub(verified[index].start)?;
                        verified[index]
                            .bytes
                            .get(relative..relative.checked_add(width)?)
                    },
                    trace,
                )
                .map_err(|error| TwoBitGenerationError::SourcePlanning {
                    stats,
                    error: Box::new(error),
                });
            stages.planning.end_ns = started.elapsed().as_nanos();
            let plan = plan?;
            drop(verified);
            Ok((plan, normalized, stats, router_stats))
        }
        .await
        .map_err(|error: TwoBitGenerationError| error.with_router(router_stats))
    }

    fn discover_walks(
        &self,
        normalized: &[f32],
        mut trace: Option<&mut TwoBitPlanTrace>,
    ) -> Result<Vec<(usize, Vec<usize>)>> {
        let count = self.pages.rows().div_ceil(256).min(159);
        let mut walks = Vec::with_capacity(2);
        let LoadedDiscovery::Graph { centroids, graphs } = &self.discovery else {
            return Err(TwoBitGenerationError::Invalid(
                "semantic plan requires membership discovery",
            ));
        };
        let Discovery::Graph {
            graph_sha256,
            diverse_graph_sha256,
            ..
        } = &self.manifest.discovery
        else {
            unreachable!()
        };
        let graph_count = if graph_sha256 == diverse_graph_sha256 {
            1
        } else {
            2
        };
        for graph in &graphs[..graph_count] {
            let seed = graph
                .search(centroids, normalized, 1, 128)
                .map_err(TwoBitGenerationError::Graph)?;
            let seed_page = seed
                .units
                .first()
                .ok_or(TwoBitGenerationError::Invalid("no seed"))?
                .0
                / 8;
            let (evaluated, exhausted) = if count > 1 {
                let found = graph
                    .search_pages_seeded(centroids, normalized, &[seed_page], count - 1, 1272)
                    .map_err(TwoBitGenerationError::Graph)?;
                (found.evaluated_units, found.work_exhausted)
            } else {
                (
                    (seed_page * 8..((seed_page + 1) * 8).min(self.pages.rows().div_ceil(32)))
                        .collect(),
                    false,
                )
            };
            if let Some(trace) = trace.as_deref_mut() {
                trace.discoveries.push(TwoBitDiscoveryTrace {
                    seed_page,
                    seed_evaluated_units: seed.evaluated_units,
                    seed_work_exhausted: seed.work_exhausted,
                    walk_evaluated_units: if count > 1 {
                        evaluated.clone()
                    } else {
                        Vec::new()
                    },
                    walk_work_exhausted: exhausted,
                });
            }
            walks.push((seed_page, evaluated));
        }
        Ok(walks)
    }

    fn plan_walks<'a>(
        &self,
        walks: &[(usize, Vec<usize>)],
        prepared: &PreparedTwoBit,
        record: impl FnMut(usize) -> Option<&'a [u8]>,
        trace: Option<&mut TwoBitPlanTrace>,
    ) -> Result<BudgetedPagePlan> {
        plan_two_bit_source_walks(
            self.pages.rows(),
            self.pages.dimensions(),
            if self.manifest.discovery.mode() == DiscoveryMode::Semantic {
                walks
                    .iter()
                    .flat_map(|(_, units)| units.iter().map(|u| u / 8))
                    .collect::<BTreeSet<_>>()
                    .len()
            } else {
                self.pages.rows().div_ceil(256).min(159)
            },
            self.semantic_profile(),
            walks,
            prepared,
            record,
            self.limits.max_query_gets,
            self.limits.max_query_bytes,
            trace,
        )
    }
    /// Offline physical plan and candidate pages in nomination order.
    /// Charges bounded retained trace payload from scratch; uses normal admission.
    /// Returned trace payload belongs to the caller after slot release.
    #[doc(hidden)]
    pub async fn diagnostic_plan(
        &self,
        query: &[f32],
    ) -> Result<(BudgetedPagePlan, TwoBitPlanTrace)> {
        let _permit = self
            .slots
            .acquire()
            .await
            .map_err(|_| TwoBitGenerationError::Invalid("query admission"))?;
        let mut trace = TwoBitPlanTrace::default();
        let (plan, _) = self.plan_inner(query, Some(&mut trace))?;
        Ok((plan, trace))
    }

    /// Diagnose an authenticated paged query with its source charges. The same
    /// slot is held through discovery, source fetch and nomination; SQ8 is not read.
    #[doc(hidden)]
    pub async fn diagnostic_plan_with_reader(
        &self,
        reader: &OneAttemptS3,
        query: &[f32],
    ) -> Result<(
        BudgetedPagePlan,
        TwoBitPlanTrace,
        Sq8ReadStats,
        Sq8ReadStats,
    )> {
        self.diagnostic_plan_with_store(reader.store(), query).await
    }
    /// Admitted diagnostic plan with semantic trace and source/router charges.
    pub async fn diagnostic_plan_with_store(
        &self,
        store: &dyn ObjectStore,
        query: &[f32],
    ) -> Result<(
        BudgetedPagePlan,
        TwoBitPlanTrace,
        Sq8ReadStats,
        Sq8ReadStats,
    )> {
        let _permit = self
            .slots
            .acquire()
            .await
            .map_err(|_| TwoBitGenerationError::Invalid("query admission"))?;
        let mut trace = TwoBitPlanTrace::default();
        if self.source.is_some() {
            let (plan, _, source, leaves) = self.plan_paged(store, query, Some(&mut trace)).await?;
            Ok((plan, trace, source, leaves))
        } else {
            let (plan, _) = self.plan_inner(query, Some(&mut trace))?;
            Ok((
                plan,
                trace,
                Sq8ReadStats::default(),
                Sq8ReadStats::default(),
            ))
        }
    }
    /// Shared-source two-graph discovery union, nomination and physical admission.
    pub async fn plan(&self, query: &[f32]) -> Result<BudgetedPagePlan> {
        let _permit = self
            .slots
            .acquire()
            .await
            .map_err(|_| TwoBitGenerationError::Invalid("query admission"))?;
        self.plan_inner(query, None).map(|(plan, _)| plan)
    }

    /// Plan against a caller-owned blob transport, retaining source charges.
    /// Counts are logical ObjectStore operations; the caller controls transport
    /// retries. Native S3 search uses OneAttemptS3 with retries disabled.
    pub async fn plan_with_store(
        &self,
        store: &dyn ObjectStore,
        query: &[f32],
    ) -> Result<(BudgetedPagePlan, Sq8ReadStats, Sq8ReadStats)> {
        let _permit = self
            .slots
            .acquire()
            .await
            .map_err(|_| TwoBitGenerationError::Invalid("query admission"))?;
        if self.source.is_some() {
            let (plan, _, stats, leaves) = self.plan_paged(store, query, None).await?;
            Ok((plan, stats, leaves))
        } else {
            Ok((
                self.plan_inner(query, None)?.0,
                Sq8ReadStats::default(),
                Sq8ReadStats::default(),
            ))
        }
    }
    /// Plan, fetch authenticated conditional ranges in one bounded wave, and rank.
    /// Reader errors retain physical request/byte/error accounting.
    pub async fn search(
        &self,
        reader: &OneAttemptS3,
        query: &[f32],
        top_k: usize,
    ) -> Result<TwoBitSearchResult> {
        self.search_inner(reader, query, top_k, None).await
    }

    /// Search with sorted unique logical IDs hidden by a caller-authorized,
    /// generation-bound mutation snapshot. Include its entire payload in
    /// `already_pinned_bytes` when opening this generation. Returns up to k
    /// visible rows; no compensating GET or full base-ID scan is performed.
    pub async fn search_excluding(
        &self,
        reader: &OneAttemptS3,
        query: &[f32],
        top_k: usize,
        excluded_ids: &[i64],
    ) -> Result<TwoBitSearchResult> {
        if excluded_ids.windows(2).any(|ids| ids[0] >= ids[1])
            || excluded_ids
                .len()
                .checked_mul(8)
                .is_none_or(|bytes| bytes as u64 > self.limits.already_pinned_bytes)
        {
            return Err(TwoBitGenerationError::Invalid(
                "mutation roster or admission",
            ));
        }
        self.search_inner(reader, query, top_k, Some(excluded_ids))
            .await
    }

    /// Root-bound upsert/delete search under the same query semaphore as base
    /// retrieval. Charge all retained snapshots in `already_pinned_bytes`.
    /// Pending puts are scanned once; no base-vector hydration or extra GET.
    /// Returns up to k rows, including underfill when nomination omits live rows.
    pub async fn search_with_mutations(
        &self,
        reader: &OneAttemptS3,
        query: &[f32],
        top_k: usize,
        mutations: &TwoBitMutationSnapshot,
    ) -> Result<TwoBitMutationSearchResult> {
        self.search_with_mutations_store(reader.store(), query, top_k, mutations)
            .await
    }
    /// Generation-bound mutation search using the admitted ObjectStore path.
    pub async fn search_with_mutations_store(
        &self,
        store: &dyn ObjectStore,
        query: &[f32],
        top_k: usize,
        mutations: &TwoBitMutationSnapshot,
    ) -> Result<TwoBitMutationSearchResult> {
        if !mutations.binds(&self.root_sha256, self.pages.dimensions())
            || mutations.resident_payload_bytes() as u64 > self.limits.already_pinned_bytes
        {
            return Err(TwoBitGenerationError::Invalid(
                "mutation binding or admission",
            ));
        }
        if top_k == 0 {
            return Err(TwoBitGenerationError::Invalid("top k"));
        }
        let fetched_rows = self
            .pages
            .rows()
            .min(self.limits.max_query_bytes / (self.pages.dimensions() + 12));
        let capacity = top_k.min(
            fetched_rows
                .checked_add(mutations.put_rows())
                .ok_or(TwoBitGenerationError::Invalid("mutation query memory"))?,
        );
        let extra = (capacity as u64)
            .checked_mul(32)
            .and_then(|n| n.checked_add(4096))
            .and_then(|n| n.checked_mul(self.limits.max_active_queries as u64))
            .and_then(|n| n.checked_add(self.modeled_memory_bytes))
            .ok_or(TwoBitGenerationError::Invalid("mutation query memory"))?;
        if extra > self.limits.max_memory_bytes {
            return Err(TwoBitGenerationError::Invalid("mutation query memory"));
        }
        let _permit = self
            .slots
            .acquire()
            .await
            .map_err(|_| TwoBitGenerationError::Invalid("query admission"))?;
        let base = self
            .search_store_unadmitted(
                store,
                query,
                top_k.min(self.pages.rows()),
                Some(mutations.excluded_ids()),
                None,
                None,
            )
            .await?;
        let candidates = mutations
            .rank_with_base(&base.ranked.candidates, query, top_k)
            .map_err(|error| {
                TwoBitGenerationError::charged_read(
                    base.source_stats,
                    RankedSq8Failure {
                        error: crate::sq8_s3_range::RangeFetchError::Score(error),
                        stats: base.ranked.stats,
                    },
                )
                .with_router(base.router_stats)
            })?;
        Ok(TwoBitMutationSearchResult {
            plan: base.plan,
            candidates,
            stats: base.ranked.stats,
            source_stats: base.source_stats,
            router_stats: base.router_stats,
            stages: base.stages,
            mutation_rows_scanned: mutations.rows().len(),
            mutation_put_rows_scored: mutations.put_rows(),
            mutation_revision: mutations.revision(),
            mutation_sha256: mutations.sha256().into(),
        })
    }

    async fn search_inner(
        &self,
        reader: &OneAttemptS3,
        query: &[f32],
        top_k: usize,
        excluded_ids: Option<&[i64]>,
    ) -> Result<TwoBitSearchResult> {
        if top_k == 0 || top_k > self.pages.rows() {
            return Err(TwoBitGenerationError::Invalid("top k"));
        }
        let _permit = self
            .slots
            .acquire()
            .await
            .map_err(|_| TwoBitGenerationError::Invalid("query admission"))?;
        self.search_unadmitted(reader, query, top_k, excluded_ids)
            .await
    }

    async fn search_unadmitted(
        &self,
        reader: &OneAttemptS3,
        query: &[f32],
        top_k: usize,
        excluded_ids: Option<&[i64]>,
    ) -> Result<TwoBitSearchResult> {
        self.search_store_unadmitted(reader.store(), query, top_k, excluded_ids, None, None)
            .await
    }

    /// Full search through a caller-owned ObjectStore, under native query admission.
    /// Transport attempts/retries are caller-owned; logical charges are returned.
    pub async fn search_with_store(
        &self,
        store: &dyn ObjectStore,
        query: &[f32],
        top_k: usize,
        excluded_ids: Option<&[i64]>,
    ) -> Result<TwoBitSearchResult> {
        if top_k == 0
            || top_k > self.rows()
            || excluded_ids.is_some_and(|ids| {
                ids.windows(2).any(|v| v[0] >= v[1])
                    || ids
                        .len()
                        .checked_mul(8)
                        .is_none_or(|n| n as u64 > self.limits.already_pinned_bytes)
            })
        {
            return Err(TwoBitGenerationError::Invalid("search admission"));
        }
        let _permit = self
            .slots
            .acquire()
            .await
            .map_err(|_| TwoBitGenerationError::Invalid("query admission"))?;
        self.search_store_unadmitted(store, query, top_k, excluded_ids, None, None)
            .await
    }

    /// One production search with a charged diagnostic trace; no repeated discovery/read pass.
    #[doc(hidden)]
    pub async fn diagnostic_search_with_store(
        &self,
        store: &dyn ObjectStore,
        query: &[f32],
        top_k: usize,
    ) -> Result<(TwoBitSearchResult, TwoBitPlanTrace)> {
        if top_k == 0 || top_k > self.rows() {
            return Err(TwoBitGenerationError::Invalid("search admission"));
        }
        let _permit = self
            .slots
            .acquire()
            .await
            .map_err(|_| TwoBitGenerationError::Invalid("query admission"))?;
        let mut trace = TwoBitPlanTrace::default();
        let result = self
            .search_store_unadmitted(store, query, top_k, None, Some(&mut trace), None)
            .await?;
        Ok((result, trace))
    }

    /// `diagnostic_search_with_store` plus opt-in SQ8 range diagnostics written to the
    /// caller's trace, which keeps every span even when the query fails. The trace's
    /// memory and range capacity are admitted before the query slot, any allocation
    /// or any GET; results, plans, charges and GETs are exactly the untraced ones.
    #[doc(hidden)]
    pub async fn diagnostic_search_with_store_traced(
        &self,
        store: &dyn ObjectStore,
        query: &[f32],
        top_k: usize,
        range_trace: &mut Sq8RangeTrace,
    ) -> Result<(TwoBitSearchResult, TwoBitPlanTrace)> {
        // Before every early return: a reused trace must not show a previous query as this one.
        range_trace.refuse();
        if top_k == 0 || top_k > self.rows() {
            return Err(TwoBitGenerationError::Invalid("search admission"));
        }
        self.admit_range_trace(
            range_trace,
            self.limits.max_query_gets,
            self.modeled_memory_bytes,
        )?;
        let _permit = self
            .slots
            .acquire()
            .await
            .map_err(|_| TwoBitGenerationError::Invalid("query admission"))?;
        let mut trace = TwoBitPlanTrace::default();
        let result = self
            .search_store_unadmitted(
                store,
                query,
                top_k,
                None,
                Some(&mut trace),
                Some(range_trace),
            )
            .await?;
        Ok((result, trace))
    }

    // Diagnostic memory is modeled per active query on top of the admitted total, and
    // the span buffer must hold every GET this query may issue. Non-resource refusal:
    // checked before the slot, any allocation or any request. This is the ONE place the trace's
    // cumulative peak is charged: a caller must not also include it in `already_pinned_bytes`
    // (the check would then count it twice), only the buffers it holds itself.
    fn admit_range_trace(
        &self,
        trace: &Sq8RangeTrace,
        max_gets: usize,
        admitted_total: u64,
    ) -> Result<()> {
        let bad = TwoBitGenerationError::Invalid;
        if trace.capacity() < max_gets {
            return Err(bad("diagnostic range capacity"));
        }
        let bytes = (trace.reserved_bytes(self.limits.max_parallel_gets) as u64)
            .checked_mul(self.limits.max_active_queries as u64)
            .and_then(|n| n.checked_add(admitted_total))
            .ok_or(bad("diagnostic memory"))?;
        if bytes > self.limits.max_memory_bytes {
            return Err(bad("diagnostic memory"));
        }
        Ok(())
    }

    async fn search_store_unadmitted(
        &self,
        store: &dyn ObjectStore,
        query: &[f32],
        top_k: usize,
        excluded_ids: Option<&[i64]>,
        trace: Option<&mut TwoBitPlanTrace>,
        mut range_trace: Option<&mut Sq8RangeTrace>,
    ) -> Result<TwoBitSearchResult> {
        let started = std::time::Instant::now();
        if let Some(range_trace) = range_trace.as_deref_mut() {
            range_trace.begin(started);
        }
        let mut stages = QueryStages::default();
        let result = self
            .search_store_measured(
                store,
                query,
                top_k,
                excluded_ids,
                trace,
                &mut stages,
                started,
                range_trace,
            )
            .await;
        Self::close_open_stages(&mut stages, started);
        match result {
            Ok(mut result) => {
                result.stages = stages;
                Ok(result)
            }
            Err(error) => Err(TwoBitGenerationError::Query {
                stages,
                error: Box::new(error),
            }),
        }
    }
    #[allow(clippy::too_many_arguments)]
    async fn search_store_measured(
        &self,
        store: &dyn ObjectStore,
        query: &[f32],
        top_k: usize,
        excluded_ids: Option<&[i64]>,
        trace: Option<&mut TwoBitPlanTrace>,
        stages: &mut QueryStages,
        started: std::time::Instant,
        range_trace: Option<&mut Sq8RangeTrace>,
    ) -> Result<TwoBitSearchResult> {
        let (plan, normalized, source_stats, router_stats) = if self.source.is_some() {
            self.plan_paged_measured(store, query, trace, stages, started)
                .await?
        } else {
            stages.discovery.start_ns = started.elapsed().as_nanos().max(1);
            let result = self.plan_inner(query, trace);
            stages.discovery.end_ns = started.elapsed().as_nanos();
            let (plan, normalized) = result?;
            (
                plan,
                normalized,
                Sq8ReadStats::default(),
                Sq8ReadStats::default(),
            )
        };

        let page_bytes = 256 * (self.pages.dimensions() + 12);
        let ranges = plan
            .ranges
            .iter()
            .map(|r| (r.start / page_bytes, (r.end - 1) / page_bytes))
            .collect::<Vec<_>>();
        stages.sq8.start_ns = started.elapsed().as_nanos().max(1);
        let ranked = rank_verified_sq8_pages_traced(
            store,
            &ObjectPath::from(self.manifest.sq8_object_key.clone()),
            &self.pages,
            &ranges,
            &self.manifest.sq8_etag,
            normalized.as_ref(),
            &self.manifest.low,
            &self.manifest.step,
            top_k,
            self.limits.max_query_gets,
            self.limits.max_query_bytes,
            self.limits.max_parallel_gets,
            excluded_ids.unwrap_or(&[]),
            range_trace,
        )
        .await;
        stages.sq8.end_ns = started.elapsed().as_nanos();
        let ranked = ranked.map_err(|sq8| {
            TwoBitGenerationError::charged_read(source_stats, sq8).with_router(router_stats)
        })?;
        if excluded_ids.is_none() && ranked.candidates.len() < top_k {
            return Err(TwoBitGenerationError::charged_read(
                source_stats,
                RankedSq8Failure {
                    error: crate::sq8_s3_range::RangeFetchError::Score(
                        crate::exact_sq8_nominee::Sq8ScoreError::InvalidRoster,
                    ),
                    stats: ranked.stats,
                },
            )
            .with_router(router_stats));
        }
        Ok(TwoBitSearchResult {
            plan,
            ranked,
            source_stats,
            router_stats,
            stages: *stages,
            source_nomination_skipped: false,
        })
    }

    fn close_open_stages(stages: &mut QueryStages, started: std::time::Instant) {
        for stage in [
            &mut stages.discovery,
            &mut stages.source,
            &mut stages.planning,
            &mut stages.sq8,
        ] {
            if stage.start_ns > 0 && stage.end_ns == 0 {
                stage.end_ns = started.elapsed().as_nanos().max(stage.start_ns);
            }
        }
    }

    /// Exact modeled memory of opt-in direct-closure search under `limits`. This
    /// admits nothing: search repeats the `total_bytes <= cap_bytes` check before
    /// any allocation, slot or I/O.
    pub fn direct_closure_memory(
        &self,
        limits: DirectClosureLimits,
    ) -> Result<DirectClosureMemory> {
        let bad = TwoBitGenerationError::Invalid;
        if limits.max_sq8_bytes == 0 || limits.max_sq8_gets == 0 {
            return Err(bad("direct closure limits"));
        }
        let rows = self.rows();
        let dimensions = self.pages.dimensions();
        let planner = (1024 * 1024_u64)
            .checked_add(TwoBitPlanTrace::scratch_bytes(rows) as u64)
            .and_then(|n| n.checked_add((dimensions as u64).checked_mul(4)?))
            .and_then(|n| n.checked_add((rows.div_ceil(256) as u64).checked_mul(512)?))
            .ok_or(bad("direct closure planner memory"))?;
        let direct = crate::returned_sq8::query_payload_bytes(
            crate::exact_sq8_nominee::Sq8Geometry { rows, dimensions },
            limits.max_sq8_bytes as u64,
            planner,
            self.limits.max_active_queries as u64,
        )
        .map_err(|_| bad("direct closure query memory"))?;
        Ok(DirectClosureMemory {
            resident_bytes: self.modeled_memory_bytes,
            direct_planner_bytes: planner,
            direct_query_bytes: direct,
            total_bytes: self
                .modeled_memory_bytes
                .checked_add(direct)
                .ok_or(bad("direct closure memory"))?,
            cap_bytes: self.limits.max_memory_bytes,
        })
    }

    // Allocation-free admission: no query/plan/payload allocation, no slot and no I/O
    // precede or occur in it. Input validity comes first and resource classification
    // last, so a malformed query never masquerades as a resource rejection when memory
    // is also short. The borrowed query width is bound to the authenticated generation
    // and its norm is validated here because direct search skips codec preparation.
    fn admit_direct_closure(
        &self,
        query: &[f32],
        top_k: usize,
        excluded_ids: Option<&[i64]>,
        limits: DirectClosureLimits,
        traced: bool,
        range_trace: Option<&Sq8RangeTrace>,
    ) -> Result<()> {
        let bad = TwoBitGenerationError::Invalid;
        if query.len() != self.pages.dimensions() {
            return Err(bad("search admission"));
        }
        // The validity `cosine_vector` enforces (finite, positive f64 squared norm),
        // scanned over the borrowed query; the normalized copy is allocated only later.
        let squared = query.iter().map(|&x| f64::from(x).powi(2)).sum::<f64>();
        if !squared.is_finite() || squared <= 0. {
            return Err(TwoBitGenerationError::Plane(SourceBuildError::Invalid(
                "cosine vector norm",
            )));
        }
        if self.discovery_mode() != DiscoveryMode::Semantic {
            return Err(bad("direct closure requires semantic discovery"));
        }
        if top_k == 0
            || top_k > self.rows()
            || excluded_ids.is_some_and(|ids| {
                ids.windows(2).any(|v| v[0] >= v[1])
                    || ids
                        .len()
                        .checked_mul(8)
                        .is_none_or(|n| n as u64 > self.limits.already_pinned_bytes)
            })
        {
            return Err(bad("search admission"));
        }
        if limits.max_sq8_bytes == 0 || limits.max_sq8_gets == 0 {
            return Err(bad("direct closure limits"));
        }
        if traced
            && self.limits.max_query_scratch_bytes < TwoBitPlanTrace::scratch_bytes(self.rows())
        {
            return Err(bad("diagnostic scratch"));
        }
        // Resource classification: checked modeled-memory arithmetic, then the cap.
        let memory = self.direct_closure_memory(limits)?;
        if memory.total_bytes > memory.cap_bytes {
            return Err(bad(DIRECT_MEMORY_CAP));
        }
        // Opt-in range diagnostics are a separate, non-resource charge on top of the
        // unchanged direct model: refused here, before the slot or any request.
        if let Some(range_trace) = range_trace {
            self.admit_range_trace(range_trace, limits.max_sq8_gets, memory.total_bytes)?;
        }
        Ok(())
    }

    // The required closure alone defines the population. Bridge pages exist only
    // inside `ranges`; they are never enumerated or stored.
    fn direct_closure_plan(
        &self,
        walks: &[(usize, Vec<usize>)],
        limits: DirectClosureLimits,
    ) -> Result<BudgetedPagePlan> {
        let closure = admit_source_walks(self.rows(), walks, self.semantic_profile())?
            .into_iter()
            .map(|unit| unit / 8)
            .collect::<BTreeSet<_>>();
        let row_bytes = self
            .pages
            .dimensions()
            .checked_add(12)
            .ok_or(TwoBitGenerationError::Invalid("direct closure geometry"))?;
        // The existing pure checked cover and byte admission; its only budget refusal is
        // the byte cap, which is the typed resource outcome here.
        let (ranges, planned_bytes) = plan_two_bit_source_cover(
            &closure,
            self.rows(),
            row_bytes,
            limits.max_sq8_gets,
            limits.max_sq8_bytes,
        )
        .map_err(|error| match error {
            TwoBitGenerationError::Budget(BudgetedPageError::InsufficientBudget) => {
                TwoBitGenerationError::Invalid(DIRECT_BYTE_CAP)
            }
            other => other,
        })?;
        Ok(BudgetedPagePlan {
            target_pages: closure.len(),
            target_shortfall: 0,
            primary_pages_retained: 0,
            selected_pages: closure.into_iter().collect(),
            ranges,
            planned_bytes,
        })
    }

    // The shared authenticated scorer under the caller's explicit direct caps. The
    // historical (clamped) `max_query_*` limits are deliberately not consulted.
    #[allow(clippy::too_many_arguments)]
    async fn direct_closure_rank(
        &self,
        store: &dyn ObjectStore,
        normalized: &[f32],
        plan: &BudgetedPagePlan,
        top_k: usize,
        excluded_ids: Option<&[i64]>,
        limits: DirectClosureLimits,
        range_trace: Option<&mut Sq8RangeTrace>,
    ) -> Result<RankedSq8> {
        let page_bytes = 256 * (self.pages.dimensions() + 12);
        let ranges = plan
            .ranges
            .iter()
            .map(|r| (r.start / page_bytes, (r.end - 1) / page_bytes))
            .collect::<Vec<_>>();
        let ranked = rank_verified_sq8_pages_traced(
            store,
            &ObjectPath::from(self.manifest.sq8_object_key.clone()),
            &self.pages,
            &ranges,
            &self.manifest.sq8_etag,
            normalized,
            &self.manifest.low,
            &self.manifest.step,
            top_k,
            limits.max_sq8_gets,
            limits.max_sq8_bytes,
            self.limits.max_parallel_gets,
            excluded_ids.unwrap_or(&[]),
            range_trace,
        )
        .await
        .map_err(|sq8| TwoBitGenerationError::charged_read(Sq8ReadStats::default(), sq8))?;
        if excluded_ids.is_none() && ranked.candidates.len() < top_k {
            return Err(TwoBitGenerationError::charged_read(
                Sq8ReadStats::default(),
                RankedSq8Failure {
                    error: crate::sq8_s3_range::RangeFetchError::Score(
                        crate::exact_sq8_nominee::Sq8ScoreError::InvalidRoster,
                    ),
                    stats: ranked.stats,
                },
            ));
        }
        Ok(ranked)
    }

    #[allow(clippy::too_many_arguments)]
    async fn direct_closure_measured(
        &self,
        store: &dyn ObjectStore,
        query: &[f32],
        top_k: usize,
        excluded_ids: Option<&[i64]>,
        limits: DirectClosureLimits,
        trace: Option<&mut TwoBitPlanTrace>,
        stages: &mut QueryStages,
        started: std::time::Instant,
        range_trace: Option<&mut Sq8RangeTrace>,
    ) -> Result<TwoBitSearchResult> {
        stages.discovery.start_ns = started.elapsed().as_nanos().max(1);
        let discovered = normalize_two_bit_diagnostic_query(query).and_then(|normalized| {
            let walks = self.local_walks(normalized.as_ref(), trace)?;
            Ok((normalized, walks))
        });
        stages.discovery.end_ns = started.elapsed().as_nanos();
        let (normalized, walks) = discovered?;
        stages.planning.start_ns = started.elapsed().as_nanos().max(1);
        let plan = self.direct_closure_plan(&walks, limits);
        stages.planning.end_ns = started.elapsed().as_nanos();
        let plan = plan?;
        stages.sq8.start_ns = started.elapsed().as_nanos().max(1);
        let ranked = self
            .direct_closure_rank(
                store,
                normalized.as_ref(),
                &plan,
                top_k,
                excluded_ids,
                limits,
                range_trace,
            )
            .await;
        stages.sq8.end_ns = started.elapsed().as_nanos();
        Ok(TwoBitSearchResult {
            plan,
            ranked: ranked?,
            source_stats: Sq8ReadStats::default(),
            router_stats: Sq8ReadStats::default(),
            stages: *stages,
            source_nomination_skipped: true,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn direct_closure_search_inner(
        &self,
        store: &dyn ObjectStore,
        query: &[f32],
        top_k: usize,
        excluded_ids: Option<&[i64]>,
        limits: DirectClosureLimits,
        trace: Option<&mut TwoBitPlanTrace>,
        mut range_trace: Option<&mut Sq8RangeTrace>,
    ) -> Result<TwoBitSearchResult> {
        self.admit_direct_closure(
            query,
            top_k,
            excluded_ids,
            limits,
            trace.is_some(),
            range_trace.as_deref(),
        )?;
        // The one query slot spans discovery, cover, every drained GET and scoring,
        // on success and on every failure, exactly like the historical search.
        let _permit = self
            .slots
            .acquire()
            .await
            .map_err(|_| TwoBitGenerationError::Invalid("query admission"))?;
        let started = std::time::Instant::now();
        if let Some(range_trace) = range_trace.as_deref_mut() {
            range_trace.begin(started);
        }
        let mut stages = QueryStages::default();
        let result = self
            .direct_closure_measured(
                store,
                query,
                top_k,
                excluded_ids,
                limits,
                trace,
                &mut stages,
                started,
                range_trace,
            )
            .await;
        Self::close_open_stages(&mut stages, started);
        match result {
            Ok(mut result) => {
                result.stages = stages;
                Ok(result)
            }
            Err(error) => Err(TwoBitGenerationError::Query {
                stages,
                error: Box::new(error),
            }),
        }
    }

    /// Opt-in direct-closure search: current local semantic discovery and its checked
    /// 256-row page closure feed a deterministic smallest-gap cover, fetched in one
    /// authenticated wave and ranked by the shared SQ8 scorer. No SOURCE I/O or
    /// nomination occurs (`source_stats` and `router_stats` are zero). The cover can
    /// bridge different gap pages than any other closure, so its population is not
    /// a superset of another strategy's. `plan.selected_pages` is the required
    /// closure; `plan.ranges` are the bytes actually fetched, bridges included.
    /// `limits` are explicit and independent of the generation's `max_query_*`.
    /// Admission, including `direct_closure_memory`, precedes any allocation or
    /// I/O. Base rows only: mutation snapshots are not supported by this entry
    /// point, and ordinary `search*` calls never route here.
    pub async fn direct_closure_search_with_store(
        &self,
        store: &dyn ObjectStore,
        query: &[f32],
        top_k: usize,
        excluded_ids: Option<&[i64]>,
        limits: DirectClosureLimits,
    ) -> Result<TwoBitSearchResult> {
        self.direct_closure_search_inner(store, query, top_k, excluded_ids, limits, None, None)
            .await
    }

    /// Direct-closure search with a charged trace: `semantic_*` fields are the
    /// production discovery, the result's `source_nomination_skipped` is set, and the
    /// nomination fields are unset rather than fabricated.
    #[doc(hidden)]
    pub async fn diagnostic_direct_closure_search_with_store(
        &self,
        store: &dyn ObjectStore,
        query: &[f32],
        top_k: usize,
        limits: DirectClosureLimits,
    ) -> Result<(TwoBitSearchResult, TwoBitPlanTrace)> {
        let mut trace = TwoBitPlanTrace::default();
        let result = self
            .direct_closure_search_inner(store, query, top_k, None, limits, Some(&mut trace), None)
            .await?;
        Ok((result, trace))
    }

    /// `diagnostic_direct_closure_search_with_store` plus opt-in SQ8 range diagnostics in
    /// the caller's trace. The trace's modeled memory is checked right after the unchanged
    /// direct memory classification, and its capacity must cover `limits.max_sq8_gets`;
    /// both refuse (non-resource) before the slot or any request.
    #[doc(hidden)]
    pub async fn diagnostic_direct_closure_search_with_store_traced(
        &self,
        store: &dyn ObjectStore,
        query: &[f32],
        top_k: usize,
        limits: DirectClosureLimits,
        range_trace: &mut Sq8RangeTrace,
    ) -> Result<(TwoBitSearchResult, TwoBitPlanTrace)> {
        // Before every early return inside admission, for the same reason as the baseline entry.
        range_trace.refuse();
        let mut trace = TwoBitPlanTrace::default();
        let result = self
            .direct_closure_search_inner(
                store,
                query,
                top_k,
                None,
                limits,
                Some(&mut trace),
                Some(range_trace),
            )
            .await?;
        Ok((result, trace))
    }
}

#[cfg(test)]
mod source_walk_tests {
    use super::*;
    use futures_util::{StreamExt, stream};

    fn assert_metadata_waves(startup: &RemoteOpenStats, names: &[&str]) {
        assert_eq!(
            startup
                .metadata
                .iter()
                .map(|r| r.name.as_str())
                .collect::<Vec<_>>(),
            names
        );
        let mut wall = startup.metadata[0].metadata_wave_wall_ns;
        assert_eq!(startup.metadata[0].metadata_wave, 0);
        for (i, batch) in startup.metadata[1..].chunks(8).enumerate() {
            wall += batch[0].metadata_wave_wall_ns;
            assert!(batch.iter().all(|r| r.metadata_wave == i as u64 + 1
                && r.metadata_wave_wall_ns == batch[0].metadata_wave_wall_ns
                && r.head_wall_ns + r.get_wall_ns + r.stream_wall_ns <= r.metadata_wave_wall_ns));
            assert!(
                batch
                    .iter()
                    .map(|r| r.payload_buffer_bound_bytes)
                    .sum::<u64>()
                    <= 32 * 1024 * 1024
            );
        }
        assert!(wall <= startup.staging_wall_ns);
    }

    fn assert_reused_root(generation: &TwoBitGeneration, bytes: u64) {
        let startup = generation.remote_open_stats().unwrap();
        let root = &startup.metadata[0];
        assert_eq!(root.name, "manifest.json");
        assert_eq!(root.reused_root_bytes, bytes);
        assert_eq!(root.retained_root_bytes, bytes);
        assert_eq!(root.logical_head_requests, 0);
        assert_eq!(root.logical_get_requests, 0);
        assert_eq!(root.bytes, 0);
        assert_eq!(root.chunks, 0);
        assert_eq!(root.payload_buffer_bound_bytes, 0);
        assert_eq!(
            root.head_wall_ns + root.get_wall_ns + root.stream_wall_ns + root.write_wall_ns,
            0
        );
        assert!(root.local_auth_wall_ns > 0 && root.local_copy_wall_ns > 0);
        assert!(root.local_auth_wall_ns + root.local_copy_wall_ns <= root.metadata_wave_wall_ns);
        assert!(
            startup.metadata[1..]
                .iter()
                .all(|row| row.reused_root_bytes == 0
                    && row.retained_root_bytes == 0
                    && row.local_auth_wall_ns == 0
                    && row.local_copy_wall_ns == 0)
        );
    }

    #[derive(Debug, Default)]
    struct RecordedStore {
        inner: object_store::memory::InMemory,
        reads: std::sync::Mutex<Vec<(String, bool, std::ops::Range<u64>, Option<String>)>>,
        writes: std::sync::Mutex<Vec<String>>,
        fail_head: std::sync::atomic::AtomicUsize,
        metadata_fault: std::sync::Mutex<Option<&'static str>>,
        root_read_budget: std::sync::Mutex<Option<(String, usize)>>,
        bad_etag_suffix: std::sync::Mutex<Option<&'static str>>,
        source_fault: std::sync::Mutex<Option<(usize, &'static str)>>,
        source_completed: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        leaf_accesses: std::sync::atomic::AtomicUsize,
    }
    impl std::fmt::Display for RecordedStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "recorded-memory")
        }
    }
    #[async_trait::async_trait]
    impl ObjectStore for RecordedStore {
        async fn get_opts(
            &self,
            path: &ObjectPath,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            if path.as_ref().ends_with("router/leaves.bin") {
                self.leaf_accesses
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            {
                let mut guard = self.root_read_budget.lock().unwrap();
                if let Some((root, remaining)) = guard.as_mut()
                    && root.as_str() == path.as_ref()
                {
                    if options.head || *remaining == 0 {
                        return Err(object_store::Error::Generic {
                            store: "recorded",
                            source: std::io::Error::other("redundant root HEAD/GET").into(),
                        });
                    }
                    *remaining -= 1;
                }
            }
            let head = options.head;
            let etag = options.if_match.clone();
            let mut result = self.inner.get_opts(path, options).await?;
            if head
                && self
                    .bad_etag_suffix
                    .lock()
                    .unwrap()
                    .is_some_and(|suffix| path.as_ref().ends_with(suffix))
            {
                result.meta.e_tag = Some("W/weak".into());
            }
            self.reads
                .lock()
                .unwrap()
                .push((path.to_string(), head, result.range.clone(), etag));
            let fault = *self.metadata_fault.lock().unwrap();
            if !head && path.as_ref().ends_with("/plane/mean.bin") {
                match fault {
                    Some("size") => result.meta.size += 1,
                    Some("range") => result.range.start += 1,
                    Some(fault @ ("short" | "long" | "corrupt")) => {
                        let original = result.payload;
                        result.payload = object_store::GetResultPayload::Stream(
                            stream::once(async move {
                                let object_store::GetResultPayload::Stream(mut body) = original
                                else {
                                    unreachable!()
                                };
                                let mut bytes = body.next().await.unwrap()?.to_vec();
                                match fault {
                                    "short" => {
                                        bytes.pop();
                                    }
                                    "long" => bytes.push(0),
                                    _ => bytes[0] ^= 1,
                                }
                                Ok(bytes::Bytes::from(bytes))
                            })
                            .boxed(),
                        );
                    }
                    _ => {}
                }
            }
            let source_fault = *self.source_fault.lock().unwrap();
            if !head
                && path.as_ref().ends_with("/plane/records.bin")
                && let Some((start, fault)) = source_fault
            {
                let affected = result.range.start == start as u64;
                if affected && fault == "etag" {
                    result.meta.e_tag = Some("wrong-source-etag".into());
                } else {
                    let original = result.payload;
                    let completed = self.source_completed.clone();
                    result.payload = object_store::GetResultPayload::Stream(
                        stream::once(async move {
                            let object_store::GetResultPayload::Stream(mut body) = original else {
                                unreachable!()
                            };
                            let mut bytes = Vec::new();
                            while let Some(chunk) = body.next().await {
                                bytes.extend_from_slice(&chunk?);
                            }
                            if fault == "delayed" {
                                // The later admitted success must finish after the first error.
                                tokio::time::sleep(std::time::Duration::from_millis(if affected {
                                    5
                                } else {
                                    20
                                }))
                                .await;
                            }
                            completed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            if affected && fault == "delayed" {
                                return Err(object_store::Error::Generic {
                                    store: "recorded",
                                    source: std::io::Error::other("delayed source miss").into(),
                                });
                            }
                            if affected && fault == "corrupt" {
                                bytes[0] ^= 1;
                            }
                            Ok(bytes::Bytes::from(bytes))
                        })
                        .boxed(),
                    );
                }
            }
            tokio::task::yield_now().await;
            Ok(result)
        }
        async fn put_opts(
            &self,
            path: &ObjectPath,
            body: object_store::PutPayload,
            options: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            if path.as_ref().ends_with("/head.json")
                && self
                    .fail_head
                    .fetch_update(
                        std::sync::atomic::Ordering::SeqCst,
                        std::sync::atomic::Ordering::SeqCst,
                        |n| n.checked_sub(1),
                    )
                    .ok()
                    == Some(1)
            {
                return Err(object_store::Error::Generic {
                    store: "recorded",
                    source: std::io::Error::other("HEAD-last failure").into(),
                });
            }
            self.writes.lock().unwrap().push(path.to_string());
            self.inner.put_opts(path, body, options).await
        }
        async fn put_multipart_opts(
            &self,
            path: &ObjectPath,
            options: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.writes.lock().unwrap().push(path.to_string());
            self.inner.put_multipart_opts(path, options).await
        }
        fn delete_stream(
            &self,
            paths: futures_util::stream::BoxStream<'static, object_store::Result<ObjectPath>>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<ObjectPath>> {
            self.inner.delete_stream(paths)
        }
        fn list(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &ObjectPath,
            to: &ObjectPath,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    #[tokio::test]
    async fn semantic_d768_diagnostic_scratch_preserves_production_search() {
        use crate::two_bit_build::TwoBitGenerationBuilder;
        use crate::two_bit_source::TwoBitSource;
        use sha2::{Digest, Sha256};
        let hash = |body: &[u8]| format!("{:x}", Sha256::digest(body));
        let rows = 257;
        let dimensions = 768;
        let temp = tempfile::tempdir().unwrap();
        let mut raw = Vec::new();
        let mut sq8 = Vec::new();
        for row in 0..rows {
            let value = (1 + row % 7) as u8;
            for _ in 0..dimensions {
                raw.extend_from_slice(&(value as f32).to_le_bytes());
            }
            sq8.extend_from_slice(&((rows - row + 1000) as i64).to_le_bytes());
            sq8.extend_from_slice(&(dimensions as f32 * (value as f32).powi(2)).to_le_bytes());
            sq8.extend(std::iter::repeat_n(value, dimensions));
        }
        let raw_path = temp.path().join("raw");
        let sq8_path = temp.path().join("sq8");
        fs::write(&raw_path, &raw).unwrap();
        fs::write(&sq8_path, &sq8).unwrap();
        let store = RecordedStore::default();
        let sq8_sha = hash(&sq8);
        let key = ObjectPath::from(format!("semantic/objects/{sq8_sha}"));
        store.put(&key, sq8.into()).await.unwrap();
        let etag = store.head(&key).await.unwrap().e_tag.unwrap();
        let root = temp.path().join("generation");
        let order = (0..rows as u64).collect::<Vec<_>>();
        let root_sha = TwoBitGenerationBuilder {
            source: TwoBitSource {
                raw: &raw_path,
                raw_sha256: &hash(&raw),
                sq8: &sq8_path,
                sq8_sha256: &sq8_sha,
                rows,
                dimensions,
            },
            base_epoch: 0,
            generation: 1,
            low: &[0.; 768],
            step: &[1.; 768],
            sq8_object_key: key.as_ref(),
            sq8_etag: &etag,
        }
        .build_with_discovery(Some(&order), DiscoveryMode::Semantic, &root, 128_000_000)
        .unwrap();
        let mut limits = TwoBitGenerationLimits {
            max_memory_bytes: 512 * 1024 * 1024,
            max_active_queries: 1,
            max_query_bytes: 16_773_120,
            max_query_gets: 32,
            max_parallel_gets: 16,
            max_source_bytes: 64 * 1024 * 1024,
            max_source_gets: 128,
            max_parallel_source_gets: 16,
            max_query_scratch_bytes: 400_000,
            already_pinned_bytes: 32 * 1024 * 1024,
        };
        let head = crate::two_bit_store::publish_two_bit_generation(
            &store,
            &ObjectPath::from("semantic/index"),
            &root,
            &root_sha,
            limits,
            None,
        )
        .await
        .unwrap();
        let production = TwoBitGeneration::open_remote(
            &store,
            &head.metadata_prefix(),
            &root_sha,
            limits,
            temp.path(),
        )
        .await
        .unwrap();
        let query = [1.; 768];
        store.reads.lock().unwrap().clear();
        let expected = production
            .search_with_store(&store, &query, 100, None)
            .await
            .unwrap();
        let expected_reads = store.reads.lock().unwrap().clone();
        assert_eq!(expected.ranked.candidates.len(), 100);
        store.reads.lock().unwrap().clear();
        assert!(
            production
                .diagnostic_search_with_store(&store, &query, 100)
                .await
                .is_err()
        );
        assert!(store.reads.lock().unwrap().is_empty());
        let trace_bytes = TwoBitPlanTrace::scratch_bytes(rows);
        limits.max_query_scratch_bytes = 400_000_usize.checked_add(trace_bytes).unwrap();
        let mut diagnostic = TwoBitGeneration::open_remote(
            &store,
            &head.metadata_prefix(),
            &root_sha,
            limits,
            temp.path(),
        )
        .await
        .unwrap();
        assert_eq!(
            diagnostic.modeled_memory_bytes - production.modeled_memory_bytes,
            trace_bytes as u64
        );
        store.reads.lock().unwrap().clear();
        let (actual, trace) = diagnostic
            .diagnostic_search_with_store(&store, &query, 100)
            .await
            .unwrap();
        assert_eq!(actual.ranked.candidates, expected.ranked.candidates);
        assert_eq!(actual.plan, expected.plan);
        assert_eq!(actual.ranked.stats, expected.ranked.stats);
        assert_eq!(actual.source_stats, expected.source_stats);
        assert_eq!(actual.router_stats, expected.router_stats);
        assert_eq!(*store.reads.lock().unwrap(), expected_reads);
        let retained = std::mem::size_of::<TwoBitPlanTrace>()
            + [
                trace.ranked_candidate_pages.capacity(),
                trace.nomination_evaluated_units.capacity(),
                trace.semantic_leaves.capacity(),
                trace.semantic_units.capacity(),
                trace.semantic_seed_additions.capacity(),
            ]
            .into_iter()
            .sum::<usize>()
                * std::mem::size_of::<usize>();
        assert!(trace.discoveries.is_empty());
        assert!(retained <= trace_bytes);
        // D768 preparation needs 399,360 bytes, in addition to the retained trace.
        diagnostic.limits.max_query_scratch_bytes = trace_bytes + 399_359;
        store.reads.lock().unwrap().clear();
        assert!(
            diagnostic
                .diagnostic_search_with_store(&store, &query, 100)
                .await
                .is_err()
        );
        assert!(store.reads.lock().unwrap().is_empty());
        diagnostic.limits.max_query_scratch_bytes += 1;
        diagnostic
            .diagnostic_search_with_store(&store, &query, 100)
            .await
            .unwrap();
    }

    fn native_full_leaf_fixture(
        profile: SemanticProfile,
        dimensions: usize,
    ) -> (
        SemanticUnitRouter,
        Vec<usize>,
        Vec<Vec<u8>>,
        crate::semantic_unit_router::RouterArtifacts,
    ) {
        use crate::semantic_unit_router::{SourceIdentity, build};
        use sha2::{Digest, Sha256};

        let hash = |body: &[u8]| format!("{:x}", Sha256::digest(body));
        let rows = match profile {
            SemanticProfile::Native100k => 32_768_usize,
            SemanticProfile::Fresh1m => 1_000_000,
        };
        // Encode unit means directly; no full canonical vector fixture is needed.
        let mut blob = b"BORSUCP1".to_vec();
        blob.extend_from_slice(&(rows as u64).to_le_bytes());
        for word in [dimensions as u32, 32, 256, 0] {
            blob.extend_from_slice(&word.to_le_bytes());
        }
        blob.resize(32 + rows.div_ceil(32) * dimensions * 2, 0);
        let centroid_sha = hash(&blob);
        let input = SourceIdentity {
            profile,
            schema: "native-full-leaf-test",
            root_sha256: &"1".repeat(64),
            centroids_sha256: &centroid_sha,
            rows,
            dimensions,
        };
        let artifacts = build(&blob, &input, profile.allocation_cap()).unwrap();
        let router = SemanticUnitRouter::open(
            &artifacts.manifest,
            &artifacts.membership,
            &hash(&artifacts.manifest),
            &input,
            profile.root_cap(),
        )
        .unwrap();
        assert_eq!(
            router.manifest().leaves.len(),
            rows.div_ceil(32).div_ceil(64)
        );
        assert!(
            router
                .manifest()
                .leaves
                .iter()
                .all(|leaf| leaf.unit_count == (rows.div_ceil(32) - leaf.leaf_id * 64).min(64))
        );
        // Identical means tie all leaves: 16 Native100k or exactly 48 Fresh1m.
        let ids = router.nominate(&vec![1.; dimensions]).unwrap();
        assert_eq!(ids, (0..profile.selected_leaf_limit()).collect::<Vec<_>>());
        let bodies = ids
            .iter()
            .map(|&id| {
                let leaf = &router.manifest().leaves[id];
                artifacts.leaves[leaf.offset..leaf.offset + leaf.bytes].to_vec()
            })
            .collect::<Vec<_>>();
        (router, ids, bodies, artifacts)
    }

    #[test]
    fn native_full_sixteen_leaf_router_validation_d1024_and_d768() {
        for (dimensions, expected_bytes) in [(1024, 2_101_248), (768, 1_576_960)] {
            let (router, ids, bodies, _) =
                native_full_leaf_fixture(SemanticProfile::Native100k, dimensions);
            assert_eq!(bodies.iter().map(Vec::len).sum::<usize>(), expected_bytes);
            let parts = bodies.iter().map(Vec::as_slice).collect::<Vec<_>>();
            let nomination = router.validate_selected(&ids, &parts).unwrap();
            assert_eq!(nomination.units, (0..1024).collect());
            assert_eq!(nomination.page_closure, (0..128).collect());
            assert!(nomination.seed_additions.is_empty());
            if dimensions == 1024 {
                let mut excess = bodies[15].clone();
                excess.push(0);
                let mut overbound = parts;
                overbound[15] = &excess;
                let error = router.validate_selected(&ids, &overbound).err().unwrap();
                assert!(error.to_string().contains("selected leaf byte cap"));
            }
        }
    }

    #[test]
    fn native_full_sixteen_leaf_generation_admission_d1024_and_d768() {
        for dimensions in [1024, 768] {
            let (router, ids, _, _) =
                native_full_leaf_fixture(SemanticProfile::Native100k, dimensions);
            assert_eq!(
                TwoBitGeneration::semantic_walks(&router, &ids, None).unwrap(),
                vec![(0, (0..1024).collect::<Vec<_>>())]
            );
            // Descriptor admission happens before local or remote payload access.
            for invalid in [vec![], vec![0, 0], vec![0; 17], vec![16]] {
                assert!(TwoBitGeneration::semantic_walks(&router, &invalid, None).is_err());
            }
        }
    }

    #[tokio::test]
    async fn native_fresh1m_membership_nomination_parity_and_bounds() {
        // Full Fresh1m metadata, with no million-row source/SQ8 allocation.
        let (router, ids, bodies, artifacts) =
            native_full_leaf_fixture(SemanticProfile::Fresh1m, 768);
        let parts = bodies.iter().map(Vec::as_slice).collect::<Vec<_>>();
        let expected = router.validate_selected(&ids, &parts).unwrap();
        assert_eq!(expected.units.len(), 3072);
        assert_eq!(expected.page_closure.len(), 384);
        assert_eq!(router.nominate_selected(&ids).unwrap(), expected);
        let mut trace = TwoBitPlanTrace::default();
        assert_eq!(
            TwoBitGeneration::semantic_walks(&router, &ids, Some(&mut trace)).unwrap(),
            vec![(expected.seed_page, expected.walk_units.clone())],
        );
        assert_eq!(trace.semantic_leaves, ids);
        assert_eq!(
            trace.semantic_units,
            expected.units.into_iter().collect::<Vec<_>>()
        );
        assert_eq!(trace.semantic_seed_additions, expected.seed_additions);
        for invalid in [
            vec![],
            vec![0; 48],
            (0..47).collect(),
            (0..49).collect(),
            (router.manifest().leaves.len()..router.manifest().leaves.len() + 48).collect(),
        ] {
            assert!(router.nominate_selected(&invalid).is_err());
        }
        let units = router.manifest().unit_count;
        let leaves = router.manifest().leaves.len();
        let (resident, peak) =
            crate::semantic_unit_router::directory_allocation_bytes(units, leaves).unwrap();
        assert_eq!(resident, 4 * units + 8 * (leaves + 1));
        assert_eq!(peak, resident + 16 * leaves);
        // Leaf bodies remain a publication authority, even though discovery no
        // longer launches read futures or depends on their availability.
        let mut damaged = artifacts.leaves;
        damaged[4] ^= 1;
        assert!(
            router
                .validate_leaf(0, &damaged[..bodies[0].len()])
                .is_err()
        );
        assert_eq!(
            router.nominate_selected(&ids).unwrap().walk_units,
            expected.walk_units
        );
    }

    #[tokio::test]
    async fn native_cold_fetch_parallelism_parity_and_drain() {
        use crate::sq8_s3_range::{
            RangeFetchError,
            cold_http_fixture::{ETAG, Fixture, Request},
        };
        use sha2::{Digest, Sha256};
        use std::{
            collections::BTreeMap,
            time::{Duration, Instant},
        };

        let deadline = Instant::now() + Duration::from_secs(90);
        tokio::time::timeout(Duration::from_secs(90), async {
            let hash = |body: &[u8]| format!("{:x}", Sha256::digest(body));
            let rows = 65_536;
            let dimensions = 32;
            let temp = tempfile::tempdir().unwrap();
            // 32 exact orthogonal directions, each occupying eight nonadjacent
            // physical pages. The transpose gives the trainer 32 distinct
            // initial seeds (units 0,64,...), while bit 2 alternates selected
            // positive and unselected negative directions within each block.
            let mut raw = Vec::with_capacity(rows * dimensions * 4);
            let mut sq8 = Vec::with_capacity(rows * (dimensions + 12));
            for row in 0..rows {
                let page = row / 256;
                let class = (page / 8 + 4 * (page % 8)) % 32;
                sq8.extend_from_slice(&((rows - row) as i64).to_le_bytes());
                sq8.extend_from_slice(&1_f32.to_le_bytes());
                for d in 0..dimensions {
                    let value = if d != class { 0_f32 } else if class & 4 == 0 { 1. } else { -1. };
                    raw.extend_from_slice(&value.to_le_bytes());
                    sq8.push((value + 1.) as u8);
                }
            }
            let raw_path = temp.path().join("raw");
            let sq8_path = temp.path().join("sq8");
            fs::write(&raw_path, &raw).unwrap();
            fs::write(&sq8_path, &sq8).unwrap();
            let sq8_sha = hash(&sq8);
            let key = ObjectPath::from(format!("cold/objects/{sq8_sha}"));
            let root = temp.path().join("generation");
            // SQ8 application IDs descend, but its vectors follow raw-row order.
            let order = (0..rows as u64).collect::<Vec<_>>();
            let root_sha = crate::two_bit_build::TwoBitGenerationBuilder {
                source: crate::two_bit_source::TwoBitSource {
                    raw: &raw_path, raw_sha256: &hash(&raw),
                    sq8: &sq8_path, sq8_sha256: &sq8_sha, rows, dimensions,
                },
                base_epoch: 0, generation: 1, low: &[-1.; 32], step: &[1.; 32],
                sq8_object_key: key.as_ref(), sq8_etag: ETAG,
            }
            .build_with_discovery(Some(&order), DiscoveryMode::Semantic, &root, 128 * 1024 * 1024)
            .unwrap();
            drop(order);
            drop(raw);
            let limits = TwoBitGenerationLimits {
                max_memory_bytes: 128 * 1024 * 1024, max_active_queries: 1,
                max_query_bytes: 32 * 256 * (dimensions + 12), max_query_gets: 32,
                max_parallel_gets: 16, max_source_bytes: 64 * 1024 * 1024,
                max_source_gets: 128, max_parallel_source_gets: 16,
                max_query_scratch_bytes: 1_048_576 + TwoBitPlanTrace::scratch_bytes(rows),
                already_pinned_bytes: 0,
            };
            let query = (0..dimensions).map(|d| if d & 4 == 0 { 0.25 } else { 0. }).collect::<Vec<_>>();
            let local = TwoBitGeneration::open(&root, &root_sha, limits).unwrap();
            let (plan, trace) = local.diagnostic_plan(&query).await.unwrap();
            assert_eq!(local.semantic_profile(), Some(SemanticProfile::Native100k));
            assert_eq!(trace.semantic_leaves.len(), 16);
            let width = local.plane.receipt().record_bytes;
            let closure = trace.semantic_units.iter().map(|unit| unit / 8).collect::<BTreeSet<_>>();
            let (source_cover, source_bytes) = plan_two_bit_source_cover(
                &closure, rows, width, limits.max_source_gets, limits.max_source_bytes,
            ).unwrap();
            // Prerequisites come from the authenticated router/planner, never
            // manually replaced walks, limits, authorities or nominated IDs.
            assert!(source_cover.len() > 32 && source_cover.len() <= 128);
            assert!((17..=32).contains(&plan.ranges.len()));
            assert!(source_cover.windows(2).all(|w| w[0].end < w[1].start));
            assert!(plan.ranges.windows(2).all(|w| w[0].end < w[1].start));
            let source_ranges = source_cover.iter().map(|r| (r.start / (32 * width), (r.end - 1) / (32 * width))).collect::<Vec<_>>();
            let sq8_ranges = plan.ranges.iter().map(|r| (r.start / (256 * (dimensions + 12)), (r.end - 1) / (256 * (dimensions + 12)))).collect::<Vec<_>>();
            let prefix = ObjectPath::from(format!("cold/generations/{root_sha}"));
            let source_key = metadata_location(&prefix, "plane/records.bin");
            let leaf_key = metadata_location(&prefix, "router/leaves.bin");
            let request = |key: &ObjectPath, start, end| -> Request {
                (key.to_string(), false, Some((start, end)), Some(ETAG.into()))
            };
            let mut expected_requests = source_cover.iter().map(|r| request(&source_key, r.start, r.end)).collect::<Vec<_>>();
            expected_requests.extend(plan.ranges.iter().map(|r| request(&key, r.start, r.end)));
            expected_requests.sort();
            let expected_trace = serde_json::to_value(&trace).unwrap();
            drop(local);
            let mut objects = BTreeMap::from([(key.to_string(), sq8)]);
            for name in ["manifest.json", "page_manifest.json", "page_digests.bin", "plane/manifest.json", "plane/mean.bin", "plane/page_digests.bin", "plane/records.bin", "router/root.bin", "router/membership.bin", "router/leaves.bin"] {
                objects.insert(metadata_location(&prefix, name).to_string(), fs::read(root.join(name)).unwrap());
            }
            assert!(Instant::now() < deadline, "fixture construction deadline");
            let fixture = Fixture::new(objects, deadline);
            let mut control_bits = None;
            let mut control_memory = None;
            for parallelism in [16, 32] {
                let limits = TwoBitGenerationLimits {
                    max_parallel_gets: parallelism, max_parallel_source_gets: parallelism, ..limits
                };
                let generation = TwoBitGeneration::open_remote(
                    fixture.reader.store(), &prefix, &root_sha, limits, temp.path(),
                ).await.unwrap();
                assert_eq!(generation.limits.max_parallel_gets, parallelism);
                assert_eq!(generation.limits.max_parallel_source_gets, parallelism);
                assert!(generation.source_cache.is_none());
                let modeled = generation.modeled_memory_bytes();
                if let Some(control) = control_memory { assert_eq!(modeled, control); }
                control_memory = Some(modeled);

                fixture.arm(None);
                let rejected = TwoBitGeneration::open_remote(
                    fixture.reader.store(), &prefix, &root_sha,
                    TwoBitGenerationLimits { max_memory_bytes: modeled - 1, ..limits }, temp.path(),
                ).await.err().unwrap();
                assert!(matches!(rejected, TwoBitGenerationError::Invalid("memory cap")));
                assert!(fixture.snapshot().requests.iter().all(|(path, head, _, _)| {
                    *head || (path != source_key.as_ref() && path != leaf_key.as_ref() && path != key.as_ref())
                }));

                fixture.arm(None);
                let before = fixture.reader.transport_stats();
                let (result, ()) = tokio::join!(
                    generation.diagnostic_search_with_store(fixture.reader.store(), &query, 10),
                    async {
                        for (stage, count) in [(0, source_cover.len()), (1, plan.ranges.len())] {
                            let expected = parallelism.min(count);
                            fixture.wait(|s| s.active[stage] >= expected).await;
                            // Keep all valid tails held long enough to catch any
                            // extra request admitted above the caller's bound.
                            tokio::time::sleep(Duration::from_millis(30)).await;
                            let state = fixture.snapshot();
                            assert_eq!(state.active[stage], expected);
                            assert_eq!(state.peak[stage], expected);
                            fixture.release(stage);
                        }
                    }
                );
                let (result, actual_trace) = result.unwrap();
                assert_eq!(result.plan, plan);
                assert_eq!(serde_json::to_value(actual_trace).unwrap(), expected_trace);
                let bits = result.ranked.candidates.iter().map(|hit| (hit.ordinal, hit.id, hit.score.to_bits())).collect::<Vec<_>>();
                assert_eq!(bits.len(), 10);
                if let Some(control) = &control_bits { assert_eq!(&bits, control); }
                control_bits = Some(bits);
                let source_stats = Sq8ReadStats { submitted_gets: source_cover.len(), verified_bytes: source_bytes, failed_gets: 0 };
                let sq8_stats = Sq8ReadStats { submitted_gets: plan.ranges.len(), verified_bytes: plan.planned_bytes, failed_gets: 0 };
                assert_eq!(result.source_stats, source_stats);
                assert_eq!(result.ranked.stats, sq8_stats);
                assert_eq!(result.router_stats, Sq8ReadStats::default());
                assert_eq!(result.stages.leaf_peak_inflight, 0);
                assert_eq!(generation.remote_open_stats().unwrap().router_head_requests, 0);
                assert_eq!(generation.slots.available_permits(), 1);
                let mut state = fixture.snapshot();
                state.requests.sort();
                assert_eq!(state.requests, expected_requests);
                assert_eq!(state.active, [0; 3]);
                assert_eq!(state.peak[..2], [parallelism.min(source_cover.len()), parallelism.min(plan.ranges.len())]);
                let after = fixture.reader.transport_stats();
                assert_eq!(after.attempts - before.attempts, expected_requests.len() as u64);
                assert_eq!(after.consumed_payload_bytes - before.consumed_payload_bytes, (source_bytes + plan.planned_bytes) as u64);

                // Refuse the exact admitted rosters before transport; changing
                // planner caps instead would allow it to bridge or drop pages.
                let source = generation.source.as_ref().unwrap();
                for (location, authority, ranges, bytes) in [
                    (&source.location, &source.authority, &source_ranges, source_bytes),
                    (&key, &generation.pages, &sq8_ranges, plan.planned_bytes),
                ] {
                    for (gets, cap) in [(ranges.len() - 1, bytes), (ranges.len(), bytes - 1)] {
                        fixture.arm(None);
                        let before = fixture.reader.transport_stats();
                        let error = fixture.reader.fetch_verified_ranges(location, authority, ranges, ETAG, gets, cap, parallelism).await.err().unwrap();
                        assert_eq!(error.stats, Sq8ReadStats::default());
                        assert!(matches!(error.error, RangeFetchError::UnexpectedMetadata));
                        assert_eq!(fixture.reader.transport_stats(), before);
                        assert!(fixture.snapshot().requests.is_empty());
                    }
                }

                // Ordinal 1 fails its digest immediately; ordinal 0 remains a
                // delayed truncated stream, while ordinal 2 is a held valid
                // sibling. SOURCE also has queued requests beyond ordinal 32.
                for (stage, cover) in [(0, &source_cover), (1, &plan.ranges)] {
                    let starts = [cover[0].start, cover[1].start, cover[2].start];
                    fixture.arm(Some((stage, starts)));
                    let before = fixture.reader.transport_stats();
                    let mut query_future = Box::pin(generation.diagnostic_search_with_store(fixture.reader.store(), &query, 10));
                    tokio::select! {
                        _ = &mut query_future => panic!("returned before earlier stream error was released"),
                        () = fixture.wait(|s| s.finished[stage].contains(&starts[1]) && s.active[stage] >= 2) => {}
                    }
                    assert!(!fixture.snapshot().finished[stage].contains(&starts[0]));
                    fixture.release_error();
                    tokio::select! {
                        _ = &mut query_future => panic!("returned before valid sibling drained"),
                        () = fixture.wait(|s| s.finished[stage].contains(&starts[0])) => {}
                    }
                    assert!(!fixture.snapshot().finished[stage].contains(&starts[2]));
                    tokio::select! {
                        _ = &mut query_future => panic!("cancelled held valid sibling"),
                        () = tokio::time::sleep(Duration::from_millis(30)) => {}
                    }
                    fixture.release_sibling();
                    let error = query_future.await.err().unwrap();
                    fn first_error(error: &TwoBitGenerationError) -> &RangeFetchError {
                        match error {
                            TwoBitGenerationError::Query { error, .. } | TwoBitGenerationError::RouterCharged { error, .. } => first_error(error),
                            TwoBitGenerationError::SourceRead(failure) | TwoBitGenerationError::PagedRead { sq8: failure, .. } => &failure.error,
                            _ => panic!("unexpected error: {error:?}"),
                        }
                    }
                    assert!(matches!(first_error(&error), RangeFetchError::Store(_)), "{error:?}");
                    let failed = Sq8ReadStats {
                        submitted_gets: cover.len(),
                        verified_bytes: cover.iter().map(|r| r.len()).sum::<usize>() - cover[0].len() - cover[1].len(),
                        failed_gets: 2,
                    };
                    assert_eq!(error.read_stats(), if stage == 0 { (failed, Sq8ReadStats::default()) } else { (source_stats, failed) });
                    assert_eq!(error.router_stats(), result.router_stats);
                    assert_eq!(generation.slots.available_permits(), 1);
                    let mut state = fixture.snapshot();
                    assert!(state.errors.is_empty(), "{:?}", state.errors);
                    assert_eq!(state.active, [0; 3]);
                    state.finished[stage].sort_unstable();
                    assert_eq!(state.finished[stage], cover.iter().map(|r| r.start).collect::<Vec<_>>());
                    let expected = expected_requests.iter().filter(|(path, _, _, _)| stage == 1 || path != key.as_ref()).cloned().collect::<Vec<_>>();
                    state.requests.sort();
                    assert_eq!(state.requests, expected);
                    let after = fixture.reader.transport_stats();
                    assert_eq!(after.attempts - before.attempts, expected.len() as u64);
                    assert_eq!(after.stream_failures - before.stream_failures, 1);
                    let charged = source_bytes + if stage == 1 { plan.planned_bytes } else { 0 };
                    assert_eq!(after.consumed_payload_bytes - before.consumed_payload_bytes, (charged - 1) as u64);
                }
            }
            fixture.finish();
        }).await.expect("whole cold scheduling fixture deadline");
    }

    #[tokio::test]
    async fn native_membership_discovery_leaf_authorized_parity() {
        use sha2::{Digest, Sha256};
        let hash = |body: &[u8]| format!("{:x}", Sha256::digest(body));
        for rows in [1_usize, 33, 257, 16_385] {
            let temp = tempfile::tempdir().unwrap();
            let mut raw = Vec::new();
            let mut sq8 = Vec::new();
            for row in 0..rows {
                let values = [
                    (1 + row / 32 * 73 % 251) as u8,
                    (1 + row / 32 * 137 % 251) as u8,
                ];
                sq8.extend_from_slice(&((rows - 1 - row) as i64).to_le_bytes());
                let norm = values.iter().map(|&v| f32::from(v).powi(2)).sum::<f32>();
                sq8.extend_from_slice(&norm.to_le_bytes());
                for v in values {
                    raw.extend_from_slice(&f32::from(v).to_le_bytes());
                    sq8.push(v);
                }
            }
            let raw_path = temp.path().join("raw");
            let sq8_path = temp.path().join("sq8");
            fs::write(&raw_path, &raw).unwrap();
            fs::write(&sq8_path, &sq8).unwrap();
            let store = RecordedStore::default();
            let sq8_sha = hash(&sq8);
            let key = ObjectPath::from(format!("membership/objects/{sq8_sha}"));
            store.put(&key, sq8.into()).await.unwrap();
            let etag = store.head(&key).await.unwrap().e_tag.unwrap();
            let root = temp.path().join("generation");
            let root_sha = crate::two_bit_build::TwoBitGenerationBuilder {
                source: crate::two_bit_source::TwoBitSource {
                    raw: &raw_path,
                    raw_sha256: &hash(&raw),
                    sq8: &sq8_path,
                    sq8_sha256: &sq8_sha,
                    rows,
                    dimensions: 2,
                },
                base_epoch: 0,
                generation: 1,
                low: &[0.; 2],
                step: &[1.; 2],
                sq8_object_key: key.as_ref(),
                sq8_etag: &etag,
            }
            .build_with_discovery(None, DiscoveryMode::Semantic, &root, 128 * 1024 * 1024)
            .unwrap();
            let limits = TwoBitGenerationLimits {
                max_memory_bytes: 128 * 1024 * 1024,
                max_active_queries: 1,
                max_query_bytes: 16_773_120,
                max_query_gets: 32,
                max_parallel_gets: 32,
                max_source_bytes: 64 * 1024 * 1024,
                max_source_gets: 128,
                max_parallel_source_gets: 32,
                max_query_scratch_bytes: 1_048_576 + TwoBitPlanTrace::scratch_bytes(rows),
                already_pinned_bytes: 0,
            };
            let local = TwoBitGeneration::open(&root, &root_sha, limits).unwrap();
            let LoadedDiscovery::Semantic { router } = &local.discovery else {
                unreachable!()
            };
            assert_eq!(router.manifest().final_unit_rows, 1);
            let leaves = fs::read(root.join("router/leaves.bin")).unwrap();
            if rows == 16_385 {
                assert!(router.manifest().leaves.iter().any(|leaf| {
                    let units = router
                        .validate_leaf(leaf.leaf_id, &leaves[leaf.offset..leaf.offset + leaf.bytes])
                        .unwrap()
                        .into_iter()
                        .collect::<BTreeSet<_>>();
                    units.last().unwrap() - units.first().unwrap() + 1 > units.len()
                }));
            }
            let prefix = ObjectPath::from(format!("membership/generations/{root_sha}"));
            for name in local
                .manifest
                .discovery
                .files(true)
                .into_iter()
                .chain(["plane/records.bin"])
            {
                assert_ne!(name, "router/leaves.bin");
                store
                    .put(
                        &metadata_location(&prefix, name),
                        fs::read(root.join(name)).unwrap().into(),
                    )
                    .await
                    .unwrap();
            }
            let mut remote =
                TwoBitGeneration::open_remote(&store, &prefix, &root_sha, limits, temp.path())
                    .await
                    .unwrap();
            assert_eq!(remote.remote_open_stats().unwrap().router_head_requests, 0);
            assert_eq!(remote.remote_open_stats().unwrap().router_head_wall_ns, 0);
            for query in [[0.8, 0.35], [-0.25, 1.]] {
                // Control: existing leaf-authorized nomination, then exactly the
                // production source cover, authenticated reads, planner and ranker.
                let normalized = normalize_two_bit_diagnostic_query(&query).unwrap();
                let ids = router.select_leaves(normalized.as_ref()).unwrap();
                let parts = ids
                    .iter()
                    .map(|&id| {
                        let l = &router.manifest().leaves[id];
                        &leaves[l.offset..l.offset + l.bytes]
                    })
                    .collect::<Vec<_>>();
                let nomination = router.validate_selected(&ids, &parts).unwrap();
                let mut trace = TwoBitPlanTrace {
                    semantic_leaves: nomination.leaf_ids,
                    semantic_units: nomination.units.into_iter().collect(),
                    semantic_seed_additions: nomination.seed_additions,
                    ..Default::default()
                };
                let walks = vec![(nomination.seed_page, nomination.walk_units)];
                let closure = admit_source_walks(rows, &walks, remote.semantic_profile())
                    .unwrap()
                    .into_iter()
                    .map(|u| u / 8)
                    .collect();
                let width = remote.plane.receipt().record_bytes;
                let (cover, _) = plan_two_bit_source_cover(
                    &closure,
                    rows,
                    width,
                    limits.max_source_gets,
                    limits.max_source_bytes,
                )
                .unwrap();
                let ranges = cover
                    .iter()
                    .map(|r| (r.start / (32 * width), (r.end - 1) / (32 * width)))
                    .collect::<Vec<_>>();
                let prepared = remote
                    .plane
                    .prepare_query(
                        &query,
                        limits.max_query_scratch_bytes - TwoBitPlanTrace::scratch_bytes(rows),
                    )
                    .unwrap();
                store.reads.lock().unwrap().clear();
                let (verified, source_stats) =
                    remote.fetch_source_ranges(&store, &ranges).await.unwrap();
                let plan = remote
                    .plan_walks(
                        &walks,
                        &prepared,
                        |row| {
                            let offset = row.checked_mul(width)?;
                            let index = verified
                                .partition_point(|range| range.start <= offset)
                                .checked_sub(1)?;
                            let relative = offset.checked_sub(verified[index].start)?;
                            verified[index]
                                .bytes
                                .get(relative..relative.checked_add(width)?)
                        },
                        Some(&mut trace),
                    )
                    .unwrap();
                drop(verified);
                let page_bytes = 256 * (2 + 12);
                let ranges = plan
                    .ranges
                    .iter()
                    .map(|r| (r.start / page_bytes, (r.end - 1) / page_bytes))
                    .collect::<Vec<_>>();
                let k = rows.min(10);
                let expected = rank_verified_sq8_pages_inner(
                    &store,
                    &key,
                    &remote.pages,
                    &ranges,
                    &etag,
                    normalized.as_ref(),
                    &[0.; 2],
                    &[1.; 2],
                    k,
                    limits.max_query_gets,
                    limits.max_query_bytes,
                    32,
                    &[],
                )
                .await
                .unwrap();
                let expected_reads = store.reads.lock().unwrap().clone();
                store.reads.lock().unwrap().clear();
                let (actual, actual_trace) = remote
                    .diagnostic_search_with_store(&store, &query, k)
                    .await
                    .unwrap();
                assert_eq!(actual.plan, plan);
                assert_eq!(
                    serde_json::to_value(actual_trace).unwrap(),
                    serde_json::to_value(trace).unwrap()
                );
                assert_eq!(actual.source_stats, source_stats);
                assert_eq!(actual.ranked.stats, expected.stats);
                assert_eq!(actual.router_stats, Sq8ReadStats::default());
                assert_eq!(actual.stages.leaf_peak_inflight, 0);
                assert_eq!(
                    actual
                        .ranked
                        .candidates
                        .iter()
                        .map(|h| (h.ordinal, h.id, h.score.to_bits()))
                        .collect::<Vec<_>>(),
                    expected
                        .candidates
                        .iter()
                        .map(|h| (h.ordinal, h.id, h.score.to_bits()))
                        .collect::<Vec<_>>()
                );
                assert_eq!(*store.reads.lock().unwrap(), expected_reads);
            }
            assert_eq!(
                store
                    .leaf_accesses
                    .load(std::sync::atomic::Ordering::Relaxed),
                0
            );
            // Every live/retired directory remains included in the next open's model.
            let memory = remote.modeled_memory_bytes();
            let next = TwoBitGeneration::open_remote(
                &store,
                &prefix,
                &root_sha,
                TwoBitGenerationLimits {
                    already_pinned_bytes: memory,
                    max_memory_bytes: 2 * memory,
                    ..limits
                },
                temp.path(),
            )
            .await
            .unwrap();
            assert_eq!(next.modeled_memory_bytes(), 2 * memory);
            assert!(
                TwoBitGeneration::open_remote(
                    &store,
                    &prefix,
                    &root_sha,
                    TwoBitGenerationLimits {
                        already_pinned_bytes: memory,
                        max_memory_bytes: 2 * memory - 1,
                        ..limits
                    },
                    temp.path()
                )
                .await
                .is_err()
            );
            let membership_key = metadata_location(&prefix, "router/membership.bin");
            let original = fs::read(root.join("router/membership.bin")).unwrap();
            let mut damaged = original.clone();
            damaged[0] ^= 1;
            store.put(&membership_key, damaged.into()).await.unwrap();
            store.reads.lock().unwrap().clear();
            assert!(
                TwoBitGeneration::open_remote(&store, &prefix, &root_sha, limits, temp.path())
                    .await
                    .is_err()
            );
            assert!(
                store
                    .reads
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|r| r.1 || !r.0.ends_with("plane/records.bin"))
            );
            store.put(&membership_key, original.into()).await.unwrap();
            store.reads.lock().unwrap().clear();
            remote
                .search_with_store(&store, &[0.8, 0.35], rows.min(10), None)
                .await
                .unwrap();
            let source_start = store
                .reads
                .lock()
                .unwrap()
                .iter()
                .find(|r| !r.1 && r.0.ends_with("plane/records.bin"))
                .unwrap()
                .2
                .start as usize;
            *store.source_fault.lock().unwrap() = Some((source_start, "corrupt"));
            let error = remote
                .search_with_store(&store, &[0.8, 0.35], rows.min(10), None)
                .await
                .err()
                .unwrap();
            assert!(error.read_stats().0.failed_gets > 0);
            assert_eq!(error.router_stats(), Sq8ReadStats::default());
            *store.source_fault.lock().unwrap() = None;
            remote.limits.max_source_bytes = 1;
            store.reads.lock().unwrap().clear();
            assert!(
                remote
                    .search_with_store(&store, &[0.8, 0.35], rows.min(10), None)
                    .await
                    .is_err()
            );
            assert!(store.reads.lock().unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn native_100k_d1024_generation_serving_scalar_oracle() {
        use crate::semantic_unit_router::{Geometry, admit};
        use crate::two_bit_build::TwoBitGenerationBuilder;
        use crate::two_bit_source::TwoBitSource;
        use sha2::{Digest, Sha256};

        let hash = |body: &[u8]| format!("{:x}", Sha256::digest(body));
        let rows = 257_usize;
        let dimensions = 1024_usize;
        let profile = SemanticProfile::Native100k;
        assert!(profile.valid_geometry(100_000, dimensions));
        assert!(!profile.valid_geometry(rows, dimensions + 1));
        assert!(!profile.valid_geometry(100_001, dimensions));
        assert!(SemanticProfile::Fresh1m.valid_geometry(1_000_000, 768));
        assert!(!SemanticProfile::Fresh1m.valid_geometry(1_000_000, dimensions));
        // Admit the maximum geometry without constructing a 100k-row fixture.
        let geometry = Geometry {
            rows: 100_000,
            dimensions,
            units: 100_000_usize.div_ceil(32),
            blob_bytes: 32 + 100_000_usize.div_ceil(32) * dimensions * 2,
        };
        let peak = admit(geometry, profile.allocation_cap(), profile).unwrap();
        assert_eq!(admit(geometry, peak, profile).unwrap(), peak);
        assert!(admit(geometry, peak - 1, profile).is_err());
        assert!(
            admit(
                Geometry {
                    dimensions: 1025,
                    ..geometry
                },
                usize::MAX,
                profile
            )
            .is_err()
        );

        let low = (0..dimensions)
            .map(|d| (d as i32 % 7 - 3) as f32 / 1024.)
            .collect::<Vec<_>>();
        let step = (0..dimensions)
            .map(|d| (1 + d % 5) as f32 / 8192.)
            .collect::<Vec<_>>();
        let code = |row: usize, d: usize| (1 + (row * 17 + d * 29) % 239) as u8;
        let order = (0..rows as u64).rev().collect::<Vec<_>>();
        let mut raw = Vec::new();
        for row in 0..rows {
            for d in 0..dimensions {
                let value = low[d] + f32::from(code(row, d)) * step[d];
                raw.extend_from_slice(&value.to_le_bytes());
            }
        }
        let mut sq8 = Vec::new();
        for (ordinal, &row) in order.iter().enumerate() {
            let row = row as usize;
            let id = (1000 + row * 73 % rows) as i64;
            let mut norm = 0_f32;
            for d in 0..dimensions {
                let value = low[d] + f32::from(code(row, d)) * step[d];
                norm += value * value;
            }
            // The partial final page has an authoritative norm sentinel:
            // recomputing it from codes must change the returned score bits.
            if ordinal == rows - 1 {
                norm = 0.125;
            }
            sq8.extend_from_slice(&id.to_le_bytes());
            sq8.extend_from_slice(&norm.to_le_bytes());
            sq8.extend((0..dimensions).map(|d| code(row, d)));
        }
        let temp = tempfile::tempdir().unwrap();
        let raw_path = temp.path().join("raw");
        let sq8_path = temp.path().join("sq8");
        fs::write(&raw_path, &raw).unwrap();
        fs::write(&sq8_path, &sq8).unwrap();
        let store = RecordedStore::default();
        let sq8_sha = hash(&sq8);
        let key = ObjectPath::from(format!("semantic/objects/{sq8_sha}"));
        store.put(&key, sq8.clone().into()).await.unwrap();
        let etag = store.head(&key).await.unwrap().e_tag.unwrap();
        let root = temp.path().join("generation");
        let builder = TwoBitGenerationBuilder {
            source: TwoBitSource {
                raw: &raw_path,
                raw_sha256: &hash(&raw),
                sq8: &sq8_path,
                sq8_sha256: &sq8_sha,
                rows,
                dimensions,
            },
            base_epoch: 0,
            generation: 1,
            low: &low,
            step: &step,
            sq8_object_key: key.as_ref(),
            sq8_etag: &etag,
        };
        let rejected = temp.path().join("rejected");
        assert!(
            builder
                .build_with_discovery(Some(&order), DiscoveryMode::Semantic, &rejected, 1)
                .is_err()
        );
        assert!(!rejected.exists());
        let root_sha = builder
            .build_with_discovery(
                Some(&order),
                DiscoveryMode::Semantic,
                &root,
                128 * 1024 * 1024,
            )
            .unwrap();
        // Four complete 256-coordinate rotation blocks; the last unit/page has one row.
        let source_width = dimensions / 4 + 8;
        let prepare_bytes = (dimensions / 4 * 256 + dimensions) * size_of::<f64>();
        let trace_bytes = TwoBitPlanTrace::scratch_bytes(rows);
        let limits = TwoBitGenerationLimits {
            max_memory_bytes: 128 * 1024 * 1024,
            max_active_queries: 1,
            max_query_bytes: sq8.len(),
            max_query_gets: 1,
            max_parallel_gets: 1,
            max_source_bytes: rows * source_width,
            max_source_gets: 1,
            max_parallel_source_gets: 1,
            max_query_scratch_bytes: prepare_bytes + trace_bytes,
            already_pinned_bytes: 0,
        };
        let local = TwoBitGeneration::open(&root, &root_sha, limits).unwrap();
        assert_eq!(local.semantic_profile(), Some(profile));
        assert_eq!(local.plane.receipt().record_bytes, source_width);
        assert!(
            TwoBitGeneration::open(
                &root,
                &root_sha,
                TwoBitGenerationLimits {
                    max_memory_bytes: local.modeled_memory_bytes - 1,
                    ..limits
                }
            )
            .is_err()
        );
        let head = crate::two_bit_store::publish_two_bit_generation(
            &store,
            &ObjectPath::from("semantic/index"),
            &root,
            &root_sha,
            limits,
            None,
        )
        .await
        .unwrap();
        let prefix = head.metadata_prefix();
        let mut remote =
            TwoBitGeneration::open_remote_from_head(&store, &head, limits, temp.path())
                .await
                .unwrap();
        let queries = [
            (0..dimensions)
                .map(|d| 0.25 + (d % 13) as f32 / 7.)
                .collect::<Vec<_>>(),
            (0..dimensions)
                .map(|d| (d as i32 % 17 - 8) as f32 / 3.)
                .collect::<Vec<_>>(),
        ];
        for query in &queries {
            // Independent scalar normalization and stored-norm/code oracle;
            // no production scorer or query-preparation helper is used here.
            let query_norm = query
                .iter()
                .fold(0_f64, |sum, &q| sum + f64::from(q).powi(2))
                .sqrt();
            let normalized = query
                .iter()
                .map(|&q| (f64::from(q) / query_norm) as f32)
                .collect::<Vec<_>>();
            let mut shift = 0_f32;
            let mut qnorm = 0_f32;
            for d in 0..dimensions {
                shift += normalized[d] * low[d];
                qnorm += normalized[d] * normalized[d];
            }
            shift -= qnorm / 2.;
            let mut oracle = sq8
                .chunks_exact(dimensions + 12)
                .enumerate()
                .map(|(ordinal, record)| {
                    let id = i64::from_le_bytes(record[..8].try_into().unwrap());
                    let norm = f32::from_le_bytes(record[8..12].try_into().unwrap());
                    let mut inner = 0_f32;
                    for d in 0..dimensions {
                        inner += f32::from(record[12 + d]) * (normalized[d] * step[d]);
                    }
                    (ordinal, id, norm - 2. * (inner + shift))
                })
                .collect::<Vec<_>>();
            oracle.sort_by(|a, b| a.2.total_cmp(&b.2).then(a.1.cmp(&b.1)));
            let local_result = local
                .search_with_store(&store, query, 10, None)
                .await
                .unwrap();
            store.reads.lock().unwrap().clear();
            let direct = remote
                .search_with_store(&store, query, 10, None)
                .await
                .unwrap();
            let direct_reads = store.reads.lock().unwrap().clone();
            store.reads.lock().unwrap().clear();
            let (diagnosed, trace) = remote
                .diagnostic_search_with_store(&store, query, 10)
                .await
                .unwrap();
            let bits = |result: &TwoBitSearchResult| {
                result
                    .ranked
                    .candidates
                    .iter()
                    .map(|hit| (hit.ordinal, hit.id, hit.score.to_bits()))
                    .collect::<Vec<_>>()
            };
            assert_eq!(
                bits(&direct),
                oracle[..10]
                    .iter()
                    .map(|&(ordinal, id, score)| (ordinal, id, score.to_bits()))
                    .collect::<Vec<_>>()
            );
            assert_eq!(bits(&direct), bits(&local_result));
            assert_eq!(bits(&direct), bits(&diagnosed));
            assert!(
                direct
                    .ranked
                    .candidates
                    .iter()
                    .any(|hit| hit.ordinal == rows - 1)
            );
            assert_eq!(direct.plan.selected_pages, [0, 1]);
            assert_eq!(direct.plan, diagnosed.plan);
            assert_eq!(direct.source_stats, diagnosed.source_stats);
            assert_eq!(direct.router_stats, diagnosed.router_stats);
            assert_eq!(direct.ranked.stats, diagnosed.ranked.stats);
            assert_eq!(*store.reads.lock().unwrap(), direct_reads);
            assert_eq!(
                trace.semantic_units,
                (0..rows.div_ceil(32)).collect::<Vec<_>>()
            );
            assert_eq!(direct.router_stats, Sq8ReadStats::default());
            assert_eq!(direct.stages.leaf_peak_inflight, 0);
            assert!(
                direct_reads
                    .iter()
                    .all(|read| !read.0.ends_with("router/leaves.bin"))
            );
            for (name, stats, bytes) in [
                (
                    "plane/records.bin",
                    direct.source_stats,
                    rows * source_width,
                ),
                (key.as_ref(), direct.ranked.stats, sq8.len()),
            ] {
                let reads = direct_reads
                    .iter()
                    .filter(|read| read.0.ends_with(name))
                    .collect::<Vec<_>>();
                assert_eq!(stats.submitted_gets, 1);
                assert_eq!(stats.failed_gets, 0);
                assert_eq!(stats.verified_bytes, bytes);
                assert_eq!(reads.len(), stats.submitted_gets);
                assert_eq!(
                    reads
                        .iter()
                        .map(|read| (read.2.end - read.2.start) as usize)
                        .sum::<usize>(),
                    bytes
                );
                assert!(reads.iter().all(|read| !read.1 && read.3.is_some()));
            }
            assert_eq!(direct_reads.len(), 2);
        }

        let query = &queries[0];
        // Refusal precedes the excluded class of reads; codec scratch precedes all I/O.
        for cap in [
            "scratch",
            "source_gets",
            "source_bytes",
            "sq8_gets",
            "sq8_bytes",
        ] {
            remote.limits = limits;
            match cap {
                "scratch" => remote.limits.max_query_scratch_bytes = prepare_bytes - 1,
                "source_gets" => remote.limits.max_source_gets = 0,
                "source_bytes" => remote.limits.max_source_bytes -= 1,
                "sq8_gets" => remote.limits.max_query_gets = 0,
                _ => remote.limits.max_query_bytes = 1,
            }
            store.reads.lock().unwrap().clear();
            let error = remote
                .search_with_store(&store, query, 10, None)
                .await
                .err()
                .unwrap();
            let reads = store.reads.lock().unwrap();
            assert!(!reads.iter().any(|read| read.0 == key.as_ref()), "{cap}");
            if cap == "scratch" {
                assert!(reads.is_empty());
            } else if cap.starts_with("source") {
                assert!(reads.is_empty());
                assert_eq!(
                    error.read_stats(),
                    (Sq8ReadStats::default(), Sq8ReadStats::default())
                );
            }
        }
        remote.limits = limits;
        remote.limits.max_query_scratch_bytes = prepare_bytes + trace_bytes - 1;
        store.reads.lock().unwrap().clear();
        assert!(
            remote
                .diagnostic_search_with_store(&store, query, 10)
                .await
                .is_err()
        );
        assert!(store.reads.lock().unwrap().is_empty());
        remote.limits = limits;

        // The bounded validator streams source records; complete leaf authentication
        // belongs to the publisher. Exercise it before transport ETags are mutated.
        let publication_index = ObjectPath::from("semantic/corruption");
        for name in ["plane/records.bin", "router/leaves.bin"] {
            let path = root.join(name);
            let original = fs::read(&path).unwrap();
            let mut damaged = original.clone();
            *damaged.last_mut().unwrap() ^= 1;
            fs::write(&path, damaged).unwrap();
            store.writes.lock().unwrap().clear();
            let error = crate::two_bit_store::publish_two_bit_generation(
                &store,
                &publication_index,
                &root,
                &root_sha,
                limits,
                None,
            )
            .await
            .err()
            .unwrap();
            let expected = if name == "plane/records.bin" {
                "source page digest"
            } else {
                "artifact identity"
            };
            assert!(
                matches!(
                    &error,
                    crate::two_bit_store::TwoBitStoreError::Generation(
                        TwoBitGenerationError::Plane(SourceBuildError::Invalid(reason))
                    ) if *reason == expected
                ),
                "{name}: {error:?}"
            );
            assert!(store.writes.lock().unwrap().is_empty());
            assert!(
                crate::two_bit_store::read_two_bit_head(&store, &publication_index)
                    .await
                    .unwrap()
                    .is_none(),
                "{name}"
            );
            fs::write(path, original).unwrap();
        }
        // The same publisher, namespace and limits must accept the restored artifacts.
        crate::two_bit_store::publish_two_bit_generation(
            &store,
            &publication_index,
            &root,
            &root_sha,
            limits,
            None,
        )
        .await
        .unwrap();

        // Update transport ETags after damage so authentication must reject the bytes.
        for name in ["router/leaves.bin", "plane/records.bin", "sq8"] {
            let (location, original) = if name == "sq8" {
                (key.clone(), sq8.clone())
            } else {
                (
                    metadata_location(&prefix, name),
                    fs::read(root.join(name)).unwrap(),
                )
            };
            let mut damaged = original.clone();
            *damaged.last_mut().unwrap() ^= 1;
            for (rejecting, body) in [(true, damaged), (false, original)] {
                store.put(&location, body.into()).await.unwrap();
                let tag = store.head(&location).await.unwrap().e_tag.unwrap();
                match name {
                    "sq8" => remote.manifest.sq8_etag = tag,
                    "plane/records.bin" => remote.source.as_mut().unwrap().etag = tag,
                    _ => (),
                }
                if name == "router/leaves.bin" {
                    store.reads.lock().unwrap().clear();
                    let result = remote
                        .search_with_store(&store, query, 10, None)
                        .await
                        .unwrap();
                    assert_eq!(result.router_stats, Sq8ReadStats::default());
                    assert!(
                        store
                            .reads
                            .lock()
                            .unwrap()
                            .iter()
                            .all(|r| !r.0.ends_with(name))
                    );
                } else if rejecting {
                    store.reads.lock().unwrap().clear();
                    let error = remote
                        .search_with_store(&store, query, 10, None)
                        .await
                        .err()
                        .unwrap();
                    let (source, sq8) = error.read_stats();
                    match name {
                        "plane/records.bin" => {
                            assert_eq!(source.failed_gets, 1);
                            assert_eq!(sq8, Sq8ReadStats::default());
                        }
                        _ => {
                            assert_eq!(source.verified_bytes, rows * source_width);
                            assert_eq!(sq8.failed_gets, 1);
                        }
                    }
                }
            }
        }
        remote
            .search_with_store(&store, query, 10, None)
            .await
            .unwrap();
        let root_path = root.join("manifest.json");
        let original = fs::read(&root_path).unwrap();
        assert!(TwoBitGeneration::open(&root, &"0".repeat(64), limits).is_err());
        // Rehash the outer root: mismatched authenticated inner identities still fail.
        for field in [
            "source_sha256",
            "source_order_sha256",
            "mean_sha256",
            "records_sha256",
            "sq8_sha256",
            "input_root_sha256",
            "generation",
        ] {
            let mut manifest: serde_json::Value = serde_json::from_slice(&original).unwrap();
            if field == "generation" {
                manifest[field] = 2.into();
            } else {
                manifest["discovery"][field] = "0".repeat(64).into();
            }
            let body = serde_json::to_vec(&manifest).unwrap();
            fs::write(&root_path, &body).unwrap();
            assert!(
                TwoBitGeneration::open(&root, &hash(&body), limits).is_err(),
                "{field}"
            );
        }
        fs::write(root_path, original).unwrap();
        TwoBitGeneration::validate_local_publication(&root, &root_sha, limits).unwrap();
    }

    #[tokio::test]
    async fn semantic_object_store_parity() {
        use crate::two_bit_build::TwoBitGenerationBuilder;
        use crate::two_bit_source::TwoBitSource;
        use object_store::PutPayload;
        use sha2::{Digest, Sha256};
        let hash = |body: &[u8]| format!("{:x}", Sha256::digest(body));
        let rows = 16_385;
        let temp = tempfile::tempdir().unwrap();
        let mut raw = Vec::new();
        let mut sq8 = Vec::new();
        for row in 0..rows {
            let a = (1 + (row / 32 * 73) % 251) as u8;
            let b = (1 + (row / 32 * 137) % 251) as u8;
            raw.extend_from_slice(&(a as f32).to_le_bytes());
            raw.extend_from_slice(&(b as f32).to_le_bytes());
            sq8.extend_from_slice(&((rows - row + 1000) as i64).to_le_bytes());
            sq8.extend_from_slice(&((a as f32).powi(2) + (b as f32).powi(2)).to_le_bytes());
            sq8.extend_from_slice(&[a, b]);
        }
        let raw_path = temp.path().join("raw");
        let sq8_path = temp.path().join("sq8");
        fs::write(&raw_path, &raw).unwrap();
        fs::write(&sq8_path, &sq8).unwrap();
        let store = std::sync::Arc::new(RecordedStore::default());
        let sq8_sha = hash(&sq8);
        let key = ObjectPath::from(format!("semantic/objects/{sq8_sha}"));
        store.put(&key, sq8.clone().into()).await.unwrap();
        let etag = store.head(&key).await.unwrap().e_tag.unwrap();
        let root = temp.path().join("generation");
        let order = (0..rows as u64).collect::<Vec<_>>();
        let builder = TwoBitGenerationBuilder {
            source: TwoBitSource {
                raw: &raw_path,
                raw_sha256: &hash(&raw),
                sq8: &sq8_path,
                sq8_sha256: &sq8_sha,
                rows,
                dimensions: 2,
            },
            base_epoch: 0,
            generation: 1,
            low: &[0.; 2],
            step: &[1.; 2],
            sq8_object_key: key.as_ref(),
            sq8_etag: &etag,
        };
        let root_sha = builder
            .build_with_discovery(Some(&order), DiscoveryMode::Semantic, &root, 128_000_000)
            .unwrap();
        assert!(!root.join("graph.bin").exists() && !root.join("diverse_graph.bin").exists());
        let limits = TwoBitGenerationLimits {
            max_memory_bytes: 128_000_000,
            max_active_queries: 2,
            max_query_bytes: 32 * 256 * 14,
            max_query_gets: 32,
            max_parallel_gets: 4,
            max_source_bytes: 64 * 1024 * 1024,
            max_source_gets: 128,
            max_parallel_source_gets: 4,
            max_query_scratch_bytes: 400_000,
            already_pinned_bytes: 0,
        };
        let index = ObjectPath::from("semantic/index");
        // Publication admits paged serving metadata and one validation page,
        // even when the eager reference's whole record allocation cannot fit.
        let mut publication_limits = TwoBitGenerationLimits {
            max_active_queries: 1,
            max_query_bytes: 2 * 1024 * 1024,
            max_source_bytes: 65536,
            already_pinned_bytes: 262144,
            ..limits
        };
        let bounded =
            TwoBitGeneration::open_inner(&root, &root_sha, publication_limits, None, true).unwrap();
        assert!(bounded.plane.record(0).is_none());
        assert!(bounded.source.is_none());
        let bounded_bytes = bounded.modeled_memory_bytes;
        let eager_bytes = TwoBitGeneration::open(&root, &root_sha, publication_limits)
            .unwrap()
            .modeled_memory_bytes;
        assert_eq!(
            eager_bytes - bounded_bytes,
            3 * fs::metadata(root.join("plane/records.bin")).unwrap().len()
                - 2 * publication_limits.max_source_bytes as u64
                - 32 * bounded.plane.receipt().record_bytes as u64,
        );
        let semantic_bytes = crate::semantic_unit_router::admit(
            crate::semantic_unit_router::Geometry {
                rows,
                dimensions: 2,
                units: rows.div_ceil(32),
                blob_bytes: 32 + rows.div_ceil(32) * 2 * 2,
            },
            usize::MAX,
            bounded.semantic_profile().unwrap(),
        )
        .unwrap() as u64;
        publication_limits.max_memory_bytes = bounded
            .modeled_memory_bytes
            .max(semantic_bytes + publication_limits.already_pinned_bytes);
        assert!(publication_limits.max_memory_bytes < eager_bytes);
        drop(bounded);
        assert!(TwoBitGeneration::open(&root, &root_sha, publication_limits).is_err());
        TwoBitGeneration::validate_local_publication(&root, &root_sha, publication_limits).unwrap();
        for rejected in [
            TwoBitGenerationLimits {
                max_memory_bytes: bounded_bytes - 1,
                ..publication_limits
            },
            TwoBitGenerationLimits {
                max_active_queries: 0,
                ..publication_limits
            },
            TwoBitGenerationLimits {
                max_source_bytes: 0,
                ..publication_limits
            },
            TwoBitGenerationLimits {
                max_query_bytes: 0,
                ..publication_limits
            },
            TwoBitGenerationLimits {
                already_pinned_bytes: u64::MAX,
                ..publication_limits
            },
            TwoBitGenerationLimits {
                already_pinned_bytes: publication_limits.max_memory_bytes,
                ..publication_limits
            },
        ] {
            assert!(
                TwoBitGeneration::validate_local_publication(&root, &root_sha, rejected).is_err()
            );
        }
        // Both local readers reject byte corruption, truncation and growth,
        // including the final partial source page and digest table entry.
        store.writes.lock().unwrap().clear();
        for name in [
            "plane/records.bin",
            "plane/page_digests.bin",
            "plane/mean.bin",
            "page_digests.bin",
            "router/root.bin",
            "router/membership.bin",
        ] {
            let path = root.join(name);
            let original = fs::read(&path).unwrap();
            for fault in ["corrupt", "short", "long"] {
                let mut damaged = original.clone();
                match fault {
                    "corrupt" => *damaged.last_mut().unwrap() ^= 1,
                    "short" => {
                        damaged.pop();
                    }
                    _ => damaged.push(0),
                }
                fs::write(&path, damaged).unwrap();
                assert!(
                    TwoBitGeneration::open(&root, &root_sha, limits).is_err(),
                    "{name} {fault}"
                );
                assert!(
                    crate::two_bit_store::publish_two_bit_generation(
                        store.as_ref(),
                        &index,
                        &root,
                        &root_sha,
                        publication_limits,
                        None,
                    )
                    .await
                    .is_err(),
                    "{name} {fault}"
                );
                assert!(store.writes.lock().unwrap().is_empty());
                assert!(
                    TwoBitGeneration::validate_local_publication(
                        &root,
                        &root_sha,
                        publication_limits
                    )
                    .is_err(),
                    "{name} {fault}"
                );
            }
            fs::write(path, original).unwrap();
        }
        let plane_path = root.join("plane/manifest.json");
        let root_path = root.join("manifest.json");
        let original_plane = fs::read(&plane_path).unwrap();
        let original_root = fs::read(&root_path).unwrap();
        let sidecar_path = root.join("plane/page_digests.bin");
        let original_sidecar = fs::read(&sidecar_path).unwrap();
        for fault in [
            "whole_sha",
            "late_page",
            "source_order",
            "schema",
            "geometry",
        ] {
            let mut plane: serde_json::Value = serde_json::from_slice(&original_plane).unwrap();
            let mut manifest: serde_json::Value = serde_json::from_slice(&original_root).unwrap();
            match fault {
                "whole_sha" => {
                    plane["records_sha256"] = "0".repeat(64).into();
                    manifest["discovery"]["records_sha256"] = plane["records_sha256"].clone();
                }
                "late_page" => {
                    let mut sidecar = original_sidecar.clone();
                    *sidecar.last_mut().unwrap() ^= 1;
                    plane["page_digest_sha256"] = hash(&sidecar).into();
                    fs::write(&sidecar_path, sidecar).unwrap();
                }
                "source_order" => plane["source_order_sha256"] = "0".repeat(64).into(),
                "schema" => plane["schema"] = "borsuk-two-bit-plane-v2".into(),
                _ => plane["record_bytes"] = 1.into(),
            }
            let plane_body = serde_json::to_vec(&plane).unwrap();
            manifest["plane_manifest_sha256"] = hash(&plane_body).into();
            let root_body = serde_json::to_vec(&manifest).unwrap();
            let trusted = hash(&root_body);
            fs::write(&plane_path, plane_body).unwrap();
            fs::write(&root_path, root_body).unwrap();
            assert!(
                TwoBitGeneration::open(&root, &trusted, limits).is_err(),
                "{fault}"
            );
            let error =
                TwoBitGeneration::validate_local_publication(&root, &trusted, publication_limits)
                    .err()
                    .unwrap();
            if fault == "whole_sha" {
                assert!(
                    matches!(
                        error,
                        TwoBitGenerationError::Plane(SourceBuildError::Invalid(
                            "artifact identity"
                        ))
                    ),
                    "{error:?}"
                );
            } else if fault == "late_page" {
                assert!(
                    matches!(
                        error,
                        TwoBitGenerationError::Plane(SourceBuildError::Invalid(
                            "source page digest"
                        ))
                    ),
                    "{error:?}"
                );
            }
            fs::write(&sidecar_path, &original_sidecar).unwrap();
        }
        fs::write(&plane_path, original_plane).unwrap();
        fs::write(&root_path, &original_root).unwrap();
        for fault in ["schema", "canonical", "object_key", "mean_binding"] {
            let mut manifest: serde_json::Value = serde_json::from_slice(&original_root).unwrap();
            match fault {
                "schema" => manifest["schema"] = "borsuk-two-bit-generation-v7".into(),
                "canonical" => manifest["canonical"]["rows"] = (rows + 1).into(),
                "object_key" => manifest["sq8_object_key"] = "wrong/objects/wrong".into(),
                _ => manifest["discovery"]["mean_sha256"] = "0".repeat(64).into(),
            }
            let body = serde_json::to_vec(&manifest).unwrap();
            fs::write(&root_path, &body).unwrap();
            assert!(
                TwoBitGeneration::open(&root, &hash(&body), limits).is_err(),
                "{fault}"
            );
            assert!(
                TwoBitGeneration::validate_local_publication(
                    &root,
                    &hash(&body),
                    publication_limits
                )
                .is_err(),
                "{fault}"
            );
        }
        fs::write(root_path, original_root).unwrap();
        // Publication alone authenticates canonical IDs and the complete
        // semantic payload/centroids; these checks must survive the new scan.
        for name in ["canonical.bin", "router/leaves.bin", "centroids.bin"] {
            let path = root.join(name);
            let original = fs::read(&path).unwrap();
            let mut damaged = original.clone();
            let byte = if name == "canonical.bin" {
                damaged.len() - (8 + 2 * 4)
            } else {
                damaged.len() - 1
            };
            damaged[byte] ^= 1;
            fs::write(&path, damaged).unwrap();
            assert!(
                crate::two_bit_store::publish_two_bit_generation(
                    store.as_ref(),
                    &index,
                    &root,
                    &root_sha,
                    publication_limits,
                    None,
                )
                .await
                .is_err(),
                "{name}"
            );
            assert!(
                !store
                    .writes
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|key| key.ends_with("/head.json"))
            );
            assert!(
                crate::two_bit_store::read_two_bit_head(store.as_ref(), &index)
                    .await
                    .unwrap()
                    .is_none()
            );
            fs::write(path, original).unwrap();
        }
        #[cfg(unix)]
        for name in [
            "manifest.json",
            "plane/records.bin",
            "plane/mean.bin",
            "page_digests.bin",
            "router/leaves.bin",
            "canonical.bin",
            "centroids.bin",
        ] {
            let path = root.join(name);
            let saved = root.join("saved-artifact");
            fs::rename(&path, &saved).unwrap();
            #[cfg(target_os = "linux")]
            let kinds = &["directory", "symlink", "socket", "fifo"][..];
            #[cfg(not(target_os = "linux"))]
            let kinds = &["directory", "symlink", "socket"][..];
            for &kind in kinds {
                let socket = match kind {
                    "directory" => {
                        fs::create_dir(&path).unwrap();
                        None
                    }
                    "symlink" => {
                        std::os::unix::fs::symlink(&saved, &path).unwrap();
                        None
                    }
                    #[cfg(target_os = "linux")]
                    "fifo" => {
                        rustix::fs::mkfifoat(
                            rustix::fs::CWD,
                            &path,
                            rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
                        )
                        .unwrap();
                        None
                    }
                    _ => Some(std::os::unix::net::UnixListener::bind(&path).unwrap()),
                };
                store.writes.lock().unwrap().clear();
                assert!(
                    crate::two_bit_store::publish_two_bit_generation(
                        store.as_ref(),
                        &index,
                        &root,
                        &root_sha,
                        publication_limits,
                        None,
                    )
                    .await
                    .is_err(),
                    "{name} {kind}"
                );
                assert!(store.writes.lock().unwrap().is_empty());
                drop(socket);
                if kind == "directory" {
                    fs::remove_dir(&path).unwrap();
                } else {
                    fs::remove_file(&path).unwrap();
                }
            }
            fs::rename(saved, path).unwrap();
        }
        // Cancellation at the first yielded remote check cannot publish a head.
        store.writes.lock().unwrap().clear();
        {
            let mut pending = std::pin::pin!(crate::two_bit_store::publish_two_bit_generation(
                store.as_ref(),
                &index,
                &root,
                &root_sha,
                publication_limits,
                None,
            ));
            assert!(futures_util::poll!(pending.as_mut()).is_pending());
        }
        assert!(store.writes.lock().unwrap().is_empty());
        assert!(
            crate::two_bit_store::read_two_bit_head(store.as_ref(), &index)
                .await
                .unwrap()
                .is_none()
        );
        let head = crate::two_bit_store::publish_two_bit_generation(
            store.as_ref(),
            &index,
            &root,
            &root_sha,
            publication_limits,
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            store.writes.lock().unwrap().last().unwrap(),
            "semantic/index/head.json"
        );
        let prefix = head.metadata_prefix();
        for name in ["centroids.bin", "graph.bin", "diverse_graph.bin"] {
            assert!(store.head(&metadata_location(&prefix, name)).await.is_err());
        }
        let root_key = metadata_location(&prefix, "manifest.json");
        let root_body = fs::read(root.join("manifest.json")).unwrap();
        let scratch = tempfile::tempdir().unwrap();
        // Published heads already own the authenticated body: no root transport.
        store.reads.lock().unwrap().clear();
        *store.root_read_budget.lock().unwrap() = Some((root_key.to_string(), 0));
        let published =
            TwoBitGeneration::open_remote_from_head(store.as_ref(), &head, limits, scratch.path())
                .await
                .unwrap();
        assert_reused_root(&published, root_body.len() as u64);
        assert!(
            store
                .reads
                .lock()
                .unwrap()
                .iter()
                .all(|read| read.0 != root_key.as_ref())
        );
        assert_eq!(fs::read_dir(scratch.path()).unwrap().count(), 0);
        *store.root_read_budget.lock().unwrap() = None;
        for (body, trusted) in [
            (b"{}".to_vec(), root_sha.clone()),
            (b"{".to_vec(), hash(b"{")),
            (b"{}".to_vec(), hash(b"{}")),
        ] {
            store.put(&root_key, body.into()).await.unwrap();
            store.reads.lock().unwrap().clear();
            assert!(
                stage_two_bit_metadata(
                    store.as_ref(),
                    &prefix,
                    &trusted,
                    u64::MAX,
                    scratch.path(),
                    None
                )
                .await
                .is_err()
            );
            let reads = store.reads.lock().unwrap().len();
            assert_eq!(reads, 2);
            assert!(
                store
                    .reads
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|r| r.0.ends_with("/manifest.json"))
            );
            tokio::task::yield_now().await;
            assert_eq!(store.reads.lock().unwrap().len(), reads);
            assert_eq!(fs::read_dir(scratch.path()).unwrap().count(), 0);
        }
        store
            .put(&root_key, root_body.clone().into())
            .await
            .unwrap();
        store.reads.lock().unwrap().clear();
        let mut lazy =
            TwoBitGeneration::open_remote(store.as_ref(), &prefix, &root_sha, limits, temp.path())
                .await
                .unwrap();
        // The complete authority-head + open path must perform exactly one root GET.
        let direct_reads = store.reads.lock().unwrap().clone();
        store.reads.lock().unwrap().clear();
        let authenticated_head = crate::two_bit_store::read_two_bit_head(store.as_ref(), &index)
            .await
            .unwrap()
            .unwrap();
        *store.root_read_budget.lock().unwrap() = Some((root_key.to_string(), 0));
        let reused = TwoBitGeneration::open_remote_from_head(
            store.as_ref(),
            &authenticated_head,
            limits,
            scratch.path(),
        )
        .await
        .unwrap();
        assert_reused_root(&reused, root_body.len() as u64);
        assert_eq!(
            reused.limits.already_pinned_bytes,
            limits.already_pinned_bytes
        );
        assert_eq!(
            reused.modeled_memory_bytes - lazy.modeled_memory_bytes,
            root_body.len() as u64
        );
        {
            let reads = store.reads.lock().unwrap();
            assert_eq!(
                reads
                    .iter()
                    .filter(|read| read.0 == root_key.as_ref() && !read.1)
                    .count(),
                1
            );
            assert!(
                reads
                    .iter()
                    .all(|read| read.0 != root_key.as_ref() || !read.1)
            );
        }
        assert_eq!(fs::read_dir(scratch.path()).unwrap().count(), 0);
        *store.root_read_budget.lock().unwrap() = None;
        drop(reused);
        drop(authenticated_head);
        drop(published);
        for _ in 0..2 {
            store.reads.lock().unwrap().clear();
            *store.root_read_budget.lock().unwrap() = Some((root_key.to_string(), 1));
            let coordinated = crate::two_bit_index::TwoBitIndex::open_coordinated(
                store.as_ref(),
                &index,
                &temp.path().join("reader-maintenance"),
                limits,
                scratch.path(),
            )
            .await
            .unwrap();
            let root_stats = &coordinated.remote_open_stats().unwrap().metadata[0];
            assert_eq!(root_stats.reused_root_bytes, head.retained_root_bytes());
            assert_eq!(
                root_stats.logical_get_requests + root_stats.logical_head_requests,
                0
            );
            assert_eq!(
                store
                    .reads
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|read| read.0 == root_key.as_ref())
                    .count(),
                1
            );
            assert_eq!(fs::read_dir(scratch.path()).unwrap().count(), 0);
            drop(coordinated);
        }
        *store.root_read_budget.lock().unwrap() = None;
        // Preserve the standalone trusted-SHA path's transport assertions.
        *store.reads.lock().unwrap() = direct_reads;
        assert!(lazy.plane.record(0).is_none());
        let startup = lazy.remote_open_stats().unwrap();
        assert_eq!(startup.source_head_requests, 1);
        assert_eq!(startup.router_head_requests, 0);
        assert_eq!(startup.router_head_wall_ns, 0);
        assert_eq!(startup.metadata.len(), 8);
        assert_metadata_waves(
            startup,
            &[
                "manifest.json",
                "page_manifest.json",
                "page_digests.bin",
                "plane/manifest.json",
                "plane/mean.bin",
                "plane/page_digests.bin",
                "router/root.bin",
                "router/membership.bin",
            ],
        );
        assert_eq!(
            startup
                .metadata
                .iter()
                .map(|r| r.logical_get_requests)
                .sum::<u64>(),
            8
        );
        assert_eq!(
            startup
                .metadata
                .iter()
                .map(|r| r.logical_head_requests)
                .sum::<u64>(),
            3
        );
        for entry in &startup.metadata {
            let retained = ["manifest.json", "page_manifest.json", "plane/manifest.json"]
                .contains(&entry.name.as_str());
            assert_eq!(entry.logical_head_requests, u64::from(retained));
            if !retained {
                assert_eq!(entry.head_wall_ns, 0);
            }
        }
        {
            let reads = store.reads.lock().unwrap();
            assert_eq!(reads.iter().filter(|(_, head, _, _)| !head).count(), 8);
            assert_eq!(reads.iter().filter(|(_, head, _, _)| *head).count(), 4);
        }
        assert!(
            store
                .reads
                .lock()
                .unwrap()
                .iter()
                .all(|(name, head, _, _)| *head
                    || ![
                        "router/leaves.bin",
                        "plane/records.bin",
                        "centroids.bin",
                        "graph.bin",
                        "diverse_graph.bin"
                    ]
                    .iter()
                    .any(|suffix| name.ends_with(suffix)))
        );
        let scratch = tempfile::tempdir().unwrap();
        store.reads.lock().unwrap().clear();
        *store.root_read_budget.lock().unwrap() = Some((root_key.to_string(), 0));
        for pinned in [0, u64::MAX] {
            assert!(
                TwoBitGeneration::open_remote_from_head(
                    store.as_ref(),
                    &head,
                    TwoBitGenerationLimits {
                        max_memory_bytes: head.retained_root_bytes(),
                        already_pinned_bytes: pinned,
                        ..limits
                    },
                    scratch.path(),
                )
                .await
                .is_err()
            );
            assert!(store.reads.lock().unwrap().is_empty());
            assert_eq!(fs::read_dir(scratch.path()).unwrap().count(), 0);
        }
        {
            let mut pending = std::pin::pin!(TwoBitGeneration::open_remote_from_head(
                store.as_ref(),
                &head,
                limits,
                scratch.path(),
            ));
            assert!(futures_util::poll!(pending.as_mut()).is_pending());
            assert_eq!(fs::read_dir(scratch.path()).unwrap().count(), 1);
        }
        assert_eq!(fs::read_dir(scratch.path()).unwrap().count(), 0);
        for fault in ["size", "range", "short", "long", "corrupt"] {
            *store.metadata_fault.lock().unwrap() = Some(fault);
            store.reads.lock().unwrap().clear();
            *store.root_read_budget.lock().unwrap() = Some((root_key.to_string(), 0));
            let error = TwoBitGeneration::open_remote_from_head(
                store.as_ref(),
                &head,
                limits,
                scratch.path(),
            )
            .await
            .err()
            .unwrap();
            if fault == "corrupt" {
                assert!(
                    matches!(error, TwoBitGenerationError::Plane(_)),
                    "{error:?}"
                );
            } else {
                assert!(
                    matches!(
                        error,
                        TwoBitGenerationError::Stage(ObjectNativeOpenError::Invalid(
                            "remote metadata length"
                        ))
                    ),
                    "{error:?}"
                );
            }
            let reads = store.reads.lock().unwrap();
            assert_eq!(
                reads
                    .iter()
                    .filter(|(name, head, _, _)| !head && name.ends_with("/plane/mean.bin"))
                    .count(),
                1
            );
            assert!(
                !reads
                    .iter()
                    .any(|(name, head, _, _)| *head && name.ends_with("/plane/mean.bin"))
            );
            assert_eq!(fs::read_dir(scratch.path()).unwrap().count(), 0);
        }
        *store.metadata_fault.lock().unwrap() = None;
        for (suffix, expected) in [
            ("plane/records.bin", "source ETag"),
            ("router/leaves.bin", "leaf ETag"),
        ] {
            *store.bad_etag_suffix.lock().unwrap() = Some(suffix);
            let result = TwoBitGeneration::open_remote_from_head(
                store.as_ref(),
                &head,
                limits,
                scratch.path(),
            )
            .await;
            if suffix == "router/leaves.bin" {
                assert_eq!(
                    result
                        .unwrap()
                        .remote_open_stats()
                        .unwrap()
                        .router_head_requests,
                    0
                );
            } else {
                let error = result.err().unwrap();
                assert!(
                    matches!(error, TwoBitGenerationError::Invalid(actual) if actual == expected)
                );
            }
            assert_eq!(fs::read_dir(scratch.path()).unwrap().count(), 0);
        }
        *store.bad_etag_suffix.lock().unwrap() = None;
        *store.root_read_budget.lock().unwrap() = None;
        // Use the same authenticated source/page fixture with a graph descriptor:
        // semantic decoded-memory admission would reject this small cap earlier.
        let root_body = fs::read(root.join("manifest.json")).unwrap();
        let mut graph_root: serde_json::Value = serde_json::from_slice(&root_body).unwrap();
        graph_root["discovery"] = serde_json::json!({
            "mode": "graph", "centroids_sha256": "a".repeat(64),
            "graph_sha256": "b".repeat(64), "graph_resident_bytes": 1,
            "diverse_graph_sha256": "c".repeat(64), "diverse_graph_resident_bytes": 1,
        });
        let graph_body = serde_json::to_vec(&graph_root).unwrap();
        // Unknown graph lengths join admission before the aggregate budget is checked.
        for name in ["centroids.bin", "graph.bin", "diverse_graph.bin"] {
            store
                .put(&metadata_location(&prefix, name), vec![0].into())
                .await
                .unwrap();
        }
        let cap = graph_body.len() as u64
            + fs::metadata(root.join("page_manifest.json")).unwrap().len()
            + fs::metadata(root.join("page_digests.bin")).unwrap().len()
            - 1;
        let root_key = metadata_location(&prefix, "manifest.json");
        store
            .put(&root_key, graph_body.clone().into())
            .await
            .unwrap();
        let diverse_key = metadata_location(&prefix, "diverse_graph.bin");
        for diverse_present in [true, false] {
            if !diverse_present {
                store.delete(&diverse_key).await.unwrap();
            }
            store.reads.lock().unwrap().clear();
            let error = stage_two_bit_metadata(
                store.as_ref(),
                &prefix,
                &hash(&graph_body),
                cap,
                scratch.path(),
                None,
            )
            .await
            .err()
            .unwrap();
            if diverse_present {
                assert!(
                    matches!(
                        error,
                        ObjectNativeOpenError::Invalid("remote metadata length")
                    ),
                    "{error:?}"
                );
            } else {
                assert!(
                    matches!(
                        &error,
                        ObjectNativeOpenError::Store(object_store::Error::NotFound { path, .. })
                            if path.as_str() == diverse_key.as_ref()
                    ),
                    "{error:?}"
                );
            }
            assert_eq!(
                store
                    .reads
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|(_, head, _, _)| !head)
                    .map(|(name, _, _, _)| name.as_str())
                    .collect::<Vec<_>>(),
                [root_key.as_ref()]
            );
            assert_eq!(fs::read_dir(scratch.path()).unwrap().count(), 0);
        }
        store.put(&root_key, root_body.into()).await.unwrap();
        let eager = TwoBitGeneration::open(&root, &root_sha, limits).unwrap();
        let LoadedDiscovery::Semantic { router, .. } = &eager.discovery else {
            panic!("semantic lost")
        };
        assert!(router.manifest().leaves.len() > 8);
        assert_eq!(router.manifest().final_unit_rows, 1);
        let query = [0.8, 0.35];
        let mut expected_trace = TwoBitPlanTrace::default();
        let (expected_plan, _) = eager.plan_inner(&query, Some(&mut expected_trace)).unwrap();
        let mut actual_trace = TwoBitPlanTrace::default();
        store.reads.lock().unwrap().clear();
        let (plan, _, source, leaves) = lazy
            .plan_paged(store.as_ref(), &query, Some(&mut actual_trace))
            .await
            .unwrap();
        assert_eq!(expected_plan, plan);
        assert_eq!(
            serde_json::to_value(&expected_trace).unwrap(),
            serde_json::to_value(&actual_trace).unwrap()
        );
        assert!(actual_trace.semantic_leaves.len() >= 8);
        assert!(actual_trace.semantic_units.len() > 256);
        assert!(actual_trace.nomination_evaluated_units.iter().all(|unit| {
            actual_trace
                .semantic_units
                .iter()
                .any(|n| n / 8 == unit / 8)
        }));
        let reads = store.reads.lock().unwrap().clone();
        let leaf_reads = reads
            .iter()
            .filter(|(name, head, _, _)| !head && name.ends_with("router/leaves.bin"))
            .collect::<Vec<_>>();
        assert!(leaf_reads.is_empty());
        assert_eq!(leaves, Sq8ReadStats::default());
        assert!(source.submitted_gets <= 128 && source.verified_bytes <= 64 * 1024 * 1024);
        let expected = eager
            .search_with_store(store.as_ref(), &query, 100, None)
            .await
            .unwrap();
        let actual = lazy
            .search_with_store(store.as_ref(), &query, 100, None)
            .await
            .unwrap();
        assert_eq!(expected.ranked.candidates, actual.ranked.candidates);
        *store.root_read_budget.lock().unwrap() = Some((root_key.to_string(), 0));
        let reused =
            TwoBitGeneration::open_remote_from_head(store.as_ref(), &head, limits, scratch.path())
                .await
                .unwrap();
        let reused_result = reused
            .search_with_store(store.as_ref(), &query, 100, None)
            .await
            .unwrap();
        assert_eq!(actual.ranked.candidates, reused_result.ranked.candidates);
        assert_eq!(actual.plan, reused_result.plan);
        assert_eq!(actual.source_stats, reused_result.source_stats);
        assert_eq!(actual.router_stats, reused_result.router_stats);
        store.reads.lock().unwrap().clear();
        assert!(matches!(
            reused
                .search_with_store(store.as_ref(), &query, 100, Some(&[1001]))
                .await
                .err()
                .unwrap(),
            TwoBitGenerationError::Invalid("search admission")
        ));
        assert!(store.reads.lock().unwrap().is_empty());
        drop(reused);
        *store.root_read_budget.lock().unwrap() = None;
        assert_eq!(expected.plan, actual.plan);
        assert_eq!(expected.ranked.stats, actual.ranked.stats);
        assert_eq!(actual.source_stats, source);
        assert_eq!(actual.router_stats, leaves);
        store.reads.lock().unwrap().clear();
        let (diagnosed, diagnosed_trace) = lazy
            .diagnostic_search_with_store(store.as_ref(), &query, 100)
            .await
            .unwrap();
        assert_eq!(diagnosed.ranked.candidates, actual.ranked.candidates);
        assert_eq!(diagnosed.plan, actual.plan);
        assert_eq!(diagnosed.source_stats, source);
        assert_eq!(diagnosed.router_stats, leaves);
        assert_eq!(
            serde_json::to_value(&diagnosed_trace).unwrap(),
            serde_json::to_value(&actual_trace).unwrap()
        );
        assert_eq!(
            store
                .reads
                .lock()
                .unwrap()
                .iter()
                .filter(|(name, head, _, _)| !head && name.ends_with("router/leaves.bin"))
                .count(),
            leaves.submitted_gets
        );

        assert!(actual.stages.discovery.end_ns <= actual.stages.source.start_ns);
        assert!(actual.stages.source.end_ns <= actual.stages.planning.start_ns);
        assert!(actual.stages.planning.end_ns <= actual.stages.sq8.start_ns);
        assert_eq!(actual.stages.leaf_peak_inflight, 0);
        assert!(actual.ranked.candidates.iter().all(|hit| hit.id >= 1001));
        // The public store seam must hold the same generation semaphore.
        store.reads.lock().unwrap().clear();
        {
            let first = lazy.slots.acquire().await.unwrap();
            let second = lazy.slots.acquire().await.unwrap();
            let mut pending =
                std::pin::pin!(lazy.search_with_store(store.as_ref(), &query, 100, None));
            assert!(futures_util::poll!(pending.as_mut()).is_pending());
            assert!(store.reads.lock().unwrap().is_empty());
            drop(first);
            drop(second);
            pending.await.unwrap();
        }
        store.reads.lock().unwrap().clear();
        assert!(
            lazy.search_with_store(store.as_ref(), &query, 0, None)
                .await
                .is_err()
        );
        assert!(
            lazy.search_with_store(store.as_ref(), &query, 100, Some(&[2, 1]))
                .await
                .is_err()
        );
        assert!(store.reads.lock().unwrap().is_empty());
        let invalid = lazy
            .search_with_store(store.as_ref(), &[0., 0.], 100, None)
            .await
            .err()
            .unwrap();
        let stages = invalid.stages().unwrap();
        assert!(
            stages.discovery.start_ns > 0 && stages.discovery.end_ns >= stages.discovery.start_ns
        );
        assert_eq!(stages.source.start_ns, 0);
        assert_eq!(stages.source.end_ns, 0);
        assert_eq!(invalid.router_stats(), Sq8ReadStats::default());
        assert!(store.reads.lock().unwrap().is_empty());
        // Admission precedes SQ8/source submissions excluded by the reduced cap.
        lazy.limits.max_source_bytes = 1;
        store.reads.lock().unwrap().clear();
        let error = lazy
            .search_with_store(store.as_ref(), &query, 100, None)
            .await
            .err()
            .unwrap();
        assert_eq!(error.router_stats(), leaves);
        assert_eq!(
            error.read_stats(),
            (Sq8ReadStats::default(), Sq8ReadStats::default())
        );
        assert!(store.reads.lock().unwrap().is_empty());
        lazy.limits = limits;
        let leaf_key = metadata_location(&prefix, "router/leaves.bin");
        let original = fs::read(root.join("router/leaves.bin")).unwrap();
        let mut corrupt = original.clone();
        corrupt[router.manifest().leaves[actual_trace.semantic_leaves[0]].offset + 4] ^= 1;
        store
            .put(&leaf_key, PutPayload::from(corrupt))
            .await
            .unwrap();
        store.reads.lock().unwrap().clear();
        let unaffected = lazy
            .search_with_store(store.as_ref(), &query, 100, None)
            .await
            .unwrap();
        assert_eq!(unaffected.ranked.candidates, actual.ranked.candidates);
        assert_eq!(unaffected.router_stats, Sq8ReadStats::default());
        assert!(
            store
                .reads
                .lock()
                .unwrap()
                .iter()
                .all(|r| !r.0.ends_with("router/leaves.bin"))
        );
        store.delete(&leaf_key).await.unwrap();
        let absent = TwoBitGeneration::open_remote(
            store.as_ref(),
            &prefix,
            &root_sha,
            limits,
            scratch.path(),
        )
        .await
        .unwrap();
        assert_eq!(absent.remote_open_stats().unwrap().router_head_requests, 0);
        assert_eq!(
            absent
                .search_with_store(store.as_ref(), &query, 100, None)
                .await
                .unwrap()
                .ranked
                .candidates,
            actual.ranked.candidates
        );
        drop(absent);
        store.put(&leaf_key, original.into()).await.unwrap();
        // Memory/pins are rejected before allocating decoded semantic metadata.
        assert!(
            TwoBitGeneration::open(
                &root,
                &root_sha,
                TwoBitGenerationLimits {
                    max_memory_bytes: eager.modeled_memory_bytes - 1,
                    ..limits
                }
            )
            .is_err()
        );
        // Publication revalidates every leaf, including unselected partitions, before HEAD.
        let original = fs::read(root.join("router/leaves.bin")).unwrap();
        let mut corrupt = original.clone();
        *corrupt.last_mut().unwrap() ^= 1;
        fs::write(root.join("router/leaves.bin"), corrupt).unwrap();
        let bad_prefix = ObjectPath::from("semantic/bad");
        assert!(
            crate::two_bit_store::publish_two_bit_generation(
                store.as_ref(),
                &bad_prefix,
                &root,
                &root_sha,
                limits,
                None
            )
            .await
            .is_err()
        );
        assert!(
            crate::two_bit_store::read_two_bit_head(store.as_ref(), &bad_prefix)
                .await
                .unwrap()
                .is_none()
        );
        fs::write(root.join("router/leaves.bin"), original).unwrap();

        use crate::two_bit_compaction::{
            TwoBitCompactionOptions, compact_two_bit_index, compact_two_bit_index_with_discovery,
        };
        use crate::two_bit_mutations::{
            TwoBitMutation, TwoBitMutationLimits, apply_two_bit_mutations,
        };
        let mutation_limits = TwoBitMutationLimits {
            max_snapshot_bytes: 65536,
            max_memory_bytes: 2_000_000,
        };
        let mutations = apply_two_bit_mutations(
            store.as_ref(),
            &head,
            2,
            None,
            &[
                TwoBitMutation {
                    id: 42,
                    vector: Some(vec![1., 0.]),
                },
                TwoBitMutation {
                    id: 1001,
                    vector: None,
                },
                TwoBitMutation {
                    id: 1002,
                    vector: Some(vec![0., 1.]),
                },
            ],
            mutation_limits,
        )
        .await
        .unwrap();
        let mutable = TwoBitGeneration::open_remote(
            store.as_ref(),
            &prefix,
            &root_sha,
            TwoBitGenerationLimits {
                already_pinned_bytes: mutations.resident_payload_bytes() as u64,
                ..limits
            },
            temp.path(),
        )
        .await
        .unwrap();
        let visible = mutable
            .search_with_mutations_store(store.as_ref(), &[1., 0.], 100, &mutations)
            .await
            .unwrap();
        assert!(visible.candidates.iter().any(|h| h.id == 42));
        assert!(visible.candidates.iter().all(|h| h.id != 1001));
        assert_eq!(visible.router_stats, Sq8ReadStats::default());
        let maintenance = temp.path().join("maintenance");
        let options = TwoBitCompactionOptions {
            mutations: mutation_limits,
            source: crate::canonical_source::TwoBitCompactionLimits {
                max_memory_bytes: 128_000_000,
                max_disk_bytes: 128_000_000,
                max_source_chunk_bytes: 1_048_576,
            },
            generation: limits,
        };
        store
            .fail_head
            .store(2, std::sync::atomic::Ordering::SeqCst);
        let failed = compact_two_bit_index(store.clone(), &index, &maintenance, options)
            .await
            .unwrap_err();
        let job_dir = maintenance.join(&root_sha);
        assert!(
            job_dir.join("ready.json").exists(),
            "must fail after durable ready: {failed:?}"
        );
        let job: serde_json::Value =
            serde_json::from_slice(&fs::read(job_dir.join("job.json")).unwrap()).unwrap();
        assert_eq!(job["discovery"], "semantic");
        assert_eq!(job["base_discovery"], "semantic");
        assert!(
            compact_two_bit_index_with_discovery(
                store.clone(),
                &index,
                &maintenance,
                options,
                Some(DiscoveryMode::Graph)
            )
            .await
            .is_err()
        );
        assert_eq!(
            crate::two_bit_store::read_two_bit_head(store.as_ref(), &index)
                .await
                .unwrap()
                .unwrap()
                .root_sha256(),
            root_sha
        );
        let compacted = compact_two_bit_index(store.clone(), &index, &maintenance, options)
            .await
            .unwrap();
        assert_eq!(
            crate::two_bit_store::discovery_mode(store.as_ref(), &compacted)
                .await
                .unwrap(),
            DiscoveryMode::Semantic
        );
        let new_root_key = metadata_location(&compacted.metadata_prefix(), "manifest.json");
        *store.root_read_budget.lock().unwrap() = Some((new_root_key.to_string(), 0));
        let current = TwoBitGeneration::open_remote_from_head(
            store.as_ref(),
            &compacted,
            limits,
            temp.path(),
        )
        .await
        .unwrap();
        assert_reused_root(&current, compacted.retained_root_bytes());
        *store.root_read_budget.lock().unwrap() = None;
        let found = current
            .search_with_store(store.as_ref(), &[1., 0.], 100, None)
            .await
            .unwrap();
        assert!(found.ranked.candidates.iter().any(|h| h.id == 42));
        assert!(found.ranked.candidates.iter().all(|h| h.id != 1001));
        drop(current);
        drop(mutable);
        drop(lazy);
        drop(eager);
        let report = crate::two_bit_gc::collect_two_bit_garbage(
            store.clone(),
            &index,
            &maintenance,
            crate::two_bit_gc::TwoBitGcLimits {
                max_memory_bytes: 4_000_000,
                mutations: mutation_limits,
                max_objects_scanned: 1000,
                max_objects_deleted: 1000,
                max_deleted_object_bytes: 128_000_000,
            },
        )
        .await
        .unwrap();
        assert!(report.scan_complete);
        for name in ROUTER_FILES {
            assert!(
                store
                    .head(&metadata_location(&compacted.metadata_prefix(), name))
                    .await
                    .is_ok()
            );
        }
        assert!(
            store
                .head(&metadata_location(&prefix, "router/leaves.bin"))
                .await
                .is_err()
        );

        // Empty roots preserve concrete semantic mode through deletion and revival.
        let empty_prefix = ObjectPath::from("semantic/empty");
        let empty = crate::two_bit_store::publish_empty_with_mode(
            store.as_ref(),
            &empty_prefix,
            2,
            1,
            None,
            Some(DiscoveryMode::Semantic),
        )
        .await
        .unwrap();
        let directory = temp.path().join("empty-maintenance");
        apply_two_bit_mutations(
            store.as_ref(),
            &empty,
            2,
            None,
            &[TwoBitMutation {
                id: 17,
                vector: Some(vec![1., 0.]),
            }],
            mutation_limits,
        )
        .await
        .unwrap();
        let revived = compact_two_bit_index(store.clone(), &empty_prefix, &directory, options)
            .await
            .unwrap();
        assert!(!revived.is_empty());
        assert_eq!(
            crate::two_bit_store::discovery_mode(store.as_ref(), &revived)
                .await
                .unwrap(),
            DiscoveryMode::Semantic
        );
        apply_two_bit_mutations(
            store.as_ref(),
            &revived,
            2,
            None,
            &[TwoBitMutation {
                id: 17,
                vector: None,
            }],
            mutation_limits,
        )
        .await
        .unwrap();
        let empty = compact_two_bit_index(store.clone(), &empty_prefix, &directory, options)
            .await
            .unwrap();
        assert!(empty.is_empty());
        assert_eq!(
            crate::two_bit_store::discovery_mode(store.as_ref(), &empty)
                .await
                .unwrap(),
            DiscoveryMode::Semantic
        );
        apply_two_bit_mutations(
            store.as_ref(),
            &empty,
            2,
            None,
            &[TwoBitMutation {
                id: 19,
                vector: Some(vec![0., 1.]),
            }],
            mutation_limits,
        )
        .await
        .unwrap();
        // A failed, unready semantic job can explicitly recover in graph mode.
        let failed = compact_two_bit_index_with_discovery(
            store.clone(),
            &empty_prefix,
            &directory,
            TwoBitCompactionOptions {
                source: crate::canonical_source::TwoBitCompactionLimits {
                    max_disk_bytes: 262144,
                    ..options.source
                },
                ..options
            },
            Some(DiscoveryMode::Semantic),
        )
        .await
        .unwrap_err();
        let job_dir = directory.join(empty.root_sha256());
        assert!(!job_dir.join("ready.json").exists());
        assert!(job_dir.join("input").exists(), "{failed:?}");
        let job_path = job_dir.join("job.json");
        let job_bytes = fs::read(&job_path).unwrap();
        let mut job: serde_json::Value = serde_json::from_slice(&job_bytes).unwrap();
        assert_eq!(job["discovery"], "semantic");
        job["mutation_sha256"] = "0".repeat(64).into();
        fs::write(&job_path, serde_json::to_vec(&job).unwrap()).unwrap();
        assert!(
            compact_two_bit_index_with_discovery(
                store.clone(),
                &empty_prefix,
                &directory,
                options,
                Some(DiscoveryMode::Graph),
            )
            .await
            .is_err()
        );
        assert!(job_dir.join("input").exists());
        fs::write(job_path, job_bytes).unwrap();
        let graph = compact_two_bit_index_with_discovery(
            store.clone(),
            &empty_prefix,
            &directory,
            options,
            Some(DiscoveryMode::Graph),
        )
        .await
        .unwrap();
        assert_eq!(
            crate::two_bit_store::discovery_mode(store.as_ref(), &graph)
                .await
                .unwrap(),
            DiscoveryMode::Graph
        );

        // Reject unsupported dimensions before sealing or capturing a job.
        let wide_prefix = ObjectPath::from("semantic/unsupported-dimensions");
        let wide = crate::two_bit_store::publish_empty_with_mode(
            store.as_ref(),
            &wide_prefix,
            1025,
            1,
            None,
            Some(DiscoveryMode::Graph),
        )
        .await
        .unwrap();
        let mutations = apply_two_bit_mutations(
            store.as_ref(),
            &wide,
            1025,
            None,
            &[TwoBitMutation {
                id: 17,
                vector: Some(vec![1.; 1025]),
            }],
            mutation_limits,
        )
        .await
        .unwrap();
        let directory = temp.path().join("wide-maintenance");
        assert!(
            compact_two_bit_index_with_discovery(
                store.clone(),
                &wide_prefix,
                &directory,
                options,
                Some(DiscoveryMode::Semantic),
            )
            .await
            .is_err()
        );
        assert!(!directory.join(wide.root_sha256()).exists());
        apply_two_bit_mutations(
            store.as_ref(),
            &wide,
            1025,
            Some(&mutations),
            &[TwoBitMutation {
                id: 18,
                vector: Some(vec![1.; 1025]),
            }],
            mutation_limits,
        )
        .await
        .expect("unsupported semantic request must leave mutations writable");
    }

    #[tokio::test]
    async fn paged_source_matches_reference_and_preserves_failure_charges() {
        assert_paged_source(513, false, 2, false).await;
    }

    // Catches partial-range hits, unauthenticated insertion, uncharged cache
    // capacity, lost failure drains and cache-dependent nomination/score changes.
    #[tokio::test]
    async fn paged_source_cache_preserves_parity_admission_and_failure_drains() {
        for dimensions in [2, 5] {
            assert_paged_source(513, false, dimensions, true).await;
        }
    }

    #[tokio::test]
    async fn empty_head_reuse_and_coordinated_restart_charge_retained_body() {
        let store = RecordedStore::default();
        let prefix = ObjectPath::from("empty/index");
        let scratch = tempfile::tempdir().unwrap();
        let head =
            crate::two_bit_store::publish_empty_two_bit_generation(&store, &prefix, 2, 1, None)
                .await
                .unwrap();
        let root_key = metadata_location(&head.metadata_prefix(), "manifest.json");
        let mut limits = TwoBitGenerationLimits {
            max_memory_bytes: 128_000_000,
            max_active_queries: 1,
            max_query_bytes: 14,
            max_query_gets: 1,
            max_parallel_gets: 1,
            max_source_bytes: 1024,
            max_source_gets: 1,
            max_parallel_source_gets: 1,
            max_query_scratch_bytes: 400_000,
            already_pinned_bytes: 0,
        };
        limits.max_memory_bytes = head.metadata_prefix().as_ref().len() as u64 * 2
            + 4096
            + 131072
            + head.retained_root_bytes()
            + 2 * 16
            + 400_000
            + 4096;
        store.reads.lock().unwrap().clear();
        *store.root_read_budget.lock().unwrap() = Some((root_key.to_string(), 0));
        let index =
            crate::two_bit_index::TwoBitIndex::open_remote(&store, head, limits, scratch.path())
                .await
                .unwrap();
        assert!(index.head().is_empty());
        assert!(store.reads.lock().unwrap().is_empty());
        drop(index);
        let maintenance = tempfile::tempdir().unwrap();
        for _ in 0..2 {
            store.reads.lock().unwrap().clear();
            *store.root_read_budget.lock().unwrap() = Some((root_key.to_string(), 1));
            let reopened = crate::two_bit_index::TwoBitIndex::open_coordinated(
                &store,
                &prefix,
                maintenance.path(),
                limits,
                scratch.path(),
            )
            .await
            .unwrap();
            assert!(reopened.head().is_empty());
            assert_eq!(
                store
                    .reads
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|read| read.0 == root_key.as_ref())
                    .count(),
                1
            );
            drop(reopened);
        }
        *store.root_read_budget.lock().unwrap() = Some((root_key.to_string(), 1));
        let head = crate::two_bit_store::read_two_bit_head(&store, &prefix)
            .await
            .unwrap()
            .unwrap();
        store.reads.lock().unwrap().clear();
        limits.max_memory_bytes -= 1;
        let error =
            crate::two_bit_index::TwoBitIndex::open_remote(&store, head, limits, scratch.path())
                .await
                .err()
                .unwrap();
        assert!(matches!(
            error,
            crate::two_bit_store::TwoBitStoreError::Invalid("empty index memory")
        ));
        assert!(store.reads.lock().unwrap().is_empty());
        assert_eq!(fs::read_dir(scratch.path()).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn fragmented_paged_source_preserves_trace_and_rank_across_get_caps() {
        assert_paged_source(262_145, true, 2, false).await;
    }

    async fn assert_paged_source(rows: usize, fragmented: bool, dimensions: usize, cache: bool) {
        use object_store::PutPayload;
        use sha2::{Digest, Sha256};
        let hash = |body: &[u8]| format!("{:x}", Sha256::digest(body));
        let temp = tempfile::tempdir().unwrap();
        let raw = (0..rows)
            .flat_map(|id| {
                (0..dimensions)
                    .map(move |d| match d {
                        0 => 1.0_f32 + (id % 7) as f32 * 0.2,
                        _ => 0.1 + (id % 11) as f32 * 0.1,
                    })
                    .flat_map(f32::to_le_bytes)
            })
            .collect::<Vec<_>>();
        let sq8 = (0..rows)
            .flat_map(|id| {
                let mut record = (id as i64).to_le_bytes().to_vec();
                let norm = (1..=dimensions)
                    .map(|value| (value * value) as f32)
                    .sum::<f32>();
                record.extend_from_slice(&norm.to_le_bytes());
                record.extend((0..dimensions).map(|d| (d + 1) as u8));
                record
            })
            .collect::<Vec<_>>();
        let raw_path = temp.path().join("raw");
        let sq8_path = temp.path().join("sq8");
        fs::write(&raw_path, &raw).unwrap();
        fs::write(&sq8_path, &sq8).unwrap();
        let store = RecordedStore::default();
        let sq8_sha = hash(&sq8);
        let key = ObjectPath::from(format!("tenant/objects/{sq8_sha}"));
        store.put(&key, PutPayload::from(sq8)).await.unwrap();
        let etag = store.head(&key).await.unwrap().e_tag.unwrap();
        let root = temp.path().join("generation");
        let root_sha = crate::two_bit_build::TwoBitGenerationBuilder {
            source: crate::two_bit_source::TwoBitSource {
                raw: &raw_path,
                raw_sha256: &hash(&raw),
                sq8: &sq8_path,
                sq8_sha256: &sq8_sha,
                rows,
                dimensions,
            },
            base_epoch: 0,
            generation: 7,
            low: &vec![0.0; dimensions],
            step: &vec![1.0; dimensions],
            sq8_object_key: key.as_ref(),
            sq8_etag: &etag,
        }
        .build(&root, 256_000_000)
        .unwrap();
        let limits = TwoBitGenerationLimits {
            max_memory_bytes: 256_000_000,
            max_active_queries: if cache { 2 } else { 1 },
            max_query_bytes: rows * (dimensions + 12),
            max_query_gets: 32,
            max_parallel_gets: 2,
            max_source_bytes: 64 * 1024 * 1024,
            max_source_gets: 128,
            max_parallel_source_gets: 2,
            max_query_scratch_bytes: 400_000,
            already_pinned_bytes: 0,
        };
        let local = TwoBitGeneration::open(&root, &root_sha, limits).unwrap();
        let prefix = ObjectPath::from(format!("tenant/generations/{root_sha}"));
        for name in METADATA_FILES {
            store
                .put(
                    &metadata_location(&prefix, name),
                    PutPayload::from(fs::read(root.join(name)).unwrap()),
                )
                .await
                .unwrap();
        }
        let mut remote =
            TwoBitGeneration::open_remote(&store, &prefix, &root_sha, limits, temp.path())
                .await
                .unwrap();
        assert!(remote.plane.record(0).is_none());
        let startup = remote.remote_open_stats().unwrap();
        assert_eq!(startup.metadata.len(), 9);
        assert_metadata_waves(
            startup,
            &[
                "manifest.json",
                "page_manifest.json",
                "page_digests.bin",
                "centroids.bin",
                "graph.bin",
                "diverse_graph.bin",
                "plane/manifest.json",
                "plane/mean.bin",
                "plane/page_digests.bin",
            ],
        );
        assert_eq!(startup.source_head_requests, 1);
        assert_eq!(
            startup
                .metadata
                .iter()
                .map(|r| r.logical_head_requests)
                .sum::<u64>(),
            6
        );
        for entry in &startup.metadata {
            if [
                "page_digests.bin",
                "plane/mean.bin",
                "plane/page_digests.bin",
            ]
            .contains(&entry.name.as_str())
            {
                assert_eq!(entry.logical_head_requests, 0);
                assert_eq!(entry.head_wall_ns, 0);
            }
        }
        assert!(
            !startup
                .metadata
                .iter()
                .any(|entry| entry.name == "plane/records.bin")
        );
        let mut query = vec![0.25; dimensions];
        query[0] = 0.5;
        if cache {
            assert_source_cache(local, remote, &store, &query, &root, &prefix, &root_sha).await;
            return;
        }
        let mut expected_trace = TwoBitPlanTrace::default();
        let (expected_plan, _) = local.plan_inner(&query, Some(&mut expected_trace)).unwrap();
        if !fragmented {
            let prepared = local.plane.prepare_query(&query, 400_000).unwrap();
            let normalized = crate::sq8_source::cosine_vector(&query).unwrap();
            for (gets, bytes) in [(32, rows * 14), (1, 256 * 14)] {
                let mut trace = TwoBitPlanTrace::default();
                let walks = local
                    .discover_walks(normalized.as_ref(), Some(&mut trace))
                    .unwrap();
                let diagnostic = plan_two_bit_source_walks(
                    rows,
                    2,
                    rows.div_ceil(256).min(159),
                    None,
                    &walks,
                    &prepared,
                    |row| local.plane.record(row),
                    gets,
                    bytes,
                    Some(&mut trace),
                )
                .unwrap();
                let mut control_limits = local.limits;
                control_limits.max_query_gets = gets;
                control_limits.max_query_bytes = bytes;
                let control = TwoBitGeneration::open(&root, &root_sha, control_limits).unwrap();
                let mut control_trace = TwoBitPlanTrace::default();
                let (control_plan, _) = control
                    .plan_inner(&query, Some(&mut control_trace))
                    .unwrap();
                assert_eq!(diagnostic, control_plan);
                assert_eq!(
                    serde_json::to_value(&trace).unwrap(),
                    serde_json::to_value(&control_trace).unwrap()
                );
                assert_eq!(
                    diagnostic,
                    plan_two_bit_source_walks(
                        rows,
                        2,
                        rows.div_ceil(256).min(159),
                        None,
                        &walks,
                        &prepared,
                        |row| local.plane.record(row),
                        gets,
                        bytes,
                        None,
                    )
                    .unwrap()
                );
            }
        }
        let mut actual_trace = TwoBitPlanTrace::default();
        let (actual_plan, _, source_stats, _) = remote
            .plan_paged(&store, &query, Some(&mut actual_trace))
            .await
            .unwrap();
        assert_eq!(expected_plan, actual_plan);
        assert_eq!(
            serde_json::to_value(&expected_trace).unwrap(),
            serde_json::to_value(actual_trace).unwrap()
        );
        if fragmented {
            assert!(
                source_stats.submitted_gets > 1,
                "fixture must exercise fragmented reads"
            );
            assert!(
                source_stats.verified_bytes < rows * 9,
                "fixture must leave gaps"
            );
            let mut prior_bytes = rows * 9;
            for cap in BTreeSet::from([
                1,
                2,
                source_stats.submitted_gets - 1,
                source_stats.submitted_gets,
            ]) {
                remote.limits.max_source_gets = cap;
                let mut trace = TwoBitPlanTrace::default();
                let (plan, _, stats, _) = remote
                    .plan_paged(&store, &query, Some(&mut trace))
                    .await
                    .unwrap();
                assert_eq!(plan, expected_plan);
                assert_eq!(
                    serde_json::to_value(&trace).unwrap(),
                    serde_json::to_value(&expected_trace).unwrap()
                );
                assert!(stats.submitted_gets <= cap);
                assert!(stats.verified_bytes <= prior_bytes);
                prior_bytes = stats.verified_bytes;
            }
            remote.limits.max_source_gets = limits.max_source_gets;
        } else {
            assert_eq!(source_stats.verified_bytes, rows * 9);
        }
        let expected = local
            .search_with_store(&store, &query, 10, None)
            .await
            .unwrap();
        let actual = remote
            .search_with_store(&store, &query, 10, None)
            .await
            .unwrap();
        assert_eq!(expected.ranked.candidates, actual.ranked.candidates);
        let index = ObjectPath::from("tenant");
        crate::two_bit_store::commit_control(
            &store,
            &index,
            &crate::two_bit_store::HeadBody {
                schema: "borsuk-two-bit-head-v2".into(),
                epoch: 1,
                generation: 7,
                root_sha256: root_sha.clone(),
                mutation: None,
                fence: None,
            },
            None,
        )
        .await
        .unwrap();
        let root_key = metadata_location(&prefix, "manifest.json");
        store.reads.lock().unwrap().clear();
        *store.root_read_budget.lock().unwrap() = Some((root_key.to_string(), 1));
        let head = crate::two_bit_store::read_two_bit_head(&store, &index)
            .await
            .unwrap()
            .unwrap();
        let scratch = tempfile::tempdir().unwrap();
        let reused = TwoBitGeneration::open_remote_from_head(&store, &head, limits, scratch.path())
            .await
            .unwrap();
        assert_reused_root(&reused, head.retained_root_bytes());
        assert_eq!(
            reused.modeled_memory_bytes - remote.modeled_memory_bytes,
            head.retained_root_bytes()
        );
        assert_eq!(
            store
                .reads
                .lock()
                .unwrap()
                .iter()
                .filter(|read| read.0 == root_key.as_ref())
                .count(),
            1
        );
        assert_eq!(fs::read_dir(scratch.path()).unwrap().count(), 0);
        let reused_result = reused
            .search_with_store(&store, &query, 10, None)
            .await
            .unwrap();
        assert_eq!(actual.ranked.candidates, reused_result.ranked.candidates);
        assert_eq!(actual.plan, reused_result.plan);
        remote = reused;
        *store.root_read_budget.lock().unwrap() = None;
        assert_eq!(expected.plan, actual.plan);
        assert_eq!(expected.ranked.stats, actual.ranked.stats);
        assert_eq!(expected.source_stats, Sq8ReadStats::default());
        assert_eq!(actual.source_stats, source_stats);

        if fragmented {
            return;
        }

        remote.limits.max_source_bytes = 1;
        assert!(matches!(
            remote.plan_paged(&store, &query, None).await,
            Err(TwoBitGenerationError::Budget(
                BudgetedPageError::InsufficientBudget
            ))
        ));
        remote.limits.max_source_bytes = limits.max_source_bytes;
        store.delete(&key).await.unwrap();
        let error = remote
            .search_with_store(&store, &query, 10, None)
            .await
            .err()
            .unwrap();
        assert!(
            matches!(&error, TwoBitGenerationError::Query { error, .. } if matches!(error.as_ref(), TwoBitGenerationError::PagedRead { .. }))
        );
        let (source, sq8) = error.read_stats();
        assert_eq!(source, source_stats);
        assert!(sq8.submitted_gets > 0 && sq8.failed_gets == sq8.submitted_gets);

        let source_key = remote.source.as_ref().unwrap().location.clone();
        let mut changed = fs::read(root.join("plane/records.bin")).unwrap();
        *changed.last_mut().unwrap() ^= 1;
        store
            .put(&source_key, PutPayload::from(changed))
            .await
            .unwrap();
        let error = remote.plan_paged(&store, &query, None).await.err().unwrap();
        assert!(matches!(error, TwoBitGenerationError::SourceRead(_)));
        assert_eq!(error.read_stats().1, Sq8ReadStats::default());
        remote.source.as_mut().unwrap().etag =
            store.head(&source_key).await.unwrap().e_tag.unwrap();
        let error = remote.plan_paged(&store, &query, None).await.err().unwrap();
        assert!(matches!(
            error,
            TwoBitGenerationError::SourceRead(RankedSq8Failure {
                error: crate::sq8_s3_range::RangeFetchError::Page(PageError::HashMismatch),
                ..
            })
        ));
        assert!(error.read_stats().0.failed_gets > 0);
    }
    async fn assert_source_cache(
        local: TwoBitGeneration,
        mut remote: TwoBitGeneration,
        store: &RecordedStore,
        query: &[f32],
        root: &Path,
        prefix: &ObjectPath,
        root_sha: &str,
    ) {
        use std::sync::atomic::Ordering;
        let width = remote.plane.receipt().record_bytes;
        let block_bytes = 256 * width;
        let payload = fs::read(root.join("plane/records.bin")).unwrap();
        let expected = local
            .diagnostic_search_with_store(store, query, 10)
            .await
            .unwrap();
        let parity = |actual: &(TwoBitSearchResult, TwoBitPlanTrace)| {
            assert_eq!(actual.0.plan, expected.0.plan);
            assert_eq!(
                serde_json::to_value(&actual.1).unwrap(),
                serde_json::to_value(&expected.1).unwrap()
            );
            let bits = |result: &TwoBitSearchResult| {
                result
                    .ranked
                    .candidates
                    .iter()
                    .map(|hit| (hit.ordinal, hit.id, hit.score.to_bits()))
                    .collect::<Vec<_>>()
            };
            assert_eq!(bits(&actual.0), bits(&expected.0));
            assert_eq!(actual.0.ranked.stats, expected.0.ranked.stats);
            assert_eq!(actual.0.router_stats, expected.0.router_stats);
        };
        let source_reads = || {
            store
                .reads
                .lock()
                .unwrap()
                .iter()
                .filter(|read| !read.1 && read.0.ends_with("/plane/records.bin"))
                .map(|read| read.2.clone())
                .collect::<Vec<_>>()
        };
        let uncached_bytes = remote.modeled_memory_bytes();
        for _ in 0..2 {
            let actual = remote
                .diagnostic_search_with_store(store, query, 10)
                .await
                .unwrap();
            parity(&actual);
            assert_eq!(
                actual.0.source_stats,
                Sq8ReadStats {
                    submitted_gets: 1,
                    verified_bytes: payload.len(),
                    failed_gets: 0,
                }
            );
            assert_eq!(remote.source_cache_stats(), SourceCacheStats::default());
        }
        remote = remote.with_source_cache(1_048_576).unwrap();
        let capacity = remote.source_cache_stats();
        assert_eq!(capacity.capacity_bytes, 3 * block_bytes);
        assert_eq!(capacity.occupied_bytes, 0);
        assert_eq!(
            remote.modeled_memory_bytes(),
            uncached_bytes + capacity.charged_bytes as u64
        );
        assert!(capacity.charged_bytes <= 1_048_576);
        let one_slot = capacity.charged_bytes - 2 * (block_bytes + std::mem::size_of::<usize>());

        // Check memory and cache-cap admission after open, before any query I/O.
        let mut rejected = TwoBitGeneration::open_remote(
            store,
            prefix,
            root_sha,
            remote.limits,
            root.parent().unwrap(),
        )
        .await
        .unwrap();
        rejected.limits.max_memory_bytes = uncached_bytes + capacity.charged_bytes as u64 - 1;
        store.reads.lock().unwrap().clear();
        assert!(rejected.with_source_cache(1_048_576).is_err());
        assert!(store.reads.lock().unwrap().is_empty());
        let rejected = TwoBitGeneration::open_remote(
            store,
            prefix,
            root_sha,
            remote.limits,
            root.parent().unwrap(),
        )
        .await
        .unwrap();
        store.reads.lock().unwrap().clear();
        assert!(rejected.with_source_cache(one_slot - 1).is_err());
        assert!(store.reads.lock().unwrap().is_empty());

        for phase in ["empty", "full", "mixed", "eviction"] {
            if phase == "mixed" || phase == "eviction" {
                remote = remote
                    .with_source_cache(if phase == "mixed" {
                        1_048_576
                    } else {
                        one_slot
                    })
                    .unwrap();
                let (ranges, stats) = remote
                    .fetch_source_ranges(store, &[(16, 16)])
                    .await
                    .unwrap();
                assert_eq!(stats.verified_bytes, width);
                assert_eq!(ranges[0].bytes.as_ref(), &payload[2 * block_bytes..]);
                assert_eq!(remote.source_cache_stats().occupied_bytes, width);
            }
            store.reads.lock().unwrap().clear();
            let actual = remote
                .diagnostic_search_with_store(store, query, 10)
                .await
                .unwrap();
            parity(&actual);
            assert!(actual.0.stages.source.end_ns >= actual.0.stages.source.start_ns);
            if phase == "full" {
                assert_eq!(actual.0.source_stats, Sq8ReadStats::default());
                assert!(source_reads().is_empty());
                assert_eq!(
                    (
                        remote.source_cache_stats().hits,
                        remote.source_cache_stats().misses
                    ),
                    (1, 1)
                );
                let (ranges, stats) = remote.fetch_source_ranges(store, &[(0, 16)]).await.unwrap();
                assert_eq!(stats, Sq8ReadStats::default());
                assert!(source_reads().is_empty());
                assert_eq!(ranges.len(), 1);
                assert_eq!(ranges[0].start, 0);
                assert_eq!(ranges[0].bytes.as_ref(), payload.as_slice());
            } else {
                // Even one cached tail block cannot fragment the original range.
                assert_eq!(source_reads(), vec![0..payload.len() as u64]);
                assert_eq!(
                    actual.0.source_stats,
                    Sq8ReadStats {
                        submitted_gets: 1,
                        verified_bytes: payload.len(),
                        failed_gets: 0,
                    }
                );
            }
            let stats = remote.source_cache_stats();
            if phase == "eviction" {
                assert_eq!(stats.occupied_bytes, width);
                assert_eq!(stats.evictions, 3);
                let (_, stats) = remote.fetch_source_ranges(store, &[(0, 7)]).await.unwrap();
                assert_eq!(stats.verified_bytes, block_bytes);
                assert_eq!(remote.source_cache_stats().evictions, 4);
            } else {
                assert_eq!(stats.occupied_bytes, payload.len());
            }
        }
        // A hit in the later original range skips only that range; output is
        // reassembled in source order even though the earlier range arrives later.
        remote = remote.with_source_cache(1_048_576).unwrap();
        remote
            .fetch_source_ranges(store, &[(16, 16)])
            .await
            .unwrap();
        store.reads.lock().unwrap().clear();
        let (ranges, stats) = remote
            .fetch_source_ranges(store, &[(0, 7), (16, 16)])
            .await
            .unwrap();
        assert_eq!(
            stats,
            Sq8ReadStats {
                submitted_gets: 1,
                verified_bytes: block_bytes,
                failed_gets: 0,
            }
        );
        assert_eq!(source_reads(), vec![0..block_bytes as u64]);
        assert_eq!((ranges[0].start, ranges[1].start), (0, 2 * block_bytes));
        assert_eq!(ranges[0].bytes.as_ref(), &payload[..block_bytes]);
        assert_eq!(ranges[1].bytes.as_ref(), &payload[2 * block_bytes..]);
        assert_eq!(
            (
                remote.source_cache_stats().hits,
                remote.source_cache_stats().misses
            ),
            (1, 2)
        );

        // Full-cache hits still undergo the original full cover admission.
        remote = remote.with_source_cache(1_048_576).unwrap();
        remote
            .diagnostic_search_with_store(store, query, 10)
            .await
            .unwrap();
        let source_cap = remote.limits.max_source_bytes;
        remote.limits.max_source_bytes = payload.len() - 1;
        store.reads.lock().unwrap().clear();
        assert!(
            remote
                .search_with_store(store, query, 10, None)
                .await
                .is_err()
        );
        assert!(store.reads.lock().unwrap().is_empty());
        remote.limits.max_source_bytes = source_cap;

        // Retain a full-block hit while another task overwrites its only slot.
        // The cloned in-memory transport shares the original authenticated object.
        let shared = std::sync::Arc::new(remote.with_source_cache(one_slot).unwrap());
        shared.fetch_source_ranges(store, &[(0, 7)]).await.unwrap();
        assert_eq!(shared.source_cache_stats().capacity_bytes, block_bytes);
        assert_ne!(
            &payload[..block_bytes],
            &payload[block_bytes..2 * block_bytes]
        );
        store.reads.lock().unwrap().clear();
        let (hit_ready, wait_for_hit) = tokio::sync::oneshot::channel::<()>();
        let colliding_generation = shared.clone();
        let colliding_store = store.inner.clone();
        let eviction = tokio::spawn(async move {
            wait_for_hit.await.unwrap();
            let before = colliding_generation.source_cache_stats();
            assert_eq!((before.hits, before.misses, before.evictions), (1, 1, 0));
            colliding_generation
                .fetch_source_ranges(&colliding_store, &[(8, 15)])
                .await
                .unwrap()
        });
        let (retained, hit_stats) = shared.fetch_source_ranges(store, &[(0, 7)]).await.unwrap();
        assert_eq!(hit_stats, Sq8ReadStats::default());
        assert!(source_reads().is_empty());
        assert_eq!(retained.len(), 1);
        assert_eq!(retained[0].start, 0);
        assert_eq!(retained[0].bytes.as_ref(), &payload[..block_bytes]);
        hit_ready.send(()).unwrap();
        let (colliding, miss_stats) = eviction.await.unwrap();
        assert_eq!(
            miss_stats,
            Sq8ReadStats {
                submitted_gets: 1,
                verified_bytes: block_bytes,
                failed_gets: 0,
            }
        );
        assert_eq!(colliding.len(), 1);
        assert_eq!(colliding[0].start, block_bytes);
        assert_eq!(
            colliding[0].bytes.as_ref(),
            &payload[block_bytes..2 * block_bytes]
        );
        let after = shared.source_cache_stats();
        assert_eq!((after.hits, after.misses, after.evictions), (1, 2, 1));
        assert_eq!(after.occupied_bytes, block_bytes);
        assert_eq!(retained[0].bytes.as_ref(), &payload[..block_bytes]);
        remote = std::sync::Arc::try_unwrap(shared).ok().unwrap();

        // Owned query copies survive concurrent lookup/insertion/eviction.
        for budget in [one_slot, 1_048_576] {
            remote = remote.with_source_cache(budget).unwrap();
            store.reads.lock().unwrap().clear();
            let (a, b) = tokio::join!(
                remote.diagnostic_search_with_store(store, query, 10),
                remote.diagnostic_search_with_store(store, query, 10),
            );
            let (a, b) = (a.unwrap(), b.unwrap());
            parity(&a);
            parity(&b);
            assert_eq!(
                a.0.source_stats.submitted_gets + b.0.source_stats.submitted_gets,
                source_reads().len()
            );
            assert_eq!(
                a.0.source_stats.verified_bytes + b.0.source_stats.verified_bytes,
                source_reads().len() * payload.len()
            );
            assert!(
                remote.source_cache_stats().occupied_bytes
                    <= remote.source_cache_stats().capacity_bytes
            );
        }

        for fault in ["etag", "corrupt", "delayed"] {
            remote = remote.with_source_cache(1_048_576).unwrap();
            remote.fetch_source_ranges(store, &[(8, 15)]).await.unwrap();
            *store.source_fault.lock().unwrap() = Some((0, fault));
            store.source_completed.store(0, Ordering::SeqCst);
            store.reads.lock().unwrap().clear();
            let error = remote
                .fetch_source_ranges(store, &[(0, 7), (16, 16)])
                .await
                .err()
                .unwrap();
            assert_eq!(
                error.read_stats(),
                (
                    Sq8ReadStats {
                        submitted_gets: 2,
                        verified_bytes: width,
                        failed_gets: 1,
                    },
                    Sq8ReadStats::default()
                )
            );
            assert_eq!(
                source_reads(),
                vec![
                    0..block_bytes as u64,
                    (2 * block_bytes) as u64..payload.len() as u64
                ]
            );
            assert_eq!(
                store.source_completed.load(Ordering::SeqCst),
                if fault == "etag" { 1 } else { 2 }
            );
            assert_eq!(remote.source_cache_stats().occupied_bytes, block_bytes);
            store.reads.lock().unwrap().clear();
            let error = remote
                .search_with_store(store, query, 10, None)
                .await
                .err()
                .unwrap();
            assert_eq!(
                error.read_stats(),
                (
                    Sq8ReadStats {
                        submitted_gets: 1,
                        verified_bytes: 0,
                        failed_gets: 1,
                    },
                    Sq8ReadStats::default()
                )
            );
            assert!(
                store
                    .reads
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|read| read.0.ends_with("/plane/records.bin"))
            );
            assert!(
                error.stages().unwrap().source.end_ns >= error.stages().unwrap().source.start_ns
            );
            assert_eq!(remote.source_cache_stats().occupied_bytes, block_bytes);
            *store.source_fault.lock().unwrap() = None;
            let (ranges, stats) = remote
                .fetch_source_ranges(store, &[(0, 7), (16, 16)])
                .await
                .unwrap();
            assert_eq!(
                stats.submitted_gets, 2,
                "failed batches must not poison or populate entries"
            );
            assert_eq!(ranges[0].bytes.as_ref(), &payload[..block_bytes]);
            assert_eq!(ranges[1].bytes.as_ref(), &payload[2 * block_bytes..]);
            let actual = remote
                .diagnostic_search_with_store(store, query, 10)
                .await
                .unwrap();
            parity(&actual);
            assert_eq!(actual.0.source_stats, Sq8ReadStats::default());
        }

        // An old pinned cache remains charged when another generation opens.
        let pins = remote.modeled_memory_bytes();
        let mut next = TwoBitGeneration::open_remote(
            store,
            prefix,
            root_sha,
            TwoBitGenerationLimits {
                already_pinned_bytes: pins,
                ..remote.limits
            },
            root.parent().unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(next.modeled_memory_bytes(), uncached_bytes + pins);
        next.limits.max_memory_bytes = uncached_bytes + pins + capacity.charged_bytes as u64 - 1;
        store.reads.lock().unwrap().clear();
        assert!(next.with_source_cache(1_048_576).is_err());
        assert!(store.reads.lock().unwrap().is_empty());
        let next = TwoBitGeneration::open_remote(
            store,
            prefix,
            root_sha,
            TwoBitGenerationLimits {
                already_pinned_bytes: pins,
                max_memory_bytes: uncached_bytes + pins + capacity.charged_bytes as u64,
                ..remote.limits
            },
            root.parent().unwrap(),
        )
        .await
        .unwrap()
        .with_source_cache(1_048_576)
        .unwrap();
        assert_eq!(
            next.modeled_memory_bytes(),
            uncached_bytes + pins + capacity.charged_bytes as u64
        );
        let actual = next
            .diagnostic_search_with_store(store, query, 10)
            .await
            .unwrap();
        parity(&actual);
        assert_eq!(
            actual.0.source_stats.submitted_gets, 1,
            "reopening must start empty"
        );
        remote = remote.with_source_cache(0).unwrap();
        assert_eq!(remote.modeled_memory_bytes(), uncached_bytes);
        assert_eq!(remote.source_cache_stats(), SourceCacheStats::default());
        let actual = remote
            .diagnostic_search_with_store(store, query, 10)
            .await
            .unwrap();
        parity(&actual);
        assert_eq!(actual.0.source_stats.submitted_gets, 1);
    }

    #[test]
    fn diagnostic_page_limit_admits_short_closure_without_relaxing_control() {
        let codec = crate::rotated_two_bit::RotatedTwoBitCodec::new(&[0.0; 2], 20260923).unwrap();
        let prepared = codec.prepare_query(&[1.0, 0.0], 400_000).unwrap();
        let record = codec.encode(&[1.0, 0.0]).unwrap();
        let walks = [(2, vec![16])];
        assert!(rank_walked_source(513, &walks, |_| Ok(1.0)).is_err());
        let mut trace = TwoBitPlanTrace::default();
        let plan = plan_two_bit_source_walks(
            513,
            2,
            1,
            Some(SemanticProfile::Native100k),
            &walks,
            &prepared,
            |_| Some(&record),
            32,
            513 * 14,
            Some(&mut trace),
        )
        .unwrap();
        assert_eq!(plan.selected_pages, vec![2]);
        assert_eq!(plan.ranges, vec![7168..7182]);
        assert_eq!(trace.primary_page, 2);
        assert_eq!(trace.ranked_candidate_pages, vec![2]);
        assert_eq!(trace.nomination_evaluated_units, vec![16]);
        for limit in [0, 3, 4, 160] {
            assert!(
                plan_two_bit_source_walks(
                    513,
                    2,
                    limit,
                    Some(SemanticProfile::Native100k),
                    &walks,
                    &prepared,
                    |_| Some(&record),
                    32,
                    513 * 14,
                    None,
                )
                .is_err()
            );
        }
    }

    #[test]
    fn fresh48_source_walk_completes_512_pages_once_and_rejects_excess_before_io() {
        let original = std::iter::once(1)
            .chain((1..512).flat_map(|page| (0..6).map(move |offset| page * 8 + offset)))
            .chain((1..6).map(|page| page * 8 + 6))
            .collect::<BTreeSet<_>>();
        let (seed, units, additions) =
            crate::semantic_unit_router::seed_walk(&original, 1_000_000, SemanticProfile::Fresh1m)
                .unwrap();
        assert_eq!(units.len(), 3079);
        let walks = [(seed, units.clone())];
        assert!(rank_walked_source(1_000_000, &walks, |_| panic!("graph cap relaxed")).is_err());
        let mut seen = BTreeSet::new();
        let ranked = rank_walked_source_with_limit(
            1_000_000,
            &walks,
            512,
            Some(SemanticProfile::Fresh1m),
            |unit| {
                assert!(seen.insert(unit), "source unit scored twice");
                Ok(if unit == 4095 { 10. } else { 1. })
            },
        )
        .unwrap();
        assert_eq!(seen, (0..4096).collect());
        assert_eq!(ranked.len(), 512);
        assert_eq!(ranked[0], (511, 10.));
        let codec = crate::rotated_two_bit::RotatedTwoBitCodec::new(&[0.; 768], 0).unwrap();
        let prepared = codec.prepare_query(&[1.; 768], 400_000).unwrap();
        let record = codec.encode(&[1.; 768]).unwrap();
        let mut trace = TwoBitPlanTrace {
            semantic_leaves: (0..48).collect(),
            semantic_units: original.into_iter().collect(),
            semantic_seed_additions: additions,
            ..Default::default()
        };
        let plan = plan_two_bit_source_walks(
            1_000_000,
            768,
            512,
            Some(SemanticProfile::Fresh1m),
            &walks,
            &prepared,
            |_| Some(&record),
            32,
            16_773_120,
            Some(&mut trace),
        )
        .unwrap();
        assert_eq!(trace.nomination_evaluated_units.len(), 4096);
        assert_eq!(
            trace
                .nomination_evaluated_units
                .iter()
                .copied()
                .collect::<BTreeSet<_>>(),
            seen
        );
        assert_eq!(trace.ranked_candidate_pages, (0..512).collect::<Vec<_>>());
        assert_eq!(
            plan.ranges.iter().map(|range| range.len()).sum::<usize>(),
            84 * 256 * 780
        );
        let retained = std::mem::size_of::<TwoBitPlanTrace>()
            + (trace.nomination_evaluated_units.capacity()
                + trace.ranked_candidate_pages.capacity()
                + trace.semantic_leaves.capacity()
                + trace.semantic_units.capacity()
                + trace.semantic_seed_additions.capacity())
                * std::mem::size_of::<usize>();
        assert!(retained <= TwoBitPlanTrace::scratch_bytes(1_000_000));
        let mut excess = units.clone();
        excess.remove(excess.iter().position(|&unit| unit == 8).unwrap());
        excess.push(512 * 8);
        assert_eq!(excess.len(), 3079);
        assert!(
            admit_source_walks(
                1_000_000,
                &[(0, excess.clone())],
                Some(SemanticProfile::Fresh1m)
            )
            .is_err()
        );
        for (limit, invalid) in [
            (512, excess.clone()),
            (513, excess),
            (512, (0..3080).collect()),
        ] {
            assert!(
                plan_two_bit_source_walks(
                    1_000_000,
                    768,
                    limit,
                    Some(SemanticProfile::Fresh1m),
                    &[(0, invalid)],
                    &prepared,
                    |_| panic!("rejected closure read SOURCE"),
                    32,
                    16_773_120,
                    None,
                )
                .is_err()
            );
        }
        assert!(
            plan_two_bit_source_walks(
                1_000_000,
                768,
                512,
                None,
                &walks,
                &prepared,
                |_| panic!("graph cap relaxed"),
                32,
                16_773_120,
                None,
            )
            .is_err()
        );
        assert!(
            plan_two_bit_source_walks(
                100_000,
                768,
                512,
                Some(SemanticProfile::Fresh1m),
                &walks,
                &prepared,
                |_| panic!("wrong profile read SOURCE"),
                32,
                16_773_120,
                None,
            )
            .is_err()
        );
    }

    #[test]
    fn diagnostic_normalization_preserves_native_query_space_and_rejections() {
        let unit = [1.0_f32, 0.0];
        assert!(matches!(
            normalize_two_bit_diagnostic_query(&unit).unwrap(),
            Cow::Borrowed(_)
        ));
        assert_eq!(
            normalize_two_bit_diagnostic_query(&[3.0, 4.0])
                .unwrap()
                .as_ref(),
            &[0.6, 0.8]
        );
        for query in [
            &[][..],
            &[0.0, 0.0],
            &[f32::NAN, 1.0],
            &[f32::INFINITY, 1.0],
        ] {
            assert!(matches!(
                normalize_two_bit_diagnostic_query(query),
                Err(TwoBitGenerationError::Plane(_))
            ));
        }
    }

    #[test]
    fn diagnostic_source_cover_charges_bridges_and_partial_tail_before_scoring() {
        let closure = BTreeSet::from([0, 2]);
        assert_eq!(
            plan_two_bit_source_cover(&closure, 513, 9, 2, 2313).unwrap(),
            (vec![0..2304, 4608..4617], 2313)
        );
        assert_eq!(
            plan_two_bit_source_cover(&closure, 513, 9, 1, 4617).unwrap(),
            (std::iter::once(0..4617).collect::<Vec<_>>(), 4617)
        );
        assert!(matches!(
            plan_two_bit_source_cover(&closure, 513, 9, 1, 4616),
            Err(TwoBitGenerationError::Budget(
                BudgetedPageError::InsufficientBudget
            ))
        ));
        assert!(plan_two_bit_source_cover(&BTreeSet::from([3]), 513, 9, 2, 4617).is_err());
    }
    #[test]
    fn diagnostic_source_walks_reject_missing_nonfinite_and_invalid_inputs() {
        use crate::rotated_two_bit::{RotatedTwoBitCodec, TwoBitError};
        let codec = RotatedTwoBitCodec::new(&[0.0; 2], 20260923).unwrap();
        let prepared = codec.prepare_query(&[1.0, 0.0], 400_000).unwrap();
        let record = codec.encode(&[1.0, 0.0]).unwrap();
        let walks = [(0, (0..9).collect())];
        assert!(matches!(
            plan_two_bit_source_walks(
                257,
                2,
                2,
                None,
                &walks,
                &prepared,
                |_| None,
                32,
                257 * 14,
                None
            ),
            Err(TwoBitGenerationError::Invalid("missing source record"))
        ));
        let mut nonfinite = record.clone();
        let scale = nonfinite.len() - 8;
        nonfinite[scale..scale + 4].copy_from_slice(&f32::NAN.to_le_bytes());
        assert!(matches!(
            plan_two_bit_source_walks(
                257,
                2,
                2,
                None,
                &walks,
                &prepared,
                |_| Some(&nonfinite),
                32,
                257 * 14,
                None,
            ),
            Err(TwoBitGenerationError::Plane(SourceBuildError::Codec(
                TwoBitError::Record
            )))
        ));
        for invalid in [
            vec![],
            vec![(0, vec![0]); 3],
            vec![(2, (0..9).collect())],
            vec![(0, vec![0, 0])],
            vec![(0, (0..10).collect())],
            vec![(0, vec![0, 1])],
        ] {
            assert!(
                plan_two_bit_source_walks(
                    257,
                    2,
                    2,
                    None,
                    &invalid,
                    &prepared,
                    |_| panic!("invalid walk scored"),
                    32,
                    257 * 14,
                    None,
                )
                .is_err()
            );
        }
        for (rows, dimensions) in [(0, 2), (257, 0), (usize::MAX, 2), (257, usize::MAX)] {
            assert!(
                plan_two_bit_source_walks(
                    rows,
                    dimensions,
                    rows.div_ceil(256).min(159),
                    None,
                    &walks,
                    &prepared,
                    |_| panic!("invalid geometry scored"),
                    32,
                    257 * 14,
                    None,
                )
                .is_err()
            );
        }
    }
    #[test]
    fn bounded_completion_recovers_unvisited_rows_without_duplicate_or_extra_work() {
        let mut units = (0..1272).collect::<Vec<_>>();
        units[1271] = 1280;
        let mut seen = std::collections::BTreeSet::new();
        let ranked = rank_walked_source(100000, &[(0, units)], |unit| {
            assert!(seen.insert(unit), "source unit scored twice");
            Ok(if unit == 1281 { 10.0 } else { 1.0 })
        })
        .unwrap();
        assert_eq!(ranked[0], (160, 10.0), "incomplete page hid its best row");
        assert_eq!(seen.len(), 1280);

        let walk = |start: usize| {
            (0..8)
                .chain((start..start + 1264).map(|page| page * 8))
                .collect::<Vec<_>>()
        };
        let mut seen = std::collections::BTreeSet::new();
        let ranked = rank_walked_source(1000000, &[(0, walk(1)), (0, walk(1265))], |unit| {
            assert!(seen.insert(unit), "source unit scored twice");
            assert!(seen.len() <= 2544, "source row allowance exceeded");
            Ok(if unit == 9 { 10.0 } else { 1.0 })
        })
        .unwrap();
        assert_eq!(seen.len(), 2544);
        assert_eq!(ranked[0], (1, 10.0));
        assert!(ranked.len() <= 318);
    }
    #[test]
    fn source_nomination_precedes_centroid_cutoff_and_scores_units_once() {
        let mut units = (0..1272).collect::<Vec<_>>();
        units[1271] = 1280;
        let mut reverse = units.clone();
        reverse.reverse();
        let mut calls = 0;
        let ranked = rank_walked_source(100000, &[(0, units), (0, reverse)], |unit| {
            calls += 1;
            Ok(if unit == 1280 { 10.0 } else { 1.0 })
        })
        .expect("walked source nomination not integrated");
        assert_eq!(calls, 1280);
        assert_eq!(ranked.len(), 159);
        assert_eq!(ranked[0], (160, 10.0));
        assert_eq!(ranked[1], (0, 1.0));
        assert!(!ranked.iter().any(|&(page, _)| page == 158));
    }
    #[test]
    fn source_nomination_handles_one_graph_partial_rows_and_rejects_invalid_walks() {
        let mut rows_scored = 0;
        let ranked = rank_walked_source(257, &[(1, (0..9).collect())], |unit| {
            rows_scored += ((unit + 1) * 32).min(257) - unit * 32;
            Ok(if unit == 8 { 2.0 } else { 1.0 })
        })
        .unwrap();
        assert_eq!(ranked, vec![(1, 2.0), (0, 1.0)]);
        assert_eq!(rows_scored, 257);
        assert_eq!(
            rank_walked_source(1, &[(0, vec![0])], |_| Ok(3.0)).unwrap(),
            vec![(0, 3.0)]
        );
        for (rows, walks) in [
            (0, vec![(0, vec![0])]),
            (257, vec![]),
            (1, vec![(0, vec![0]); 3]),
            (257, vec![(2, vec![0])]),
            (257, vec![(0, vec![0, 0])]),
            (257, vec![(0, vec![0, 9])]),
            (257, vec![(0, vec![0, 1])]),
            (100000, vec![(0, (0..1273).collect())]),
        ] {
            assert!(rank_walked_source(rows, &walks, |_| panic!("invalid walk scored")).is_err());
        }
        assert!(rank_walked_source(1, &[(0, vec![0])], |_| Ok(f64::NAN)).is_err());
    }

    // ===== Direct-closure candidate: actual builder/store/planner/scorer fixtures =====
    use crate::exact_sq8_nominee::ScoredNominee;
    use crate::sq8_s3_range::{
        RangeFetchError as DirectFetchError,
        cold_http_fixture::{ETAG as DIRECT_ETAG, Fixture as DirectHttp, Request as DirectRequest},
    };
    use std::time::{Duration, Instant};

    // Independent smallest-gap cover: bridge the earliest smallest gap until at most
    // `max_gets` runs remain. Page intervals become byte ranges clipped at the object end.
    fn smallest_gap_cover(
        pages: &BTreeSet<usize>,
        rows: usize,
        row_bytes: usize,
        max_gets: usize,
    ) -> Vec<std::ops::Range<usize>> {
        let mut runs: Vec<(usize, usize)> = Vec::new();
        for &page in pages {
            if let Some(run) = runs.last_mut()
                && run.1 == page
            {
                run.1 += 1;
                continue;
            }
            runs.push((page, page + 1));
        }
        while runs.len() > max_gets {
            let (index, _) = (0..runs.len() - 1)
                .map(|i| (i, runs[i + 1].0 - runs[i].1))
                .min_by_key(|&(i, gap)| (gap, i))
                .unwrap();
            runs[index].1 = runs[index + 1].1;
            runs.remove(index + 1);
        }
        runs.iter()
            .map(|&(first, end)| first * 256 * row_bytes..(end * 256).min(rows) * row_bytes)
            .collect()
    }

    // The native cosine contract: f64 squared norm, f32 quotient, borrowed when already unit.
    fn cosine_unit(query: &[f32]) -> Vec<f32> {
        let squared = query.iter().map(|&x| f64::from(x).powi(2)).sum::<f64>();
        if (squared - 1.).abs() <= 1e-6 {
            return query.to_vec();
        }
        let norm = squared.sqrt();
        query
            .iter()
            .map(|&x| (f64::from(x) / norm) as f32)
            .collect()
    }

    // Sequential scalar f32 reference over complete rows of `sq8` (no production scorer,
    // no blocked lanes): score = norm - 2 * (sum code * (q * step) + (sum q * low - |q|^2 / 2)).
    fn scalar_rank(
        sq8: &[u8],
        dimensions: usize,
        rows: impl Iterator<Item = usize>,
        unit_query: &[f32],
        low: &[f32],
        step: &[f32],
    ) -> Vec<(usize, i64, f32)> {
        let mut shift = 0_f32;
        let mut qnorm = 0_f32;
        for d in 0..dimensions {
            shift += unit_query[d] * low[d];
            qnorm += unit_query[d] * unit_query[d];
        }
        shift -= qnorm / 2.0;
        let width = dimensions + 12;
        let mut scored = Vec::new();
        for ordinal in rows {
            let row = &sq8[ordinal * width..(ordinal + 1) * width];
            let id = i64::from_le_bytes(row[..8].try_into().unwrap());
            let norm = f32::from_le_bytes(row[8..12].try_into().unwrap());
            let mut inner = 0_f32;
            for d in 0..dimensions {
                inner += f32::from(row[12 + d]) * (unit_query[d] * step[d]);
            }
            scored.push((ordinal, id, norm - 2.0 * (inner + shift)));
        }
        scored.sort_by(|a, b| a.2.total_cmp(&b.2).then(a.1.cmp(&b.1)));
        scored
    }

    // The same score as plain geometry, in f64: stored norm - 2 q.x + |q|^2 for
    // x = low + code * step. This pins the meaning of the f32 reference above.
    fn geometric_score(
        sq8: &[u8],
        dimensions: usize,
        ordinal: usize,
        unit_query: &[f32],
        low: &[f32],
        step: &[f32],
    ) -> f64 {
        let width = dimensions + 12;
        let row = &sq8[ordinal * width..(ordinal + 1) * width];
        let norm = f64::from(f32::from_le_bytes(row[8..12].try_into().unwrap()));
        let (mut dot, mut qq) = (0_f64, 0_f64);
        for d in 0..dimensions {
            let x = f64::from(low[d]) + f64::from(row[12 + d]) * f64::from(step[d]);
            dot += f64::from(unit_query[d]) * x;
            qq += f64::from(unit_query[d]).powi(2);
        }
        norm - 2. * dot + qq
    }

    fn assert_hits_match(hits: &[ScoredNominee], oracle: &[(usize, i64, f32)]) {
        assert!(hits.len() <= oracle.len());
        for (rank, (hit, want)) in hits.iter().zip(oracle).enumerate() {
            assert_eq!(
                (hit.ordinal, hit.id, hit.score.to_bits()),
                (want.0, want.1, want.2.to_bits()),
                "rank {rank}"
            );
        }
    }

    fn assert_geometry_agrees(
        sq8: &[u8],
        dimensions: usize,
        oracle: &[(usize, i64, f32)],
        unit_query: &[f32],
        low: &[f32],
        step: &[f32],
    ) {
        for want in oracle.iter().take(8) {
            let geometric = geometric_score(sq8, dimensions, want.0, unit_query, low, step);
            assert!(
                (f64::from(want.2) - geometric).abs() < 1e-3,
                "ordinal {}: {} vs {geometric}",
                want.0,
                want.2
            );
        }
    }

    // True when equal scores are ordered by ID against ordinal order somewhere.
    fn has_id_tie_against_ordinal_order(oracle: &[(usize, i64, f32)]) -> bool {
        oracle
            .windows(2)
            .any(|w| w[0].2.to_bits() == w[1].2.to_bits() && w[0].0 > w[1].0 && w[0].1 < w[1].1)
    }

    #[test]
    fn direct_bridge_counterexample_changes_population_and_winner_pure() {
        use crate::exact_sq8_nominee::Sq8Geometry;
        use crate::returned_sq8::{ReturnedRange, rank_returned_ranges};
        let dimensions = 7;
        let row_bytes = dimensions + 12;
        // Sixteen pages whose last page keeps only 100 rows.
        let rows = 15 * 256 + 100;
        let page_bytes = 256 * row_bytes;
        let end = rows * row_bytes;
        let first = BTreeSet::from([0, 5, 15]);
        let second = BTreeSet::from([0, 5, 8, 11, 15]);
        let (a, a_bytes) = cover_pages(&first, rows, row_bytes, 256, 2).unwrap();
        let (b, b_bytes) = cover_pages(&second, rows, row_bytes, 256, 2).unwrap();
        // [0,6) + [15,16) versus [0,1) + [5,16): the shorter final page is clipped.
        assert_eq!(a, vec![0..6 * page_bytes, 15 * page_bytes..end]);
        assert_eq!(b, vec![0..page_bytes, 5 * page_bytes..end]);
        assert_eq!(a_bytes, 6 * page_bytes + end - 15 * page_bytes);
        assert_eq!(b_bytes, page_bytes + end - 5 * page_bytes);
        assert_eq!(smallest_gap_cover(&first, rows, row_bytes, 2), a);
        assert_eq!(smallest_gap_cover(&second, rows, row_bytes, 2), b);
        // The larger required set loses the earlier incidental pages 1..=4.
        for page in 1..=4 {
            let span = page * page_bytes..(page + 1) * page_bytes;
            assert!(a.iter().any(|r| r.start <= span.start && span.end <= r.end));
            assert!(b.iter().all(|r| r.end <= span.start || span.end <= r.start));
        }
        // One GET covers everything between the first and last required page.
        assert_eq!(
            cover_pages(&second, rows, row_bytes, 256, 1).unwrap().0,
            vec![0..end]
        );

        // Page 2 holds the only rows that can win: tiny norms against 40+ elsewhere.
        let low = vec![0_f32; dimensions];
        let step = vec![0.5_f32; dimensions];
        let mut object = Vec::new();
        for row in 0..rows {
            let id = 9_000 - ((row * 37 + 11) % rows) as i64;
            let norm = if row / 256 == 2 {
                0.25 + (row % 5) as f32 * 0.125
            } else {
                40. + (row % 5) as f32 * 0.5
            };
            object.extend_from_slice(&id.to_le_bytes());
            object.extend_from_slice(&norm.to_le_bytes());
            object.extend((0..dimensions).map(|d| 1 + ((row + d) % 3) as u8));
        }
        let query = (0..dimensions)
            .map(|d| 1. + 0.25 * d as f32)
            .collect::<Vec<_>>();
        let unit = cosine_unit(&query);
        let geometry = Sq8Geometry { rows, dimensions };
        let mut top = Vec::new();
        for (cover, bytes) in [(&a, a_bytes), (&b, b_bytes)] {
            let population = cover
                .iter()
                .map(|r| ReturnedRange {
                    start: r.start,
                    bytes: &object[r.clone()],
                })
                .collect::<Vec<_>>();
            let production =
                rank_returned_ranges(geometry, &population, &unit, &low, &step, 12, bytes).unwrap();
            let fetched = cover
                .iter()
                .flat_map(|r| r.start / row_bytes..r.end / row_bytes)
                .collect::<Vec<_>>();
            assert_eq!(fetched.len(), bytes / row_bytes);
            let oracle = scalar_rank(&object, dimensions, fetched.into_iter(), &unit, &low, &step);
            assert_hits_match(&production, &oracle);
            assert_geometry_agrees(&object, dimensions, &oracle, &unit, &low, &step);
            top.push(production);
        }
        // The first cover's best twelve are all page-2 rows; the second cover cannot see them.
        assert!(top[0].iter().all(|hit| (512..768).contains(&hit.ordinal)));
        assert!(top[1].iter().all(|hit| !(512..768).contains(&hit.ordinal)));
        assert!(top[0][11].score < top[1][0].score);
        assert!(top[0].iter().all(|a| top[1].iter().all(|b| a.id != b.id)));
    }

    struct ClassFixture {
        temp: tempfile::TempDir,
        root_sha: String,
        sq8: Vec<u8>,
        key: ObjectPath,
        prefix: ObjectPath,
        objects: std::collections::BTreeMap<String, Vec<u8>>,
        limits: TwoBitGenerationLimits,
        rows: usize,
        dimensions: usize,
    }

    // The cold-fetch class construction (32 orthogonal directions on eight nonadjacent
    // pages each) so semantic discovery selects a gapped closure; only application IDs
    // (a permutation unrelated to ordinals) and stored norms (period-7 ties) differ.
    fn class_fixture(dimensions: usize) -> ClassFixture {
        use sha2::{Digest, Sha256};
        assert!(dimensions >= 32);
        let hash = |body: &[u8]| format!("{:x}", Sha256::digest(body));
        let rows = 65_536;
        let temp = tempfile::tempdir().unwrap();
        let mut raw = Vec::with_capacity(rows * dimensions * 4);
        let mut sq8 = Vec::with_capacity(rows * (dimensions + 12));
        for row in 0..rows {
            let page = row / 256;
            let class = (page / 8 + 4 * (page % 8)) % 32;
            let id = 100_000 + ((row * 40_503 + 12_345) % 65_537) as i64;
            let norm = 1. + (row % 7) as f32 * 0.25;
            sq8.extend_from_slice(&id.to_le_bytes());
            sq8.extend_from_slice(&norm.to_le_bytes());
            for d in 0..dimensions {
                let value = if d != class {
                    0_f32
                } else if class & 4 == 0 {
                    1.
                } else {
                    -1.
                };
                raw.extend_from_slice(&value.to_le_bytes());
                sq8.push((value + 1.) as u8);
            }
        }
        let raw_path = temp.path().join("raw");
        let sq8_path = temp.path().join("sq8");
        fs::write(&raw_path, &raw).unwrap();
        fs::write(&sq8_path, &sq8).unwrap();
        let sq8_sha = hash(&sq8);
        let key = ObjectPath::from(format!("direct/objects/{sq8_sha}"));
        let root = temp.path().join("generation");
        let order = (0..rows as u64).collect::<Vec<_>>();
        let root_sha = crate::two_bit_build::TwoBitGenerationBuilder {
            source: crate::two_bit_source::TwoBitSource {
                raw: &raw_path,
                raw_sha256: &hash(&raw),
                sq8: &sq8_path,
                sq8_sha256: &sq8_sha,
                rows,
                dimensions,
            },
            base_epoch: 0,
            generation: 1,
            low: &vec![-1.; dimensions],
            step: &vec![1.; dimensions],
            sq8_object_key: key.as_ref(),
            sq8_etag: DIRECT_ETAG,
        }
        .build_with_discovery(
            Some(&order),
            DiscoveryMode::Semantic,
            &root,
            128 * 1024 * 1024,
        )
        .unwrap();
        drop(order);
        drop(raw);
        let limits = TwoBitGenerationLimits {
            max_memory_bytes: 256 * 1024 * 1024,
            max_active_queries: 1,
            max_query_bytes: 32 * 256 * (dimensions + 12),
            max_query_gets: 32,
            max_parallel_gets: 16,
            max_source_bytes: 64 * 1024 * 1024,
            max_source_gets: 128,
            max_parallel_source_gets: 16,
            max_query_scratch_bytes: 1_048_576 + TwoBitPlanTrace::scratch_bytes(rows),
            already_pinned_bytes: 0,
        };
        let prefix = ObjectPath::from(format!("direct/generations/{root_sha}"));
        let mut objects = std::collections::BTreeMap::from([(key.to_string(), sq8.clone())]);
        for name in [
            "manifest.json",
            "page_manifest.json",
            "page_digests.bin",
            "plane/manifest.json",
            "plane/mean.bin",
            "plane/page_digests.bin",
            "plane/records.bin",
            "router/root.bin",
            "router/membership.bin",
            "router/leaves.bin",
        ] {
            objects.insert(
                metadata_location(&prefix, name).to_string(),
                fs::read(root.join(name)).unwrap(),
            );
        }
        ClassFixture {
            temp,
            root_sha,
            sq8,
            key,
            prefix,
            objects,
            limits,
            rows,
            dimensions,
        }
    }

    // Positive weights 0.5 + 0.25 * (d % 4) on exactly the sixteen positive classes pick
    // the nearest leaves; the unequal weights and nonunit norm keep the query away from
    // any axis. Coordinates beyond the class directions only dilute it. Native100k keeps
    // the 8 nearest leaves plus those of the next 8 within 1.15x the 8th squared
    // distance, so the selected count is not a cap of 16 but 12 here: with prototypes
    // +e_c, the squared distances are 2 - 2 * q_c = 1.320 (weight 1.25, four classes),
    // 1.456 (1.0, four), 1.592 (0.75, four) and 1.728 (0.5, four); the 8th distance
    // 1.456 bounds 1.674, which admits the 1.592 group and excludes the 1.728 group.
    fn class_query(dimensions: usize) -> Vec<f32> {
        (0..dimensions)
            .map(|d| {
                if d >= 32 {
                    0.125
                } else if d & 4 == 0 {
                    0.5 + 0.25 * (d % 4) as f32
                } else {
                    0.
                }
            })
            .collect()
    }

    // `Fixture::arm(None)` holds SQ8 bodies until released: release them once any is in
    // flight. A SOURCE or leaf GET from the work would stay held and fail loudly.
    async fn run_direct_http<T>(
        fixture: &DirectHttp,
        work: impl std::future::Future<Output = T>,
    ) -> T {
        fixture.arm(None);
        let (value, ()) = tokio::join!(work, async {
            fixture.wait(|s| s.active[1] >= 1).await;
            fixture.release(1);
        });
        value
    }

    // Same, for the historical path that also reads SOURCE: release both stages in order.
    async fn run_source_and_sq8_http<T>(
        fixture: &DirectHttp,
        work: impl std::future::Future<Output = T>,
    ) -> T {
        fixture.arm(None);
        let (value, ()) = tokio::join!(work, async {
            fixture.wait(|s| s.active[0] >= 1).await;
            fixture.release(0);
            fixture.wait(|s| s.active[1] >= 1).await;
            fixture.release(1);
        });
        value
    }

    #[tokio::test]
    async fn native_direct_closure_cold_oracle_zero_source_memory_and_caps() {
        let deadline = Instant::now() + Duration::from_secs(90);
        tokio::time::timeout(Duration::from_secs(90), async {
            let ClassFixture {
                temp,
                root_sha,
                sq8,
                key,
                prefix,
                objects,
                limits,
                rows,
                dimensions,
            } = class_fixture(32);
            assert!(Instant::now() < deadline, "fixture construction deadline");
            let row_bytes = dimensions + 12;
            let key_string = key.to_string();
            let fixture = DirectHttp::new(objects, deadline);
            let mut generation = TwoBitGeneration::open_remote(
                fixture.reader.store(),
                &prefix,
                &root_sha,
                limits,
                temp.path(),
            )
            .await
            .unwrap();
            // Startup SOURCE HEAD is accounted separately from query GETs.
            assert_eq!(
                generation.remote_open_stats().unwrap().source_head_requests,
                1
            );
            assert!(
                fixture
                    .snapshot()
                    .requests
                    .iter()
                    .any(|(path, head, _, _)| { *head && path.ends_with("/plane/records.bin") })
            );
            let store = fixture.reader.store();
            let query = class_query(dimensions);
            let unit = cosine_unit(&query);
            let (low, step) = (vec![-1_f32; dimensions], vec![1_f32; dimensions]);

            // Ordinary calls stay historical: SOURCE nomination, clamped SQ8 limits.
            let ordinary = run_source_and_sq8_http(
                &fixture,
                generation.search_with_store(store, &query, 10, None),
            )
            .await
            .unwrap();
            assert!(ordinary.source_stats.submitted_gets > 0);
            assert!(!ordinary.source_nomination_skipped);
            assert!(ordinary.ranked.stats.submitted_gets <= limits.max_query_gets);
            assert!(!fixture.snapshot().requests.is_empty());
            let ordinary_bits = ordinary
                .ranked
                .candidates
                .iter()
                .map(|h| (h.ordinal, h.id, h.score.to_bits()))
                .collect::<Vec<_>>();

            // Direct search: current discovery, checked closure, deterministic cover.
            let dlimits = DirectClosureLimits {
                max_sq8_bytes: rows * row_bytes,
                max_sq8_gets: 32,
            };
            let before = fixture.reader.transport_stats();
            let (result, trace) = run_direct_http(
                &fixture,
                generation.diagnostic_direct_closure_search_with_store(store, &query, 50, dlimits),
            )
            .await
            .unwrap();
            let state = fixture.snapshot();
            // ZERO SOURCE and leaf GETs: every request after arming is an SQ8 range.
            assert!(
                state.requests.iter().all(|(path, head, range, etag)| {
                    *path == key_string
                        && !*head
                        && range.is_some()
                        && etag.as_deref() == Some(DIRECT_ETAG)
                }),
                "{:?}",
                state.requests
            );
            assert_eq!(state.errors, Vec::<String>::new());
            // Required closure from the production discovery trace; independent cover.
            let closure = trace
                .semantic_units
                .iter()
                .map(|unit| unit / 8)
                .collect::<BTreeSet<_>>();
            // Twelve leaves (see `class_query`), each a full 64-unit leaf of the 2,048 units.
            assert_eq!(trace.semantic_leaves.len(), 12);
            assert_eq!(trace.semantic_units.len(), 12 * 64);
            assert!(result.source_nomination_skipped);
            assert!(trace.ranked_candidate_pages.is_empty());
            assert!(trace.nomination_evaluated_units.is_empty());
            assert!(trace.discoveries.is_empty());
            assert_eq!(
                result.plan.selected_pages,
                closure.iter().copied().collect::<Vec<_>>()
            );
            assert_eq!(result.plan.target_pages, closure.len());
            assert_eq!(
                (
                    result.plan.target_shortfall,
                    result.plan.primary_pages_retained
                ),
                (0, 0)
            );
            let cover = smallest_gap_cover(&closure, rows, row_bytes, dlimits.max_sq8_gets);
            assert_eq!(result.plan.ranges, cover);
            let planned = cover.iter().map(|r| r.len()).sum::<usize>();
            assert_eq!(result.plan.planned_bytes, planned);
            let covered_pages = planned / (256 * row_bytes);
            assert!(closure.len() < covered_pages && covered_pages < rows / 256);
            assert!(2 < cover.len() && cover.len() <= dlimits.max_sq8_gets);
            assert!(cover.windows(2).all(|w| w[0].end < w[1].start));
            // Full ordered range identity on the wire, not just totals.
            let mut actual = state.requests.clone();
            actual.sort();
            let mut expected = cover
                .iter()
                .map(|r| -> DirectRequest {
                    (
                        key_string.clone(),
                        false,
                        Some((r.start, r.end)),
                        Some(DIRECT_ETAG.into()),
                    )
                })
                .collect::<Vec<_>>();
            expected.sort();
            assert_eq!(actual, expected);
            assert_eq!(
                result.ranked.stats,
                Sq8ReadStats {
                    submitted_gets: cover.len(),
                    verified_bytes: planned,
                    failed_gets: 0
                }
            );
            assert_eq!(result.source_stats, Sq8ReadStats::default());
            assert_eq!(result.router_stats, Sq8ReadStats::default());
            let after = fixture.reader.transport_stats();
            assert_eq!(after.attempts - before.attempts, cover.len() as u64);
            assert_eq!(
                after.consumed_payload_bytes - before.consumed_payload_bytes,
                planned as u64
            );
            let stages = result.stages;
            assert_eq!((stages.source.start_ns, stages.source.end_ns), (0, 0));
            assert!(
                0 < stages.discovery.start_ns
                    && stages.discovery.start_ns <= stages.discovery.end_ns
            );
            assert!(stages.discovery.end_ns <= stages.planning.start_ns);
            assert!(stages.planning.start_ns <= stages.planning.end_ns);
            assert!(stages.planning.end_ns <= stages.sq8.start_ns);
            assert!(stages.sq8.start_ns <= stages.sq8.end_ns);
            assert_eq!(stages.leaf_peak_inflight, 0);
            assert_eq!(generation.slots.available_permits(), 1);

            // Independent sequential f32 reference over every row of the fetched ranges.
            let fetched = state
                .requests
                .iter()
                .flat_map(|(_, _, range, _)| {
                    let (start, end) = range.unwrap();
                    start / row_bytes..end / row_bytes
                })
                .collect::<Vec<_>>();
            assert_eq!(fetched.len(), planned / row_bytes);
            let oracle = scalar_rank(&sq8, dimensions, fetched.into_iter(), &unit, &low, &step);
            assert_eq!(result.ranked.candidates.len(), 50);
            assert_hits_match(&result.ranked.candidates, &oracle);
            assert_geometry_agrees(&sq8, dimensions, &oracle, &unit, &low, &step);
            assert!(has_id_tie_against_ordinal_order(&oracle[..50]));
            let direct_bits = result
                .ranked
                .candidates
                .iter()
                .map(|h| (h.ordinal, h.id, h.score.to_bits()))
                .collect::<Vec<_>>();

            // Exact modeled memory, from independent arithmetic.
            let memory = generation.direct_closure_memory(dlimits).unwrap();
            let planner = 1_048_576
                + TwoBitPlanTrace::scratch_bytes(rows) as u64
                + 4 * dimensions as u64
                + 512 * rows.div_ceil(256) as u64;
            let ranking =
                256 * rows.min(dlimits.max_sq8_bytes / row_bytes) as u64 + 4 * dimensions as u64;
            let direct = (3 * dlimits.max_sq8_bytes as u64 + planner + ranking)
                * limits.max_active_queries as u64;
            assert_eq!(memory.resident_bytes, generation.modeled_memory_bytes());
            assert_eq!(memory.direct_planner_bytes, planner);
            assert_eq!(memory.direct_query_bytes, direct);
            assert_eq!(memory.total_bytes, memory.resident_bytes + direct);
            assert_eq!(memory.cap_bytes, limits.max_memory_bytes);

            // Cap minus one is refused before any I/O with a machine-readable reason;
            // the exact cap is admitted and reproduces the same ranking.
            let refused = |error: &TwoBitGenerationError, reason: DirectClosureRejection| {
                assert_eq!(error.direct_resource_rejection(), Some(reason), "{error:?}");
            };
            fixture.arm(None);
            let attempts = fixture.reader.transport_stats().attempts;
            generation.limits.max_memory_bytes = memory.total_bytes - 1;
            let error = generation
                .direct_closure_search_with_store(store, &query, 50, None, dlimits)
                .await
                .err()
                .unwrap();
            refused(&error, DirectClosureRejection::ModeledMemory);
            assert!(fixture.snapshot().requests.is_empty());
            assert_eq!(fixture.reader.transport_stats().attempts, attempts);
            assert_eq!(generation.slots.available_permits(), 1);
            generation.limits.max_memory_bytes = memory.total_bytes;
            let exact = run_direct_http(
                &fixture,
                generation.direct_closure_search_with_store(store, &query, 50, None, dlimits),
            )
            .await
            .unwrap();
            assert_eq!(
                exact
                    .ranked
                    .candidates
                    .iter()
                    .map(|h| (h.ordinal, h.id, h.score.to_bits()))
                    .collect::<Vec<_>>(),
                direct_bits
            );
            generation.limits.max_memory_bytes = limits.max_memory_bytes;

            // Planned-byte cap minus one: refused after local discovery, before any GET.
            fixture.arm(None);
            let error = generation
                .direct_closure_search_with_store(
                    store,
                    &query,
                    50,
                    None,
                    DirectClosureLimits {
                        max_sq8_bytes: planned - 1,
                        ..dlimits
                    },
                )
                .await
                .err()
                .unwrap();
            refused(&error, DirectClosureRejection::PlannedBytes);
            assert!(fixture.snapshot().requests.is_empty());
            assert_eq!(generation.slots.available_permits(), 1);
            assert!(error.stages().is_some_and(|s| s.discovery.end_ns > 0));
            assert_eq!(
                error.read_stats(),
                (Sq8ReadStats::default(), Sq8ReadStats::default())
            );

            // A one-GET allowance bridges everything between the first and last
            // required page: more bytes, and a byte cap that is just short is refused.
            let one_get = smallest_gap_cover(&closure, rows, row_bytes, 1);
            assert_eq!(one_get.len(), 1);
            let one_bytes = one_get[0].len();
            assert!(one_bytes > planned);
            let single = run_direct_http(
                &fixture,
                generation.direct_closure_search_with_store(
                    store,
                    &query,
                    50,
                    None,
                    DirectClosureLimits {
                        max_sq8_bytes: one_bytes,
                        max_sq8_gets: 1,
                    },
                ),
            )
            .await
            .unwrap();
            assert_eq!(single.plan.ranges, one_get);
            assert_eq!(single.plan.selected_pages, result.plan.selected_pages);
            assert_eq!(single.ranked.stats.submitted_gets, 1);
            let single_oracle = scalar_rank(
                &sq8,
                dimensions,
                one_get[0].start / row_bytes..one_get[0].end / row_bytes,
                &unit,
                &low,
                &step,
            );
            assert_hits_match(&single.ranked.candidates, &single_oracle);
            fixture.arm(None);
            let error = generation
                .direct_closure_search_with_store(
                    store,
                    &query,
                    50,
                    None,
                    DirectClosureLimits {
                        max_sq8_bytes: one_bytes - 1,
                        max_sq8_gets: 1,
                    },
                )
                .await
                .err()
                .unwrap();
            refused(&error, DirectClosureRejection::PlannedBytes);
            assert!(fixture.snapshot().requests.is_empty());

            // Exclusions use the historical roster rule and keep the rest of the ranking.
            let mut pinned_limits = limits;
            pinned_limits.already_pinned_bytes = 64;
            let roster_generation = TwoBitGeneration::open_remote(
                store,
                &prefix,
                &root_sha,
                pinned_limits,
                temp.path(),
            )
            .await
            .unwrap();
            let mut excluded = oracle[..3].iter().map(|r| r.1).collect::<Vec<_>>();
            excluded.sort_unstable();
            let filtered = run_direct_http(
                &fixture,
                roster_generation.direct_closure_search_with_store(
                    store,
                    &query,
                    50,
                    Some(&excluded),
                    dlimits,
                ),
            )
            .await
            .unwrap();
            let visible = oracle
                .iter()
                .filter(|r| !excluded.contains(&r.1))
                .copied()
                .collect::<Vec<_>>();
            assert_hits_match(&filtered.ranked.candidates, &visible);
            assert_eq!(filtered.ranked.candidates.len(), 50);
            fixture.arm(None);
            for bad in [vec![5, 3], vec![3, 3], vec![1; 9]] {
                let error = roster_generation
                    .direct_closure_search_with_store(store, &query, 50, Some(&bad), dlimits)
                    .await
                    .err()
                    .unwrap();
                assert!(matches!(
                    error,
                    TwoBitGenerationError::Invalid("search admission")
                ));
                assert_eq!(error.direct_resource_rejection(), None);
            }
            assert!(fixture.snapshot().requests.is_empty());

            // Direct caps are independent of the historical clamp: a generation whose
            // ordinary limits admit one page and one GET still serves the same query.
            let narrow = TwoBitGeneration::open_remote(
                store,
                &prefix,
                &root_sha,
                TwoBitGenerationLimits {
                    max_query_bytes: 256 * row_bytes,
                    max_query_gets: 1,
                    ..limits
                },
                temp.path(),
            )
            .await
            .unwrap();
            let narrow_result = run_direct_http(
                &fixture,
                narrow.direct_closure_search_with_store(store, &query, 50, None, dlimits),
            )
            .await
            .unwrap();
            assert_eq!(narrow_result.plan, result.plan);
            assert_eq!(
                narrow_result
                    .ranked
                    .candidates
                    .iter()
                    .map(|h| (h.ordinal, h.id, h.score.to_bits()))
                    .collect::<Vec<_>>(),
                direct_bits
            );

            // A second generation opened while the direct-enabled first stays pinned
            // charges the first's whole direct-enabled model before its own.
            let pinned = TwoBitGeneration::open_remote(
                store,
                &prefix,
                &root_sha,
                TwoBitGenerationLimits {
                    already_pinned_bytes: memory.total_bytes,
                    ..limits
                },
                temp.path(),
            )
            .await
            .unwrap();
            assert_eq!(
                pinned.modeled_memory_bytes(),
                generation.modeled_memory_bytes() + memory.total_bytes
            );
            let second = pinned.direct_closure_memory(dlimits).unwrap();
            assert_eq!(
                second.total_bytes,
                memory.resident_bytes + memory.total_bytes + memory.direct_query_bytes
            );
            assert!(
                TwoBitGeneration::open_remote(
                    store,
                    &prefix,
                    &root_sha,
                    TwoBitGenerationLimits {
                        max_memory_bytes: pinned.modeled_memory_bytes() - 1,
                        already_pinned_bytes: memory.total_bytes,
                        ..limits
                    },
                    temp.path(),
                )
                .await
                .is_err()
            );
            let mut pinned = pinned;
            pinned.limits.max_memory_bytes = second.total_bytes - 1;
            fixture.arm(None);
            let error = pinned
                .direct_closure_search_with_store(store, &query, 50, None, dlimits)
                .await
                .err()
                .unwrap();
            refused(&error, DirectClosureRejection::ModeledMemory);
            assert!(fixture.snapshot().requests.is_empty());
            pinned.limits.max_memory_bytes = second.total_bytes;
            let admitted = run_direct_http(
                &fixture,
                pinned.direct_closure_search_with_store(store, &query, 50, None, dlimits),
            )
            .await
            .unwrap();
            assert_eq!(admitted.plan, result.plan);

            // Historical search is unchanged after all of the above.
            let again = run_source_and_sq8_http(
                &fixture,
                generation.search_with_store(store, &query, 10, None),
            )
            .await
            .unwrap();
            assert_eq!(again.plan, ordinary.plan);
            assert_eq!(again.source_stats, ordinary.source_stats);
            assert_eq!(again.ranked.stats, ordinary.ranked.stats);
            assert_eq!(
                again
                    .ranked
                    .candidates
                    .iter()
                    .map(|h| (h.ordinal, h.id, h.score.to_bits()))
                    .collect::<Vec<_>>(),
                ordinary_bits
            );
            fixture.finish();
        })
        .await
        .expect("whole direct closure cold fixture deadline");
    }

    #[tokio::test]
    async fn native_direct_closure_real_planner_bridge_counterexample_changes_winner() {
        let deadline = Instant::now() + Duration::from_secs(90);
        tokio::time::timeout(Duration::from_secs(90), async {
            let ClassFixture {
                temp,
                root_sha,
                sq8,
                key,
                prefix,
                objects,
                limits,
                rows,
                dimensions,
            } = class_fixture(32);
            let row_bytes = dimensions + 12;
            let page_bytes = 256 * row_bytes;
            let key_string = key.to_string();
            let fixture = DirectHttp::new(objects, deadline);
            let generation = TwoBitGeneration::open_remote(
                fixture.reader.store(),
                &prefix,
                &root_sha,
                limits,
                temp.path(),
            )
            .await
            .unwrap();
            let store = fixture.reader.store();
            let dlimits = DirectClosureLimits {
                max_sq8_bytes: rows * row_bytes,
                max_sq8_gets: 2,
            };
            // Class 12 occupies page 3 here, and no other class-12 page is in either cover.
            let mut query = vec![0_f32; dimensions];
            query[3] = 0.25;
            query[12] = -1.5;
            query[13] = 0.5;
            let unit = cosine_unit(&query);
            let normalized = normalize_two_bit_diagnostic_query(&query).unwrap();
            assert_eq!(&*normalized, unit.as_slice());
            let (low, step) = (vec![-1_f32; dimensions], vec![1_f32; dimensions]);
            let page_units = |pages: &[usize]| {
                pages
                    .iter()
                    .flat_map(|&page| page * 8..page * 8 + 8)
                    .collect::<Vec<_>>()
            };
            let mut tops = Vec::new();
            for (pages, ranges, bridges) in [
                (
                    vec![0_usize, 5, 15],
                    vec![0..6 * page_bytes, 15 * page_bytes..16 * page_bytes],
                    vec![1_usize, 2, 3, 4],
                ),
                (
                    vec![0, 5, 8, 11, 15],
                    vec![0..page_bytes, 5 * page_bytes..16 * page_bytes],
                    vec![6, 7, 9, 10, 12, 13, 14],
                ),
            ] {
                // The real admission path: checked walks -> closure -> deterministic cover.
                let walks = vec![(0_usize, page_units(&pages))];
                let plan = generation.direct_closure_plan(&walks, dlimits).unwrap();
                assert_eq!(plan.selected_pages, pages);
                assert_eq!(plan.target_pages, pages.len());
                assert_eq!(plan.ranges, ranges);
                assert_eq!(
                    plan.ranges,
                    smallest_gap_cover(
                        &pages.iter().copied().collect(),
                        rows,
                        row_bytes,
                        dlimits.max_sq8_gets
                    )
                );
                assert_eq!(
                    plan.planned_bytes,
                    ranges.iter().map(|r| r.len()).sum::<usize>()
                );
                let covered = plan.planned_bytes / page_bytes;
                assert_eq!(covered - pages.len(), bridges.len());
                for page in 0..16 {
                    let inside = plan
                        .ranges
                        .iter()
                        .any(|r| r.start <= page * page_bytes && (page + 1) * page_bytes <= r.end);
                    assert_eq!(inside, pages.contains(&page) || bridges.contains(&page));
                }
                let ranked = run_direct_http(
                    &fixture,
                    generation.direct_closure_rank(
                        store,
                        &normalized,
                        &plan,
                        20,
                        None,
                        dlimits,
                        None,
                    ),
                )
                .await
                .unwrap();
                let state = fixture.snapshot();
                let mut actual = state.requests.clone();
                actual.sort();
                assert_eq!(
                    actual,
                    plan.ranges
                        .iter()
                        .map(|r| -> DirectRequest {
                            (
                                key_string.clone(),
                                false,
                                Some((r.start, r.end)),
                                Some(DIRECT_ETAG.into()),
                            )
                        })
                        .collect::<Vec<_>>()
                );
                let fetched = plan
                    .ranges
                    .iter()
                    .flat_map(|r| r.start / row_bytes..r.end / row_bytes)
                    .collect::<Vec<_>>();
                let oracle = scalar_rank(&sq8, dimensions, fetched.into_iter(), &unit, &low, &step);
                assert_eq!(ranked.candidates.len(), 20);
                assert_hits_match(&ranked.candidates, &oracle);
                assert_geometry_agrees(&sq8, dimensions, &oracle, &unit, &low, &step);
                assert_eq!(
                    ranked.stats,
                    Sq8ReadStats {
                        submitted_gets: 2,
                        verified_bytes: plan.planned_bytes,
                        failed_gets: 0
                    }
                );
                tops.push(ranked.candidates);
            }
            // Page 3 (bridge in the first cover only) supplies every first-cover winner and
            // none of the second cover's: the larger required set is not a recall superset.
            assert!(tops[0].iter().all(|hit| (768..1024).contains(&hit.ordinal)));
            assert!(
                tops[1]
                    .iter()
                    .all(|hit| !(768..1024).contains(&hit.ordinal))
            );
            assert!(tops[0][19].score < tops[1][0].score);
            assert!(tops[0].iter().all(|a| tops[1].iter().all(|b| a.id != b.id)));
            fixture.finish();
        })
        .await
        .expect("whole bridge counterexample fixture deadline");
    }

    #[tokio::test]
    async fn native_direct_closure_shares_the_query_slot_with_ordinary_search_and_drains() {
        let deadline = Instant::now() + Duration::from_secs(90);
        tokio::time::timeout(Duration::from_secs(90), async {
            let ClassFixture {
                temp,
                root_sha,
                key,
                prefix,
                objects,
                limits,
                rows,
                dimensions,
                ..
            } = class_fixture(32);
            let row_bytes = dimensions + 12;
            let key_string = key.to_string();
            let fixture = DirectHttp::new(objects, deadline);
            let generation = TwoBitGeneration::open_remote(
                fixture.reader.store(),
                &prefix,
                &root_sha,
                limits,
                temp.path(),
            )
            .await
            .unwrap();
            let store = fixture.reader.store();
            let query = class_query(dimensions);
            let dlimits = DirectClosureLimits {
                max_sq8_bytes: rows * row_bytes,
                max_sq8_gets: 32,
            };
            let is_source = |path: &String| path.ends_with("/plane/records.bin");

            // Direct first: its held SQ8 bodies keep the only slot; a queued ordinary
            // search issues no SOURCE GET until the direct query has fully finished.
            fixture.arm(None);
            let mut direct = Box::pin(generation.direct_closure_search_with_store(
                store, &query, 10, None, dlimits,
            ));
            tokio::select! {
                _ = &mut direct => panic!("direct returned with its SQ8 bodies held"),
                () = fixture.wait(|s| s.active[1] >= 1) => {}
            }
            assert_eq!(generation.slots.available_permits(), 0);
            let mut ordinary = Box::pin(generation.search_with_store(store, &query, 10, None));
            tokio::select! {
                _ = &mut ordinary => panic!("ordinary search bypassed the held direct slot"),
                _ = &mut direct => panic!("direct returned with its SQ8 bodies held"),
                () = tokio::time::sleep(Duration::from_millis(50)) => {}
            }
            assert_eq!(generation.slots.available_permits(), 0);
            assert!(fixture.snapshot().requests.iter().all(|(path, _, _, _)| !is_source(path)));
            fixture.release(1);
            let direct = direct.await.unwrap();
            assert_eq!(direct.source_stats, Sq8ReadStats::default());
            let (ordinary, ()) = tokio::join!(ordinary, async {
                fixture.wait(|s| s.active[0] >= 1).await;
                fixture.release(0);
            });
            let ordinary = ordinary.unwrap();
            assert!(ordinary.source_stats.submitted_gets > 0);
            assert_eq!(generation.slots.available_permits(), 1);

            // Ordinary first: a queued direct search issues no SQ8 GET while SOURCE is held.
            fixture.arm(None);
            let mut ordinary = Box::pin(generation.search_with_store(store, &query, 10, None));
            tokio::select! {
                _ = &mut ordinary => panic!("ordinary returned with SOURCE bodies held"),
                () = fixture.wait(|s| s.active[0] >= 1) => {}
            }
            assert_eq!(generation.slots.available_permits(), 0);
            let mut direct = Box::pin(generation.direct_closure_search_with_store(
                store, &query, 10, None, dlimits,
            ));
            tokio::select! {
                _ = &mut direct => panic!("direct bypassed the held ordinary slot"),
                _ = &mut ordinary => panic!("ordinary returned with SOURCE bodies held"),
                () = tokio::time::sleep(Duration::from_millis(50)) => {}
            }
            assert!(fixture.snapshot().requests.iter().all(|(path, _, _, _)| is_source(path)));
            fixture.release(0);
            let (ordinary, ()) = tokio::join!(ordinary, async {
                fixture.wait(|s| s.active[1] >= 1).await;
                fixture.release(1);
            });
            ordinary.unwrap();
            direct.await.unwrap();
            assert_eq!(generation.slots.available_permits(), 1);

            // Drain: an early digest failure and a delayed stream failure leave a held
            // valid sibling in flight; the slot spans all of them and every GET finishes.
            let (result, _) = run_direct_http(
                &fixture,
                generation.diagnostic_direct_closure_search_with_store(store, &query, 10, dlimits),
            )
            .await
            .unwrap();
            let cover = result.plan.ranges.clone();
            assert!(cover.len() >= 3);
            let starts = [cover[0].start, cover[1].start, cover[2].start];
            fixture.arm(Some((1, starts)));
            let before = fixture.reader.transport_stats();
            let mut query_future = Box::pin(generation.diagnostic_direct_closure_search_with_store(
                store, &query, 10, dlimits,
            ));
            tokio::select! {
                _ = &mut query_future => panic!("returned before the earlier stream error was released"),
                () = fixture.wait(|s| s.finished[1].contains(&starts[1]) && s.active[1] >= 2) => {}
            }
            assert!(!fixture.snapshot().finished[1].contains(&starts[0]));
            assert_eq!(generation.slots.available_permits(), 0);
            fixture.release_error();
            tokio::select! {
                _ = &mut query_future => panic!("returned before the valid sibling drained"),
                () = fixture.wait(|s| s.finished[1].contains(&starts[0])) => {}
            }
            assert!(!fixture.snapshot().finished[1].contains(&starts[2]));
            tokio::select! {
                _ = &mut query_future => panic!("cancelled the held valid sibling"),
                () = tokio::time::sleep(Duration::from_millis(30)) => {}
            }
            assert_eq!(generation.slots.available_permits(), 0);
            fixture.release_sibling();
            let error = query_future.await.err().unwrap();
            fn first_error(error: &TwoBitGenerationError) -> &DirectFetchError {
                match error {
                    TwoBitGenerationError::Query { error, .. } => first_error(error),
                    TwoBitGenerationError::Read(failure) => &failure.error,
                    _ => panic!("unexpected error: {error:?}"),
                }
            }
            assert!(matches!(first_error(&error), DirectFetchError::Store(_)), "{error:?}");
            assert_eq!(
                error.read_stats(),
                (
                    Sq8ReadStats::default(),
                    Sq8ReadStats {
                        submitted_gets: cover.len(),
                        verified_bytes: cover.iter().map(|r| r.len()).sum::<usize>()
                            - cover[0].len()
                            - cover[1].len(),
                        failed_gets: 2,
                    }
                )
            );
            assert_eq!(error.router_stats(), Sq8ReadStats::default());
            assert_eq!(error.direct_resource_rejection(), None);
            assert!(error.stages().is_some_and(|s| s.sq8.end_ns >= s.sq8.start_ns && s.sq8.start_ns > 0));
            assert_eq!(generation.slots.available_permits(), 1);
            let mut state = fixture.snapshot();
            assert!(state.errors.is_empty(), "{:?}", state.errors);
            assert_eq!(state.active, [0; 3]);
            state.finished[1].sort_unstable();
            assert_eq!(state.finished[1], cover.iter().map(|r| r.start).collect::<Vec<_>>());
            state.requests.sort();
            assert!(state.requests.iter().all(|(path, _, _, _)| *path == key_string));
            assert_eq!(state.requests.len(), cover.len());
            let after = fixture.reader.transport_stats();
            assert_eq!(after.attempts - before.attempts, cover.len() as u64);
            assert_eq!(after.stream_failures - before.stream_failures, 1);
            assert_eq!(
                after.consumed_payload_bytes - before.consumed_payload_bytes,
                (result.plan.planned_bytes - 1) as u64
            );
            fixture.finish();
        })
        .await
        .expect("whole direct slot fixture deadline");
    }

    // Wraps the recording store: SQ8 responses can be falsified after authentication
    // inputs are fixed (ETag, size, range, payload bytes) or refused as a failed precondition.
    #[derive(Debug)]
    struct SqFaultStore {
        inner: RecordedStore,
        sq8_path: String,
        fault: std::sync::Mutex<&'static str>,
    }
    impl std::fmt::Display for SqFaultStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "sq8-fault-fixture")
        }
    }
    #[async_trait::async_trait]
    impl ObjectStore for SqFaultStore {
        async fn get_opts(
            &self,
            path: &ObjectPath,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            let target = !options.head && path.as_ref() == self.sq8_path;
            let fault = *self.fault.lock().unwrap();
            if target && fault == "precondition" {
                return Err(object_store::Error::Generic {
                    store: "sq8-fault-fixture",
                    source: std::io::Error::other("injected failed precondition").into(),
                });
            }
            let mut result = self.inner.get_opts(path, options).await?;
            if target {
                match fault {
                    "etag" => result.meta.e_tag = Some("\"replaced\"".into()),
                    "size" => result.meta.size += 1,
                    "range" => result.range.start += 1,
                    "corrupt" | "short" | "long" => {
                        let object_store::GetResultPayload::Stream(body) = result.payload else {
                            unreachable!()
                        };
                        result.payload = object_store::GetResultPayload::Stream(
                            body.map(move |chunk| {
                                let mut bytes = chunk?.to_vec();
                                match fault {
                                    "corrupt" => bytes[0] ^= 1,
                                    "short" => {
                                        bytes.pop();
                                    }
                                    _ => bytes.push(0),
                                }
                                Ok(bytes::Bytes::from(bytes))
                            })
                            .boxed(),
                        );
                    }
                    _ => {}
                }
            }
            Ok(result)
        }
        async fn put_opts(
            &self,
            path: &ObjectPath,
            body: object_store::PutPayload,
            options: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.inner.put_opts(path, body, options).await
        }
        async fn put_multipart_opts(
            &self,
            path: &ObjectPath,
            options: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(path, options).await
        }
        fn delete_stream(
            &self,
            paths: futures_util::stream::BoxStream<'static, object_store::Result<ObjectPath>>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<ObjectPath>> {
            self.inner.delete_stream(paths)
        }
        fn list(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &ObjectPath,
            to: &ObjectPath,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    struct TinyDirect {
        temp: tempfile::TempDir,
        store: SqFaultStore,
        prefix: ObjectPath,
        root_sha: String,
        key: ObjectPath,
        etag: String,
        sq8: Vec<u8>,
        low: Vec<f32>,
        step: Vec<f32>,
        limits: TwoBitGenerationLimits,
    }

    // Actual builder, publication and authenticated open at a small shape: dyadic
    // low/step, nonzero codes, rows r, r+97, ... with identical codes and norm (exact
    // score ties whose ID order differs from ordinal order), and one tail row.
    async fn tiny_direct(rows: usize, dimensions: usize) -> TinyDirect {
        use crate::two_bit_build::TwoBitGenerationBuilder;
        use crate::two_bit_source::TwoBitSource;
        use sha2::{Digest, Sha256};
        let hash = |body: &[u8]| format!("{:x}", Sha256::digest(body));
        let temp = tempfile::tempdir().unwrap();
        let low = (0..dimensions)
            .map(|d| -0.5 + 0.125 * (d % 4) as f32)
            .collect::<Vec<_>>();
        let step = (0..dimensions)
            .map(|d| 0.0625 * (1 + d % 3) as f32)
            .collect::<Vec<_>>();
        let mut raw = Vec::new();
        let mut sq8 = Vec::new();
        for row in 0..rows {
            let class = row % 97;
            let id = 5_000 - ((row * 7 + 3) % rows) as i64;
            let mut squared = 0_f64;
            let mut codes = Vec::with_capacity(dimensions);
            for d in 0..dimensions {
                let code = ((class + 3 * d) % 11 + 1) as u8;
                let value = low[d] + f32::from(code) * step[d];
                squared += f64::from(value).powi(2);
                raw.extend_from_slice(&value.to_le_bytes());
                codes.push(code);
            }
            sq8.extend_from_slice(&id.to_le_bytes());
            sq8.extend_from_slice(&(squared as f32).to_le_bytes());
            sq8.extend_from_slice(&codes);
        }
        let raw_path = temp.path().join("raw");
        let sq8_path = temp.path().join("sq8");
        fs::write(&raw_path, &raw).unwrap();
        fs::write(&sq8_path, &sq8).unwrap();
        let inner = RecordedStore::default();
        let sq8_sha = hash(&sq8);
        let key = ObjectPath::from(format!("tiny/objects/{sq8_sha}"));
        inner.put(&key, sq8.clone().into()).await.unwrap();
        let etag = inner.head(&key).await.unwrap().e_tag.unwrap();
        let root = temp.path().join("generation");
        let order = (0..rows as u64).collect::<Vec<_>>();
        let root_sha = TwoBitGenerationBuilder {
            source: TwoBitSource {
                raw: &raw_path,
                raw_sha256: &hash(&raw),
                sq8: &sq8_path,
                sq8_sha256: &sq8_sha,
                rows,
                dimensions,
            },
            base_epoch: 0,
            generation: 1,
            low: &low,
            step: &step,
            sq8_object_key: key.as_ref(),
            sq8_etag: &etag,
        }
        .build_with_discovery(Some(&order), DiscoveryMode::Semantic, &root, 128_000_000)
        .unwrap();
        let limits = TwoBitGenerationLimits {
            max_memory_bytes: 512 * 1024 * 1024,
            max_active_queries: 1,
            max_query_bytes: 16_773_120,
            max_query_gets: 32,
            max_parallel_gets: 16,
            max_source_bytes: 64 * 1024 * 1024,
            max_source_gets: 128,
            max_parallel_source_gets: 16,
            max_query_scratch_bytes: 400_000 + TwoBitPlanTrace::scratch_bytes(rows),
            already_pinned_bytes: 1024,
        };
        let head = crate::two_bit_store::publish_two_bit_generation(
            &inner,
            &ObjectPath::from("tiny/index"),
            &root,
            &root_sha,
            limits,
            None,
        )
        .await
        .unwrap();
        TinyDirect {
            temp,
            store: SqFaultStore {
                inner,
                sq8_path: key.to_string(),
                fault: std::sync::Mutex::new("ok"),
            },
            prefix: head.metadata_prefix(),
            root_sha,
            key,
            etag,
            sq8,
            low,
            step,
            limits,
        }
    }

    #[tokio::test]
    async fn native_direct_closure_tiny_widths_tail_ties_admission_and_authenticated_failures() {
        let rows = 513;
        for dimensions in [24_usize, 40] {
            let tiny = tiny_direct(rows, dimensions).await;
            let row_bytes = dimensions + 12;
            let object_bytes = rows * row_bytes;
            let mut generation = TwoBitGeneration::open_remote(
                &tiny.store,
                &tiny.prefix,
                &tiny.root_sha,
                tiny.limits,
                tiny.temp.path(),
            )
            .await
            .unwrap();
            assert_eq!(
                generation.remote_open_stats().unwrap().source_head_requests,
                1
            );
            let reads = &tiny.store.inner.reads;
            let dlimits = DirectClosureLimits {
                max_sq8_bytes: object_bytes,
                max_sq8_gets: 1,
            };
            let query = (0..dimensions)
                .map(|d| 0.75 + 0.25 * ((d * 5) % 7) as f32 - if d % 3 == 0 { 1. } else { 0. })
                .collect::<Vec<_>>();
            let unit = cosine_unit(&query);

            // All 513 rows (two full pages plus a one-row tail) in one range, ranked
            // completely and compared with the scalar reference in literal order.
            reads.lock().unwrap().clear();
            let (result, trace) = generation
                .diagnostic_direct_closure_search_with_store(&tiny.store, &query, rows, dlimits)
                .await
                .unwrap();
            let recorded = reads.lock().unwrap().clone();
            assert_eq!(
                recorded,
                vec![(
                    tiny.key.to_string(),
                    false,
                    0..object_bytes as u64,
                    Some(tiny.etag.clone())
                )]
            );
            assert!(recorded.iter().all(|(path, _, _, _)| {
                !path.ends_with("/plane/records.bin") && !path.ends_with("/router/leaves.bin")
            }));
            assert_eq!(result.plan.selected_pages, vec![0, 1, 2]);
            assert_eq!(result.plan.ranges, vec![0..object_bytes]);
            assert_eq!(result.plan.planned_bytes, object_bytes);
            assert_eq!(
                result.ranked.stats,
                Sq8ReadStats {
                    submitted_gets: 1,
                    verified_bytes: object_bytes,
                    failed_gets: 0
                }
            );
            assert_eq!(result.source_stats, Sq8ReadStats::default());
            assert_eq!(result.router_stats, Sq8ReadStats::default());
            assert!(result.source_nomination_skipped);
            assert_eq!(trace.semantic_leaves, vec![0]);
            assert_eq!(trace.semantic_units, (0..17).collect::<Vec<_>>());
            assert!(trace.semantic_seed_additions.is_empty());
            let oracle = scalar_rank(&tiny.sq8, dimensions, 0..rows, &unit, &tiny.low, &tiny.step);
            assert_eq!(result.ranked.candidates.len(), rows);
            assert_hits_match(&result.ranked.candidates, &oracle);
            assert_geometry_agrees(&tiny.sq8, dimensions, &oracle, &unit, &tiny.low, &tiny.step);
            assert!(oracle.iter().any(|r| r.0 == rows - 1));
            assert!(has_id_tie_against_ordinal_order(&oracle));
            assert!(
                tiny.sq8
                    .chunks_exact(row_bytes)
                    .all(|row| row[12..].iter().all(|&c| c > 0))
            );
            let top50 = generation
                .direct_closure_search_with_store(&tiny.store, &query, 50, None, dlimits)
                .await
                .unwrap();
            assert_hits_match(&top50.ranked.candidates, &oracle);
            assert_eq!(top50.ranked.candidates.len(), 50);

            // Exclusions keep the rest of the ranking; the roster fits the pinned bytes.
            let mut excluded = oracle[..2].iter().map(|r| r.1).collect::<Vec<_>>();
            excluded.sort_unstable();
            let filtered = generation
                .direct_closure_search_with_store(
                    &tiny.store,
                    &query,
                    rows,
                    Some(&excluded),
                    dlimits,
                )
                .await
                .unwrap();
            let visible = oracle
                .iter()
                .filter(|r| !excluded.contains(&r.1))
                .copied()
                .collect::<Vec<_>>();
            assert_eq!(filtered.ranked.candidates.len(), rows - 2);
            assert_hits_match(&filtered.ranked.candidates, &visible);

            // Admission failures: no request, no slot left held, no resource reason.
            let permits = generation.slots.available_permits();
            assert_eq!(permits, 1);
            let limits_zero_bytes = DirectClosureLimits {
                max_sq8_bytes: 0,
                ..dlimits
            };
            let limits_zero_gets = DirectClosureLimits {
                max_sq8_gets: 0,
                ..dlimits
            };
            let limits_overflow = DirectClosureLimits {
                max_sq8_bytes: usize::MAX,
                ..dlimits
            };
            let mut wrong = query.clone();
            wrong.pop();
            let mut wide = query.clone();
            wide.push(1.);
            let zero = vec![0_f32; dimensions];
            let mut nan = query.clone();
            nan[1] = f32::NAN;
            let mut inf = query.clone();
            inf[2] = f32::INFINITY;
            let huge = vec![1_f32; 1 << 20];
            let cases: Vec<(&str, Vec<f32>, usize, DirectClosureLimits, bool)> = vec![
                ("short", wrong, 10, dlimits, false),
                ("wide", wide, 10, dlimits, false),
                ("huge", huge, 10, dlimits, false),
                ("top_k_zero", query.clone(), 0, dlimits, false),
                (
                    "top_k_rows_plus_one",
                    query.clone(),
                    rows + 1,
                    dlimits,
                    false,
                ),
                ("zero_bytes", query.clone(), 10, limits_zero_bytes, false),
                ("zero_gets", query.clone(), 10, limits_zero_gets, false),
                ("overflow", query.clone(), 10, limits_overflow, false),
                ("zero_query", zero, 10, dlimits, false),
                ("nan_query", nan, 10, dlimits, false),
                ("inf_query", inf, 10, dlimits, false),
            ];
            for (name, bad, k, bad_limits, after_slot) in cases {
                reads.lock().unwrap().clear();
                let error = generation
                    .direct_closure_search_with_store(&tiny.store, &bad, k, None, bad_limits)
                    .await
                    .err()
                    .unwrap();
                assert!(reads.lock().unwrap().is_empty(), "{name}");
                assert_eq!(generation.slots.available_permits(), 1, "{name}");
                assert_eq!(error.direct_resource_rejection(), None, "{name}");
                assert_eq!(error.stages().is_some(), after_slot, "{name}: {error:?}");
                assert_eq!(
                    error.read_stats(),
                    (Sq8ReadStats::default(), Sq8ReadStats::default())
                );
                // Every malformed input is refused during allocation-free admission.
                assert!(
                    matches!(
                        error,
                        TwoBitGenerationError::Invalid(_) | TwoBitGenerationError::Plane(_)
                    ),
                    "{name}: {error:?}"
                );
            }
            // Trace scratch is checked before any allocation, only for diagnostic calls.
            let scratch = generation.limits.max_query_scratch_bytes;
            generation.limits.max_query_scratch_bytes = TwoBitPlanTrace::scratch_bytes(rows) - 1;
            let error = generation
                .diagnostic_direct_closure_search_with_store(&tiny.store, &query, 10, dlimits)
                .await
                .err()
                .unwrap();
            assert!(matches!(
                error,
                TwoBitGenerationError::Invalid("diagnostic scratch")
            ));
            assert!(reads.lock().unwrap().is_empty());
            generation
                .direct_closure_search_with_store(&tiny.store, &query, 10, None, dlimits)
                .await
                .unwrap();
            generation.limits.max_query_scratch_bytes = scratch;
            // Memory cap minus one and planned-byte cap minus one are machine-readable.
            let memory = generation.direct_closure_memory(dlimits).unwrap();
            generation.limits.max_memory_bytes = memory.total_bytes - 1;
            reads.lock().unwrap().clear();
            // Input validity precedes resource classification: with the cap one byte short,
            // each malformed input is still a non-resource error with zero I/O.
            let mut short_query = query.clone();
            short_query.pop();
            let mut nan_query = query.clone();
            nan_query[1] = f32::NAN;
            let mut inf_query = query.clone();
            inf_query[2] = f32::NEG_INFINITY;
            let malformed: Vec<(&str, Vec<f32>, usize)> = vec![
                ("short", short_query, 10),
                ("zero", vec![0_f32; dimensions], 10),
                ("nan", nan_query, 10),
                ("neg_inf", inf_query, 10),
                ("top_k_zero", query.clone(), 0),
                ("top_k_rows_plus_one", query.clone(), rows + 1),
            ];
            for (name, bad, k) in malformed {
                let error = generation
                    .direct_closure_search_with_store(&tiny.store, &bad, k, None, dlimits)
                    .await
                    .err()
                    .unwrap();
                assert_eq!(error.direct_resource_rejection(), None, "{name}: {error:?}");
                assert!(reads.lock().unwrap().is_empty(), "{name}");
                assert_eq!(generation.slots.available_permits(), 1, "{name}");
            }
            let error = generation
                .direct_closure_search_with_store(&tiny.store, &query, 10, None, dlimits)
                .await
                .err()
                .unwrap();
            assert_eq!(
                error.direct_resource_rejection(),
                Some(DirectClosureRejection::ModeledMemory)
            );
            assert!(reads.lock().unwrap().is_empty());
            generation.limits.max_memory_bytes = memory.total_bytes;
            generation
                .direct_closure_search_with_store(&tiny.store, &query, 10, None, dlimits)
                .await
                .unwrap();
            reads.lock().unwrap().clear();
            let error = generation
                .direct_closure_search_with_store(
                    &tiny.store,
                    &query,
                    10,
                    None,
                    DirectClosureLimits {
                        max_sq8_bytes: object_bytes - 1,
                        ..dlimits
                    },
                )
                .await
                .err()
                .unwrap();
            assert_eq!(
                error.direct_resource_rejection(),
                Some(DirectClosureRejection::PlannedBytes)
            );
            assert!(reads.lock().unwrap().is_empty());

            // Authenticated response failures keep their charge and release the slot.
            let failed = Sq8ReadStats {
                submitted_gets: 1,
                verified_bytes: 0,
                failed_gets: 1,
            };
            for fault in [
                "precondition",
                "etag",
                "size",
                "range",
                "corrupt",
                "short",
                "long",
            ] {
                *tiny.store.fault.lock().unwrap() = fault;
                let error = generation
                    .direct_closure_search_with_store(&tiny.store, &query, 10, None, dlimits)
                    .await
                    .err()
                    .unwrap();
                let TwoBitGenerationError::Query { error: inner, .. } = &error else {
                    panic!("{fault}: {error:?}");
                };
                let TwoBitGenerationError::Read(failure) = inner.as_ref() else {
                    panic!("{fault}: {inner:?}");
                };
                match fault {
                    "precondition" => assert!(matches!(failure.error, DirectFetchError::Store(_))),
                    "corrupt" | "short" => {
                        assert!(matches!(failure.error, DirectFetchError::Page(_)))
                    }
                    _ => assert!(
                        matches!(failure.error, DirectFetchError::UnexpectedMetadata),
                        "{fault}: {failure:?}"
                    ),
                }
                assert_eq!(
                    error.read_stats(),
                    (Sq8ReadStats::default(), failed),
                    "{fault}"
                );
                assert_eq!(error.router_stats(), Sq8ReadStats::default());
                assert_eq!(error.direct_resource_rejection(), None);
                assert_eq!(generation.slots.available_permits(), 1, "{fault}");
            }
            *tiny.store.fault.lock().unwrap() = "ok";
            let healthy = generation
                .direct_closure_search_with_store(&tiny.store, &query, 10, None, dlimits)
                .await
                .unwrap();
            assert_hits_match(&healthy.ranked.candidates, &oracle);

            // The root is authenticated before any direct query can exist.
            let mut bad_root = tiny.root_sha.clone();
            bad_root.replace_range(..1, if bad_root.starts_with('0') { "1" } else { "0" });
            assert!(
                TwoBitGeneration::open_remote(
                    &tiny.store,
                    &tiny.prefix,
                    &bad_root,
                    tiny.limits,
                    tiny.temp.path(),
                )
                .await
                .is_err()
            );
        }
    }

    fn tiny_query(dimensions: usize) -> Vec<f32> {
        (0..dimensions)
            .map(|d| 0.75 + 0.25 * ((d * 5) % 7) as f32 - if d % 3 == 0 { 1. } else { 0. })
            .collect()
    }

    fn ranking_bits(result: &TwoBitSearchResult) -> Vec<(usize, i64, u32)> {
        result
            .ranked
            .candidates
            .iter()
            .map(|hit| (hit.ordinal, hit.id, hit.score.to_bits()))
            .collect()
    }

    #[tokio::test]
    async fn native_sq8_range_trace_parity_overlap_and_critical_path_for_baseline_and_direct() {
        let deadline = Instant::now() + Duration::from_secs(90);
        tokio::time::timeout(Duration::from_secs(90), async {
            let ClassFixture {
                temp,
                root_sha,
                prefix,
                objects,
                limits,
                rows,
                dimensions,
                ..
            } = class_fixture(32);
            let row_bytes = dimensions + 12;
            let fixture = DirectHttp::new(objects, deadline);
            let generation = TwoBitGeneration::open_remote(
                fixture.reader.store(),
                &prefix,
                &root_sha,
                limits,
                temp.path(),
            )
            .await
            .unwrap();
            let store = fixture.reader.store();
            let query = class_query(dimensions);
            let dlimits = DirectClosureLimits {
                max_sq8_bytes: rows * row_bytes,
                max_sq8_gets: 32,
            };
            let wire = |fixture: &DirectHttp| {
                let mut requests = fixture.snapshot().requests;
                requests.sort();
                requests
            };

            // Baseline: tracing changes no GET, byte, plan, charge, plan trace or ranking bit.
            let (plain, plain_plan) = run_source_and_sq8_http(
                &fixture,
                generation.diagnostic_search_with_store(store, &query, 10),
            )
            .await
            .unwrap();
            let plain_wire = wire(&fixture);
            let mut trace = Sq8RangeTrace::new(32).unwrap();
            let (traced, traced_plan) = run_source_and_sq8_http(
                &fixture,
                generation.diagnostic_search_with_store_traced(store, &query, 10, &mut trace),
            )
            .await
            .unwrap();
            assert_eq!(wire(&fixture), plain_wire);
            assert_eq!(traced.plan, plain.plan);
            assert_eq!(traced.ranked.stats, plain.ranked.stats);
            assert_eq!(traced.source_stats, plain.source_stats);
            assert_eq!(ranking_bits(&traced), ranking_bits(&plain));
            assert_eq!(
                serde_json::to_value(&traced_plan).unwrap(),
                serde_json::to_value(&plain_plan).unwrap()
            );
            // Only the SQ8 wave is traced; SOURCE reads stay untraced and unchanged.
            assert_eq!(trace.ranges().len(), traced.plan.ranges.len());
            for (span, range) in trace.ranges().iter().zip(&traced.plan.ranges) {
                assert_eq!(
                    (span.start_byte, span.end_byte),
                    (range.start as u64, range.end as u64)
                );
                assert_eq!(span.outcome, "ok");
            }
            assert_eq!(trace.outcome, "ranked");
            assert_eq!(generation.slots.available_permits(), 1);

            // Direct: two bodies are held in flight at once, then released.
            let mut direct_trace = Sq8RangeTrace::new(32).unwrap();
            fixture.arm(None);
            let work = generation.diagnostic_direct_closure_search_with_store_traced(
                store,
                &query,
                10,
                dlimits,
                &mut direct_trace,
            );
            let (direct, ()) = tokio::join!(work, async {
                fixture.wait(|state| state.active[1] >= 2).await;
                // The traced query owns the one slot for its whole drain.
                assert_eq!(generation.slots.available_permits(), 0);
                fixture.release(1);
            });
            let (direct, _) = direct.unwrap();
            let direct_wire = wire(&fixture);
            assert!(fixture.snapshot().peak[1] >= 2);
            assert_eq!(generation.slots.available_permits(), 1);
            let (plain_direct, _) = run_direct_http(
                &fixture,
                generation.diagnostic_direct_closure_search_with_store(store, &query, 10, dlimits),
            )
            .await
            .unwrap();
            assert_eq!(wire(&fixture), direct_wire);
            assert_eq!(direct.plan, plain_direct.plan);
            assert_eq!(direct.ranked.stats, plain_direct.ranked.stats);
            assert_eq!(ranking_bits(&direct), ranking_bits(&plain_direct));
            assert_eq!(direct.source_stats, plain_direct.source_stats);
            assert!(direct.source_nomination_skipped);

            let cover = &direct.plan.ranges;
            assert!(cover.len() > 2);
            assert_eq!(direct_trace.outcome, "ranked");
            assert_eq!(direct_trace.planned_ranges as usize, cover.len());
            assert_eq!(direct_trace.planned_bytes, direct.plan.planned_bytes as u64);
            assert_eq!(direct_trace.dropped_ranges, 0);
            let spans = direct_trace.ranges();
            assert_eq!(spans.len(), cover.len());
            for (span, range) in spans.iter().zip(cover) {
                assert_eq!(
                    (span.start_byte, span.end_byte),
                    (range.start as u64, range.end as u64)
                );
                let chain = [
                    span.first_poll_ns,
                    span.request_ns,
                    span.headers_ns,
                    span.metadata_ns,
                    span.first_chunk_ns,
                    span.last_chunk_ns,
                    span.eof_ns,
                    span.auth_start_ns,
                    span.auth_end_ns,
                    span.complete_ns,
                ]
                .map(Option::unwrap);
                assert!(chain.windows(2).all(|w| w[0] <= w[1]), "{chain:?}");
                assert_eq!(span.outcome, "ok");
                assert_eq!(span.body_bytes, span.end_byte - span.start_byte);
                assert_eq!(span.copy_count, span.chunks);
            }
            // At least two ranges were in flight together: neither finished before the other began.
            assert!(spans.iter().enumerate().any(|(i, a)| {
                spans[i + 1..].iter().any(|b| {
                    a.first_poll_ns.unwrap() < b.complete_ns.unwrap()
                        && b.first_poll_ns.unwrap() < a.complete_ns.unwrap()
                })
            }));
            // Authentication of every range precedes the barrier, and ranking follows it.
            let all = direct_trace.all_ranges_complete_ns.unwrap();
            assert!(spans.iter().all(|span| span.complete_ns.unwrap() <= all));
            let (rank_start, rank_end) = (
                direct_trace.rank_start_ns.unwrap(),
                direct_trace.rank_end_ns.unwrap(),
            );
            assert!(all <= rank_start && rank_start <= rank_end);
            // The payload is released after ranking, still inside the SQ8 stage.
            let release_end = direct_trace.release_end_ns.unwrap();
            assert!(rank_end <= release_end);
            let phases = direct_trace.rank.unwrap();
            assert_eq!(phases.ranges as usize, cover.len());
            assert_eq!(
                phases.rows as usize,
                cover.iter().map(|r| r.len() / row_bytes).sum::<usize>()
            );
            // The trace shares the stage origin: it sits inside the SQ8 stage.
            let stage = direct.stages.sq8;
            assert!(stage.start_ns <= u128::from(direct_trace.fetch_start_ns.unwrap()));
            assert!(u128::from(release_end) <= stage.end_ns);
            fixture.finish();
        })
        .await
        .expect("whole range trace fixture deadline");
    }

    #[tokio::test]
    async fn native_sq8_range_trace_memory_and_capacity_are_refused_before_slot_or_any_get() {
        let (rows, dimensions) = (513, 24);
        let tiny = tiny_direct(rows, dimensions).await;
        let object_bytes = rows * (dimensions + 12);
        let mut generation = TwoBitGeneration::open_remote(
            &tiny.store,
            &tiny.prefix,
            &tiny.root_sha,
            tiny.limits,
            tiny.temp.path(),
        )
        .await
        .unwrap();
        let reads = &tiny.store.inner.reads;
        let dlimits = DirectClosureLimits {
            max_sq8_bytes: object_bytes,
            max_sq8_gets: 1,
        };
        let query = tiny_query(dimensions);
        let mut trace = Sq8RangeTrace::new(1).unwrap();
        let charge = |trace: &Sq8RangeTrace, generation: &TwoBitGeneration| {
            trace.reserved_bytes(generation.limits.max_parallel_gets) as u64
                * generation.limits.max_active_queries as u64
        };
        let direct_charge = charge(&trace, &generation);
        let memory = generation.direct_closure_memory(dlimits).unwrap();
        // One byte short of the modeled diagnostic state: a non-resource refusal with no I/O.
        reads.lock().unwrap().clear();
        generation.limits.max_memory_bytes = memory.total_bytes + direct_charge - 1;
        let error = generation
            .diagnostic_direct_closure_search_with_store_traced(
                &tiny.store,
                &query,
                10,
                dlimits,
                &mut trace,
            )
            .await
            .err()
            .unwrap();
        assert!(
            matches!(error, TwoBitGenerationError::Invalid("diagnostic memory")),
            "{error:?}"
        );
        assert_eq!(error.direct_resource_rejection(), None);
        assert!(error.stages().is_none());
        assert!(reads.lock().unwrap().is_empty());
        assert_eq!(generation.slots.available_permits(), 1);
        assert_eq!(trace.outcome, "admission_refused");
        // The direct model itself is unchanged: the same cap still admits untraced search.
        generation
            .direct_closure_search_with_store(&tiny.store, &query, 10, None, dlimits)
            .await
            .unwrap();
        // The exact modeled total admits and traces.
        reads.lock().unwrap().clear();
        generation.limits.max_memory_bytes = memory.total_bytes + direct_charge;
        let (result, _) = generation
            .diagnostic_direct_closure_search_with_store_traced(
                &tiny.store,
                &query,
                10,
                dlimits,
                &mut trace,
            )
            .await
            .unwrap();
        assert_eq!(result.ranked.stats.submitted_gets, 1);
        assert_eq!((trace.outcome, trace.ranges().len()), ("ranked", 1));
        // A buffer smaller than the GET cap is refused first, before any request.
        reads.lock().unwrap().clear();
        let mut narrow = Sq8RangeTrace::new(1).unwrap();
        let wide = DirectClosureLimits {
            max_sq8_gets: 2,
            ..dlimits
        };
        let error = generation
            .diagnostic_direct_closure_search_with_store_traced(
                &tiny.store,
                &query,
                10,
                wide,
                &mut narrow,
            )
            .await
            .err()
            .unwrap();
        assert!(matches!(
            error,
            TwoBitGenerationError::Invalid("diagnostic range capacity")
        ));
        assert!(reads.lock().unwrap().is_empty());
        // Direct resource classification comes first and keeps its machine-readable reason.
        generation.limits.max_memory_bytes = memory.total_bytes - 1;
        let error = generation
            .diagnostic_direct_closure_search_with_store_traced(
                &tiny.store,
                &query,
                10,
                dlimits,
                &mut trace,
            )
            .await
            .err()
            .unwrap();
        assert_eq!(
            error.direct_resource_rejection(),
            Some(DirectClosureRejection::ModeledMemory)
        );
        assert!(reads.lock().unwrap().is_empty());
        // Baseline traced search has the same two refusals, ahead of the slot.
        generation.limits.max_memory_bytes = tiny.limits.max_memory_bytes;
        let mut one = Sq8RangeTrace::new(1).unwrap();
        let error = generation
            .diagnostic_search_with_store_traced(&tiny.store, &query, 10, &mut one)
            .await
            .err()
            .unwrap();
        assert!(matches!(
            error,
            TwoBitGenerationError::Invalid("diagnostic range capacity")
        ));
        let mut full = Sq8RangeTrace::new(32).unwrap();
        generation.limits.max_memory_bytes =
            generation.modeled_memory_bytes() + charge(&full, &generation) - 1;
        let error = generation
            .diagnostic_search_with_store_traced(&tiny.store, &query, 10, &mut full)
            .await
            .err()
            .unwrap();
        assert!(matches!(
            error,
            TwoBitGenerationError::Invalid("diagnostic memory")
        ));
        assert_eq!(full.outcome, "admission_refused");
        assert!(reads.lock().unwrap().is_empty());
        assert_eq!(generation.slots.available_permits(), 1);
    }

    #[tokio::test]
    async fn native_sq8_range_trace_early_refusal_never_exposes_a_reused_trace_for_baseline_and_direct()
     {
        let (rows, dimensions) = (513, 24);
        let tiny = tiny_direct(rows, dimensions).await;
        let object_bytes = rows * (dimensions + 12);
        let mut generation = TwoBitGeneration::open_remote(
            &tiny.store,
            &tiny.prefix,
            &tiny.root_sha,
            tiny.limits,
            tiny.temp.path(),
        )
        .await
        .unwrap();
        let reads = &tiny.store.inner.reads;
        let dlimits = DirectClosureLimits {
            max_sq8_bytes: object_bytes,
            max_sq8_gets: 1,
        };
        let query = tiny_query(dimensions);
        // A successful traced query leaves real evidence in the trace.
        async fn dirty(
            generation: &TwoBitGeneration,
            store: &SqFaultStore,
            query: &[f32],
            limits: DirectClosureLimits,
            trace: &mut Sq8RangeTrace,
        ) {
            generation
                .diagnostic_direct_closure_search_with_store_traced(store, query, 10, limits, trace)
                .await
                .unwrap();
            assert_eq!((trace.outcome, trace.ranges().len()), ("ranked", 1));
            assert!(trace.rank.is_some() && trace.all_ranges_complete_ns.is_some());
        }
        let pristine = |trace: &Sq8RangeTrace, outcome: &str, case: &str| {
            assert_eq!(trace.outcome, outcome, "{case}");
            assert!(trace.ranges().is_empty(), "{case}");
            assert_eq!(
                (
                    trace.planned_ranges,
                    trace.planned_bytes,
                    trace.max_parallel,
                    trace.dropped_ranges
                ),
                (0, 0, 0, 0),
                "{case}"
            );
            assert_eq!(
                (
                    trace.fetch_start_ns,
                    trace.all_ranges_complete_ns,
                    trace.rank_start_ns,
                    trace.rank_end_ns
                ),
                (None, None, None, None),
                "{case}"
            );
            assert_eq!(trace.release_end_ns, None, "{case}");
            assert_eq!(trace.rank, None, "{case}");
        };
        let mut trace = Sq8RangeTrace::new(32).unwrap();
        let mut narrow = Sq8RangeTrace::new(1).unwrap();
        let memory = generation.direct_closure_memory(dlimits).unwrap();
        let direct_charge = trace.reserved_bytes(generation.limits.max_parallel_gets) as u64
            * generation.limits.max_active_queries as u64;
        let wide = DirectClosureLimits {
            max_sq8_gets: 2,
            ..dlimits
        };
        let short_query = &query[1..];

        // Direct entry: every refusal that precedes the query leaves an explicit refused
        // state, never the previous query's spans, and keeps its typed native error.
        let cases: [(&str, bool, usize, usize, bool); 5] = [
            ("direct top_k zero", false, 0, 0, false),
            ("direct short query", false, 10, 0, true),
            ("direct narrow capacity", true, 10, 0, false),
            ("direct diagnostic memory", false, 10, 1, false),
            ("direct modeled memory", false, 10, 2, false),
        ];
        for (case, use_narrow, top_k, memory_case, short) in cases {
            let target = if use_narrow { &mut narrow } else { &mut trace };
            dirty(&generation, &tiny.store, &query, dlimits, target).await;
            reads.lock().unwrap().clear();
            generation.limits.max_memory_bytes = match memory_case {
                1 => memory.total_bytes + direct_charge - 1,
                2 => memory.total_bytes - 1,
                _ => tiny.limits.max_memory_bytes,
            };
            let limits = if use_narrow { wide } else { dlimits };
            let error = generation
                .diagnostic_direct_closure_search_with_store_traced(
                    &tiny.store,
                    if short { short_query } else { &query[..] },
                    top_k,
                    limits,
                    target,
                )
                .await
                .err()
                .unwrap();
            generation.limits.max_memory_bytes = tiny.limits.max_memory_bytes;
            match case {
                "direct top_k zero" | "direct short query" => assert!(
                    matches!(error, TwoBitGenerationError::Invalid("search admission")),
                    "{case}: {error:?}"
                ),
                "direct narrow capacity" => assert!(
                    matches!(
                        error,
                        TwoBitGenerationError::Invalid("diagnostic range capacity")
                    ),
                    "{case}: {error:?}"
                ),
                "direct diagnostic memory" => assert!(
                    matches!(error, TwoBitGenerationError::Invalid("diagnostic memory")),
                    "{case}: {error:?}"
                ),
                _ => assert_eq!(
                    error.direct_resource_rejection(),
                    Some(DirectClosureRejection::ModeledMemory),
                    "{case}"
                ),
            }
            assert!(reads.lock().unwrap().is_empty(), "{case}");
            assert_eq!(generation.slots.available_permits(), 1, "{case}");
            pristine(target, "admission_refused", case);
        }

        // Refused after the query began (planned bytes) is a started query with no spans.
        dirty(&generation, &tiny.store, &query, dlimits, &mut trace).await;
        reads.lock().unwrap().clear();
        let error = generation
            .diagnostic_direct_closure_search_with_store_traced(
                &tiny.store,
                &query,
                10,
                DirectClosureLimits {
                    max_sq8_bytes: object_bytes - 1,
                    ..dlimits
                },
                &mut trace,
            )
            .await
            .err()
            .unwrap();
        assert_eq!(
            error.direct_resource_rejection(),
            Some(DirectClosureRejection::PlannedBytes)
        );
        assert!(reads.lock().unwrap().is_empty());
        pristine(&trace, "started", "direct planned bytes");

        // Baseline entry: the same guarantee for its three early refusals.
        let baseline_charge = trace.reserved_bytes(generation.limits.max_parallel_gets) as u64
            * generation.limits.max_active_queries as u64;
        let cases: [(&str, bool, usize, bool); 3] = [
            ("baseline top_k zero", false, 0, false),
            ("baseline narrow capacity", true, 10, false),
            ("baseline diagnostic memory", false, 10, true),
        ];
        for (case, use_narrow, top_k, memory_case) in cases {
            let target = if use_narrow { &mut narrow } else { &mut trace };
            dirty(&generation, &tiny.store, &query, dlimits, target).await;
            reads.lock().unwrap().clear();
            if memory_case {
                generation.limits.max_memory_bytes =
                    generation.modeled_memory_bytes() + baseline_charge - 1;
            }
            let error = generation
                .diagnostic_search_with_store_traced(&tiny.store, &query, top_k, target)
                .await
                .err()
                .unwrap();
            generation.limits.max_memory_bytes = tiny.limits.max_memory_bytes;
            let reason = match case {
                "baseline top_k zero" => "search admission",
                "baseline narrow capacity" => "diagnostic range capacity",
                _ => "diagnostic memory",
            };
            assert!(
                matches!(error, TwoBitGenerationError::Invalid(found) if found == reason),
                "{case}: {error:?}"
            );
            assert!(reads.lock().unwrap().is_empty(), "{case}");
            assert_eq!(generation.slots.available_permits(), 1, "{case}");
            pristine(target, "admission_refused", case);
        }
        // The refused state does not poison the trace: the next query is fully traced again.
        dirty(&generation, &tiny.store, &query, dlimits, &mut trace).await;
    }

    #[tokio::test]
    async fn native_sq8_range_trace_failure_outcomes_are_truthful_and_leave_ranking_unstarted() {
        let (rows, dimensions) = (513, 24);
        let tiny = tiny_direct(rows, dimensions).await;
        let object_bytes = rows * (dimensions + 12);
        let generation = TwoBitGeneration::open_remote(
            &tiny.store,
            &tiny.prefix,
            &tiny.root_sha,
            tiny.limits,
            tiny.temp.path(),
        )
        .await
        .unwrap();
        let dlimits = DirectClosureLimits {
            max_sq8_bytes: object_bytes,
            max_sq8_gets: 1,
        };
        let query = tiny_query(dimensions);
        let mut trace = Sq8RangeTrace::new(1).unwrap();
        let failed = Sq8ReadStats {
            submitted_gets: 1,
            verified_bytes: 0,
            failed_gets: 1,
        };
        // (fault, outcome, headers, metadata, eof, authentication started, body bytes)
        for (fault, outcome, headers, metadata, eof, auth, body) in [
            (
                "precondition",
                "store_headers",
                false,
                false,
                false,
                false,
                0,
            ),
            ("etag", "metadata", true, false, false, false, 0),
            ("size", "metadata", true, false, false, false, 0),
            ("range", "metadata", true, false, false, false, 0),
            ("corrupt", "page_auth", true, true, true, true, object_bytes),
            (
                "short",
                "page_auth",
                true,
                true,
                true,
                true,
                object_bytes - 1,
            ),
            (
                "long",
                "overlong",
                true,
                true,
                false,
                false,
                object_bytes + 1,
            ),
        ] {
            *tiny.store.fault.lock().unwrap() = fault;
            let error = generation
                .diagnostic_direct_closure_search_with_store_traced(
                    &tiny.store,
                    &query,
                    10,
                    dlimits,
                    &mut trace,
                )
                .await
                .err()
                .unwrap();
            assert_eq!(error.read_stats().1, failed, "{fault}");
            assert_eq!(generation.slots.available_permits(), 1, "{fault}");
            assert_eq!(trace.outcome, "fetch_failed", "{fault}");
            assert_eq!(trace.ranges().len(), 1, "{fault}");
            let span = trace.ranges()[0];
            assert_eq!(span.outcome, outcome, "{fault}");
            assert_eq!(
                (span.start_byte, span.end_byte),
                (0, object_bytes as u64),
                "{fault}"
            );
            assert!(
                span.first_poll_ns.is_some() && span.request_ns.is_some(),
                "{fault}"
            );
            assert!(span.complete_ns.is_some(), "{fault}");
            assert_eq!(span.headers_ns.is_some(), headers, "{fault}");
            assert_eq!(span.metadata_ns.is_some(), metadata, "{fault}");
            assert_eq!(span.eof_ns.is_some(), eof, "{fault}");
            assert_eq!(span.auth_start_ns.is_some(), auth, "{fault}");
            assert_eq!(span.auth_end_ns.is_some(), auth, "{fault}");
            assert_eq!(span.body_bytes, body as u64, "{fault}");
            // The drain completed before the failure was reported; ranking never started.
            assert!(trace.all_ranges_complete_ns.is_some(), "{fault}");
            assert_eq!(
                (trace.rank_start_ns, trace.rank_end_ns, trace.release_end_ns),
                (None, None, None)
            );
            assert_eq!(trace.rank, None);
        }
        *tiny.store.fault.lock().unwrap() = "ok";
        let (traced, _) = generation
            .diagnostic_direct_closure_search_with_store_traced(
                &tiny.store,
                &query,
                10,
                dlimits,
                &mut trace,
            )
            .await
            .unwrap();
        let (plain, _) = generation
            .diagnostic_direct_closure_search_with_store(&tiny.store, &query, 10, dlimits)
            .await
            .unwrap();
        assert_eq!(traced.plan, plain.plan);
        assert_eq!(traced.ranked.stats, plain.ranked.stats);
        assert_eq!(ranking_bits(&traced), ranking_bits(&plain));
        assert_eq!((trace.outcome, trace.ranges()[0].outcome), ("ranked", "ok"));
        assert_eq!(generation.slots.available_permits(), 1);
    }

    #[test]
    fn direct_closure_rejects_graph_discovery_before_any_io() {
        use sha2::{Digest, Sha256};
        let hash = |body: &[u8]| format!("{:x}", Sha256::digest(body));
        let (rows, dimensions) = (513, 2);
        let temp = tempfile::tempdir().unwrap();
        let raw = (0..rows)
            .flat_map(|id| {
                (0..dimensions)
                    .map(move |d| match d {
                        0 => 1.0_f32 + (id % 7) as f32 * 0.2,
                        _ => 0.1 + (id % 11) as f32 * 0.1,
                    })
                    .flat_map(f32::to_le_bytes)
            })
            .collect::<Vec<_>>();
        let sq8 = (0..rows)
            .flat_map(|id| {
                let mut record = (id as i64).to_le_bytes().to_vec();
                record.extend_from_slice(&5_f32.to_le_bytes());
                record.extend((0..dimensions).map(|d| (d + 1) as u8));
                record
            })
            .collect::<Vec<_>>();
        let raw_path = temp.path().join("raw");
        let sq8_path = temp.path().join("sq8");
        fs::write(&raw_path, &raw).unwrap();
        fs::write(&sq8_path, &sq8).unwrap();
        let sq8_sha = hash(&sq8);
        let key = format!("tenant/objects/{sq8_sha}");
        let root = temp.path().join("generation");
        let root_sha = crate::two_bit_build::TwoBitGenerationBuilder {
            source: crate::two_bit_source::TwoBitSource {
                raw: &raw_path,
                raw_sha256: &hash(&raw),
                sq8: &sq8_path,
                sq8_sha256: &sq8_sha,
                rows,
                dimensions,
            },
            base_epoch: 0,
            generation: 7,
            low: &vec![0.0; dimensions],
            step: &vec![1.0; dimensions],
            sq8_object_key: &key,
            sq8_etag: "etag",
        }
        .build(&root, 256_000_000)
        .unwrap();
        let limits = TwoBitGenerationLimits {
            max_memory_bytes: 256_000_000,
            max_active_queries: 1,
            max_query_bytes: rows * (dimensions + 12),
            max_query_gets: 32,
            max_parallel_gets: 2,
            max_source_bytes: 64 * 1024 * 1024,
            max_source_gets: 128,
            max_parallel_source_gets: 2,
            max_query_scratch_bytes: 400_000,
            already_pinned_bytes: 0,
        };
        let generation = TwoBitGeneration::open(&root, &root_sha, limits).unwrap();
        assert_eq!(generation.discovery_mode(), DiscoveryMode::Graph);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        // An absent store proves no I/O could have been needed or attempted.
        let error = runtime
            .block_on(generation.direct_closure_search_with_store(
                &object_store::memory::InMemory::new(),
                &[1., 0.],
                1,
                None,
                DirectClosureLimits {
                    max_sq8_bytes: rows * (dimensions + 12),
                    max_sq8_gets: 2,
                },
            ))
            .err()
            .unwrap();
        assert!(matches!(
            error,
            TwoBitGenerationError::Invalid("direct closure requires semantic discovery")
        ));
        assert_eq!(error.direct_resource_rejection(), None);
    }
}
