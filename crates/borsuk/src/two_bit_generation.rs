//! A single authenticated root for frozen two-bit nomination and on-demand SQ8.
use crate::semantic_unit_router::{SemanticProfile, SemanticUnitRouter, SourceIdentity};
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
        OneAttemptS3, RankedSq8, RankedSq8Failure, Sq8ReadStats, fetch_verified_ranges_inner,
        rank_verified_sq8_pages_inner,
    },
    two_bit_mutations::{TwoBitMutationHit, TwoBitMutationSnapshot},
    two_bit_source::{SourceBuildError, SourcePlaneReceipt, TwoBitPlane, read_authenticated},
    unit_centroid_graph::{UnitCentroidGraph, UnitCentroidGraphError},
    unit_centroid_pages::{UnitCentroidError, UnitCentroidPages},
};
use futures_util::{StreamExt, stream};
use object_store::GetOptions;
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
    /// Concurrent GETs per admitted query.
    pub max_parallel_gets: usize,
    /// Maximum authenticated source bytes per query, separate from SQ8.
    pub max_source_bytes: usize,
    /// Maximum source range GETs per query.
    pub max_source_gets: usize,
    /// Concurrent source range GETs per query.
    pub max_parallel_source_gets: usize,
    /// Codec lookup scratch per query.
    pub max_query_scratch_bytes: usize,
    /// Payload charged to other pinned generations and immutable mutation snapshots.
    pub already_pinned_bytes: u64,
}
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
const LEAF_BYTES: usize = 2 * 1024 * 1024;

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
        local: std::path::PathBuf,
        remote: Option<(ObjectPath, String)>,
    },
}
struct RemoteSource {
    authority: PageAuthority,
    location: ObjectPath,
    etag: String,
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
    /// Query preparation and graph/semantic discovery, including leaf reads.
    pub discovery: StageInterval,
    /// Authenticated source range fetch, after releasing leaf bodies.
    pub source: StageInterval,
    /// Shared source scoring and physical SQ8 plan.
    pub planning: StageInterval,
    /// Native authenticated SQ8 fetch/rank, after releasing source bodies.
    pub sq8: StageInterval,
    /// Maximum simultaneous selected-leaf read futures actually entered.
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
    /// Selected whole-leaf charges; separate from source/SQ8.
    pub router_stats: Sq8ReadStats,
    /// Stage intervals from the admitted query.
    pub stages: QueryStages,
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
    /// Logical whole-leaf object HEAD; zero for graph mode.
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
    /// Source nomination order: up to318 graph pages or1024 semantic closure pages.
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
        // Retain the graph allowance, cover the full semantic page closure and
        // charge semantic IDs too. Nomination reserves up to2544 slots; seed
        // completion retains at most8 slots (at most7 additions).
        let ids = (2 * rows.div_ceil(256).min(159)).max(rows.div_ceil(256).min(1024))
            + 2 * (units.min(128) + units.min(1272))
            + units.min(2544)
            + 16
            + units.min(1024)
            + 8;
        ids.checked_mul(std::mem::size_of::<usize>())
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Self>()))
            .and_then(|bytes| bytes.checked_add(2 * std::mem::size_of::<TwoBitDiscoveryTrace>()))
            .expect("bounded trace geometry")
    }
}
/// Offline source nomination and SQ8 page admission using the production planner.
/// The caller binds the prepared query and authenticated records to one source;
/// discovery, source-read charges and retained trace memory remain caller-owned.
/// The explicit nomination limit must fit the physical pages; graph production
/// retains min(total pages,159), while semantic discovery supplies its full closure.
#[doc(hidden)]
pub fn plan_two_bit_source_walks<'a>(
    rows: usize,
    dimensions: usize,
    nomination_page_limit: usize,
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
    let mut nomination_evaluated_units = Vec::with_capacity(if trace.is_some() {
        rows.div_ceil(32).min(2544)
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
    let ranked = if nomination_page_limit == rows.div_ceil(256).min(159) {
        rank_walked_source(rows, walks, score)
    } else {
        rank_walked_source_with_limit(rows, walks, nomination_page_limit, score)
    }?;
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
fn rank_walked_source(
    rows: usize,
    walks: &[(usize, Vec<usize>)],
    score: impl FnMut(usize) -> Result<f64>,
) -> Result<Vec<(usize, f64)>> {
    rank_walked_source_with_limit(rows, walks, rows.div_ceil(256).min(159), score)
}

fn rank_walked_source_with_limit(
    rows: usize,
    walks: &[(usize, Vec<usize>)],
    page_limit: usize,
    mut score: impl FnMut(usize) -> Result<f64>,
) -> Result<Vec<(usize, f64)>> {
    let invalid = || TwoBitGenerationError::Invalid("walked source geometry");
    if rows == 0
        || walks.is_empty()
        || walks.len() > 2
        || page_limit == 0
        || page_limit > rows.div_ceil(256)
    {
        return Err(invalid());
    }
    let count = page_limit;
    let mut units = Vec::with_capacity(2 * 1272);
    for (seed, evaluated) in walks {
        if *seed >= rows.div_ceil(256) || evaluated.is_empty() || evaluated.len() > 1272 {
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
    let mut memo = Vec::with_capacity(units.len());
    for unit in units {
        let value = score(unit)?;
        if !value.is_finite() {
            return Err(TwoBitGenerationError::Invalid("nonfinite source score"));
        }
        memo.push((unit, value));
    }
    fn page_maxima(mut units: Vec<(usize, f64)>) -> Vec<(usize, f64)> {
        units.sort_unstable_by_key(|&(page, _)| page);
        let mut pages = Vec::<(usize, f64)>::with_capacity(units.len());
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
    );
    let walked_count = memo.len();
    let mut completion = global.clone();
    completion.sort_unstable_by(|&(lp, l), &(rp, r)| r.total_cmp(&l).then(lp.cmp(&rp)));
    'complete: for (page, _) in completion {
        for unit in page * 8..((page + 1) * 8).min(rows.div_ceil(32)) {
            if memo.len() == 2544 {
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
    );
    let mut selected = Vec::with_capacity(2 * count);
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
        )?;
        let decode_wall_ns = decode_started.elapsed().as_nanos();
        let router_head_started = std::time::Instant::now();
        let mut router_head_requests = 0;
        if let LoadedDiscovery::Semantic { router, remote, .. } = &mut generation.discovery {
            router_head_requests = 1;
            let location = metadata_location(prefix, "router/leaves.bin");
            let head = store
                .head(&location)
                .await
                .map_err(TwoBitGenerationError::SourceHead)?;
            let etag = head
                .e_tag
                .filter(|tag| {
                    !tag.is_empty() && !tag.starts_with("W/") && !tag.chars().any(char::is_control)
                })
                .ok_or(TwoBitGenerationError::Invalid("leaf ETag"))?;
            if head.size != router.manifest().leaf_payload.bytes as u64 {
                return Err(TwoBitGenerationError::Invalid("leaf HEAD geometry"));
            }
            *remote = Some((location, etag));
        }
        generation.remote_open_stats = Some(RemoteOpenStats {
            metadata,
            staging_wall_ns,
            decode_wall_ns,
            source_head_requests: 1,
            source_head_wall_ns,
            router_head_requests,
            router_head_wall_ns: if router_head_requests == 0 {
                0
            } else {
                router_head_started.elapsed().as_nanos()
            },
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
        Self::open_inner(root, trusted_sha256, limits, None)
    }
    fn open_inner(
        root: &Path,
        trusted_sha256: &str,
        limits: TwoBitGenerationLimits,
        remote: Option<(ObjectPath, String, u64)>,
    ) -> Result<Self> {
        let paged = remote.is_some();
        let mut limits = limits;
        let bad = TwoBitGenerationError::Invalid;
        let size = |name: &str| {
            fs::metadata(root.join(name))
                .map(|m| m.len())
                .map_err(TwoBitGenerationError::Io)
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
        if manifest.discovery.mode() == DiscoveryMode::Semantic {
            limits.max_source_bytes = limits.max_source_bytes.min(64 * 1024 * 1024);
            limits.max_source_gets = limits.max_source_gets.min(128);
            limits.max_parallel_source_gets = limits.max_parallel_source_gets.min(16);
            limits.max_query_bytes = limits.max_query_bytes.min(16_773_120);
            limits.max_query_gets = limits.max_query_gets.min(32);
            limits.max_parallel_gets = limits.max_parallel_gets.min(16);
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
        // covers one sequential1400-evaluation walk plus two retained traces
        // and at most318 union-page planner vectors.
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
        let modeled = disk
            .checked_mul(3)
            .and_then(|n| n.checked_add(codec_memory))
            .and_then(|n| n.checked_add(131072))
            .and_then(|n| n.checked_add(manifest.discovery.graph_memory()?))
            .and_then(|n| {
                n.checked_add(if manifest.discovery.mode() == DiscoveryMode::Semantic {
                    // Conservative binary decoding/prototype and root/membership copies;
                    // leaf buffers/validated units for every concurrent query, no cache.
                    let root = admitted_size("router/root.bin").ok()? as u64;
                    root.checked_mul(32)?.checked_add(
                        (LEAF_BYTES as u64 * 2 + 1024 * 1024)
                            .checked_mul(limits.max_active_queries as u64)?,
                    )?
                } else {
                    0
                })
            })
            .and_then(|n| n.checked_add(query_memory))
            .and_then(|n| n.checked_add(source_query_memory))
            .and_then(|n| n.checked_add(limits.already_pinned_bytes))
            .ok_or(bad("memory overflow"))?;
        if modeled > limits.max_memory_bytes {
            return Err(bad("memory cap"));
        }
        let memory = usize::try_from(limits.max_memory_bytes).map_err(|_| bad("memory width"))?;
        let (plane, source) = if let Some((location, etag, bytes)) = remote {
            let (plane, authority) = TwoBitPlane::open_metadata(
                &root.join("plane"),
                &manifest.plane_manifest_sha256,
                &manifest.sq8_object_sha256,
                manifest.generation,
                memory,
            )
            .map_err(TwoBitGenerationError::Plane)?;
            if u64::try_from(authority.object_bytes()).map_err(|_| bad("source size"))? != bytes {
                return Err(bad("source HEAD geometry"));
            }
            (
                plane,
                Some(RemoteSource {
                    authority,
                    location,
                    etag,
                }),
            )
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
                LoadedDiscovery::Semantic {
                    router,
                    local: root.join("router/leaves.bin"),
                    remote: None,
                }
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
        })
    }
    /// Authenticated concrete discovery mode.
    pub fn discovery_mode(&self) -> DiscoveryMode {
        self.manifest.discovery.mode()
    }

    fn semantic_walks(
        router: &SemanticUnitRouter,
        ids: &[usize],
        bodies: &[Vec<u8>],
        trace: Option<&mut TwoBitPlanTrace>,
    ) -> Result<Vec<(usize, Vec<usize>)>> {
        let parts = bodies.iter().map(Vec::as_slice).collect::<Vec<_>>();
        let nomination = router
            .validate_selected(ids, &parts)
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
        let LoadedDiscovery::Semantic { router, local, .. } = &self.discovery else {
            return self.discover_walks(normalized, trace);
        };
        let ids = router
            .select_leaves(normalized)
            .map_err(|e| TwoBitGenerationError::Router(e.to_string()))?;
        let mut file = fs::File::open(local).map_err(TwoBitGenerationError::Io)?;
        if file.metadata().map_err(TwoBitGenerationError::Io)?.len()
            != router.manifest().leaf_payload.bytes as u64
        {
            return Err(TwoBitGenerationError::Invalid("leaf file length"));
        }
        let mut bodies = Vec::with_capacity(ids.len());
        use std::io::{Read, Seek, SeekFrom};
        for &id in &ids {
            let leaf = &router.manifest().leaves[id];
            file.seek(SeekFrom::Start(leaf.offset as u64))
                .map_err(TwoBitGenerationError::Io)?;
            let mut body = vec![0; leaf.bytes];
            file.read_exact(&mut body)
                .map_err(TwoBitGenerationError::Io)?;
            bodies.push(body);
        }
        Self::semantic_walks(router, &ids, &bodies, trace)
    }
    async fn remote_walks(
        &self,
        store: &dyn ObjectStore,
        normalized: &[f32],
        trace: Option<&mut TwoBitPlanTrace>,
        peak: &std::sync::atomic::AtomicUsize,
    ) -> Result<(Vec<(usize, Vec<usize>)>, Sq8ReadStats)> {
        let LoadedDiscovery::Semantic {
            router,
            remote: Some((location, etag)),
            ..
        } = &self.discovery
        else {
            return Ok((
                self.local_walks(normalized, trace)?,
                Sq8ReadStats::default(),
            ));
        };
        let ids = router
            .select_leaves(normalized)
            .map_err(|e| TwoBitGenerationError::Router(e.to_string()))?;
        let bytes = ids
            .iter()
            .map(|id| router.manifest().leaves[*id].bytes)
            .sum::<usize>();
        if ids.len() > 16 || bytes > LEAF_BYTES {
            return Err(TwoBitGenerationError::Invalid("leaf admission"));
        }
        let active = std::sync::atomic::AtomicUsize::new(0);
        struct Active<'a>(&'a std::sync::atomic::AtomicUsize);
        impl Drop for Active<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
        let active = &active;
        let outcomes = stream::iter(ids.clone().into_iter().map(|id| async move {
            let _active = Active(active);
            peak.fetch_max(
                active.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1,
                std::sync::atomic::Ordering::Relaxed,
            );
            let leaf = &router.manifest().leaves[id];
            let range = leaf.offset as u64..(leaf.offset + leaf.bytes) as u64;
            let result = store
                .get_opts(
                    location,
                    GetOptions::new()
                        .with_range(Some(range.clone()))
                        .with_if_match(Some(etag.clone())),
                )
                .await
                .map_err(TwoBitGenerationError::SourceHead)?;
            if result.range != range
                || result.meta.size != router.manifest().leaf_payload.bytes as u64
                || result.meta.e_tag.as_ref() != Some(etag)
            {
                return Err(TwoBitGenerationError::Invalid("leaf response identity"));
            }
            let mut body = Vec::with_capacity(leaf.bytes);
            let mut chunks = result.into_stream();
            while let Some(chunk) = chunks.next().await {
                let chunk = chunk.map_err(TwoBitGenerationError::SourceHead)?;
                if chunk.len() > leaf.bytes - body.len() {
                    return Err(TwoBitGenerationError::Invalid("leaf response length"));
                }
                body.extend_from_slice(&chunk);
            }
            router
                .validate_leaf(id, &body)
                .map_err(|e| TwoBitGenerationError::Router(e.to_string()))?;
            Ok(body)
        }))
        .buffered(self.limits.max_parallel_source_gets.min(16))
        .collect::<Vec<_>>()
        .await;
        // Drain every admitted read, including after the first error.
        let mut stats = Sq8ReadStats {
            submitted_gets: ids.len(),
            ..Default::default()
        };
        let mut bodies = Vec::with_capacity(ids.len());
        let mut error = None;
        for outcome in outcomes {
            match outcome {
                Ok(body) => {
                    stats.verified_bytes += body.len();
                    bodies.push(body);
                }
                Err(e) => {
                    stats.failed_gets += 1;
                    error.get_or_insert(e);
                }
            }
        }
        if let Some(error) = error {
            return Err(error.with_router(stats));
        }
        let walks = Self::semantic_walks(router, &ids, &bodies, trace)
            .map_err(|error| error.with_router(stats))?;
        // No leaf buffer survives into source cover/planning/fetch.
        Ok((walks, stats))
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
    async fn plan_paged_measured<'a>(
        &self,
        store: &dyn ObjectStore,
        query: &'a [f32],
        mut trace: Option<&mut TwoBitPlanTrace>,
        stages: &mut QueryStages,
        started: std::time::Instant,
    ) -> Result<(BudgetedPagePlan, Cow<'a, [f32]>, Sq8ReadStats, Sq8ReadStats)> {
        stages.discovery.start_ns = started.elapsed().as_nanos().max(1);
        let source = self
            .source
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
        let peak = std::sync::atomic::AtomicUsize::new(0);
        let result = self
            .remote_walks(store, normalized.as_ref(), trace.as_deref_mut(), &peak)
            .await;
        stages.discovery.end_ns = started.elapsed().as_nanos();
        stages.leaf_peak_inflight = peak.load(std::sync::atomic::Ordering::Relaxed);
        let (walks, router_stats) = result?;
        async {
            let closure = walks
                .iter()
                .flat_map(|(_, units)| units.iter().map(|unit| unit / 8))
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
            let fetched = fetch_verified_ranges_inner(
                store,
                &source.location,
                &source.authority,
                &ranges,
                &source.etag,
                self.limits.max_source_gets,
                self.limits.max_source_bytes,
                self.limits.max_parallel_source_gets,
            )
            .await;
            stages.source.end_ns = started.elapsed().as_nanos();
            let (verified, stats) = fetched.map_err(TwoBitGenerationError::SourceRead)?;
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
                "semantic plan requires leaf reader",
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
    /// Admitted diagnostic plan with semantic trace and all source/leaf charges.
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
        self.search_store_unadmitted(reader.store(), query, top_k, excluded_ids, None)
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
        self.search_store_unadmitted(store, query, top_k, excluded_ids, None)
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
            .search_store_unadmitted(store, query, top_k, None, Some(&mut trace))
            .await?;
        Ok((result, trace))
    }

    async fn search_store_unadmitted(
        &self,
        store: &dyn ObjectStore,
        query: &[f32],
        top_k: usize,
        excluded_ids: Option<&[i64]>,
        trace: Option<&mut TwoBitPlanTrace>,
    ) -> Result<TwoBitSearchResult> {
        let started = std::time::Instant::now();
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
            )
            .await;
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
    async fn search_store_measured(
        &self,
        store: &dyn ObjectStore,
        query: &[f32],
        top_k: usize,
        excluded_ids: Option<&[i64]>,
        trace: Option<&mut TwoBitPlanTrace>,
        stages: &mut QueryStages,
        started: std::time::Instant,
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
        let ranked = rank_verified_sq8_pages_inner(
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
        })
    }
}

#[cfg(test)]
mod source_walk_tests {
    use super::*;

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
        let head = crate::two_bit_store::publish_two_bit_generation(
            store.as_ref(),
            &index,
            &root,
            &root_sha,
            limits,
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
        assert_eq!(startup.router_head_requests, 1);
        assert!(startup.router_head_wall_ns > 0);
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
            assert_eq!(reads.iter().filter(|(_, head, _, _)| *head).count(), 5);
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
            let error = TwoBitGeneration::open_remote_from_head(
                store.as_ref(),
                &head,
                limits,
                scratch.path(),
            )
            .await
            .err()
            .unwrap();
            assert!(matches!(error, TwoBitGenerationError::Invalid(actual) if actual == expected));
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
        assert_eq!(leaf_reads.len(), actual_trace.semantic_leaves.len());
        for (read, &id) in leaf_reads.iter().zip(&actual_trace.semantic_leaves) {
            let leaf = &router.manifest().leaves[id];
            assert_eq!(
                read.2,
                leaf.offset as u64..(leaf.offset + leaf.bytes) as u64
            );
            assert!(read.3.is_some());
        }
        assert!(leaves.submitted_gets <= 16 && leaves.verified_bytes <= LEAF_BYTES);
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
        assert!((2..=4).contains(&actual.stages.leaf_peak_inflight));
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
        assert!(
            store
                .reads
                .lock()
                .unwrap()
                .iter()
                .all(|(name, _, _, _)| name.ends_with("router/leaves.bin"))
        );
        lazy.limits = limits;
        let leaf_key = metadata_location(&prefix, "router/leaves.bin");
        let original = fs::read(root.join("router/leaves.bin")).unwrap();
        let mut corrupt = original.clone();
        corrupt[router.manifest().leaves[actual_trace.semantic_leaves[0]].offset + 4] ^= 1;
        store
            .put(&leaf_key, PutPayload::from(corrupt))
            .await
            .unwrap();
        if let LoadedDiscovery::Semantic {
            remote: Some((_, tag)),
            ..
        } = &mut lazy.discovery
        {
            *tag = store.head(&leaf_key).await.unwrap().e_tag.unwrap();
        }
        store.reads.lock().unwrap().clear();
        let error = lazy
            .search_with_store(store.as_ref(), &query, 100, None)
            .await
            .err()
            .unwrap();
        assert_eq!(error.router_stats().submitted_gets, leaves.submitted_gets);
        assert_eq!(error.router_stats().failed_gets, 1);
        assert_eq!(
            error.read_stats(),
            (Sq8ReadStats::default(), Sq8ReadStats::default())
        );
        assert!(
            store
                .reads
                .lock()
                .unwrap()
                .iter()
                .all(|(name, _, _, _)| name.ends_with("router/leaves.bin"))
        );
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
        assert!(visible.router_stats.submitted_gets > 0);
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
            769,
            1,
            None,
            Some(DiscoveryMode::Graph),
        )
        .await
        .unwrap();
        let mutations = apply_two_bit_mutations(
            store.as_ref(),
            &wide,
            769,
            None,
            &[TwoBitMutation {
                id: 17,
                vector: Some(vec![1.; 769]),
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
            769,
            Some(&mutations),
            &[TwoBitMutation {
                id: 18,
                vector: Some(vec![1.; 769]),
            }],
            mutation_limits,
        )
        .await
        .expect("unsupported semantic request must leave mutations writable");
    }

    #[tokio::test]
    async fn paged_source_matches_reference_and_preserves_failure_charges() {
        assert_paged_source(513, false).await;
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
        assert_paged_source(262_145, true).await;
    }

    async fn assert_paged_source(rows: usize, fragmented: bool) {
        use object_store::PutPayload;
        use sha2::{Digest, Sha256};
        let hash = |body: &[u8]| format!("{:x}", Sha256::digest(body));
        let temp = tempfile::tempdir().unwrap();
        let raw = (0..rows)
            .flat_map(|id| {
                [
                    1.0_f32 + (id % 7) as f32 * 0.2,
                    0.1 + (id % 11) as f32 * 0.1,
                ]
                .into_iter()
                .flat_map(f32::to_le_bytes)
            })
            .collect::<Vec<_>>();
        let sq8 = (0..rows)
            .flat_map(|id| {
                let mut record = (id as i64).to_le_bytes().to_vec();
                record.extend_from_slice(&5.0_f32.to_le_bytes());
                record.extend_from_slice(&[1, 2]);
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
                dimensions: 2,
            },
            base_epoch: 0,
            generation: 7,
            low: &[0.0; 2],
            step: &[1.0; 2],
            sq8_object_key: key.as_ref(),
            sq8_etag: &etag,
        }
        .build(&root, 256_000_000)
        .unwrap();
        let limits = TwoBitGenerationLimits {
            max_memory_bytes: 256_000_000,
            max_active_queries: 1,
            max_query_bytes: rows * 14,
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
        let query = [0.5, 0.25];
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
            plan_two_bit_source_walks(257, 2, 2, &walks, &prepared, |_| None, 32, 257 * 14, None),
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
}
