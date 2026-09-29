//! A single authenticated root for frozen two-bit nomination and on-demand SQ8.
use crate::{
    budgeted_page_rank::{BudgetedPageError, BudgetedPagePlan, choose_budgeted_pages_sparse},
    canonical_source::CanonicalSource,
    object_native_generation::{
        MetadataReadStats, ObjectNativeOpenError, ObjectNativeSearchResult,
        stage_generation_metadata,
    },
    sq8_page_authority::{PageAuthority, PageError},
    sq8_s3_range::{OneAttemptS3, RankedSq8Failure, Sq8ReadStats},
    two_bit_mutations::{TwoBitMutationHit, TwoBitMutationSnapshot},
    two_bit_source::{SourceBuildError, SourcePlaneReceipt, TwoBitPlane, read_authenticated},
    unit_centroid_graph::{UnitCentroidGraph, UnitCentroidGraphError},
    unit_centroid_pages::{UnitCentroidError, UnitCentroidPages},
};
use object_store::{ObjectStore, path::Path as ObjectPath};
use serde::Deserialize;
use std::{borrow::Cow, fs, path::Path};
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
    /// Codec lookup scratch per query.
    pub max_query_scratch_bytes: usize,
    /// Payload charged to other pinned generations and immutable mutation snapshots.
    pub already_pinned_bytes: u64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Manifest {
    pub(crate) schema: String,
    pub(crate) generation: u64,
    pub(crate) base_epoch: u64,
    pub(crate) plane_manifest_sha256: String,
    pub(crate) page_manifest_sha256: String,
    pub(crate) centroids_sha256: String,
    pub(crate) graph_sha256: String,
    pub(crate) graph_resident_bytes: usize,
    pub(crate) diverse_graph_sha256: String,
    pub(crate) diverse_graph_resident_bytes: usize,
    pub(crate) sq8_object_sha256: String,
    pub(crate) sq8_object_key: String,
    pub(crate) sq8_etag: String,
    pub(crate) canonical: CanonicalSource,
    pub(crate) low: Vec<f32>,
    pub(crate) step: Vec<f32>,
}
pub(crate) const SCHEMA: &str = "borsuk-two-bit-generation-v4";
pub(crate) const METADATA_FILES: [&str; 9] = [
    "manifest.json",
    "page_manifest.json",
    "page_digests.bin",
    "centroids.bin",
    "graph.bin",
    "diverse_graph.bin",
    "plane/manifest.json",
    "plane/mean.bin",
    "plane/records.bin",
];
/// Remote namespace startup accounting; query measurements remain separate.
#[derive(Debug, serde::Serialize)]
pub struct RemoteOpenStats {
    /// Fixed, bounded roster of metadata-object transfers.
    pub metadata: Vec<MetadataReadStats>,
    /// All metadata staging, including scratch creation and transfers.
    pub staging_wall_ns: u128,
    /// Authenticated local decoding and admission after staging.
    pub decode_wall_ns: u128,
}

/// Immutable metadata; SQ8 rows are fetched conditionally and never cached here.
pub struct TwoBitGeneration {
    root_sha256: String,
    modeled_memory_bytes: u64,
    plane: TwoBitPlane,
    pages: PageAuthority,
    centroids: UnitCentroidPages,
    graphs: [UnitCentroidGraph; 2],
    manifest: Manifest,
    limits: TwoBitGenerationLimits,
    slots: Semaphore,
    remote_open_stats: Option<RemoteOpenStats>,
}

/// One generation-pinned base fetch merged with a bounded immutable delta.
pub struct TwoBitMutationSearchResult {
    /// Physical base-page admission, unchanged by mutation visibility.
    pub plan: BudgetedPagePlan,
    /// Up to k visible logical candidates ordered by score then ID.
    pub candidates: Vec<TwoBitMutationHit>,
    /// Base-query GETs/bytes/failures; earlier snapshot recovery is lifecycle I/O.
    pub stats: Sq8ReadStats,
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
    /// Source two-bit max-row nomination order of up to318 union pages.
    pub ranked_candidate_pages: Vec<usize>,
    /// Globally distinct32-row units source-scored, including bounded completion.
    pub nomination_evaluated_units: Vec<usize>,
    /// First ranked page, required by physical admission.
    pub primary_page: usize,
    /// Nearest then diversity discovery; identical graph identities run once.
    pub discoveries: Vec<TwoBitDiscoveryTrace>,
}
impl TwoBitPlanTrace {
    /// Conservative retained ID/record payload excluding allocator overhead.
    pub fn scratch_bytes(rows: usize) -> usize {
        let units = rows.div_ceil(32);
        std::mem::size_of::<Self>()
            + 2 * std::mem::size_of::<TwoBitDiscoveryTrace>()
            + 2 * (rows.div_ceil(256).min(159) + units.min(128) + units.min(1272))
                * std::mem::size_of::<usize>()
            + units.min(2544) * std::mem::size_of::<usize>()
    }
}
// Source nomination over bounded graph walks; no corpus-sized query allocation.
fn rank_walked_source(
    rows: usize,
    walks: &[(usize, Vec<usize>)],
    mut score: impl FnMut(usize) -> Result<f64>,
) -> Result<Vec<(usize, f64)>> {
    let invalid = || TwoBitGenerationError::Invalid("walked source geometry");
    if rows == 0 || walks.is_empty() || walks.len() > 2 {
        return Err(invalid());
    }
    let count = rows.div_ceil(256).min(159);
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
        let staging_started = std::time::Instant::now();
        let (scratch, metadata) = stage_generation_metadata(
            store,
            prefix,
            trusted_sha256,
            limits
                .max_memory_bytes
                .saturating_sub(limits.already_pinned_bytes),
            &METADATA_FILES,
            scratch_parent,
        )
        .await
        .map_err(TwoBitGenerationError::Stage)?;
        let staging_wall_ns = staging_started.elapsed().as_nanos();
        let decode_started = std::time::Instant::now();
        let mut generation = Self::open(scratch.path(), trusted_sha256, limits)?;
        generation.remote_open_stats = Some(RemoteOpenStats {
            metadata,
            staging_wall_ns,
            decode_wall_ns: decode_started.elapsed().as_nanos(),
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
        let bad = TwoBitGenerationError::Invalid;
        let size = |name: &str| {
            fs::metadata(root.join(name))
                .map(|m| m.len())
                .map_err(TwoBitGenerationError::Io)
        };
        let manifest_size = size("manifest.json")?;
        if manifest_size == 0
            || manifest_size > 65536
            || limits.max_memory_bytes < 131072
            || limits.max_active_queries == 0
            || limits.max_query_bytes == 0
            || limits.max_query_gets == 0
            || limits.max_parallel_gets == 0
            || limits.max_query_scratch_bytes == 0
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
            || manifest.graph_resident_bytes == 0
            || manifest.diverse_graph_resident_bytes == 0
            || manifest.sq8_etag.is_empty()
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
        let files = [
            "plane/manifest.json",
            "plane/mean.bin",
            "plane/records.bin",
            "page_manifest.json",
            "page_digests.bin",
            "centroids.bin",
            "graph.bin",
            "diverse_graph.bin",
        ];
        let mut disk = manifest_size;
        let mut sizes = Vec::with_capacity(files.len());
        for name in files {
            let length = size(name)?;
            disk = disk.checked_add(length).ok_or(bad("metadata size"))?;
            sizes.push((name, length));
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
            || admitted_size("plane/records.bin")? != expected_records
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
        let modeled = disk
            .checked_mul(3)
            .and_then(|n| n.checked_add(codec_memory))
            .and_then(|n| n.checked_add(131072))
            .and_then(|n| n.checked_add(manifest.graph_resident_bytes as u64))
            .and_then(|n| n.checked_add(manifest.diverse_graph_resident_bytes as u64))
            .and_then(|n| n.checked_add(query_memory))
            .and_then(|n| n.checked_add(limits.already_pinned_bytes))
            .ok_or(bad("memory overflow"))?;
        if modeled > limits.max_memory_bytes {
            return Err(bad("memory cap"));
        }
        let plane = TwoBitPlane::open(
            &root.join("plane"),
            &manifest.plane_manifest_sha256,
            &manifest.sq8_object_sha256,
            usize::try_from(limits.max_memory_bytes).map_err(|_| bad("memory width"))?,
        )
        .map_err(TwoBitGenerationError::Plane)?;
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
        let centroid_blob = read("centroids.bin", &manifest.centroids_sha256)?;
        let centroids =
            UnitCentroidPages::decode(&centroid_blob).map_err(TwoBitGenerationError::Centroid)?;
        let mut graphs = Vec::with_capacity(2);
        for (name, digest, resident) in [
            (
                "graph.bin",
                manifest.graph_sha256.as_str(),
                manifest.graph_resident_bytes,
            ),
            (
                "diverse_graph.bin",
                manifest.diverse_graph_sha256.as_str(),
                manifest.diverse_graph_resident_bytes,
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
                UnitCentroidGraph::decode_bounded(&blob, &centroid_blob, &centroids, resident)
                    .map_err(TwoBitGenerationError::Graph)?,
            );
        }
        let graphs: [UnitCentroidGraph; 2] = graphs.try_into().map_err(|_| bad("graph roster"))?;
        let receipt = plane.receipt();
        if pages.generation() != manifest.generation
            || pages.object_sha256() != manifest.sq8_object_sha256
            || pages.rows() != receipt.rows
            || pages.dimensions() != receipt.dimensions
            || pages.page_rows() != 256
            || centroids.rows() != receipt.rows
            || centroids.dimensions() != receipt.dimensions
            || centroids.page_rows() != 256
            || centroids.unit_rows() != 32
            || manifest.low.len() != receipt.dimensions
        {
            return Err(bad("generation binding"));
        }
        Ok(Self {
            root_sha256: trusted_sha256.into(),
            modeled_memory_bytes: modeled,
            plane,
            pages,
            centroids,
            graphs,
            manifest,
            limits,
            slots: Semaphore::new(limits.max_active_queries),
            remote_open_stats: None,
        })
    }
    fn plan_inner<'a>(
        &self,
        query: &'a [f32],
        mut trace: Option<&mut TwoBitPlanTrace>,
    ) -> Result<(BudgetedPagePlan, Cow<'a, [f32]>)> {
        let count = self.pages.rows().div_ceil(256).min(159);
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
        let normalized =
            crate::sq8_source::cosine_vector(query).map_err(TwoBitGenerationError::Plane)?;
        let mut walks = Vec::with_capacity(2);
        let graph_count = if self.manifest.graph_sha256 == self.manifest.diverse_graph_sha256 {
            1
        } else {
            2
        };
        for graph in &self.graphs[..graph_count] {
            let seed = graph
                .search(&self.centroids, normalized.as_ref(), 1, 128)
                .map_err(TwoBitGenerationError::Graph)?;
            let seed_page = seed
                .units
                .first()
                .ok_or(TwoBitGenerationError::Invalid("no seed"))?
                .0
                / 8;
            let (evaluated, exhausted) = if count > 1 {
                let found = graph
                    .search_pages_seeded(
                        &self.centroids,
                        normalized.as_ref(),
                        &[seed_page],
                        count - 1,
                        1272,
                    )
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
        let mut nomination_evaluated_units =
            Vec::with_capacity(if trace.is_some() { 2544 } else { 0 });
        let ranked = rank_walked_source(self.pages.rows(), &walks, |unit| {
            if trace.is_some() {
                nomination_evaluated_units.push(unit);
            }
            let mut maximum = f64::NEG_INFINITY;
            for row in unit * 32..((unit + 1) * 32).min(self.pages.rows()) {
                maximum = maximum.max(
                    prepared
                        .score(self.plane.record(row).unwrap())
                        .map_err(|e| TwoBitGenerationError::Plane(SourceBuildError::Codec(e)))?,
                );
            }
            Ok(maximum)
        })?;
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
            self.pages.rows(),
            self.pages.dimensions(),
            self.pages.rows().div_ceil(256),
            self.limits.max_query_gets,
            self.limits.max_query_bytes,
        )
        .map(|plan| (plan, normalized))
        .map_err(TwoBitGenerationError::Budget)
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
    /// Shared-source two-graph discovery union, nomination and physical admission.
    pub async fn plan(&self, query: &[f32]) -> Result<BudgetedPagePlan> {
        let _permit = self
            .slots
            .acquire()
            .await
            .map_err(|_| TwoBitGenerationError::Invalid("query admission"))?;
        self.plan_inner(query, None).map(|(plan, _)| plan)
    }
    /// Plan, fetch authenticated conditional ranges in one bounded wave, and rank.
    /// Reader errors retain physical request/byte/error accounting.
    pub async fn search(
        &self,
        reader: &OneAttemptS3,
        query: &[f32],
        top_k: usize,
    ) -> Result<ObjectNativeSearchResult> {
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
    ) -> Result<ObjectNativeSearchResult> {
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
            .search_unadmitted(
                reader,
                query,
                top_k.min(self.pages.rows()),
                Some(mutations.excluded_ids()),
            )
            .await?;
        let candidates = mutations
            .rank_with_base(&base.ranked.candidates, query, top_k)
            .map_err(|error| {
                TwoBitGenerationError::Read(RankedSq8Failure {
                    error: crate::sq8_s3_range::RangeFetchError::Score(error),
                    stats: base.ranked.stats,
                })
            })?;
        Ok(TwoBitMutationSearchResult {
            plan: base.plan,
            candidates,
            stats: base.ranked.stats,
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
    ) -> Result<ObjectNativeSearchResult> {
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
    ) -> Result<ObjectNativeSearchResult> {
        let (plan, normalized) = self.plan_inner(query, None)?;
        let page_bytes = 256 * (self.pages.dimensions() + 12);
        let ranges = plan
            .ranges
            .iter()
            .map(|r| (r.start / page_bytes, (r.end - 1) / page_bytes))
            .collect::<Vec<_>>();
        let ranked = reader
            .rank_verified_sq8_pages_excluding(
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
            .await
            .map_err(TwoBitGenerationError::Read)?;
        if excluded_ids.is_none() && ranked.candidates.len() < top_k {
            return Err(TwoBitGenerationError::Read(RankedSq8Failure {
                error: crate::sq8_s3_range::RangeFetchError::Score(
                    crate::exact_sq8_nominee::Sq8ScoreError::InvalidRoster,
                ),
                stats: ranked.stats,
            }));
        }
        Ok(ObjectNativeSearchResult { plan, ranked })
    }
}

#[cfg(test)]
mod source_walk_tests {
    use super::*;
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
