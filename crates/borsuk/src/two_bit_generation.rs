//! A single authenticated root for frozen two-bit nomination and on-demand SQ8.
use crate::{
    budgeted_page_rank::{BudgetedPageError, BudgetedPagePlan, choose_budgeted_pages_sparse},
    canonical_source::CanonicalSource,
    object_native_generation::{
        ObjectNativeOpenError, ObjectNativeSearchResult, stage_generation_metadata,
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
    pub(crate) plane_manifest_sha256: String,
    pub(crate) page_manifest_sha256: String,
    pub(crate) centroids_sha256: String,
    pub(crate) graph_sha256: String,
    pub(crate) graph_resident_bytes: usize,
    pub(crate) sq8_object_sha256: String,
    pub(crate) sq8_object_key: String,
    pub(crate) sq8_etag: String,
    pub(crate) canonical: CanonicalSource,
    pub(crate) low: Vec<f32>,
    pub(crate) step: Vec<f32>,
}
pub(crate) const METADATA_FILES: [&str; 8] = [
    "manifest.json",
    "page_manifest.json",
    "page_digests.bin",
    "centroids.bin",
    "graph.bin",
    "plane/manifest.json",
    "plane/mean.bin",
    "plane/records.bin",
];
/// Immutable metadata; SQ8 rows are fetched conditionally and never cached here.
pub struct TwoBitGeneration {
    root_sha256: String,
    modeled_memory_bytes: u64,
    plane: TwoBitPlane,
    pages: PageAuthority,
    centroids: UnitCentroidPages,
    graph: UnitCentroidGraph,
    manifest: Manifest,
    limits: TwoBitGenerationLimits,
    slots: Semaphore,
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
        let scratch = stage_generation_metadata(
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
        Self::open(scratch.path(), trusted_sha256, limits)
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
        if manifest.schema != "borsuk-two-bit-generation-v2"
            || manifest.generation == 0
            || manifest.graph_resident_bytes == 0
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
        // covers the capped 1400 graph evaluations and 159-page planner vectors.
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
        let graph_blob = read("graph.bin", &manifest.graph_sha256)?;
        if UnitCentroidGraph::preflight_resident_bytes(&graph_blob, &centroids)
            .map_err(TwoBitGenerationError::Graph)?
            != manifest.graph_resident_bytes
        {
            return Err(bad("graph memory declaration"));
        }
        let graph = UnitCentroidGraph::decode_bounded(
            &graph_blob,
            &centroid_blob,
            &centroids,
            manifest.graph_resident_bytes,
        )
        .map_err(TwoBitGenerationError::Graph)?;
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
            graph,
            manifest,
            limits,
            slots: Semaphore::new(limits.max_active_queries),
        })
    }
    fn plan_inner<'a>(
        &self,
        query: &'a [f32],
        ranking: Option<&mut Vec<usize>>,
    ) -> Result<(BudgetedPagePlan, Cow<'a, [f32]>)> {
        let count = self.pages.rows().div_ceil(256).min(159);
        let trace_bytes = if ranking.is_some() {
            count * std::mem::size_of::<usize>()
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
        let seed = self
            .graph
            .search(&self.centroids, normalized.as_ref(), 1, 128)
            .map_err(TwoBitGenerationError::Graph)?;
        let seed_page = seed
            .units
            .first()
            .ok_or(TwoBitGenerationError::Invalid("no seed"))?
            .0
            / 8;
        let mut candidates = vec![seed_page];
        if count > 1 {
            let found = self
                .graph
                .search_pages_seeded(
                    &self.centroids,
                    normalized.as_ref(),
                    &[seed_page],
                    count - 1,
                    1272,
                )
                .map_err(TwoBitGenerationError::Graph)?;
            candidates.extend(found.pages.iter().map(|&(p, _)| p));
        }
        candidates.sort_unstable();
        if candidates.len() != count || candidates.windows(2).any(|p| p[0] >= p[1]) {
            return Err(TwoBitGenerationError::Invalid("candidate geometry"));
        }
        let mut ranked = Vec::with_capacity(count);
        for page in candidates {
            let mut score = f64::NEG_INFINITY;
            for row in page * 256..((page + 1) * 256).min(self.pages.rows()) {
                score = score.max(
                    prepared
                        .score(self.plane.record(row).unwrap())
                        .map_err(|e| TwoBitGenerationError::Plane(SourceBuildError::Codec(e)))?,
                );
            }
            ranked.push((page, score));
        }
        ranked.sort_unstable_by(|&(lp, l), &(rp, r)| r.total_cmp(&l).then(lp.cmp(&rp)));
        if let Some(ranking) = ranking {
            ranking.extend(ranked.iter().map(|&(page, _)| page));
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
    /// Reserves at most159 page IDs from query scratch; uses normal admission.
    /// Returned trace payload belongs to the caller after slot release.
    #[doc(hidden)]
    pub async fn diagnostic_plan(&self, query: &[f32]) -> Result<(BudgetedPagePlan, Vec<usize>)> {
        let _permit = self
            .slots
            .acquire()
            .await
            .map_err(|_| TwoBitGenerationError::Invalid("query admission"))?;
        let mut ranking = Vec::with_capacity(self.pages.rows().div_ceil(256).min(159));
        let (plan, _) = self.plan_inner(query, Some(&mut ranking))?;
        Ok((plan, ranking))
    }
    /// Frozen V296 nomination and physical admission, under a shared query slot.
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
