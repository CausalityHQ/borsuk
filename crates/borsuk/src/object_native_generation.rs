//! A source-only router and authenticated remote SQ8 pages for one generation.

use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::Read;
use std::path::Path;

use object_store::path::Path as ObjectPath;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::sync::Semaphore;

use crate::budgeted_page_rank::{
    BudgetedPageError, BudgetedPagePlan, choose_budgeted_pages_sparse,
};
use crate::pq64_nominee::Pq64Error;
use crate::pq64_router_artifact::{RouterArtifactError, SourceRouterArtifact, load_source_router};
use crate::sq8_page_authority::{PageAuthority, PageError};
use crate::sq8_s3_range::{
    OneAttemptS3, RangeFetchError, RankedSq8, RankedSq8Failure, Sq8ReadStats,
};
use crate::unit_centroid_graph::{UnitCentroidGraph, UnitCentroidGraphError};
use crate::unit_centroid_pages::{UnitCentroidError, UnitCentroidPages};

const MAX_MANIFEST: usize = 64 * 1024;

#[derive(Debug)]
pub enum ObjectNativeOpenError {
    Io(std::io::Error),
    Invalid(&'static str),
    HashMismatch(&'static str),
    Router(RouterArtifactError),
    Page(PageError),
    Centroid(UnitCentroidError),
    Graph(UnitCentroidGraphError),
}

#[derive(Debug)]
pub enum ObjectNativePlanError {
    Router(Pq64Error),
    Graph(UnitCentroidGraphError),
    Centroid(UnitCentroidError),
    Budget(BudgetedPageError),
    Arithmetic,
}

#[derive(Debug)]
pub enum ObjectNativeSearchError {
    Plan(ObjectNativePlanError),
    Read(RankedSq8Failure),
}

pub struct ObjectNativeSearchResult {
    pub plan: BudgetedPagePlan,
    pub ranked: RankedSq8,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    schema: String,
    generation: u64,
    rows: usize,
    dimensions: usize,
    page_rows: usize,
    unit_rows: usize,
    router_manifest_sha256: String,
    page_manifest_sha256: String,
    centroids_sha256: String,
    graph_sha256: String,
    graph_resident_bytes: u64,
    sq8_object_sha256: String,
    sq8_object_key: String,
    sq8_etag: String,
}

/// Admission estimate for loading overlap and concurrent query buffers.
/// Measure cgroup RSS separately; allocator and transport overhead can differ.
#[derive(Clone, Copy)]
pub struct ObjectNativeLimits {
    pub max_memory_bytes: u64,
    pub max_active_queries: u64,
    pub max_query_bytes: u64,
    pub max_query_gets: usize,
    pub max_parallel_gets: usize,
    pub max_router_regions: usize,
    pub max_router_shortlist: usize,
    pub already_pinned_bytes: u64,
}

/// Immutable router, page hashes and conditional remote object identity.
/// The SQ8 vector plane is never loaded by this generation.
pub struct ObjectNativeGeneration {
    router: SourceRouterArtifact,
    pages: PageAuthority,
    centroids: UnitCentroidPages,
    graph: UnitCentroidGraph,
    object_key: ObjectPath,
    etag: String,
    max_query_bytes: usize,
    max_query_gets: usize,
    max_parallel_gets: usize,
    max_router_regions: usize,
    max_router_shortlist: usize,
    query_slots: Semaphore,
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn is_hash(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn read_capped(path: &Path, cap: u64) -> Result<Vec<u8>, ObjectNativeOpenError> {
    let file = File::open(path).map_err(ObjectNativeOpenError::Io)?;
    let size = file.metadata().map_err(ObjectNativeOpenError::Io)?.len();
    if size == 0 || size > cap {
        return Err(ObjectNativeOpenError::Invalid("artifact length"));
    }
    let mut bytes = Vec::new();
    file.take(cap.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(ObjectNativeOpenError::Io)?;
    if bytes.len() as u64 != size {
        return Err(ObjectNativeOpenError::Invalid("artifact length"));
    }
    Ok(bytes)
}

impl ObjectNativeGeneration {
    /// Open local routing metadata only. `trusted_sha256` must come from an
    /// authorized generation pointer, outside this API's trust boundary.
    pub fn open(
        root: &Path,
        trusted_sha256: &str,
        limits: ObjectNativeLimits,
    ) -> Result<Self, ObjectNativeOpenError> {
        if !is_hash(trusted_sha256) {
            return Err(ObjectNativeOpenError::Invalid("trusted digest"));
        }
        let raw = read_capped(&root.join("manifest.json"), MAX_MANIFEST as u64)?;
        if sha256(&raw) != trusted_sha256 {
            return Err(ObjectNativeOpenError::HashMismatch("generation manifest"));
        }
        let manifest: Manifest = serde_json::from_slice(&raw)
            .map_err(|_| ObjectNativeOpenError::Invalid("generation schema"))?;
        if manifest.schema != "borsuk-object-native-generation-v1"
            || manifest.generation == 0
            || manifest.rows == 0
            || manifest.dimensions == 0
            || manifest.page_rows != 256
            || manifest.unit_rows == 0
            || manifest.page_rows % manifest.unit_rows != 0
            || !is_hash(&manifest.router_manifest_sha256)
            || !is_hash(&manifest.page_manifest_sha256)
            || !is_hash(&manifest.centroids_sha256)
            || !is_hash(&manifest.graph_sha256)
            || manifest.graph_resident_bytes == 0
            || !is_hash(&manifest.sq8_object_sha256)
            || manifest.sq8_object_key != format!("objects/{}", manifest.sq8_object_sha256)
            || manifest.sq8_etag.is_empty()
            || manifest.sq8_etag.chars().any(char::is_control)
            || limits.max_active_queries == 0
            || limits.max_query_bytes == 0
            || limits.max_query_gets == 0
            || limits.max_parallel_gets == 0
            || limits.max_router_regions == 0
            || limits.max_router_regions > manifest.rows.div_ceil(manifest.page_rows)
            || limits.max_router_shortlist == 0
            || limits.max_router_shortlist > manifest.rows
            || limits.max_active_queries > usize::MAX as u64
            || limits.max_query_bytes > usize::MAX as u64
        {
            return Err(ObjectNativeOpenError::Invalid("generation identity"));
        }
        let router_root = root.join("router");
        let router_manifest = read_capped(&router_root.join("manifest.json"), MAX_MANIFEST as u64)?;
        if sha256(&router_manifest) != manifest.router_manifest_sha256 {
            return Err(ObjectNativeOpenError::HashMismatch("router manifest"));
        }
        let page_manifest = read_capped(&root.join("page_manifest.json"), MAX_MANIFEST as u64)?;
        if sha256(&page_manifest) != manifest.page_manifest_sha256 {
            return Err(ObjectNativeOpenError::HashMismatch("page manifest"));
        }
        let page_count = manifest.rows.div_ceil(manifest.page_rows);
        let sidecar_bytes = page_count
            .checked_mul(32)
            .ok_or(ObjectNativeOpenError::Invalid("page count"))?;
        let sidecar_size = fs::metadata(root.join("page_digests.bin"))
            .map_err(ObjectNativeOpenError::Io)?
            .len();
        if sidecar_size != sidecar_bytes as u64 {
            return Err(ObjectNativeOpenError::Invalid("page digest length"));
        }
        let centroid_size = fs::metadata(root.join("centroids.bin"))
            .map_err(ObjectNativeOpenError::Io)?
            .len();
        let expected_centroid_size = manifest
            .rows
            .div_ceil(manifest.unit_rows)
            .checked_mul(manifest.dimensions)
            .and_then(|v| v.checked_mul(2))
            .and_then(|v| v.checked_add(32))
            .ok_or(ObjectNativeOpenError::Invalid("centroid size"))?;
        if centroid_size != expected_centroid_size as u64 {
            return Err(ObjectNativeOpenError::Invalid("centroid length"));
        }
        let graph_size = fs::metadata(root.join("graph.bin"))
            .map_err(ObjectNativeOpenError::Io)?
            .len();
        // Three copies allow authenticated reads and decoding. Three query
        // buffers cover response collection, transport and scoring scratch.
        // PQ nomination materializes one tuple per selected-region row.
        // Retiring generations are charged through `already_pinned_bytes`.
        let router_disk_bytes = ["summaries", "books", "codes", "low", "step"]
            .into_iter()
            .try_fold(0_u64, |total, name| {
                let size = fs::metadata(router_root.join(format!("{name}.bin")))
                    .map_err(ObjectNativeOpenError::Io)?
                    .len();
                total
                    .checked_add(size)
                    .ok_or(ObjectNativeOpenError::Invalid("router size"))
            })?;
        let planner_bytes = page_count
            .checked_mul(16)
            .and_then(|v| {
                v.checked_add(
                    limits
                        .max_router_regions
                        .checked_mul(manifest.page_rows)?
                        .checked_mul(16)?,
                )
            })
            .and_then(|v| v.checked_add(limits.max_router_shortlist.checked_mul(1200)?))
            .and_then(|v| v.checked_add(64 * 256 * 4))
            .ok_or(ObjectNativeOpenError::Invalid("planner memory"))?
            as u64;
        let modeled = router_disk_bytes
            .checked_add(sidecar_size)
            .and_then(|v| v.checked_mul(3))
            .and_then(|v| v.checked_add(centroid_size.checked_mul(3)?))
            .and_then(|v| v.checked_add(graph_size))
            .and_then(|v| v.checked_add(manifest.graph_resident_bytes))
            .and_then(|v| {
                v.checked_add(
                    limits
                        .max_active_queries
                        .checked_mul(limits.max_query_bytes)?
                        .checked_mul(3)?
                        .checked_add(planner_bytes)?,
                )
            })
            .and_then(|v| v.checked_add(limits.already_pinned_bytes))
            .ok_or(ObjectNativeOpenError::Invalid("memory arithmetic"))?;
        if modeled > limits.max_memory_bytes {
            return Err(ObjectNativeOpenError::Invalid("memory cap"));
        }
        let router = load_source_router(&router_root, &manifest.router_manifest_sha256)
            .map_err(ObjectNativeOpenError::Router)?;
        let sidecar = read_capped(&root.join("page_digests.bin"), sidecar_size)?;
        let pages = PageAuthority::load(&page_manifest, &manifest.page_manifest_sha256, &sidecar)
            .map_err(ObjectNativeOpenError::Page)?;
        let centroid_blob = read_capped(&root.join("centroids.bin"), centroid_size)?;
        if sha256(&centroid_blob) != manifest.centroids_sha256 {
            return Err(ObjectNativeOpenError::HashMismatch("centroids"));
        }
        let centroids =
            UnitCentroidPages::decode(&centroid_blob).map_err(ObjectNativeOpenError::Centroid)?;
        let graph_blob = read_capped(&root.join("graph.bin"), graph_size)?;
        if sha256(&graph_blob) != manifest.graph_sha256 {
            return Err(ObjectNativeOpenError::HashMismatch("graph"));
        }
        let graph = UnitCentroidGraph::decode_bounded(
            &graph_blob,
            &centroid_blob,
            &centroids,
            usize::try_from(manifest.graph_resident_bytes)
                .map_err(|_| ObjectNativeOpenError::Invalid("graph memory"))?,
        )
        .map_err(ObjectNativeOpenError::Graph)?;
        if UnitCentroidGraph::preflight_resident_bytes(&graph_blob, &centroids)
            .map_err(ObjectNativeOpenError::Graph)? as u64
            != manifest.graph_resident_bytes
        {
            return Err(ObjectNativeOpenError::Invalid("graph resident declaration"));
        }
        if router.generation != manifest.generation
            || router.router.rows() != manifest.rows
            || router.router.dimensions() != manifest.dimensions
            || router.router.page_rows() != manifest.page_rows
            || router.sq8_sha256 != manifest.sq8_object_sha256
            || pages.generation() != manifest.generation
            || pages.rows() != manifest.rows
            || pages.dimensions() != manifest.dimensions
            || pages.page_rows() != manifest.page_rows
            || pages.object_sha256() != manifest.sq8_object_sha256
            || centroids.rows() != manifest.rows
            || centroids.dimensions() != manifest.dimensions
            || centroids.page_rows() != manifest.page_rows
            || centroids.unit_rows() != manifest.unit_rows
            || graph.node_count() != centroids.unit_count()
        {
            return Err(ObjectNativeOpenError::Invalid("generation binding"));
        }
        Ok(Self {
            router,
            pages,
            centroids,
            graph,
            object_key: ObjectPath::from(manifest.sq8_object_key),
            etag: manifest.sq8_etag,
            max_query_bytes: limits.max_query_bytes as usize,
            max_query_gets: limits.max_query_gets,
            max_parallel_gets: limits.max_parallel_gets,
            max_router_regions: limits.max_router_regions,
            max_router_shortlist: limits.max_router_shortlist,
            query_slots: Semaphore::new(limits.max_active_queries as usize),
        })
    }

    pub fn router(&self) -> &SourceRouterArtifact {
        &self.router
    }
    pub fn pages(&self) -> &PageAuthority {
        &self.pages
    }
    pub fn centroids(&self) -> &UnitCentroidPages {
        &self.centroids
    }
    pub fn graph(&self) -> &UnitCentroidGraph {
        &self.graph
    }
    pub fn object_key(&self) -> &ObjectPath {
        &self.object_key
    }
    pub fn etag(&self) -> &str {
        &self.etag
    }

    /// PQ-primary nomination followed by cached sparse graph page expansion.
    /// Historical V155 used local SQ8-exact primaries; its recall does not
    /// transfer to this cold object-native proxy without a new measurement.
    /// The caller freezes `regions`, `shortlist`, `primary_count` and `beta`.
    pub fn plan_pages(
        &self,
        query: &[f32],
        regions: usize,
        shortlist: usize,
        primary_count: usize,
        beta: usize,
    ) -> Result<BudgetedPagePlan, ObjectNativePlanError> {
        if regions > self.max_router_regions
            || shortlist > self.max_router_shortlist
            || primary_count == 0
            || primary_count > shortlist
        {
            return Err(ObjectNativePlanError::Router(Pq64Error::InvalidRequest));
        }
        let nominees = self
            .router
            .router
            .nominate(query, regions, shortlist)
            .map_err(ObjectNativePlanError::Router)?;
        let primary = &nominees[..primary_count];
        let primary_pages = primary
            .iter()
            .map(|row| row / self.pages.page_rows())
            .collect::<BTreeSet<_>>();
        let p = primary_pages.len();
        let found = self
            .graph
            .search_pages_seeded_with_scores(
                &self.centroids,
                query,
                &primary_pages.iter().copied().collect::<Vec<_>>(),
                p.checked_mul(4).ok_or(ObjectNativePlanError::Arithmetic)?,
                p.checked_mul(16).ok_or(ObjectNativePlanError::Arithmetic)?,
            )
            .map_err(ObjectNativePlanError::Graph)?;
        let mut pages = primary_pages;
        pages.extend(found.pages.iter().map(|&(page, _)| page));
        let (scored, _) = self
            .centroids
            .score_pages_sparse_cached(
                query,
                &pages.into_iter().collect::<Vec<_>>(),
                &found.evaluated_scores,
            )
            .map_err(ObjectNativePlanError::Centroid)?;
        choose_budgeted_pages_sparse(
            &scored,
            primary,
            self.pages.rows(),
            self.pages.dimensions(),
            beta,
            self.max_query_gets,
            self.max_query_bytes,
        )
        .map_err(ObjectNativePlanError::Budget)
    }

    /// Plan, fetch and rank from one pinned generation without a vector cache.
    pub async fn search(
        &self,
        reader: &OneAttemptS3,
        query: &[f32],
        regions: usize,
        shortlist: usize,
        beta: usize,
        top_k: usize,
    ) -> Result<ObjectNativeSearchResult, ObjectNativeSearchError> {
        let _permit = self.query_slots.acquire().await.map_err(|_| {
            ObjectNativeSearchError::Read(RankedSq8Failure {
                error: RangeFetchError::UnexpectedMetadata,
                stats: Sq8ReadStats::default(),
            })
        })?;
        let plan = self
            .plan_pages(query, regions, shortlist, top_k, beta)
            .map_err(ObjectNativeSearchError::Plan)?;
        let page_bytes = self
            .pages
            .page_rows()
            .checked_mul(self.pages.dimensions() + 12)
            .ok_or(ObjectNativeSearchError::Plan(
                ObjectNativePlanError::Arithmetic,
            ))?;
        let ranges = plan
            .ranges
            .iter()
            .map(|range| (range.start / page_bytes, (range.end - 1) / page_bytes))
            .collect::<Vec<_>>();
        let ranked = reader
            .rank_verified_sq8_pages(
                &self.object_key,
                &self.pages,
                &ranges,
                &self.etag,
                query,
                &self.router.low,
                &self.router.step,
                top_k,
                self.max_query_gets,
                self.max_query_bytes,
                self.max_parallel_gets,
            )
            .await
            .map_err(ObjectNativeSearchError::Read)?;
        Ok(ObjectNativeSearchResult { plan, ranked })
    }

    /// `ranges` are inclusive page pairs produced by an admitted plan.
    pub async fn rank_pages(
        &self,
        reader: &OneAttemptS3,
        ranges: &[(usize, usize)],
        query: &[f32],
        top_k: usize,
    ) -> Result<RankedSq8, RankedSq8Failure> {
        let _permit = self
            .query_slots
            .acquire()
            .await
            .map_err(|_| RankedSq8Failure {
                error: RangeFetchError::UnexpectedMetadata,
                stats: Sq8ReadStats::default(),
            })?;
        reader
            .rank_verified_sq8_pages(
                &self.object_key,
                &self.pages,
                ranges,
                &self.etag,
                query,
                &self.router.low,
                &self.router.step,
                top_k,
                self.max_query_gets,
                self.max_query_bytes,
                self.max_parallel_gets,
            )
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn opens_bound_metadata_without_vector_plane_and_rejects_changes() {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("borsuk-object-native-{suffix}"));
        let router_dir = root.join("router");
        fs::create_dir_all(&router_dir).unwrap();
        let sq8 = vec![0u8; 256 * (64 + 12)];
        let sq8_hash = sha256(&sq8);
        let centroids_blob = UnitCentroidPages::build_from_sq8_reader(
            &mut sq8.as_slice(),
            256,
            64,
            32,
            256,
            &[0.0; 64],
            &[1.0; 64],
        )
        .unwrap();
        let centroids = UnitCentroidPages::decode(&centroids_blob).unwrap();
        let graph_blob = UnitCentroidGraph::build(&centroids, &centroids_blob)
            .unwrap()
            .encode()
            .unwrap();
        let graph_resident =
            UnitCentroidGraph::preflight_resident_bytes(&graph_blob, &centroids).unwrap();
        fs::write(root.join("centroids.bin"), &centroids_blob).unwrap();
        fs::write(root.join("graph.bin"), &graph_blob).unwrap();
        let digest = Sha256::digest(&sq8).to_vec();
        fs::write(root.join("page_digests.bin"), &digest).unwrap();
        let page_manifest = serde_json::to_vec(&serde_json::json!({
            "schema":"borsuk-v115-sq8-page-authority-v2", "generation":1,
            "rows":256, "dimensions":64, "page_rows":256,
            "object_sha256":sq8_hash, "page_digest_sha256":sha256(&digest),
        }))
        .unwrap();
        fs::write(root.join("page_manifest.json"), &page_manifest).unwrap();
        let mut sections = serde_json::Map::new();
        for (name, mut bytes) in [
            ("summaries", vec![0; 64 * 4]),
            ("books", vec![0; 64 * 256 * 4]),
            ("codes", vec![0; 256 * 64]),
            ("low", vec![0; 64 * 4]),
            ("step", vec![0; 64 * 4]),
        ] {
            if name == "step" {
                for chunk in bytes.chunks_exact_mut(4) {
                    chunk.copy_from_slice(&1.0_f32.to_le_bytes());
                }
            }
            fs::write(router_dir.join(format!("{name}.bin")), &bytes).unwrap();
            sections.insert(
                name.to_owned(),
                serde_json::json!({
                    "bytes":bytes.len(), "sha256":sha256(&bytes),
                }),
            );
        }
        let router_manifest = serde_json::to_vec(&serde_json::json!({
            "schema":"borsuk-source-router-v2", "generation":1,
            "source_sha256":"a".repeat(64), "layout_sha256":"b".repeat(64),
            "sq8_sha256":sq8_hash,
            "geometry":{"rows":256,"dimensions":64,"page_rows":256,
                        "blocks_per_page":1,"subspaces":64,"pq_width":1,
                        "pq_partition":"balanced_floor_v1"},
            "sections":sections,
        }))
        .unwrap();
        fs::write(router_dir.join("manifest.json"), &router_manifest).unwrap();
        let manifest = serde_json::to_vec(&serde_json::json!({
            "schema":"borsuk-object-native-generation-v1", "generation":1,
            "rows":256,"dimensions":64,"page_rows":256,"unit_rows":32,
            "router_manifest_sha256":sha256(&router_manifest),
            "page_manifest_sha256":sha256(&page_manifest),
            "centroids_sha256":sha256(&centroids_blob),
            "graph_sha256":sha256(&graph_blob),
            "graph_resident_bytes":graph_resident,
            "sq8_object_sha256":sq8_hash,
            "sq8_object_key":format!("objects/{sq8_hash}"), "sq8_etag":"etag-1",
        }))
        .unwrap();
        fs::write(root.join("manifest.json"), &manifest).unwrap();
        let limits = ObjectNativeLimits {
            max_memory_bytes: 1_000_000,
            max_active_queries: 2,
            max_query_bytes: 32_768,
            max_query_gets: 2,
            max_parallel_gets: 2,
            max_router_regions: 1,
            max_router_shortlist: 128,
            already_pinned_bytes: 0,
        };
        let opened = ObjectNativeGeneration::open(&root, &sha256(&manifest), limits).unwrap();
        assert_eq!(opened.router().router.rows(), 256);
        assert_eq!(opened.pages().object_sha256(), sq8_hash);
        assert_eq!(opened.graph().node_count(), 8);
        let plan = opened.plan_pages(&[0.0; 64], 1, 128, 100, 4).unwrap();
        assert_eq!(plan.ranges, vec![0..sq8.len()]);
        assert_eq!(opened.object_key().as_ref(), format!("objects/{sq8_hash}"));
        assert!(matches!(
            ObjectNativeGeneration::open(&root, &"f".repeat(64), limits),
            Err(ObjectNativeOpenError::HashMismatch("generation manifest"))
        ));
        assert!(matches!(
            ObjectNativeGeneration::open(
                &root,
                &sha256(&manifest),
                ObjectNativeLimits {
                    max_memory_bytes: 1,
                    ..limits
                }
            ),
            Err(ObjectNativeOpenError::Invalid("memory cap"))
        ));
        fs::write(root.join("page_digests.bin"), vec![0; 32]).unwrap();
        assert!(matches!(
            ObjectNativeGeneration::open(&root, &sha256(&manifest), limits),
            Err(ObjectNativeOpenError::Page(PageError::HashMismatch))
                | Err(ObjectNativeOpenError::Page(PageError::InvalidSidecar))
        ));
        fs::remove_dir_all(root).unwrap();
    }
}
