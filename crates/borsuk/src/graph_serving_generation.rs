//! Authenticated, owned graph generation loaded under an explicit resource cap.
//! The trusted digest must come from a conditional generation pointer; this
//! module does not publish the pointer or perform live S3 range requests.

use std::fs::{self, File};
use std::io::{self, Read};
use std::path::Path;
use std::sync::Arc;

use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::graph_generation_resources::{
    GraphResourceError, GraphResourceEstimate, GraphResourceGeometry,
};
use crate::native_source_id_map::{NativeSourceIdMap, SourceIdMapError};
use crate::native_source_tier::{
    NativeSourceTier, ScoredSourceCandidate, SourceCandidate, SourceReadStats, SourceTierError,
};
use crate::pq64_router_artifact::{RouterArtifactError, SourceRouterArtifact, load_source_router};
use crate::sq8_page_authority::{PageAuthority, PageError};
use crate::unit_centroid_graph::{UnitCentroidGraph, UnitCentroidGraphError};
use crate::unit_centroid_pages::{UnitCentroidError, UnitCentroidPages};

const SCHEMA: &str = "borsuk-graph-generation-v1";
const MAX_MANIFEST_BYTES: u64 = 64 * 1024;

/// Failure to authenticate, admit, decode or bind one immutable generation.
#[derive(Debug)]
pub enum GraphGenerationLoadError {
    /// A local artifact could not be read.
    Io(io::Error),
    /// A format or identity declaration violates the v1 contract.
    Invalid(&'static str),
    /// A content digest differs from the trusted manifest.
    HashMismatch(&'static str),
    /// Checked arithmetic or the modeled memory ceiling rejected the load.
    Resource(GraphResourceError),
    /// The source-only PQ64 router failed authentication or decoding.
    Router(RouterArtifactError),
    /// The centroid plane failed decoding.
    Centroid(UnitCentroidError),
    /// The graph failed preflight or decoding.
    Graph(UnitCentroidGraphError),
    /// The exact source tier failed authentication.
    Source(SourceTierError),
    /// The source-ID map failed authentication.
    Map(SourceIdMapError),
    /// The SQ8 page authority failed authentication.
    Page(PageError),
}

impl std::fmt::Display for GraphGenerationLoadError {
    fn fmt(&self, output: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(output, "graph generation load: {self:?}")
    }
}
impl std::error::Error for GraphGenerationLoadError {}

impl From<io::Error> for GraphGenerationLoadError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}
impl From<GraphResourceError> for GraphGenerationLoadError {
    fn from(value: GraphResourceError) -> Self {
        Self::Resource(value)
    }
}
impl From<RouterArtifactError> for GraphGenerationLoadError {
    fn from(value: RouterArtifactError) -> Self {
        Self::Router(value)
    }
}
impl From<UnitCentroidError> for GraphGenerationLoadError {
    fn from(value: UnitCentroidError) -> Self {
        Self::Centroid(value)
    }
}
impl From<UnitCentroidGraphError> for GraphGenerationLoadError {
    fn from(value: UnitCentroidGraphError) -> Self {
        Self::Graph(value)
    }
}
impl From<SourceTierError> for GraphGenerationLoadError {
    fn from(value: SourceTierError) -> Self {
        Self::Source(value)
    }
}
impl From<SourceIdMapError> for GraphGenerationLoadError {
    fn from(value: SourceIdMapError) -> Self {
        Self::Map(value)
    }
}
impl From<PageError> for GraphGenerationLoadError {
    fn from(value: PageError) -> Self {
        Self::Page(value)
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Artifact {
    bytes: u64,
    sha256: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Artifacts {
    router_manifest: Artifact,
    centroids: Artifact,
    graph: Artifact,
    source: Artifact,
    id_map: Artifact,
    page_manifest: Artifact,
    page_digests: Artifact,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    schema: String,
    generation: u64,
    metric: String,
    normalization: String,
    rows: u64,
    dimensions: u64,
    page_rows: u64,
    unit_rows: u64,
    blocks_per_page: u64,
    source_verification_block_bytes: u64,
    max_nominees: u64,
    source_sha256: String,
    layout_sha256: String,
    sq8_object_key: String,
    sq8_etag: String,
    sq8_sha256: String,
    sq8_object_bytes: u64,
    graph_resident_bytes: u64,
    artifacts: Artifacts,
}

/// An operator-chosen ceiling and concurrency allowance, independent of row
/// count or corpus name. A higher-recall profile can request a larger cap.
#[derive(Clone, Copy, Debug)]
pub struct GraphGenerationLimits {
    /// Maximum modeled payload during loading and concurrent queries.
    pub max_modeled_bytes: u64,
    /// Maximum simultaneously active queries planned for this generation.
    pub max_active_queries: u64,
    /// Reserved transient payload per query, above the geometry-derived floor.
    pub transient_bytes_per_query: u64,
    /// Payload already pinned by retiring generations or shared caches.
    pub already_pinned_bytes: u64,
}

/// Immutable planes and object identity owned by a pinned `Arc` generation.
pub struct GraphServingGeneration {
    router: SourceRouterArtifact,
    centroids: UnitCentroidPages,
    graph: UnitCentroidGraph,
    source: NativeSourceTier,
    map: NativeSourceIdMap,
    pages: PageAuthority,
    object_key: String,
    etag: String,
    generation: u64,
    max_nominees: usize,
    resource_estimate: GraphResourceEstimate,
}

impl GraphServingGeneration {
    /// Authenticated source-only PQ64 router.
    pub fn router(&self) -> &SourceRouterArtifact {
        &self.router
    }
    /// Decoded f32 unit-centroid plane bound to this graph.
    pub fn centroids(&self) -> &UnitCentroidPages {
        &self.centroids
    }
    /// Authenticated centroid adjacency.
    pub fn graph(&self) -> &UnitCentroidGraph {
        &self.graph
    }
    /// Number of rows in the authenticated exact source tier.
    pub fn source_rows(&self) -> u64 {
        self.source.rows()
    }
    /// Resolve a fixed source-ID union through this generation's bound map.
    pub fn resolve_source_union(
        &self,
        router_ids: &[u64],
        expansion_ids: &[u64],
    ) -> Result<Vec<SourceCandidate>, SourceIdMapError> {
        self.map
            .resolve_union(&self.source, router_ids, expansion_ids)
    }
    /// Rank fixed candidates by exact squared L2, with authenticated read cost.
    pub fn rank_exact_source_l2_with_stats(
        &self,
        query: &[f32],
        candidates: &[SourceCandidate],
        top_k: usize,
    ) -> Result<(Vec<ScoredSourceCandidate>, SourceReadStats), SourceTierError> {
        self.source
            .rank_exact_l2_with_stats(query, candidates, top_k)
    }
    /// Generation-bound SQ8 page digests and remote object hash.
    pub fn pages(&self) -> &PageAuthority {
        &self.pages
    }
    /// Immutable content-addressed SQ8 object key.
    pub fn object_key(&self) -> &str {
        &self.object_key
    }
    /// Pinned SQ8 object ETag for conditional range requests.
    pub fn etag(&self) -> &str {
        &self.etag
    }
    /// Generation number from the trusted manifest.
    pub fn generation(&self) -> u64 {
        self.generation
    }
    /// Maximum SQ8 nominees declared by the generation.
    pub fn max_nominees(&self) -> usize {
        self.max_nominees
    }
    /// Checked modeled payload including loading overlap and concurrency.
    pub fn resource_estimate(&self) -> GraphResourceEstimate {
        self.resource_estimate
    }
}

fn is_hash(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn require_artifact(
    artifact: &Artifact,
    expected_bytes: Option<u64>,
) -> Result<(), GraphGenerationLoadError> {
    if artifact.bytes == 0
        || !is_hash(&artifact.sha256)
        || expected_bytes.is_some_and(|value| value != artifact.bytes)
    {
        return Err(GraphGenerationLoadError::Invalid("artifact declaration"));
    }
    Ok(())
}

fn metadata_length(path: &Path, artifact: &Artifact) -> Result<(), GraphGenerationLoadError> {
    if fs::metadata(path)?.len() != artifact.bytes {
        return Err(GraphGenerationLoadError::Invalid("artifact byte length"));
    }
    Ok(())
}

fn read_authenticated(
    path: &Path,
    artifact: &Artifact,
    role: &'static str,
) -> Result<Vec<u8>, GraphGenerationLoadError> {
    let mut file = File::open(path)?;
    if file.metadata()?.len() != artifact.bytes {
        return Err(GraphGenerationLoadError::Invalid("artifact byte length"));
    }
    let length = usize::try_from(artifact.bytes)
        .map_err(|_| GraphGenerationLoadError::Invalid("artifact address space"))?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(length)
        .map_err(|_| GraphGenerationLoadError::Invalid("artifact allocation"))?;
    bytes.resize(length, 0);
    file.read_exact(&mut bytes)?;
    let mut extra = [0_u8; 1];
    if file.read(&mut extra)? != 0 || sha256(&bytes) != artifact.sha256 {
        return Err(GraphGenerationLoadError::HashMismatch(role));
    }
    Ok(bytes)
}

fn read_small_manifest(path: &Path) -> Result<Vec<u8>, GraphGenerationLoadError> {
    let file = File::open(path)?;
    if file.metadata()?.len() > MAX_MANIFEST_BYTES {
        return Err(GraphGenerationLoadError::Invalid("manifest length"));
    }
    let mut raw = Vec::new();
    file.take(MAX_MANIFEST_BYTES + 1).read_to_end(&mut raw)?;
    if raw.len() as u64 > MAX_MANIFEST_BYTES {
        return Err(GraphGenerationLoadError::Invalid("manifest length"));
    }
    Ok(raw)
}

/// Inspect the small, authenticated nested manifest before its loader reads
/// any large PQ array. Its geometry and section lengths must refine the outer
/// generation's already-admitted resource estimate.
fn preflight_router_manifest(
    root: &Path,
    raw: &[u8],
    manifest: &Manifest,
) -> Result<(), GraphGenerationLoadError> {
    let declaration: serde_json::Value = serde_json::from_slice(raw)
        .map_err(|_| GraphGenerationLoadError::Invalid("router declaration"))?;
    let geometry = declaration
        .get("geometry")
        .ok_or(GraphGenerationLoadError::Invalid("router declaration"))?;
    let number = |value: &serde_json::Value, field: &str| {
        value.get(field).and_then(serde_json::Value::as_u64)
    };
    if declaration
        .get("schema")
        .and_then(serde_json::Value::as_str)
        != Some("borsuk-source-router-v2")
        || number(&declaration, "generation") != Some(manifest.generation)
        || declaration
            .get("source_sha256")
            .and_then(serde_json::Value::as_str)
            != Some(manifest.source_sha256.as_str())
        || declaration
            .get("layout_sha256")
            .and_then(serde_json::Value::as_str)
            != Some(manifest.layout_sha256.as_str())
        || declaration
            .get("sq8_sha256")
            .and_then(serde_json::Value::as_str)
            != Some(manifest.sq8_sha256.as_str())
        || number(geometry, "rows") != Some(manifest.rows)
        || number(geometry, "dimensions") != Some(manifest.dimensions)
        || number(geometry, "page_rows") != Some(manifest.page_rows)
        || number(geometry, "blocks_per_page") != Some(manifest.blocks_per_page)
        || number(geometry, "subspaces") != Some(64)
        || number(geometry, "pq_width") != Some(manifest.dimensions.div_ceil(64))
        || geometry
            .get("pq_partition")
            .and_then(serde_json::Value::as_str)
            != Some("balanced_floor_v1")
    {
        return Err(GraphGenerationLoadError::Invalid("router declaration"));
    }
    let blocks = manifest
        .rows
        .div_ceil(manifest.page_rows)
        .checked_mul(manifest.blocks_per_page)
        .ok_or(GraphGenerationLoadError::Invalid("router section overflow"))?;
    let summary_bytes = blocks
        .checked_mul(manifest.dimensions)
        .and_then(|n| n.checked_mul(4))
        .ok_or(GraphGenerationLoadError::Invalid("router section overflow"))?;
    let book_bytes = 64_u64
        .checked_mul(256)
        .and_then(|n| n.checked_mul(manifest.dimensions.div_ceil(64)))
        .and_then(|n| n.checked_mul(4))
        .ok_or(GraphGenerationLoadError::Invalid("router section overflow"))?;
    let code_bytes = manifest
        .rows
        .checked_mul(64)
        .ok_or(GraphGenerationLoadError::Invalid("router section overflow"))?;
    let affine_bytes = manifest
        .dimensions
        .checked_mul(4)
        .ok_or(GraphGenerationLoadError::Invalid("router section overflow"))?;
    let sections = declaration
        .get("sections")
        .ok_or(GraphGenerationLoadError::Invalid("router declaration"))?;
    for (name, expected) in [
        ("summaries", summary_bytes),
        ("books", book_bytes),
        ("codes", code_bytes),
        ("low", affine_bytes),
        ("step", affine_bytes),
    ] {
        let section = sections
            .get(name)
            .ok_or(GraphGenerationLoadError::Invalid("router declaration"))?;
        if number(section, "bytes") != Some(expected)
            || !section
                .get("sha256")
                .and_then(serde_json::Value::as_str)
                .is_some_and(is_hash)
            || fs::metadata(root.join(format!("{name}.bin")))?.len() != expected
        {
            return Err(GraphGenerationLoadError::Invalid("router declaration"));
        }
    }
    Ok(())
}

fn preflight_centroid_header(
    bytes: &[u8],
    manifest: &Manifest,
) -> Result<(), GraphGenerationLoadError> {
    if bytes.len() < 32 || &bytes[..8] != b"BORSUCP1" || bytes[28..32] != [0; 4] {
        return Err(GraphGenerationLoadError::Invalid("centroid header"));
    }
    let rows = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
    let field =
        |offset: usize| u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as u64;
    if rows != manifest.rows
        || field(16) != manifest.dimensions
        || field(20) != manifest.unit_rows
        || field(24) != manifest.page_rows
    {
        return Err(GraphGenerationLoadError::Invalid("centroid header"));
    }
    Ok(())
}

impl Manifest {
    fn validate(
        &self,
        limits: GraphGenerationLimits,
    ) -> Result<GraphResourceEstimate, GraphGenerationLoadError> {
        if self.schema != SCHEMA
            || self.generation == 0
            || self.metric != "l2"
            || self.normalization != "none"
            || self.page_rows != 256
            || self.unit_rows != 32
            || self.rows < 64
            || self.rows > usize::MAX as u64
            || self.dimensions > usize::MAX as u64
            || self.max_nominees == 0
            || self.max_nominees > self.rows
            || self.sq8_object_key != format!("objects/{}", self.sq8_sha256)
            || self.sq8_etag.is_empty()
            || self.sq8_etag.chars().any(char::is_control)
            || !is_hash(&self.source_sha256)
            || !is_hash(&self.layout_sha256)
            || !is_hash(&self.sq8_sha256)
        {
            return Err(GraphGenerationLoadError::Invalid(
                "generation identity or geometry",
            ));
        }
        let pages = self.rows.div_ceil(self.page_rows);
        let units = self.rows.div_ceil(self.unit_rows);
        let encoded_centroids = units
            .checked_mul(self.dimensions)
            .and_then(|n| n.checked_mul(2))
            .and_then(|n| n.checked_add(32))
            .ok_or(GraphGenerationLoadError::Invalid("centroid size overflow"))?;
        let source_bytes = self
            .rows
            .checked_mul(
                self.dimensions
                    .checked_mul(4)
                    .and_then(|n| n.checked_add(8))
                    .ok_or(GraphGenerationLoadError::Invalid("source row overflow"))?,
            )
            .and_then(|n| n.checked_add(64))
            .ok_or(GraphGenerationLoadError::Invalid("source size overflow"))?;
        let map_bytes = self
            .rows
            .checked_mul(16)
            .and_then(|n| n.checked_add(96))
            .ok_or(GraphGenerationLoadError::Invalid("map size overflow"))?;
        let page_digest_bytes = pages
            .checked_mul(32)
            .ok_or(GraphGenerationLoadError::Invalid("page digest overflow"))?;
        let sq8_bytes = self
            .rows
            .checked_mul(
                self.dimensions
                    .checked_add(12)
                    .ok_or(GraphGenerationLoadError::Invalid("SQ8 row overflow"))?,
            )
            .ok_or(GraphGenerationLoadError::Invalid("SQ8 size overflow"))?;
        if self.sq8_object_bytes != sq8_bytes || units < 2 {
            return Err(GraphGenerationLoadError::Invalid("SQ8 or graph geometry"));
        }
        for artifact in [
            &self.artifacts.router_manifest,
            &self.artifacts.centroids,
            &self.artifacts.graph,
            &self.artifacts.source,
            &self.artifacts.id_map,
            &self.artifacts.page_manifest,
            &self.artifacts.page_digests,
        ] {
            require_artifact(artifact, None)?;
        }
        require_artifact(&self.artifacts.centroids, Some(encoded_centroids))?;
        require_artifact(&self.artifacts.source, Some(source_bytes))?;
        require_artifact(&self.artifacts.id_map, Some(map_bytes))?;
        require_artifact(&self.artifacts.page_digests, Some(page_digest_bytes))?;
        if self.artifacts.router_manifest.bytes > MAX_MANIFEST_BYTES
            || self.artifacts.page_manifest.bytes > MAX_MANIFEST_BYTES
        {
            return Err(GraphGenerationLoadError::Invalid("nested manifest length"));
        }
        // Known per-query arrays include the authenticated source block,
        // source row, query coefficients, PQ table, page-score roster and
        // candidate/score copies. The caller may reserve more for planner,
        // graph and allocator costs, but cannot declare less than this floor.
        let minimum_transient = [
            self.source_verification_block_bytes,
            self.dimensions
                .checked_mul(4)
                .and_then(|n| n.checked_add(8))
                .ok_or(GraphGenerationLoadError::Invalid("query scratch overflow"))?,
            self.dimensions
                .checked_mul(8)
                .ok_or(GraphGenerationLoadError::Invalid("query scratch overflow"))?,
            64 * 256 * 4,
            pages
                .checked_mul(24)
                .ok_or(GraphGenerationLoadError::Invalid("query scratch overflow"))?,
            self.max_nominees
                .checked_mul(48)
                .ok_or(GraphGenerationLoadError::Invalid("query scratch overflow"))?,
        ]
        .into_iter()
        .try_fold(0_u64, |acc, value| acc.checked_add(value))
        .ok_or(GraphGenerationLoadError::Invalid("query scratch overflow"))?;
        if limits.transient_bytes_per_query < minimum_transient {
            return Err(GraphGenerationLoadError::Invalid("query scratch allowance"));
        }
        GraphResourceGeometry {
            rows: self.rows,
            dimensions: self.dimensions,
            page_rows: self.page_rows,
            unit_rows: self.unit_rows,
            blocks_per_page: self.blocks_per_page,
            source_verification_block_bytes: self.source_verification_block_bytes,
            graph_encoded_bytes: self.artifacts.graph.bytes,
            graph_resident_bytes: self.graph_resident_bytes,
            max_active_queries: limits.max_active_queries,
            transient_bytes_per_query: limits.transient_bytes_per_query,
            already_pinned_bytes: limits.already_pinned_bytes,
        }
        .admit(limits.max_modeled_bytes)
        .map_err(Into::into)
    }
}

/// Load after pinning `trusted_manifest_sha256` from the authorized
/// generation pointer. Admission runs before any large artifact is decoded.
pub fn load_graph_generation(
    root: &Path,
    trusted_manifest_sha256: &str,
    limits: GraphGenerationLimits,
) -> Result<Arc<GraphServingGeneration>, GraphGenerationLoadError> {
    if !is_hash(trusted_manifest_sha256) {
        return Err(GraphGenerationLoadError::Invalid("trusted digest"));
    }
    let raw = read_small_manifest(&root.join("manifest.json"))?;
    if sha256(&raw) != trusted_manifest_sha256 {
        return Err(GraphGenerationLoadError::HashMismatch(
            "generation manifest",
        ));
    }
    let manifest: Manifest = serde_json::from_slice(&raw)
        .map_err(|_| GraphGenerationLoadError::Invalid("generation manifest schema"))?;
    let resource_estimate = manifest.validate(limits)?;
    let artifacts = &manifest.artifacts;
    let router_root = root.join("router");
    let router_manifest = read_authenticated(
        &router_root.join("manifest.json"),
        &artifacts.router_manifest,
        "router manifest",
    )?;
    preflight_router_manifest(&router_root, &router_manifest, &manifest)?;
    let router = load_source_router(&router_root, &artifacts.router_manifest.sha256)?;
    let centroid_blob = read_authenticated(
        &root.join("centroids.bin"),
        &artifacts.centroids,
        "centroids",
    )?;
    preflight_centroid_header(&centroid_blob, &manifest)?;
    let centroids = UnitCentroidPages::decode(&centroid_blob)?;
    let graph_blob = read_authenticated(&root.join("graph.bin"), &artifacts.graph, "graph")?;
    let actual_graph_bytes = UnitCentroidGraph::preflight_resident_bytes(&graph_blob, &centroids)?;
    if u64::try_from(actual_graph_bytes).ok() != Some(manifest.graph_resident_bytes) {
        return Err(GraphGenerationLoadError::Invalid(
            "graph resident declaration",
        ));
    }
    let graph = UnitCentroidGraph::decode_bounded(
        &graph_blob,
        &centroid_blob,
        &centroids,
        actual_graph_bytes,
    )?;
    let page_manifest = read_authenticated(
        &root.join("page_manifest.json"),
        &artifacts.page_manifest,
        "page manifest",
    )?;
    let page_digests = read_authenticated(
        &root.join("page_digests.bin"),
        &artifacts.page_digests,
        "page digests",
    )?;
    let pages = PageAuthority::load(
        &page_manifest,
        &artifacts.page_manifest.sha256,
        &page_digests,
    )?;
    let rows = usize::try_from(manifest.rows)
        .map_err(|_| GraphGenerationLoadError::Invalid("rows address space"))?;
    let dimensions = usize::try_from(manifest.dimensions)
        .map_err(|_| GraphGenerationLoadError::Invalid("dimensions address space"))?;
    if router.generation != manifest.generation
        || router.source_sha256 != manifest.source_sha256
        || router.layout_sha256 != manifest.layout_sha256
        || router.sq8_sha256 != manifest.sq8_sha256
        || router.router.rows() != rows
        || router.router.dimensions() != dimensions
        || router.router.page_rows() != manifest.page_rows as usize
        || router.router.blocks_per_page() != manifest.blocks_per_page as usize
        || centroids.rows() != rows
        || centroids.dimensions() != dimensions
        || centroids.unit_rows() != manifest.unit_rows as usize
        || centroids.page_rows() != manifest.page_rows as usize
        || pages.rows() != rows
        || pages.dimensions() != dimensions
        || pages.page_rows() != manifest.page_rows as usize
        || pages.generation() != manifest.generation
        || pages.object_sha256() != manifest.sq8_sha256
        || pages.object_bytes() as u64 != manifest.sq8_object_bytes
    {
        return Err(GraphGenerationLoadError::Invalid(
            "artifact generation binding",
        ));
    }
    metadata_length(&root.join("source.bin"), &artifacts.source)?;
    let source = NativeSourceTier::open_authenticated(
        &root.join("source.bin"),
        &artifacts.source.sha256,
        &manifest.source_sha256,
        manifest.rows,
        dimensions,
        manifest.generation,
        manifest.source_verification_block_bytes as usize,
    )?;
    metadata_length(&root.join("id_map.bin"), &artifacts.id_map)?;
    let map = NativeSourceIdMap::open_authenticated(
        &root.join("id_map.bin"),
        &artifacts.id_map.sha256,
        &manifest.source_sha256,
        &artifacts.source.sha256,
        manifest.rows,
        manifest.generation,
    )?;
    if !map.binds_to(&source) {
        return Err(GraphGenerationLoadError::Invalid("source ID map binding"));
    }
    Ok(Arc::new(GraphServingGeneration {
        router,
        centroids,
        graph,
        source,
        map,
        pages,
        object_key: manifest.sq8_object_key,
        etag: manifest.sq8_etag,
        generation: manifest.generation,
        max_nominees: manifest.max_nominees as usize,
        resource_estimate,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native_source_id_map::write_source_id_map;
    use crate::native_source_tier::write_source_tier;
    use serde_json::{Value, json};
    use tempfile::TempDir;

    fn descriptor(path: &Path) -> Value {
        let bytes = fs::read(path).unwrap();
        json!({"bytes": bytes.len(), "sha256": sha256(&bytes)})
    }

    fn floats(values: &[f32]) -> Vec<u8> {
        values
            .iter()
            .flat_map(|value| value.to_le_bytes())
            .collect()
    }

    fn fixture() -> (TempDir, String) {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let router_root = root.join("router");
        fs::create_dir(&router_root).unwrap();
        let rows = 512usize;
        let dimensions = 64usize;
        let generation = 7u64;
        let source_sha = "a".repeat(64);
        let layout_sha = "b".repeat(64);
        let mut sq8 = Vec::new();
        for row in 0..rows {
            sq8.extend_from_slice(&(row as i64).to_le_bytes());
            sq8.extend_from_slice(&1.0f32.to_le_bytes());
            sq8.extend_from_slice(&vec![(row / 32 + 1) as u8; dimensions]);
        }
        let sq8_sha = sha256(&sq8);
        let low = vec![0.0f32; dimensions];
        let step = vec![1.0f32; dimensions];
        let centroids_blob = UnitCentroidPages::build_from_sq8_reader(
            &mut sq8.as_slice(),
            rows,
            dimensions,
            32,
            256,
            &low,
            &step,
        )
        .unwrap();
        fs::write(root.join("centroids.bin"), &centroids_blob).unwrap();
        let centroids = UnitCentroidPages::decode(&centroids_blob).unwrap();
        let graph_blob = UnitCentroidGraph::build(&centroids, &centroids_blob)
            .unwrap()
            .encode()
            .unwrap();
        fs::write(root.join("graph.bin"), &graph_blob).unwrap();
        let graph_resident =
            UnitCentroidGraph::preflight_resident_bytes(&graph_blob, &centroids).unwrap();
        let mut sections = serde_json::Map::new();
        for (name, bytes) in [
            ("summaries", floats(&vec![0.0f32; 2 * 2 * dimensions])),
            ("books", floats(&vec![0.0f32; 64 * 256])),
            ("codes", vec![0u8; rows * 64]),
            ("low", floats(&low)),
            ("step", floats(&step)),
        ] {
            fs::write(router_root.join(format!("{name}.bin")), &bytes).unwrap();
            sections.insert(
                name.to_owned(),
                descriptor(&router_root.join(format!("{name}.bin"))),
            );
        }
        let router_manifest = json!({
            "schema":"borsuk-source-router-v2", "generation":generation,
            "source_sha256":source_sha, "layout_sha256":layout_sha,
            "sq8_sha256":sq8_sha,
            "geometry":{"rows":rows,"dimensions":dimensions,"page_rows":256,
                "blocks_per_page":2,"subspaces":64,"pq_width":1,
                "pq_partition":"balanced_floor_v1"},
            "sections":sections,
        });
        fs::write(
            router_root.join("manifest.json"),
            serde_json::to_vec(&router_manifest).unwrap(),
        )
        .unwrap();
        let page_digests = sq8
            .chunks(256 * (dimensions + 12))
            .flat_map(|page| Sha256::digest(page).to_vec())
            .collect::<Vec<_>>();
        fs::write(root.join("page_digests.bin"), &page_digests).unwrap();
        let page_manifest = json!({
            "schema":"borsuk-v115-sq8-page-authority-v2", "generation":generation,
            "rows":rows,"dimensions":dimensions,"page_rows":256,
            "object_sha256":sq8_sha,"page_digest_sha256":sha256(&page_digests),
        });
        fs::write(
            root.join("page_manifest.json"),
            serde_json::to_vec(&page_manifest).unwrap(),
        )
        .unwrap();
        let source_artifact = write_source_tier(
            &root.join("source.bin"),
            rows as u64,
            dimensions,
            generation,
            &source_sha,
            (0..rows).map(|row| (row as u64, vec![1.0 + row as f32 / 1000.0; dimensions])),
        )
        .unwrap();
        write_source_id_map(
            &root.join("id_map.bin"),
            rows as u64,
            generation,
            &source_sha,
            &source_artifact,
            (0..rows as u64).into_iter(),
        )
        .unwrap();
        let manifest = json!({
            "schema":SCHEMA,"generation":generation,"metric":"l2",
            "normalization":"none","rows":rows,"dimensions":dimensions,
            "page_rows":256,"unit_rows":32,"blocks_per_page":2,
            "source_verification_block_bytes":4096,"max_nominees":512,
            "source_sha256":source_sha,"layout_sha256":layout_sha,
            "sq8_object_key":format!("objects/{sq8_sha}"),"sq8_etag":"etag-7",
            "sq8_sha256":sq8_sha,"sq8_object_bytes":sq8.len(),
            "graph_resident_bytes":graph_resident,
            "artifacts":{
                "router_manifest":descriptor(&router_root.join("manifest.json")),
                "centroids":descriptor(&root.join("centroids.bin")),
                "graph":descriptor(&root.join("graph.bin")),
                "source":descriptor(&root.join("source.bin")),
                "id_map":descriptor(&root.join("id_map.bin")),
                "page_manifest":descriptor(&root.join("page_manifest.json")),
                "page_digests":descriptor(&root.join("page_digests.bin")),
            },
        });
        let raw = serde_json::to_vec(&manifest).unwrap();
        fs::write(root.join("manifest.json"), &raw).unwrap();
        (directory, sha256(&raw))
    }

    fn limits() -> GraphGenerationLimits {
        GraphGenerationLimits {
            max_modeled_bytes: u64::MAX,
            max_active_queries: 2,
            transient_bytes_per_query: 1024 * 1024,
            already_pinned_bytes: 0,
        }
    }

    fn rewrite_outer(root: &Path, edit: impl FnOnce(&mut Value)) -> String {
        let path = root.join("manifest.json");
        let mut value: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        edit(&mut value);
        let raw = serde_json::to_vec(&value).unwrap();
        fs::write(path, &raw).unwrap();
        sha256(&raw)
    }

    #[test]
    fn loads_owned_authenticated_generation_and_fences_memory_before_decoding() {
        let (directory, digest) = fixture();
        let root = directory.path();
        let pinned = load_graph_generation(root, &digest, limits()).unwrap();
        assert_eq!(pinned.generation(), 7);
        assert_eq!(pinned.graph().node_count(), 16);
        assert_eq!(pinned.source_rows(), 512);
        assert_eq!(pinned.max_nominees(), 512);
        assert_eq!(pinned.pages().object_sha256(), pinned.router().sq8_sha256);
        let candidates = pinned.resolve_source_union(&[0, 1], &[2]).unwrap();
        let (ranked, stats) = pinned
            .rank_exact_source_l2_with_stats(&vec![1.0; 64], &candidates, 1)
            .unwrap();
        assert_eq!(ranked[0].source_id, 0);
        assert_eq!(stats.source_rows, 3);
        let mut under = limits();
        under.max_modeled_bytes = pinned.resource_estimate().total_peak_bytes - 1;
        fs::remove_file(root.join("centroids.bin")).unwrap();
        assert!(matches!(
            load_graph_generation(root, &digest, under),
            Err(GraphGenerationLoadError::Resource(
                GraphResourceError::InsufficientBudget
            ))
        ));
    }

    #[test]
    fn rejects_corrupt_graph_and_wrong_generation() {
        let (directory, digest) = fixture();
        let root = directory.path();
        let graph_path = root.join("graph.bin");
        let mut graph = fs::read(&graph_path).unwrap();
        let last = graph.len() - 1;
        graph[last] ^= 1;
        fs::write(&graph_path, &graph).unwrap();
        assert!(matches!(
            load_graph_generation(root, &digest, limits()),
            Err(GraphGenerationLoadError::HashMismatch("graph"))
        ));
        let (directory, _) = fixture();
        let root = directory.path();
        let mut manifest: Value =
            serde_json::from_slice(&fs::read(root.join("manifest.json")).unwrap()).unwrap();
        manifest["generation"] = json!(8);
        let raw = serde_json::to_vec(&manifest).unwrap();
        fs::write(root.join("manifest.json"), &raw).unwrap();
        assert!(matches!(
            load_graph_generation(root, &sha256(&raw), limits()),
            Err(GraphGenerationLoadError::Invalid("router declaration"))
        ));
    }

    #[test]
    fn rejects_nested_router_geometry_before_large_array_load() {
        let (directory, _) = fixture();
        let root = directory.path();
        let router_path = root.join("router/manifest.json");
        let mut router: Value = serde_json::from_slice(&fs::read(&router_path).unwrap()).unwrap();
        router["geometry"]["rows"] = json!(100_000_000);
        fs::write(&router_path, serde_json::to_vec(&router).unwrap()).unwrap();
        let outer_path = root.join("manifest.json");
        let mut outer: Value = serde_json::from_slice(&fs::read(&outer_path).unwrap()).unwrap();
        outer["artifacts"]["router_manifest"] = descriptor(&router_path);
        let raw = serde_json::to_vec(&outer).unwrap();
        fs::write(&outer_path, &raw).unwrap();
        fs::remove_file(root.join("router/codes.bin")).unwrap();
        assert!(matches!(
            load_graph_generation(root, &sha256(&raw), limits()),
            Err(GraphGenerationLoadError::Invalid("router declaration"))
        ));
    }

    #[test]
    fn rejects_centroid_header_and_graph_resident_claims() {
        let (directory, _) = fixture();
        let root = directory.path();
        let centroid_path = root.join("centroids.bin");
        let mut bytes = fs::read(&centroid_path).unwrap();
        bytes[8..16].copy_from_slice(&511_u64.to_le_bytes());
        fs::write(&centroid_path, &bytes).unwrap();
        let digest = rewrite_outer(root, |outer| {
            outer["artifacts"]["centroids"] = descriptor(&centroid_path);
        });
        assert!(matches!(
            load_graph_generation(root, &digest, limits()),
            Err(GraphGenerationLoadError::Invalid("centroid header"))
        ));

        let (directory, _) = fixture();
        let root = directory.path();
        let digest = rewrite_outer(root, |outer| {
            let count = outer["graph_resident_bytes"].as_u64().unwrap();
            outer["graph_resident_bytes"] = json!(count + 1);
        });
        assert!(matches!(
            load_graph_generation(root, &digest, limits()),
            Err(GraphGenerationLoadError::Invalid(
                "graph resident declaration"
            ))
        ));
    }

    #[test]
    fn rejects_identity_and_nominee_policy_before_array_load() {
        for change in ["layout", "sq8", "nominees_zero", "nominees_large", "etag"] {
            let (directory, _) = fixture();
            let root = directory.path();
            let digest = rewrite_outer(root, |outer| match change {
                "layout" => outer["layout_sha256"] = json!("c".repeat(64)),
                "sq8" => {
                    outer["sq8_sha256"] = json!("c".repeat(64));
                    outer["sq8_object_key"] = json!(format!("objects/{}", "c".repeat(64)));
                }
                "nominees_zero" => outer["max_nominees"] = json!(0),
                "nominees_large" => outer["max_nominees"] = json!(513),
                "etag" => outer["sq8_etag"] = json!("bad\netag"),
                _ => unreachable!(),
            });
            fs::remove_file(root.join("centroids.bin")).unwrap();
            let result = load_graph_generation(root, &digest, limits());
            assert!(
                matches!(
                    result,
                    Err(GraphGenerationLoadError::Invalid("router declaration"))
                        | Err(GraphGenerationLoadError::Invalid(
                            "generation identity or geometry"
                        ))
                ),
                "{change}"
            );
        }
    }

    #[test]
    fn rejects_corrupt_source_map_before_exposing_generation() {
        let (directory, digest) = fixture();
        let root = directory.path();
        let path = root.join("id_map.bin");
        let mut bytes = fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 1;
        fs::write(path, bytes).unwrap();
        assert!(matches!(
            load_graph_generation(root, &digest, limits()),
            Err(GraphGenerationLoadError::Map(_))
        ));
    }
}
