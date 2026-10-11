//! Deterministic semantic grouping and whole-leaf nomination over original FP16 unit means.
//!
//! Construction and complete publication validation read all centroids and leaves.
//! Serving opens only an authenticated root and membership, then nominates complete
//! leaves from an inverse directory. The caller owns generation binding and publication;
//! no generation input schema is hard-coded here. Current geometry is deliberately
//! admitted through explicit native and fresh-scale profiles.
use crate::{VectorMetric, train_logical_cell_centroids, unit_centroid_pages::UnitCentroidPages};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::BTreeSet, error::Error};

/// Errors include malformed identities/geometry, admission and native trainer failures.
pub type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

const ROOT_CAP: usize = 64 * 1024;
const BLOB_CAP: usize = 8 * 1024 * 1024;
const BINARY_HEADER: usize = 512;
const DIRECTORY_BYTES: usize = 64;
/// Maximum admitted construction payload, not an RSS limit.
pub const ALLOCATION_CAP: usize = 128 * 1024 * 1024;
const HEADER_BYTES: usize = 32;
const LEAF_UNITS: usize = 64;
const ITERATIONS: usize = 12;
const SCHEMA: &str = "BORSUSR3";

/// Authenticated leaf partition contract; experimental spatial mode is opt-in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LeafPartition {
    /// Frozen radius-order chunks of at most 64 units.
    Radial64V1,
    /// Deterministic widest-axis bisection with balanced leaf quotas.
    WidestAxisQuotaV1,
}
impl LeafPartition {
    fn code(self) -> usize {
        match self {
            Self::Radial64V1 => 1,
            Self::WidestAxisQuotaV1 => 2,
        }
    }
    fn from_code(code: usize) -> Result<Self> {
        match code {
            1 => Ok(Self::Radial64V1),
            2 => Ok(Self::WidestAxisQuotaV1),
            _ => Err("unknown leaf partition".into()),
        }
    }
    fn group_sort(self) -> &'static str {
        match self {
            Self::Radial64V1 => "squared distance, original unit ID",
            Self::WidestAxisQuotaV1 => {
                "widest f64 axis, numeric coordinate/unit ID, balanced quotas; terminal squared distance/unit ID"
            }
        }
    }
}

/// Explicit geometry and payload admission; persisted profiles have no default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SemanticProfile {
    /// Existing 1–100,000 rows, 1–1024 coordinates.
    Native100k,
    /// Fresh research arm: exactly one million rows and 768 coordinates.
    Fresh1m,
    /// Generic explicit scale geometry: 1–1,000,000 rows, 1–1024 coordinates.
    /// Uses existing scale algorithm/caps; geometry is not runtime qualification.
    Scale1m,
}
impl SemanticProfile {
    /// Construction payload ceiling, independently of measured process RSS.
    pub const fn allocation_cap(self) -> usize {
        match self {
            Self::Native100k => ALLOCATION_CAP,
            Self::Fresh1m | Self::Scale1m => 512 * 1024 * 1024,
        }
    }
    /// Maximum encoded root length.
    pub const fn root_cap(self) -> usize {
        match self {
            Self::Native100k => 1024 * 1024,
            Self::Fresh1m | Self::Scale1m => 4 * 1024 * 1024,
        }
    }
    /// Whether the supplied source geometry belongs to this profile.
    pub fn valid_geometry(self, rows: usize, dimensions: usize) -> bool {
        match self {
            Self::Native100k => (1..=100_000).contains(&rows) && (1..=1024).contains(&dimensions),
            Self::Fresh1m => rows == 1_000_000 && dimensions == 768,
            Self::Scale1m => (1..=1_000_000).contains(&rows) && (1..=1024).contains(&dimensions),
        }
    }
    pub(crate) const fn selected_leaf_limit(self) -> usize {
        match self {
            Self::Native100k => 16,
            Self::Fresh1m | Self::Scale1m => 48,
        }
    }
    pub(crate) fn selected_leaf_bytes(self, dimensions: usize) -> Result<usize> {
        match self {
            Self::Native100k => Ok(16 * LEAF_UNITS * (4 + 1024 * 2)),
            Self::Fresh1m => Ok(48 * LEAF_UNITS * (4 + 768 * 2)),
            Self::Scale1m => {
                require((1..=1024).contains(&dimensions), "selected leaf dimensions")?;
                product(&[48, LEAF_UNITS, sum(&[4, product(&[dimensions, 2])?])?])
            }
        }
    }
    pub(crate) const fn walk_unit_limit(self) -> usize {
        self.selected_leaf_limit() * LEAF_UNITS + 7
    }
    pub(crate) const fn closure_page_limit(self) -> usize {
        match self {
            Self::Native100k => 1024,
            Self::Fresh1m => 512,
            // Every nominated unit can occupy a distinct physical page. Payload
            // admission separately bounds the cover's actual bytes and GETs.
            Self::Scale1m => self.selected_leaf_limit() * LEAF_UNITS,
        }
    }
    pub(crate) const fn source_unit_limit(self) -> usize {
        match self {
            Self::Native100k => 2544,
            Self::Fresh1m => 4096,
            Self::Scale1m => self.closure_page_limit() * 8,
        }
    }
    fn code(self) -> u32 {
        match self {
            Self::Native100k => 1,
            Self::Fresh1m => 2,
            Self::Scale1m => 3,
        }
    }
    fn from_code(code: u32) -> Result<Self> {
        match code {
            1 => Ok(Self::Native100k),
            2 => Ok(Self::Fresh1m),
            3 => Ok(Self::Scale1m),
            _ => Err("unknown semantic profile".into()),
        }
    }
}
#[allow(missing_docs)]
#[derive(Clone, Copy)]
pub struct Geometry {
    pub rows: usize,
    pub dimensions: usize,
    pub units: usize,
    pub blob_bytes: usize,
}

#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Artifact {
    pub bytes: usize,
    pub sha256: String,
}

#[allow(missing_docs)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Algorithm {
    pub trainer: String,
    pub metric: String,
    pub iterations: usize,
    pub requested_centers: usize,
    pub training_centers: usize,
    pub max_leaf_units: usize,
    pub nearest_ties: String,
    pub group_sort: String,
    pub root_prototype: String,
    pub normalization: String,
    pub payload: String,
}

#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Leaf {
    pub leaf_id: usize,
    pub group_ordinal: usize,
    pub chunk_ordinal: usize,
    pub offset: usize,
    pub bytes: usize,
    pub unit_count: usize,
    pub source_rows: usize,
    pub sha256: String,
    pub prototype: Vec<f32>,
}

#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub profile: SemanticProfile,
    pub partition: LeafPartition,
    pub schema: String,
    pub input_schema: String,
    pub input_root_sha256: String,
    pub input_centroids_sha256: String,
    pub input_centroids_bytes: usize,
    pub rows: usize,
    pub dimensions: usize,
    pub unit_rows: usize,
    pub page_rows: usize,
    pub unit_count: usize,
    pub final_unit_rows: usize,
    pub algorithm: Algorithm,
    pub modeled_peak_allocation_bytes: usize,
    pub modeled_allocation_limit_bytes: usize,
    pub allocation_model: String,
    pub membership: Artifact,
    pub leaf_payload: Artifact,
    pub leaves: Vec<Leaf>,
}

/// Authenticated source binding supplied by the enclosing generation or research CLI.
/// `root_sha256` is an input identity, never the output router's own digest.
pub struct SourceIdentity<'a> {
    /// Explicit admission profile authenticated by the enclosing generation.
    pub profile: SemanticProfile,
    /// Nonempty source format (at most 256 bytes); caller validates its meaning.
    pub schema: &'a str,
    /// Authenticated input root or source descriptor digest.
    pub root_sha256: &'a str,
    /// Digest of the original native FP16 unit-centroid bytes.
    pub centroids_sha256: &'a str,
    /// Physical source row count.
    pub rows: usize,
    /// Coordinates per unit mean.
    pub dimensions: usize,
}

/// Complete binary publication with unchanged membership and FP16 leaf encoding.
pub struct RouterArtifacts {
    /// BORSUSR3 root with exact leaf ranges, identities and f32 bits.
    pub manifest: Vec<u8>,
    /// One little-endian u32 leaf ID per original unit.
    pub membership: Vec<u8>,
    /// Concatenated unit ID and unchanged FP16 records.
    pub leaves: Vec<u8>,
}

fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn require(ok: bool, message: &str) -> Result<()> {
    if !ok {
        return Err(message.into());
    }
    Ok(())
}
fn valid_sha(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}
fn product(values: &[usize]) -> Result<usize> {
    values.iter().try_fold(1_usize, |total, value| {
        total
            .checked_mul(*value)
            .ok_or_else(|| "allocation arithmetic overflow".into())
    })
}

fn sum(values: &[usize]) -> Result<usize> {
    values.iter().try_fold(0_usize, |total, value| {
        total
            .checked_add(*value)
            .ok_or_else(|| "allocation arithmetic overflow".into())
    })
}

/// Fixed midpoint rule for the sampled SQ8 boundary experiment, in physical order.
pub fn sampled_boundary_position(rows: usize, unit: usize) -> Result<usize> {
    let units = sum(&[rows, 31])? / 32;
    require(rows > 0 && unit < units, "boundary sample unit")?;
    Ok(sum(&[product(&[unit, 32])?, 16])?.min(rows - 1))
}

/// Reconstruct one sampled SQ8 row and apply the existing cosine normalization.
/// The caller authenticates calibration and record identity; no logical ID is used here.
pub fn decode_sampled_boundary_row(codes: &[u8], low: &[f32], step: &[f32]) -> Result<Vec<f32>> {
    require(
        (1..=1024).contains(&codes.len()) && low.len() == codes.len() && step.len() == codes.len(),
        "boundary sample dimensions",
    )?;
    let mut row = Vec::with_capacity(codes.len());
    for ((&code, &low), &step) in codes.iter().zip(low).zip(step) {
        let value = low + f32::from(code) * step;
        require(
            low.is_finite() && step.is_finite() && step > 0. && value.is_finite(),
            "boundary reconstruction/calibration",
        )?;
        row.push(value);
    }
    Ok(crate::sq8_source::cosine_vector(&row)?.into_owned())
}

/// Checked live-payload phase bounds, not measured RSS or allocator overhead.
#[allow(missing_docs)]
#[derive(Debug, Serialize)]
pub struct SampledBoundaryAllocation {
    pub samples: usize,
    pub graph: usize,
    pub insertion_scratch: usize,
    pub edge_pairs: usize,
    pub validation: usize,
    pub adjacency: usize,
    pub serialization: usize,
    pub query_scratch: usize,
    pub metadata: usize,
    pub construction_phase: usize,
    pub replay_phase: usize,
    pub peak: usize,
}

/// Admit before metadata/sample allocation. Samples and graph must be dropped before replay.
/// `metadata` covers caller-owned authenticated metadata and decoding copies;
/// `replay` covers the existing paired replay excluding this adjacency's allocations.
pub fn sampled_boundary_allocation(
    rows: usize,
    dimensions: usize,
    leaves: usize,
    metadata: usize,
    replay: usize,
    cap: usize,
) -> Result<SampledBoundaryAllocation> {
    use std::mem::size_of;
    let units = sum(&[rows, 31])? / 32;
    require(
        rows > 0
            && units <= u32::MAX as usize
            && (1..=1024).contains(&dimensions)
            && leaves > 0
            && leaves <= units,
        "boundary allocation geometry",
    )?;
    let samples = product(&[
        units,
        sum(&[size_of::<Vec<f32>>(), product(&[dimensions, 4])?])?,
    ])?;
    // The private builder caps levels at 16. Vec growth retains up to 64 base
    // and 32 upper IDs after reciprocal pruning, including the temporary +1 arc.
    let graph = product(&[
        units,
        sum(&[
            size_of::<Vec<Vec<u32>>>(),
            product(&[17, size_of::<Vec<u32>>()])?,
            product(&[64 + 16 * 32, 4])?,
        ])?,
    ])?;
    // levels/order/epoch marks; frontier can visit every node (ef does NOT bound it).
    // Bound doubling heaps, found/selected vectors, cached-key pruning and sort scratch.
    let insertion_scratch = sum(&[
        product(&[units, size_of::<usize>() + 4 + 4 + 2 * 8])?,
        128 * 8 * 4 + 64 * 32,
    ])?;
    let edge_pairs = product(&[units, 32, 8])?;
    let validation = sum(&[rows.div_ceil(8), product(&[dimensions, 20])?, 8192 + 12])?;
    let adjacency = sum(&[
        product(&[sum(&[leaves, 1])?, size_of::<usize>()])?,
        product(&[leaves, 16, 8])?,
    ])?;
    // Per-link JSON objects include BTreeMap's spare node slots, keys and Value
    // headers (2 KiB bound); encoded/readback buffers are separately capped at 4 MiB.
    let serialization = sum(&[product(&[leaves, 16, 2048])?, 4 * 4 * 1024 * 1024])?;
    let query_scratch = sum(&[product(&[leaves, 64])?, 640 * 32])?;
    let construction_phase = sum(&[
        metadata,
        samples,
        graph,
        insertion_scratch,
        edge_pairs,
        validation,
        adjacency,
        serialization,
    ])?;
    let replay_phase = sum(&[replay, adjacency, query_scratch])?;
    let peak = construction_phase.max(replay_phase);
    require(peak <= cap, "boundary phase allocation cap")?;
    Ok(SampledBoundaryAllocation {
        samples,
        graph,
        insertion_scratch,
        edge_pairs,
        validation,
        adjacency,
        serialization,
        query_scratch,
        metadata,
        construction_phase,
        replay_phase,
        peak,
    })
}

/// One retained directed sampled-graph cut count, not an exact kNN count.
#[allow(missing_docs)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct SampledBoundaryLink {
    pub leaf: u32,
    pub count: u32,
}

/// Generic source-only adjacency over admitted physical-unit membership.
/// Fixed midpoint and raw counts carry midpoint, leaf-size and popularity bias.
#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct SampledBoundaryAdjacency {
    offsets: Vec<usize>,
    links: Vec<SampledBoundaryLink>,
    directed_cross_leaf_arcs: u64,
    zero_link_leaves: usize,
}

impl SampledBoundaryAdjacency {
    /// Sequential nondiverse m16/m0=32/ef64 construction, with no repaired edges.
    /// Sample vectors are borrowed and never retained by the returned adjacency.
    pub fn build(
        samples: &[Vec<f32>],
        membership: &[usize],
        leaves: usize,
        cap: usize,
    ) -> Result<Self> {
        let dimensions = samples.first().ok_or("boundary empty samples")?.len();
        sampled_boundary_allocation(
            product(&[samples.len(), 32])?,
            dimensions,
            leaves,
            0,
            0,
            cap,
        )?;
        require(
            samples.len() == membership.len() && membership.iter().all(|&leaf| leaf < leaves),
            "boundary membership",
        )?;
        for sample in samples {
            let squared = sample.iter().map(|&v| f64::from(v).powi(2)).sum::<f64>();
            require(
                sample.len() == dimensions
                    && sample.iter().all(|v| v.is_finite())
                    && (squared - 1.).abs() <= 1e-5,
                "boundary normalized samples",
            )?;
        }
        let graph = crate::centroid_hnsw::build_hnsw_adjacency(samples, 16, 32, 64, 64);
        match graph {
            Some(graph) => Self::project(&graph.neighbours, membership, leaves),
            None => Ok(Self {
                offsets: vec![0; leaves + 1],
                links: Vec::new(),
                directed_cross_leaf_arcs: 0,
                zero_link_leaves: leaves,
            }),
        }
    }

    fn project(towers: &[Vec<Vec<u32>>], membership: &[usize], leaves: usize) -> Result<Self> {
        require(
            towers.len() == membership.len()
                && leaves > 0
                && membership.iter().all(|&leaf| leaf < leaves),
            "boundary graph membership",
        )?;
        let mut pairs = Vec::with_capacity(product(&[towers.len(), 32])?);
        for (unit, tower) in towers.iter().enumerate() {
            let base = tower.last().ok_or("boundary missing base layer")?;
            require(base.len() <= 32, "boundary base width")?;
            for &target in base {
                let target = target as usize;
                require(target < membership.len(), "boundary arc target")?;
                if membership[unit] != membership[target] {
                    pairs.push((
                        u32::try_from(membership[unit])?,
                        u32::try_from(membership[target])?,
                    ));
                }
            }
        }
        let directed_cross_leaf_arcs = pairs.len() as u64;
        pairs.sort_unstable(); // In-place; never allocate a leaf-squared count matrix.
        let mut offsets = Vec::with_capacity(sum(&[leaves, 1])?);
        let mut links = Vec::with_capacity(product(&[leaves, 16])?);
        let mut at = 0;
        for source in 0..leaves {
            offsets.push(links.len());
            let mut top = Vec::with_capacity(17);
            while at < pairs.len() && pairs[at].0 as usize == source {
                let target = pairs[at].1;
                let mut count = 0_u32;
                while at < pairs.len() && pairs[at] == (source as u32, target) {
                    count = count.checked_add(1).ok_or("boundary cut count overflow")?;
                    at += 1;
                }
                top.push(SampledBoundaryLink {
                    leaf: target,
                    count,
                });
                top.sort_unstable_by(|a, b| b.count.cmp(&a.count).then(a.leaf.cmp(&b.leaf)));
                top.truncate(16);
            }
            links.extend(top);
        }
        offsets.push(links.len());
        let zero_link_leaves = offsets.windows(2).filter(|pair| pair[0] == pair[1]).count();
        Ok(Self {
            offsets,
            links,
            directed_cross_leaf_arcs,
            zero_link_leaves,
        })
    }

    /// Select first40 L2 plus eight vote replacements; shortages retain original L2 order.
    /// For fewer than48 leaves, return every leaf in the same deterministic policy.
    pub fn select(&self, l2: &[usize]) -> Result<Vec<usize>> {
        let leaves = self.offsets.len() - 1;
        require(l2.len() == leaves, "boundary L2 length")?;
        let mut rank = vec![usize::MAX; leaves];
        for (position, &leaf) in l2.iter().enumerate() {
            require(
                leaf < leaves && rank[leaf] == usize::MAX,
                "boundary L2 permutation",
            )?;
            rank[leaf] = position;
        }
        let seeds = leaves.min(40);
        let mut votes = vec![0_u64; leaves];
        for (position, &leaf) in l2[..seeds].iter().enumerate() {
            for link in &self.links[self.offsets[leaf]..self.offsets[leaf + 1]] {
                let target = link.leaf as usize;
                if rank[target] >= seeds {
                    votes[target] = boundary_vote(
                        votes[target],
                        u64::from(link.count),
                        (40 - position) as u64,
                    )?;
                }
            }
        }
        let mut candidates = l2[seeds..]
            .iter()
            .copied()
            .filter(|&leaf| votes[leaf] > 0)
            .collect::<Vec<_>>();
        candidates.sort_unstable_by(|&a, &b| {
            votes[b]
                .cmp(&votes[a])
                .then(rank[a].cmp(&rank[b]))
                .then(a.cmp(&b))
        });
        let target = leaves.min(48);
        let mut selected = Vec::with_capacity(target);
        selected.extend_from_slice(&l2[..seeds]);
        selected.extend(candidates.into_iter().take(target - seeds));
        for &leaf in &l2[seeds..] {
            if selected.len() == target {
                break;
            }
            if !selected.contains(&leaf) {
                selected.push(leaf);
            }
        }
        Ok(selected)
    }
}

fn boundary_vote(total: u64, count: u64, weight: u64) -> Result<u64> {
    total
        .checked_add(count.checked_mul(weight).ok_or("boundary vote overflow")?)
        .ok_or_else(|| "boundary vote overflow".into())
}

/// Check native centroid geometry and exact encoded size before decoding.
pub fn preflight(
    blob: &[u8],
    rows: usize,
    dimensions: usize,
    profile: SemanticProfile,
) -> Result<Geometry> {
    if blob.len() < HEADER_BYTES || &blob[..8] != b"BORSUCP1" || blob[28..32] != [0; 4] {
        return Err("centroid header".into());
    }
    let header_rows = usize::try_from(u64::from_le_bytes(blob[8..16].try_into()?))?;
    let header_dimensions = u32::from_le_bytes(blob[16..20].try_into()?) as usize;
    let unit_rows = u32::from_le_bytes(blob[20..24].try_into()?);
    let page_rows = u32::from_le_bytes(blob[24..28].try_into()?);
    if !profile.valid_geometry(header_rows, header_dimensions)
        || unit_rows != 32
        || page_rows != 256
        || header_rows != rows
        || header_dimensions != dimensions
    {
        return Err("centroid geometry or manifest agreement".into());
    }
    let units = sum(&[rows, 31])? / 32;
    let blob_bytes = sum(&[HEADER_BYTES, product(&[units, dimensions, 2])?])?;
    if blob_bytes != blob.len() || (profile == SemanticProfile::Native100k && blob_bytes > BLOB_CAP)
    {
        return Err("centroid exact payload length or unit cap".into());
    }
    Ok(Geometry {
        rows,
        dimensions,
        units,
        blob_bytes,
    })
}

/// Conservative maximum of phases whose allocations do not coexist; not RSS.
pub fn admit(geometry: Geometry, cap: usize, profile: SemanticProfile) -> Result<usize> {
    admit_with_partition(geometry, cap, profile, LeafPartition::Radial64V1)
}

/// Mode-aware payload admission for construction and enclosing publication adapters.
pub(crate) fn admit_with_partition(
    geometry: Geometry,
    cap: usize,
    profile: SemanticProfile,
    partition: LeafPartition,
) -> Result<usize> {
    let estimate = allocation_phases(geometry, profile, partition)?
        .into_iter()
        .max()
        .unwrap();
    require(
        estimate <= cap,
        &format!("modeled allocation admission: {estimate} bytes exceeds {cap}"),
    )?;
    Ok(estimate)
}

fn allocation_phases(
    geometry: Geometry,
    profile: SemanticProfile,
    partition: LeafPartition,
) -> Result<[usize; 3]> {
    let Geometry {
        rows,
        dimensions: d,
        units: u,
        blob_bytes: b,
    } = geometry;
    require(
        size_of::<usize>() == 8
            && profile.valid_geometry(rows, d)
            && u == rows.div_ceil(32)
            && b == sum(&[HEADER_BYTES, product(&[u, d, 2])?])?,
        "allocation geometry",
    )?;
    let c = u.div_ceil(LEAF_UNITS);
    let l = product(&[2, c])?
        .checked_sub(1)
        .ok_or("allocation geometry")?;
    let f = sum(&[product(&[u, d, 4])?, product(&[u, 4])?])?;
    let v = sum(&[
        product(&[u, d, 4])?,
        product(&[u, size_of::<Vec<f32>>() + 64])?,
    ])?;
    let i = product(&[c, sum(&[product(&[d, 4])?, 128])?])?;
    // Every nonterminal child loses >=31 quotas above fanout32; at <=32
    // all children are terminal. 489 centers therefore need <=16 frames.
    let h = (c - 1).div_ceil(31);
    let r = product(&[
        h,
        sum(&[product(&[32, d, 16])?, product(&[u, 64])?, 32 * 128])?,
    ])?;
    let p = product(&[u, sum(&[product(&[d, 2])?, 4])?])?;
    let w = sum(&[
        BINARY_HEADER,
        product(&[l, sum(&[DIRECTORY_BYTES, product(&[d, 4])?])?])?,
    ])?;
    let prototypes = product(&[3, l, sum(&[product(&[d, 4])?, 256])?])?;
    // In-place sorting adds only recursion; publication also holds one group roster.
    let partition_stack = if partition == LeafPartition::WidestAxisQuotaV1 {
        product(&[c.next_power_of_two().trailing_zeros() as usize + 1, 128])?
    } else {
        0
    };
    let replay = if partition == LeafPartition::WidestAxisQuotaV1 {
        product(&[u, size_of::<(f64, usize)>()])?
    } else {
        0
    };
    let training = sum(&[
        2 * ROOT_CAP,
        b,
        f,
        product(&[2, v])?,
        r,
        product(&[2, i])?,
        8 * 1024 * 1024,
    ])?;
    let emit = sum(&[
        2 * ROOT_CAP,
        b,
        f,
        v,
        i,
        product(&[64, u])?,
        p,
        product(&[4, u])?,
        prototypes,
        partition_stack,
        4 * 1024 * 1024,
        8 * 1024 * 1024,
    ])?;
    let validate = sum(&[
        2 * ROOT_CAP,
        b,
        f,
        product(&[2, p])?,
        product(&[16, u])?,
        product(&[3, w])?,
        prototypes,
        replay,
        partition_stack,
        8 * 1024 * 1024,
    ])?;
    Ok([training, emit, validate])
}

/// Authenticate the bounded root and charge its mode against the caller's remaining budget.
/// The phase model includes router root/membership/payload/centroid buffers; callers
/// separately reserve their other retained inputs before this pre-allocation check.
pub fn admit_authenticated_root(
    body: &[u8],
    root_sha256: &str,
    input: &SourceIdentity<'_>,
    available: usize,
) -> Result<(LeafPartition, usize)> {
    require(
        (BINARY_HEADER..=input.profile.root_cap()).contains(&body.len())
            && valid_sha(root_sha256)
            && hash(body) == root_sha256
            && &body[..8] == b"BORSUSR3"
            && input.profile.valid_geometry(input.rows, input.dimensions),
        "publication router root identity/geometry",
    )?;
    let partition = LeafPartition::from_code(word32(body, 88))?;
    let units = input.rows.div_ceil(32);
    let estimate = admit_with_partition(
        Geometry {
            rows: input.rows,
            dimensions: input.dimensions,
            units,
            blob_bytes: sum(&[HEADER_BYTES, product(&[units, input.dimensions, 2])?])?,
        },
        available.min(input.profile.allocation_cap()),
        input.profile,
        partition,
    )?;
    // Full header/directory/source checks allocate only after caller-budget admission.
    decode_root(body, input)?;
    Ok((partition, estimate))
}

fn squared_distance(left: &[f32], right: &[f32]) -> f64 {
    left.iter()
        .zip(right)
        .map(|(a, b)| (f64::from(*a) - f64::from(*b)).powi(2))
        .sum()
}

/// Assign original units to trained centers with the frozen distance/ID ordering.
pub fn assign_groups(sample: &[Vec<f32>], centers: &[Vec<f32>]) -> Result<Vec<Vec<(f64, usize)>>> {
    if sample.is_empty()
        || sample[0].is_empty()
        || sample
            .iter()
            .any(|v| v.len() != sample[0].len() || v.iter().any(|x| !x.is_finite()))
        || centers.is_empty()
        || centers
            .iter()
            .any(|center| center.len() != sample[0].len() || center.iter().any(|v| !v.is_finite()))
    {
        return Err("trainer returned invalid centers".into());
    }
    let mut assignments = Vec::with_capacity(sample.len());
    let mut counts = vec![0_usize; centers.len()];
    for (unit, vector) in sample.iter().enumerate() {
        let mut best = 0;
        let mut distance = squared_distance(vector, &centers[0]);
        for (ordinal, center) in centers.iter().enumerate().skip(1) {
            let candidate = squared_distance(vector, center);
            if candidate < distance {
                best = ordinal;
                distance = candidate;
            }
        }
        if !distance.is_finite() {
            return Err("nonfinite assignment distance".into());
        }
        counts[best] += 1;
        assignments.push((best, distance, unit));
    }
    let mut groups = counts
        .into_iter()
        .map(Vec::with_capacity)
        .collect::<Vec<_>>();
    for (best, distance, unit) in assignments {
        groups[best].push((distance, unit));
    }
    for group in &mut groups {
        group.sort_unstable_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
    }
    Ok(groups)
}

fn prototype(scorer: &UnitCentroidPages, units: &[usize]) -> Result<Vec<f32>> {
    let mut sums = vec![0.0_f64; scorer.dimensions()];
    for &unit in units {
        let vector = scorer.unit_centroid(unit).ok_or("prototype unit ID")?;
        for (sum, value) in sums.iter_mut().zip(vector) {
            *sum += f64::from(*value);
        }
    }
    let mean = sums
        .into_iter()
        .map(|sum| (sum / units.len() as f64) as f32)
        .collect::<Vec<_>>();
    if mean.iter().any(|v| !v.is_finite()) {
        return Err("nonfinite root prototype".into());
    }
    Ok(mean)
}

fn unit_row_count(geometry: Geometry, unit: usize) -> usize {
    (geometry.rows - 32 * unit).min(32)
}

fn spatial_leaves(
    group: &mut [(f64, usize)],
    leaves: usize,
    scorer: &UnitCentroidPages,
    emit: &mut impl FnMut(&[(f64, usize)]) -> Result<()>,
) -> Result<()> {
    if leaves == 1 {
        group.sort_unstable_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
        return emit(group);
    }
    let mut axis = 0;
    let mut widest = 0.0_f64;
    for d in 0..scorer.dimensions() {
        let mut low = f64::INFINITY;
        let mut high = f64::NEG_INFINITY;
        for &(_, unit) in group.iter() {
            let value = f64::from(scorer.unit_centroid(unit).ok_or("partition unit ID")?[d]);
            low = low.min(value);
            high = high.max(value);
        }
        if high - low > widest {
            widest = high - low;
            axis = d;
        }
    }
    group.sort_unstable_by(|a, b| {
        let x = scorer.unit_centroid(a.1).unwrap()[axis];
        let y = scorer.unit_centroid(b.1).unwrap()[axis];
        x.partial_cmp(&y).unwrap().then(a.1.cmp(&b.1))
    });
    let left_leaves = leaves / 2;
    let left_units = group.len() / leaves * left_leaves + (group.len() % leaves).min(left_leaves);
    let (left, right) = group.split_at_mut(left_units);
    spatial_leaves(left, left_leaves, scorer, emit)?;
    spatial_leaves(right, leaves - left_leaves, scorer, emit)
}

fn partition_group(
    group: &mut [(f64, usize)],
    scorer: &UnitCentroidPages,
    partition: LeafPartition,
    emit: &mut impl FnMut(&[(f64, usize)]) -> Result<()>,
) -> Result<()> {
    if group.is_empty() {
        return Ok(());
    }
    if partition == LeafPartition::WidestAxisQuotaV1 && group.len() > LEAF_UNITS {
        let leaves = group.len().div_ceil(LEAF_UNITS);
        spatial_leaves(group, leaves, scorer, emit)
    } else {
        for chunk in group.chunks(LEAF_UNITS) {
            emit(chunk)?;
        }
        Ok(())
    }
}

fn quota_sizes_match(total: usize, leaves: usize, size: &impl Fn(usize) -> usize) -> bool {
    if leaves != total.div_ceil(LEAF_UNITS) {
        return false;
    }
    fn visit(n: usize, l: usize, offset: usize, size: &impl Fn(usize) -> usize) -> bool {
        if l == 1 {
            return size(offset) == n;
        }
        let left = l / 2;
        let units = n / l * left + (n % l).min(left);
        visit(units, left, offset, size) && visit(n - units, l - left, offset + left, size)
    }
    leaves > 0 && visit(total, leaves, 0, size)
}

/// Build using the existing deterministic trainer and an explicit allocation cap.
/// The source descriptor must already be authenticated by the caller.
pub fn build(
    blob: &[u8],
    input: &SourceIdentity<'_>,
    allocation_limit: usize,
) -> Result<RouterArtifacts> {
    build_with_partition(blob, input, allocation_limit, LeafPartition::Radial64V1)
}

/// Build with an explicit experimental partition, preserving training and source payloads.
pub fn build_with_partition(
    blob: &[u8],
    input: &SourceIdentity<'_>,
    allocation_limit: usize,
    partition: LeafPartition,
) -> Result<RouterArtifacts> {
    require(
        allocation_limit <= input.profile.allocation_cap(),
        "construction allocation limit",
    )?;
    let geometry = preflight(blob, input.rows, input.dimensions, input.profile)?;
    admit_with_partition(geometry, allocation_limit, input.profile, partition)?;
    require(
        (1..=256).contains(&input.schema.len())
            && valid_sha(input.root_sha256)
            && valid_sha(input.centroids_sha256)
            && hash(blob) == input.centroids_sha256,
        "source identity",
    )?;
    let scorer = UnitCentroidPages::decode(blob)?;
    let sample = (0..geometry.units)
        .map(|unit| scorer.unit_centroid(unit).unwrap().to_vec())
        .collect::<Vec<_>>();
    let requested = geometry.units.div_ceil(LEAF_UNITS);
    let identical = sample.iter().all(|vector| vector == &sample[0]);
    let training_centers = if identical { 1 } else { requested };
    // No retry or fallback geometry: duplicate-center and other trainer errors propagate.
    let centers = train_logical_cell_centroids(
        &sample,
        VectorMetric::SquaredEuclidean,
        training_centers,
        ITERATIONS,
    )?;
    let groups = assign_groups(&sample, &centers)?;
    drop((sample, centers));
    build_assigned(
        blob,
        input,
        allocation_limit,
        partition,
        training_centers,
        groups,
        scorer,
    )
}

// Shared production emission seam; tests fix assignments without claiming trainer behavior.
fn build_assigned(
    blob: &[u8],
    input: &SourceIdentity<'_>,
    allocation_limit: usize,
    partition: LeafPartition,
    training_centers: usize,
    mut groups: Vec<Vec<(f64, usize)>>,
    scorer: UnitCentroidPages,
) -> Result<RouterArtifacts> {
    let geometry = preflight(blob, input.rows, input.dimensions, input.profile)?;
    let estimate = admit_with_partition(geometry, allocation_limit, input.profile, partition)?;
    let requested = geometry.units.div_ceil(LEAF_UNITS);
    let record_bytes = 4 + geometry.dimensions * 2;
    let mut membership = vec![0_u8; geometry.units * 4];
    let mut payload = Vec::with_capacity(geometry.units * record_bytes);
    let mut leaves = Vec::with_capacity(2 * requested - 1);
    for (group_ordinal, group) in groups.iter_mut().enumerate() {
        let mut chunk_ordinal = 0;
        partition_group(group, &scorer, partition, &mut |chunk| {
            let leaf_id = leaves.len();
            let offset = payload.len();
            let units = chunk.iter().map(|(_, unit)| *unit).collect::<Vec<_>>();
            let mut source_rows = 0;
            for &unit in &units {
                membership[unit * 4..unit * 4 + 4]
                    .copy_from_slice(&u32::try_from(leaf_id)?.to_le_bytes());
                payload.extend_from_slice(&u32::try_from(unit)?.to_le_bytes());
                let original = HEADER_BYTES + unit * geometry.dimensions * 2;
                payload.extend_from_slice(&blob[original..original + geometry.dimensions * 2]);
                source_rows += unit_row_count(geometry, unit);
            }
            leaves.push(Leaf {
                leaf_id,
                group_ordinal,
                chunk_ordinal,
                offset,
                bytes: payload.len() - offset,
                unit_count: units.len(),
                source_rows,
                sha256: hash(&payload[offset..]),
                prototype: prototype(&scorer, &units)?,
            });
            chunk_ordinal += 1;
            Ok(())
        })?;
    }
    drop((scorer, groups));
    let manifest = Manifest {
        profile: input.profile,
        partition,
        schema: SCHEMA.into(),
        input_schema: input.schema.into(),
        input_root_sha256: input.root_sha256.into(),
        input_centroids_sha256: hash(&blob),
        input_centroids_bytes: blob.len(),
        rows: geometry.rows,
        dimensions: geometry.dimensions,
        unit_rows: 32,
        page_rows: 256,
        unit_count: geometry.units,
        final_unit_rows: unit_row_count(geometry, geometry.units - 1),
        algorithm: fixed_algorithm(requested, training_centers, partition),
        modeled_peak_allocation_bytes: estimate,
        modeled_allocation_limit_bytes: allocation_limit,
        allocation_model:
            "conservative allocation capacities; not RSS or an enforced process limit".into(),
        membership: Artifact {
            bytes: membership.len(),
            sha256: hash(&membership),
        },
        leaf_payload: Artifact {
            bytes: payload.len(),
            sha256: hash(&payload),
        },
        leaves,
    };
    let body = manifest.encode()?;
    validate_publication(&body, &membership, &payload, input, blob)?;
    Ok(RouterArtifacts {
        manifest: body,
        membership,
        leaves: payload,
    })
}

fn fixed_algorithm(requested: usize, training: usize, partition: LeafPartition) -> Algorithm {
    Algorithm {
        trainer: "train_logical_cell_centroids".into(),
        metric: "SquaredEuclidean".into(),
        iterations: ITERATIONS,
        requested_centers: requested,
        training_centers: training,
        max_leaf_units: LEAF_UNITS,
        nearest_ties: "center ordinal".into(),
        group_sort: partition.group_sort().into(),
        root_prototype: "unweighted unit mean; f64 accumulation to finite f32".into(),
        normalization: "none".into(),
        payload: "original FP16 little endian".into(),
    }
}
fn word32(body: &[u8], offset: usize) -> usize {
    u32::from_le_bytes(body[offset..offset + 4].try_into().unwrap()) as usize
}
fn word64(body: &[u8], offset: usize) -> Result<usize> {
    Ok(usize::try_from(u64::from_le_bytes(
        body[offset..offset + 8].try_into()?,
    ))?)
}
fn put32(body: &mut [u8], offset: usize, value: usize) -> Result<()> {
    body[offset..offset + 4].copy_from_slice(&u32::try_from(value)?.to_le_bytes());
    Ok(())
}
fn put64(body: &mut [u8], offset: usize, value: usize) -> Result<()> {
    body[offset..offset + 8].copy_from_slice(&u64::try_from(value)?.to_le_bytes());
    Ok(())
}
fn put_digest(body: &mut [u8], offset: usize, digest: &str) -> Result<()> {
    require(valid_sha(digest), "binary digest")?;
    for (slot, pair) in body[offset..offset + 32]
        .iter_mut()
        .zip(digest.as_bytes().chunks_exact(2))
    {
        *slot = u8::from_str_radix(std::str::from_utf8(pair)?, 16)?;
    }
    Ok(())
}
fn digest_text(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut text = String::with_capacity(64);
    for byte in bytes {
        write!(&mut text, "{byte:02x}").unwrap();
    }
    text
}
impl Manifest {
    /// Encode the fixed BORSUSR3 header, directory and exact little-endian f32 bits.
    /// Publication/open still validate geometry, identities and the full partition.
    pub fn encode(&self) -> Result<Vec<u8>> {
        require(
            self.schema == SCHEMA
                && self.algorithm
                    == fixed_algorithm(
                        self.unit_count.div_ceil(LEAF_UNITS),
                        self.algorithm.training_centers,
                        self.partition,
                    )
                && [1, self.algorithm.requested_centers].contains(&self.algorithm.training_centers),
            "binary schema or algorithm identity",
        )?;
        require(
            (1..=256).contains(&self.input_schema.len()),
            "source schema length",
        )?;
        let plane = product(&[self.leaves.len(), self.dimensions, 4])?;
        let directory = product(&[self.leaves.len(), DIRECTORY_BYTES])?;
        let length = sum(&[BINARY_HEADER, directory, plane])?;
        require(length <= self.profile.root_cap(), "binary root cap")?;
        let mut body = vec![0; length];
        body[..8].copy_from_slice(b"BORSUSR3");
        put32(&mut body, 8, BINARY_HEADER)?;
        put32(&mut body, 12, self.profile.code() as usize)?;
        put64(&mut body, 16, self.rows)?;
        for (offset, value) in [
            (24, self.dimensions),
            (28, self.unit_count),
            (32, self.leaves.len()),
            (36, self.algorithm.requested_centers),
            (40, self.algorithm.training_centers),
            (44, self.input_schema.len()),
        ] {
            put32(&mut body, offset, value)?;
        }
        for (offset, value) in [
            (48, self.input_centroids_bytes),
            (56, self.membership.bytes),
            (64, self.leaf_payload.bytes),
            (72, self.modeled_peak_allocation_bytes),
            (80, self.modeled_allocation_limit_bytes),
        ] {
            put64(&mut body, offset, value)?;
        }
        put32(&mut body, 88, self.partition.code())?;
        for (offset, digest) in [
            (128, &self.input_root_sha256),
            (160, &self.input_centroids_sha256),
            (192, &self.membership.sha256),
            (224, &self.leaf_payload.sha256),
        ] {
            put_digest(&mut body, offset, digest)?;
        }
        body[256..256 + self.input_schema.len()].copy_from_slice(self.input_schema.as_bytes());
        for (id, leaf) in self.leaves.iter().enumerate() {
            require(
                leaf.prototype.len() == self.dimensions,
                "prototype encoding geometry",
            )?;
            let start = BINARY_HEADER + id * DIRECTORY_BYTES;
            for (offset, value) in [
                (0, leaf.group_ordinal),
                (4, leaf.chunk_ordinal),
                (16, leaf.bytes),
                (20, leaf.unit_count),
                (24, leaf.source_rows),
            ] {
                put32(&mut body, start + offset, value)?;
            }
            put64(&mut body, start + 8, leaf.offset)?;
            put_digest(&mut body, start + 32, &leaf.sha256)?;
            let start = BINARY_HEADER + directory + id * self.dimensions * 4;
            for (word, value) in body[start..start + self.dimensions * 4]
                .chunks_exact_mut(4)
                .zip(&leaf.prototype)
            {
                word.copy_from_slice(&value.to_bits().to_le_bytes());
            }
        }
        Ok(body)
    }
}
// Called only after the enclosing root SHA and profile byte cap are checked.
fn decode_root(body: &[u8], input: &SourceIdentity<'_>) -> Result<Manifest> {
    require(
        body.len() >= BINARY_HEADER && &body[..8] == b"BORSUSR3",
        "binary root version/header",
    )?;
    let profile = SemanticProfile::from_code(word32(body, 12) as u32)?;
    let partition = LeafPartition::from_code(word32(body, 88))?;
    let rows = word64(body, 16)?;
    let d = word32(body, 24);
    let u = word32(body, 28);
    let l = word32(body, 32);
    let requested = word32(body, 36);
    let training = word32(body, 40);
    let schema_len = word32(body, 44);
    require(
        word32(body, 8) == BINARY_HEADER
            && profile == input.profile
            && profile.valid_geometry(rows, d)
            && rows == input.rows
            && d == input.dimensions
            && u == rows.div_ceil(32)
            && requested == u.div_ceil(LEAF_UNITS)
            && [1, requested].contains(&training)
            && (1..=2 * requested - 1).contains(&l)
            && (1..=256).contains(&schema_len)
            && body[92..128].iter().all(|&b| b == 0)
            && body[256 + schema_len..BINARY_HEADER]
                .iter()
                .all(|&b| b == 0),
        "binary header geometry/reserved",
    )?;
    let schema = std::str::from_utf8(&body[256..256 + schema_len])?;
    require(schema == input.schema, "binary source schema")?;
    let b = sum(&[HEADER_BYTES, product(&[u, d, 2])?])?;
    let payload = product(&[u, sum(&[4, product(&[d, 2])?])?])?;
    let limit = word64(body, 80)?;
    require(
        limit <= profile.allocation_cap(),
        "binary construction limit",
    )?;
    let estimate = admit_with_partition(
        Geometry {
            rows,
            dimensions: d,
            units: u,
            blob_bytes: b,
        },
        limit,
        profile,
        partition,
    )?;
    let directory = product(&[l, DIRECTORY_BYTES])?;
    let plane_start = sum(&[BINARY_HEADER, directory])?;
    require(
        body.len() == sum(&[plane_start, product(&[l, d, 4])?])?
            && word64(body, 48)? == b
            && word64(body, 56)? == product(&[u, 4])?
            && word64(body, 64)? == payload
            && word64(body, 72)? == estimate,
        "binary exact lengths/accounting",
    )?;
    // Validate every directory and coefficient before allocating leaf/prototype vectors.
    let mut end = 0;
    let mut total_units = 0;
    let mut total_rows = 0;
    let mut previous: Option<(usize, usize, usize)> = None;
    for id in 0..l {
        let start = BINARY_HEADER + id * DIRECTORY_BYTES;
        let group = word32(body, start);
        let chunk = word32(body, start + 4);
        let bytes = word32(body, start + 16);
        let units = word32(body, start + 20);
        let source_rows = word32(body, start + 24);
        let ordered = match previous {
            None => chunk == 0,
            Some((g, c, n)) if g == group => {
                chunk == c + 1 && (partition == LeafPartition::WidestAxisQuotaV1 || n == LEAF_UNITS)
            }
            Some((g, _, _)) => group > g && chunk == 0,
        };
        require(
            group < training
                && ordered
                && (1..=LEAF_UNITS).contains(&units)
                && word64(body, start + 8)? == end
                && bytes == units * (4 + d * 2)
                && (1..=units * 32).contains(&source_rows)
                && body[start + 28..start + 32] == [0; 4],
            "binary leaf geometry/reserved",
        )?;
        end = sum(&[end, bytes])?;
        total_units = sum(&[total_units, units])?;
        total_rows = sum(&[total_rows, source_rows])?;
        previous = Some((group, chunk, units));
    }
    require(
        end == payload && total_units == u && total_rows == rows,
        "binary complete directory",
    )?;
    if partition == LeafPartition::WidestAxisQuotaV1 {
        let mut start = 0;
        while start < l {
            let group = word32(body, BINARY_HEADER + start * DIRECTORY_BYTES);
            let mut end = start + 1;
            while end < l && word32(body, BINARY_HEADER + end * DIRECTORY_BYTES) == group {
                end += 1;
            }
            let count = (start..end).try_fold(0, |total, id| {
                sum(&[
                    total,
                    word32(body, BINARY_HEADER + id * DIRECTORY_BYTES + 20),
                ])
            })?;
            require(
                quota_sizes_match(count, end - start, &|id| {
                    word32(body, BINARY_HEADER + (start + id) * DIRECTORY_BYTES + 20)
                }),
                "binary spatial quota sizes",
            )?;
            start = end;
        }
    }
    for word in body[plane_start..].chunks_exact(4) {
        require(
            f32::from_bits(u32::from_le_bytes(word.try_into()?)).is_finite(),
            "binary nonfinite prototype",
        )?;
    }
    // Hash strings and nested vectors are allocated only after the complete scan.
    let input_root_sha256 = digest_text(&body[128..160]);
    let input_centroids_sha256 = digest_text(&body[160..192]);
    require(
        input_root_sha256 == input.root_sha256 && input_centroids_sha256 == input.centroids_sha256,
        "binary source digests",
    )?;
    let mut leaves = Vec::with_capacity(l);
    for id in 0..l {
        let start = BINARY_HEADER + id * DIRECTORY_BYTES;
        let prototype_start = plane_start + id * d * 4;
        leaves.push(Leaf {
            leaf_id: id,
            group_ordinal: word32(body, start),
            chunk_ordinal: word32(body, start + 4),
            offset: word64(body, start + 8)?,
            bytes: word32(body, start + 16),
            unit_count: word32(body, start + 20),
            source_rows: word32(body, start + 24),
            sha256: digest_text(&body[start + 32..start + 64]),
            prototype: body[prototype_start..prototype_start + d * 4]
                .chunks_exact(4)
                .map(|word| f32::from_bits(u32::from_le_bytes(word.try_into().unwrap())))
                .collect(),
        });
    }
    Ok(Manifest {
        profile,
        partition,
        schema: SCHEMA.into(),
        input_schema: schema.into(),
        input_root_sha256,
        input_centroids_sha256,
        input_centroids_bytes: b,
        rows,
        dimensions: d,
        unit_rows: 32,
        page_rows: 256,
        unit_count: u,
        final_unit_rows: (rows - 32 * (u - 1)).min(32),
        algorithm: fixed_algorithm(requested, training, partition),
        modeled_peak_allocation_bytes: estimate,
        modeled_allocation_limit_bytes: limit,
        allocation_model:
            "conservative allocation capacities; not RSS or an enforced process limit".into(),
        membership: Artifact {
            bytes: u * 4,
            sha256: digest_text(&body[192..224]),
        },
        leaf_payload: Artifact {
            bytes: payload,
            sha256: digest_text(&body[224..256]),
        },
        leaves,
    })
}

/// Checked payload capacities for the inverse directory and its construction peak.
/// The latter includes both per-leaf counting/cursor and row-count scratch arrays.
pub(crate) fn directory_allocation_bytes(units: usize, leaves: usize) -> Result<(usize, usize)> {
    require(
        size_of::<usize>() == 8
            && units > 0
            && units <= u32::MAX as usize
            && leaves > 0
            && leaves <= units,
        "membership directory geometry",
    )?;
    let persistent = sum(&[
        product(&[units, size_of::<u32>()])?,
        product(&[sum(&[leaves, 1])?, size_of::<usize>()])?,
    ])?;
    Ok((
        persistent,
        sum(&[persistent, product(&[2, leaves, size_of::<usize>()])?])?,
    ))
}

/// Root and membership authenticated without loading the centroid or leaf objects.
/// Construction limits and the caller's root byte cap bound retained metadata.
pub struct SemanticUnitRouter {
    manifest: Manifest,
    membership: Vec<usize>,
    leaf_offsets: Vec<usize>,
    leaf_units: Vec<u32>,
}

impl SemanticUnitRouter {
    /// Authenticate the exact root and membership before exposing leaf ranges.
    /// The enclosing generation supplies both the router SHA and source binding.
    pub fn open(
        body: &[u8],
        membership: &[u8],
        root_sha256: &str,
        input: &SourceIdentity<'_>,
        root_byte_cap: usize,
    ) -> Result<Self> {
        require(
            body.len() <= root_byte_cap.min(input.profile.root_cap())
                && valid_sha(root_sha256)
                && hash(body) == root_sha256,
            "router root size or identity",
        )?;
        require(
            input.profile.valid_geometry(input.rows, input.dimensions)
                && (1..=256).contains(&input.schema.len())
                && valid_sha(input.root_sha256)
                && valid_sha(input.centroids_sha256),
            "source identity or geometry",
        )?;
        let manifest = decode_root(body, input)?;
        let geometry = Geometry {
            rows: input.rows,
            dimensions: input.dimensions,
            units: input.rows.div_ceil(32),
            blob_bytes: HEADER_BYTES + input.rows.div_ceil(32) * input.dimensions * 2,
        };
        let requested = geometry.units.div_ceil(LEAF_UNITS);
        let algorithm = &manifest.algorithm;
        let estimate = admit_with_partition(
            geometry,
            manifest
                .modeled_allocation_limit_bytes
                .min(input.profile.allocation_cap()),
            input.profile,
            manifest.partition,
        )?;
        require(
            manifest.profile == input.profile
                && manifest.schema == SCHEMA
                && manifest.input_schema == input.schema
                && manifest.input_root_sha256 == input.root_sha256
                && manifest.input_centroids_sha256 == input.centroids_sha256
                && manifest.input_centroids_bytes == geometry.blob_bytes
                && manifest.rows == geometry.rows
                && manifest.dimensions == geometry.dimensions
                && manifest.unit_rows == 32
                && manifest.page_rows == 256
                && manifest.unit_count == geometry.units
                && manifest.final_unit_rows == unit_row_count(geometry, geometry.units - 1)
                && manifest.modeled_peak_allocation_bytes == estimate
                && manifest.modeled_allocation_limit_bytes <= input.profile.allocation_cap()
                && manifest.allocation_model
                    == "conservative allocation capacities; not RSS or an enforced process limit"
                && algorithm.trainer == "train_logical_cell_centroids"
                && algorithm.metric == "SquaredEuclidean"
                && algorithm.iterations == ITERATIONS
                && algorithm.requested_centers == requested
                && [1, requested].contains(&algorithm.training_centers)
                && algorithm.max_leaf_units == LEAF_UNITS
                && algorithm.nearest_ties == "center ordinal"
                && algorithm.group_sort == manifest.partition.group_sort()
                && algorithm.root_prototype
                    == "unweighted unit mean; f64 accumulation to finite f32"
                && algorithm.normalization == "none"
                && algorithm.payload == "original FP16 little endian",
            "router identities, geometry or algorithm",
        )?;
        let record_bytes = 4 + geometry.dimensions * 2;
        require(
            membership.len() == geometry.units * 4
                && manifest.membership.bytes == membership.len()
                && manifest.membership.sha256 == hash(membership)
                && manifest.leaf_payload.bytes == geometry.units * record_bytes
                && valid_sha(&manifest.leaf_payload.sha256)
                && (1..=2 * requested - 1).contains(&manifest.leaves.len()),
            "router body descriptors",
        )?;
        let (_, directory_peak) =
            directory_allocation_bytes(geometry.units, manifest.leaves.len())?;
        require(
            directory_peak <= manifest.modeled_allocation_limit_bytes,
            "membership directory allocation cap",
        )?;
        let membership = membership
            .chunks_exact(4)
            .map(|word| u32::from_le_bytes(word.try_into().unwrap()) as usize)
            .collect::<Vec<_>>();
        let mut counts = vec![0; manifest.leaves.len()];
        let mut rows = vec![0; manifest.leaves.len()];
        for (unit, &leaf) in membership.iter().enumerate() {
            require(leaf < manifest.leaves.len(), "membership leaf ID")?;
            counts[leaf] += 1;
            rows[leaf] += unit_row_count(geometry, unit);
        }
        let mut offset = 0;
        let mut previous: Option<&Leaf> = None;
        for (id, leaf) in manifest.leaves.iter().enumerate() {
            let ordered = previous.map_or(leaf.chunk_ordinal == 0, |prev| {
                if prev.group_ordinal == leaf.group_ordinal {
                    prev.chunk_ordinal.checked_add(1) == Some(leaf.chunk_ordinal)
                        && (manifest.partition == LeafPartition::WidestAxisQuotaV1
                            || prev.unit_count == LEAF_UNITS)
                } else {
                    leaf.group_ordinal > prev.group_ordinal && leaf.chunk_ordinal == 0
                }
            });
            require(
                leaf.leaf_id == id
                    && (1..=LEAF_UNITS).contains(&leaf.unit_count)
                    && leaf.group_ordinal < algorithm.training_centers
                    && ordered
                    && leaf.offset == offset
                    && leaf.bytes == leaf.unit_count * record_bytes
                    && leaf.unit_count == counts[id]
                    && leaf.source_rows == rows[id]
                    && valid_sha(&leaf.sha256)
                    && leaf.prototype.len() == geometry.dimensions
                    && leaf.prototype.iter().all(|v| v.is_finite()),
                "leaf numbering, order, geometry or membership",
            )?;
            offset = sum(&[offset, leaf.bytes])?;
            require(offset <= manifest.leaf_payload.bytes, "leaf bounds")?;
            previous = Some(leaf);
        }
        require(
            offset == manifest.leaf_payload.bytes,
            "incomplete leaf ranges",
        )?;
        if manifest.partition == LeafPartition::WidestAxisQuotaV1 {
            let mut start = 0;
            while start < manifest.leaves.len() {
                let group = manifest.leaves[start].group_ordinal;
                let end = start
                    + manifest.leaves[start..]
                        .iter()
                        .take_while(|leaf| leaf.group_ordinal == group)
                        .count();
                let count = counts[start..end]
                    .iter()
                    .try_fold(0, |n, &size| sum(&[n, size]))?;
                require(
                    quota_sizes_match(count, end - start, &|id| counts[start + id]),
                    "spatial membership quota sizes",
                )?;
                start = end;
            }
        }
        let mut leaf_offsets = Vec::with_capacity(manifest.leaves.len() + 1);
        leaf_offsets.push(0);
        for count in &mut counts {
            let start = *leaf_offsets.last().ok_or("membership directory offset")?;
            leaf_offsets.push(sum(&[start, *count])?);
            *count = start;
        }
        let mut leaf_units = vec![0_u32; geometry.units];
        // Scanning in source-unit order makes every directory slice ascending.
        // Query nomination visits only selected slices, never all membership.
        for (unit, &leaf) in membership.iter().enumerate() {
            leaf_units[counts[leaf]] = u32::try_from(unit)?;
            counts[leaf] += 1;
        }
        Ok(Self {
            manifest,
            membership,
            leaf_offsets,
            leaf_units,
        })
    }

    /// Immutable authenticated descriptors for bounded range reads.
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// Authenticated leaf label for each physical unit, independent of logical IDs.
    pub fn unit_membership(&self) -> &[usize] {
        &self.membership
    }

    /// Normalize an original f32 cosine query using the existing native helper,
    /// then nominate leaves. Prepare source scoring from the original query first.
    pub fn nominate(&self, query: &[f32]) -> Result<Vec<usize>> {
        let normalized = crate::sq8_source::cosine_vector(query)?;
        self.select_leaves(&normalized)
    }

    /// Frozen nomination for a query already normalized by the native helper.
    pub fn select_leaves(&self, normalized_query: &[f32]) -> Result<Vec<usize>> {
        rank_leaves(
            normalized_query,
            self.manifest
                .leaves
                .iter()
                .map(|leaf| leaf.prototype.as_slice()),
            self.manifest.profile,
        )
    }

    /// Rank every authenticated leaf by an explicit metric, with leaf-ID ties.
    /// Supports squared Euclidean and cosine distance. Cosine accepts finite,
    /// nonzero queries; a zero prototype has zero similarity (distance one).
    /// This routing convention differs from `VectorMetric::distance` for zero
    /// and very small norms; it must not inherit that scorer's norm threshold.
    /// The caller chooses an admitted leaf count through `nominate_selected`.
    /// This does not change the persisted selection rule used by serving.
    /// Caller-owned ranking payload is at most 24 bytes per leaf (scores + IDs).
    pub fn rank_leaves_by_metric(
        &self,
        query: &[f32],
        metric: &VectorMetric,
    ) -> Result<Vec<usize>> {
        Ok(rank_prototypes(
            query,
            self.manifest
                .leaves
                .iter()
                .map(|leaf| leaf.prototype.as_slice()),
            metric,
        )?
        .into_iter()
        .map(|(_, id)| id)
        .collect())
    }

    /// Nominate every unit in the selected root-authenticated membership slices.
    /// No leaf payload or source record is needed to establish this partition.
    pub fn nominate_selected(&self, leaf_ids: &[usize]) -> Result<Nomination> {
        require(
            !leaf_ids.is_empty()
                && leaf_ids.len() <= self.manifest.profile.selected_leaf_limit()
                && (self.manifest.profile != SemanticProfile::Fresh1m || leaf_ids.len() == 48),
            "selected leaf count",
        )?;
        let mut selected = BTreeSet::new();
        let mut units = BTreeSet::new();
        for &id in leaf_ids {
            require(id < self.manifest.leaves.len(), "selected leaf ID")?;
            require(selected.insert(id), "duplicate selected leaf")?;
            units.extend(
                self.leaf_units[self.leaf_offsets[id]..self.leaf_offsets[id + 1]]
                    .iter()
                    .map(|&unit| unit as usize),
            );
        }
        Self::complete_nomination(leaf_ids, units, self.manifest.rows, self.manifest.profile)
    }

    /// Validate one exact whole-leaf range. Digest authentication fixes record order.
    /// Publication checks original coefficient/prototype identities separately;
    /// serving nominates directly from membership without reading these bodies.
    pub fn validate_leaf(&self, leaf_id: usize, body: &[u8]) -> Result<Vec<usize>> {
        let leaf = self
            .manifest
            .leaves
            .get(leaf_id)
            .ok_or("selected leaf ID")?;
        require(
            body.len() == leaf.bytes && hash(body) == leaf.sha256,
            "selected leaf size/hash",
        )?;
        let mut units = Vec::with_capacity(leaf.unit_count);
        let mut seen = BTreeSet::new();
        let mut rows = 0;
        for record in body.chunks_exact(4 + self.manifest.dimensions * 2) {
            let unit = u32::from_le_bytes(record[..4].try_into()?) as usize;
            require(
                self.membership.get(unit) == Some(&leaf_id) && seen.insert(unit),
                "selected leaf disjoint membership",
            )?;
            rows += (self.manifest.rows - unit * 32).min(32);
            units.push(unit);
        }
        require(
            units.len() == leaf.unit_count && rows == leaf.source_rows,
            "selected leaf count/rows",
        )?;
        Ok(units)
    }

    /// Authenticate the selected whole leaves and retain every unit, with only
    /// missing seed-page units added. No padding beyond the original page closure.
    pub fn validate_selected(&self, leaf_ids: &[usize], bodies: &[&[u8]]) -> Result<Nomination> {
        require(
            !leaf_ids.is_empty()
                && leaf_ids.len() <= self.manifest.profile.selected_leaf_limit()
                && (self.manifest.profile != SemanticProfile::Fresh1m || leaf_ids.len() == 48)
                && leaf_ids.len() == bodies.len(),
            "selected leaf count",
        )?;
        let mut selected = BTreeSet::new();
        let mut units = BTreeSet::new();
        let mut bytes = 0;
        for (&id, body) in leaf_ids.iter().zip(bodies) {
            require(selected.insert(id), "duplicate selected leaf")?;
            bytes = sum(&[bytes, body.len()])?;
            require(
                bytes
                    <= self
                        .manifest
                        .profile
                        .selected_leaf_bytes(self.manifest.dimensions)?,
                "selected leaf byte cap",
            )?;
            for unit in self.validate_leaf(id, body)? {
                require(units.insert(unit), "disjoint selected units")?;
            }
        }
        Self::complete_nomination(leaf_ids, units, self.manifest.rows, self.manifest.profile)
    }

    fn complete_nomination(
        leaf_ids: &[usize],
        units: BTreeSet<usize>,
        rows: usize,
        profile: SemanticProfile,
    ) -> Result<Nomination> {
        let (seed_page, walk_units, seed_additions) = seed_walk(&units, rows, profile)?;
        let page_closure = units.iter().map(|u| u / 8).collect();
        Ok(Nomination {
            leaf_ids: leaf_ids.to_vec(),
            units,
            seed_page,
            walk_units,
            seed_additions,
            page_closure,
        })
    }
}

/// Complete query nomination from the authenticated partition.
#[derive(Debug, PartialEq, Eq)]
pub struct Nomination {
    /// Leaf IDs in nomination order.
    pub leaf_ids: Vec<usize>,
    /// Every authenticated semantic unit; never truncated to nearest 256.
    pub units: BTreeSet<usize>,
    /// Lowest physical page containing a semantic unit.
    pub seed_page: usize,
    /// All semantic units plus only missing seed-page units, ordered by unit ID.
    pub walk_units: Vec<usize>,
    /// Units added to satisfy the shared scorer's complete seed-page invariant.
    pub seed_additions: Vec<usize>,
    /// Distinct original physical pages; seed additions cannot enlarge this set.
    pub page_closure: BTreeSet<usize>,
}

/// Publication-only complete partition/original-FP16/prototype validation.
/// Unlike `open` and `validate_selected`, this intentionally reads all objects.
pub fn validate_publication(
    body: &[u8],
    membership: &[u8],
    payload: &[u8],
    input: &SourceIdentity<'_>,
    blob: &[u8],
) -> Result<()> {
    let geometry = preflight(blob, input.rows, input.dimensions, input.profile)?;
    require(
        hash(blob) == input.centroids_sha256,
        "original centroid identity",
    )?;
    let router = SemanticUnitRouter::open(
        body,
        membership,
        &hash(body),
        input,
        input.profile.root_cap(),
    )?;
    let manifest = router.manifest();
    admit_with_partition(
        geometry,
        manifest.modeled_allocation_limit_bytes,
        input.profile,
        manifest.partition,
    )?;
    require(
        payload.len() == manifest.leaf_payload.bytes
            && hash(payload) == manifest.leaf_payload.sha256,
        "complete payload size/hash",
    )?;
    let scorer = UnitCentroidPages::decode(blob)?;
    let identical =
        (1..geometry.units).all(|unit| scorer.unit_centroid(unit) == scorer.unit_centroid(0));
    require(
        manifest.algorithm.training_centers
            == if identical {
                1
            } else {
                geometry.units.div_ceil(LEAF_UNITS)
            },
        "training center count",
    )?;
    let mut seen = vec![false; geometry.units];
    for leaf in &manifest.leaves {
        let part = &payload[leaf.offset..leaf.offset + leaf.bytes];
        let units = router.validate_leaf(leaf.leaf_id, part)?;
        let expected = prototype(&scorer, &units)?;
        require(
            leaf.prototype
                .iter()
                .map(|v| v.to_bits())
                .eq(expected.iter().map(|v| v.to_bits())),
            "root prototype identity",
        )?;
        for (unit, record) in units
            .into_iter()
            .zip(part.chunks_exact(4 + geometry.dimensions * 2))
        {
            require(!seen[unit], "complete disjoint unit partition")?;
            seen[unit] = true;
            let original = HEADER_BYTES + unit * geometry.dimensions * 2;
            require(
                record[4..] == blob[original..original + geometry.dimensions * 2],
                "original FP16 unit identity",
            )?;
        }
    }
    require(seen.iter().all(|v| *v), "incomplete unit partition")?;
    if manifest.partition == LeafPartition::WidestAxisQuotaV1 {
        let mut start = 0;
        while start < manifest.leaves.len() {
            let group = manifest.leaves[start].group_ordinal;
            let end = start
                + manifest.leaves[start..]
                    .iter()
                    .take_while(|leaf| leaf.group_ordinal == group)
                    .count();
            let first_unit = router.leaf_offsets[start];
            let last_unit = router.leaf_offsets[end];
            let mut roster = Vec::with_capacity(last_unit - first_unit);
            for &unit in &router.leaf_units[first_unit..last_unit] {
                roster.push((0.0, unit as usize));
            }
            // Centers are not persisted: replay proves spatial leaf sets, not assignment/radius order.
            let mut id = start;
            partition_group(&mut roster, &scorer, manifest.partition, &mut |chunk| {
                require(
                    id < end
                        && chunk.len() == manifest.leaves[id].unit_count
                        && chunk.iter().all(|&(_, unit)| router.membership[unit] == id),
                    "spatial partition replay membership",
                )?;
                id += 1;
                Ok(())
            })?;
            require(id == end, "spatial partition replay leaf count")?;
            start = end;
        }
    }
    Ok(())
}

/// Frozen first-eight/1.15-boundary/max-sixteen rule for normalized coordinates.
pub fn select_leaves(query: &[f32], prototypes: &[Vec<f32>]) -> Result<Vec<usize>> {
    rank_leaves(
        query,
        prototypes.iter().map(Vec::as_slice),
        SemanticProfile::Native100k,
    )
}
fn rank_leaves<'a>(
    query: &[f32],
    prototypes: impl ExactSizeIterator<Item = &'a [f32]>,
    profile: SemanticProfile,
) -> Result<Vec<usize>> {
    let ranked = rank_prototypes(query, prototypes, &VectorMetric::SquaredEuclidean)?;
    let mut count = match profile {
        SemanticProfile::Native100k => ranked.len().min(8),
        SemanticProfile::Scale1m => ranked.len().min(48),
        SemanticProfile::Fresh1m => {
            require(ranked.len() >= 48, "fresh selected leaf count")?;
            48
        }
    };
    if profile == SemanticProfile::Native100k && count == 8 {
        let boundary = ranked[7].0;
        for &(distance, _) in ranked.iter().take(16).skip(8) {
            if distance > 1.15 * boundary {
                break;
            }
            count += 1;
        }
    }
    Ok(ranked[..count].iter().map(|&(_, id)| id).collect())
}

fn rank_prototypes<'a>(
    query: &[f32],
    prototypes: impl ExactSizeIterator<Item = &'a [f32]>,
    metric: &VectorMetric,
) -> Result<Vec<(f64, usize)>> {
    require(
        matches!(
            metric,
            VectorMetric::SquaredEuclidean | VectorMetric::Cosine
        ),
        "unsupported root ranking metric",
    )?;
    require(
        !query.is_empty() && query.iter().all(|x| x.is_finite()) && prototypes.len() > 0,
        "root query geometry",
    )?;
    let query_norm = if *metric == VectorMetric::Cosine {
        let norm = query.iter().map(|&q| f64::from(q).powi(2)).sum::<f64>();
        require(norm.is_finite() && norm > 0.0, "cosine root query norm")?;
        norm.sqrt()
    } else {
        1.0
    };
    let mut ranked = Vec::with_capacity(prototypes.len());
    for (id, prototype) in prototypes.enumerate() {
        require(
            prototype.len() == query.len() && prototype.iter().all(|x| x.is_finite()),
            "root prototype geometry",
        )?;
        let distance = if *metric == VectorMetric::SquaredEuclidean {
            let mut distance = 0_f64;
            for (&q, &p) in query.iter().zip(prototype) {
                let delta = f64::from(q) - f64::from(p);
                distance += delta * delta;
            }
            distance
        } else {
            let mut dot = 0_f64;
            let mut norm = 0_f64;
            for (&q, &p) in query.iter().zip(prototype) {
                let p = f64::from(p);
                dot += f64::from(q) * p;
                norm += p * p;
            }
            require(
                dot.is_finite() && norm.is_finite(),
                "cosine root prototype norm",
            )?;
            if norm == 0.0 {
                1.0
            } else {
                1.0 - (dot / query_norm / norm.sqrt()).clamp(-1.0, 1.0)
            }
        };
        require(distance.is_finite(), "root distance")?;
        ranked.push((distance, id));
    }
    ranked.sort_unstable_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
    Ok(ranked)
}

/// Complete only the lowest nominated physical page, preserving the page closure.
pub fn seed_walk(
    units: &BTreeSet<usize>,
    rows: usize,
    profile: SemanticProfile,
) -> Result<(usize, Vec<usize>, Vec<usize>)> {
    require(
        match profile {
            SemanticProfile::Native100k => (1..=100_000).contains(&rows),
            SemanticProfile::Fresh1m => rows == 1_000_000,
            SemanticProfile::Scale1m => (1..=1_000_000).contains(&rows),
        } && !units.is_empty()
            && units.len() <= profile.selected_leaf_limit() * LEAF_UNITS
            && units.last().is_some_and(|&u| u < rows.div_ceil(32)),
        "semantic units",
    )?;
    require(
        units
            .iter()
            .map(|unit| unit / 8)
            .collect::<BTreeSet<_>>()
            .len()
            <= profile.closure_page_limit(),
        "semantic closure page cap",
    )?;
    let seed = units.first().ok_or("no semantic seed")? / 8;
    let additions = (seed * 8..((seed + 1) * 8).min(rows.div_ceil(32)))
        .filter(|u| !units.contains(u))
        .collect::<Vec<_>>();
    let mut walk = units.clone();
    walk.extend(additions.iter().copied());
    require(
        walk.len() <= profile.walk_unit_limit(),
        "semantic walk unit cap",
    )?;
    Ok((seed, walk.into_iter().collect(), additions))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spatial_blob(rows: usize, vectors: &[Vec<f32>]) -> Vec<u8> {
        assert_eq!(vectors.len(), rows.div_ceil(32));
        let dimensions = vectors[0].len();
        let mut bytes = b"BORSUCP1".to_vec();
        bytes.extend_from_slice(&(rows as u64).to_le_bytes());
        for word in [dimensions as u32, 32, 256, 0] {
            bytes.extend_from_slice(&word.to_le_bytes());
        }
        for vector in vectors {
            assert_eq!(vector.len(), dimensions);
            for &value in vector {
                bytes.extend_from_slice(&half::f16::from_f32(value).to_bits().to_le_bytes());
            }
        }
        bytes
    }

    fn spatial_chunks(scorer: &UnitCentroidPages, mut group: Vec<(f64, usize)>) -> Vec<Vec<usize>> {
        let mut chunks = Vec::new();
        partition_group(
            &mut group,
            scorer,
            LeafPartition::WidestAxisQuotaV1,
            &mut |chunk| {
                chunks.push(chunk.iter().map(|&(_, unit)| unit).collect());
                Ok(())
            },
        )
        .unwrap();
        chunks
    }

    #[test]
    fn spatial_quota_edges_and_terminal_radius_order() {
        for (n, sizes) in [
            (0_usize, vec![]),
            (1, vec![1]),
            (64, vec![64]),
            (65, vec![33, 32]),
            (127, vec![64, 63]),
            (128, vec![64, 64]),
            (129, vec![43, 43, 43]),
            (193, vec![49, 48, 48, 48]),
        ] {
            let vectors = (0..n.max(1))
                .map(|id| vec![id as f32, 0.])
                .collect::<Vec<_>>();
            let blob = spatial_blob(n.max(1) * 32, &vectors);
            let scorer = UnitCentroidPages::decode(&blob).unwrap();
            let mut group = (0..n).map(|id| ((n - id) as f64, id)).collect::<Vec<_>>();
            group.sort_unstable_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
            let chunks = spatial_chunks(&scorer, group.clone());
            assert_eq!(
                chunks.iter().map(Vec::len).collect::<Vec<_>>(),
                sizes,
                "n={n}"
            );
            assert!(quota_sizes_match(n, chunks.len(), &|i| chunks[i].len()) || n == 0);
            assert_eq!(
                chunks.iter().flatten().copied().collect::<BTreeSet<_>>(),
                (0..n).collect()
            );
            for chunk in &chunks {
                assert!(chunk.windows(2).all(|pair| pair[0] > pair[1]));
            }
            if n <= 64 {
                assert_eq!(
                    chunks.into_iter().flatten().collect::<Vec<_>>(),
                    group.iter().map(|&(_, id)| id).collect::<Vec<_>>()
                );
            }
            if n > 0 {
                let blob = self::blob(n * 32);
                let digest = hash(&blob);
                let input = SourceIdentity {
                    centroids_sha256: &digest,
                    ..source(&blob)
                };
                let artifacts = build_with_partition(
                    &blob,
                    &input,
                    ALLOCATION_CAP,
                    LeafPartition::WidestAxisQuotaV1,
                )
                .unwrap();
                let router = open(&artifacts, &input);
                assert_eq!(
                    router
                        .manifest()
                        .leaves
                        .iter()
                        .map(|leaf| leaf.unit_count)
                        .collect::<Vec<_>>(),
                    sizes
                );
            }
        }
        assert!(!quota_sizes_match(129, 4, &|_| 32));
        assert!(!quota_sizes_match(65, 2, &|i| [32, 33][i]));
    }

    #[test]
    fn spatial_axis_ties_signed_zero_identical_and_extremes() {
        // Equal spans choose axis zero, which reverses axis one's split.
        let vectors = (0..128)
            .map(|id| {
                if id % 2 == 0 {
                    vec![1., -1.]
                } else {
                    vec![-1., 1.]
                }
            })
            .collect::<Vec<_>>();
        let blob = spatial_blob(4096, &vectors);
        let scorer = UnitCentroidPages::decode(&blob).unwrap();
        let group = (0..128).map(|id| (1., id)).collect::<Vec<_>>();
        let chunks = spatial_chunks(&scorer, group.clone());
        assert_eq!(chunks[0], (1..128).step_by(2).collect::<Vec<_>>());
        assert_eq!(chunks[1], (0..128).step_by(2).collect::<Vec<_>>());
        let wider_second = vectors
            .iter()
            .map(|v| vec![v[0], v[1] * 2.])
            .collect::<Vec<_>>();
        let wider_blob = spatial_blob(4096, &wider_second);
        let wider = UnitCentroidPages::decode(&wider_blob).unwrap();
        assert_eq!(
            spatial_chunks(&wider, group.clone())[0],
            (0..128).step_by(2).collect::<Vec<_>>()
        );
        for signed in [false, true] {
            let vectors = (0..128)
                .map(|id| vec![if signed && id % 2 == 0 { -0. } else { 0. }, 0.])
                .collect::<Vec<_>>();
            let blob = spatial_blob(4096, &vectors);
            let scorer = UnitCentroidPages::decode(&blob).unwrap();
            assert_eq!(
                spatial_chunks(&scorer, group.clone()),
                vec![(0..64).collect::<Vec<_>>(), (64..128).collect::<Vec<_>>()]
            );
        }
        let vectors = (0..128)
            .map(|id| vec![if id % 2 == 0 { 65504. } else { -65504. }, 1.])
            .collect::<Vec<_>>();
        let blob = spatial_blob(4096, &vectors);
        let scorer = UnitCentroidPages::decode(&blob).unwrap();
        assert_eq!(
            spatial_chunks(&scorer, group)[0],
            (1..128).step_by(2).collect::<Vec<_>>()
        );
    }

    #[test]
    fn spatial_post_assignment_l2_top48_falsifier() {
        // Fixed post-assignment groups isolate partitioning; this is not a trainer proof.
        let vectors = (0..3200)
            .map(|id| {
                vec![
                    if id < 128 {
                        if id % 2 == 0 { 1. } else { -1. }
                    } else {
                        0.5
                    },
                    0.,
                ]
            })
            .collect::<Vec<_>>();
        let blob = spatial_blob(3200 * 32, &vectors);
        let digest = hash(&blob);
        let input = SourceIdentity {
            profile: SemanticProfile::Scale1m,
            centroids_sha256: &digest,
            ..source(&blob)
        };
        let mut groups = vec![(0..128).map(|id| (1., id)).collect::<Vec<_>>()];
        groups.extend((0..48).map(|g| {
            (128 + g * 64..128 + (g + 1) * 64)
                .map(|id| (0.25, id))
                .collect::<Vec<_>>()
        }));
        let radial = build_assigned(
            &blob,
            &input,
            input.profile.allocation_cap(),
            LeafPartition::Radial64V1,
            50,
            groups.clone(),
            UnitCentroidPages::decode(&blob).unwrap(),
        )
        .unwrap();
        let spatial = build_assigned(
            &blob,
            &input,
            input.profile.allocation_cap(),
            LeafPartition::WidestAxisQuotaV1,
            50,
            groups,
            UnitCentroidPages::decode(&blob).unwrap(),
        )
        .unwrap();
        let r = open(&radial, &input);
        let s = open(&spatial, &input);
        for router in [&r, &s] {
            assert_eq!(router.manifest().leaves.len(), 50);
            assert_eq!(
                router.leaf_units.iter().copied().collect::<BTreeSet<_>>(),
                (0..3200_u32).collect()
            );
            assert!(
                router
                    .manifest()
                    .leaves
                    .iter()
                    .all(|leaf| leaf.unit_count == 64)
            );
            assert_eq!(router.manifest().input_centroids_sha256, digest);
        }
        assert_eq!(r.manifest().leaves[0].prototype, [0., 0.]);
        assert_eq!(r.manifest().leaves[1].prototype, [0., 0.]);
        assert_eq!(s.manifest().leaves[0].prototype, [-1., 0.]);
        assert_eq!(s.manifest().leaves[1].prototype, [1., 0.]);
        let r_ids = r.nominate(&[1., 0.]).unwrap();
        let s_ids = s.nominate(&[1., 0.]).unwrap();
        assert_eq!(r_ids, (2..50).collect::<Vec<_>>());
        assert_eq!(s_ids, (1..49).collect::<Vec<_>>());
        let radial_selection = r
            .validate_selected(&r_ids, &parts(&r, &radial.leaves, &r_ids))
            .unwrap();
        let spatial_selection = s
            .validate_selected(&s_ids, &parts(&s, &spatial.leaves, &s_ids))
            .unwrap();
        assert_eq!(r.nominate_selected(&r_ids).unwrap(), radial_selection);
        assert_eq!(s.nominate_selected(&s_ids).unwrap(), spatial_selection);
        // Closure grows 384->392 pages (16 gained/8 lost); equal units do not imply equal fetch work.
        assert_eq!(radial_selection.page_closure, (16..400).collect());
        assert_eq!(spatial_selection.page_closure, (0..392).collect());
        assert_eq!(
            spatial_selection
                .page_closure
                .difference(&radial_selection.page_closure)
                .copied()
                .collect::<Vec<_>>(),
            (0..16).collect::<Vec<_>>()
        );
        assert_eq!(
            radial_selection
                .page_closure
                .difference(&spatial_selection.page_closure)
                .copied()
                .collect::<Vec<_>>(),
            (392..400).collect::<Vec<_>>()
        );
        assert_eq!(
            spatial_selection
                .units
                .difference(&radial_selection.units)
                .copied()
                .collect::<Vec<_>>(),
            (0..128).step_by(2).collect::<Vec<_>>()
        );
        assert_eq!(
            radial_selection
                .units
                .difference(&spatial_selection.units)
                .copied()
                .collect::<Vec<_>>(),
            (3136..3200).collect::<Vec<_>>()
        );
    }

    #[test]
    fn spatial_full_builder_roundtrip_dimensions_partial_and_determinism() {
        for dimensions in [1, 2, 1024] {
            for rows in [1_usize, 33, 4097] {
                let vectors = (0..rows.div_ceil(32))
                    .map(|id| {
                        let mut vector = vec![0.; dimensions];
                        if dimensions == 2 {
                            vector[0] = id as f32;
                        }
                        vector
                    })
                    .collect::<Vec<_>>();
                let blob = spatial_blob(rows, &vectors);
                let digest = hash(&blob);
                let input = SourceIdentity {
                    profile: SemanticProfile::Scale1m,
                    dimensions,
                    centroids_sha256: &digest,
                    ..source(&blob)
                };
                let a = build_with_partition(
                    &blob,
                    &input,
                    input.profile.allocation_cap(),
                    LeafPartition::WidestAxisQuotaV1,
                )
                .unwrap();
                let b = build_with_partition(
                    &blob,
                    &input,
                    input.profile.allocation_cap(),
                    LeafPartition::WidestAxisQuotaV1,
                )
                .unwrap();
                assert_eq!(a.manifest, b.manifest);
                assert_eq!(a.membership, b.membership);
                assert_eq!(a.leaves, b.leaves);
                let router = open(&a, &input);
                assert_eq!(
                    router.manifest().partition,
                    LeafPartition::WidestAxisQuotaV1
                );
                assert_eq!(router.manifest().final_unit_rows, 1);
                let ids = router.nominate(&vec![1.; dimensions]).unwrap();
                let nomination = router
                    .validate_selected(&ids, &parts(&router, &a.leaves, &ids))
                    .unwrap();
                assert_eq!(router.nominate_selected(&ids).unwrap(), nomination);
                assert_eq!(nomination.units.len(), rows.div_ceil(32));
                assert_eq!(nomination.page_closure, (0..rows.div_ceil(256)).collect());
            }
        }
        let blob = spatial_blob(1, &[vec![0.; 1025]]);
        let digest = hash(&blob);
        let input = SourceIdentity {
            dimensions: 1025,
            centroids_sha256: &digest,
            ..source(&blob)
        };
        assert!(
            build_with_partition(
                &blob,
                &input,
                ALLOCATION_CAP,
                LeafPartition::WidestAxisQuotaV1
            )
            .is_err()
        );
        for value in [f32::INFINITY, f32::NAN] {
            let blob = spatial_blob(1, &[vec![value, 0.]]);
            let digest = hash(&blob);
            let input = SourceIdentity {
                centroids_sha256: &digest,
                ..source(&blob)
            };
            assert!(
                build_with_partition(
                    &blob,
                    &input,
                    ALLOCATION_CAP,
                    LeafPartition::WidestAxisQuotaV1
                )
                .is_err()
            );
        }
    }

    #[test]
    fn spatial_axis1023_top48_gains_and_displaced_losses() {
        // Fixed assignments isolate partitioning; the separate public-build fixture tests training.
        let vectors = (0..3200)
            .map(|id| {
                let mut row = vec![0.; 1024];
                row[1023] = if id < 128 {
                    if id % 2 == 0 { 1. } else { -1. }
                } else {
                    0.5
                };
                row
            })
            .collect::<Vec<_>>();
        let blob = spatial_blob(3200 * 32, &vectors);
        let digest = hash(&blob);
        let input = SourceIdentity {
            profile: SemanticProfile::Scale1m,
            dimensions: 1024,
            centroids_sha256: &digest,
            ..source(&blob)
        };
        let groups = || {
            let mut groups = vec![(0..128).map(|id| (1., id)).collect::<Vec<_>>()];
            groups.extend((0..48).map(|g| {
                (128 + g * 64..128 + (g + 1) * 64)
                    .map(|id| (0.25, id))
                    .collect()
            }));
            groups
        };
        let radial = build_assigned(
            &blob,
            &input,
            input.profile.allocation_cap(),
            LeafPartition::Radial64V1,
            50,
            groups(),
            UnitCentroidPages::decode(&blob).unwrap(),
        )
        .unwrap();
        let spatial = build_assigned(
            &blob,
            &input,
            input.profile.allocation_cap(),
            LeafPartition::WidestAxisQuotaV1,
            50,
            groups(),
            UnitCentroidPages::decode(&blob).unwrap(),
        )
        .unwrap();
        for a in [&radial, &spatial] {
            validate_publication(&a.manifest, &a.membership, &a.leaves, &input, &blob).unwrap();
        }
        let r = open(&radial, &input);
        let s = open(&spatial, &input);
        for unit in 0..3200 {
            assert_eq!(
                r.manifest().leaves[r.unit_membership()[unit]].group_ordinal,
                s.manifest().leaves[s.unit_membership()[unit]].group_ordinal
            );
        }
        let mut query = vec![0.; 1024];
        query[1023] = 1.;
        let rank = |router: &SemanticUnitRouter| {
            let ranked = router
                .rank_leaves_by_metric(&query, &VectorMetric::SquaredEuclidean)
                .unwrap();
            router.nominate_selected(&ranked[..48]).unwrap()
        };
        let radial = rank(&r);
        let spatial = rank(&s);
        assert_eq!(radial.leaf_ids, (2..50).collect::<Vec<_>>());
        assert_eq!(spatial.leaf_ids, (1..49).collect::<Vec<_>>());
        assert_eq!(
            spatial
                .units
                .difference(&radial.units)
                .copied()
                .collect::<Vec<_>>(),
            (0..128).step_by(2).collect::<Vec<_>>()
        );
        assert_eq!(
            radial
                .units
                .difference(&spatial.units)
                .copied()
                .collect::<Vec<_>>(),
            (3136..3200).collect::<Vec<_>>()
        );
        assert_eq!(
            spatial
                .page_closure
                .difference(&radial.page_closure)
                .count(),
            16
        );
        assert_eq!(
            radial
                .page_closure
                .difference(&spatial.page_closure)
                .count(),
            8
        );
        let gain = 0;
        let displaced = 3136 * 32;
        assert!(
            !radial.page_closure.contains(&(gain / 256))
                && spatial.page_closure.contains(&(gain / 256))
        );
        assert!(
            radial.page_closure.contains(&(displaced / 256))
                && !spatial.page_closure.contains(&(displaced / 256))
        );
    }

    #[test]
    fn spatial_version_mode_algorithm_and_balance_refusals() {
        let blob = blob(129 * 32);
        let digest = hash(&blob);
        let input = SourceIdentity {
            centroids_sha256: &digest,
            ..source(&blob)
        };
        let a = build_with_partition(
            &blob,
            &input,
            ALLOCATION_CAP,
            LeafPartition::WidestAxisQuotaV1,
        )
        .unwrap();
        let manifest = open(&a, &input).manifest().clone();
        for code in [0, 3, u32::MAX] {
            let mut body = a.manifest.clone();
            body[88..92].copy_from_slice(&code.to_le_bytes());
            assert!(
                SemanticUnitRouter::open(&body, &a.membership, &hash(&body), &input, 1 << 20)
                    .is_err()
            );
        }
        let mut body = a.manifest.clone();
        body[..8].copy_from_slice(b"BORSUSR2");
        assert!(
            SemanticUnitRouter::open(&body, &a.membership, &hash(&body), &input, 1 << 20).is_err()
        );
        for offset in [92, 127] {
            let mut body = a.manifest.clone();
            body[offset] = 1;
            assert!(
                SemanticUnitRouter::open(&body, &a.membership, &hash(&body), &input, 1 << 20)
                    .is_err()
            );
        }
        let mut bad = manifest.clone();
        bad.partition = LeafPartition::Radial64V1;
        assert!(bad.encode().is_err());
        bad.algorithm = fixed_algorithm(3, 1, bad.partition);
        bad.modeled_peak_allocation_bytes = admit(
            preflight(&blob, input.rows, input.dimensions, input.profile).unwrap(),
            bad.modeled_allocation_limit_bytes,
            input.profile,
        )
        .unwrap();
        let body = bad.encode().unwrap();
        let error = SemanticUnitRouter::open(&body, &a.membership, &hash(&body), &input, 1 << 20)
            .err()
            .unwrap();
        assert!(error.to_string().contains("binary leaf geometry/reserved"));
        for field in 0..11 {
            let mut bad = manifest.clone();
            match field {
                0 => bad.algorithm.trainer.push('x'),
                1 => bad.algorithm.metric.push('x'),
                2 => bad.algorithm.iterations += 1,
                3 => bad.algorithm.requested_centers += 1,
                4 => bad.algorithm.training_centers = 2,
                5 => bad.algorithm.max_leaf_units -= 1,
                6 => bad.algorithm.nearest_ties.push('x'),
                7 => bad.algorithm.group_sort.push('x'),
                8 => bad.algorithm.root_prototype.push('x'),
                9 => bad.algorithm.normalization.push('x'),
                _ => bad.algorithm.payload.push('x'),
            }
            assert!(bad.encode().is_err(), "field={field}");
        }
        let mut bad = manifest;
        let mut offset = 0;
        for (leaf, units) in bad.leaves.iter_mut().zip([44, 42, 43]) {
            leaf.unit_count = units;
            leaf.bytes = units * 8;
            leaf.source_rows = units * 32;
            leaf.offset = offset;
            offset += leaf.bytes;
        }
        let body = bad.encode().unwrap();
        let error = SemanticUnitRouter::open(&body, &a.membership, &hash(&body), &input, 1 << 20)
            .err()
            .unwrap();
        assert!(error.to_string().contains("spatial quota sizes"));
    }

    #[test]
    fn spatial_publication_rejects_rehashed_membership_fp16_and_prototypes() {
        let blob = blob(129 * 32);
        let digest = hash(&blob);
        let input = SourceIdentity {
            centroids_sha256: &digest,
            ..source(&blob)
        };
        let a = build_with_partition(
            &blob,
            &input,
            ALLOCATION_CAP,
            LeafPartition::WidestAxisQuotaV1,
        )
        .unwrap();
        let manifest = open(&a, &input).manifest().clone();
        let mut bad = manifest.clone();
        let mut membership = a.membership.clone();
        let mut payload = a.leaves.clone();
        // Equal prototypes/FP16 survive this cross-leaf swap; spatial replay must reject it.
        let next = bad.leaves[1].offset;
        payload[..4].copy_from_slice(&43_u32.to_le_bytes());
        payload[next..next + 4].copy_from_slice(&0_u32.to_le_bytes());
        membership[..4].copy_from_slice(&1_u32.to_le_bytes());
        membership[43 * 4..44 * 4].copy_from_slice(&0_u32.to_le_bytes());
        bad.membership.sha256 = hash(&membership);
        bad.leaf_payload.sha256 = hash(&payload);
        for leaf in &mut bad.leaves {
            leaf.sha256 = hash(&payload[leaf.offset..leaf.offset + leaf.bytes]);
        }
        let body = bad.encode().unwrap();
        SemanticUnitRouter::open(&body, &membership, &hash(&body), &input, 1 << 20).unwrap();
        let error = validate_publication(&body, &membership, &payload, &input, &blob)
            .err()
            .unwrap();
        assert!(
            error
                .to_string()
                .contains("spatial partition replay membership")
        );
        let mut bad = manifest.clone();
        let mut payload = a.leaves.clone();
        payload[5] = 0x80; // Signed-zero payload alteration leaves the numeric prototype unchanged.
        bad.leaves[0].sha256 = hash(&payload[..bad.leaves[0].bytes]);
        bad.leaf_payload.sha256 = hash(&payload);
        let body = bad.encode().unwrap();
        assert!(validate_publication(&body, &a.membership, &payload, &input, &blob).is_err());
        let mut bad = manifest;
        bad.leaves[0].prototype[0] = -0.;
        let body = bad.encode().unwrap();
        assert!(validate_publication(&body, &a.membership, &a.leaves, &input, &blob).is_err());
    }

    #[test]
    fn spatial_allocation_admission_accounts_for_replay() {
        let geometry = Geometry {
            rows: 129 * 32,
            dimensions: 2,
            units: 129,
            blob_bytes: 32 + 129 * 4,
        };
        let profile = SemanticProfile::Scale1m;
        let radial = admit(geometry, profile.allocation_cap(), profile).unwrap();
        let spatial = admit_with_partition(
            geometry,
            profile.allocation_cap(),
            profile,
            LeafPartition::WidestAxisQuotaV1,
        )
        .unwrap();
        assert_eq!(spatial, radial + 3 * 128);
        let radial_phases =
            allocation_phases(geometry, profile, LeafPartition::Radial64V1).unwrap();
        let spatial_phases =
            allocation_phases(geometry, profile, LeafPartition::WidestAxisQuotaV1).unwrap();
        assert_eq!(spatial_phases[0], radial_phases[0]);
        assert_eq!(spatial_phases[1] - radial_phases[1], 3 * 128);
        assert_eq!(
            spatial_phases[2] - radial_phases[2],
            129 * size_of::<(f64, usize)>() + 3 * 128
        );
        assert!(
            admit_with_partition(
                geometry,
                spatial - 1,
                profile,
                LeafPartition::WidestAxisQuotaV1
            )
            .is_err()
        );
        assert_eq!(
            admit_with_partition(geometry, spatial, profile, LeafPartition::WidestAxisQuotaV1)
                .unwrap(),
            spatial
        );
        let blob = blob(geometry.rows);
        let digest = hash(&blob);
        let input = SourceIdentity {
            profile,
            centroids_sha256: &digest,
            ..source(&blob)
        };
        let artifacts = build_with_partition(
            &blob,
            &input,
            profile.allocation_cap(),
            LeafPartition::WidestAxisQuotaV1,
        )
        .unwrap();
        let root_sha = hash(&artifacts.manifest);
        assert!(
            admit_authenticated_root(
                &artifacts.manifest,
                &root_sha,
                &input,
                radial + (spatial - radial) / 2
            )
            .is_err()
        );
        assert_eq!(
            admit_authenticated_root(&artifacts.manifest, &root_sha, &input, spatial).unwrap(),
            (LeafPartition::WidestAxisQuotaV1, spatial)
        );
        let mut tampered = artifacts.manifest.clone();
        tampered[88..92].copy_from_slice(&1_u32.to_le_bytes());
        assert!(admit_authenticated_root(&tampered, &root_sha, &input, spatial).is_err());
    }

    #[test]
    fn sampled_boundary_midpoints_decode_and_dimension_contract() {
        for (rows, expected) in [
            (1_usize, vec![0]),
            (32, vec![16]),
            (33, vec![16, 32]),
            (48, vec![16, 47]),
            (65, vec![16, 48, 64]),
        ] {
            assert_eq!(
                (0..rows.div_ceil(32))
                    .map(|u| sampled_boundary_position(rows, u).unwrap())
                    .collect::<Vec<_>>(),
                expected
            );
            assert!(sampled_boundary_position(rows, rows.div_ceil(32)).is_err());
        }
        assert!(sampled_boundary_position(0, 0).is_err());
        assert!(sampled_boundary_position(usize::MAX, 0).is_err());
        assert_eq!(
            decode_sampled_boundary_row(&[2], &[-3.], &[1.]).unwrap(),
            [-1.]
        );
        let row = decode_sampled_boundary_row(&[3, 4], &[0., 0.], &[1., 1.]).unwrap();
        assert_eq!(row, [0.6, 0.8]);
        let row =
            decode_sampled_boundary_row(&vec![1; 1024], &vec![0.; 1024], &vec![1.; 1024]).unwrap();
        assert_eq!(row, vec![1. / 32.; 1024]);
        assert!(
            decode_sampled_boundary_row(&vec![1; 1025], &vec![0.; 1025], &vec![1.; 1025]).is_err()
        );
        for (codes, low, step) in [
            (&[0][..], 0., 1.),
            (&[1], f32::NAN, 1.),
            (&[1], 0., 0.),
            (&[1], 0., -1.),
            (&[255], 0., f32::MAX),
        ] {
            assert!(decode_sampled_boundary_row(codes, &[low], &[step]).is_err());
        }
        assert!(decode_sampled_boundary_row(&[1, 2], &[1.], &[1.]).is_err());
    }

    #[test]
    fn sampled_boundary_phase_admission_and_checked_votes() {
        let model = sampled_boundary_allocation(
            1_000_000,
            1024,
            977,
            112 * 1024 * 1024,
            164_435_968,
            512 * 1024 * 1024,
        )
        .unwrap();
        assert_eq!(
            model.samples,
            128_000_000 + 31_250 * std::mem::size_of::<Vec<f32>>()
        );
        assert_eq!(model.edge_pairs, 31_250 * 32 * 8);
        assert_eq!(model.peak, model.construction_phase.max(model.replay_phase));
        assert!(model.peak > 256 * 1024 * 1024);
        assert!(
            sampled_boundary_allocation(
                1_000_000,
                1024,
                977,
                112 * 1024 * 1024,
                164_435_968,
                model.peak - 1
            )
            .is_err()
        );
        for (rows, dimensions, metadata) in [(usize::MAX, 1, 0), (32, 1025, 0), (32, 1, usize::MAX)]
        {
            assert!(
                sampled_boundary_allocation(rows, dimensions, 1, metadata, 0, usize::MAX).is_err()
            );
        }
        assert_eq!(boundary_vote(3, 2, 40).unwrap(), 83);
        assert!(boundary_vote(u64::MAX, 1, 1).is_err());
        assert!(boundary_vote(0, u64::MAX, 40).is_err());
    }

    #[test]
    fn sampled_boundary_counts_directed_base_only_and_top16_ties() {
        // Physical membership is deliberately nonidentity. Upper arcs differ from base arcs.
        let graph = vec![
            vec![vec![4], vec![2, 3]],
            vec![vec![0]],
            vec![vec![1]],
            vec![vec![]],
            vec![vec![0]],
            vec![vec![0, 1, 3]], // The same-leaf5->0 arc must contribute nothing.
        ];
        let a = SampledBoundaryAdjacency::project(&graph, &[2, 0, 1, 0, 1, 2], 3).unwrap();
        let links = |leaf| &a.links[a.offsets[leaf]..a.offsets[leaf + 1]];
        assert_eq!(links(0), [SampledBoundaryLink { leaf: 2, count: 1 }]);
        assert_eq!(
            links(1),
            [
                SampledBoundaryLink { leaf: 0, count: 1 },
                SampledBoundaryLink { leaf: 2, count: 1 }
            ]
        );
        assert_eq!(
            links(2),
            [
                SampledBoundaryLink { leaf: 0, count: 3 },
                SampledBoundaryLink { leaf: 1, count: 1 }
            ]
        );
        assert_eq!(a.directed_cross_leaf_arcs, 7);
        let mut graph = vec![vec![vec![]]; 19];
        graph[0][0] = (1..19).collect();
        let a =
            SampledBoundaryAdjacency::project(&graph, &(0..19).collect::<Vec<_>>(), 19).unwrap();
        assert_eq!(
            a.links.iter().map(|l| l.leaf).collect::<Vec<_>>(),
            (1..=16).collect::<Vec<_>>()
        );
        assert_eq!(a.directed_cross_leaf_arcs, 18);
        assert_eq!(a.offsets[1], 16);
    }

    #[test]
    fn sampled_boundary_votes_l2_ties_exclusions_fallback_and_exact48() {
        let mut graph = vec![vec![vec![]]; 64];
        graph[0][0] = vec![50, 51, 2]; // seed2 must never be a replacement.
        graph[1][0] = vec![52];
        graph[39][0] = vec![52]; // 39+1 ties the two40 votes.
        let a =
            SampledBoundaryAdjacency::project(&graph, &(0..64).collect::<Vec<_>>(), 64).unwrap();
        let mut l2 = (0..64).collect::<Vec<_>>();
        l2.swap(50, 51);
        let selected = a.select(&l2).unwrap();
        let expected = (0..40)
            .chain([51, 50, 52, 40, 41, 42, 43, 44])
            .collect::<Vec<_>>();
        assert_eq!(selected, expected);
        assert_eq!(selected.iter().collect::<BTreeSet<_>>().len(), 48);
        // Independent direct sum oracle for the projected one-row-per-leaf graph.
        let mut votes = [0_u64; 64];
        for seed in 0..40 {
            for &dest in &graph[seed][0] {
                votes[dest as usize] += (40 - seed) as u64;
            }
        }
        assert_eq!((votes[50], votes[51], votes[52]), (40, 40, 40));
        let empty = SampledBoundaryAdjacency::project(
            &vec![vec![vec![]]; 64],
            &(0..64).collect::<Vec<_>>(),
            64,
        )
        .unwrap();
        assert_eq!(empty.select(&l2).unwrap(), l2[..48]);
        l2[63] = l2[0];
        assert!(empty.select(&l2).is_err());
        let singleton =
            SampledBoundaryAdjacency::build(&[vec![1.]], &[0], 1, 32 * 1024 * 1024).unwrap();
        assert_eq!(singleton.select(&[0]).unwrap(), [0]);
    }

    #[test]
    fn sampled_boundary_equal_distance_graph_is_repeatable_and_bounded() {
        let samples = vec![vec![1., 0.]; 64];
        let membership = (0..64).map(|u| u % 3).collect::<Vec<_>>();
        let build =
            || SampledBoundaryAdjacency::build(&samples, &membership, 3, 32 * 1024 * 1024).unwrap();
        assert_eq!(build(), build());
        assert!(SampledBoundaryAdjacency::build(&samples, &membership, 3, 1).is_err());
        assert!(
            SampledBoundaryAdjacency::build(&[vec![0., 0.]], &[0], 1, 32 * 1024 * 1024).is_err()
        );
        assert!(SampledBoundaryAdjacency::build(&[vec![1.]], &[1], 1, 32 * 1024 * 1024).is_err());
    }

    #[test]
    fn cosine_leaf_metric_removes_mean_length_bias_at_fixed_48() {
        let mut prototypes = vec![vec![1.0, 0.5]; 50];
        prototypes[0] = vec![0.125, 0.0];
        prototypes[49] = vec![-1.0, 0.0];
        let baseline = rank_leaves(
            &[1.0, 0.0],
            prototypes.iter().map(Vec::as_slice),
            SemanticProfile::Scale1m,
        )
        .unwrap();
        assert_eq!(baseline, (1..49).collect::<Vec<_>>());
        let selected = rank_prototypes(
            &[1.0, 0.0],
            prototypes.iter().map(Vec::as_slice),
            &VectorMetric::Cosine,
        )
        .unwrap()
        .into_iter()
        .take(SemanticProfile::Scale1m.selected_leaf_limit())
        .map(|(_, id)| id)
        .collect::<Vec<_>>();
        assert_eq!(selected, (0..48).collect::<Vec<_>>());
    }

    #[test]
    fn cosine_leaf_metric_zero_negative_ties_and_finite_extremes() {
        let prototypes = [[0.0, -0.0], [-1.0, 0.0], [1.0, 0.0], [2.0, 0.0], [0.0, 1.0]];
        for scale in [1.0, 8.0, f32::MAX, f32::from_bits(1)] {
            let ranked = rank_prototypes(
                &[scale, 0.0],
                prototypes.iter().map(|p| p.as_slice()),
                &VectorMetric::Cosine,
            )
            .unwrap();
            assert_eq!(
                ranked.iter().map(|&(_, id)| id).collect::<Vec<_>>(),
                vec![2, 3, 0, 4, 1]
            );
            assert_eq!(
                ranked.iter().map(|&(d, _)| d).collect::<Vec<_>>(),
                vec![0.0, 0.0, 1.0, 1.0, 2.0]
            );
        }
        for scale in [f32::MAX, f32::from_bits(1)] {
            let prototypes = [[scale, 0.0], [0.0, scale], [-scale, 0.0]];
            let ranked = rank_prototypes(
                &[scale, 0.0],
                prototypes.iter().map(|p| p.as_slice()),
                &VectorMetric::Cosine,
            )
            .unwrap();
            assert_eq!(ranked, vec![(0.0, 0), (1.0, 1), (2.0, 2)]);
        }
    }

    #[test]
    fn cosine_leaf_metric_refuses_invalid_geometry_and_unsupported_metrics() {
        let prototypes = [[1.0, 0.0]];
        for query in [
            vec![],
            vec![0.0, 0.0],
            vec![f32::NAN, 1.0],
            vec![f32::INFINITY, 0.0],
            vec![1.0],
        ] {
            assert!(
                rank_prototypes(
                    &query,
                    prototypes.iter().map(|p| p.as_slice()),
                    &VectorMetric::Cosine
                )
                .is_err()
            );
        }
        for prototype in [
            vec![],
            vec![1.0],
            vec![f32::NAN, 1.0],
            vec![f32::INFINITY, 1.0],
        ] {
            assert!(
                rank_prototypes(
                    &[1.0, 0.0],
                    std::iter::once(prototype.as_slice()),
                    &VectorMetric::Cosine
                )
                .is_err()
            );
        }
        assert!(rank_prototypes(&[1.0, 0.0], std::iter::empty(), &VectorMetric::Cosine).is_err());
        for metric in [
            VectorMetric::Euclidean,
            VectorMetric::InnerProduct,
            VectorMetric::Angular,
        ] {
            assert!(
                rank_prototypes(
                    &[1.0, 0.0],
                    prototypes.iter().map(|p| p.as_slice()),
                    &metric
                )
                .is_err()
            );
        }
        // Preserve the historical L2 zero-query admission and exact score.
        assert_eq!(
            rank_prototypes(
                &[0.0, 0.0],
                prototypes.iter().map(|p| p.as_slice()),
                &VectorMetric::SquaredEuclidean
            )
            .unwrap(),
            vec![(1.0, 0)]
        );
    }

    #[test]
    fn cosine_leaf_metric_authenticated_router_keeps_default_nomination() {
        let blob = blob(100_000);
        let digest = hash(&blob);
        let input = SourceIdentity {
            centroids_sha256: &digest,
            ..source(&blob)
        };
        let artifacts = build(&blob, &input, ALLOCATION_CAP).unwrap();
        let router = open(&artifacts, &input);
        assert_eq!(router.manifest().leaves.len(), 49);
        for metric in [VectorMetric::SquaredEuclidean, VectorMetric::Cosine] {
            assert_eq!(
                router.rank_leaves_by_metric(&[3.0, 4.0], &metric).unwrap(),
                (0..49).collect::<Vec<_>>()
            );
        }
        let selected = router.nominate(&[3.0, 4.0]).unwrap();
        assert_eq!(selected, (0..16).collect::<Vec<_>>());
        assert!(
            router
                .nominate_selected(&(0..49).collect::<Vec<_>>())
                .is_err()
        );
        assert_eq!(router.manifest().encode().unwrap(), artifacts.manifest);

        // Authenticate a nondegenerate root whose metric orders differ.
        let mut manifest = router.manifest().clone();
        manifest.leaves[0].prototype = vec![0.125, 0.0];
        for leaf in &mut manifest.leaves[1..] {
            leaf.prototype = vec![1.0, 0.5];
        }
        let mut angular_artifacts = artifacts;
        angular_artifacts.manifest = manifest.encode().unwrap();
        let angular_router = open(&angular_artifacts, &input);
        let l2 = angular_router
            .rank_leaves_by_metric(&[1.0, 0.0], &VectorMetric::SquaredEuclidean)
            .unwrap();
        let cosine = angular_router
            .rank_leaves_by_metric(&[1.0, 0.0], &VectorMetric::Cosine)
            .unwrap();
        assert_eq!(l2, (1..49).chain([0]).collect::<Vec<_>>());
        assert_eq!(cosine, (0..49).collect::<Vec<_>>());
        let nominated = angular_router.nominate_selected(&cosine[..16]).unwrap();
        assert_eq!(nominated.leaf_ids, cosine[..16]);
    }

    #[test]
    fn scale_nomination_preserves_scattered_closure_and_fresh_limits() {
        for pages in [513, 48 * LEAF_UNITS] {
            let units = (0..pages).map(|page| page * 8 + 1).collect::<BTreeSet<_>>();
            let (seed, walk, additions) =
                seed_walk(&units, 1_000_000, SemanticProfile::Scale1m).unwrap();
            assert_eq!(seed, 0);
            assert_eq!(additions, vec![0, 2, 3, 4, 5, 6, 7]);
            assert_eq!(walk.len(), pages + 7);
            assert!(units.iter().all(|unit| walk.binary_search(unit).is_ok()));
            assert_eq!(
                walk.iter()
                    .map(|unit| unit / 8)
                    .collect::<BTreeSet<_>>()
                    .len(),
                pages
            );
            assert!(seed_walk(&units, 1_000_000, SemanticProfile::Fresh1m).is_err());
        }
        let excess = (0..=48 * LEAF_UNITS).collect::<BTreeSet<_>>();
        assert!(seed_walk(&excess, 1_000_000, SemanticProfile::Scale1m).is_err());
        assert_eq!(SemanticProfile::Fresh1m.closure_page_limit(), 512);
        assert_eq!(SemanticProfile::Fresh1m.source_unit_limit(), 4096);
    }

    #[test]
    fn scale_profile_dimension_population_leaf_and_million_row_arithmetic_bounds() {
        let profile = SemanticProfile::Scale1m;
        assert_eq!(
            profile.allocation_cap(),
            SemanticProfile::Fresh1m.allocation_cap()
        );
        assert_eq!(profile.root_cap(), SemanticProfile::Fresh1m.root_cap());
        for dimensions in [1, 3, 255, 257, 768, 1024] {
            for rows in [1, 31, 32, 33, 100_001, 999_999, 1_000_000] {
                assert!(profile.valid_geometry(rows, dimensions));
            }
            assert_eq!(
                profile.selected_leaf_bytes(dimensions).unwrap(),
                48 * 64 * (4 + dimensions * 2)
            );
            let prototypes = vec![vec![0.; dimensions]; 3];
            assert_eq!(
                rank_leaves(
                    &vec![1.; dimensions],
                    prototypes.iter().map(Vec::as_slice),
                    profile
                )
                .unwrap(),
                [0, 1, 2]
            );
        }
        for (rows, dimensions) in [
            (0, 1),
            (1_000_001, 1),
            (1, 0),
            (1, 1025),
            (usize::MAX, usize::MAX),
        ] {
            assert!(!profile.valid_geometry(rows, dimensions));
        }
        assert!(profile.selected_leaf_bytes(1025).is_err());
        let geometry = Geometry {
            rows: 1_000_000,
            dimensions: 1024,
            units: 31250,
            blob_bytes: 64_000_032,
        };
        let peak = admit(geometry, profile.allocation_cap(), profile).unwrap();
        assert_eq!(peak, 506_729_928);
        assert!(admit(geometry, peak - 1, profile).is_err());
        assert_eq!(512 + (2 * 489 - 1) * (64 + 4 * 1024), 4_064_832);
        assert!(4_064_832 <= profile.root_cap());
        assert_eq!(profile.selected_leaf_bytes(1024).unwrap(), 6_303_744);
        assert!(
            admit(
                Geometry {
                    dimensions: 1025,
                    ..geometry
                },
                profile.allocation_cap(),
                profile
            )
            .is_err()
        );
    }

    #[test]
    fn scale_profile_build_and_nomination_support_singleton_and_partial_pages() {
        for rows in [1, 31, 33, 257] {
            let blob = blob(rows);
            let digest = hash(&blob);
            let input = SourceIdentity {
                profile: SemanticProfile::Scale1m,
                centroids_sha256: &digest,
                ..source(&blob)
            };
            let artifacts = build(&blob, &input, input.profile.allocation_cap()).unwrap();
            validate_publication(
                &artifacts.manifest,
                &artifacts.membership,
                &artifacts.leaves,
                &input,
                &blob,
            )
            .unwrap();
            let router = open(&artifacts, &input);
            let ids = router.nominate(&[1., 0.]).unwrap();
            assert_eq!(ids, [0]);
            let nomination = router
                .validate_selected(&ids, &parts(&router, &artifacts.leaves, &ids))
                .unwrap();
            assert_eq!(nomination.units, (0..rows.div_ceil(32)).collect());
            assert_eq!(nomination.page_closure, (0..rows.div_ceil(256)).collect());
            assert_eq!(router.manifest().profile, SemanticProfile::Scale1m);
        }
    }

    fn blob(rows: usize) -> Vec<u8> {
        let mut bytes = b"BORSUCP1".to_vec();
        bytes.extend_from_slice(&(rows as u64).to_le_bytes());
        for word in [2_u32, 32, 256, 0] {
            bytes.extend_from_slice(&word.to_le_bytes());
        }
        bytes.resize(32 + rows.div_ceil(32) * 4, 0);
        bytes
    }
    fn source(blob: &[u8]) -> SourceIdentity<'static> {
        SourceIdentity {
            profile: SemanticProfile::Native100k,
            schema: "native-generation-test",
            root_sha256: "1111111111111111111111111111111111111111111111111111111111111111",
            centroids_sha256: "",
            rows: u64::from_le_bytes(blob[8..16].try_into().unwrap()) as usize,
            dimensions: 2,
        }
    }
    fn open(a: &RouterArtifacts, source: &SourceIdentity<'_>) -> SemanticUnitRouter {
        SemanticUnitRouter::open(
            &a.manifest,
            &a.membership,
            &hash(&a.manifest),
            source,
            1 << 20,
        )
        .unwrap()
    }
    fn parts<'a>(router: &SemanticUnitRouter, payload: &'a [u8], ids: &[usize]) -> Vec<&'a [u8]> {
        ids.iter()
            .map(|&id| {
                let leaf = &router.manifest().leaves[id];
                &payload[leaf.offset..leaf.offset + leaf.bytes]
            })
            .collect()
    }

    #[test]
    fn membership_directory_parity_and_capacity() {
        assert_eq!(
            directory_allocation_bytes(3125, 76).unwrap(),
            (13116, 14332)
        );
        for (units, leaves) in [(0, 1), (1, 0), (1, 2), (usize::MAX, 1), (1, usize::MAX)] {
            assert!(directory_allocation_bytes(units, leaves).is_err());
        }
        for rows in [1, 33, 257, 4097] {
            let mut blob = blob(rows);
            for unit in 0..rows.div_ceil(32) {
                for (d, value) in [(unit * 73 % 251) as f32, (unit * 137 % 251) as f32]
                    .into_iter()
                    .enumerate()
                {
                    blob[32 + unit * 4 + d * 2..34 + unit * 4 + d * 2]
                        .copy_from_slice(&half::f16::from_f32(value).to_bits().to_le_bytes());
                }
            }
            let digest = hash(&blob);
            let source = SourceIdentity {
                centroids_sha256: &digest,
                ..source(&blob)
            };
            let artifacts = build(&blob, &source, ALLOCATION_CAP).unwrap();
            let router = open(&artifacts, &source);
            let (persistent, peak) = directory_allocation_bytes(
                router.manifest.unit_count,
                router.manifest.leaves.len(),
            )
            .unwrap();
            assert_eq!(
                persistent,
                router.leaf_units.capacity() * 4 + router.leaf_offsets.capacity() * 8
            );
            assert_eq!(peak, persistent + 16 * router.manifest.leaves.len());
            let mut scattered = false;
            for leaf in &router.manifest.leaves {
                let units = &router.leaf_units
                    [router.leaf_offsets[leaf.leaf_id]..router.leaf_offsets[leaf.leaf_id + 1]];
                scattered |= units.windows(2).any(|pair| pair[1] > pair[0] + 1);
                let ids = [leaf.leaf_id];
                assert_eq!(
                    router.nominate_selected(&ids).unwrap(),
                    router
                        .validate_selected(&ids, &parts(&router, &artifacts.leaves, &ids))
                        .unwrap()
                );
            }
            if rows == 4097 {
                assert!(scattered);
            }
            let ids = router.nominate(&[3., 4.]).unwrap();
            assert_eq!(
                router.nominate_selected(&ids).unwrap(),
                router
                    .validate_selected(&ids, &parts(&router, &artifacts.leaves, &ids))
                    .unwrap()
            );
            for invalid in [
                vec![],
                vec![0, 0],
                vec![router.manifest.leaves.len()],
                vec![0; 17],
            ] {
                assert!(router.nominate_selected(&invalid).is_err());
            }
            // The source, root and membership remain the nomination authority.
            let mut damaged = artifacts.membership.clone();
            damaged[0] ^= 1;
            assert!(
                SemanticUnitRouter::open(
                    &artifacts.manifest,
                    &damaged,
                    &hash(&artifacts.manifest),
                    &source,
                    1 << 20
                )
                .is_err()
            );
            let bad_source = SourceIdentity {
                root_sha256: &"0".repeat(64),
                ..source
            };
            assert!(
                SemanticUnitRouter::open(
                    &artifacts.manifest,
                    &artifacts.membership,
                    &hash(&artifacts.manifest),
                    &bad_source,
                    1 << 20
                )
                .is_err()
            );
            assert!(
                SemanticUnitRouter::open(
                    &artifacts.manifest,
                    &artifacts.membership,
                    &"0".repeat(64),
                    &source,
                    1 << 20
                )
                .is_err()
            );
            assert!(
                SemanticUnitRouter::open(
                    &artifacts.manifest,
                    &artifacts.membership,
                    &hash(&artifacts.manifest),
                    &source,
                    artifacts.manifest.len() - 1
                )
                .is_err()
            );
        }
    }

    #[test]
    fn profile_geometry_phase_caps_and_maximum_binary_size() {
        let geometry = Geometry {
            rows: 1_000_000,
            dimensions: 768,
            units: 31250,
            blob_bytes: 48000032,
        };
        let cap = SemanticProfile::Fresh1m.allocation_cap();
        let peak = admit(geometry, cap, SemanticProfile::Fresh1m).unwrap();
        assert_eq!(peak, 391631304);
        assert_eq!(
            peak + 128 * 768
                + 32 * 1_000_000_usize.div_ceil(256)
                + 256 * (768 + 12)
                + 262144
                + 8_000_000,
            400316456
        );
        assert_eq!(
            admit(geometry, peak, SemanticProfile::Fresh1m).unwrap(),
            peak
        );
        assert!(admit(geometry, peak - 1, SemanticProfile::Fresh1m).is_err());
        assert!(admit(geometry, cap, SemanticProfile::Native100k).is_err());
        for bad in [
            Geometry {
                rows: 999999,
                ..geometry
            },
            Geometry {
                dimensions: 767,
                ..geometry
            },
            Geometry {
                units: usize::MAX,
                ..geometry
            },
            Geometry {
                blob_bytes: usize::MAX,
                ..geometry
            },
        ] {
            assert!(admit(bad, cap, SemanticProfile::Fresh1m).is_err());
        }
        let tiny = blob(1);
        let digest = hash(&tiny);
        let source = SourceIdentity {
            centroids_sha256: &digest,
            ..source(&tiny)
        };
        let artifacts = build(&tiny, &source, ALLOCATION_CAP).unwrap();
        let mut manifest = open(&artifacts, &source).manifest().clone();
        manifest.leaves[0].prototype[0] = -0.0;
        let signed = manifest.encode().unwrap();
        let signed_router = SemanticUnitRouter::open(
            &signed,
            &artifacts.membership,
            &hash(&signed),
            &source,
            1 << 20,
        )
        .unwrap();
        assert_eq!(
            signed_router.manifest().leaves[0].prototype[0].to_bits(),
            (-0.0_f32).to_bits()
        );

        manifest.profile = SemanticProfile::Fresh1m;
        manifest.dimensions = 768;
        let mut leaf = manifest.leaves[0].clone();
        leaf.prototype = vec![0.; 768];
        manifest.leaves = vec![leaf; 977];
        assert_eq!(manifest.encode().unwrap().len(), 3064384);
        // f32 encoding is bit-exact, including negative zero.
        manifest.leaves[0].prototype[0] = -0.0;
        let body = manifest.encode().unwrap();
        assert_eq!(
            &body[512 + 977 * 64..512 + 977 * 64 + 4],
            &(-0.0_f32).to_bits().to_le_bytes()
        );
    }

    #[test]
    fn frozen_native100k_membership_leaves_and_nomination() {
        let mut blob = blob(4097);
        for unit in 0..129 {
            for (d, value) in [unit as f32, 2.].into_iter().enumerate() {
                blob[32 + unit * 4 + d * 2..34 + unit * 4 + d * 2]
                    .copy_from_slice(&half::f16::from_f32(value).to_bits().to_le_bytes());
            }
        }
        let digest = hash(&blob);
        let source = SourceIdentity {
            centroids_sha256: &digest,
            ..source(&blob)
        };
        let artifacts = build(&blob, &source, ALLOCATION_CAP).unwrap();
        assert_eq!(
            hash(&artifacts.membership),
            "919207b3fc34a3d945d3310331767bec4033cb7873859bad8d43f72a06f510b0"
        );
        assert_eq!(
            hash(&artifacts.leaves),
            "a308a30fec67ef28269b8c7826438dd8dbcff1fe0f692de7f0685c2a34ee8906"
        );
        assert_eq!(&artifacts.manifest[..8], b"BORSUSR3");
        assert_eq!(word32(&artifacts.manifest, 88), 1);
        let explicit =
            build_with_partition(&blob, &source, ALLOCATION_CAP, LeafPartition::Radial64V1)
                .unwrap();
        assert_eq!(artifacts.manifest, explicit.manifest);
        assert_eq!(artifacts.membership, explicit.membership);
        assert_eq!(artifacts.leaves, explicit.leaves);
        let router = open(&artifacts, &source);
        let legacy_prototypes = router
            .manifest()
            .leaves
            .iter()
            .map(|leaf| leaf.prototype.clone())
            .collect::<Vec<_>>();
        for query in [[0., 1.], [1., 0.], [-1., 0.]] {
            assert_eq!(
                router.select_leaves(&query).unwrap(),
                select_leaves(&query, &legacy_prototypes).unwrap()
            );
        }
        let whole = self::blob(100000);
        let digest = hash(&whole);
        let input = SourceIdentity {
            centroids_sha256: &digest,
            ..self::source(&whole)
        };
        let a = build(&whole, &input, ALLOCATION_CAP).unwrap();
        let r = open(&a, &input);
        assert_eq!(r.nominate(&[1., 0.]).unwrap(), (0..16).collect::<Vec<_>>());
        assert_eq!(r.manifest().leaves.len(), 49);
    }

    fn fresh48_units() -> BTreeSet<usize> {
        std::iter::once(1)
            .chain((1..512).flat_map(|page| (0..6).map(move |offset| page * 8 + offset)))
            .chain((1..6).map(|page| page * 8 + 6))
            .collect()
    }

    #[test]
    fn fresh48_seed_completion_retains_all_3072_units_and_512_pages() {
        let units = fresh48_units();
        let (seed, walk, additions) =
            seed_walk(&units, 1_000_000, SemanticProfile::Fresh1m).unwrap();
        assert_eq!(units.len(), 3072);
        assert_eq!(seed, 0);
        assert_eq!(walk.len(), 3079);
        assert_eq!(additions, [0, 2, 3, 4, 5, 6, 7]);
        assert!(units.iter().all(|unit| walk.binary_search(unit).is_ok()));
        assert_eq!(
            walk.iter().map(|u| u / 8).collect::<BTreeSet<_>>(),
            (0..512).collect()
        );
        assert!(seed_walk(&units, 1_000_000, SemanticProfile::Native100k).is_err());
        let mut excess = units.clone();
        excess.insert(15);
        assert!(seed_walk(&excess, 1_000_000, SemanticProfile::Fresh1m).is_err());
        excess = units;
        excess.remove(&8);
        excess.insert(512 * 8);
        assert_eq!(excess.len(), 3072);
        assert!(seed_walk(&excess, 1_000_000, SemanticProfile::Fresh1m).is_err());
    }

    #[test]
    fn fresh_binary_root_selected_leaves_and_scattered_closure_without_training() {
        let geometry = Geometry {
            rows: 1_000_000,
            dimensions: 768,
            units: 31250,
            blob_bytes: 48000032,
        };
        let profile = SemanticProfile::Fresh1m;
        let peak = admit(geometry, profile.allocation_cap(), profile).unwrap();
        let input = SourceIdentity {
            profile,
            schema: "synthetic-scale",
            root_sha256: &"1".repeat(64),
            centroids_sha256: &"2".repeat(64),
            rows: geometry.rows,
            dimensions: geometry.dimensions,
        };
        let selected_units = fresh48_units();
        let order = selected_units
            .iter()
            .copied()
            .chain((0..geometry.units).filter(|u| !selected_units.contains(u)))
            .collect::<Vec<_>>();
        let mut membership = vec![0; geometry.units * 4];
        let mut leaves = Vec::new();
        let mut bodies = Vec::new();
        let mut offset = 0;
        for (id, units) in order.chunks(64).enumerate() {
            let mut body = Vec::new();
            for &unit in units {
                membership[unit * 4..unit * 4 + 4].copy_from_slice(&(id as u32).to_le_bytes());
                if id < 48 {
                    body.extend_from_slice(&(unit as u32).to_le_bytes());
                    body.resize(body.len() + 1536, 0);
                }
            }
            let bytes = units.len() * 1540;
            leaves.push(Leaf {
                leaf_id: id,
                group_ordinal: id,
                chunk_ordinal: 0,
                offset,
                bytes,
                unit_count: units.len(),
                source_rows: units.len() * 32,
                sha256: if id < 48 { hash(&body) } else { "3".repeat(64) },
                prototype: vec![0.; 768],
            });
            offset += bytes;
            if id < 48 {
                bodies.push(body);
            }
        }
        let manifest = Manifest {
            profile,
            partition: LeafPartition::Radial64V1,
            schema: SCHEMA.into(),
            input_schema: input.schema.into(),
            input_root_sha256: input.root_sha256.into(),
            input_centroids_sha256: input.centroids_sha256.into(),
            input_centroids_bytes: geometry.blob_bytes,
            rows: geometry.rows,
            dimensions: geometry.dimensions,
            unit_rows: 32,
            page_rows: 256,
            unit_count: geometry.units,
            final_unit_rows: 32,
            algorithm: fixed_algorithm(489, 489, LeafPartition::Radial64V1),
            modeled_peak_allocation_bytes: peak,
            modeled_allocation_limit_bytes: profile.allocation_cap(),
            allocation_model:
                "conservative allocation capacities; not RSS or an enforced process limit".into(),
            membership: Artifact {
                bytes: membership.len(),
                sha256: hash(&membership),
            },
            leaf_payload: Artifact {
                bytes: offset,
                sha256: "4".repeat(64),
            },
            leaves,
        };
        let root = manifest.encode().unwrap();
        let router =
            SemanticUnitRouter::open(&root, &membership, &hash(&root), &input, profile.root_cap())
                .unwrap();
        let ids = router.nominate(&[1.; 768]).unwrap();
        assert_eq!(ids, (0..48).collect::<Vec<_>>());
        let nomination = router
            .validate_selected(&ids, &bodies.iter().map(Vec::as_slice).collect::<Vec<_>>())
            .unwrap();
        assert_eq!(nomination.units, selected_units);
        assert_eq!(nomination.walk_units.len(), 3079);
        assert_eq!(nomination.page_closure.len(), 512);
        assert_eq!(bodies.iter().map(Vec::len).sum::<usize>(), 4_730_880);
        let leaf_excess = router
            .validate_selected(&[0; 49], &[bodies[0].as_slice(); 49])
            .err()
            .unwrap();
        assert!(leaf_excess.to_string().contains("selected leaf count"));
        let byte_excess = router
            .validate_selected(&[0; 48], &[vec![0; 4_730_881].as_slice(); 48])
            .err()
            .unwrap();
        assert!(byte_excess.to_string().contains("selected leaf byte cap"));
        assert!(
            router
                .validate_selected(
                    &ids[..47],
                    &bodies[..47].iter().map(Vec::as_slice).collect::<Vec<_>>()
                )
                .is_err()
        );
        let expected = (0..48).collect::<Vec<_>>();
        assert_eq!(router.nominate(&[1.; 768]).unwrap(), expected);
        let mut distinct = router.manifest.clone();
        for leaf in &mut distinct.leaves {
            leaf.prototype[0] = leaf.leaf_id as f32;
        }
        let distinct = SemanticUnitRouter {
            manifest: distinct,
            membership: router.membership.clone(),
            leaf_offsets: router.leaf_offsets.clone(),
            leaf_units: router.leaf_units.clone(),
        };
        assert_eq!(distinct.select_leaves(&[0.; 768]).unwrap(), expected);
        let mut overflow = root.clone();
        overflow[64..72].copy_from_slice(&u64::MAX.to_le_bytes());
        assert!(
            SemanticUnitRouter::open(
                &overflow,
                &membership,
                &hash(&overflow),
                &input,
                profile.root_cap()
            )
            .is_err()
        );
        let native = SourceIdentity {
            profile: SemanticProfile::Native100k,
            ..input
        };
        assert!(
            SemanticUnitRouter::open(
                &root,
                &membership,
                &hash(&root),
                &native,
                profile.root_cap()
            )
            .is_err()
        );
    }

    #[test]
    fn schema_marker_is_bounded_before_construction() {
        let blob = blob(1);
        let digest = hash(&blob);
        let marker = "x".repeat(257);
        let source = SourceIdentity {
            schema: &marker,
            centroids_sha256: &digest,
            ..source(&blob)
        };
        assert!(build(&blob, &source, ALLOCATION_CAP).is_err());
    }

    #[test]
    fn selected_validation_is_bounded_and_retains_every_unit_and_partial_tail() {
        let blob = blob(32_769);
        let digest = hash(&blob);
        let source = SourceIdentity {
            centroids_sha256: &digest,
            ..source(&blob)
        };
        let artifacts = build(&blob, &source, ALLOCATION_CAP).unwrap();
        validate_publication(
            &artifacts.manifest,
            &artifacts.membership,
            &artifacts.leaves,
            &source,
            &blob,
        )
        .unwrap();
        let router = open(&artifacts, &source);
        assert_eq!(router.manifest().input_schema, "native-generation-test");
        let ids = router.nominate(&[3.0, 4.0]).unwrap();
        assert_eq!(ids, (0..16).collect::<Vec<_>>());
        assert_eq!(
            ids,
            router
                .select_leaves(&crate::sq8_source::cosine_vector(&[3.0, 4.0]).unwrap())
                .unwrap()
        );
        let selected = parts(&router, &artifacts.leaves, &ids);
        let nomination = router.validate_selected(&ids, &selected).unwrap();
        assert_eq!(nomination.leaf_ids, ids);
        assert_eq!(nomination.units, (0..1024).collect());
        assert_eq!(nomination.walk_units, (0..1024).collect::<Vec<_>>());
        assert!(nomination.seed_additions.is_empty());
        assert_eq!(nomination.page_closure, (0..128).collect());
        let last = router
            .validate_selected(&[16], &parts(&router, &artifacts.leaves, &[16]))
            .unwrap();
        assert_eq!(last.units, [1024].into_iter().collect());
        assert_eq!(last.seed_page, 128);
        assert_eq!(last.page_closure, [128].into_iter().collect());
        assert_eq!(last.walk_units, [1024]);
        assert!(last.seed_additions.is_empty());

        // A bad unselected leaf cannot affect bounded serving validation; complete
        // publication must still reject it. No full payload is passed to serving.
        let mut corrupt = artifacts.leaves.clone();
        *corrupt.last_mut().unwrap() ^= 1;
        router
            .validate_selected(&ids, &parts(&router, &corrupt, &ids))
            .unwrap();
        assert!(
            validate_publication(
                &artifacts.manifest,
                &artifacts.membership,
                &corrupt,
                &source,
                &blob
            )
            .is_err()
        );
        assert!(
            router
                .validate_selected(&[16], &parts(&router, &corrupt, &[16]))
                .is_err()
        );
        assert!(
            router
                .validate_selected(&[0, 0], &[selected[0], selected[0]])
                .is_err()
        );
        assert!(
            router
                .validate_selected(&[0, 1], &[selected[1], selected[0]])
                .is_err()
        );
        assert!(router.validate_selected(&ids, &selected[..15]).is_err());
        assert!(router.validate_selected(&[], &[]).is_err());
        assert!(router.validate_leaf(17, &[]).is_err());
        assert!(
            router
                .validate_leaf(0, &selected[0][..selected[0].len() - 1])
                .is_err()
        );
        assert!(
            router
                .validate_leaf(0, &[selected[0], &[0]].concat())
                .is_err()
        );
        assert!(router.nominate(&[0., 0.]).is_err());
        assert!(router.nominate(&[f32::NAN, 1.]).is_err());
        assert!(router.nominate(&[1.]).is_err());
        assert!(
            SemanticUnitRouter::open(
                &artifacts.manifest,
                &artifacts.membership,
                &hash(&artifacts.manifest),
                &source,
                artifacts.manifest.len() - 1
            )
            .is_err()
        );
        assert!(
            SemanticUnitRouter::open(
                &artifacts.manifest,
                &artifacts.membership,
                &"0".repeat(64),
                &source,
                1 << 20
            )
            .is_err()
        );
        let mut membership = artifacts.membership.clone();
        membership[0] ^= 1;
        assert!(
            SemanticUnitRouter::open(
                &artifacts.manifest,
                &membership,
                &hash(&artifacts.manifest),
                &source,
                1 << 20
            )
            .is_err()
        );
    }

    #[test]
    fn refreshed_hashes_cannot_hide_bad_membership_partition_or_nonfinite_values() {
        let blob = blob(4097);
        let digest = hash(&blob);
        let source = SourceIdentity {
            centroids_sha256: &digest,
            ..source(&blob)
        };
        let artifacts = build(&blob, &source, ALLOCATION_CAP).unwrap();
        let manifest = open(&artifacts, &source).manifest().clone();
        for (offset, value) in [
            (12, 99_u32),
            (8, 511),
            (24, 769),
            (28, u32::MAX),
            (32, u32::MAX),
            (512, 1),
            (516, 1),
            (528, 1),
            (532, 63),
            (536, 1),
            (540, 1),
        ] {
            let mut body = artifacts.manifest.clone();
            body[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
            assert!(
                SemanticUnitRouter::open(
                    &body,
                    &artifacts.membership,
                    &hash(&body),
                    &source,
                    1 << 20
                )
                .is_err(),
                "{offset}"
            );
        }
        for offset in [0, 88, 127, 511] {
            let mut body = artifacts.manifest.clone();
            body[offset] ^= 1;
            assert!(
                SemanticUnitRouter::open(
                    &body,
                    &artifacts.membership,
                    &hash(&body),
                    &source,
                    1 << 20
                )
                .is_err()
            );
        }
        for length in [0, 7, 511, artifacts.manifest.len() - 1] {
            let body = &artifacts.manifest[..length];
            assert!(
                SemanticUnitRouter::open(
                    body,
                    &artifacts.membership,
                    &hash(body),
                    &source,
                    1 << 20
                )
                .is_err()
            );
        }
        let mut trailing = artifacts.manifest.clone();
        trailing.push(0);
        assert!(
            SemanticUnitRouter::open(
                &trailing,
                &artifacts.membership,
                &hash(&trailing),
                &source,
                1 << 20
            )
            .is_err()
        );
        for value in [f32::INFINITY, f32::NAN] {
            let mut bad = manifest.clone();
            bad.leaves[0].prototype[0] = value;
            let body = bad.encode().unwrap();
            assert!(
                SemanticUnitRouter::open(
                    &body,
                    &artifacts.membership,
                    &hash(&body),
                    &source,
                    1 << 20
                )
                .is_err()
            );
        }
        for id in [1_u32, u32::MAX] {
            let mut membership = artifacts.membership.clone();
            membership[..4].copy_from_slice(&id.to_le_bytes());
            let mut bad = manifest.clone();
            bad.membership.sha256 = hash(&membership);
            let body = bad.encode().unwrap();
            assert!(
                SemanticUnitRouter::open(&body, &membership, &hash(&body), &source, 1 << 20)
                    .is_err()
            );
        }
        let first_bytes = manifest.leaves[0].bytes;
        for (offset, bytes) in [
            (0, 1_u32.to_le_bytes().to_vec()),
            (0, 64_u32.to_le_bytes().to_vec()),
            (4, 0x7e00_u16.to_le_bytes().to_vec()),
        ] {
            let mut payload = artifacts.leaves.clone();
            payload[offset..offset + bytes.len()].copy_from_slice(&bytes);
            let mut bad = manifest.clone();
            bad.leaves[0].sha256 = hash(&payload[..first_bytes]);
            bad.leaf_payload.sha256 = hash(&payload);
            let body = bad.encode().unwrap();
            let router = SemanticUnitRouter::open(
                &body,
                &artifacts.membership,
                &hash(&body),
                &source,
                1 << 20,
            )
            .unwrap();
            if offset == 0 {
                assert!(
                    router
                        .validate_selected(&[0], &[&payload[..first_bytes]])
                        .is_err()
                );
            }
            assert!(
                validate_publication(&body, &artifacts.membership, &payload, &source, &blob)
                    .is_err()
            );
        }
        // Signed-zero tampering preserves the prototype. Only full publication
        // has the original centroid authority needed to reject rewritten FP16.
        let mut payload = artifacts.leaves.clone();
        payload[5] = 0x80;
        let mut bad = manifest.clone();
        bad.leaves[0].sha256 = hash(&payload[..first_bytes]);
        bad.leaf_payload.sha256 = hash(&payload);
        let body = bad.encode().unwrap();
        let router =
            SemanticUnitRouter::open(&body, &artifacts.membership, &hash(&body), &source, 1 << 20)
                .unwrap();
        router
            .validate_selected(&[0], &[&payload[..first_bytes]])
            .unwrap();
        assert!(
            validate_publication(&body, &artifacts.membership, &payload, &source, &blob).is_err()
        );
        assert!(build(&blob, &source, 1).is_err());
        assert!(build(&blob, &source, ALLOCATION_CAP + 1).is_err());
    }
}
