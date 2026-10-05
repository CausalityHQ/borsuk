//! Isolated, unqualified hierarchical semantic-cell research prototype.
//!
//! No production reader, defaults, or formats are changed. Build accepts only
//! authenticated current-generation source artifacts, never queries or truth.
//! The complete directory is admitted/authenticated at startup and kept resident.
//! Query routing has no directory I/O. WholeCell reads all selected cell payload
//! in one wave; TwoStage is retained for synthetic source/refinement comparisons.
//! Binary hierarchy assignment is local to each node; cells contain unchanged
//! two-bit records followed by colocated, at most 32-row SQ8 refinement blocks.
//! The old physical 256-row page closure does not participate.
//!
//! All GET counters mean actual positioned **local range reads**, not network
//! operations, retries, latency, billing, or cache behavior. Dependency waves
//! are explicit; this serial adapter cannot establish remote parallelism/RSS.
//! Payload models exclude allocator, runtime, OS cache and other processes.
//!
//! With C cells, P=max(1,C-1) binary pages and K=sum(ceil(cell_rows/32))
//! refinement descriptors, full directory size B is O(P*D+P+C+K), not a
//! size-independent resident root. Preload charges 5B+256(P+C)+8*65536;
//! retained parsed payload charges 4B+256(P+C)+8*65536. N>100k is rejected;
//! extending this formula to 100M is arithmetic, not a feasibility claim.

use crate::{
    VectorMetric,
    exact_sq8_nominee::Sq8Geometry,
    returned_sq8::{ReturnedRange, rank_returned_ranges},
    rotated_two_bit::RotatedTwoBitCodec,
    sq8_source::cosine_vector,
    train_logical_cell_centroids,
    two_bit_generation::{Manifest as InputManifest, SCHEMA as INPUT_SCHEMA},
    two_bit_source::SourcePlaneReceipt,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    os::unix::{fs::FileExt, fs::OpenOptionsExt},
    path::{Path, PathBuf},
    time::Instant,
};

/// Fallible research build and admission operations.
pub type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
/// Exact build configuration marker.
pub const BUILD_SCHEMA: &str = "borsuk-hierarchical-cells-build-v2";
const SCHEMA: &str = "borsuk-hierarchical-cells-resident-v4";
const ROOT_CAP: usize = 65536;
const PAGE_CAP: usize = 65536;
const BLOCK_ROWS: usize = 32;
// Same fixed seed admitted by the current native plane-v3 reader.
const NATIVE_CODEC_SEED: u32 = 20260923;

fn require(ok: bool, message: &str) -> Result<()> {
    if ok { Ok(()) } else { Err(message.into()) }
}
fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn valid_sha(sha: &str) -> bool {
    sha.len() == 64
        && sha
            .bytes()
            .all(|v| v.is_ascii_hexdigit() && !v.is_ascii_uppercase())
}

/// Pinned local immutable artifact; the parent owns descriptor authentication.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Artifact {
    /// Local regular file.
    pub path: PathBuf,
    /// Exact bytes, not a maximum.
    pub bytes: usize,
    /// Trusted lowercase SHA256.
    pub sha256: String,
}
impl Artifact {
    fn open(&self, cap: usize) -> Result<File> {
        self.open_streamed(cap, rustix::fs::OFlags::NONBLOCK)
    }
    /// Secure final authentication of an already admitted large source object.
    /// Reuses the exact-length/SHA/EOF stream without allocating an object Vec.
    pub(crate) fn authenticate_streamed(&self, admitted_bytes: usize) -> Result<()> {
        self.open_streamed(admitted_bytes, rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK)
            .map(drop)
    }
    fn open_streamed(&self, cap: usize, flags: rustix::fs::OFlags) -> Result<File> {
        require(
            self.bytes > 0 && self.bytes <= cap && valid_sha(&self.sha256),
            "artifact descriptor/cap",
        )?;
        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(flags.bits() as i32)
            .open(&self.path)?;
        let metadata = file.metadata()?;
        require(
            metadata.is_file() && metadata.len() == self.bytes as u64,
            "artifact regular file/exact length",
        )?;
        authenticate_artifact_file(&mut file, self.bytes, &self.sha256)?;
        file.seek(SeekFrom::Start(0))?;
        Ok(file)
    }
    /// Read an authenticated small artifact with an allocation cap.
    pub fn read(&self, cap: usize) -> Result<Vec<u8>> {
        require(
            self.bytes > 0 && self.bytes <= cap && valid_sha(&self.sha256),
            "artifact descriptor/cap",
        )?;
        let mut file = regular_file(&self.path, self.bytes)?;
        let mut bytes = vec![0; self.bytes];
        file.read_exact(&mut bytes)?;
        require(
            file.read(&mut [0])? == 0 && hash(&bytes) == self.sha256,
            "artifact digest/length",
        )?;
        Ok(bytes)
    }
}
pub(crate) const ARTIFACT_STREAM_BUFFER_BYTES: usize = 65536;
fn authenticate_artifact_file(file: &mut File, bytes: usize, sha256: &str) -> Result<()> {
    let mut digest = Sha256::new();
    let mut remaining = bytes;
    let mut buffer = [0_u8; ARTIFACT_STREAM_BUFFER_BYTES];
    while remaining > 0 {
        let amount = remaining.min(buffer.len());
        file.read_exact(&mut buffer[..amount])?;
        digest.update(&buffer[..amount]);
        remaining -= amount;
    }
    require(file.read(&mut [0])? == 0 && format!("{:x}", digest.finalize()) == sha256,
        "artifact digest/length")
}

/// Explicit, truth-free build inputs and prototype limits; no tuning defaults.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BuildConfig {
    /// Must equal [`BUILD_SCHEMA`].
    pub schema: String,
    /// Current native generation root, binding canonical, plane and SQ8.
    pub generation: Artifact,
    /// Current plane-v3 receipt; preserves codec seed, mean and scalar format.
    pub plane: Artifact,
    /// Current canonical normalized FP32 records, ID + D little-endian f32s.
    pub canonical: Artifact,
    /// Physical-to-source-ordinal LE u64 permutation.
    pub order: Artifact,
    /// Unchanged Rust-v1 two-bit records in that physical order.
    pub records: Artifact,
    /// Unchanged codec mean, LE f32s.
    pub mean: Artifact,
    /// Unchanged ID/norm/SQ8 records in that physical order.
    pub sq8: Artifact,
    /// Maximum rows in a semantic cell (1..=512).
    pub cell_rows: usize,
    /// Maximum node training sample (2..=1024).
    pub sample_rows: usize,
    /// Maximum directory dependency depth (1..=32).
    pub max_depth: usize,
    /// Conservative build payload admission, excluding host overhead.
    pub max_build_payload_bytes: usize,
    /// Maximum new output including the complete unpublished staging copy.
    pub max_output_bytes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Span {
    offset: usize,
    bytes: usize,
    sha256: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cell {
    id: usize,
    first_row: usize,
    whole: Span,
    source: Span,
    refinement: Vec<Span>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Target {
    Directory { span: Span },
    Cell { cell: Cell },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Node {
    rows: usize,
    prototype: Vec<f32>,
    target: Target,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Directory {
    children: Vec<Node>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    schema: String,
    input: BuildConfig,
    rows: usize,
    dimensions: usize,
    seed: u32,
    mean: Vec<f32>,
    low: Vec<f32>,
    step: Vec<f32>,
    root_directory: Span,
    directory_bytes: usize,
    directory_sha256: String,
    cell_bytes: usize,
    build: BuildReceipt,
}

/// Build identity and geometry/resource arithmetic, never measured feasibility.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BuildReceipt {
    /// Digest of the completed research manifest.
    pub root_sha256: String,
    /// Cells emitted.
    pub cells: usize,
    /// Authenticated directory pages emitted.
    pub directories: usize,
    /// Largest emitted cell.
    pub max_cell_rows: usize,
    /// Largest directory dependency depth.
    pub max_depth: usize,
    /// Nondegenerate skewed sampled splits repaired with fixed-center capacities.
    pub semantic_repairs: usize,
    /// Deterministic coordinate medians for identical samples/degenerate centers.
    pub geometry_fallbacks: usize,
    /// Actual canonical row reads during admission/training/assignment/layout.
    pub canonical_row_reads: usize,
    /// Conservative build payload model, excludes runtime/allocator/cache.
    pub modeled_build_payload_bytes: usize,
    /// Pinned input bytes coexisting with staging/output, counted once each.
    pub input_bytes: usize,
    /// Complete new output bytes, including manifest.
    pub output_bytes: usize,
}

fn read_at(file: &File, offset: usize, bytes: usize) -> Result<Vec<u8>> {
    let mut body = vec![0; bytes];
    file.read_exact_at(&mut body, offset as u64)?;
    Ok(body)
}
fn new_file(path: &Path) -> Result<File> {
    Ok(OpenOptions::new().write(true).create_new(true).open(path)?)
}

// Minimize the sum of selected rounded f32 margins (d_left - d_right),
// not exact-real distance differences. The unconstrained count clamped to
// [ceil(N/4), N-ceil(N/4)] minimizes that objective; sorting selects its
// cheapest rows. Balanced sampled assignments keep their exact membership.
// Only compact (margin, source ordinal, slot) scratch is allocated, never vectors.
fn capacity_partition(ids: &[usize], delta: &[f32], left: &mut [bool]) -> Result<bool> {
    require(
        ids.len() >= 2
            && ids.len() == delta.len()
            && ids.len() == left.len()
            && delta.iter().all(|v| v.is_finite())
            && left.iter().zip(delta).all(|(&a, &d)| a == (d <= 0.)),
        "capacity partition geometry/nonfinite/assignment",
    )?;
    let lower = ids.len().div_ceil(4);
    let count = left.iter().filter(|v| **v).count();
    if count.min(ids.len() - count) >= lower {
        return Ok(false);
    }
    let count = count.clamp(lower, ids.len() - lower);
    let mut order = Vec::new();
    order.try_reserve_exact(ids.len())?;
    order.extend(delta.iter().enumerate().map(|(slot, &d)| {
        // Both signed zeros are the same assignment cost/ID tie.
        (if d == 0. { 0. } else { d }, ids[slot], slot)
    }));
    order.sort_unstable_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
    left.fill(false);
    for item in &order[..count] {
        left[item.2] = true;
    }
    Ok(true)
}

/// Exact batch predicate, with its predetermined extension to outside rows.
#[derive(Debug, Clone)]
pub enum SplitBoundary {
    /// None is ordinary rounded delta<=0; Some is the last-left normalized
    /// (rounded delta, source ID) key of a capacity repair.
    Centers { left: Vec<f32>, right: Vec<f32>, separation: f32, cut: Option<(f32, usize)> },
    /// Coordinate total_cmp intentionally preserves signed zeros.
    Coordinate { axis: usize, cut: (f32, usize), range: f32 },
}
impl SplitBoundary {
    fn delta(&self, vector: &[f32]) -> Result<f32> {
        let Self::Centers { left, right, .. } = self else { return Err("not a center boundary".into()); };
        let a = VectorMetric::SquaredEuclidean.distance(vector, left)?;
        let b = VectorMetric::SquaredEuclidean.distance(vector, right)?;
        let d = a - b;
        require(a.is_finite() && b.is_finite() && d.is_finite(), "boundary nonfinite delta")?;
        Ok(d)
    }
    /// Evaluate the original rounded predicate on the unchanged vector and ID.
    pub fn goes_left(&self, vector: &[f32], id: usize) -> Result<bool> {
        match self {
            Self::Centers { cut, .. } => {
                let d = self.delta(vector)?;
                Ok(match cut {
                    None => d <= 0.,
                    Some((score, tie)) => {
                        let d = if d == 0. { 0. } else { d };
                        let score = if *score == 0. { 0. } else { *score };
                        d.total_cmp(&score).then(id.cmp(tie)).is_le()
                    }
                })
            }
            Self::Coordinate { axis, cut, .. } => {
                let v = *vector.get(*axis).ok_or("boundary coordinate")?;
                require(v.is_finite(), "boundary nonfinite coordinate")?;
                Ok(v.total_cmp(&cut.0).then(id.cmp(&cut.1)).is_le())
            }
        }
    }
    /// Declared rounded-score margin proxy; degeneracy forbids crossing only.
    pub fn margin(&self, vector: &[f32]) -> Result<Option<f64>> {
        let value = match self {
            Self::Centers { separation, cut, .. } => {
                if !separation.is_finite() || *separation <= 0. { return Ok(None); }
                (f64::from(self.delta(vector)?) - f64::from(cut.map_or(0., |v| v.0))).abs()
                    / (2. * f64::from(*separation).sqrt())
            }
            Self::Coordinate { axis, cut, range } => {
                if !range.is_finite() || *range <= 0. { return Ok(None); }
                (f64::from(*vector.get(*axis).ok_or("boundary coordinate")?) - f64::from(cut.0)).abs()
            }
        };
        require(value.is_finite(), "boundary nonfinite margin")?;
        Ok(Some(value))
    }
}
/// Ordered primary leaf captured from the actual Builder path.
#[derive(Debug, Clone)]
pub struct ReplayCell { pub cell_id: usize, pub ids: Vec<usize> }
/// Preorder boundary identity, with exact original children and leaf roster.
#[derive(Debug, Clone, Default)]
pub struct ReplayNode {
    pub depth: usize,
    pub boundary: Option<SplitBoundary>,
    pub children: Option<[usize; 2]>,
    pub cell: Option<ReplayCell>,
}
/// Successful exact replay of an authenticated retained capacity-v4 layout.
#[derive(Debug, Clone)]
pub struct PrimaryReplay {
    pub logical_rows: usize,
    pub dimensions: usize,
    pub nodes: Vec<ReplayNode>,
}

struct Builder<'a> {
    config: &'a BuildConfig,
    canonical: File,
    records: File,
    sq8: File,
    directories: File,
    cells: File,
    inverse: Vec<usize>,
    dimensions: usize,
    record_bytes: usize,
    directory_bytes: usize,
    directory_digest: Sha256,
    cell_bytes: usize,
    next_row: usize,
    receipt: BuildReceipt,
    replay: Option<Vec<ReplayNode>>,
}
impl Builder<'_> {
    fn vector(&mut self, id: usize) -> Result<Vec<f32>> {
        let body = read_at(
            &self.canonical,
            self.inverse[id] * (8 + self.dimensions * 4),
            8 + self.dimensions * 4,
        )?;
        require(
            i64::from_le_bytes(body[..8].try_into()?) == id as i64,
            "canonical ID changed",
        )?;
        let vector = body[8..]
            .chunks_exact(4)
            .map(|v| f32::from_le_bytes(v.try_into().unwrap()))
            .collect::<Vec<_>>();
        let normalized = cosine_vector(&vector)?;
        require(
            vector
                .iter()
                .zip(normalized.iter())
                .all(|(a, b)| (a - b).abs() <= 1e-5),
            "canonical is not normalized",
        )?;
        self.receipt.canonical_row_reads += 1;
        Ok(vector)
    }
    fn append(&mut self, directory: bool, body: &[u8]) -> Result<Span> {
        let bytes = if directory {
            self.directory_bytes
        } else {
            self.cell_bytes
        };
        require(
            self.directory_bytes
                .checked_add(self.cell_bytes)
                .and_then(|n| n.checked_add(body.len() + ROOT_CAP))
                .is_some_and(|n| n <= self.config.max_output_bytes),
            "output/staging byte cap",
        )?;
        if directory {
            self.directories.write_all(body)?;
            self.directory_digest.update(body);
            self.directory_bytes += body.len();
        } else {
            self.cells.write_all(body)?;
            self.cell_bytes += body.len();
        }
        Ok(Span {
            offset: bytes,
            bytes: body.len(),
            sha256: hash(body),
        })
    }
    fn node(&mut self, mut ids: Vec<usize>, depth: usize) -> Result<Node> {
        let replay_index = self.replay.as_mut().map(|nodes| {
            let i = nodes.len();
            nodes.push(ReplayNode { depth, ..ReplayNode::default() });
            i
        });
        require(depth <= self.config.max_depth, "directory depth cap")?;
        self.receipt.max_depth = self.receipt.max_depth.max(depth);
        let mut sums = vec![0_f64; self.dimensions];
        let mut minimum = vec![f32::INFINITY; self.dimensions];
        let mut maximum = vec![f32::NEG_INFINITY; self.dimensions];
        for &id in &ids {
            let vector = self.vector(id)?;
            for coordinate in 0..self.dimensions {
                sums[coordinate] += f64::from(vector[coordinate]);
                minimum[coordinate] = minimum[coordinate].min(vector[coordinate]);
                maximum[coordinate] = maximum[coordinate].max(vector[coordinate]);
            }
        }
        let prototype = sums
            .iter()
            .map(|v| (v / ids.len() as f64) as f32)
            .collect::<Vec<_>>();
        let rows = ids.len();
        if rows <= self.config.cell_rows {
            // Keep nearby rows together inside a cell. Ties retain source ordinals.
            let mut distances = Vec::with_capacity(rows);
            for id in ids {
                distances.push((
                    VectorMetric::SquaredEuclidean.distance(&self.vector(id)?, &prototype)?,
                    id,
                ));
            }
            distances.sort_unstable_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
            ids = distances.iter().map(|v| v.1).collect();
            let mut source = Vec::with_capacity(rows * (8 + self.record_bytes));
            let mut sq8 = Vec::with_capacity(rows * (12 + self.dimensions));
            for &id in &ids {
                source.extend_from_slice(&(id as i64).to_le_bytes());
                source.extend_from_slice(&read_at(
                    &self.records,
                    self.inverse[id] * self.record_bytes,
                    self.record_bytes,
                )?);
                let encoded = read_at(
                    &self.sq8,
                    self.inverse[id] * (12 + self.dimensions),
                    12 + self.dimensions,
                )?;
                require(
                    i64::from_le_bytes(encoded[..8].try_into()?) == id as i64,
                    "SQ8 ID changed",
                )?;
                sq8.extend_from_slice(&encoded);
            }
            let mut whole_digest = Sha256::new();
            whole_digest.update(&source);
            whole_digest.update(&sq8);
            let whole = Span {
                offset: self.cell_bytes,
                bytes: source.len() + sq8.len(),
                sha256: format!("{:x}", whole_digest.finalize()),
            };
            let source = self.append(false, &source)?;
            let mut refinement = Vec::with_capacity(rows.div_ceil(BLOCK_ROWS));
            for block in sq8.chunks(BLOCK_ROWS * (12 + self.dimensions)) {
                refinement.push(self.append(false, block)?);
            }
            let cell = Cell {
                id: self.receipt.cells,
                first_row: self.next_row,
                whole,
                source,
                refinement,
            };
            self.next_row += rows;
            self.receipt.cells += 1;
            self.receipt.max_cell_rows = self.receipt.max_cell_rows.max(rows);
            if let Some(index) = replay_index {
                self.replay.as_mut().unwrap()[index].cell = Some(ReplayCell { cell_id: cell.id, ids });
            }
            return Ok(Node {
                rows,
                prototype,
                target: Target::Cell { cell },
            });
        }
        ids.sort_unstable();
        let sample_count = self.config.sample_rows.min(rows);
        let mut sample = Vec::with_capacity(sample_count);
        for slot in 0..sample_count {
            sample.push(self.vector(ids[slot * rows / sample_count])?);
        }
        let identical_sample = sample.iter().all(|v| v == &sample[0]);
        let mut assigned = Vec::new();
        assigned.try_reserve_exact(rows)?;
        assigned.resize(rows, false);
        let mut delta = Vec::new();
        let mut degenerate = identical_sample;
        let mut boundary = None;
        if !identical_sample {
            // Same two sampled centers/distances; failures propagate, no sweep.
            let centers =
                train_logical_cell_centroids(&sample, VectorMetric::SquaredEuclidean, 2, 4)?;
            require(
                centers.len() == 2
                    && centers.iter().all(|center| {
                        center.len() == self.dimensions && center.iter().all(|v| v.is_finite())
                    }),
                "sampled center geometry/nonfinite",
            )?;
            let separation = VectorMetric::SquaredEuclidean.distance(&centers[0], &centers[1])?;
            require(separation.is_finite(), "nonfinite sampled separator")?;
            degenerate = separation <= 0.;
            if replay_index.is_some() {
                boundary = Some(SplitBoundary::Centers { left: centers[0].clone(),
                    right: centers[1].clone(), separation, cut: None });
            }
            drop(sample);
            delta.try_reserve_exact(rows)?;
            for (slot, &id) in ids.iter().enumerate() {
                let vector = self.vector(id)?;
                let a = VectorMetric::SquaredEuclidean.distance(&vector, &centers[0])?;
                let b = VectorMetric::SquaredEuclidean.distance(&vector, &centers[1])?;
                let margin = a - b;
                require(
                    a.is_finite() && b.is_finite() && margin.is_finite(),
                    "nonfinite sampled assignment",
                )?;
                assigned[slot] = a <= b;
                delta.push(margin);
            }
        } else {
            drop(sample);
        }
        let count = assigned.iter().filter(|v| **v).count();
        if count.min(rows - count) < rows.div_ceil(4) {
            if degenerate {
                self.receipt.geometry_fallbacks += 1;
                require(
                    minimum
                        .iter()
                        .zip(&maximum)
                        .all(|(a, b)| (b - a).is_finite()),
                    "coordinate range overflow",
                )?;
                let coordinate = (0..self.dimensions)
                    .max_by(|&a, &b| {
                        (maximum[a] - minimum[a])
                            .total_cmp(&(maximum[b] - minimum[b]))
                            .then(b.cmp(&a))
                    })
                    .ok_or("empty coordinate geometry")?;
                let mut projected = Vec::new();
                projected.try_reserve_exact(rows)?;
                for &id in &ids {
                    projected.push((self.vector(id)?[coordinate], id));
                }
                projected.sort_unstable_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
                if replay_index.is_some() {
                    boundary = Some(SplitBoundary::Coordinate { axis: coordinate,
                        cut: projected[rows / 2 - 1], range: maximum[coordinate] - minimum[coordinate] });
                }
                // Preserve the original projected child order: node sums its
                // prototype before sorting IDs, so floating-point order matters.
                for (slot, item) in projected.iter().enumerate() {
                    ids[slot] = item.1;
                    assigned[slot] = slot < rows / 2;
                }
            } else if capacity_partition(&ids, &delta, &mut assigned)? {
                self.receipt.semantic_repairs += 1;
                if let Some(SplitBoundary::Centers { cut, .. }) = &mut boundary {
                    *cut = ids.iter().zip(&delta).zip(&assigned).filter(|(_, a)| **a)
                        .map(|((&id, &d), _)| (if d == 0. { 0. } else { d }, id))
                        .max_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
                }
            }
        }
        drop(delta);
        drop(minimum);
        drop(maximum);
        drop(sums);
        let count = assigned.iter().filter(|v| **v).count();
        require(
            count.min(rows - count) >= rows.div_ceil(4),
            "complete capacity-bounded partition",
        )?;
        let mut left = Vec::new();
        let mut right = Vec::new();
        left.try_reserve_exact(count)?;
        right.try_reserve_exact(rows - count)?;
        for (id, a) in ids.into_iter().zip(assigned) {
            if a {
                left.push(id);
            } else {
                right.push(id);
            }
        }
        let left_index = self.replay.as_ref().map_or(0, Vec::len);
        let left_node = self.node(left, depth + 1)?;
        let right_index = self.replay.as_ref().map_or(0, Vec::len);
        let right_node = self.node(right, depth + 1)?;
        if let Some(index) = replay_index {
            let capture = &mut self.replay.as_mut().unwrap()[index];
            capture.boundary = boundary;
            capture.children = Some([left_index, right_index]);
        }
        let children = vec![left_node, right_node];
        let body = serde_json::to_vec(&Directory { children })?;
        require(body.len() <= PAGE_CAP, "directory page cap")?;
        let span = self.append(true, &body)?;
        self.receipt.directories += 1;
        Ok(Node {
            rows,
            prototype,
            target: Target::Directory { span },
        })
    }
}

/// Stream a source-only hierarchy into a new local research artifact directory.
/// Inputs must remain immutable; they are reauthenticated before installation.
/// The parent must qualify this exact source before any real/paid experiment.
pub fn build(config: &BuildConfig, output: &Path) -> Result<BuildReceipt> {
    Ok(build_captured(config, output, false)?.0)
}

fn build_captured(config: &BuildConfig, output: &Path, capture: bool) -> Result<(BuildReceipt, Vec<ReplayNode>)> {
    require(
        config.schema == BUILD_SCHEMA
            && (1..=512).contains(&config.cell_rows)
            && (2..=1024).contains(&config.sample_rows)
            && (1..=32).contains(&config.max_depth),
        "build schema/limits",
    )?;
    require(
        fs::symlink_metadata(output).is_err(),
        "output already exists",
    )?;
    let root: InputManifest = serde_json::from_slice(&config.generation.read(ROOT_CAP)?)?;
    let plane: SourcePlaneReceipt = serde_json::from_slice(&config.plane.read(ROOT_CAP)?)?;
    require(plane.seed == NATIVE_CODEC_SEED, "current fixed SQ2 seed")?;
    let rows = root.canonical.rows;
    let dimensions = root.canonical.dimensions;
    require(
        root.schema == INPUT_SCHEMA
            && (1..=100_000).contains(&rows)
            && (1..=768).contains(&dimensions),
        "current source schema/prototype geometry",
    )?;
    require(
        plane.schema == "borsuk-two-bit-plane-v3"
            && !plane.query_or_truth_used
            && plane.rows == rows
            && plane.dimensions == dimensions
            && (9..=200).contains(&plane.record_bytes)
            && root.plane_manifest_sha256 == config.plane.sha256
            && root.canonical.sha256 == config.canonical.sha256
            && root.canonical.bytes == config.canonical.bytes as u64
            && root.canonical.bytes == (rows * (dimensions * 4 + 8)) as u64
            && root.sq8_object_sha256 == config.sq8.sha256
            && plane.sq8_sha256 == config.sq8.sha256
            && plane.source_order_sha256 == config.order.sha256
            && plane.records_sha256 == config.records.sha256
            && plane.mean_sha256 == config.mean.sha256
            && config.order.bytes == rows * 8
            && config.sq8.bytes == rows * (dimensions + 12)
            && config.mean.bytes == dimensions * 4
            && config.records.bytes == rows * plane.record_bytes,
        "input root/plane/geometry binding",
    )?;
    require(
        root.low.len() == dimensions
            && root.step.len() == dimensions
            && root.low.iter().all(|v| v.is_finite())
            && root.step.iter().all(|v| v.is_finite() && *v > 0.),
        "SQ8 coefficients",
    )?;
    // 128 bytes/row covers inverse/recursive ID rosters, assignment flags and
    // margins, plus the explicitly reserved 24-byte margin/ID/slot sort scratch.
    // Partition scratch is dropped before recursion. Samples/trainer clones,
    // prototypes, cell buffers and serialization are separate terms below;
    // runtime/allocator/cache overhead remains an external charge.
    let modeled = rows * 128
        + config.sample_rows * dimensions * 16
        + config.max_depth * dimensions * 64
        + config.cell_rows * (dimensions + 12 + plane.record_bytes + 8) * 4
        + 16 * PAGE_CAP;
    require(
        modeled <= config.max_build_payload_bytes,
        "build payload admission",
    )?;
    let mean = config
        .mean
        .read(dimensions * 4)?
        .chunks_exact(4)
        .map(|v| f32::from_le_bytes(v.try_into().unwrap()))
        .collect::<Vec<_>>();
    let codec = RotatedTwoBitCodec::new(&mean, plane.seed)?;
    require(
        plane.record_bytes == codec.record_bytes(),
        "current two-bit scalar/width format",
    )?;
    let order = config.order.read(rows * 8)?;
    let mut inverse = vec![usize::MAX; rows];
    for (physical, encoded) in order.chunks_exact(8).enumerate() {
        let id = usize::try_from(u64::from_le_bytes(encoded.try_into()?))?;
        require(
            id < rows && inverse[id] == usize::MAX,
            "full source ordinal permutation",
        )?;
        inverse[id] = physical;
    }
    drop(order);
    let canonical = config.canonical.open(rows * (dimensions * 4 + 8))?;
    let records = config.records.open(rows * codec.record_bytes())?;
    let sq8 = config.sq8.open(rows * (dimensions + 12))?;
    let parent = output
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let staging = tempfile::tempdir_in(parent)?;
    let mut builder = Builder {
        config,
        canonical,
        records,
        sq8,
        directories: new_file(&staging.path().join("directories.bin"))?,
        cells: new_file(&staging.path().join("cells.bin"))?,
        inverse,
        dimensions,
        record_bytes: codec.record_bytes(),
        directory_bytes: 0,
        directory_digest: Sha256::new(),
        cell_bytes: 0,
        next_row: 0,
        receipt: BuildReceipt {
            modeled_build_payload_bytes: modeled,
            input_bytes: [
                &config.generation,
                &config.plane,
                &config.canonical,
                &config.order,
                &config.records,
                &config.mean,
                &config.sq8,
            ]
            .iter()
            .map(|v| v.bytes)
            .sum(),
            ..BuildReceipt::default()
        },
        replay: capture.then(Vec::new),
    };
    // Full ID roster validation precedes hierarchy fitting.
    let mut validation_query = vec![0.; dimensions];
    validation_query[0] = 1.;
    let prepared = codec.prepare_query(&validation_query, 400_000)?;
    for id in 0..rows {
        builder.vector(id)?;
        let encoded = read_at(
            &builder.sq8,
            builder.inverse[id] * (dimensions + 12),
            dimensions + 12,
        )?;
        require(
            i64::from_le_bytes(encoded[..8].try_into()?) == id as i64
                && f32::from_le_bytes(encoded[8..12].try_into()?).is_finite()
                && f32::from_le_bytes(encoded[8..12].try_into()?) > 0.,
            "SQ8/canonical/order ID binding",
        )?;
        prepared.score(&read_at(
            &builder.records,
            builder.inverse[id] * codec.record_bytes(),
            codec.record_bytes(),
        )?)?;
    }
    drop(prepared);
    let top = builder.node((0..rows).collect(), 1)?;
    let root_directory = match top.target {
        Target::Directory { span } => span,
        cell => {
            let body = serde_json::to_vec(&Directory {
                children: vec![Node {
                    rows,
                    prototype: top.prototype,
                    target: cell,
                }],
            })?;
            builder.receipt.directories += 1;
            builder.append(true, &body)?
        }
    };
    require(builder.next_row == rows, "complete output row roster")?;
    // Detect changes while random-seek layout was built; no manifest on failure.
    for artifact in [
        &config.generation,
        &config.plane,
        &config.canonical,
        &config.order,
        &config.records,
        &config.mean,
        &config.sq8,
    ] {
        artifact.open(artifact.bytes)?;
    }
    builder.directories.sync_all()?;
    builder.cells.sync_all()?;
    let mut manifest = Manifest {
        schema: SCHEMA.into(),
        input: config.clone(),
        rows,
        dimensions,
        seed: plane.seed,
        mean,
        low: root.low,
        step: root.step,
        root_directory,
        directory_bytes: builder.directory_bytes,
        directory_sha256: format!("{:x}", builder.directory_digest.clone().finalize()),
        cell_bytes: builder.cell_bytes,
        build: builder.receipt.clone(),
    };
    // Receipt excludes its own hash. Resolve the byte-count decimal width
    // before freezing, including output sizes just across a power of ten.
    let mut body = Vec::new();
    for _ in 0..8 {
        body = serde_json::to_vec(&manifest)?;
        let bytes = builder.directory_bytes + builder.cell_bytes + body.len();
        if manifest.build.output_bytes == bytes {
            break;
        }
        manifest.build.output_bytes = bytes;
    }
    require(
        manifest.build.output_bytes == builder.directory_bytes + builder.cell_bytes + body.len(),
        "manifest byte-count fixed point",
    )?;
    require(
        body.len() <= ROOT_CAP
            && body.len() + builder.directory_bytes + builder.cell_bytes <= config.max_output_bytes,
        "manifest/output cap",
    )?;
    let mut out = new_file(&staging.path().join("manifest.json"))?;
    out.write_all(&body)?;
    out.sync_all()?;
    File::open(staging.path())?.sync_all()?;
    rustix::fs::renameat_with(
        rustix::fs::CWD,
        staging.path(),
        rustix::fs::CWD,
        output,
        rustix::fs::RenameFlags::NOREPLACE,
    )?;
    File::open(parent)?.sync_all()?;
    let mut receipt = manifest.build;
    receipt.root_sha256 = hash(&body);
    receipt.output_bytes = body.len() + builder.directory_bytes + builder.cell_bytes;
    Ok((receipt, builder.replay.take().unwrap_or_default()))
}

/// Replay only from authenticated original inputs through the current builder.
/// Directory byte identity proves topology, prototype bits and cell IDs;
/// complete cell byte identity proves ordered memberships and SQ2/SQ8 bodies.
/// Missing originals or any mismatch refuses; no lossy boundary inference.
pub fn replay_primary(config: &BuildConfig, retained: &Prototype, scratch_parent: &Path) -> Result<PrimaryReplay> {
    let old = retained.source_identity();
    require(config.schema == old.schema && config.cell_rows == old.cell_rows
        && config.sample_rows == old.sample_rows && config.max_depth == old.max_depth,
        "retained capacity-v4 builder policy mismatch")?;
    for (a, b) in [(&config.generation, &old.generation), (&config.plane, &old.plane),
        (&config.canonical, &old.canonical), (&config.order, &old.order),
        (&config.records, &old.records), (&config.mean, &old.mean), (&config.sq8, &old.sq8)] {
        require(a.bytes == b.bytes && a.sha256 == b.sha256, "retained original input identity mismatch")?;
    }
    let scratch = tempfile::tempdir_in(scratch_parent)?;
    let output = scratch.path().join("replay");
    let (receipt, nodes) = build_captured(config, &output, true)?;
    let root: Manifest = serde_json::from_slice(&fs::read(output.join("manifest.json"))?)?;
    require(root.rows == retained.rows() && root.dimensions == retained.dimensions()
        && root.directory_bytes == retained.manifest.directory_bytes
        && root.directory_sha256 == retained.manifest.directory_sha256
        && serde_json::to_vec(&root.root_directory)? == serde_json::to_vec(&retained.manifest.root_directory)?
        && root.cell_bytes == retained.manifest.cell_bytes
        && root.seed == retained.manifest.seed
        && root.mean.iter().map(|v| v.to_bits()).eq(retained.manifest.mean.iter().map(|v| v.to_bits()))
        && root.low.iter().map(|v| v.to_bits()).eq(retained.manifest.low.iter().map(|v| v.to_bits()))
        && root.step.iter().map(|v| v.to_bits()).eq(retained.manifest.step.iter().map(|v| v.to_bits()))
        && receipt.cells == retained.manifest.build.cells
        && receipt.semantic_repairs == retained.manifest.build.semantic_repairs
        && receipt.geometry_fallbacks == retained.manifest.build.geometry_fallbacks, "primary replay topology/prototype/coefficient mismatch")?;
    let replay_cells = File::open(output.join("cells.bin"))?;
    for page in retained.directories.values() {
        for node in &page.children {
            if let Target::Cell { cell } = &node.target {
                let a = read_at(&retained.cells, cell.whole.offset, cell.whole.bytes)?;
                let b = read_at(&replay_cells, cell.whole.offset, cell.whole.bytes)?;
                require(hash(&a) == cell.whole.sha256 && a == b, "primary replay ordered bodies mismatch")?;
            }
        }
    }
    Ok(PrimaryReplay { logical_rows: root.rows, dimensions: root.dimensions, nodes })
}

/// Routing-only identity; no source/payload read and no exactly24 restriction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SelectedCell { pub cell_id: usize, pub primary: bool, pub distance_bits: u32 }

/// Actual local payload operations in one stage (metadata syscalls excluded).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ReadStats {
    /// Submitted logical positioned range reads, without remote retry claims.
    pub submitted_gets: usize,
    /// Requested bytes, including failed reads.
    pub requested_bytes: usize,
    /// Bytes whose exact length and SHA256 were accepted.
    pub verified_bytes: usize,
    /// Failed submitted reads; budget rejection submits no operation.
    pub failed_gets: usize,
}
/// Sequential payload dependencies following the already pinned research root.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FetchStage {
    /// Startup directory preload; successful resident query accounting stays zero.
    Directory,
    /// Complete contiguous cell payload, including overfetched SQ8 records.
    WholeCell,
    /// Bounded cell-local IDs and unchanged SQ2 records.
    Source,
    /// Selected colocated SQ8 blocks.
    Refinement,
}
/// One dependent fetch wave; reads in this local adapter are serial.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchWave {
    /// 1 follows the root load; each subsequent nonempty wave depends on prior work.
    pub dependency: usize,
    /// Payload stage.
    pub stage: FetchStage,
    /// Actual local logical range read count.
    pub submitted_gets: usize,
    /// Attempted payload bytes.
    pub requested_bytes: usize,
    /// Authenticated payload bytes.
    pub verified_bytes: usize,
    /// This adapter performs one local range read at a time.
    pub max_parallel_gets: usize,
}
/// Full query accounting, also returned on errors.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Accounting {
    /// Always zero in resident queries; preload is separately charged.
    pub directory: ReadStats,
    /// Combined SQ2/SQ8 cell reads; source/refinement reads are zero for WholeCell.
    pub whole_cell: ReadStats,
    /// Complete selected cell-source reads.
    pub source: ReadStats,
    /// Selected cell-local refinement reads.
    pub refinement: ReadStats,
    /// All dependent fetch waves; excludes the separately reported root open.
    pub waves: Vec<FetchWave>,
    /// Conservative per-query payload model including diagnostics.
    pub modeled_query_payload_bytes: usize,
}
impl Accounting {
    fn wave(&mut self, stage: FetchStage) -> usize {
        let index = self.waves.len();
        self.waves.push(FetchWave {
            dependency: index + 1,
            stage,
            submitted_gets: 0,
            requested_bytes: 0,
            verified_bytes: 0,
            max_parallel_gets: 1,
        });
        index
    }
    fn stats(&mut self, stage: FetchStage) -> &mut ReadStats {
        match stage {
            FetchStage::Directory => &mut self.directory,
            FetchStage::WholeCell => &mut self.whole_cell,
            FetchStage::Source => &mut self.source,
            FetchStage::Refinement => &mut self.refinement,
        }
    }
    fn fetch(
        &mut self,
        file: &File,
        span: &Span,
        stage: FetchStage,
        wave: usize,
        cap: (usize, usize),
    ) -> Result<Vec<u8>> {
        let stats = self.stats(stage);
        require(
            span.bytes > 0
                && valid_sha(&span.sha256)
                && stats.submitted_gets < cap.0
                && stats
                    .requested_bytes
                    .checked_add(span.bytes)
                    .is_some_and(|n| n <= cap.1),
            "query stage GET/byte admission",
        )?;
        stats.submitted_gets += 1;
        stats.requested_bytes += span.bytes;
        self.waves[wave].submitted_gets += 1;
        self.waves[wave].requested_bytes += span.bytes;
        let result = read_at(file, span.offset, span.bytes).and_then(|body| {
            require(hash(&body) == span.sha256, "range SHA256 mismatch")?;
            Ok(body)
        });
        match result {
            Ok(body) => {
                self.stats(stage).verified_bytes += body.len();
                self.waves[wave].verified_bytes += body.len();
                Ok(body)
            }
            Err(error) => {
                self.stats(stage).failed_gets += 1;
                Err(error)
            }
        }
    }
}

/// Explicit payload policy. Only WholeCell is exposed by the diagnostic runner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FetchPolicy {
    /// One exact contiguous read per selected cell, then local nomination/ranking.
    WholeCell,
    /// Two dependent payload waves, retained for synthetic parity comparisons.
    TwoStage,
}

/// Explicit query geometry and resource envelope; no hidden widening.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchOptions {
    /// Required explicit payload policy; never inferred from caps.
    pub fetch_policy: FetchPolicy,
    /// Narrow hierarchy beam (1..=64), reported as primary router coverage.
    pub primary_beam: usize,
    /// Independent wider beam (primary..=64), unioned for boundary coverage.
    pub boundary_beam: usize,
    /// Best max-SQ2-score blocks selected *within each cell* (1..=16).
    pub blocks_per_cell: usize,
    /// Maximum selected cells across the union of both routing beams (1..=128).
    pub max_cells: usize,
    /// Maximum total cell reads for WholeCell (1..=128).
    pub max_cell_gets: usize,
    /// Maximum total fetched SQ2 + all SQ8 payload for WholeCell.
    pub max_cell_bytes: usize,
    /// Maximum cell-source range reads for TwoStage.
    pub max_source_gets: usize,
    /// Maximum cell-source bytes for TwoStage.
    pub max_source_bytes: usize,
    /// Maximum local refinement-block reads for TwoStage.
    pub max_refinement_gets: usize,
    /// Maximum SQ8 payload bytes for TwoStage, including selected block tails.
    pub max_refinement_bytes: usize,
    /// Payload memory cap; root/runtime/allocator/OS cache are separate charges.
    pub max_query_payload_bytes: usize,
}
/// Fixed, routing-only interventions. Neither policy scores source/SQ8 records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NominationPolicy {
    /// Actual union of the existing independent primary8 and wider24 routes.
    Hierarchical8And24,
    /// All resident leaves ranked by unchanged squared-L2, retaining top24.
    GlobalTop24,
}
/// Source-range and scratch caps for the fixed routing-only probe.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NominationLimits {
    /// At most 24 authenticated source-range reads per policy/request.
    pub max_source_gets: usize,
    /// At most 16 MiB of source payload per policy/request.
    pub max_source_bytes: usize,
    /// Modeled transient payload, separate from admitted resident directories.
    pub max_query_payload_bytes: usize,
}
/// One selected leaf, with exact stored prototype bits and source-order roster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NominatedCell {
    /// Authenticated layout cell ID, never a source ordinal.
    pub cell_id: usize,
    /// Existing normalized-query squared-L2 to the unnormalized stored mean.
    pub distance: f32,
    /// Lossless representation of the distance for offline replay.
    pub distance_bits: u32,
    /// Membership in primary8 (global policy uses its first eight).
    pub primary: bool,
    /// Exact f32 bits of the stored arithmetic mean; never renormalized.
    pub prototype_bits: Vec<u32>,
    /// Concatenation position only, not a source-membership interval.
    pub first_row: usize,
    /// Exact source ordinal roster in stored row order.
    pub source_ids: Vec<i64>,
    /// Authenticated source range offset within cells.bin.
    pub source_offset: usize,
    /// Authenticated source range length.
    pub source_bytes: usize,
    /// Authenticated source range digest.
    pub source_sha256: String,
    /// Complete cell bytes for occupancy/fetch comparison; these are not read.
    pub whole_cell_bytes: usize,
}
/// Truth-free routing coverage, without local block nomination or refinement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NominationReceipt {
    /// Fixed selection policy.
    pub policy: NominationPolicy,
    /// Total authenticated resident leaves considered available to routing.
    pub resident_leaf_cells: usize,
    /// Conservative count of routing distance calls (exact for global ranking).
    pub routing_distance_evaluations_bound: usize,
    /// Distance-call bound multiplied by source dimensions.
    pub routing_coordinate_evaluations_bound: usize,
    /// Selected cells ordered by distance.total_cmp, then ascending cell ID.
    pub selected: Vec<NominatedCell>,
    /// Primary8 source ordinals, sorted unique.
    pub primary_ids: Vec<i64>,
    /// All 24 cells' source ordinals, sorted unique.
    pub covered_ids: Vec<i64>,
    /// Actual source reads and bounded query scratch; all other reads are zero.
    pub accounting: Accounting,
}

/// Measured stage wall/process CPU interval, excludes other stages.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StageTime {
    /// Wall elapsed nanoseconds in this local diagnostic adapter.
    pub wall_ns: u128,
    /// Whole-process CPU elapsed nanoseconds; not per-thread or remote CPU.
    pub process_cpu_ns: i128,
    /// Linux process RSS snapshot at stage end, not a stage-specific peak.
    pub rss_after_bytes: Option<u64>,
    /// Linux lifetime process high-water snapshot; independent peaks cannot add.
    pub process_high_water_bytes: Option<u64>,
}
fn cpu_ns() -> i128 {
    let time = rustix::time::clock_gettime(rustix::time::ClockId::ProcessCPUTime);
    i128::from(time.tv_sec) * 1_000_000_000 + i128::from(time.tv_nsec)
}
fn elapsed(start: (Instant, i128)) -> StageTime {
    let (rss_after_bytes, process_high_water_bytes) = process_memory_snapshot();
    StageTime {
        wall_ns: start.0.elapsed().as_nanos(),
        process_cpu_ns: cpu_ns() - start.1,
        rss_after_bytes,
        process_high_water_bytes,
    }
}
/// Linux RSS/lifetime-HWM snapshots; external monitoring must measure actual
/// per-stage/concurrent peaks and cgroup/OS-cache resource use.
pub fn process_memory_snapshot() -> (Option<u64>, Option<u64>) {
    let status = fs::read_to_string("/proc/self/status").unwrap_or_default();
    let field = |name: &str| {
        status.lines().find_map(|line| {
            line.strip_prefix(name)
                .and_then(|v| v.split_whitespace().next())
                .and_then(|v| v.parse::<u64>().ok())
                .and_then(|v| v.checked_mul(1024))
        })
    };
    (field("VmRSS:"), field("VmHWM:"))
}
/// Unchanged SQ8 arithmetic result; logical IDs remain source ordinals.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RankedRow {
    /// Row in the candidate's logical concatenation of cell refinement rows.
    pub ordinal: usize,
    /// Source ordinal stored unchanged in the SQ8 record.
    pub id: i64,
    /// Existing SQ8 squared-L2 score, tie broken by logical ID.
    pub score: f32,
}
/// Truth-free stage rosters, frozen before offline loss attribution.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SearchTrace {
    /// Primary-beam source IDs, sorted unique.
    pub primary_ids: Vec<i64>,
    /// Union of primary/wider-beam cell IDs' source rosters, sorted unique.
    pub covered_ids: Vec<i64>,
    /// Entire locally nominated refinement blocks' source IDs, sorted unique.
    pub nominated_ids: Vec<i64>,
    /// Final up-to-k SQ8 results; underfill is explicit.
    pub returned: Vec<RankedRow>,
    /// Resource charges and dependency waves.
    pub accounting: Accounting,
    /// Directory routing and boundary expansion wall/CPU.
    pub routing: StageTime,
    /// Query preparation, cell fetching and local SQ2 nomination wall/CPU.
    pub local_nomination: StageTime,
    /// Unchanged SQ8 ranking wall/CPU; includes refinement reads for TwoStage.
    pub final_ranking: StageTime,
}
/// Query rejection with every operation charged before failure.
#[derive(Debug)]
pub struct SearchFailure {
    /// Specific execution/admission error.
    pub message: String,
    /// Attempted and verified operations, including failed reads.
    pub accounting: Accounting,
}
impl std::fmt::Display for SearchFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}
impl Error for SearchFailure {}

/// Admitted full-directory payload model; this is source arithmetic, not RSS.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectoryAdmission {
    /// Actual complete encoded directory byte count from the authenticated root.
    pub encoded_bytes: usize,
    /// Parsed graph capacity allowance, including strings, node/refinement
    /// vectors, topology containers and root/codec/parser working storage.
    pub modeled_parsed_payload_bytes: usize,
    /// Encoded input plus parsed graph allowance coexist during startup.
    pub modeled_preload_peak_bytes: usize,
    /// Tracked actual Vec/String owned capacities after parse; map/container
    /// overhead remains in the separately modeled topology/root allowance.
    pub parsed_owned_capacity_bytes: usize,
}
fn directory_admission(encoded: usize, pages: usize, cells: usize) -> Result<DirectoryAdmission> {
    let parsed = encoded
        .checked_mul(4)
        .and_then(|n| n.checked_add(pages.checked_add(cells)?.checked_mul(256)?))
        .and_then(|n| n.checked_add(8 * ROOT_CAP))
        .ok_or("directory memory overflow")?;
    Ok(DirectoryAdmission {
        encoded_bytes: encoded,
        modeled_parsed_payload_bytes: parsed,
        modeled_preload_peak_bytes: encoded
            .checked_add(parsed)
            .ok_or("directory memory overflow")?,
        parsed_owned_capacity_bytes: 0,
    })
}
fn manifest_directory_admission(manifest: &Manifest, bytes: usize) -> Result<DirectoryAdmission> {
    require(manifest.seed == NATIVE_CODEC_SEED, "current fixed SQ2 seed")?;
    require(
        manifest.schema == SCHEMA
            && manifest.input.schema == BUILD_SCHEMA
            && (1..=100_000).contains(&manifest.rows)
            && (1..=768).contains(&manifest.dimensions)
            && (1..=512).contains(&manifest.input.cell_rows)
            && (1..=32).contains(&manifest.input.max_depth)
            && manifest.mean.len() == manifest.dimensions
            && manifest.mean.iter().all(|v| v.is_finite())
            && manifest.low.len() == manifest.dimensions
            && manifest.step.len() == manifest.dimensions
            && manifest.low.iter().all(|v| v.is_finite())
            && manifest.step.iter().all(|v| v.is_finite() && *v > 0.)
            && in_bounds(&manifest.root_directory, manifest.directory_bytes, PAGE_CAP)
            && valid_sha(&manifest.directory_sha256)
            && (1..=manifest.rows).contains(&manifest.build.cells)
            && (1..=manifest.rows).contains(&manifest.build.directories)
            && manifest.build.directories.checked_mul(PAGE_CAP).is_some_and(|v| manifest.directory_bytes <= v)
            && manifest.directory_bytes.checked_add(manifest.cell_bytes)
                .and_then(|n| n.checked_add(bytes)).is_some_and(|n| n <= manifest.input.max_output_bytes),
        "research root schema/geometry/resource binding",
    )?;
    directory_admission(manifest.directory_bytes, manifest.build.directories, manifest.build.cells)
}

/// Authenticated retained-root metadata only. Reading this descriptor never
/// opens the directory or cell body. Its actual directory digest/length drives
/// overlap admission before the resident router is allocated.
#[derive(Debug, Clone, Serialize)]
pub struct PrimaryLayoutMetadata {
    pub root: Artifact,
    pub directory: Artifact,
    pub cell_file: PathBuf,
    pub cell_bytes: usize,
    pub rows: usize,
    pub dimensions: usize,
    pub cells: usize,
    pub directory_admission: DirectoryAdmission,
    pub modeled_metadata_payload_bytes: usize,
}
fn page_owned_capacity(page: &Directory) -> usize {
    page.children.capacity() * std::mem::size_of::<Node>()
        + page
            .children
            .iter()
            .map(|node| {
                node.prototype.capacity() * std::mem::size_of::<f32>()
                    + match &node.target {
                        Target::Directory { span } => span.sha256.capacity(),
                        Target::Cell { cell } => {
                            cell.whole.sha256.capacity()
                                + cell.source.sha256.capacity()
                                + cell.refinement.capacity() * std::mem::size_of::<Span>()
                                + cell
                                    .refinement
                                    .iter()
                                    .map(|span| span.sha256.capacity())
                                    .sum::<usize>()
                        }
                    }
            })
            .sum::<usize>()
}

/// Root and the complete parsed directory are pinned; queries have no directory
/// file handle/path, lazy loader, or shared directory cache fallback.
pub struct Prototype {
    admitted_root_sha256: String,
    manifest: Manifest,
    directories: BTreeMap<usize, Directory>,
    cells: File,
    codec: RotatedTwoBitCodec,
    /// Actual root payload load, separate from every query's directory waves.
    pub startup: ReadStats,
    /// One complete authenticated directory read during startup, never hidden
    /// among query operations. Root open is separately reported above.
    pub startup_directory: ReadStats,
    /// Full resident graph and startup coexistence resource charges.
    pub directory_admission: DirectoryAdmission,
}
fn regular_file(path: &Path, bytes: usize) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(rustix::fs::OFlags::NONBLOCK.bits() as i32)
        .open(path)?;
    require(
        file.metadata()?.is_file() && file.metadata()?.len() == bytes as u64,
        "prototype file type/exact size",
    )?;
    Ok(file)
}
fn in_bounds(span: &Span, bytes: usize, cap: usize) -> bool {
    span.bytes > 0
        && span.bytes <= cap
        && valid_sha(&span.sha256)
        && span
            .offset
            .checked_add(span.bytes)
            .is_some_and(|n| n <= bytes)
}
impl Prototype {
    /// Parse and authenticate only the small retained root after admitting its
    /// root/parser allowance. No directory or payload file is opened here.
    pub fn admit_root_metadata(root: &Artifact, max_metadata_payload_bytes: usize) -> Result<PrimaryLayoutMetadata> {
        let modeled = root.bytes.checked_mul(8).ok_or("retained root metadata overflow")?;
        require(root.bytes > 0 && root.bytes <= ROOT_CAP && modeled <= max_metadata_payload_bytes,
            "retained root metadata admission")?;
        require(root.path.file_name().is_some_and(|v| v == "manifest.json"), "retained root filename")?;
        let body = read_source_probe_artifact(root, ROOT_CAP)?;
        let manifest: Manifest = serde_json::from_slice(&body)?;
        let admission = manifest_directory_admission(&manifest, body.len())?;
        let parent = root.path.parent().ok_or("retained root parent")?;
        Ok(PrimaryLayoutMetadata { root: root.clone(), directory: Artifact { path: parent.join("directories.bin"),
            bytes: manifest.directory_bytes, sha256: manifest.directory_sha256 }, cell_file: parent.join("cells.bin"),
            cell_bytes: manifest.cell_bytes, rows: manifest.rows, dimensions: manifest.dimensions, cells: manifest.build.cells,
            directory_admission: admission, modeled_metadata_payload_bytes: modeled })
    }
    /// Admit and authenticate the complete 100k research directory at startup.
    /// The cap is checked before opening/reading/allocating the directory body.
    /// Full-directory residency grows with source size; N>100k is unsupported.
    pub fn open(
        path: &Path,
        root_sha256: &str,
        max_resident_directory_payload_bytes: usize,
    ) -> Result<Self> {
        Self::open_inner(
            path,
            root_sha256,
            max_resident_directory_payload_bytes,
            None,
            None,
        )
    }
    /// New-mode entrypoint: exact trusted root descriptor and regular
    /// NOFOLLOW/NONBLOCK root/directory/cell inputs; legacy open is unchanged.
    pub fn open_for_source_probes(root: &Artifact, cap: usize) -> Result<Self> {
        Self::open_for_source_probes_traced(root, cap, &mut SourceProbeLayoutStartup::default())
    }
    /// Preserve each actual startup operation on failure as well as success.
    /// The supplied snapshot is reset for this one root/directory open attempt.
    pub fn open_for_source_probes_traced(
        root: &Artifact,
        cap: usize,
        startup: &mut SourceProbeLayoutStartup,
    ) -> Result<Self> {
        *startup = SourceProbeLayoutStartup::default();
        let started = (Instant::now(), cpu_ns());
        let result = (|| {
            require(
                root.path.file_name().is_some_and(|v| v == "manifest.json"),
                "probe root filename",
            )?;
            Self::open_inner(
                root.path.parent().ok_or("probe root parent")?,
                &root.sha256,
                cap,
                Some(root),
                Some(&mut *startup),
            )
        })();
        startup.complete = probe_elapsed(started);
        result
    }
    fn open_inner(
        path: &Path,
        root_sha256: &str,
        max_resident_directory_payload_bytes: usize,
        secure_root: Option<&Artifact>,
        mut startup_trace: Option<&mut SourceProbeLayoutStartup>,
    ) -> Result<Self> {
        let bytes = match secure_root {
            Some(root) => root.bytes,
            None => usize::try_from(fs::metadata(path.join("manifest.json"))?.len())?,
        };
        let descriptor = Artifact {
            path: path.join("manifest.json"),
            bytes,
            sha256: root_sha256.into(),
        };
        let body = if let Some(startup) = startup_trace.as_deref_mut() {
            read_source_probe_artifact_tracked(&descriptor, ROOT_CAP, &mut startup.root)?
        } else if secure_root.is_some() {
            read_source_probe_artifact(&descriptor, ROOT_CAP)?
        } else {
            descriptor.read(ROOT_CAP)?
        };
        let manifest: Manifest = serde_json::from_slice(&body)?;
        let admission = manifest_directory_admission(&manifest, body.len())?;
        if let Some(startup) = startup_trace.as_deref_mut() {
            startup.admission = Some(admission.clone());
        }
        require(
            max_resident_directory_payload_bytes <= 512 * 1024 * 1024
                && admission.modeled_preload_peak_bytes <= max_resident_directory_payload_bytes,
            "resident directory payload admission",
        )?;
        let codec = RotatedTwoBitCodec::new(&manifest.mean, manifest.seed)?;
        let open_file = if secure_root.is_some() {
            probe_file
        } else {
            regular_file
        };
        let directories = open_file(&path.join("directories.bin"), manifest.directory_bytes)?;
        let cells = open_file(&path.join("cells.bin"), manifest.cell_bytes)?;
        let directory_body = if let Some(startup) = startup_trace.as_deref_mut() {
            startup.directory.submitted_gets = 1;
            startup.directory.requested_bytes = manifest.directory_bytes;
            startup.directory.failed_gets = 1;
            let body = read_at(&directories, 0, manifest.directory_bytes)?;
            require(
                hash(&body) == manifest.directory_sha256,
                "whole directory SHA256",
            )?;
            startup.directory.verified_bytes = body.len();
            startup.directory.failed_gets = 0;
            body
        } else {
            let body = read_at(&directories, 0, manifest.directory_bytes)?;
            require(
                hash(&body) == manifest.directory_sha256,
                "whole directory SHA256",
            )?;
            body
        };
        let mut prototype = Self {
            admitted_root_sha256: root_sha256.into(),
            manifest,
            directories: BTreeMap::new(),
            cells,
            codec,
            startup: ReadStats {
                submitted_gets: 1,
                requested_bytes: body.len(),
                verified_bytes: body.len(),
                failed_gets: 0,
            },
            startup_directory: ReadStats {
                submitted_gets: 1,
                requested_bytes: directory_body.len(),
                verified_bytes: directory_body.len(),
                failed_gets: 0,
            },
            directory_admission: admission,
        };
        prototype.preload(&directory_body)?;
        if let Some(startup) = startup_trace {
            startup.admission = Some(prototype.directory_admission.clone());
        }
        Ok(prototype)
    }
    fn preload(&mut self, body: &[u8]) -> Result<()> {
        let mut pending = vec![(self.manifest.root_directory.clone(), self.rows())];
        let mut page_spans = BTreeMap::new();
        let mut cells = BTreeMap::new();
        let mut parsed_capacity = 0;
        while let Some((span, rows)) = pending.pop() {
            require(
                in_bounds(&span, body.len(), PAGE_CAP)
                    && !page_spans.contains_key(&span.offset)
                    && page_spans.len() < self.manifest.build.directories,
                "resident directory topology",
            )?;
            let encoded = &body[span.offset..span.offset + span.bytes];
            require(hash(encoded) == span.sha256, "directory page SHA256")?;
            let page: Directory = serde_json::from_slice(encoded)?;
            require((1..=2).contains(&page.children.len()), "directory fanout")?;
            // Builder emits canonical JSON with two children/page. f32 vectors
            // need at most twice encoded-coordinate capacity; node/Span vectors
            // and 64-byte SHA strings fit the remaining 4B allowance. Track all
            // owned Vec/String capacities as a falsifier of this model. The
            // 256*(pages+cells) plus 8*ROOT_CAP term covers topology maps, the
            // traversal stack, codec/root, and one page's parser scratch.
            parsed_capacity += page_owned_capacity(&page);
            require(
                parsed_capacity <= 4 * body.len(),
                "parsed directory capacity model",
            )?;
            for node in &page.children {
                self.validate_node(node, span.offset)?;
                match &node.target {
                    Target::Directory { span } => pending.push((span.clone(), node.rows)),
                    Target::Cell { cell } => {
                        require(
                            cells
                                .insert(
                                    cell.id,
                                    (
                                        cell.first_row,
                                        node.rows,
                                        cell.source.offset,
                                        cell.refinement
                                            .last()
                                            .map(|span| span.offset + span.bytes)
                                            .ok_or("empty refinement")?,
                                    ),
                                )
                                .is_none(),
                            "duplicate resident cell",
                        )?;
                    }
                }
            }
            require(
                page.children.iter().map(|node| node.rows).sum::<usize>() == rows,
                "directory row coverage",
            )?;
            page_spans.insert(span.offset, span.bytes);
            self.directories.insert(span.offset, page);
        }
        require(
            page_spans.len() == self.manifest.build.directories
                && cells.len() == self.manifest.build.cells,
            "complete resident directory/cell counts",
        )?;
        let mut end = 0;
        for (offset, bytes) in page_spans {
            require(offset == end, "complete directory byte coverage")?;
            end += bytes;
        }
        require(end == body.len(), "directory trailing bytes")?;
        let mut row = 0;
        let mut byte = 0;
        for (expected, (id, (first, rows, start, end))) in cells.into_iter().enumerate() {
            require(
                id == expected && first == row && start == byte,
                "complete resident cell layout",
            )?;
            row += rows;
            byte = end;
        }
        require(
            row == self.rows() && byte == self.manifest.cell_bytes,
            "resident source/refinement byte coverage",
        )?;
        self.directory_admission.parsed_owned_capacity_bytes = parsed_capacity;
        Ok(())
    }
    /// Source row count authenticated in the research root.
    pub fn rows(&self) -> usize {
        self.manifest.rows
    }
    /// Original vector dimensions.
    pub fn dimensions(&self) -> usize {
        self.manifest.dimensions
    }
    fn validate_node(&self, node: &Node, parent_offset: usize) -> Result<()> {
        require(
            node.rows > 0
                && node.rows <= self.rows()
                && node.prototype.len() == self.dimensions()
                && node.prototype.iter().all(|v| v.is_finite()),
            "directory child geometry/prototype",
        )?;
        match &node.target {
            Target::Directory { span } => require(
                in_bounds(span, self.manifest.directory_bytes, PAGE_CAP)
                    && span
                        .offset
                        .checked_add(span.bytes)
                        .is_some_and(|n| n <= parent_offset),
                "acyclic directory span",
            )?,
            Target::Cell { cell } => {
                require(
                    node.rows <= self.manifest.input.cell_rows
                        && cell.id < self.manifest.build.cells
                        && cell
                            .first_row
                            .checked_add(node.rows)
                            .is_some_and(|n| n <= self.rows())
                        && in_bounds(
                            &cell.source,
                            self.manifest.cell_bytes,
                            node.rows * (self.codec.record_bytes() + 8),
                        )
                        && cell.source.bytes == node.rows * (self.codec.record_bytes() + 8)
                        && in_bounds(
                            &cell.whole,
                            self.manifest.cell_bytes,
                            node.rows * (self.codec.record_bytes() + self.dimensions() + 20),
                        )
                        && cell.whole.offset == cell.source.offset
                        && cell.whole.bytes
                            == node.rows * (self.codec.record_bytes() + self.dimensions() + 20)
                        && cell.refinement.len() == node.rows.div_ceil(BLOCK_ROWS),
                    "cell source/block geometry",
                )?;
                let mut end = cell.source.offset + cell.source.bytes;
                for (block, span) in cell.refinement.iter().enumerate() {
                    let expected =
                        (node.rows - block * BLOCK_ROWS).min(BLOCK_ROWS) * (self.dimensions() + 12);
                    require(
                        in_bounds(span, self.manifest.cell_bytes, expected)
                            && span.bytes == expected
                            && span.offset == end,
                        "colocated refinement geometry",
                    )?;
                    end += span.bytes;
                }
                require(
                    end == cell.whole.offset + cell.whole.bytes,
                    "whole-cell exact extent",
                )?;
            }
        }
        Ok(())
    }
    fn route(&self, query: &[f32], options: SearchOptions) -> Result<(Vec<Node>, Vec<Node>)> {
        let root = Node {
            rows: self.rows(),
            prototype: vec![0.; self.dimensions()],
            target: Target::Directory {
                span: self.manifest.root_directory.clone(),
            },
        };
        let mut primary = vec![root.clone()];
        let mut wider = vec![root];
        for _ in 0..=self.manifest.input.max_depth {
            if primary
                .iter()
                .chain(&wider)
                .all(|node| matches!(&node.target, Target::Cell { .. }))
            {
                return Ok((primary, wider));
            }
            let expand = |frontier: &[Node], beam: usize| -> Result<Vec<Node>> {
                let mut ranked = Vec::new();
                for node in frontier {
                    let children = match &node.target {
                        Target::Directory { span } => self
                            .directories
                            .get(&span.offset)
                            .ok_or("admitted resident directory page missing")?
                            .children
                            .as_slice(),
                        Target::Cell { .. } => std::slice::from_ref(node),
                    };
                    for child in children {
                        let tie = match &child.target {
                            Target::Directory { span } => (0, span.offset),
                            Target::Cell { cell } => (1, cell.id),
                        };
                        ranked.push((
                            VectorMetric::SquaredEuclidean.distance(query, &child.prototype)?,
                            tie,
                            child.clone(),
                        ));
                    }
                }
                ranked.sort_unstable_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
                Ok(ranked.into_iter().take(beam).map(|v| v.2).collect())
            };
            primary = expand(&primary, options.primary_beam)?;
            wider = expand(&wider, options.boundary_beam)?;
        }
        Err("routing exceeded directory depth cap".into())
    }
    /// Authenticated original build/source descriptors, without opening inputs.
    pub fn source_identity(&self) -> &BuildConfig {
        &self.manifest.input
    }

    /// Actual unchanged independent primary8/wider24 union, bounded by32.
    pub fn select_cells(&self, query: &[f32]) -> Result<Vec<SelectedCell>> {
        require(query.len() == self.dimensions(), "selection query geometry")?;
        let query = cosine_vector(query)?;
        let options = SearchOptions { fetch_policy: FetchPolicy::WholeCell,
            primary_beam: 8, boundary_beam: 24, blocks_per_cell: 16,
            max_cells: 32, max_cell_gets: 32, max_cell_bytes: 16 * 1024 * 1024,
            max_source_gets: 32, max_source_bytes: 16 * 1024 * 1024,
            max_refinement_gets: 32, max_refinement_bytes: 16 * 1024 * 1024,
            max_query_payload_bytes: 512 * 1024 * 1024 };
        let (primary, wider) = self.route(&query, options)?;
        let primary_ids = primary.iter().map(|n| match &n.target {
            Target::Cell { cell } => Ok(cell.id), _ => Err("unresolved selection")
        }).collect::<std::result::Result<BTreeSet<_>, _>>()?;
        let mut union = BTreeMap::new();
        for node in primary.into_iter().chain(wider) {
            let Target::Cell { cell } = &node.target else { return Err("unresolved selection".into()); };
            union.insert(cell.id, SelectedCell { cell_id: cell.id, primary: primary_ids.contains(&cell.id),
                distance_bits: VectorMetric::SquaredEuclidean.distance(&query, &node.prototype)?.to_bits() });
        }
        require(!union.is_empty() && union.len() <= 32, "selection union cap")?;
        let mut selected = union.into_values().collect::<Vec<_>>();
        selected.sort_by(|a, b| f32::from_bits(a.distance_bits).total_cmp(&f32::from_bits(b.distance_bits))
            .then(a.cell_id.cmp(&b.cell_id)));
        Ok(selected)
    }

    /// Exact retained coefficient bits for unchanged SQ8 scoring.
    pub fn sq8_coefficients(&self) -> (&[f32], &[f32]) { (&self.manifest.low, &self.manifest.step) }

    /// Authenticate a complete retained leaf and return its ordered unchanged
    /// SQ8 bodies. Used only during overlap build/open admission, not routing.
    pub fn primary_cell_sq8(&self, id: usize) -> Result<Vec<u8>> {
        for directory in self.directories.values() {
            for node in &directory.children {
                if let Target::Cell { cell } = &node.target {
                    if cell.id == id {
                        let bytes = read_at(&self.cells, cell.whole.offset, cell.whole.bytes)?;
                        require(hash(&bytes) == cell.whole.sha256, "retained whole-cell authentication")?;
                        let records = &bytes[cell.source.bytes..];
                        for (a, b) in bytes[..cell.source.bytes].chunks_exact(self.codec.record_bytes() + 8)
                            .zip(records.chunks_exact(self.dimensions() + 12)) {
                            require(a[..8] == b[..8], "retained primary source/SQ8 ID")?;
                        }
                        return Ok(records.to_vec());
                    }
                }
            }
        }
        Err("retained cell ID absent".into())
    }

    fn nomination_inner(
        &self,
        query: &[f32],
        policy: NominationPolicy,
        limits: NominationLimits,
        accounting: &mut Accounting,
    ) -> Result<NominationReceipt> {
        require(
            query.len() == self.dimensions()
                && (1..=24).contains(&limits.max_source_gets)
                && (1..=16 * 1024 * 1024).contains(&limits.max_source_bytes)
                && limits.max_query_payload_bytes <= 512 * 1024 * 1024,
            "nomination geometry/limits",
        )?;
        // Existing route clones only bounded frontiers. Global ranking holds at
        // most 25 score/ID pairs, never a copy of all resident leaf prototypes.
        // Include output rosters/prototype bits, BTreeSet IDs, and source bytes.
        let routing_scratch = 8 * (8 + 24)
            * (self.dimensions() * 4
                + self.manifest.input.cell_rows.div_ceil(BLOCK_ROWS) * 128 + 512);
        let payload = routing_scratch + self.dimensions() * 16
            + 24 * self.manifest.input.cell_rows * 128
            + 2 * limits.max_source_bytes + 65536;
        accounting.modeled_query_payload_bytes = payload;
        require(payload <= limits.max_query_payload_bytes, "nomination payload admission")?;
        let normalized = cosine_vector(query)?;
        let mut primary_cells = BTreeSet::new();
        let mut ranked = Vec::with_capacity(25);
        let mut leaf_count = 0;
        match policy {
            NominationPolicy::Hierarchical8And24 => {
                // Only beams are consulted by route; search configuration and
                // its existing public behavior are deliberately unchanged.
                let options = SearchOptions {
                    fetch_policy: FetchPolicy::TwoStage,
                    primary_beam: 8, boundary_beam: 24, blocks_per_cell: 4,
                    max_cells: 24, max_cell_gets: 24, max_cell_bytes: 0,
                    max_source_gets: limits.max_source_gets,
                    max_source_bytes: limits.max_source_bytes,
                    max_refinement_gets: 1, max_refinement_bytes: 0,
                    max_query_payload_bytes: limits.max_query_payload_bytes,
                };
                let (primary, wider) = self.route(&normalized, options)?;
                for node in &primary {
                    let Target::Cell { cell } = &node.target else {
                        return Err("unresolved primary nomination frontier".into());
                    };
                    primary_cells.insert(cell.id);
                }
                let mut selected = BTreeMap::new();
                for node in primary.into_iter().chain(wider) {
                    let Target::Cell { cell } = &node.target else {
                        return Err("unresolved nomination frontier".into());
                    };
                    selected.insert(cell.id, node);
                }
                // Actual union cardinality is checked, never silently truncated.
                require(selected.len() == 24 && primary_cells.len() == 8,
                    "nomination requires actual hierarchy union24/primary8")?;
                for (id, node) in selected {
                    ranked.push((VectorMetric::SquaredEuclidean.distance(
                        &normalized, &node.prototype)?, id));
                }
                leaf_count = self.manifest.build.cells;
            }
            NominationPolicy::GlobalTop24 => {
                for page in self.directories.values() {
                    for node in &page.children {
                        if let Target::Cell { cell } = &node.target {
                            leaf_count += 1;
                            let distance = VectorMetric::SquaredEuclidean.distance(
                                &normalized, &node.prototype)?;
                            require(distance.is_finite(), "nomination finite leaf distance")?;
                            ranked.push((distance, cell.id));
                            ranked.sort_unstable_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
                            ranked.truncate(24);
                        }
                    }
                }
                require(leaf_count == self.manifest.build.cells && ranked.len() == 24,
                    "nomination requires complete resident leaves/top24")?;
            }
        }
        ranked.sort_unstable_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
        require(ranked.iter().all(|v| v.0.is_finite()), "nomination finite distances")?;
        if policy == NominationPolicy::GlobalTop24 {
            primary_cells.extend(ranked.iter().take(8).map(|v| v.1));
        }
        // Reference only the selected resident leaves, in deterministic rank order.
        let selected = ranked.iter().map(|&(distance, id)| {
            let node = self.directories.values().flat_map(|page| &page.children)
                .find(|node| matches!(&node.target, Target::Cell { cell } if cell.id == id))
                .ok_or("selected resident leaf missing")?;
            Ok((distance, node))
        }).collect::<Result<Vec<_>>>()?;
        let source_bytes = selected.iter().map(|(_, node)| match &node.target {
            Target::Cell { cell } => cell.source.bytes,
            _ => 0,
        }).sum::<usize>();
        require(selected.len() <= limits.max_source_gets && source_bytes <= limits.max_source_bytes,
            "nomination selected source batch admission")?;
        let routing_distance_evaluations_bound = match policy {
            NominationPolicy::Hierarchical8And24 => 2 * (8 + 24) * (self.manifest.input.max_depth + 1) + 24,
            NominationPolicy::GlobalTop24 => leaf_count,
        };
        let mut receipt = NominationReceipt {
            policy, resident_leaf_cells: leaf_count,
            routing_distance_evaluations_bound,
            routing_coordinate_evaluations_bound: routing_distance_evaluations_bound * self.dimensions(),
            selected: Vec::with_capacity(24),
            primary_ids: Vec::new(), covered_ids: Vec::new(), accounting: Accounting::default(),
        };
        let mut primary_ids = BTreeSet::new();
        let mut covered_ids = BTreeSet::new();
        let wave = accounting.wave(FetchStage::Source);
        for (distance, node) in selected {
            let Target::Cell { cell } = &node.target else { unreachable!() };
            let body = accounting.fetch(&self.cells, &cell.source, FetchStage::Source, wave,
                (limits.max_source_gets, limits.max_source_bytes))?;
            let mut source_ids = Vec::with_capacity(node.rows);
            for record in body.chunks_exact(8 + self.codec.record_bytes()) {
                let id = i64::from_le_bytes(record[..8].try_into()?);
                require(id >= 0 && (id as usize) < self.rows() && covered_ids.insert(id),
                    "nomination source ordinal roster/duplicate ID")?;
                source_ids.push(id);
            }
            require(source_ids.len() == node.rows, "nomination exact source roster")?;
            let primary = primary_cells.contains(&cell.id);
            if primary { primary_ids.extend(source_ids.iter().copied()); }
            receipt.selected.push(NominatedCell {
                cell_id: cell.id, distance, distance_bits: distance.to_bits(), primary,
                prototype_bits: node.prototype.iter().map(|v| v.to_bits()).collect(),
                first_row: cell.first_row, source_ids, source_offset: cell.source.offset,
                source_bytes: cell.source.bytes, source_sha256: cell.source.sha256.clone(),
                whole_cell_bytes: cell.whole.bytes,
            });
        }
        receipt.primary_ids = primary_ids.into_iter().collect();
        receipt.covered_ids = covered_ids.into_iter().collect();
        Ok(receipt)
    }

    /// Fixed truth-free routing probe on an admitted layout. Rejects underfilled
    /// selections and non-24 hierarchy unions; never prepares/scores SQ2 or SQ8.
    pub fn nominate(
        &self,
        query: &[f32],
        policy: NominationPolicy,
        limits: NominationLimits,
    ) -> std::result::Result<NominationReceipt, SearchFailure> {
        let mut accounting = Accounting::default();
        match self.nomination_inner(query, policy, limits, &mut accounting) {
            Ok(mut receipt) => { receipt.accounting = accounting; Ok(receipt) }
            Err(error) => Err(SearchFailure { message: error.to_string(), accounting }),
        }
    }

    fn query_inner(
        &self,
        query: &[f32],
        top_k: usize,
        options: SearchOptions,
        accounting: &mut Accounting,
    ) -> Result<SearchTrace> {
        require(
            query.len() == self.dimensions()
                && top_k > 0
                && top_k <= self.rows()
                && (1..=64).contains(&options.primary_beam)
                && (options.primary_beam..=64).contains(&options.boundary_beam)
                && (1..=16).contains(&options.blocks_per_cell)
                && (1..=128).contains(&options.max_cells)
                && (1..=128).contains(&options.max_cell_gets)
                && (1..=128).contains(&options.max_source_gets)
                && (1..=2048).contains(&options.max_refinement_gets)
                && options.max_query_payload_bytes <= 512 * 1024 * 1024,
            "query geometry/limits",
        )?;
        let frontier_cells = options.primary_beam + options.boundary_beam;
        let rows = options.max_cells * self.manifest.input.cell_rows;
        // Root and full directory were admitted once at open. Only cloned
        // routing frontiers, diagnostic rosters and actual response/rank buffers
        // are multiplied by concurrent queries; host/runtime charges are external.
        let routing_scratch = 8
            * frontier_cells
            * (self.dimensions() * 4
                + self.manifest.input.cell_rows.div_ceil(BLOCK_ROWS) * 128
                + 512);
        let responses = match options.fetch_policy {
            FetchPolicy::WholeCell => options.max_cell_bytes.checked_mul(3),
            FetchPolicy::TwoStage => options
                .max_source_bytes
                .checked_mul(2)
                .and_then(|n| n.checked_add(options.max_refinement_bytes.checked_mul(3)?)),
        }
        .ok_or("query memory overflow")?;
        let payload = (routing_scratch + 400_000 + rows * 512)
            .checked_add(responses)
            .ok_or("query memory overflow")?;
        require(
            payload <= options.max_query_payload_bytes,
            "query payload/scratch admission",
        )?;
        accounting.modeled_query_payload_bytes = payload;
        let normalized_query = cosine_vector(query)?;
        let started = (Instant::now(), cpu_ns());
        let (primary, wider) = self.route(&normalized_query, options)?;
        let routing = elapsed(started);
        let primary_cells = primary
            .iter()
            .filter_map(|n| match &n.target {
                Target::Cell { cell } => Some(cell.id),
                _ => None,
            })
            .collect::<BTreeSet<_>>();
        let mut selected = BTreeMap::new();
        for node in primary.into_iter().chain(wider) {
            match &node.target {
                Target::Cell { cell } => {
                    selected.insert(cell.id, node);
                }
                _ => return Err("unresolved directory frontier".into()),
            }
        }
        let (stage, fetch_cap) = match options.fetch_policy {
            FetchPolicy::WholeCell => (
                FetchStage::WholeCell,
                (options.max_cell_gets, options.max_cell_bytes),
            ),
            FetchPolicy::TwoStage => (
                FetchStage::Source,
                (options.max_source_gets, options.max_source_bytes),
            ),
        };
        require(
            selected.len() <= options.max_cells
                && selected.len() <= fetch_cap.0
                && selected
                    .values()
                    .map(|n| match &n.target {
                        Target::Cell { cell } => match options.fetch_policy {
                            FetchPolicy::WholeCell => cell.whole.bytes,
                            FetchPolicy::TwoStage => cell.source.bytes,
                        },
                        _ => 0,
                    })
                    .sum::<usize>()
                    <= fetch_cap.1,
            "selected cell batch admission",
        )?;
        let started = (Instant::now(), cpu_ns());
        // Match native serving: preserve original f32 input for SQ2 preparation;
        // f32 prenormalization can perturb lookup scores and block ties.
        let prepared = self.codec.prepare_query(query, 400_000)?;
        let mut primary_ids = BTreeSet::new();
        let mut covered_ids = BTreeSet::new();
        let mut nominated_ids = BTreeSet::new();
        let mut refinements = Vec::new();
        let mut whole_bodies = BTreeMap::new();
        let cell_wave = accounting.wave(stage);
        for node in selected.values() {
            let Target::Cell { cell } = &node.target else {
                unreachable!()
            };
            let span = match options.fetch_policy {
                FetchPolicy::WholeCell => &cell.whole,
                FetchPolicy::TwoStage => &cell.source,
            };
            let body = accounting.fetch(&self.cells, span, stage, cell_wave, fetch_cap)?;
            let source = &body[..cell.source.bytes];
            if options.fetch_policy == FetchPolicy::WholeCell {
                require(
                    hash(source) == cell.source.sha256,
                    "cell source SHA256 mismatch",
                )?;
            }
            let mut ids = Vec::with_capacity(node.rows);
            let mut maxima = vec![f64::NEG_INFINITY; node.rows.div_ceil(BLOCK_ROWS)];
            for (row, record) in source
                .chunks_exact(8 + self.codec.record_bytes())
                .enumerate()
            {
                let id = i64::from_le_bytes(record[..8].try_into()?);
                require(
                    id >= 0 && (id as usize) < self.rows() && covered_ids.insert(id),
                    "cell ordinal roster/duplicate ID",
                )?;
                if primary_cells.contains(&cell.id) {
                    primary_ids.insert(id);
                }
                ids.push(id);
                maxima[row / BLOCK_ROWS] =
                    maxima[row / BLOCK_ROWS].max(prepared.score(&record[8..])?);
            }
            let mut blocks = (0..maxima.len()).collect::<Vec<_>>();
            blocks.sort_unstable_by(|&a, &b| maxima[b].total_cmp(&maxima[a]).then(a.cmp(&b)));
            for block in blocks.into_iter().take(options.blocks_per_cell) {
                let expected =
                    ids[block * BLOCK_ROWS..((block + 1) * BLOCK_ROWS).min(ids.len())].to_vec();
                nominated_ids.extend(expected.iter().copied());
                refinements.push((
                    cell.first_row + block * BLOCK_ROWS,
                    cell.refinement[block].clone(),
                    expected,
                    cell.whole.offset,
                ));
            }
            if options.fetch_policy == FetchPolicy::WholeCell {
                whole_bodies.insert(cell.whole.offset, body);
            }
        }
        let local_nomination = elapsed(started);
        if options.fetch_policy == FetchPolicy::TwoStage {
            require(
                refinements.len() <= options.max_refinement_gets
                    && refinements.iter().map(|v| v.1.bytes).sum::<usize>()
                        <= options.max_refinement_bytes,
                "refinement batch admission",
            )?;
        }
        refinements.sort_unstable_by_key(|v| v.0);
        let started = (Instant::now(), cpu_ns());
        let mut bodies = Vec::with_capacity(refinements.len());
        if options.fetch_policy == FetchPolicy::TwoStage {
            let wave = accounting.wave(FetchStage::Refinement);
            for (_, span, expected, _) in &refinements {
                let body = accounting.fetch(
                    &self.cells,
                    span,
                    FetchStage::Refinement,
                    wave,
                    (options.max_refinement_gets, options.max_refinement_bytes),
                )?;
                require(
                    body.chunks_exact(self.dimensions() + 12)
                        .zip(expected)
                        .all(|(row, id)| i64::from_le_bytes(row[..8].try_into().unwrap()) == *id),
                    "refinement/source ordinal binding",
                )?;
                bodies.push(body);
            }
        }
        let mut ranges = Vec::with_capacity(refinements.len());
        for (index, (first, span, expected, whole_offset)) in refinements.iter().enumerate() {
            let body = match options.fetch_policy {
                FetchPolicy::TwoStage => bodies[index].as_slice(),
                FetchPolicy::WholeCell => {
                    let retained = whole_bodies
                        .get(whole_offset)
                        .ok_or("whole-cell buffer missing")?;
                    let start = span
                        .offset
                        .checked_sub(*whole_offset)
                        .ok_or("whole-cell block offset")?;
                    retained
                        .get(start..start + span.bytes)
                        .ok_or("whole-cell block extent")?
                }
            };
            if options.fetch_policy == FetchPolicy::WholeCell {
                require(hash(body) == span.sha256, "refinement SHA256 mismatch")?;
                require(
                    body.chunks_exact(self.dimensions() + 12)
                        .zip(expected)
                        .all(|(row, id)| i64::from_le_bytes(row[..8].try_into().unwrap()) == *id),
                    "refinement/source ordinal binding",
                )?;
            }
            ranges.push(ReturnedRange {
                start: first * (self.dimensions() + 12),
                bytes: body,
            });
        }
        let count = top_k.min(nominated_ids.len());
        let ranked = rank_returned_ranges(
            Sq8Geometry {
                rows: self.rows(),
                dimensions: self.dimensions(),
            },
            &ranges,
            &normalized_query,
            &self.manifest.low,
            &self.manifest.step,
            count,
            match options.fetch_policy {
                FetchPolicy::WholeCell => options.max_cell_bytes,
                FetchPolicy::TwoStage => options.max_refinement_bytes,
            },
        )
        .map_err(|error| format!("unchanged SQ8 ranking: {error:?}"))?;
        let returned = ranked
            .into_iter()
            .map(|v| RankedRow {
                ordinal: v.ordinal,
                id: v.id,
                score: v.score,
            })
            .collect();
        let final_ranking = elapsed(started);
        Ok(SearchTrace {
            primary_ids: primary_ids.into_iter().collect(),
            covered_ids: covered_ids.into_iter().collect(),
            nominated_ids: nominated_ids.into_iter().collect(),
            returned,
            accounting: Accounting::default(),
            routing,
            local_nomination,
            final_ranking,
        })
    }
    /// Perform one truth-free bounded search; no automatic retries or widening.
    /// Underfill is represented by the returned roster length.
    pub fn search(
        &self,
        query: &[f32],
        top_k: usize,
        options: SearchOptions,
    ) -> std::result::Result<SearchTrace, SearchFailure> {
        let mut accounting = Accounting::default();
        match self.query_inner(query, top_k, options, &mut accounting) {
            Ok(mut trace) => {
                trace.accounting = accounting;
                Ok(trace)
            }
            Err(error) => Err(SearchFailure {
                message: error.to_string(),
                accounting,
            }),
        }
    }
}

/// Fresh packed source-witness artifact marker; resident-v4 layout is unchanged.
pub const SOURCE_PROBE_SCHEMA: &str = "borsuk-source-witness-router-v1";
const PROBE_MARKER: &[u8; 8] = b"BSWP0001";
const PROBE_COUNT: usize = 16;
const PROBE_CELLS: usize = 24;
const PROBE_CAP: usize = 128 * 1024 * 1024;

// Instrumentation deliberately does not open /proc or any file during routing.
fn probe_elapsed(start: (Instant, i128)) -> StageTime {
    StageTime {
        wall_ns: start.0.elapsed().as_nanos(),
        process_cpu_ns: cpu_ns() - start.1,
        rss_after_bytes: None,
        process_high_water_bytes: None,
    }
}
fn probe_reserved<T>(count: usize) -> Result<Vec<T>> {
    require(
        count
            .checked_mul(std::mem::size_of::<T>())
            .is_some_and(|n| n <= PROBE_CAP),
        "probe allocation bound",
    )?;
    let mut values = Vec::new();
    values.try_reserve_exact(count)?;
    Ok(values)
}
fn probe_file(path: &Path, bytes: usize) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags((rustix::fs::OFlags::NONBLOCK | rustix::fs::OFlags::NOFOLLOW).bits() as i32)
        .open(path)?;
    require(
        file.metadata()?.is_file() && file.metadata()?.len() == bytes as u64,
        "probe regular file/exact length",
    )?;
    Ok(file)
}
/// Secure exact-descriptor read used by the new modes. Cap rejection precedes
/// file open/body allocation; final symlinks and nonregular/FIFO inputs fail.
pub fn read_source_probe_artifact(artifact: &Artifact, cap: usize) -> Result<Vec<u8>> {
    read_source_probe_artifact_tracked(artifact, cap, &mut ReadStats::default())
}
/// Exact secure read, retaining actual submitted/verified/failed bytes on errors.
/// The caller supplies an initially empty snapshot for this operation.
pub fn read_source_probe_artifact_tracked(
    artifact: &Artifact,
    cap: usize,
    stats: &mut ReadStats,
) -> Result<Vec<u8>> {
    require(
        artifact.bytes > 0
            && artifact.bytes <= cap
            && cap <= PROBE_CAP
            && valid_sha(&artifact.sha256),
        "probe artifact descriptor/cap",
    )?;
    let mut file = probe_file(&artifact.path, artifact.bytes)?;
    let mut body = probe_reserved(artifact.bytes)?;
    body.resize(artifact.bytes, 0);
    stats.submitted_gets += 1;
    stats.requested_bytes += artifact.bytes;
    stats.failed_gets += 1;
    let result = (|| -> Result<()> {
        file.read_exact(&mut body)?;
        require(
            file.read(&mut [0])? == 0 && hash(&body) == artifact.sha256,
            "probe artifact SHA256/EOF",
        )
    })();
    if result.is_ok() {
        stats.failed_gets -= 1;
        stats.verified_bytes += body.len();
    }
    result?;
    Ok(body)
}
/// Incremental root/directory startup snapshot, populated before each I/O and
/// retained by the caller on rejection. Metadata opens submit no body read.
#[derive(Debug, Clone, Default, Serialize)]
pub struct SourceProbeLayoutStartup {
    /// Actual root descriptor body authentication attempts.
    pub root: ReadStats,
    /// Actual full directory body authentication attempts.
    pub directory: ReadStats,
    /// Derived preload model when a valid root exposes it, even on cap rejection.
    pub admission: Option<DirectoryAdmission>,
    /// Complete startup wall/process CPU including failed reads/authentication.
    pub complete: StageTime,
}
/// Execution rejection with actual submitted I/O and a complete attempt timer.
#[derive(Debug, Serialize)]
pub struct SourceProbeFailure {
    /// Admission, authentication, numeric or execution failure (INVALID).
    pub message: String,
    /// Actual cell reads, including failed submitted operations.
    pub accounting: Accounting,
    /// Actual sidecar startup read, including failures after submission.
    pub startup: ReadStats,
    /// One complete wall/process CPU interval; never a sum of stage timers.
    pub complete: StageTime,
}
impl std::fmt::Display for SourceProbeFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}
impl Error for SourceProbeFailure {}
/// Bounded sidecar startup and retained ownership arithmetic (not measured RSS).
#[derive(Debug, Clone, Serialize)]
pub struct SourceProbeAdmission {
    /// Exact packed sidecar bytes authenticated at open.
    pub encoded_bytes: usize,
    /// Conservative resident packed records/rosters/descriptors/codec allowance.
    pub modeled_resident_bytes: usize,
    /// Resident allowance plus the complete encoded startup buffer.
    pub modeled_preload_peak_bytes: usize,
    /// Tracked actual owned capacities, excluding map/allocator/runtime overhead.
    pub owned_capacity_bytes: usize,
}
/// Source-only construction receipt; source inputs are authenticated cell spans.
#[derive(Debug, Serialize)]
pub struct SourceProbeBuildReceipt {
    /// Externally authenticate this descriptor before serving it.
    pub artifact: Artifact,
    /// Original whole-cell reads; subspan authentication does not submit reads.
    pub accounting: Accounting,
    /// Complete build CPU/wall, including authentication/output/fsync.
    pub complete: StageTime,
    /// Conservative output, roster and one-cell decoded working-set allowance.
    pub modeled_peak_payload_bytes: usize,
}
#[derive(Debug)]
struct ProbeCell {
    cell: Cell,
    source_ids: Vec<i64>,
    local_ordinals: Vec<usize>,
    start: usize,
}
/// Packed original witnesses and bounded rosters/descriptors only. No retained
/// file, path, lazy loader, f32 corpus copy or per-query filesystem operation.
/// Builder proves source membership by reads. Serving trusts the externally
/// authenticated sidecar SHA plus root binding; root SHA alone is not a proof
/// of any witness's source membership.
pub struct SourceProbeRouter {
    root_sha256: String,
    rows: usize,
    dimensions: usize,
    low: Vec<f32>,
    step: Vec<f32>,
    cells: Vec<ProbeCell>,
    packed: Vec<u8>,
    /// Actual one sidecar body read at startup, distinct from layout startup.
    pub startup: ReadStats,
    /// Complete sidecar open/authentication/validation CPU/wall.
    pub startup_time: StageTime,
    /// Admitted peak and tracked retained capacity.
    pub admission: SourceProbeAdmission,
}
fn probe_leaves(layout: &Prototype) -> Result<Vec<&Node>> {
    let mut leaves = probe_reserved(layout.manifest.build.cells)?;
    for node in layout.directories.values().flat_map(|p| &p.children) {
        if matches!(node.target, Target::Cell { .. }) {
            leaves.push(node);
        }
    }
    leaves.sort_unstable_by_key(|node| match &node.target {
        Target::Cell { cell } => cell.id,
        _ => unreachable!(),
    });
    require(
        leaves.len() == layout.manifest.build.cells,
        "probe complete resident leaves",
    )?;
    Ok(leaves)
}
fn probe_encoded_bytes(layout: &Prototype) -> Result<usize> {
    let witnesses = probe_leaves(layout)?
        .iter()
        .map(|node| node.rows.min(PROBE_COUNT))
        .sum::<usize>();
    104_usize
        .checked_add(
            layout
                .dimensions()
                .checked_mul(8)
                .ok_or("probe geometry overflow")?,
        )
        .and_then(|n| n.checked_add(layout.rows().checked_mul(8)?))
        .and_then(|n| n.checked_add(layout.manifest.build.cells.checked_mul(16)?))
        .and_then(|n| n.checked_add(witnesses.checked_mul(layout.dimensions().checked_add(20)?)?))
        .filter(|n| *n <= PROBE_CAP)
        .ok_or_else(|| "probe encoded byte overflow/cap".into())
}
fn probe_admission(layout: &Prototype, encoded: usize) -> Result<SourceProbeAdmission> {
    let resident = encoded
        .checked_mul(2)
        .and_then(|n| n.checked_add(layout.manifest.build.cells.checked_mul(4096)?))
        .and_then(|n| n.checked_add(8 * ROOT_CAP))
        .ok_or("probe resident overflow")?;
    Ok(SourceProbeAdmission {
        encoded_bytes: encoded,
        modeled_resident_bytes: resident,
        modeled_preload_peak_bytes: resident
            .checked_add(encoded)
            .ok_or("probe preload overflow")?,
        owned_capacity_bytes: 0,
    })
}
// Decode f32 coordinates first, accumulate norm in f64, divide in f64 and cast
// normalized coordinates to f32. Never normalize or rewrite stored SQ8 bytes.
fn probe_normalized(record: &[u8], low: &[f32], step: &[f32]) -> Result<Vec<f32>> {
    require(
        record.len() == low.len() + 12 && step.len() == low.len(),
        "probe decoded geometry",
    )?;
    let norm = f32::from_le_bytes(record[8..12].try_into()?);
    require(norm.is_finite(), "probe stored norm finite")?;
    let mut vector = probe_reserved(low.len())?;
    let mut squared = 0_f64;
    for d in 0..low.len() {
        let value = low[d] + f32::from(record[d + 12]) * step[d];
        require(value.is_finite(), "probe nonfinite decoded coordinate")?;
        squared += f64::from(value) * f64::from(value);
        vector.push(value);
    }
    require(
        squared.is_finite() && squared > 0.,
        "probe zero/nonfinite decoded norm",
    )?;
    let length = squared.sqrt();
    for value in &mut vector {
        *value = (f64::from(*value) / length) as f32;
    }
    Ok(vector)
}
fn probe_greedy(records: &[u8], ids: &[i64], low: &[f32], step: &[f32]) -> Result<Vec<usize>> {
    let width = low.len() + 12;
    require(
        !ids.is_empty() && ids.len() <= 512 && records.len() == ids.len() * width,
        "probe greedy cell geometry",
    )?;
    // At most one cell's decoded vectors; never all-N f32 resident copies.
    let vectors = records
        .chunks_exact(width)
        .map(|r| probe_normalized(r, low, step))
        .collect::<Result<Vec<_>>>()?;
    let mut selected = probe_reserved(ids.len().min(PROBE_COUNT))?;
    let mut nearest = vec![f64::INFINITY; ids.len()];
    let mut used = vec![false; ids.len()];
    let first = (0..ids.len())
        .min_by_key(|&i| ids[i])
        .ok_or("empty probe cell")?;
    selected.push(first);
    used[first] = true;
    while selected.len() < ids.len().min(PROBE_COUNT) {
        let last = *selected.last().ok_or("empty probe selection")?;
        for row in 0..ids.len() {
            let distance = vectors[row]
                .iter()
                .zip(&vectors[last])
                .map(|(&a, &b)| {
                    let delta = f64::from(a) - f64::from(b);
                    delta * delta
                })
                .sum::<f64>();
            require(distance.is_finite(), "probe nonfinite nearest distance")?;
            nearest[row] = nearest[row].min(distance);
        }
        let next = (0..ids.len())
            .filter(|&i| !used[i])
            .max_by(|&a, &b| nearest[a].total_cmp(&nearest[b]).then(ids[b].cmp(&ids[a])))
            .ok_or("probe missing next witness")?;
        used[next] = true;
        selected.push(next);
    }
    Ok(selected)
}
/// Build fixed16 source-only probes. No requests/truth parameter or reader.
/// Authenticates whole/source/refinement spans and exact source/SQ8 ID binding
/// before selecting witnesses, writes a fresh no-overwrite artifact and fsyncs
/// it and its parent. Failure reports actual submitted cell reads.
pub fn build_source_probes(
    layout: &Prototype,
    output: &Path,
) -> std::result::Result<SourceProbeBuildReceipt, SourceProbeFailure> {
    let started = (Instant::now(), cpu_ns());
    let mut accounting = Accounting::default();
    let work = (|| -> Result<(Artifact, usize)> {
        require(
            fs::symlink_metadata(output).is_err(),
            "probe output already exists/no overwrite",
        )?;
        let bytes = probe_encoded_bytes(layout)?;
        let modeled = bytes
            .checked_add(
                layout
                    .rows()
                    .checked_mul(64)
                    .ok_or("probe build overflow")?,
            )
            .and_then(|n| n.checked_add(512 * (layout.dimensions() * 8 + 4096)))
            .ok_or("probe build payload overflow")?;
        require(
            modeled <= 256 * 1024 * 1024
                && modeled
                    .checked_add(layout.directory_admission.modeled_parsed_payload_bytes)
                    .is_some_and(|n| n <= 512 * 1024 * 1024),
            "probe build payload/resident aggregate admission",
        )?;
        accounting.modeled_query_payload_bytes = modeled;
        let mut body = probe_reserved(bytes)?;
        body.extend_from_slice(PROBE_MARKER);
        body.extend_from_slice(layout.admitted_root_sha256.as_bytes());
        for value in [
            layout.rows(),
            layout.dimensions(),
            layout.manifest.build.cells,
            PROBE_COUNT,
        ] {
            body.extend_from_slice(&(value as u64).to_le_bytes());
        }
        for values in [&layout.manifest.low, &layout.manifest.step] {
            for value in values {
                body.extend_from_slice(&value.to_le_bytes());
            }
        }
        let mut seen = vec![false; layout.rows()];
        let wave = accounting.wave(FetchStage::WholeCell);
        for node in probe_leaves(layout)? {
            let Target::Cell { cell } = &node.target else {
                unreachable!()
            };
            let whole = accounting.fetch(
                &layout.cells,
                &cell.whole,
                FetchStage::WholeCell,
                wave,
                (layout.manifest.build.cells, layout.manifest.cell_bytes),
            )?;
            let source = &whole[..cell.source.bytes];
            require(
                hash(source) == cell.source.sha256,
                "probe source span SHA256",
            )?;
            let ids = source
                .chunks_exact(8 + layout.codec.record_bytes())
                .map(|record| i64::from_le_bytes(record[..8].try_into().unwrap()))
                .collect::<Vec<_>>();
            require(ids.len() == node.rows, "probe exact source roster")?;
            let mut records = probe_reserved(node.rows * (layout.dimensions() + 12))?;
            for span in &cell.refinement {
                let start = span
                    .offset
                    .checked_sub(cell.whole.offset)
                    .ok_or("probe block offset")?;
                let block = whole
                    .get(start..start + span.bytes)
                    .ok_or("probe block extent")?;
                require(hash(block) == span.sha256, "probe refinement SHA256")?;
                records.extend_from_slice(block);
            }
            require(
                records.len() == node.rows * (layout.dimensions() + 12),
                "probe SQ8 exact extent",
            )?;
            for (&id, record) in ids
                .iter()
                .zip(records.chunks_exact(layout.dimensions() + 12))
            {
                require(
                    id >= 0
                        && (id as usize) < seen.len()
                        && !seen[id as usize]
                        && i64::from_le_bytes(record[..8].try_into()?) == id,
                    "probe complete unique source/SQ8 ID binding",
                )?;
                seen[id as usize] = true;
            }
            let selected =
                probe_greedy(&records, &ids, &layout.manifest.low, &layout.manifest.step)?;
            body.extend_from_slice(&(cell.id as u64).to_le_bytes());
            body.extend_from_slice(&(node.rows as u64).to_le_bytes());
            for id in &ids {
                body.extend_from_slice(&id.to_le_bytes());
            }
            for ordinal in selected {
                body.extend_from_slice(&(ordinal as u64).to_le_bytes());
                let start = ordinal * (layout.dimensions() + 12);
                body.extend_from_slice(&records[start..start + layout.dimensions() + 12]);
            }
        }
        require(
            seen.iter().all(|v| *v) && body.len() == bytes,
            "probe complete source IDs/output geometry",
        )?;
        let parent = output
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let mut file = new_file(output)?;
        file.write_all(&body)?;
        file.sync_all()?;
        File::open(parent)?.sync_all()?;
        Ok((
            Artifact {
                path: output.into(),
                bytes: body.len(),
                sha256: hash(&body),
            },
            modeled,
        ))
    })();
    match work {
        Ok((artifact, modeled)) => Ok(SourceProbeBuildReceipt {
            artifact,
            accounting,
            complete: probe_elapsed(started),
            modeled_peak_payload_bytes: modeled,
        }),
        Err(error) => Err(SourceProbeFailure {
            message: error.to_string(),
            accounting,
            startup: ReadStats::default(),
            complete: probe_elapsed(started),
        }),
    }
}
struct ProbeCursor<'a> {
    body: &'a [u8],
    offset: usize,
}
impl<'a> ProbeCursor<'a> {
    fn take(&mut self, bytes: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(bytes)
            .ok_or("probe cursor overflow")?;
        let value = self
            .body
            .get(self.offset..end)
            .ok_or("probe truncated sidecar")?;
        self.offset = end;
        Ok(value)
    }
    fn usize(&mut self) -> Result<usize> {
        Ok(usize::try_from(u64::from_le_bytes(
            self.take(8)?.try_into()?,
        ))?)
    }
}
/// Fixed whole-cell descriptor caps. Descriptor totals are reported separately
/// from actual zero-I/O coverage selection, with no implied payload fetch.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceProbeLimits {
    /// Must equal24, with no hierarchy or widening.
    pub max_cells: usize,
    /// At most24 selected whole-cell descriptors.
    pub max_cell_gets: usize,
    /// At most16MiB selected whole-cell descriptor bytes.
    pub max_cell_bytes: usize,
    /// Bounded resident scoring and roster scratch, excluding router residency.
    pub max_query_payload_bytes: usize,
}
/// Selected source roster and hypothetical whole-cell fetch descriptor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceProbeSelection {
    /// Actual resident layout cell ID.
    pub cell_id: usize,
    /// Minimum native SQ8 witness score, ordered then by cell ID.
    pub score: f32,
    /// Lossless score for replay.
    pub score_bits: u32,
    /// Authenticated source roster retained at sidecar preload.
    pub source_ids: Vec<i64>,
    /// Concatenation position, never source membership interval.
    pub first_row: usize,
    /// Whole-cell fetch offset, not read during coverage.
    pub whole_offset: usize,
    /// Whole-cell fetch bytes, not read during coverage.
    pub whole_bytes: usize,
    /// Whole-cell fetch SHA256, not independently verified during coverage.
    pub whole_sha256: String,
}
/// I/O-free source-witness coverage receipt, frozen before truth attribution.
#[derive(Debug, Serialize, Deserialize)]
pub struct SourceProbeNomination {
    /// All cells scored, never prefiltered by a hierarchy.
    pub resident_cells: usize,
    /// Exact count of native witness scores.
    pub witness_scores: usize,
    /// Ordered fixed top24 selection and retained source rosters.
    pub selected: Vec<SourceProbeSelection>,
    /// Sorted unique selected source IDs.
    pub covered_ids: Vec<i64>,
    /// Hypothetical selected whole-cell GETs; actual query GETs remain zero.
    pub selected_fetch_gets: usize,
    /// Hypothetical selected whole-cell bytes; actual query reads remain zero.
    pub selected_fetch_bytes: usize,
    /// Actual query reads (zero) and admitted scoring/roster scratch.
    pub accounting: Accounting,
    /// Complete selection wall/CPU includes query preparation and rosters.
    pub complete: StageTime,
}
/// Separately admitted all16-block full-SQ8 diagnostic.
#[derive(Debug, Serialize, Deserialize)]
pub struct SourceProbeSearchTrace {
    /// Frozen source-witness selection; separate descriptor totals and actual I/O.
    pub nomination: SourceProbeNomination,
    /// Actual full-cell read, SQ2 nomination and unchanged SQ8 rank trace.
    pub trace: SearchTrace,
    /// One complete query timer including routing/read/auth/nomination/ranking.
    pub complete: StageTime,
}
impl SourceProbeRouter {
    /// Authenticate a trusted fresh sidecar and root binding before serving.
    /// Admission and exact encoded geometry precede body open/read/allocation.
    pub fn open(
        layout: &Prototype,
        artifact: &Artifact,
        max_payload_bytes: usize,
    ) -> std::result::Result<Self, SourceProbeFailure> {
        let started = (Instant::now(), cpu_ns());
        let mut startup = ReadStats::default();
        let work = (|| -> Result<Self> {
            let bytes = probe_encoded_bytes(layout)?;
            let mut admission = probe_admission(layout, bytes)?;
            require(
                artifact.bytes == bytes
                    && valid_sha(&artifact.sha256)
                    && max_payload_bytes <= 512 * 1024 * 1024
                    && admission.modeled_preload_peak_bytes <= max_payload_bytes
                    && max_payload_bytes
                        .checked_add(layout.directory_admission.modeled_parsed_payload_bytes)
                        .is_some_and(|n| n <= 512 * 1024 * 1024),
                "probe sidecar exact geometry/preload aggregate admission",
            )?;
            // File open/type errors submit no body read. Once admitted/opened,
            // every read attempt, including authentication failure, is charged.
            let mut file = probe_file(&artifact.path, artifact.bytes)?;
            let mut body = probe_reserved(bytes)?;
            body.resize(bytes, 0);
            startup.submitted_gets = 1;
            startup.requested_bytes = bytes;
            startup.failed_gets = 1;
            file.read_exact(&mut body)?;
            require(
                file.read(&mut [0])? == 0 && hash(&body) == artifact.sha256,
                "probe sidecar SHA256/EOF",
            )?;
            startup.failed_gets = 0;
            startup.verified_bytes = bytes;
            let mut cursor = ProbeCursor {
                body: &body,
                offset: 0,
            };
            require(
                cursor.take(8)? == PROBE_MARKER
                    && cursor.take(64)? == layout.admitted_root_sha256.as_bytes()
                    && cursor.usize()? == layout.rows()
                    && cursor.usize()? == layout.dimensions()
                    && cursor.usize()? == layout.manifest.build.cells
                    && cursor.usize()? == PROBE_COUNT,
                "probe fresh marker/root/geometry/fixed16 binding",
            )?;
            for values in [&layout.manifest.low, &layout.manifest.step] {
                for value in values {
                    require(
                        cursor.take(4)? == value.to_le_bytes(),
                        "probe exact codec bits",
                    )?;
                }
            }
            let mut cells = probe_reserved(layout.manifest.build.cells)?;
            let mut packed = probe_reserved(bytes)?;
            let mut seen = vec![false; layout.rows()];
            for node in probe_leaves(layout)? {
                let Target::Cell { cell } = &node.target else {
                    unreachable!()
                };
                require(
                    cursor.usize()? == cell.id && cursor.usize()? == node.rows,
                    "probe cell ID/rows binding",
                )?;
                let mut ids = probe_reserved(node.rows)?;
                for _ in 0..node.rows {
                    let id = i64::from_le_bytes(cursor.take(8)?.try_into()?);
                    require(
                        id >= 0 && (id as usize) < seen.len() && !seen[id as usize],
                        "probe unique complete source IDs",
                    )?;
                    seen[id as usize] = true;
                    ids.push(id);
                }
                let mut ordinals = probe_reserved(node.rows.min(PROBE_COUNT))?;
                let start = packed.len() / (layout.dimensions() + 12);
                for _ in 0..node.rows.min(PROBE_COUNT) {
                    let ordinal = cursor.usize()?;
                    let record = cursor.take(layout.dimensions() + 12)?;
                    require(
                        ordinal < ids.len()
                            && !ordinals.contains(&ordinal)
                            && i64::from_le_bytes(record[..8].try_into()?) == ids[ordinal],
                        "probe witness local ordinal/source ID binding",
                    )?;
                    // Validate numeric guards without retaining decoded vectors.
                    drop(probe_normalized(
                        record,
                        &layout.manifest.low,
                        &layout.manifest.step,
                    )?);
                    ordinals.push(ordinal);
                    packed.extend_from_slice(record);
                }
                require(
                    ids[ordinals[0]] == *ids.iter().min().ok_or("probe empty roster")?,
                    "probe first witness smallest ID",
                )?;
                cells.push(ProbeCell {
                    cell: cell.clone(),
                    source_ids: ids,
                    local_ordinals: ordinals,
                    start,
                });
            }
            require(
                cursor.offset == bytes && seen.iter().all(|v| *v),
                "probe complete sidecar/source roster",
            )?;
            let low = layout.manifest.low.clone();
            let step = layout.manifest.step.clone();
            admission.owned_capacity_bytes = packed.capacity()
                + (low.capacity() + step.capacity()) * 4
                + cells.capacity() * std::mem::size_of::<ProbeCell>()
                + cells
                    .iter()
                    .map(|c| {
                        c.source_ids.capacity() * 8
                            + c.local_ordinals.capacity() * 8
                            + c.cell.refinement.capacity() * std::mem::size_of::<Span>()
                            + c.cell.whole.sha256.capacity()
                            + c.cell.source.sha256.capacity()
                            + c.cell
                                .refinement
                                .iter()
                                .map(|s| s.sha256.capacity())
                                .sum::<usize>()
                    })
                    .sum::<usize>()
                + 64;
            require(
                admission.owned_capacity_bytes <= admission.modeled_resident_bytes,
                "probe owned capacity model",
            )?;
            Ok(Self {
                root_sha256: layout.admitted_root_sha256.clone(),
                rows: layout.rows(),
                dimensions: layout.dimensions(),
                low,
                step,
                cells,
                packed,
                startup: ReadStats::default(),
                startup_time: StageTime::default(),
                admission,
            })
        })();
        match work {
            Ok(mut router) => {
                router.startup = startup;
                router.startup_time = probe_elapsed(started);
                Ok(router)
            }
            Err(error) => Err(SourceProbeFailure {
                message: error.to_string(),
                accounting: Accounting::default(),
                startup,
                complete: probe_elapsed(started),
            }),
        }
    }
    fn ranked_cells(&self, query: &[f32]) -> Result<Vec<(f32, usize)>> {
        require(
            query.len() == self.dimensions && self.cells.len() >= PROBE_CELLS,
            "probe query dimensions/at least24 cells",
        )?;
        let normalized = cosine_vector(query)?;
        let count = self.packed.len() / (self.dimensions + 12);
        let ordinals = (0..count).collect::<Vec<_>>();
        let scores = crate::exact_sq8_nominee::score_nominees(
            &self.packed,
            Sq8Geometry {
                rows: count,
                dimensions: self.dimensions,
            },
            &ordinals,
            &normalized,
            &self.low,
            &self.step,
        )
        .map_err(|error| format!("probe unchanged native SQ8: {error:?}"))?;
        let mut ranked = probe_reserved(PROBE_CELLS + 1)?;
        for cell in &self.cells {
            let score = scores[cell.start..cell.start + cell.local_ordinals.len()]
                .iter()
                .map(|v| v.score)
                .min_by(f32::total_cmp)
                .ok_or("probe empty witness set")?;
            ranked.push((score, cell.cell.id));
            ranked.sort_unstable_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
            ranked.truncate(PROBE_CELLS);
        }
        Ok(ranked)
    }
    fn scratch(&self) -> Result<usize> {
        let witnesses = self.packed.len() / (self.dimensions + 12);
        witnesses
            .checked_mul(256)
            .and_then(|n| n.checked_add(self.rows.checked_mul(128)?))
            .and_then(|n| n.checked_add(self.dimensions * 16 + 65536))
            .ok_or_else(|| "probe query scratch overflow".into())
    }
    /// I/O-free fixed all-cell native min-witness top24, ties ascending cell ID.
    pub fn select(&self, query: &[f32]) -> Result<Vec<usize>> {
        require(self.scratch()? <= PROBE_CAP, "probe selection scratch cap")?;
        Ok(self.ranked_cells(query)?.into_iter().map(|v| v.1).collect())
    }
    /// Coverage-only nomination. Every source roster is already resident;
    /// selected fetch descriptors are capped but no cell payload is fetched.
    pub fn nominate(
        &self,
        query: &[f32],
        limits: SourceProbeLimits,
    ) -> std::result::Result<SourceProbeNomination, SourceProbeFailure> {
        let started = (Instant::now(), cpu_ns());
        let mut accounting = Accounting::default();
        let work = (|| -> Result<SourceProbeNomination> {
            let scratch = self.scratch()?;
            require(
                limits.max_cells == PROBE_CELLS
                    && limits.max_cell_gets == PROBE_CELLS
                    && limits.max_cell_bytes > 0
                    && limits.max_cell_bytes <= 16 * 1024 * 1024
                    && limits.max_query_payload_bytes <= 512 * 1024 * 1024
                    && scratch <= limits.max_query_payload_bytes
                    && limits
                        .max_query_payload_bytes
                        .checked_add(self.admission.modeled_resident_bytes)
                        .is_some_and(|n| n <= 512 * 1024 * 1024),
                "probe fixed24 descriptor/query scratch aggregate admission",
            )?;
            accounting.modeled_query_payload_bytes = scratch;
            let ranked = self.ranked_cells(query)?;
            let bytes = ranked
                .iter()
                .map(|v| self.cells[v.1].cell.whole.bytes)
                .sum::<usize>();
            require(
                bytes <= limits.max_cell_bytes,
                "probe selected whole-cell descriptor admission",
            )?;
            let selected = ranked
                .into_iter()
                .map(|(score, id)| {
                    let cell = &self.cells[id];
                    SourceProbeSelection {
                        cell_id: id,
                        score,
                        score_bits: score.to_bits(),
                        source_ids: cell.source_ids.clone(),
                        first_row: cell.cell.first_row,
                        whole_offset: cell.cell.whole.offset,
                        whole_bytes: cell.cell.whole.bytes,
                        whole_sha256: cell.cell.whole.sha256.clone(),
                    }
                })
                .collect::<Vec<_>>();
            let covered_ids = selected
                .iter()
                .flat_map(|c| c.source_ids.iter().copied())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            Ok(SourceProbeNomination {
                resident_cells: self.cells.len(),
                witness_scores: self.packed.len() / (self.dimensions + 12),
                selected,
                covered_ids,
                selected_fetch_gets: PROBE_CELLS,
                selected_fetch_bytes: bytes,
                accounting: Accounting::default(),
                complete: StageTime::default(),
            })
        })();
        match work {
            Ok(mut receipt) => {
                receipt.accounting = accounting;
                receipt.complete = probe_elapsed(started);
                Ok(receipt)
            }
            Err(error) => Err(SourceProbeFailure {
                message: error.to_string(),
                accounting,
                startup: ReadStats::default(),
                complete: probe_elapsed(started),
            }),
        }
    }
}

impl Prototype {
    fn query_probed_inner(
        &self,
        router: &SourceProbeRouter,
        nomination: &SourceProbeNomination,
        query: &[f32],
        top_k: usize,
        options: SearchOptions,
        accounting: &mut Accounting,
    ) -> Result<SearchTrace> {
        require(
            router.root_sha256 == self.admitted_root_sha256
                && top_k > 0
                && top_k <= self.rows()
                && options.fetch_policy == FetchPolicy::WholeCell
                && options.blocks_per_cell == 16
                && options.primary_beam == 8
                && options.boundary_beam == 24
                && options.max_cells == 24
                && options.max_cell_gets == 24
                && options.max_cell_bytes > 0
                && options.max_cell_bytes <= 16 * 1024 * 1024
                && options.max_query_payload_bytes <= 512 * 1024 * 1024,
            "probe full-SQ8 fixed24/all16 options/root binding",
        )?;
        let frontier_cells = options.primary_beam + options.boundary_beam;
        let rows = options.max_cells * self.manifest.input.cell_rows;
        // Root and full directory were admitted once at open. Only cloned
        // routing frontiers, diagnostic rosters and actual response/rank buffers
        // are multiplied by concurrent queries; host/runtime charges are external.
        let routing_scratch = 8
            * frontier_cells
            * (self.dimensions() * 4
                + self.manifest.input.cell_rows.div_ceil(BLOCK_ROWS) * 128
                + 512);
        let responses = match options.fetch_policy {
            FetchPolicy::WholeCell => options.max_cell_bytes.checked_mul(3),
            FetchPolicy::TwoStage => options
                .max_source_bytes
                .checked_mul(2)
                .and_then(|n| n.checked_add(options.max_refinement_bytes.checked_mul(3)?)),
        }
        .ok_or("query memory overflow")?;
        let payload = (routing_scratch + 400_000 + rows * 512)
            .checked_add(responses)
            .ok_or("query memory overflow")?;
        require(
            payload <= options.max_query_payload_bytes,
            "query payload/scratch admission",
        )?;
        accounting.modeled_query_payload_bytes = payload;

        let combined = payload
            .checked_add(router.scratch()?)
            .ok_or("probe full-query memory overflow")?;
        require(
            combined <= options.max_query_payload_bytes
                && options
                    .max_query_payload_bytes
                    .checked_add(self.directory_admission.modeled_parsed_payload_bytes)
                    .and_then(|n| n.checked_add(router.admission.modeled_resident_bytes))
                    .is_some_and(|n| n <= 512 * 1024 * 1024),
            "probe full-query combined payload/resident aggregate admission",
        )?;
        accounting.modeled_query_payload_bytes = combined;
        let normalized_query = cosine_vector(query)?;
        let routing = nomination.complete.clone();
        let mut selected = BTreeMap::new();
        for choice in &nomination.selected {
            let node = self
                .directories
                .values()
                .flat_map(|p| &p.children)
                .find(|n| matches!(&n.target, Target::Cell { cell } if cell.id == choice.cell_id))
                .ok_or("probe selected resident leaf missing")?;
            selected.insert(choice.cell_id, node.clone());
        }
        require(selected.len() == 24, "probe actual full24 selected cells")?;
        let primary_cells = nomination
            .selected
            .iter()
            .take(8)
            .map(|c| c.cell_id)
            .collect::<BTreeSet<_>>();
        let (stage, fetch_cap) = match options.fetch_policy {
            FetchPolicy::WholeCell => (
                FetchStage::WholeCell,
                (options.max_cell_gets, options.max_cell_bytes),
            ),
            FetchPolicy::TwoStage => (
                FetchStage::Source,
                (options.max_source_gets, options.max_source_bytes),
            ),
        };
        require(
            selected.len() <= options.max_cells
                && selected.len() <= fetch_cap.0
                && selected
                    .values()
                    .map(|n| match &n.target {
                        Target::Cell { cell } => match options.fetch_policy {
                            FetchPolicy::WholeCell => cell.whole.bytes,
                            FetchPolicy::TwoStage => cell.source.bytes,
                        },
                        _ => 0,
                    })
                    .sum::<usize>()
                    <= fetch_cap.1,
            "selected cell batch admission",
        )?;
        let started = (Instant::now(), cpu_ns());
        // Match native serving: preserve original f32 input for SQ2 preparation;
        // f32 prenormalization can perturb lookup scores and block ties.
        let prepared = self.codec.prepare_query(query, 400_000)?;
        let mut primary_ids = BTreeSet::new();
        let mut covered_ids = BTreeSet::new();
        let mut nominated_ids = BTreeSet::new();
        let mut refinements = Vec::new();
        let mut whole_bodies = BTreeMap::new();
        let cell_wave = accounting.wave(stage);
        for node in selected.values() {
            let Target::Cell { cell } = &node.target else {
                unreachable!()
            };
            let span = match options.fetch_policy {
                FetchPolicy::WholeCell => &cell.whole,
                FetchPolicy::TwoStage => &cell.source,
            };
            let body = accounting.fetch(&self.cells, span, stage, cell_wave, fetch_cap)?;
            let source = &body[..cell.source.bytes];
            if options.fetch_policy == FetchPolicy::WholeCell {
                require(
                    hash(source) == cell.source.sha256,
                    "cell source SHA256 mismatch",
                )?;
            }
            let mut ids = Vec::with_capacity(node.rows);
            let mut maxima = vec![f64::NEG_INFINITY; node.rows.div_ceil(BLOCK_ROWS)];
            for (row, record) in source
                .chunks_exact(8 + self.codec.record_bytes())
                .enumerate()
            {
                let id = i64::from_le_bytes(record[..8].try_into()?);
                require(
                    id >= 0 && (id as usize) < self.rows() && covered_ids.insert(id),
                    "cell ordinal roster/duplicate ID",
                )?;
                if primary_cells.contains(&cell.id) {
                    primary_ids.insert(id);
                }
                ids.push(id);
                maxima[row / BLOCK_ROWS] =
                    maxima[row / BLOCK_ROWS].max(prepared.score(&record[8..])?);
            }
            let mut blocks = (0..maxima.len()).collect::<Vec<_>>();
            blocks.sort_unstable_by(|&a, &b| maxima[b].total_cmp(&maxima[a]).then(a.cmp(&b)));
            for block in blocks.into_iter().take(options.blocks_per_cell) {
                let expected =
                    ids[block * BLOCK_ROWS..((block + 1) * BLOCK_ROWS).min(ids.len())].to_vec();
                nominated_ids.extend(expected.iter().copied());
                refinements.push((
                    cell.first_row + block * BLOCK_ROWS,
                    cell.refinement[block].clone(),
                    expected,
                    cell.whole.offset,
                ));
            }
            if options.fetch_policy == FetchPolicy::WholeCell {
                whole_bodies.insert(cell.whole.offset, body);
            }
        }
        let local_nomination = probe_elapsed(started);
        if options.fetch_policy == FetchPolicy::TwoStage {
            require(
                refinements.len() <= options.max_refinement_gets
                    && refinements.iter().map(|v| v.1.bytes).sum::<usize>()
                        <= options.max_refinement_bytes,
                "refinement batch admission",
            )?;
        }
        refinements.sort_unstable_by_key(|v| v.0);
        let started = (Instant::now(), cpu_ns());
        let mut bodies = Vec::with_capacity(refinements.len());
        if options.fetch_policy == FetchPolicy::TwoStage {
            let wave = accounting.wave(FetchStage::Refinement);
            for (_, span, expected, _) in &refinements {
                let body = accounting.fetch(
                    &self.cells,
                    span,
                    FetchStage::Refinement,
                    wave,
                    (options.max_refinement_gets, options.max_refinement_bytes),
                )?;
                require(
                    body.chunks_exact(self.dimensions() + 12)
                        .zip(expected)
                        .all(|(row, id)| i64::from_le_bytes(row[..8].try_into().unwrap()) == *id),
                    "refinement/source ordinal binding",
                )?;
                bodies.push(body);
            }
        }
        let mut ranges = Vec::with_capacity(refinements.len());
        for (index, (first, span, expected, whole_offset)) in refinements.iter().enumerate() {
            let body = match options.fetch_policy {
                FetchPolicy::TwoStage => bodies[index].as_slice(),
                FetchPolicy::WholeCell => {
                    let retained = whole_bodies
                        .get(whole_offset)
                        .ok_or("whole-cell buffer missing")?;
                    let start = span
                        .offset
                        .checked_sub(*whole_offset)
                        .ok_or("whole-cell block offset")?;
                    retained
                        .get(start..start + span.bytes)
                        .ok_or("whole-cell block extent")?
                }
            };
            if options.fetch_policy == FetchPolicy::WholeCell {
                require(hash(body) == span.sha256, "refinement SHA256 mismatch")?;
                require(
                    body.chunks_exact(self.dimensions() + 12)
                        .zip(expected)
                        .all(|(row, id)| i64::from_le_bytes(row[..8].try_into().unwrap()) == *id),
                    "refinement/source ordinal binding",
                )?;
            }
            ranges.push(ReturnedRange {
                start: first * (self.dimensions() + 12),
                bytes: body,
            });
        }
        let count = top_k.min(nominated_ids.len());
        let ranked = rank_returned_ranges(
            Sq8Geometry {
                rows: self.rows(),
                dimensions: self.dimensions(),
            },
            &ranges,
            &normalized_query,
            &self.manifest.low,
            &self.manifest.step,
            count,
            match options.fetch_policy {
                FetchPolicy::WholeCell => options.max_cell_bytes,
                FetchPolicy::TwoStage => options.max_refinement_bytes,
            },
        )
        .map_err(|error| format!("unchanged SQ8 ranking: {error:?}"))?;
        let returned = ranked
            .into_iter()
            .map(|v| RankedRow {
                ordinal: v.ordinal,
                id: v.id,
                score: v.score,
            })
            .collect();
        let final_ranking = probe_elapsed(started);
        Ok(SearchTrace {
            primary_ids: primary_ids.into_iter().collect(),
            covered_ids: covered_ids.into_iter().collect(),
            nominated_ids: nominated_ids.into_iter().collect(),
            returned,
            accounting: Accounting::default(),
            routing,
            local_nomination,
            final_ranking,
        })
    }

    /// Separately admitted full-SQ8 path. The caller must establish the paired
    /// coverage gate first. All16 blocks admitted; native query preparation and
    /// ranking arithmetic match the old path exactly, including nonunit inputs.
    pub fn search_probed(
        &self,
        router: &SourceProbeRouter,
        query: &[f32],
        top_k: usize,
        options: SearchOptions,
    ) -> std::result::Result<SourceProbeSearchTrace, SourceProbeFailure> {
        let started = (Instant::now(), cpu_ns());
        let nomination = router.nominate(
            query,
            SourceProbeLimits {
                max_cells: options.max_cells,
                max_cell_gets: options.max_cell_gets,
                max_cell_bytes: options.max_cell_bytes,
                max_query_payload_bytes: options.max_query_payload_bytes,
            },
        );
        let nomination = match nomination {
            Ok(value) => value,
            Err(mut error) => {
                error.complete = probe_elapsed(started);
                return Err(error);
            }
        };
        let mut accounting = Accounting::default();
        match self.query_probed_inner(router, &nomination, query, top_k, options, &mut accounting) {
            Ok(mut trace) => {
                trace.accounting = accounting;
                require(
                    trace.covered_ids == nomination.covered_ids
                        && trace.nominated_ids == trace.covered_ids,
                    "probe all-block/source roster binding",
                )
                .map_err(|error| SourceProbeFailure {
                    message: error.to_string(),
                    accounting: trace.accounting.clone(),
                    startup: ReadStats::default(),
                    complete: probe_elapsed(started),
                })?;
                Ok(SourceProbeSearchTrace {
                    nomination,
                    trace,
                    complete: probe_elapsed(started),
                })
            }
            Err(error) => Err(SourceProbeFailure {
                message: error.to_string(),
                accounting,
                startup: ReadStats::default(),
                complete: probe_elapsed(started),
            }),
        }
    }
}

/// Descriptive stage coverage on an externally frozen panel, not independent
/// causal estimates or a qualification threshold. Primary misses can be
/// recovered by boundary expansion; only remaining boundary misses are additive.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LossDecomposition {
    /// Number of distinct externally provided truth IDs.
    pub truth_count: usize,
    /// Truth in primary cells.
    pub primary_hits: usize,
    /// Truth in the primary plus boundary union.
    pub boundary_hits: usize,
    /// Truth in locally nominated refinement blocks.
    pub nomination_hits: usize,
    /// Truth among final returned IDs.
    pub returned_hits: usize,
    /// Truth outside primary cells, before boundary expansion.
    pub router_misses: usize,
    /// Primary misses recovered by the independent wider beam.
    pub boundary_recovered: usize,
    /// Truth still missing after boundary expansion.
    pub boundary_misses: usize,
    /// Covered truth lost during local block nomination.
    pub local_nomination_misses: usize,
    /// Nominated truth lost during final SQ8 ranking/top-k.
    pub final_ranking_misses: usize,
}
/// Attach truth only after a search is frozen; this function performs no routing.
pub fn decompose_loss(trace: &SearchTrace, truth: &[i64]) -> Result<LossDecomposition> {
    let truth_set = truth.iter().copied().collect::<BTreeSet<_>>();
    require(
        !truth.is_empty() && truth_set.len() == truth.len() && truth.iter().all(|v| *v >= 0),
        "unique ordinal truth roster",
    )?;
    let primary = trace.primary_ids.iter().copied().collect::<BTreeSet<_>>();
    let covered = trace.covered_ids.iter().copied().collect::<BTreeSet<_>>();
    let nominated = trace.nominated_ids.iter().copied().collect::<BTreeSet<_>>();
    let returned = trace.returned.iter().map(|v| v.id).collect::<BTreeSet<_>>();
    require(
        primary.is_subset(&covered)
            && nominated.is_subset(&covered)
            && returned.is_subset(&nominated),
        "nested diagnostic coverage",
    )?;
    let primary_hits = truth_set.intersection(&primary).count();
    let boundary_hits = truth_set.intersection(&covered).count();
    let nomination_hits = truth_set.intersection(&nominated).count();
    let returned_hits = truth_set.intersection(&returned).count();
    Ok(LossDecomposition {
        truth_count: truth.len(),
        primary_hits,
        boundary_hits,
        nomination_hits,
        returned_hits,
        router_misses: truth.len() - primary_hits,
        boundary_recovered: boundary_hits - primary_hits,
        boundary_misses: truth.len() - boundary_hits,
        local_nomination_misses: boundary_hits - nomination_hits,
        final_ranking_misses: nomination_hits - returned_hits,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn source_probe_limits() -> SourceProbeLimits {
        SourceProbeLimits {
            max_cells: 24,
            max_cell_gets: 24,
            max_cell_bytes: 16 * 1024 * 1024,
            max_query_payload_bytes: 128 * 1024 * 1024,
        }
    }
    fn source_probe_fixture(dir: &Path, rows: usize) -> Prototype {
        let vectors = (0..rows)
            .map(|i| {
                let theta = (i % 97) as f64 / 96. * std::f64::consts::FRAC_PI_2;
                vec![theta.cos() as f32, theta.sin() as f32]
            })
            .collect::<Vec<_>>();
        let config = fixture_with_vectors(dir, &vectors, &[0.2, 0.1], true);
        let path = dir.join("layout-probes");
        let receipt = build(&config, &path).unwrap();
        Prototype::open_for_source_probes(
            &Artifact {
                path: path.join("manifest.json"),
                bytes: fs::metadata(path.join("manifest.json")).unwrap().len() as usize,
                sha256: receipt.root_sha256,
            },
            128 * 1024 * 1024,
        )
        .unwrap()
    }
    #[test]
    fn source_probe_greedy_matches_independent_oracle_ties_shortcell_and_numeric_guards() {
        let ids = (0..21).map(|i| 100 - i).collect::<Vec<i64>>();
        let low = [-0.5_f32, 0.125];
        let step = [1. / 255., 1. / 255.];
        let mut records = Vec::new();
        for (i, id) in ids.iter().enumerate() {
            records.extend_from_slice(&id.to_le_bytes());
            records.extend_from_slice(&1_f32.to_le_bytes());
            records
                .extend_from_slice(&[if i % 3 == 0 { 200 } else { 40 }, (i % 3 * 40 + 40) as u8]);
        }
        // Independent oracle recomputes each candidate's nearest distance from
        // scratch on every iteration; no production helper or incremental state.
        let points = records
            .chunks_exact(14)
            .map(|r| {
                let xy = [
                    low[0] + f32::from(r[12]) * step[0],
                    low[1] + f32::from(r[13]) * step[1],
                ];
                let norm = (f64::from(xy[0]).powi(2) + f64::from(xy[1]).powi(2)).sqrt();
                [
                    (f64::from(xy[0]) / norm) as f32,
                    (f64::from(xy[1]) / norm) as f32,
                ]
            })
            .collect::<Vec<_>>();
        let mut oracle = vec![20];
        while oracle.len() < 16 {
            let mut candidates = (0..ids.len())
                .filter(|i| !oracle.contains(i))
                .map(|i| {
                    let nearest = oracle
                        .iter()
                        .map(|&j| {
                            let x = f64::from(points[i][0]) - f64::from(points[j][0]);
                            let y = f64::from(points[i][1]) - f64::from(points[j][1]);
                            x * x + y * y
                        })
                        .min_by(f64::total_cmp)
                        .unwrap();
                    (nearest, ids[i], i)
                })
                .collect::<Vec<_>>();
            candidates.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)));
            oracle.push(candidates[0].2);
        }
        assert_eq!(probe_greedy(&records, &ids, &low, &step).unwrap(), oracle);
        let short = [9_i64, 2, 5];
        let tied = short
            .iter()
            .flat_map(|id| {
                let mut r = id.to_le_bytes().to_vec();
                r.extend_from_slice(&1_f32.to_le_bytes());
                r.extend_from_slice(&[1, 0]);
                r
            })
            .collect::<Vec<_>>();
        assert_eq!(
            probe_greedy(&tied, &short, &[0., 0.], &[1., 1.]).unwrap(),
            vec![1, 2, 0]
        );
        let mut zero = tied[..14].to_vec();
        zero[12..].fill(0);
        assert!(probe_greedy(&zero, &[9], &[0., 0.], &[1., 1.]).is_err());
        zero[12] = 1;
        zero[8..12].copy_from_slice(&f32::NAN.to_le_bytes());
        assert!(probe_greedy(&zero, &[9], &[0., 0.], &[1., 1.]).is_err());
        assert!(probe_greedy(&tied[..14], &[9], &[f32::INFINITY, 0.], &[1., 1.]).is_err());
    }
    #[test]
    fn source_probe_raw_identity_native_score_top24_and_no_query_io() {
        let temp = tempfile::tempdir().unwrap();
        let layout = source_probe_fixture(temp.path(), 1024);
        let output = temp.path().join("probes.bin");
        let build = build_source_probes(&layout, &output).unwrap();
        assert_eq!(
            build.accounting.whole_cell.submitted_gets,
            layout.manifest.build.cells
        );
        assert_eq!(
            build.accounting.whole_cell.verified_bytes,
            layout.manifest.cell_bytes
        );
        let second = build_source_probes(&layout, &temp.path().join("probes-again.bin")).unwrap();
        assert_eq!(build.artifact.sha256, second.artifact.sha256);
        assert!(build_source_probes(&layout, &output).is_err());
        let router = SourceProbeRouter::open(&layout, &build.artifact, 128 * 1024 * 1024).unwrap();
        for probe in &router.cells {
            let node = probe_leaves(&layout).unwrap().into_iter().find(|node|
                matches!(&node.target, Target::Cell { cell } if cell.id == probe.cell.id)).unwrap();
            assert_eq!(probe.local_ordinals.len(), node.rows.min(16));
            let whole = read_at(
                &layout.cells,
                probe.cell.whole.offset,
                probe.cell.whole.bytes,
            )
            .unwrap();
            for (slot, &local) in probe.local_ordinals.iter().enumerate() {
                let raw = probe.cell.source.bytes + local * (layout.dimensions() + 12);
                let packed = (probe.start + slot) * (layout.dimensions() + 12);
                assert_eq!(&router.packed[packed..packed + 14], &whole[raw..raw + 14]);
                assert_eq!(
                    i64::from_le_bytes(router.packed[packed..packed + 8].try_into().unwrap()),
                    probe.source_ids[local]
                );
            }
        }
        let query = [3.125_f32, 0.875];
        let normalized = cosine_vector(&query).unwrap();
        let ordinals = (0..router.packed.len() / 14).collect::<Vec<_>>();
        let scores = crate::exact_sq8_nominee::score_nominees(
            &router.packed,
            Sq8Geometry {
                rows: ordinals.len(),
                dimensions: 2,
            },
            &ordinals,
            &normalized,
            &layout.manifest.low,
            &layout.manifest.step,
        )
        .unwrap();
        let mut oracle = router
            .cells
            .iter()
            .map(|cell| {
                let score = scores[cell.start..cell.start + cell.local_ordinals.len()]
                    .iter()
                    .map(|s| s.score)
                    .min_by(f32::total_cmp)
                    .unwrap();
                (score, cell.cell.id)
            })
            .collect::<Vec<_>>();
        oracle.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
        oracle.truncate(24);
        let options = SearchOptions {
            fetch_policy: FetchPolicy::WholeCell,
            primary_beam: 8,
            boundary_beam: 24,
            blocks_per_cell: 16,
            max_cells: 24,
            max_cell_gets: 24,
            max_cell_bytes: 16 * 1024 * 1024,
            max_source_gets: 24,
            max_source_bytes: 16 * 1024 * 1024,
            max_refinement_gets: 384,
            max_refinement_bytes: 16 * 1024 * 1024,
            max_query_payload_bytes: 128 * 1024 * 1024,
        };
        let mut capped = options;
        capped.max_cell_bytes = 1;
        let error = layout
            .search_probed(&router, &query, 100, capped)
            .err()
            .unwrap();
        assert_eq!(error.accounting.whole_cell.submitted_gets, 0);
        assert_eq!(error.accounting.source.submitted_gets, 0);
        assert!(error.complete.wall_ns > 0);
        let full = layout.search_probed(&router, &query, 100, options).unwrap();
        assert_eq!(full.trace.covered_ids, full.trace.nominated_ids);
        assert_eq!(full.trace.accounting.whole_cell.submitted_gets, 24);
        assert_eq!(full.trace.accounting.source.submitted_gets, 0);
        let mut all_ranges = Vec::new();
        for selected in &full.nomination.selected {
            let probe = &router.cells[selected.cell_id];
            let records = read_at(
                &layout.cells,
                probe.cell.source.offset + probe.cell.source.bytes,
                probe.source_ids.len() * 14,
            )
            .unwrap();
            all_ranges.push((probe.cell.first_row * 14, records));
        }
        all_ranges.sort_by_key(|v| v.0);
        let ranges = all_ranges
            .iter()
            .map(|(start, bytes)| ReturnedRange {
                start: *start,
                bytes,
            })
            .collect::<Vec<_>>();
        let native = rank_returned_ranges(
            Sq8Geometry {
                rows: layout.rows(),
                dimensions: 2,
            },
            &ranges,
            &normalized,
            &layout.manifest.low,
            &layout.manifest.step,
            100,
            16 * 1024 * 1024,
        )
        .unwrap();
        assert_eq!(
            full.trace
                .returned
                .iter()
                .map(|r| (r.id, r.score.to_bits()))
                .collect::<Vec<_>>(),
            native
                .iter()
                .map(|r| (r.id, r.score.to_bits()))
                .collect::<Vec<_>>()
        );
        assert!(full.complete.wall_ns >= full.nomination.complete.wall_ns);
        drop(layout);
        // Remove every source input, layout and the sidecar. The resident router
        // still routes/scores/returns rosters without any surviving file handle.
        for entry in fs::read_dir(temp.path()).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                fs::remove_dir_all(path).unwrap();
            } else {
                fs::remove_file(path).unwrap();
            }
        }
        let nominated = router.nominate(&query, source_probe_limits()).unwrap();
        assert_eq!(
            router.select(&query).unwrap(),
            oracle.iter().map(|v| v.1).collect::<Vec<_>>()
        );
        assert_eq!(
            nominated
                .selected
                .iter()
                .map(|v| (v.score_bits, v.cell_id))
                .collect::<Vec<_>>(),
            oracle
                .iter()
                .map(|v| (v.0.to_bits(), v.1))
                .collect::<Vec<_>>()
        );
        assert_eq!(nominated.accounting.whole_cell.submitted_gets, 0);
        assert_eq!(nominated.accounting.source.submitted_gets, 0);
        assert!(nominated.accounting.waves.is_empty());
        assert_eq!(nominated.selected_fetch_gets, 24);
    }
    #[test]
    fn source_probe_cap_binding_tamper_missing_id_fifo_and_symlink_fail_closed() {
        let temp = tempfile::tempdir().unwrap();
        let mut layout = source_probe_fixture(temp.path(), 1024);
        let artifact = build_source_probes(&layout, &temp.path().join("probes.bin"))
            .unwrap()
            .artifact;
        let missing = Artifact {
            path: temp.path().join("absent"),
            ..artifact.clone()
        };
        let error = SourceProbeRouter::open(&layout, &missing, 1).err().unwrap();
        assert_eq!(error.startup.submitted_gets, 0);
        let body = fs::read(&artifact.path).unwrap();
        let mut changed = body.clone();
        changed[0] ^= 1;
        fs::write(&artifact.path, &changed).unwrap();
        let error = SourceProbeRouter::open(&layout, &artifact, 128 * 1024 * 1024)
            .err()
            .unwrap();
        assert_eq!(error.startup.submitted_gets, 1);
        assert_eq!(error.startup.failed_gets, 1);
        let changed_artifact = Artifact {
            sha256: hash(&changed),
            ..artifact.clone()
        };
        assert!(SourceProbeRouter::open(&layout, &changed_artifact, 128 * 1024 * 1024).is_err());
        changed = body.clone();
        let roster = 104 + layout.dimensions() * 8 + 16;
        changed[roster..roster + 8].copy_from_slice(&(-1_i64).to_le_bytes());
        fs::write(&artifact.path, &changed).unwrap();
        assert!(
            SourceProbeRouter::open(
                &layout,
                &Artifact {
                    sha256: hash(&changed),
                    ..artifact.clone()
                },
                128 * 1024 * 1024
            )
            .is_err()
        );
        fs::write(&artifact.path, &body).unwrap();
        layout.admitted_root_sha256 = "0".repeat(64);
        assert!(SourceProbeRouter::open(&layout, &artifact, 128 * 1024 * 1024).is_err());
        let link = temp.path().join("probe-link");
        std::os::unix::fs::symlink(&artifact.path, &link).unwrap();
        assert!(
            read_source_probe_artifact(
                &Artifact {
                    path: link,
                    ..artifact.clone()
                },
                PROBE_CAP
            )
            .is_err()
        );
        let fifo = temp.path().join("probe-fifo");
        assert!(
            std::process::Command::new("mkfifo")
                .arg(&fifo)
                .status()
                .unwrap()
                .success()
        );
        assert!(
            read_source_probe_artifact(
                &Artifact {
                    path: fifo,
                    ..artifact
                },
                PROBE_CAP
            )
            .is_err()
        );
    }

    #[test]
    fn source_probe_shortcell_builder_and_original_span_tamper() {
        let temp = tempfile::tempdir().unwrap();
        let layout = source_probe_fixture(temp.path(), 3);
        let built = build_source_probes(&layout, &temp.path().join("short.bin")).unwrap();
        let router = SourceProbeRouter::open(&layout, &built.artifact, 128 * 1024 * 1024).unwrap();
        assert_eq!(router.cells.len(), 1);
        assert_eq!(router.cells[0].local_ordinals.len(), 3);
        assert!(router.select(&[3.125, 0.875]).is_err());
        let path = temp.path().join("layout-probes/cells.bin");
        let mut bytes = fs::read(&path).unwrap();
        bytes[0] ^= 1;
        fs::write(path, bytes).unwrap();
        let rejected = temp.path().join("tampered-source.bin");
        let error = build_source_probes(&layout, &rejected).err().unwrap();
        assert_eq!(error.accounting.whole_cell.submitted_gets, 1);
        assert_eq!(error.accounting.whole_cell.failed_gets, 1);
        assert_eq!(error.accounting.whole_cell.verified_bytes, 0);
        assert!(!rejected.exists());
        // Explicit sixteen-block cells catch accidental retention of the old
        // four-block nominee arm. Keep this in the existing mandatory test.
        let multi = temp.path().join("sixteen-block");
        fs::create_dir(&multi).unwrap();
        let mut config = fixture_with_mean(&multi, 31 * 512 + 33, 2, true, &[0.2, 0.1]);
        config.cell_rows = 512;
        let output = multi.join("layout");
        fs::create_dir(&output).unwrap();
        let codec = RotatedTwoBitCodec::new(&[0.2, 0.1], NATIVE_CODEC_SEED).unwrap();
        let code = codec.encode(&[1., 0.]).unwrap();
        let mut cell_body = Vec::new();
        let mut nodes = Vec::new();
        let mut first = 0;
        let append = |body: &mut Vec<u8>, bytes: &[u8]| {
            let span = Span {
                offset: body.len(),
                bytes: bytes.len(),
                sha256: hash(bytes),
            };
            body.extend_from_slice(bytes);
            span
        };
        for id in 0..32 {
            let rows = if id == 1 { 33 } else { 512 };
            let mut source = Vec::new();
            let mut raw = Vec::new();
            for ordinal in first..first + rows {
                source.extend_from_slice(&(ordinal as i64).to_le_bytes());
                source.extend_from_slice(&code);
                raw.extend_from_slice(&(ordinal as i64).to_le_bytes());
                raw.extend_from_slice(&1_f32.to_le_bytes());
                raw.extend_from_slice(&[1, 0]);
            }
            let mut whole = source.clone();
            whole.extend_from_slice(&raw);
            let whole = Span {
                offset: cell_body.len(),
                bytes: whole.len(),
                sha256: hash(&whole),
            };
            let source = append(&mut cell_body, &source);
            let refinement = raw
                .chunks(BLOCK_ROWS * 14)
                .map(|b| append(&mut cell_body, b))
                .collect();
            nodes.push(Node {
                rows,
                prototype: vec![1., 0.],
                target: Target::Cell {
                    cell: Cell {
                        id,
                        first_row: first,
                        whole,
                        source,
                        refinement,
                    },
                },
            });
            first += rows;
        }
        let mut directories = Vec::new();
        let mut pages = 0;
        while nodes.len() > 1 {
            nodes = nodes
                .chunks(2)
                .map(|children| {
                    pages += 1;
                    let body = serde_json::to_vec(&Directory {
                        children: children.to_vec(),
                    })
                    .unwrap();
                    Node {
                        rows: children.iter().map(|n| n.rows).sum(),
                        prototype: vec![1., 0.],
                        target: Target::Directory {
                            span: append(&mut directories, &body),
                        },
                    }
                })
                .collect();
        }
        let Target::Directory {
            span: root_directory,
        } = nodes.pop().unwrap().target
        else {
            unreachable!()
        };
        let manifest = Manifest {
            schema: SCHEMA.into(),
            input: config,
            rows: first,
            dimensions: 2,
            seed: NATIVE_CODEC_SEED,
            mean: vec![0.2, 0.1],
            low: vec![0., 0.],
            step: vec![1., 1.],
            root_directory,
            directory_bytes: directories.len(),
            directory_sha256: hash(&directories),
            cell_bytes: cell_body.len(),
            build: BuildReceipt {
                cells: 32,
                directories: pages,
                max_cell_rows: 512,
                max_depth: 5,
                ..Default::default()
            },
        };
        fs::write(output.join("cells.bin"), cell_body).unwrap();
        fs::write(output.join("directories.bin"), directories).unwrap();
        let root = serde_json::to_vec(&manifest).unwrap();
        fs::write(output.join("manifest.json"), &root).unwrap();
        let descriptor = Artifact {
            path: output.join("manifest.json"),
            bytes: root.len(),
            sha256: hash(&root),
        };
        let layout = Prototype::open_for_source_probes(&descriptor, 128 * 1024 * 1024).unwrap();
        let probes = build_source_probes(&layout, &multi.join("probes.bin"))
            .unwrap()
            .artifact;
        let router = SourceProbeRouter::open(&layout, &probes, 128 * 1024 * 1024).unwrap();
        assert_eq!(router.cells.len(), 32);
        assert_eq!(router.cells[0].cell.refinement.len(), 16);
        assert_eq!(router.cells[1].cell.refinement.len(), 2);
        assert_eq!(router.cells[1].cell.refinement[1].bytes, 14);
        let options = SearchOptions {
            fetch_policy: FetchPolicy::WholeCell,
            primary_beam: 8,
            boundary_beam: 24,
            blocks_per_cell: 16,
            max_cells: 24,
            max_cell_gets: 24,
            max_cell_bytes: 16 * 1024 * 1024,
            max_source_gets: 24,
            max_source_bytes: 16 * 1024 * 1024,
            max_refinement_gets: 384,
            max_refinement_bytes: 16 * 1024 * 1024,
            max_query_payload_bytes: 128 * 1024 * 1024,
        };
        let trace = layout
            .search_probed(&router, &[3.125, 0.875], 100, options)
            .unwrap();
        assert_eq!(trace.trace.accounting.whole_cell.submitted_gets, 24);
        assert_eq!(trace.trace.accounting.refinement.submitted_gets, 0);
        assert_eq!(trace.trace.covered_ids.len(), 23 * 512 + 33);
        assert_eq!(trace.trace.nominated_ids, trace.trace.covered_ids);
        let normalized = cosine_vector(&[3.125, 0.875]).unwrap().into_owned();
        let mut original = trace
            .nomination
            .selected
            .iter()
            .map(|choice| {
                let cell = &router.cells[choice.cell_id];
                (
                    cell.cell.first_row * 14,
                    read_at(
                        &layout.cells,
                        cell.cell.source.offset + cell.cell.source.bytes,
                        cell.source_ids.len() * 14,
                    )
                    .unwrap(),
                )
            })
            .collect::<Vec<_>>();
        original.sort_by_key(|v| v.0);
        let ranges = original
            .iter()
            .map(|(start, bytes)| ReturnedRange {
                start: *start,
                bytes,
            })
            .collect::<Vec<_>>();
        let independent = rank_returned_ranges(
            Sq8Geometry {
                rows: layout.rows(),
                dimensions: 2,
            },
            &ranges,
            &normalized,
            &layout.manifest.low,
            &layout.manifest.step,
            100,
            16 * 1024 * 1024,
        )
        .unwrap();
        assert_eq!(
            trace
                .trace
                .returned
                .iter()
                .map(|r| (r.id, r.score.to_bits()))
                .collect::<Vec<_>>(),
            independent
                .iter()
                .map(|r| (r.id, r.score.to_bits()))
                .collect::<Vec<_>>()
        );
        let mut old = options;
        old.blocks_per_cell = 4;
        let error = layout
            .search_probed(&router, &[3.125, 0.875], 100, old)
            .err()
            .unwrap();
        assert_eq!(error.accounting.whole_cell.submitted_gets, 0);
    }

    #[test]
    fn overlap_centroid_capacity_coordinate_tie_outsider_predicates() {
        let ordinary = SplitBoundary::Centers { left: vec![0.], right: vec![2.],
            separation: 4., cut: None };
        assert!(ordinary.goes_left(&[1.], 100).unwrap());
        assert!(!ordinary.goes_left(&[2.], 0).unwrap());
        let shifted = SplitBoundary::Centers { left: vec![0.], right: vec![2.],
            separation: 4., cut: Some((4., 7)) };
        assert!(shifted.goes_left(&[2.], 7).unwrap());
        assert!(!shifted.goes_left(&[2.], 8).unwrap());
        assert_eq!(shifted.margin(&[2.]).unwrap(), Some(0.));
        assert_eq!(ordinary.margin(&[2.]).unwrap(), Some(1.));
        let coordinate = SplitBoundary::Coordinate { axis: 0, cut: (-0., 3), range: 1. };
        assert!(coordinate.goes_left(&[-0.], 3).unwrap());
        assert!(!coordinate.goes_left(&[0.], 0).unwrap());
        let zero = SplitBoundary::Centers { left: vec![0.], right: vec![2.],
            separation: 4., cut: Some((-0., 7)) };
        assert!(zero.goes_left(&[1.], 7).unwrap());
        assert!(!zero.goes_left(&[1.], 8).unwrap());
    }

    #[test]
    fn overlap_replay_primary_parity_and_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let config = fixture(dir.path(), 40, 2, false);
        let output = dir.path().join("retained");
        let receipt = build(&config, &output).unwrap();
        let retained = Prototype::open(&output, &receipt.root_sha256, 16 * 1024 * 1024).unwrap();
        let replay = replay_primary(&config, &retained, dir.path()).unwrap();
        assert_eq!(replay.logical_rows, 40);
        assert_eq!(replay.nodes.iter().filter_map(|n| n.cell.as_ref()).map(|c| c.ids.len()).sum::<usize>(), 40);
        let mut wrong = config.clone();
        wrong.cell_rows += 1;
        assert!(replay_primary(&wrong, &retained, dir.path()).is_err());
    }

    #[test]
    fn overlap_builder_captures_actual_capacity_last_left_key() {
        let dir = tempfile::tempdir().unwrap();
        let vectors = (0..1025).map(|id| if id % 257 < 32 { vec![0., 1.] } else { vec![1., 0.] }).collect::<Vec<_>>();
        let mut config = fixture_with_vectors(dir.path(), &vectors, &[0., 0.], false);
        config.cell_rows = 512;
        let output = dir.path().join("primary");
        let receipt = build(&config, &output).unwrap();
        assert_eq!(receipt.semantic_repairs, 1);
        let retained = Prototype::open(&output, &receipt.root_sha256, 64 * 1024 * 1024).unwrap();
        let replay = replay_primary(&config, &retained, dir.path()).unwrap();
        let root = &replay.nodes[0];
        let boundary = root.boundary.as_ref().unwrap();
        let SplitBoundary::Centers { cut: Some(cut), .. } = boundary else { panic!("actual capacity cut missing"); };
        let mut stack = vec![root.children.unwrap()[0]];
        let mut left_ids = BTreeSet::new();
        while let Some(node) = stack.pop() {
            let node = &replay.nodes[node];
            if let Some(cell) = &node.cell { left_ids.extend(cell.ids.iter().copied()); }
            else { stack.extend(node.children.unwrap()); }
        }
        let mut last = None;
        for (id, vector) in vectors.iter().enumerate() {
            let delta = boundary.delta(vector).unwrap();
            let key = (if delta == 0. { 0. } else { delta }, id);
            assert_eq!(boundary.goes_left(vector, id).unwrap(), left_ids.contains(&id));
            if left_ids.contains(&id) && last.is_none_or(|v: (f32, usize)| key.0.total_cmp(&v.0).then(key.1.cmp(&v.1)).is_gt()) { last = Some(key); }
        }
        let last = last.unwrap();
        assert_eq!((cut.0.to_bits(), cut.1), (last.0.to_bits(), last.1));
        assert!(boundary.goes_left(&vectors[cut.1], cut.1).unwrap());
        assert!(!boundary.goes_left(&vectors[cut.1], vectors.len() + 1).unwrap());
        assert!(replay.nodes.iter().any(|n| matches!(&n.boundary, Some(SplitBoundary::Coordinate { .. }))));
    }

    fn overlap_fixture() -> (tempfile::TempDir, std::sync::Arc<crate::semantic_cell_overlap::OverlapIndex>) {
        use crate::semantic_cell_overlap::{OverlapBuildConfig, build_overlap, OverlapIndex};
        let dir = tempfile::tempdir().unwrap();
        let config = fixture(dir.path(), 80, 2, false);
        let primary = dir.path().join("primary");
        let receipt = build(&config, &primary).unwrap();
        let root = Artifact { path: primary.join("manifest.json"),
            bytes: fs::metadata(primary.join("manifest.json")).unwrap().len() as usize, sha256: receipt.root_sha256 };
        let candidate = dir.path().join("candidate");
        let c = OverlapBuildConfig { schema: crate::semantic_cell_overlap::BUILD_SCHEMA.into(), retained_root: root,
            original: config, overlap: true, max_resident_payload_bytes: 128 * 1024 * 1024,
            max_build_payload_bytes: 128 * 1024 * 1024, max_output_bytes: 16 * 1024 * 1024 };
        let receipt = build_overlap(&c, &candidate).unwrap();
        assert!(receipt.admitted > 0);
        let root = Artifact { path: candidate.join("manifest.json"),
            bytes: fs::metadata(candidate.join("manifest.json")).unwrap().len() as usize, sha256: receipt.root_sha256 };
        let index = std::sync::Arc::new(OverlapIndex::open(&root, 128 * 1024 * 1024).unwrap());
        (dir, index)
    }

    #[test]
    fn overlap_pinned_replace_owner_absent_replica_present() {
        use crate::semantic_cell_overlap::{OverlapRevisions, search_selected_sq8};
        let (_dir, index) = overlap_fixture();
        let (id, &(owner, destination)) = index.replica_destinations().iter().enumerate().find(|(_, p)| p.1.is_some()).unwrap();
        let destination = destination.unwrap();
        let query = [3.125, 0.875];
        let mut plan = index.plan(&query).unwrap();
        plan.selected.retain(|s| s.cell_id == destination);
        plan.extents.retain(|e| e.cell_id == destination);
        plan.bytes = plan.extents.iter().map(|e| e.bytes).sum();
        assert!(plan.extents.iter().all(|e| e.cell_id != owner));
        let mut publisher = OverlapRevisions::new(index.clone(), 80, 4096).unwrap();
        let old = publisher.pin();
        let before = search_selected_sq8(&old, &plan, &query, 80).unwrap();
        let old_bits = before.ranked.iter().find(|s| s.id == id as i64).unwrap().score_bits;
        let mut replacement = (id as i64).to_le_bytes().to_vec();
        replacement.extend_from_slice(&1000_f32.to_le_bytes());
        replacement.extend_from_slice(&[0, 0]);
        publisher.replace_sq8(&replacement).unwrap();
        let new = publisher.pin();
        let after = search_selected_sq8(&new, &plan, &query, 80).unwrap();
        let item = after.ranked.iter().find(|s| s.id == id as i64).unwrap();
        assert_ne!(item.score_bits, old_bits);
        assert_eq!(item.physical_ordinal, None);
        assert_eq!(after.ranked.iter().filter(|s| s.id == id as i64).count(), 1);
        assert_eq!(search_selected_sq8(&old, &plan, &query, 80).unwrap().ranked.iter().find(|s| s.id == id as i64).unwrap().score_bits, old_bits);
        assert_ne!(old.delta_sha256(), new.delta_sha256());
        assert_eq!(old.revision(), 0);
        assert_eq!(new.revision(), 1);
    }

    #[test]
    fn overlap_delete_all_underfill_and_capacity_refusal() {
        use crate::semantic_cell_overlap::{OverlapRevisions, search_selected_sq8};
        let (_dir, index) = overlap_fixture();
        let query = [3.125, 0.875];
        let plan = index.plan(&query).unwrap();
        let mut publisher = OverlapRevisions::new(index.clone(), 80, 4096).unwrap();
        let old = publisher.pin();
        for id in 0..80 { publisher.delete(id).unwrap(); }
        let new = publisher.pin();
        let trace = search_selected_sq8(&new, &plan, &query, 100).unwrap();
        assert!(trace.underfill && trace.ranked.is_empty());
        assert!(!search_selected_sq8(&old, &plan, &query, 100).unwrap().ranked.is_empty());
        let mut full = OverlapRevisions::new(index, 1, 32).unwrap();
        full.delete(0).unwrap();
        let frozen = full.pin();
        assert!(full.delete(1).is_err());
        assert_eq!(full.pin().revision(), frozen.revision());
        assert_eq!(full.pin().delta_sha256(), frozen.delta_sha256());
        assert!(full.delete(80).is_err());
        assert!(full.unsupported_maintenance().is_err());
    }

    #[test]
    fn overlap_full_scanner_matches_independent_sq8() {
        use crate::semantic_cell_overlap::{OverlapBuildConfig, OverlapIndex, OverlapRevisions, build_overlap, search_selected_sq8};
        // An independent scalar oracle: no production normalization, scoring,
        // decoding or ranking helper participates in the expected values.
        fn scalar_reference(body: &[u8], query: &[f32], low: &[f32], step: &[f32]) -> Vec<(i64, u32)> {
            let mut squared = 0_f64;
            for &value in query { squared += f64::from(value).powi(2); }
            let normalized = if (squared - 1.).abs() <= 1e-6 { query.to_vec() } else {
                let norm = squared.sqrt();
                query.iter().map(|&value| (f64::from(value) / norm) as f32).collect()
            };
            let mut shift = 0_f32;
            let mut qnorm = 0_f32;
            let mut weights = Vec::new();
            for coordinate in 0..query.len() {
                shift += normalized[coordinate] * low[coordinate];
                qnorm += normalized[coordinate] * normalized[coordinate];
                weights.push(normalized[coordinate] * step[coordinate]);
            }
            shift -= qnorm / 2.;
            let mut expected = Vec::new();
            for row in body.chunks_exact(query.len() + 12) {
                let id = i64::from_le_bytes(row[..8].try_into().unwrap());
                let stored_norm = f32::from_le_bytes(row[8..12].try_into().unwrap());
                let mut inner = 0_f32;
                for coordinate in 0..query.len() {
                    inner += f32::from(row[12 + coordinate]) * weights[coordinate];
                }
                let score = stored_norm - 2. * (inner + shift);
                expected.push((id, score));
            }
            expected.sort_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
            expected.into_iter().map(|(id, score)| (id, score.to_bits())).collect()
        }
        struct FrameOracle {
            owners: BTreeMap<i64, usize>,
            destinations: BTreeMap<i64, usize>,
            bodies: BTreeMap<i64, Vec<u8>>,
            representatives: BTreeMap<i64, (usize, usize, usize)>,
            selected_ids: BTreeSet<i64>,
            selected_replicas: BTreeSet<i64>,
            cells: usize,
        }
        // Parse the documented64-byte frames directly. Expected offsets and
        // physical ordinals never use Extent, validate_frame, mapping helpers,
        // production fetches or representative_location.
        fn decode_frames(body: &[u8], selected: &BTreeSet<usize>, logical: usize) -> FrameOracle {
            let mut oracle = FrameOracle { owners:BTreeMap::new(), destinations:BTreeMap::new(),
                bodies:BTreeMap::new(), representatives:BTreeMap::new(), selected_ids:BTreeSet::new(),
                selected_replicas:BTreeSet::new(), cells:0 };
            let mut offset = 0; let mut ordinal = 0;
            while offset < body.len() {
                let header = &body[offset..offset + 64];
                assert_eq!(&header[..8], b"BSOV0001");
                let field = |slot: usize| u64::from_le_bytes(header[8 + slot * 8..16 + slot * 8].try_into().unwrap()) as usize;
                let cell = field(0); let dimensions = field(1); let primary = field(2); let replicas = field(3);
                assert_eq!(cell, oracle.cells); assert_eq!(dimensions, 2);
                assert_eq!(field(4), ordinal); assert_eq!(field(5), logical); assert_eq!(field(6), 1);
                let width = dimensions + 12;
                for slot in 0..primary + replicas {
                    let framed_offset = offset + 64 + slot * width;
                    let row = &body[framed_offset..framed_offset + width];
                    let id = i64::from_le_bytes(row[..8].try_into().unwrap());
                    assert!(id >= 0 && (id as usize) < logical);
                    if let Some(previous) = oracle.bodies.get(&id) { assert_eq!(previous.as_slice(), row); }
                    else { oracle.bodies.insert(id, row.to_vec()); }
                    // Sequential physical decoding makes this the smallest
                    // global ordinal, even when that copy was not fetched.
                    oracle.representatives.entry(id).or_insert((ordinal + slot, cell, framed_offset));
                    if slot < primary { assert!(oracle.owners.insert(id, cell).is_none()); }
                    else { assert!(oracle.destinations.insert(id, cell).is_none()); }
                    if selected.contains(&cell) {
                        oracle.selected_ids.insert(id);
                        if slot >= primary { oracle.selected_replicas.insert(id); }
                    }
                }
                offset += 64 + (primary + replicas) * width;
                ordinal += primary + replicas; oracle.cells += 1;
            }
            assert_eq!(offset, body.len()); assert_eq!(oracle.owners.len(), logical);
            assert_eq!(ordinal, logical + oracle.destinations.len());
            oracle
        }
        for logical_rows in [80, 2048] {
        let dir = tempfile::tempdir().unwrap();
        let vectors = (0..logical_rows).map(|id| {
            if logical_rows == 2048 { return if id % 2 == 0 { vec![1., 0.] } else { vec![0., 1.] }; }
            let x = (id + 1) as f64; let y = (81 - id) as f64;
            let norm = (x * x + y * y).sqrt();
            vec![(x / norm) as f32, (y / norm) as f32]
        }).collect::<Vec<_>>();
        let mut original_config = fixture_with_vectors(dir.path(), &vectors, &[0.125, -0.25], true);
        // Exercise nonzero lows, nonunit steps and stored reconstructed norms
        // on the actual full-scanner fixture, with every input digest rebound.
        let low = [-0.125_f32, -0.25]; let step = [1_f32 / 127., 1_f32 / 191.];
        let mut sq8 = fs::read(&original_config.sq8.path).unwrap();
        for row in sq8.chunks_exact_mut(14) {
            let id = i64::from_le_bytes(row[..8].try_into().unwrap()) as usize;
            let mut norm = 0_f32;
            for coordinate in 0..2 {
                let code = ((vectors[id][coordinate] - low[coordinate]) / step[coordinate])
                    .round_ties_even().clamp(0., 255.) as u8;
                row[12 + coordinate] = code;
                let decoded = low[coordinate] + f32::from(code) * step[coordinate];
                norm += decoded * decoded;
            }
            row[8..12].copy_from_slice(&norm.to_le_bytes());
        }
        original_config.sq8 = artifact(&original_config.sq8.path, &sq8);
        let mut plane: serde_json::Value = serde_json::from_slice(&fs::read(&original_config.plane.path).unwrap()).unwrap();
        plane["sq8_sha256"] = serde_json::json!(original_config.sq8.sha256);
        original_config.plane = artifact(&original_config.plane.path, &serde_json::to_vec(&plane).unwrap());
        let mut generation: serde_json::Value = serde_json::from_slice(&fs::read(&original_config.generation.path).unwrap()).unwrap();
        generation["sq8_object_sha256"] = serde_json::json!(original_config.sq8.sha256);
        generation["sq8_object_key"] = serde_json::json!(format!("objects/{}", original_config.sq8.sha256));
        generation["plane_manifest_sha256"] = serde_json::json!(original_config.plane.sha256);
        generation["low"] = serde_json::json!(low); generation["step"] = serde_json::json!(step);
        original_config.generation = artifact(&original_config.generation.path, &serde_json::to_vec(&generation).unwrap());
        let primary = dir.path().join("primary");
        let primary_receipt = build(&original_config, &primary).unwrap();
        let retained_root = Artifact { path: primary.join("manifest.json"),
            bytes: fs::metadata(primary.join("manifest.json")).unwrap().len() as usize, sha256: primary_receipt.root_sha256 };
        let candidate_output = dir.path().join("candidate");
        let candidate_config = OverlapBuildConfig { schema: crate::semantic_cell_overlap::BUILD_SCHEMA.into(),
            retained_root, original: original_config, overlap: true, max_resident_payload_bytes: 128 * 1024 * 1024,
            max_build_payload_bytes: 128 * 1024 * 1024, max_output_bytes: 16 * 1024 * 1024 };
        let candidate_receipt = build_overlap(&candidate_config, &candidate_output).unwrap();
        let candidate_root = Artifact { path: candidate_output.join("manifest.json"),
            bytes: fs::metadata(candidate_output.join("manifest.json")).unwrap().len() as usize,
            sha256: candidate_receipt.root_sha256 };
        let candidate = std::sync::Arc::new(OverlapIndex::open(&candidate_root, 128 * 1024 * 1024).unwrap());
        let config = OverlapBuildConfig { schema: crate::semantic_cell_overlap::BUILD_SCHEMA.into(),
            retained_root: candidate.primary_root_identity().clone(), original: candidate.router().source_identity().clone(),
            overlap: false, max_resident_payload_bytes: 128 * 1024 * 1024,
            max_build_payload_bytes: 128 * 1024 * 1024, max_output_bytes: 16 * 1024 * 1024 };
        let output = dir.path().join("control");
        let receipt = build_overlap(&config, &output).unwrap();
        let root = Artifact { path: output.join("manifest.json"), bytes: fs::metadata(output.join("manifest.json")).unwrap().len() as usize,
            sha256: receipt.root_sha256 };
        let control = std::sync::Arc::new(OverlapIndex::open(&root, 128 * 1024 * 1024).unwrap());
        assert_eq!(control.physical_rows(), control.logical_rows());
        let query = [3.125, 0.875];
        let a = control.plan(&query).unwrap(); let b = candidate.plan(&query).unwrap();
        assert_eq!(a.selected, b.selected);
        assert!(a.selected.len() <= 32);
        if logical_rows == 80 { assert!(a.selected.len() < 24, "actual union must not require exactly24"); }
        let selected = a.selected.iter().map(|cell| cell.cell_id).collect::<BTreeSet<_>>();
        let control_oracle = decode_frames(&fs::read(output.join("sq8-cells.bin")).unwrap(), &selected, logical_rows);
        let candidate_oracle = decode_frames(&fs::read(candidate_output.join("sq8-cells.bin")).unwrap(), &selected, logical_rows);
        assert_eq!(control_oracle.owners, candidate_oracle.owners);
        assert!(control_oracle.destinations.is_empty());
        let mapping = fs::read(candidate_output.join("placement.bin")).unwrap();
        assert_eq!(mapping.len(), logical_rows * 16);
        for (id, row) in mapping.chunks_exact(16).enumerate() {
            let owner = u64::from_le_bytes(row[..8].try_into().unwrap()) as usize;
            let destination = u64::from_le_bytes(row[8..].try_into().unwrap());
            assert_eq!(control_oracle.owners[&(id as i64)], owner);
            assert_eq!(candidate_oracle.destinations.get(&(id as i64)).copied(),
                if destination == u64::MAX { None } else { Some(destination as usize) });
        }
        let cs = OverlapRevisions::new(control.clone(), 0, 0).unwrap().pin();
        let ts = OverlapRevisions::new(candidate.clone(), 0, 0).unwrap().pin();
        let ct = search_selected_sq8(&cs, &a, &query, logical_rows).unwrap();
        let tt = search_selected_sq8(&ts, &b, &query, logical_rows).unwrap();
        assert!(ct.base_ids.iter().copied().eq(control_oracle.selected_ids.iter().copied()));
        assert!(tt.base_ids.iter().copied().eq(candidate_oracle.selected_ids.iter().copied()));
        assert!(tt.replica_ids.iter().copied().eq(candidate_oracle.selected_replicas.iter().copied()));
        let expected_union = control_oracle.selected_ids.union(&candidate_oracle.selected_replicas).copied().collect::<BTreeSet<_>>();
        assert_eq!(candidate_oracle.selected_ids, expected_union);
        if logical_rows == 2048 {
            assert!(selected.len() < candidate_oracle.cells);
            assert!(control_oracle.selected_ids.len() < logical_rows);
            let added = candidate_oracle.selected_ids.difference(&control_oracle.selected_ids).copied().collect::<Vec<_>>();
            assert!(!added.is_empty(), "selected replicas must supply owner-absent coverage");
            for id in &added {
                assert!(!selected.contains(&candidate_oracle.owners[id]));
                assert!(selected.contains(&candidate_oracle.destinations[id]));
            }
            assert!(tt.ranked.iter().any(|row| !selected.contains(&candidate_oracle.representatives[&row.id].1)),
                "global representative may reside in an unselected frame");
        } else { assert_eq!(ct.base_ids, tt.base_ids); }
        let original = config.original.sq8.read(config.original.sq8.bytes).unwrap();
        for row in original.chunks_exact(14) {
            let id = i64::from_le_bytes(row[..8].try_into().unwrap());
            assert_eq!(candidate_oracle.bodies[&id].as_slice(), row);
            assert_eq!(control_oracle.bodies[&id].as_slice(), row);
        }
        let (retained_low, retained_step) = control.router().sq8_coefficients();
        assert_eq!(retained_low.iter().map(|v| v.to_bits()).collect::<Vec<_>>(), low.map(f32::to_bits));
        assert_eq!(retained_step.iter().map(|v| v.to_bits()).collect::<Vec<_>>(), step.map(f32::to_bits));
        let expected = scalar_reference(&original, &query, &low, &step);
        assert!(original.chunks_exact(14).any(|row| f32::from_le_bytes(row[8..12].try_into().unwrap()).to_bits() != 1_f32.to_bits()));
        // Preserve production parity as an additional comparison, independently
        // of the scalar expected IDs and bits above.
        let normalized = cosine_vector(&query).unwrap();
        let mut reference = crate::exact_sq8_nominee::score_nominees(&original,
            Sq8Geometry { rows: logical_rows, dimensions: 2 }, &(0..logical_rows).collect::<Vec<_>>(), &normalized, &low, &step).unwrap();
        reference.sort_by(|a, b| a.score.total_cmp(&b.score).then(a.id.cmp(&b.id)));
        assert_eq!(reference.iter().map(|s| (s.id, s.score.to_bits())).collect::<Vec<_>>(), expected);
        let control_expected = expected.iter().copied().filter(|row| control_oracle.selected_ids.contains(&row.0)).collect::<Vec<_>>();
        let candidate_expected = expected.iter().copied().filter(|row| candidate_oracle.selected_ids.contains(&row.0)).collect::<Vec<_>>();
        assert_eq!(ct.ranked.iter().map(|s| (s.id, s.score_bits)).collect::<Vec<_>>(), control_expected);
        assert_eq!(tt.ranked.iter().map(|s| (s.id, s.score_bits)).collect::<Vec<_>>(), candidate_expected);
        for (trace, oracle) in [(&ct, &control_oracle), (&tt, &candidate_oracle)] {
            for s in &trace.ranked {
                let (ordinal, _, offset) = oracle.representatives[&s.id];
                assert_eq!(s.physical_ordinal, Some(ordinal)); assert_eq!(s.framed_file_offset, Some(offset));
            }
        }
        }
    }

    #[test]
    fn overlap_authenticated_open_and_selected_fetch_refuse_corruption() {
        use crate::semantic_cell_overlap::{OverlapIndex, OverlapRevisions, search_selected_sq8};
        let (dir, index) = overlap_fixture();
        let root_path = dir.path().join("candidate/manifest.json");
        let root = Artifact { bytes: fs::metadata(&root_path).unwrap().len() as usize,
            sha256: index.root_sha256().into(), path: root_path };
        let mapping_path = dir.path().join("candidate/placement.bin");
        let mut mapping = fs::read(&mapping_path).unwrap();
        mapping[0] ^= 1; fs::write(&mapping_path, &mapping).unwrap();
        assert!(OverlapIndex::open(&root, 128 * 1024 * 1024).is_err());
        mapping[0] ^= 1; fs::write(&mapping_path, &mapping).unwrap();
        let query = [3.125, 0.875]; let plan = index.plan(&query).unwrap();
        let path = dir.path().join("candidate/sq8-cells.bin");
        let file = OpenOptions::new().write(true).open(path).unwrap();
        file.write_all_at(&[0xff], (plan.extents[0].offset + 64 + 8) as u64).unwrap();
        assert!(OverlapIndex::open(&root, 128 * 1024 * 1024).is_err());
        let snapshot = OverlapRevisions::new(index, 0, 0).unwrap().pin();
        let error = search_selected_sq8(&snapshot, &plan, &query, 100).unwrap_err();
        assert_eq!(error.accounting.payload.submitted_gets, 1);
        assert_eq!(error.accounting.payload.failed_gets, 1);
        assert_eq!(error.accounting.payload.requested_bytes, plan.extents[0].bytes);
    }

    #[test]
    fn overlap_streamed_original_auth_boundaries_and_mutation() {
        let dir = tempfile::tempdir().unwrap();
        let empty = artifact(&dir.path().join("empty"), &[]);
        assert!(empty.authenticate_streamed(308_000_000).is_err());
        for length in [1, 65535, 65536, 65537, 131072, 131073] {
            let body = (0..length).map(|i| (i % 251) as u8).collect::<Vec<_>>();
            let input = artifact(&dir.path().join(format!("stream-{length}")), &body);
            // A large admitted object cap is NOT an allocating-reader cap.
            input.authenticate_streamed(308_000_000).unwrap();
            input.authenticate_streamed(length).unwrap();
            assert!(input.authenticate_streamed(length - 1).err().unwrap().to_string().contains("artifact descriptor/cap"));
            let mut corrupt = body.clone(); corrupt[0] ^= 1;
            fs::write(&input.path, corrupt).unwrap();
            assert!(input.authenticate_streamed(308_000_000).err().unwrap().to_string().contains("artifact digest/length"));
            fs::write(&input.path, &body[..length - 1]).unwrap();
            assert!(input.authenticate_streamed(308_000_000).is_err());
            let mut grown = body.clone(); grown.push(0);
            fs::write(&input.path, grown).unwrap();
            assert!(input.authenticate_streamed(308_000_000).is_err());
            fs::write(&input.path, &body).unwrap();
        }
        let body = vec![0x5a; ARTIFACT_STREAM_BUFFER_BYTES + 1];
        let input = artifact(&dir.path().join("after-metadata"), &body);
        // Exercise stream EOF checks after exact metadata admission, through
        // the same stream core used by the final secure wrapper.
        let mut file = File::open(&input.path).unwrap();
        assert_eq!(file.metadata().unwrap().len(), input.bytes as u64);
        OpenOptions::new().write(true).open(&input.path).unwrap().set_len((input.bytes - 1) as u64).unwrap();
        assert!(authenticate_artifact_file(&mut file, input.bytes, &input.sha256).is_err());
        fs::write(&input.path, &body).unwrap();
        let mut file = File::open(&input.path).unwrap();
        assert_eq!(file.metadata().unwrap().len(), input.bytes as u64);
        let mut grown = body; grown.push(0);
        fs::write(&input.path, &grown).unwrap();
        assert!(authenticate_artifact_file(&mut file, input.bytes, &input.sha256).err().unwrap()
            .to_string().contains("artifact digest/length"));
        grown.pop(); fs::write(&input.path, &grown).unwrap();
        input.authenticate_streamed(308_000_000).unwrap();
        let link = dir.path().join("stream-link");
        std::os::unix::fs::symlink(&input.path, &link).unwrap();
        let symlink = Artifact { path:link, ..input };
        assert!(symlink.authenticate_streamed(308_000_000).is_err());
    }

    #[test]
    fn overlap_metadata_cap_refuses_before_missing_heavy_files() {
        use crate::semantic_cell_overlap::{OverlapAdmission, OverlapIndex};
        let (dir, index) = overlap_fixture();
        let root = Artifact { path: dir.path().join("candidate/manifest.json"),
            bytes: fs::metadata(dir.path().join("candidate/manifest.json")).unwrap().len() as usize,
            sha256: index.root_sha256().into() };
        let metadata = OverlapAdmission::read(&root, 128 * 1024 * 1024).unwrap();
        assert_eq!(metadata.primary_directory().sha256, index.router().manifest.directory_sha256);
        assert_eq!(metadata.primary_directory().bytes, index.router().manifest.directory_bytes);
        assert!(metadata.modeled_preload_peak_bytes() > metadata.modeled_metadata_payload_bytes());
        fs::remove_file(dir.path().join("primary/directories.bin")).unwrap();
        fs::remove_file(dir.path().join("primary/cells.bin")).unwrap();
        fs::remove_file(dir.path().join("candidate/sq8-cells.bin")).unwrap();
        fs::remove_file(dir.path().join("candidate/placement.bin")).unwrap();
        fs::remove_file(dir.path().join("candidate/boundaries.json")).unwrap();
        // Successful metadata admission must not touch ANY of these bodies.
        let checked = OverlapAdmission::read(&root, 128 * 1024 * 1024).unwrap();
        let refused = OverlapIndex::open(&root, checked.modeled_preload_peak_bytes() - 1).err().unwrap();
        assert!(refused.to_string().contains("overlap preload admission"));
        assert!(OverlapAdmission::read(&root, root.bytes * 8 - 1).err().unwrap().to_string().contains("metadata admission"));
        let overlap_body = fs::read(&root.path).unwrap();
        let mut malformed: serde_json::Value = serde_json::from_slice(&overlap_body).unwrap();
        malformed["cells"][0]["offset"] = serde_json::json!(1);
        let malformed_body = serde_json::to_vec(&malformed).unwrap();
        fs::write(&root.path, &malformed_body).unwrap();
        let malformed_root = Artifact { path: root.path.clone(), bytes: malformed_body.len(), sha256: hash(&malformed_body) };
        assert!(OverlapAdmission::read(&malformed_root, 128 * 1024 * 1024).err().unwrap()
            .to_string().contains("overlap metadata extent geometry"));
        fs::write(&root.path, overlap_body).unwrap();
        let path = dir.path().join("primary/manifest.json");
        let original = fs::read(&path).unwrap();
        let mut corrupt = original.clone(); corrupt[0] ^= 1;
        fs::write(&path, corrupt).unwrap();
        assert!(OverlapIndex::open_admitted(checked).err().unwrap().to_string().contains("probe artifact SHA256/EOF"),
            "metadata must be reauthenticated before instantiation");
        fs::write(&path, original).unwrap();
        fs::remove_file(&path).unwrap();
        assert!(OverlapAdmission::read(&root, 128 * 1024 * 1024).is_err());
        fs::remove_file(&root.path).unwrap();
        assert!(OverlapAdmission::read(&root, 128 * 1024 * 1024).is_err());
    }

    #[test]
    fn overlap_pair_individually_fit_combined_overcap_has_zero_heavy_opens() {
        use crate::semantic_cell_overlap::{OverlapAdmission, OverlapPairAdmission, OverlapBuildConfig, build_overlap};
        let (dir, candidate) = overlap_fixture();
        let config = OverlapBuildConfig { schema: crate::semantic_cell_overlap::BUILD_SCHEMA.into(),
            retained_root: candidate.primary_root_identity().clone(), original: candidate.router().source_identity().clone(),
            overlap: false, max_resident_payload_bytes: 128 * 1024 * 1024,
            max_build_payload_bytes: 128 * 1024 * 1024, max_output_bytes: 16 * 1024 * 1024 };
        let output = dir.path().join("control");
        let receipt = build_overlap(&config, &output).unwrap();
        let control_root = Artifact { path: output.join("manifest.json"), bytes: fs::metadata(output.join("manifest.json")).unwrap().len() as usize,
            sha256: receipt.root_sha256 };
        let candidate_root = Artifact { path: dir.path().join("candidate/manifest.json"),
            bytes: fs::metadata(dir.path().join("candidate/manifest.json")).unwrap().len() as usize,
            sha256: candidate.root_sha256().into() };
        let a = OverlapAdmission::read(&control_root, 128 * 1024 * 1024).unwrap();
        let b = OverlapAdmission::read(&candidate_root, 128 * 1024 * 1024).unwrap();
        let cap = a.modeled_preload_peak_bytes().max(b.modeled_preload_peak_bytes());
        assert!(OverlapAdmission::read(&control_root, cap).is_ok());
        assert!(OverlapAdmission::read(&candidate_root, cap).is_ok());
        fs::write(dir.path().join("primary/directories.bin"), b"corrupt body must remain unread").unwrap();
        fs::remove_file(dir.path().join("candidate/sq8-cells.bin")).unwrap();
        let spy = std::cell::Cell::new(0);
        let result = OverlapPairAdmission::read(&control_root, &candidate_root, cap, cap, 0, 0)
            .and_then(|admitted| { spy.set(spy.get() + 1); admitted.open() });
        assert_eq!(spy.get(), 0);
        assert!(result.err().unwrap().to_string().contains("paired resident/evaluator/query coexistence"));
        // Evaluator/query charges also refuse without reaching the opener.
        let result = OverlapPairAdmission::read(&control_root, &candidate_root, cap, 128 * 1024 * 1024, 128 * 1024 * 1024, 1)
            .and_then(|admitted| { spy.set(spy.get() + 1); admitted.open() });
        assert_eq!(spy.get(), 0);
        assert!(result.is_err());
        // Candidate metadata reauthentication precedes even the CONTROL body
        // opener: its corrupt directory would otherwise fail first.
        let pair = OverlapPairAdmission::read(&control_root, &candidate_root, cap, 128 * 1024 * 1024, 0, 0).unwrap();
        let mut body = fs::read(&candidate_root.path).unwrap(); body[0] ^= 1;
        fs::write(&candidate_root.path, body).unwrap();
        assert!(pair.open().err().unwrap().to_string().contains("probe artifact SHA256/EOF"));
    }

    fn artifact(path: &Path, bytes: &[u8]) -> Artifact {
        std::fs::write(path, bytes).unwrap();
        Artifact {
            path: path.into(),
            bytes: bytes.len(),
            sha256: hash(bytes),
        }
    }

    // Interleaved original positions make old 256-row closure an observable bug.
    fn fixture(dir: &Path, rows: usize, dimensions: usize, identical: bool) -> BuildConfig {
        fixture_with_mean(dir, rows, dimensions, identical, &vec![0_f32; dimensions])
    }

    fn fixture_with_mean(
        dir: &Path,
        rows: usize,
        dimensions: usize,
        identical: bool,
        mean: &[f32],
    ) -> BuildConfig {
        let vectors = (0..rows)
            .map(|id| {
                let mut vector = vec![0_f32; dimensions];
                vector[if identical { 0 } else { id % 2 }] = 1.;
                vector
            })
            .collect::<Vec<_>>();
        fixture_with_vectors(dir, &vectors, mean, false)
    }

    fn fixture_with_vectors(
        dir: &Path,
        vectors: &[Vec<f32>],
        mean: &[f32],
        quantized_sq8: bool,
    ) -> BuildConfig {
        let rows = vectors.len();
        let dimensions = mean.len();
        assert!(vectors.iter().all(|v| v.len() == dimensions));
        let codec = RotatedTwoBitCodec::new(mean, 20260923).unwrap();
        let order = (0..rows).rev().collect::<Vec<_>>();
        let mut canonical = Vec::new();
        let mut codes = Vec::new();
        let mut sq8 = Vec::new();
        for &id in &order {
            let vector = &vectors[id];
            canonical.extend_from_slice(&(id as i64).to_le_bytes());
            for value in vector {
                canonical.extend_from_slice(&value.to_le_bytes());
            }
            codes.extend_from_slice(&codec.encode(vector).unwrap());
            sq8.extend_from_slice(&(id as i64).to_le_bytes());
            if quantized_sq8 {
                // Native [0,1] SQ8 span: f32 ties-even codes and sequential
                // f32 reconstructed squared norm, matching stored rank inputs.
                let encoded = vector
                    .iter()
                    .map(|&v| (v * 255.).round_ties_even().clamp(0., 255.) as u8)
                    .collect::<Vec<_>>();
                let mut norm = 0_f32;
                for &code in &encoded {
                    let decoded = f32::from(code) * (1. / 255.);
                    norm += decoded * decoded;
                }
                assert!(norm.is_finite() && norm > 0.);
                sq8.extend_from_slice(&norm.to_le_bytes());
                sq8.extend(encoded);
            } else {
                // Preserve the existing binary fixture. Negative cancellation
                // rows are prototype-order evidence only, never ranking inputs.
                sq8.extend_from_slice(&1_f32.to_le_bytes());
                sq8.extend(vector.iter().map(|&v| v as u8));
            }
        }
        let canonical = artifact(&dir.join("canonical"), &canonical);
        let records = artifact(&dir.join("codes"), &codes);
        let sq8 = artifact(&dir.join("sq8"), &sq8);
        let order = artifact(
            &dir.join("order"),
            &order
                .iter()
                .flat_map(|&v| (v as u64).to_le_bytes())
                .collect::<Vec<_>>(),
        );
        let mean = artifact(
            &dir.join("mean"),
            &mean
                .iter()
                .flat_map(|value| value.to_le_bytes())
                .collect::<Vec<_>>(),
        );
        let plane = artifact(
            &dir.join("plane.json"),
            &serde_json::to_vec(&SourcePlaneReceipt {
                schema: "borsuk-two-bit-plane-v3".into(),
                rows,
                dimensions,
                seed: 20260923,
                record_bytes: codec.record_bytes(),
                source_sha256: "0".repeat(64),
                sq8_sha256: sq8.sha256.clone(),
                source_order_sha256: order.sha256.clone(),
                mean_sha256: mean.sha256.clone(),
                records_sha256: records.sha256.clone(),
                page_rows: 32,
                page_digest_sha256: "0".repeat(64),
                query_or_truth_used: false,
            })
            .unwrap(),
        );
        let generation = artifact(&dir.join("generation.json"), &serde_json::to_vec(&json!({
            "schema": crate::two_bit_generation::SCHEMA, "generation": 1, "base_epoch": 0,
            "plane_manifest_sha256": plane.sha256, "page_manifest_sha256": "0".repeat(64),
            "discovery": {"mode":"graph", "centroids_sha256":"0".repeat(64),
                "graph_sha256":"0".repeat(64), "graph_resident_bytes":1,
                "diverse_graph_sha256":"0".repeat(64), "diverse_graph_resident_bytes":1},
            "sq8_object_sha256": sq8.sha256, "sq8_object_key":format!("objects/{}",sq8.sha256), "sq8_etag":"fixture",
            "canonical": {"rows":rows,"dimensions":dimensions,"bytes":canonical.bytes,
                "sha256":canonical.sha256,"object_key":format!("objects/{}",canonical.sha256)},
            "low":vec![0_f32;dimensions],
            "step":vec![if quantized_sq8 { 1_f32 / 255. } else { 1. };dimensions]
        })).unwrap());
        BuildConfig {
            schema: BUILD_SCHEMA.into(),
            generation,
            plane,
            canonical,
            order,
            records,
            mean,
            sq8,
            cell_rows: 32,
            sample_rows: 32,
            max_depth: 24,
            max_build_payload_bytes: 64 * 1024 * 1024,
            max_output_bytes: 16 * 1024 * 1024,
        }
    }

    fn options() -> SearchOptions {
        SearchOptions {
            fetch_policy: FetchPolicy::TwoStage,
            primary_beam: 1,
            boundary_beam: 2,
            blocks_per_cell: 1,
            max_cells: 4,
            max_cell_gets: 4,
            max_cell_bytes: 1024 * 1024,
            max_source_gets: 4,
            max_source_bytes: 1024 * 1024,
            max_refinement_gets: 4,
            max_refinement_bytes: 1024 * 1024,
            max_query_payload_bytes: 64 * 1024 * 1024,
        }
    }

    fn nomination_limits() -> NominationLimits {
        NominationLimits {
            max_source_gets: 24,
            max_source_bytes: 1024 * 1024,
            max_query_payload_bytes: 64 * 1024 * 1024,
        }
    }

    #[test]
    fn capacity_partition_matches_exhaustive_cost_capacity_and_id_ties() {
        // An inverted cost sort, a rounded-down lower bound, or slot-based
        // ties must lose to this independently enumerated feasible optimum.
        for rows in 2_usize..=6 {
            let ids = (0..rows).rev().collect::<Vec<_>>();
            let lower = rows.div_ceil(4);
            for pattern in 0..3_usize.pow(rows as u32) {
                let mut word = pattern;
                let delta = (0..rows)
                    .map(|_| {
                        let value = (word % 3) as f32 - 1.;
                        word /= 3;
                        value
                    })
                    .collect::<Vec<_>>();
                let mut left = delta.iter().map(|d| *d <= 0.).collect::<Vec<_>>();
                let original = left.clone();
                let count = left.iter().filter(|v| **v).count();
                let repaired = capacity_partition(&ids, &delta, &mut left).unwrap();
                assert_eq!(repaired, count.min(rows - count) < lower);
                assert_eq!(
                    left.iter().filter(|v| **v).count(),
                    count.clamp(lower, rows - lower)
                );
                if !repaired {
                    assert_eq!(left, original);
                }
                let cost = left
                    .iter()
                    .zip(&delta)
                    .filter_map(|(&a, &d)| a.then_some(d))
                    .sum::<f32>();
                let optimum = (0..1_usize << rows)
                    .filter(|mask| (lower..=rows - lower).contains(&(mask.count_ones() as usize)))
                    .map(|mask| {
                        delta
                            .iter()
                            .enumerate()
                            .filter_map(|(slot, &d)| ((mask >> slot) & 1 == 1).then_some(d))
                            .sum::<f32>()
                    })
                    .min_by(f32::total_cmp)
                    .unwrap();
                assert_eq!(cost, optimum, "rows={rows} pattern={pattern}");
            }
        }
        let ids = [60, 10, 40, 30, 20, 70, 50];
        for (delta, expected) in [
            ([1.; 7], vec![10, 20]),
            ([-1.; 7], vec![10, 20, 30, 40, 50]),
            ([0.; 7], vec![10, 20, 30, 40, 50]),
            ([0., -0., 0., -0., 0., -0., 0.], vec![10, 20, 30, 40, 50]),
        ] {
            let mut left = delta.iter().map(|d| *d <= 0.).collect::<Vec<_>>();
            assert!(capacity_partition(&ids, &delta, &mut left).unwrap());
            let mut actual = ids
                .iter()
                .zip(left)
                .filter_map(|(&id, a)| a.then_some(id))
                .collect::<Vec<_>>();
            actual.sort_unstable();
            assert_eq!(actual, expected);
        }
        // Near one ULP, unequal exact-real margins can round to the same
        // f32 delta. Capacity ties must use IDs, not the lost low-order bits.
        let ids = [30, 10, 40, 20, 50, 60, 70];
        let right = [
            2_f32.powi(-26),
            2_f32.powi(-27),
            2_f32.powi(-26),
            2_f32.powi(-27),
            0.,
            0.,
            0.,
        ];
        let delta = right.map(|b| 1_f32 - b);
        assert_eq!(delta, [1.; 7]);
        assert!(1_f64 - f64::from(right[0]) < 1_f64 - f64::from(right[1]));
        let mut left = [false; 7];
        assert!(capacity_partition(&ids, &delta, &mut left).unwrap());
        assert_eq!(
            ids.iter()
                .zip(left)
                .filter_map(|(&id, a)| a.then_some(id))
                .collect::<Vec<_>>(),
            vec![10, 20]
        );

        let mut left = [true, false];
        assert!(capacity_partition(&[0, 1], &[f32::NAN, 1.], &mut left).is_err());
        assert!(capacity_partition(&[0, 1], &[f32::INFINITY, 1.], &mut left).is_err());
        assert!(capacity_partition(&[0, 1], &[0.], &mut left).is_err());
    }

    #[test]
    fn balanced_and_identical_builder_memberships_are_unchanged() {
        for identical in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let mut config = fixture(temp.path(), 64, 2, identical);
            config.sample_rows = 64;
            let output = temp.path().join("candidate");
            let built = build(&config, &output).unwrap();
            assert_eq!(built.semantic_repairs, 0);
            assert_eq!(built.geometry_fallbacks, usize::from(identical));
            let prototype = Prototype::open(&output, &built.root_sha256, 64 * 1024 * 1024).unwrap();
            let page = &prototype.directories[&prototype.manifest.root_directory.offset];
            let memberships = page
                .children
                .iter()
                .map(|node| {
                    let Target::Cell { cell } = &node.target else {
                        panic!("expected 32-row leaf")
                    };
                    read_at(&prototype.cells, cell.source.offset, cell.source.bytes)
                        .unwrap()
                        .chunks_exact(8 + prototype.codec.record_bytes())
                        .map(|row| i64::from_le_bytes(row[..8].try_into().unwrap()))
                        .collect::<Vec<_>>()
                })
                .collect::<BTreeSet<_>>();
            let expected = if identical {
                BTreeSet::from([(0..32).collect::<Vec<_>>(), (32..64).collect::<Vec<_>>()])
            } else {
                BTreeSet::from([
                    (0..64).step_by(2).collect::<Vec<_>>(),
                    (1..64).step_by(2).collect::<Vec<_>>(),
                ])
            };
            assert_eq!(memberships, expected);
        }
    }

    #[test]
    fn identical_sample_fallback_preserves_projected_child_sum_order() {
        // The two sampled rows are identical, but the complete node is not.
        // ID-order y sums erase the tiny term (1 + tiny - 1), while the old
        // coordinate order cancels first and retains it. Median membership
        // alone cannot protect prototype bytes from this rounding regression.
        let temp = tempfile::tempdir().unwrap();
        let mut vectors = vec![vec![1., 0.]; 11];
        vectors[1] = vec![0., 1.];
        vectors[2] = vec![1., 1e-20];
        vectors[3] = vec![0., -1.];
        vectors[4] = vec![-1., 0.];
        let mut config = fixture_with_vectors(temp.path(), &vectors, &[0., 0.], false);
        config.sample_rows = 2;
        config.cell_rows = 6;
        let output = temp.path().join("candidate");
        let built = build(&config, &output).unwrap();
        assert_eq!((built.semantic_repairs, built.geometry_fallbacks), (0, 1));
        let prototype = Prototype::open(&output, &built.root_sha256, 64 * 1024 * 1024).unwrap();
        let page = &prototype.directories[&prototype.manifest.root_directory.offset];
        assert_eq!(page.children[0].rows, 5);
        assert_eq!(page.children[1].rows, 6);
        assert_eq!(page.children[0].prototype[0].to_bits(), 0.2_f32.to_bits());
        let preserved = (f64::from(1e-20_f32) / 5.) as f32;
        assert_eq!(page.children[0].prototype[1].to_bits(), preserved.to_bits());
        assert_ne!(preserved, 0.);
    }

    #[test]
    fn skewed_builder_is_lossless_deterministic_and_preserves_sq2_sq8_tail_ranking() {
        let temp = tempfile::tempdir().unwrap();
        let vectors = (0..1025)
            .map(|id| {
                if id % 257 < 32 {
                    vec![0., 1.]
                } else {
                    vec![1., 0.]
                }
            })
            .collect::<Vec<_>>();
        let mut config = fixture_with_vectors(temp.path(), &vectors, &[0., 0.], false);
        config.cell_rows = 512;
        let a = temp.path().join("a");
        let b = temp.path().join("b");
        let first = build(&config, &a).unwrap();
        let second = build(&config, &b).unwrap();
        assert_eq!(first.root_sha256, second.root_sha256);
        for name in ["manifest.json", "directories.bin", "cells.bin"] {
            assert_eq!(
                fs::read(a.join(name)).unwrap(),
                fs::read(b.join(name)).unwrap()
            );
        }
        assert_eq!(first.semantic_repairs, 1);
        assert_eq!(first.geometry_fallbacks, 1);
        assert_eq!(first.cells, 3);
        assert!(first.max_cell_rows <= 512 && first.max_depth <= config.max_depth);
        let prototype = Prototype::open(&a, &first.root_sha256, 64 * 1024 * 1024).unwrap();
        let root = &prototype.directories[&prototype.manifest.root_directory.offset];
        let mut counts = root.children.iter().map(|v| v.rows).collect::<Vec<_>>();
        counts.sort_unstable();
        assert_eq!(counts, vec![257, 768]); // ceil(1025/4), not coordinate median.
        let codes = fs::read(&config.records.path).unwrap();
        let sq8 = fs::read(&config.sq8.path).unwrap();
        let mut seen = BTreeSet::new();
        let mut tail = false;
        for page in prototype.directories.values() {
            for node in &page.children {
                let Target::Cell { cell } = &node.target else {
                    continue;
                };
                let source =
                    read_at(&prototype.cells, cell.source.offset, cell.source.bytes).unwrap();
                let refinement = cell
                    .refinement
                    .iter()
                    .flat_map(|span| read_at(&prototype.cells, span.offset, span.bytes).unwrap())
                    .collect::<Vec<_>>();
                assert_eq!(
                    source.len(),
                    node.rows * (8 + prototype.codec.record_bytes())
                );
                assert_eq!(refinement.len(), node.rows * 14);
                tail |= cell.refinement.last().unwrap().bytes == 14;
                for (record, exact) in source
                    .chunks_exact(8 + prototype.codec.record_bytes())
                    .zip(refinement.chunks_exact(14))
                {
                    let id = i64::from_le_bytes(record[..8].try_into().unwrap());
                    assert!(seen.insert(id));
                    let physical = 1024 - id as usize; // independent reversed input permutation.
                    assert_eq!(
                        &record[8..],
                        &codes[physical * prototype.codec.record_bytes()
                            ..(physical + 1) * prototype.codec.record_bytes()]
                    );
                    assert_eq!(exact, &sq8[physical * 14..(physical + 1) * 14]);
                }
            }
        }
        assert_eq!(seen, (0..1025_i64).collect::<BTreeSet<_>>());
        assert!(tail); // the 257-row constrained child ends with one SQ8 row.
        let mut all = options();
        all.boundary_beam = 8;
        all.blocks_per_cell = 16;
        all.max_cells = 8;
        all.max_source_gets = 8;
        all.max_refinement_gets = 64;
        let two_stage = prototype.search(&[1., 0.], 1025, all).unwrap();
        all.fetch_policy = FetchPolicy::WholeCell;
        all.max_cell_gets = 8;
        let whole = prototype.search(&[1., 0.], 1025, all).unwrap();
        let expected = (0..1025_i64)
            .filter(|id| id % 257 >= 32)
            .chain((0..1025_i64).filter(|id| id % 257 < 32))
            .collect::<Vec<_>>();
        for trace in [&two_stage, &whole] {
            assert_eq!(trace.covered_ids, (0..1025_i64).collect::<Vec<_>>());
            assert_eq!(trace.nominated_ids, trace.covered_ids);
            assert_eq!(
                trace.returned.iter().map(|r| r.id).collect::<Vec<_>>(),
                expected
            );
            for row in &trace.returned {
                // Unchanged SQ8 score is squared L2: orthogonal unit rows cost 2.
                assert_eq!(row.score, if row.id % 257 < 32 { 2. } else { 0. });
            }
        }
        assert_eq!(two_stage.accounting.refinement.verified_bytes, 1025 * 14);

        // With one block/cell, tied major-mode codes choose block zero. The
        // repaired mixed cell has IDs 32..160, and the 768 remaining major IDs
        // split at 384; those three independently derived first blocks follow.
        let expected_nominees = (32..64_i64)
            .chain(161..193)
            .chain(609..641)
            .collect::<Vec<_>>();
        let mut restricted = all;
        restricted.fetch_policy = FetchPolicy::TwoStage;
        restricted.blocks_per_cell = 1;
        let selective_two = prototype.search(&[1., 0.], 1025, restricted).unwrap();
        restricted.fetch_policy = FetchPolicy::WholeCell;
        let selective_whole = prototype.search(&[1., 0.], 1025, restricted).unwrap();
        for trace in [&selective_two, &selective_whole] {
            assert_eq!(trace.nominated_ids, expected_nominees);
            assert!(trace.nominated_ids.len() < trace.covered_ids.len());
            assert_eq!(
                trace.returned.iter().map(|r| r.id).collect::<Vec<_>>(),
                expected_nominees
            );
            assert!(trace.returned.iter().all(|r| r.score == 0.));
        }
        let ranking = |trace: &SearchTrace| {
            trace
                .returned
                .iter()
                .map(|r| (r.ordinal, r.id, r.score.to_bits()))
                .collect::<Vec<_>>()
        };
        assert_eq!(ranking(&selective_two), ranking(&selective_whole));
        assert_eq!(selective_two.accounting.refinement.verified_bytes, 96 * 14);

        // Distinct normalized rows produce nonconstant margins. Minority rows
        // at either end of the full trainer sample exercise either center.
        // This oracle repairs by single cheapest exchanges, never calling the
        // partition helper or sorting its full delta permutation.
        for minority_first in [true, false] {
            let case = tempfile::tempdir().unwrap();
            let vectors = (0..129)
                .map(|id| {
                    let minority = if minority_first { id < 8 } else { id >= 121 };
                    let angle = if minority {
                        1.15 + (id % 8) as f64 * 0.002
                    } else {
                        0.10 + id as f64 * 0.0005
                    };
                    let (y, x) = angle.sin_cos();
                    cosine_vector(&[x as f32, y as f32]).unwrap().into_owned()
                })
                .collect::<Vec<_>>();
            assert!(vectors.windows(2).all(|w| w[0] != w[1]));
            let centers =
                train_logical_cell_centroids(&vectors, VectorMetric::SquaredEuclidean, 2, 4)
                    .unwrap();
            assert_eq!(centers[0][0] < 0.6, minority_first);
            let delta = vectors
                .iter()
                .map(|row| {
                    VectorMetric::SquaredEuclidean
                        .distance(row, &centers[0])
                        .unwrap()
                        - VectorMetric::SquaredEuclidean
                            .distance(row, &centers[1])
                            .unwrap()
                })
                .collect::<Vec<_>>();
            assert!(delta.windows(2).any(|w| w[0] != w[1]));
            let mut expected_left = delta.iter().map(|d| *d <= 0.).collect::<Vec<_>>();
            let count = expected_left.iter().filter(|a| **a).count();
            assert_eq!(count, if minority_first { 8 } else { 121 });
            let target: usize = if minority_first { 33 } else { 96 };
            for _ in target.min(count)..target.max(count) {
                let id = if count < target {
                    (0..129)
                        .filter(|&id| !expected_left[id])
                        .min_by(|&a, &b| delta[a].total_cmp(&delta[b]).then(a.cmp(&b)))
                        .unwrap()
                } else {
                    (0..129)
                        .filter(|&id| expected_left[id])
                        .max_by(|&a, &b| delta[a].total_cmp(&delta[b]).then(a.cmp(&b)))
                        .unwrap()
                };
                expected_left[id] = count < target;
            }
            let expected_left = expected_left
                .iter()
                .enumerate()
                .filter_map(|(id, &a)| a.then_some(id))
                .collect::<BTreeSet<_>>();
            assert_eq!(expected_left.len(), target);
            assert!(expected_left.iter().all(|&a| {
                (0..129)
                    .filter(|b| !expected_left.contains(b))
                    .all(|b| delta[a] <= delta[b])
            }));
            let mut config = fixture_with_vectors(case.path(), &vectors, &[0., 0.], true);
            config.cell_rows = 64;
            config.sample_rows = 129;
            let output = case.path().join("candidate");
            let built = build(&config, &output).unwrap();
            assert!(built.semantic_repairs > 0 && built.max_cell_rows <= 64);
            let candidate = Prototype::open(&output, &built.root_sha256, 64 * 1024 * 1024).unwrap();
            let root = &candidate.directories[&candidate.manifest.root_directory.offset];
            assert_eq!(root.children[0].rows, target);
            let mut actual_left = BTreeSet::new();
            let mut pending = vec![&root.children[0]];
            while let Some(node) = pending.pop() {
                match &node.target {
                    Target::Directory { span } => {
                        pending.extend(&candidate.directories[&span.offset].children)
                    }
                    Target::Cell { cell } => {
                        let source =
                            read_at(&candidate.cells, cell.source.offset, cell.source.bytes)
                                .unwrap();
                        actual_left.extend(
                            source
                                .chunks_exact(8 + candidate.codec.record_bytes())
                                .map(|r| i64::from_le_bytes(r[..8].try_into().unwrap()) as usize),
                        );
                    }
                }
            }
            assert_eq!(actual_left, expected_left);

            let query = [1., 0.];
            let prepared = candidate.codec.prepare_query(&query, 400_000).unwrap();
            let mut expected_nominees = BTreeSet::new();
            let mut partial = false;
            for page in candidate.directories.values() {
                for node in &page.children {
                    let Target::Cell { cell } = &node.target else {
                        continue;
                    };
                    let source =
                        read_at(&candidate.cells, cell.source.offset, cell.source.bytes).unwrap();
                    let records = source
                        .chunks_exact(8 + candidate.codec.record_bytes())
                        .collect::<Vec<_>>();
                    // The best individual SQ2 row picks the maximum-score block;
                    // row-order ties independently choose the earlier block.
                    let (best, _) = records
                        .iter()
                        .enumerate()
                        .map(|(row, record)| (row, prepared.score(&record[8..]).unwrap()))
                        .max_by(|a, b| a.1.total_cmp(&b.1).then(b.0.cmp(&a.0)))
                        .unwrap();
                    let first = best / BLOCK_ROWS * BLOCK_ROWS;
                    expected_nominees.extend(
                        records[first..(first + BLOCK_ROWS).min(records.len())]
                            .iter()
                            .map(|r| i64::from_le_bytes(r[..8].try_into().unwrap())),
                    );
                    partial |= records.len() > BLOCK_ROWS;
                }
            }
            assert!(partial && expected_nominees.len() < 129);
            let mut restricted = options();
            restricted.boundary_beam = 8;
            restricted.max_cells = 8;
            restricted.max_source_gets = 8;
            restricted.max_refinement_gets = 8;
            let two = candidate.search(&query, 129, restricted).unwrap();
            restricted.fetch_policy = FetchPolicy::WholeCell;
            restricted.max_cell_gets = 8;
            let whole = candidate.search(&query, 129, restricted).unwrap();
            let encoded = fs::read(&config.sq8.path).unwrap();
            let mut expected_ranking = Vec::new();
            for row in encoded.chunks_exact(14) {
                let id = i64::from_le_bytes(row[..8].try_into().unwrap());
                let norm = f32::from_le_bytes(row[8..12].try_into().unwrap());
                let x = f32::from(row[12]) * (1_f32 / 255.);
                let y = f32::from(row[13]) * (1_f32 / 255.);
                assert_eq!(norm.to_bits(), (x * x + y * y).to_bits());
                if expected_nominees.contains(&id) {
                    expected_ranking.push((id, norm - 2. * (x - 0.5)));
                }
            }
            expected_ranking.sort_unstable_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
            let expected_nominees = expected_nominees.into_iter().collect::<Vec<_>>();
            for trace in [&two, &whole] {
                assert_eq!(trace.covered_ids, (0..129_i64).collect::<Vec<_>>());
                assert_eq!(trace.nominated_ids, expected_nominees);
                assert_eq!(
                    trace
                        .returned
                        .iter()
                        .map(|r| (r.id, r.score.to_bits()))
                        .collect::<Vec<_>>(),
                    expected_ranking
                        .iter()
                        .map(|&(id, score)| (id, score.to_bits()))
                        .collect::<Vec<_>>()
                );
            }
            assert_eq!(ranking(&two), ranking(&whole));
        }
    }

    #[test]
    fn partition_format_and_receipt_reject_obsolete_or_incomplete_artifacts() {
        let temp = tempfile::tempdir().unwrap();
        let config = fixture(temp.path(), 4, 2, false);
        let mut obsolete = config.clone();
        obsolete.schema = "borsuk-hierarchical-cells-build-v1".into();
        assert!(build(&obsolete, &temp.path().join("obsolete")).is_err());
        let output = temp.path().join("candidate");
        build(&config, &output).unwrap();
        let path = output.join("manifest.json");
        let mut manifest: serde_json::Value =
            serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        manifest["schema"] = json!("borsuk-hierarchical-cells-resident-v3");
        let body = serde_json::to_vec(&manifest).unwrap();
        fs::write(&path, &body).unwrap();
        assert!(Prototype::open(&output, &hash(&body), 64 * 1024 * 1024).is_err());
        let mut receipt = serde_json::to_value(BuildReceipt::default()).unwrap();
        receipt.as_object_mut().unwrap().remove("semantic_repairs");
        assert!(serde_json::from_value::<BuildReceipt>(receipt).is_err());
    }

    #[test]
    fn nomination_global_finds_leaf_hidden_by_hierarchy_pruning() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = fixture(temp.path(), 64, 2, true);
        config.cell_rows = 1;
        let output = temp.path().join("candidate");
        let built = build(&config, &output).unwrap();
        let mut prototype = Prototype::open(&output, &built.root_sha256, 64 * 1024 * 1024).unwrap();
        // Synthetic routing geometry only: 32 two-leaf subtrees. Propagate
        // arithmetic means bottom-up, retaining the admitted topology/rosters.
        let mut means = BTreeMap::<usize, Vec<f32>>::new();
        let y = 0.75_f32.sqrt();
        for (&offset, page) in &mut prototype.directories {
            for node in &mut page.children {
                node.prototype = match &node.target {
                    Target::Directory { span } => means[&span.offset].clone(),
                    Target::Cell { cell } => match cell.id {
                        62 => vec![1., 0.],
                        63 => vec![-1., 0.],
                        id => vec![0.5, if id % 2 == 0 { y } else { -y }],
                    },
                };
            }
            assert_eq!(page.children.len(), 2);
            means.insert(offset, (0..2).map(|axis|
                (page.children[0].prototype[axis] + page.children[1].prototype[axis]) / 2.
            ).collect());
        }
        // Resident routing must not load directories after admission.
        fs::remove_file(output.join("directories.bin")).unwrap();
        let query = [3., 0.];
        let original_bits = query.map(f32::to_bits);
        let hierarchical = prototype.nominate(&query, NominationPolicy::Hierarchical8And24, nomination_limits()).unwrap();
        let global = prototype.nominate(&query, NominationPolicy::GlobalTop24, nomination_limits()).unwrap();
        assert_eq!(query.map(f32::to_bits), original_bits);
        assert_eq!(hierarchical.selected.len(), 24);
        assert!(!hierarchical.selected.iter().any(|v| v.cell_id == 62));
        assert_eq!(global.selected[0].cell_id, 62);
        assert_eq!(global.selected[0].distance_bits, 0_f32.to_bits());
        assert_eq!(global.routing_distance_evaluations_bound, 64);
        assert_eq!(global.routing_coordinate_evaluations_bound, 128);
        assert_eq!(global.selected.iter().skip(1).map(|v| v.cell_id).collect::<Vec<_>>(), (0..23).collect::<Vec<_>>());
        assert_eq!(global.selected[1].prototype_bits, vec![0.5_f32.to_bits(), y.to_bits()]);
        let normalized = prototype.nominate(&[1., 0.], NominationPolicy::GlobalTop24, nomination_limits()).unwrap();
        assert_eq!(serde_json::to_value(&global).unwrap(), serde_json::to_value(normalized).unwrap());
        for receipt in [&hierarchical, &global] {
            assert_eq!(receipt.resident_leaf_cells, 64);
            assert_eq!(receipt.primary_ids.len(), 8);
            assert_eq!(receipt.covered_ids.len(), 24);
            assert_eq!(receipt.accounting.source.submitted_gets, 24);
            assert_eq!(receipt.accounting.source.verified_bytes,
                receipt.selected.iter().map(|v| v.source_bytes).sum::<usize>());
            assert_eq!(receipt.accounting.directory.submitted_gets, 0);
            assert_eq!(receipt.accounting.whole_cell.submitted_gets, 0);
            assert_eq!(receipt.accounting.refinement.submitted_gets, 0);
            assert_eq!(receipt.accounting.waves.len(), 1);
        }
    }

    #[test]
    fn nomination_unpruned_parity_caps_source_binding_and_failure_charges() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = fixture(temp.path(), 32, 2, true);
        config.cell_rows = 1;
        let output = temp.path().join("candidate");
        let built = build(&config, &output).unwrap();
        let mut prototype = Prototype::open(&output, &built.root_sha256, 64 * 1024 * 1024).unwrap();
        let hierarchy = prototype.nominate(&[1., 0.], NominationPolicy::Hierarchical8And24, nomination_limits()).unwrap();
        let global = prototype.nominate(&[1., 0.], NominationPolicy::GlobalTop24, nomination_limits()).unwrap();
        assert_eq!(serde_json::to_value(&hierarchy.selected).unwrap(), serde_json::to_value(&global.selected).unwrap());
        assert_eq!(global.selected.iter().map(|v| v.cell_id).collect::<Vec<_>>(), (0..24).collect::<Vec<_>>());
        let mut search_options = options();
        search_options.primary_beam = 8;
        search_options.boundary_beam = 24;
        search_options.max_cells = 24;
        search_options.max_source_gets = 24;
        search_options.max_refinement_gets = 24;
        search_options.blocks_per_cell = 4;
        let search = prototype.search(&[1., 0.], 10, search_options).unwrap();
        assert_eq!(hierarchy.covered_ids, search.covered_ids);
        assert_eq!(hierarchy.primary_ids, search.primary_ids);
        // Each cell has one block, so identical selections imply exact downstream
        // nomination parity with unchanged search, without a probe scoring path.
        assert_eq!(global.covered_ids, search.nominated_ids);
        for limits in [
            NominationLimits { max_source_gets: 23, ..nomination_limits() },
            NominationLimits { max_source_bytes: 1, ..nomination_limits() },
            NominationLimits { max_query_payload_bytes: 1, ..nomination_limits() },
        ] {
            let error = prototype.nominate(&[1., 0.], NominationPolicy::GlobalTop24, limits).unwrap_err();
            assert_eq!(error.accounting.source.submitted_gets, 0);
            assert!(error.accounting.modeled_query_payload_bytes > 0);
        }
        for query in [[0., 0.], [f32::NAN, 0.]] {
            assert!(prototype.nominate(&query, NominationPolicy::GlobalTop24, nomination_limits()).is_err());
        }
        let selected = &global.selected[0];
        let mut bytes = fs::read(output.join("cells.bin")).unwrap();
        let original = bytes.clone();
        bytes[selected.source_offset] ^= 1;
        fs::write(output.join("cells.bin"), &bytes).unwrap();
        let error = prototype.nominate(&[1., 0.], NominationPolicy::GlobalTop24, nomination_limits()).unwrap_err();
        assert_eq!(error.accounting.source.submitted_gets, 1);
        assert_eq!(error.accounting.source.failed_gets, 1);
        assert_eq!(error.accounting.source.verified_bytes, 0);
        assert_eq!(error.accounting.refinement.submitted_gets, 0);
        // Even an authenticated range may not use out-of-domain source IDs.
        bytes = original;
        bytes[selected.source_offset..selected.source_offset + 8].copy_from_slice(&32_i64.to_le_bytes());
        fs::write(output.join("cells.bin"), &bytes).unwrap();
        for page in prototype.directories.values_mut() {
            for node in &mut page.children {
                if let Target::Cell { cell } = &mut node.target {
                    if cell.id == selected.cell_id {
                        cell.source.sha256 = hash(&bytes[cell.source.offset..cell.source.offset + cell.source.bytes]);
                    }
                }
            }
        }
        let error = prototype.nominate(&[1., 0.], NominationPolicy::GlobalTop24, nomination_limits()).unwrap_err();
        assert!(error.message.contains("source ordinal"));
        assert_eq!(error.accounting.source.verified_bytes, selected.source_bytes);
        assert_eq!(error.accounting.source.failed_gets, 0);
        // A valid digest also cannot authorize duplicate source IDs across cells.
        bytes[selected.source_offset..selected.source_offset + 8]
            .copy_from_slice(&global.selected[1].source_ids[0].to_le_bytes());
        fs::write(output.join("cells.bin"), &bytes).unwrap();
        for page in prototype.directories.values_mut() {
            for node in &mut page.children {
                if let Target::Cell { cell } = &mut node.target {
                    if cell.id == selected.cell_id {
                        cell.source.sha256 = hash(&bytes[cell.source.offset..cell.source.offset + cell.source.bytes]);
                    }
                }
            }
        }
        let error = prototype.nominate(&[1., 0.], NominationPolicy::GlobalTop24, nomination_limits()).unwrap_err();
        assert!(error.message.contains("duplicate ID"));
        assert_eq!(error.accounting.source.submitted_gets, 2);
        assert_eq!(error.accounting.source.failed_gets, 0);

        config.cell_rows = 8;
        let small = temp.path().join("underfilled");
        let built = build(&config, &small).unwrap();
        let prototype = Prototype::open(&small, &built.root_sha256, 64 * 1024 * 1024).unwrap();
        for policy in [NominationPolicy::Hierarchical8And24, NominationPolicy::GlobalTop24] {
            let error = prototype.nominate(&[1., 0.], policy, nomination_limits()).unwrap_err();
            assert_eq!(error.accounting.source.submitted_gets, 0);
        }
    }

    #[test]
    fn nonunit_query_nonzero_mean_preserves_native_sq2_scoring_and_nomination() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = fixture_with_mean(temp.path(), 65, 2, false, &[0.31_f32, -0.17_f32]);
        config.cell_rows = 512;
        let output = temp.path().join("candidate");
        let receipt = build(&config, &output).unwrap();
        let prototype = Prototype::open(&output, &receipt.root_sha256, 64 * 1024 * 1024).unwrap();
        let root = prototype
            .directories
            .get(&prototype.manifest.root_directory.offset)
            .unwrap();
        let Target::Cell { cell } = &root.children[0].target else {
            panic!("single-cell fixture")
        };
        let body = read_at(&prototype.cells, cell.source.offset, cell.source.bytes).unwrap();
        let row_bytes = 8 + prototype.codec.record_bytes();
        let records = body.chunks_exact(row_bytes).collect::<Vec<_>>();
        let a = prototype.codec.encode(&[1_f32, 0_f32]).unwrap();
        let b = prototype.codec.encode(&[0_f32, 1_f32]).unwrap();
        let differences = [[1_f32, 0_f32], [0_f32, 1_f32]].map(|basis| {
            let native = prototype.codec.prepare_query(&basis, 400_000).unwrap();
            native.score(&a).unwrap() - native.score(&b).unwrap()
        });
        let ratio = -differences[0] / differences[1];
        assert!(ratio.is_finite() && ratio > 0.);
        let best_block = |scores: &[f64]| {
            scores
                .chunks(BLOCK_ROWS)
                .enumerate()
                .map(|(block, rows)| (block, rows.iter().copied().max_by(f64::total_cmp).unwrap()))
                .max_by(|a, b| a.1.total_cmp(&b.1).then(b.0.cmp(&a.0)))
                .unwrap()
                .0
        };
        // Construct a bounded rounding/tie witness from two native code rows.
        // No queries/truth, trainer retries or production policy tuning enter it.
        let mut witness = None;
        'find: for index in 0..128 {
            let scale = 0.25_f32 + index as f32 * 0.37_f32;
            let center = (ratio * f64::from(scale)) as f32;
            for bits in center.to_bits().saturating_sub(2)..=center.to_bits().saturating_add(2) {
                let query = [scale, f32::from_bits(bits)];
                if query[0].to_bits() == query[1].to_bits() {
                    continue;
                }
                let normalized = cosine_vector(&query).unwrap();
                let native = prototype.codec.prepare_query(&query, 400_000).unwrap();
                let prenormalized = prototype.codec.prepare_query(&normalized, 400_000).unwrap();
                let native_scores = records
                    .iter()
                    .map(|row| native.score(&row[8..]).unwrap())
                    .collect::<Vec<_>>();
                let normalized_scores = records
                    .iter()
                    .map(|row| prenormalized.score(&row[8..]).unwrap())
                    .collect::<Vec<_>>();
                if best_block(&native_scores) != best_block(&normalized_scores) {
                    witness = Some((query, native_scores, normalized_scores));
                    break 'find;
                }
            }
        }
        let (query, native_scores, normalized_scores) =
            witness.expect("bounded fixture must expose prenormalized SQ2 nomination drift");
        assert_ne!(query.iter().map(|&v| f64::from(v).powi(2)).sum::<f64>(), 1.);
        assert_ne!(query[0].to_bits(), query[1].to_bits());
        assert!(
            native_scores
                .iter()
                .zip(&normalized_scores)
                .any(|(a, b)| a.to_bits() != b.to_bits())
        );
        let mut expected = records
            .chunks(BLOCK_ROWS)
            .nth(best_block(&native_scores))
            .unwrap()
            .iter()
            .map(|row| i64::from_le_bytes(row[..8].try_into().unwrap()))
            .collect::<Vec<_>>();
        expected.sort_unstable();
        let mut options = options();
        options.fetch_policy = FetchPolicy::WholeCell;
        let trace = prototype.search(&query, 10, options).unwrap();
        assert_eq!(trace.covered_ids, (0..65_i64).collect::<Vec<_>>());
        assert_eq!(trace.nominated_ids, expected);
    }

    #[test]
    fn whole_cell_matches_two_stage_with_one_wave_and_full_payload_charges() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = fixture(temp.path(), 257, 768, true);
        config.cell_rows = 64;
        let output = temp.path().join("candidate");
        let receipt = build(&config, &output).unwrap();
        let prototype = Prototype::open(&output, &receipt.root_sha256, 64 * 1024 * 1024).unwrap();
        let startup_bytes = prototype.startup_directory.verified_bytes;
        let mut query = vec![0_f32; 768];
        query[0] = 1_f32;
        let two_stage = prototype.search(&query, 10, options()).unwrap();
        let mut whole_options = options();
        whole_options.fetch_policy = FetchPolicy::WholeCell;
        let whole = prototype.search(&query, 10, whole_options).unwrap();
        assert_eq!(whole.primary_ids, two_stage.primary_ids);
        assert_eq!(whole.covered_ids, two_stage.covered_ids);
        assert_eq!(whole.nominated_ids, two_stage.nominated_ids);
        let ranking = |trace: &SearchTrace| {
            trace
                .returned
                .iter()
                .map(|row| (row.ordinal, row.id, row.score.to_bits()))
                .collect::<Vec<_>>()
        };
        assert_eq!(ranking(&whole), ranking(&two_stage));
        assert!(whole.nominated_ids.len() < whole.covered_ids.len());
        assert_eq!(
            whole.accounting.whole_cell.submitted_gets,
            two_stage.accounting.source.submitted_gets
        );
        let all_bytes = whole.covered_ids.len() * 988;
        assert_eq!(whole.accounting.whole_cell.requested_bytes, all_bytes);
        assert_eq!(whole.accounting.whole_cell.verified_bytes, all_bytes);
        assert!(
            all_bytes
                > two_stage.accounting.source.verified_bytes
                    + two_stage.accounting.refinement.verified_bytes
        );
        assert_eq!(whole.accounting.directory.submitted_gets, 0);
        assert_eq!(whole.accounting.source.submitted_gets, 0);
        assert_eq!(whole.accounting.source.requested_bytes, 0);
        assert_eq!(whole.accounting.refinement.submitted_gets, 0);
        assert_eq!(whole.accounting.refinement.requested_bytes, 0);
        assert_eq!(whole.accounting.waves.len(), 1);
        assert_eq!(whole.accounting.waves[0].stage, FetchStage::WholeCell);
        assert_eq!(whole.accounting.waves[0].requested_bytes, all_bytes);
        assert_eq!(two_stage.accounting.waves.len(), 2);
        assert_eq!(prototype.startup_directory.verified_bytes, startup_bytes);
        whole_options.max_cell_bytes = all_bytes - 1;
        let rejected = prototype.search(&query, 10, whole_options).unwrap_err();
        assert_eq!(rejected.accounting.whole_cell.submitted_gets, 0);
        assert_eq!(rejected.accounting.whole_cell.requested_bytes, 0);
        assert!(rejected.accounting.waves.is_empty());
    }

    #[test]
    fn semantic_cells_do_not_close_over_old_pages_and_keep_unchanged_ranking() {
        let temp = tempfile::tempdir().unwrap();
        let config = fixture(temp.path(), 257, 768, false);
        let output = temp.path().join("candidate");
        let receipt = build(&config, &output).unwrap();
        let prototype = Prototype::open(&output, &receipt.root_sha256, 64 * 1024 * 1024).unwrap();
        let mut query = vec![0_f32; 768];
        query[0] = 1_f32;
        let trace = prototype.search(&query, 10, options()).unwrap();
        assert!(trace.covered_ids.len() <= 3 * config.cell_rows);
        assert!(trace.covered_ids.len() < 256);
        assert!(
            trace
                .nominated_ids
                .iter()
                .all(|id| trace.covered_ids.contains(id))
        );
        assert!(
            trace
                .returned
                .iter()
                .all(|row| trace.nominated_ids.contains(&row.id))
        );
        assert_eq!(
            trace.accounting.source.verified_bytes,
            trace.covered_ids.len() * (8 + 200)
        );
        assert_eq!(
            trace.accounting.refinement.verified_bytes,
            trace.nominated_ids.len() * 780
        );
        assert!(
            trace
                .returned
                .iter()
                .all(|row| row.id % 2 == 0 && row.score == 0.)
        );
        let raw_sq8 = std::fs::read(&config.sq8.path).unwrap();
        let all = crate::exact_sq8_nominee::score_nominees(
            &raw_sq8,
            Sq8Geometry {
                rows: 257,
                dimensions: 768,
            },
            &(0..257).collect::<Vec<_>>(),
            &query,
            &vec![0_f32; 768],
            &vec![1_f32; 768],
        )
        .unwrap();
        for returned in &trace.returned {
            assert_eq!(
                returned.score,
                all.iter().find(|row| row.id == returned.id).unwrap().score
            );
        }
        assert!(
            trace
                .accounting
                .waves
                .windows(2)
                .all(|w| w[0].dependency < w[1].dependency)
        );
        assert_eq!(
            trace.accounting.directory.submitted_gets,
            trace
                .accounting
                .waves
                .iter()
                .filter(|w| w.stage == FetchStage::Directory)
                .map(|w| w.submitted_gets)
                .sum::<usize>()
        );
    }

    #[test]
    fn identical_geometry_is_bounded_and_reproducible_without_truth() {
        let temp = tempfile::tempdir().unwrap();
        let config = fixture(temp.path(), 129, 2, true);
        let a = temp.path().join("a");
        let b = temp.path().join("b");
        let first = build(&config, &a).unwrap();
        let second = build(&config, &b).unwrap();
        assert_eq!(first.root_sha256, second.root_sha256);
        for name in ["manifest.json", "directories.bin", "cells.bin"] {
            assert_eq!(
                std::fs::read(a.join(name)).unwrap(),
                std::fs::read(b.join(name)).unwrap()
            );
        }
        assert!(first.max_cell_rows <= 32);
        assert!(first.max_depth <= config.max_depth);
        assert!(first.geometry_fallbacks > 0);
        assert_eq!(first.semantic_repairs, 0);
        let prototype = Prototype::open(&a, &first.root_sha256, 64 * 1024 * 1024).unwrap();
        let trace = prototype.search(&[1., 0.], 10, options()).unwrap();
        assert!(trace.returned.windows(2).all(|w| w[0].id < w[1].id));
        assert!(build(&config, &a).is_err());
    }

    #[test]
    fn source_id_binding_budgets_and_corruption_fail_closed_with_charges() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = fixture(temp.path(), 65, 2, false);
        let mut wrong = std::fs::read(&config.canonical.path).unwrap();
        wrong[..8].copy_from_slice(&0_i64.to_le_bytes());
        // Even an independently hashed descriptor cannot override root authority.
        config.canonical = artifact(&config.canonical.path, &wrong);
        assert!(build(&config, &temp.path().join("bad")).is_err());
        // Repinning root+plane hashes cannot authorize a non-native codec seed.
        config = fixture(temp.path(), 65, 2, false);
        let mut plane: SourcePlaneReceipt =
            serde_json::from_slice(&config.plane.read(ROOT_CAP).unwrap()).unwrap();
        plane.seed = NATIVE_CODEC_SEED + 1;
        config.plane = artifact(&config.plane.path, &serde_json::to_vec(&plane).unwrap());
        let mut generation: serde_json::Value =
            serde_json::from_slice(&config.generation.read(ROOT_CAP).unwrap()).unwrap();
        generation["plane_manifest_sha256"] = json!(config.plane.sha256);
        config.generation = artifact(
            &config.generation.path,
            &serde_json::to_vec(&generation).unwrap(),
        );
        let displaced_mean = temp.path().join("displaced-mean");
        std::fs::rename(&config.mean.path, &displaced_mean).unwrap();
        assert!(
            build(&config, &temp.path().join("wrong-seed"))
                .unwrap_err()
                .to_string()
                .contains("current fixed SQ2 seed")
        );
        std::fs::rename(&displaced_mean, &config.mean.path).unwrap();
        config = fixture(temp.path(), 65, 2, false);
        let output = temp.path().join("candidate");
        let receipt = build(&config, &output).unwrap();
        let prototype = Prototype::open(&output, &receipt.root_sha256, 64 * 1024 * 1024).unwrap();
        let root_path = output.join("manifest.json");
        let original_root = std::fs::read(&root_path).unwrap();
        let mut wrong_root: Manifest = serde_json::from_slice(&original_root).unwrap();
        wrong_root.seed = NATIVE_CODEC_SEED + 1;
        let wrong_root_bytes = serde_json::to_vec(&wrong_root).unwrap();
        std::fs::write(&root_path, &wrong_root_bytes).unwrap();
        let directories = output.join("directories.bin");
        let displaced_directory = output.join("displaced-directory");
        std::fs::rename(&directories, &displaced_directory).unwrap();
        let error = Prototype::open(&output, &hash(&wrong_root_bytes), 64 * 1024 * 1024)
            .err()
            .unwrap();
        assert!(error.to_string().contains("current fixed SQ2 seed"));
        std::fs::rename(&displaced_directory, &directories).unwrap();
        std::fs::write(&root_path, &original_root).unwrap();
        let mut capped = options();
        capped.max_source_bytes = 1;
        let error = prototype.search(&[1., 0.], 1, capped).unwrap_err();
        assert_eq!(error.accounting.source.submitted_gets, 0);
        assert_eq!(error.accounting.refinement.submitted_gets, 0);
        assert_eq!(error.accounting.directory.verified_bytes, 0);
        let mut body = std::fs::read(output.join("cells.bin")).unwrap();
        body[0] ^= 1;
        std::fs::write(output.join("cells.bin"), body).unwrap();
        let error = prototype.search(&[0., 1.], 1, options()).unwrap_err();
        assert_eq!(error.accounting.directory.submitted_gets, 0);
        assert_eq!(error.accounting.source.failed_gets, 1);
        assert_eq!(error.accounting.source.verified_bytes, 0);
        assert_eq!(error.accounting.refinement.submitted_gets, 0);
    }

    #[test]
    fn loss_receipt_separates_boundary_recovery_nomination_and_final_ranking() {
        let trace = SearchTrace {
            primary_ids: vec![0],
            covered_ids: vec![0, 1, 2],
            nominated_ids: vec![0, 1],
            returned: vec![RankedRow {
                ordinal: 0,
                id: 1,
                score: 0.,
            }],
            ..SearchTrace::default()
        };
        let loss = decompose_loss(&trace, &[0, 1, 2, 3]).unwrap();
        assert_eq!(
            (
                loss.router_misses,
                loss.boundary_recovered,
                loss.boundary_misses
            ),
            (3, 2, 1)
        );
        assert_eq!(
            (loss.local_nomination_misses, loss.final_ranking_misses),
            (1, 1)
        );
        assert_eq!(
            loss.boundary_misses
                + loss.local_nomination_misses
                + loss.final_ranking_misses
                + loss.returned_hits,
            4
        );
        assert!(decompose_loss(&trace, &[0, 0]).is_err());
    }

    #[test]
    fn cell_local_block_nomination_omits_other_blocks_even_for_tied_codes() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = fixture(temp.path(), 257, 2, true);
        config.cell_rows = 64;
        let output = temp.path().join("candidate");
        let receipt = build(&config, &output).unwrap();
        assert_eq!(
            receipt.output_bytes,
            ["manifest.json", "directories.bin", "cells.bin"]
                .iter()
                .map(|name| std::fs::metadata(output.join(name)).unwrap().len() as usize)
                .sum::<usize>()
        );
        let prototype = Prototype::open(&output, &receipt.root_sha256, 64 * 1024 * 1024).unwrap();
        let trace = prototype.search(&[1., 0.], 10, options()).unwrap();
        assert!(trace.nominated_ids.len() < trace.covered_ids.len());
        assert!(trace.nominated_ids.len() <= BLOCK_ROWS * trace.accounting.source.submitted_gets);
        assert_eq!(
            trace.accounting.refinement.submitted_gets,
            trace.accounting.source.submitted_gets
        );
        assert!(trace.returned.iter().all(|row| row.score == 0.));
        // Admit every cell and block in this same fixture. Its final cell has
        // 33 rows, so ranking/charging must include the final one-row SQ8 tail.
        let mut all = options();
        all.boundary_beam = 8;
        all.blocks_per_cell = 2;
        all.max_cells = 8;
        all.max_source_gets = 8;
        all.max_refinement_gets = 16;
        let full = prototype.search(&[1., 0.], 257, all).unwrap();
        let expected = (0..257_i64).collect::<Vec<_>>();
        assert_eq!(full.covered_ids, expected);
        assert_eq!(full.nominated_ids, expected);
        assert_eq!(
            full.returned.iter().map(|row| row.id).collect::<Vec<_>>(),
            expected
        );
        assert!(full.returned.iter().all(|row| row.score == 0.));
        assert_eq!(
            full.accounting.source.verified_bytes,
            257 * (8 + prototype.codec.record_bytes())
        );
        assert_eq!(full.accounting.refinement.verified_bytes, 257 * (12 + 2));
        let mut invalid = options();
        invalid.max_query_payload_bytes = 1;
        let refused = prototype.search(&[1., 0.], 1, invalid).unwrap_err();
        for stats in [
            &refused.accounting.directory,
            &refused.accounting.whole_cell,
            &refused.accounting.source,
            &refused.accounting.refinement,
        ] {
            assert_eq!(stats.submitted_gets, 0);
            assert_eq!(stats.requested_bytes, 0);
        }
        assert!(refused.accounting.waves.is_empty());
        assert_eq!(refused.accounting.modeled_query_payload_bytes, 0);
    }

    #[test]
    fn resident_directory_preload_is_admitted_charged_and_has_no_query_reads() {
        let temp = tempfile::tempdir().unwrap();
        let config = fixture(temp.path(), 257, 2, true);
        let output = temp.path().join("candidate");
        let receipt = build(&config, &output).unwrap();
        let directory = output.join("directories.bin");
        let encoded_bytes = std::fs::metadata(&directory).unwrap().len() as usize;
        let displaced = output.join("displaced-directory");
        std::fs::rename(&directory, &displaced).unwrap();
        // A cap rejection occurs before any attempt to open/read the directory.
        let error = Prototype::open(&output, &receipt.root_sha256, 1)
            .err()
            .unwrap();
        assert!(
            error
                .to_string()
                .contains("resident directory payload admission")
        );
        std::fs::rename(&displaced, &directory).unwrap();
        let prototype = Prototype::open(&output, &receipt.root_sha256, 64 * 1024 * 1024).unwrap();
        assert_eq!(prototype.startup_directory.submitted_gets, 1);
        assert_eq!(prototype.startup_directory.verified_bytes, encoded_bytes);
        assert_eq!(prototype.directory_admission.encoded_bytes, encoded_bytes);
        assert!(prototype.directory_admission.parsed_owned_capacity_bytes <= 4 * encoded_bytes);
        // No file handle or lazy path may be used by query routing.
        std::fs::remove_file(&directory).unwrap();
        for _ in 0..2 {
            let trace = prototype.search(&[1., 0.], 10, options()).unwrap();
            assert_eq!(trace.accounting.directory.submitted_gets, 0);
            assert_eq!(trace.accounting.directory.requested_bytes, 0);
            assert_eq!(trace.accounting.directory.verified_bytes, 0);
            assert_eq!(trace.accounting.waves.len(), 2);
            assert_eq!(trace.accounting.waves[0].stage, FetchStage::Source);
            assert_eq!(trace.accounting.waves[1].stage, FetchStage::Refinement);
        }
    }

    #[test]
    fn resident_preload_rejects_corrupt_unused_interior_page() {
        let temp = tempfile::tempdir().unwrap();
        let config = fixture(temp.path(), 257, 2, true);
        let output = temp.path().join("candidate");
        let receipt = build(&config, &output).unwrap();
        let prototype = Prototype::open(&output, &receipt.root_sha256, 64 * 1024 * 1024).unwrap();
        let root = prototype
            .directories
            .get(&prototype.manifest.root_directory.offset)
            .unwrap();
        // Identical prototypes tie on directory offset; a width-one route takes
        // the lower offset, so the other root subtree is unused by this query.
        let unused = root
            .children
            .iter()
            .filter_map(|node| match &node.target {
                Target::Directory { span } => Some(span.clone()),
                _ => None,
            })
            .max_by_key(|span| span.offset)
            .unwrap();
        let mut narrow = options();
        narrow.boundary_beam = 1;
        prototype.search(&[1., 0.], 10, narrow).unwrap();
        let path = output.join("directories.bin");
        let mut body = std::fs::read(&path).unwrap();
        body[unused.offset] ^= 1;
        std::fs::write(&path, &body).unwrap();
        assert!(Prototype::open(&output, &receipt.root_sha256, 64 * 1024 * 1024).is_err());
        // Even independently repinning the whole-file hash cannot override
        // the authenticated child descriptor inside the unchanged parent page.
        let root_path = output.join("manifest.json");
        let mut manifest: Manifest =
            serde_json::from_slice(&std::fs::read(&root_path).unwrap()).unwrap();
        manifest.directory_sha256 = hash(&body);
        let root_body = serde_json::to_vec(&manifest).unwrap();
        std::fs::write(&root_path, &root_body).unwrap();
        let error = Prototype::open(&output, &hash(&root_body), 64 * 1024 * 1024)
            .err()
            .unwrap();
        assert!(error.to_string().contains("directory page SHA256"));
    }
}

/// Source-neighborhood screening only; this API neither builds an index nor
/// accepts queries, truth, nomination policy, or a parameter sweep.
pub mod split_balance_diagnostic {
    use super::*;
    use serde_json::{Value, json};
    use std::{fmt, os::unix::fs::MetadataExt, path::Component};

    /// Exact diagnostic configuration, with all numerical controls frozen here.
    pub const CONFIG_SCHEMA: &str = "borsuk-constrained-split-config-v2";
    /// Terminal report marker; a PASS is not ANN recall or product qualification.
    pub const REPORT_SCHEMA: &str = "borsuk-constrained-split-diagnostic-v2";
    const AUTH_CAP: usize = 1024 * 1024 * 1024;
    const MEMORY_CAP: usize = 512 * 1024 * 1024;
    const DIRECTORY_CAP: usize = 16 * 1024 * 1024;
    const OUTPUT_CAP: usize = 2 * 1024 * 1024;
    const PARENT_ROWS_CAP: usize = 32768;
    const CANDIDATES: usize = 8;
    const MIN_VERIFIED: usize = 4;
    const PANEL: usize = 128;
    const NEIGHBORS: usize = 16;

    /// Exact original artifacts. Paths are local; nothing is fetched or decoded
    /// from SQ8 into original vectors. Root pins bind canonical/order identities.
    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct DatasetInputs {
        /// Original hierarchical manifest.
        pub root: Artifact,
        /// Complete original directory bytes.
        pub directories: Artifact,
        /// Complete original cells, including their authenticated ID rosters.
        pub cells: Artifact,
        /// Original normalized ID + FP32 canonical records.
        pub canonical: Artifact,
        /// Original physical-to-source-ordinal LE u64 permutation.
        pub order: Artifact,
    }

    /// Strict source-only inputs. Only the two original datasets are admitted;
    /// no query/truth fields, numerical overrides, or optional sweep controls.
    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct Config {
        /// Must equal [`CONFIG_SCHEMA`].
        pub schema: String,
        /// Exactly `cohere` and `relaion`, in this deterministic map order.
        pub datasets: BTreeMap<String, DatasetInputs>,
    }

    /// Separately authenticated supervisor authority, never supplied by the
    /// diagnostic body itself. The caller owns the immutable run and closure.
    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct SupervisorReceipt {
        /// Immutable expected supervisor run identity.
        pub run_id: String,
        /// SHA256 of this run's authenticated configuration.
        pub config_sha256: String,
        /// SHA256 of the exact report bytes collected after process exit.
        pub report_sha256: String,
        /// Original process's observed exit code, not its claimed body status.
        pub process_exit_code: i32,
        /// Original supervisor completed the resource/error/cleanup closure.
        pub resources_closed: bool,
    }

    /// Admit a PASS only with an independently authenticated matching original
    /// supervisor receipt. The report is never standalone terminal authority.
    pub fn admit_pass(
        report_bytes: &[u8],
        expected_run_id: &str,
        supervisor: &SupervisorReceipt,
    ) -> Result<()> {
        require(
            !report_bytes.is_empty() && report_bytes.len() <= OUTPUT_CAP,
            "diagnostic supervisor report byte cap",
        )?;
        let report: Value = serde_json::from_slice(report_bytes)?;
        require(
            report["schema"] == REPORT_SCHEMA
                && report["status"] == "PASS"
                && report["requires_matching_supervisor_exit_receipt"] == true
                && report["durability"]["standalone_authority"] == false
                && report["durability"]["status"] == "SUPERVISOR_EXIT_RECEIPT_REQUIRED"
                && !expected_run_id.is_empty()
                && supervisor.run_id == expected_run_id
                && valid_sha(&supervisor.report_sha256)
                && supervisor.report_sha256 == hash(report_bytes)
                && valid_sha(&supervisor.config_sha256)
                && report["config_sha256"] == supervisor.config_sha256
                && supervisor.process_exit_code == 0
                && supervisor.resources_closed,
            "diagnostic PASS requires matching supervisor exit-zero/resource closure receipt",
        )
    }

    #[derive(Debug)]
    struct InputUnavailable;
    impl fmt::Display for InputUnavailable {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("original input unavailable; no download or reconstruction")
        }
    }
    impl Error for InputUnavailable {}

    fn reserved<T>(capacity: usize) -> Result<Vec<T>> {
        require(
            capacity
                .checked_mul(std::mem::size_of::<T>())
                .is_some_and(|bytes| bytes <= MEMORY_CAP / 4),
            "diagnostic allocation cap/overflow",
        )?;
        let mut values = Vec::new();
        values.try_reserve_exact(capacity)?;
        Ok(values)
    }
    fn filled<T: Clone>(count: usize, value: T) -> Result<Vec<T>> {
        let mut values = reserved(count)?;
        values.resize(count, value);
        Ok(values)
    }

    // Walk pinned directory handles, so neither a final symlink nor a symlink
    // in an ancestor can redirect an input/output. NONBLOCK rejects FIFOs
    // without hanging. This does not alter the prototype's existing file API.
    fn secure_open(path: &Path, flags: rustix::fs::OFlags) -> Result<File> {
        require(
            path.is_absolute() && path.as_os_str().len() <= 4096,
            "diagnostic absolute bounded local path",
        )?;
        let parts = path.components().collect::<Vec<_>>();
        require(
            !parts.is_empty()
                && parts.len() <= 128
                && parts[1..]
                    .iter()
                    .all(|part| matches!(part, Component::Normal(_))),
            "diagnostic path components",
        )?;
        let mut directory = rustix::fs::open(
            "/",
            rustix::fs::OFlags::RDONLY
                | rustix::fs::OFlags::DIRECTORY
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        )?;
        if parts.len() == 1 {
            return Ok(File::from(directory));
        }
        for (index, component) in parts[1..].iter().enumerate() {
            let Component::Normal(name) = component else {
                unreachable!()
            };
            let last = index + 2 == parts.len();
            let fd = rustix::fs::openat(
                &directory,
                *name,
                (if last {
                    flags
                } else {
                    rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::DIRECTORY
                }) | rustix::fs::OFlags::NOFOLLOW
                    | rustix::fs::OFlags::NONBLOCK
                    | rustix::fs::OFlags::CLOEXEC,
                rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
            )?;
            if last {
                return Ok(File::from(fd));
            }
            directory = fd;
        }
        Err("diagnostic empty path".into())
    }

    fn create_output(directory: &File, output: &Path) -> Result<File> {
        // Resolve only the basename against the parent that will be fsynced.
        // A rename/replacement of its absolute pathname cannot redirect this.
        require(
            output.is_absolute() && output.as_os_str().len() <= 4096,
            "diagnostic absolute bounded new output",
        )?;
        let name = output.file_name().ok_or("diagnostic output basename")?;
        require(
            directory.metadata()?.is_dir(),
            "diagnostic pinned output parent",
        )?;
        let file = File::from(rustix::fs::openat(
            directory,
            name,
            rustix::fs::OFlags::WRONLY
                | rustix::fs::OFlags::CREATE
                | rustix::fs::OFlags::EXCL
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::NONBLOCK
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
        )?);
        require(file.metadata()?.is_file(), "diagnostic regular new output")?;
        Ok(file)
    }

    // Keep the descriptor and stat identity. Per-row digests bind later source
    // reads to the initial authenticated stream, without streaming 1GiB twice.
    struct Pinned {
        file: File,
        stamp: (u64, u64, u64, i64, i64, i64, i64),
    }
    fn stamp(file: &File) -> Result<(u64, u64, u64, i64, i64, i64, i64)> {
        let m = file.metadata()?;
        require(m.is_file(), "diagnostic regular file")?;
        Ok((
            m.dev(),
            m.ino(),
            m.len(),
            m.mtime(),
            m.mtime_nsec(),
            m.ctime(),
            m.ctime_nsec(),
        ))
    }
    impl Pinned {
        fn open(a: &Artifact, cap: usize) -> Result<Self> {
            require(
                a.bytes > 0 && a.bytes <= cap && valid_sha(&a.sha256),
                "diagnostic artifact descriptor/cap",
            )?;
            let file = match secure_open(&a.path, rustix::fs::OFlags::RDONLY) {
                Ok(file) => file,
                Err(error)
                    if error.downcast_ref::<rustix::io::Errno>()
                        == Some(&rustix::io::Errno::NOENT) =>
                {
                    return Err(Box::new(InputUnavailable));
                }
                Err(error) => return Err(error),
            };
            let identity = stamp(&file)?;
            require(
                identity.2 == a.bytes as u64,
                "diagnostic exact artifact length",
            )?;
            Ok(Self {
                file,
                stamp: identity,
            })
        }
        fn unchanged(&self) -> Result<()> {
            require(
                stamp(&self.file)? == self.stamp,
                "diagnostic input changed during replay",
            )
        }
        fn small(a: &Artifact, cap: usize, budget: &mut Budget) -> Result<Vec<u8>> {
            let pin = Self::open(a, cap)?;
            budget.admit(a.bytes)?;
            let body = pin.at(0, a.bytes)?;
            require(hash(&body) == a.sha256, "diagnostic artifact SHA256")?;
            budget.verified(a.bytes)?;
            budget.poll()?;
            pin.unchanged()?;
            Ok(body)
        }
        fn at(&self, offset: usize, bytes: usize) -> Result<Vec<u8>> {
            require(
                offset
                    .checked_add(bytes)
                    .is_some_and(|end| end as u64 <= self.stamp.2),
                "diagnostic range/overflow",
            )?;
            let mut body = filled(bytes, 0_u8)?;
            self.file.read_exact_at(&mut body, offset as u64)?;
            Ok(body)
        }
    }

    #[derive(Default)]
    struct Counts {
        admitted_bytes: usize,
        auth_bytes: usize,
        canonical_auth_rows: usize,
        canonical_stream_read_bytes: usize,
        canonical_replay_rows: usize,
        parent_rows: usize,
        trainer_calls: usize,
        trainer_distance_upper_bound: usize,
        assignment_distances: usize,
        neighbor_distances: usize,
    }
    struct Budget {
        start: Instant,
        counts: Counts,
        cgroup: Option<PathBuf>,
    }
    fn cgroup_controls(body: &str, memory: &str, swap: &str) -> Result<()> {
        let fields = body.split_whitespace().collect::<Vec<_>>();
        require(fields.len() == 2, "diagnostic cpu.max fields")?;
        let quota = fields[0].parse::<u64>()?;
        let period = fields[1].parse::<u64>()?;
        let memory = memory.trim().parse::<usize>()?;
        require(
            quota > 0
                && period > 0
                && quota <= period
                && memory > 0
                && memory <= MEMORY_CAP
                && swap.trim() == "0",
            "diagnostic CPU1/512Mi/noSwap controls",
        )
    }
    impl Budget {
        fn new(enforce: bool) -> Result<Self> {
            let cgroup = if enforce {
                let body = fs::read_to_string("/proc/self/cgroup")?;
                let path = body
                    .lines()
                    .find_map(|line| line.strip_prefix("0::"))
                    .ok_or("diagnostic requires cgroup v2")?;
                require(
                    Path::new(path).is_absolute()
                        && !Path::new(path)
                            .components()
                            .any(|p| matches!(p, Component::ParentDir)),
                    "diagnostic cgroup path",
                )?;
                let path = Path::new("/sys/fs/cgroup").join(path.trim_start_matches('/'));
                cgroup_controls(
                    &fs::read_to_string(path.join("cpu.max"))?,
                    &fs::read_to_string(path.join("memory.max"))?,
                    &fs::read_to_string(path.join("memory.swap.max"))?,
                )?;
                Some(path)
            } else {
                None
            };
            let budget = Self {
                start: Instant::now(),
                counts: Counts::default(),
                cgroup,
            };
            budget.poll()?;
            Ok(budget)
        }
        fn poll(&self) -> Result<()> {
            require(
                self.start.elapsed().as_secs_f64() < 180.,
                "diagnostic 180s deadline",
            )?;
            if let Some(path) = &self.cgroup {
                cgroup_controls(
                    &fs::read_to_string(path.join("cpu.max"))?,
                    &fs::read_to_string(path.join("memory.max"))?,
                    &fs::read_to_string(path.join("memory.swap.max"))?,
                )?;
                let current = fs::read_to_string(path.join("memory.current"))?
                    .trim()
                    .parse::<usize>()?;
                let peak = fs::read_to_string(path.join("memory.peak"))?
                    .trim()
                    .parse::<usize>()?;
                require(
                    current <= MEMORY_CAP && peak <= MEMORY_CAP,
                    "diagnostic charged memory cap",
                )?;
                let status = fs::read_to_string("/proc/self/status")?;
                let swap = status
                    .lines()
                    .find_map(|line| line.strip_prefix("VmSwap:"))
                    .and_then(|line| line.split_whitespace().next())
                    .ok_or("diagnostic process swap accounting")?
                    .parse::<usize>()?;
                require(swap == 0, "diagnostic process swap")?;
            }
            Ok(())
        }
        fn admit(&mut self, bytes: usize) -> Result<()> {
            let admitted = self
                .counts
                .admitted_bytes
                .checked_add(bytes)
                .ok_or("diagnostic auth count overflow")?;
            require(
                admitted <= AUTH_CAP,
                "diagnostic 1GiB cumulative input read cap",
            )?;
            self.counts.admitted_bytes = admitted;
            Ok(())
        }
        fn verified(&mut self, bytes: usize) -> Result<()> {
            let verified = self
                .counts
                .auth_bytes
                .checked_add(bytes)
                .ok_or("diagnostic verified read count overflow")?;
            require(
                verified <= AUTH_CAP && verified <= self.counts.admitted_bytes,
                "diagnostic cumulative verified input read cap/admission",
            )?;
            self.counts.auth_bytes = verified;
            Ok(())
        }
        fn parents(&mut self, rows: usize) -> Result<()> {
            self.counts.parent_rows = self
                .counts
                .parent_rows
                .checked_add(rows)
                .ok_or("diagnostic parent count overflow")?;
            require(
                self.counts.parent_rows <= PARENT_ROWS_CAP,
                "diagnostic parent row cap",
            )
        }
        fn receipt(&self) -> Result<Value> {
            let observed = if let Some(path) = &self.cgroup {
                json!({"path":path,"cpu_max":fs::read_to_string(path.join("cpu.max"))?.trim(),
                    "memory_max":fs::read_to_string(path.join("memory.max"))?.trim(),
                    "memory_swap_max":fs::read_to_string(path.join("memory.swap.max"))?.trim(),
                    "memory_peak_bytes":fs::read_to_string(path.join("memory.peak"))?.trim().parse::<usize>()?})
            } else {
                Value::Null
            };
            Ok(json!({"elapsed_seconds":self.start.elapsed().as_secs_f64(),
                "limits":{"cpu":1,"charged_memory_bytes":MEMORY_CAP,"swap_bytes":0,
                    "seconds":180,"output_bytes":OUTPUT_CAP,"parent_rows":PARENT_ROWS_CAP,
                    "cumulative_authenticated_input_read_bytes":AUTH_CAP}, "observed_controls":observed,
                "operations":{"cumulative_verified_input_read_bytes":self.counts.auth_bytes,
                    "admitted_authentication_read_bytes":self.counts.admitted_bytes,
                    "canonical_rereads_charged_to_same_limit":true,
                    "canonical_authentication_rows":self.counts.canonical_auth_rows,
                    "canonical_stream_underlying_read_bytes":self.counts.canonical_stream_read_bytes,
                    "canonical_replay_rows":self.counts.canonical_replay_rows,
                    "parent_rows":self.counts.parent_rows,"trainer_calls":self.counts.trainer_calls,
                    "trainer_requested_max_iterations":4,
                    "trainer_distance_evaluations_upper_bound":self.counts.trainer_distance_upper_bound,
                    "assignment_distance_evaluations":self.counts.assignment_distances,
                    "cosine_neighbor_distance_evaluations":self.counts.neighbor_distances}}))
        }
    }

    fn descriptors(config: &Config) -> Result<()> {
        require(
            config.schema == CONFIG_SCHEMA
                && config.datasets.len() == 2
                && config.datasets.contains_key("cohere")
                && config.datasets.contains_key("relaion"),
            "diagnostic frozen schema/datasets",
        )?;
        let mut paths = BTreeSet::new();
        let mut total = 0_usize;
        for inputs in config.datasets.values() {
            for (a, cap) in [
                (&inputs.root, ROOT_CAP),
                (&inputs.directories, DIRECTORY_CAP),
                (&inputs.cells, 100_000 * 988),
                (&inputs.canonical, 100_000 * (8 + 4 * 768)),
                (&inputs.order, 100_000 * 8),
            ] {
                require(
                    a.path.is_absolute()
                        && paths.insert(&a.path)
                        && a.bytes > 0
                        && a.bytes <= cap
                        && valid_sha(&a.sha256),
                    "diagnostic declared artifact limits/uniqueness",
                )?;
                total = total
                    .checked_add(a.bytes)
                    .ok_or("diagnostic declared input overflow")?;
            }
        }
        require(
            total <= AUTH_CAP - ROOT_CAP,
            "diagnostic declared streamed input cap",
        )
    }

    const PRODUCTION_GEOMETRY: (usize, usize) = (100_000, 768);
    fn geometry(
        manifest: &Manifest,
        inputs: &DatasetInputs,
        expected: (usize, usize),
    ) -> Result<()> {
        let rows = manifest.rows;
        let dimensions = manifest.dimensions;
        require(
            manifest.schema == SCHEMA
                && manifest.input.schema == BUILD_SCHEMA
                && (rows, dimensions) == expected
                && (1..=100_000).contains(&rows)
                && (1..=768).contains(&dimensions)
                && manifest.seed == NATIVE_CODEC_SEED
                && (1..=512).contains(&manifest.input.cell_rows)
                && (2..=1024).contains(&manifest.input.sample_rows)
                && (1..=32).contains(&manifest.input.max_depth)
                && (1..=rows).contains(&manifest.build.cells)
                && (1..=rows).contains(&manifest.build.directories)
                && manifest.build.max_cell_rows <= manifest.input.cell_rows
                && manifest.build.max_depth <= manifest.input.max_depth
                && manifest.mean.len() == dimensions
                && manifest.mean.iter().all(|v| v.is_finite())
                && manifest.low.len() == dimensions
                && manifest.low.iter().all(|v| v.is_finite())
                && manifest.step.len() == dimensions
                && manifest.step.iter().all(|v| v.is_finite() && *v > 0.)
                && manifest.directory_bytes == inputs.directories.bytes
                && manifest.directory_sha256 == inputs.directories.sha256
                && manifest.cell_bytes == inputs.cells.bytes
                && manifest.input.canonical.bytes == inputs.canonical.bytes
                && manifest.input.canonical.sha256 == inputs.canonical.sha256
                && inputs.canonical.bytes == rows * (8 + 4 * dimensions)
                && manifest.input.order.bytes == inputs.order.bytes
                && manifest.input.order.sha256 == inputs.order.sha256
                && inputs.order.bytes == rows * 8
                && manifest.input.records.bytes % rows == 0
                && (9..=200).contains(&(manifest.input.records.bytes / rows))
                && in_bounds(&manifest.root_directory, manifest.directory_bytes, PAGE_CAP),
            "diagnostic exact original root/geometry/pin binding",
        )?;
        require(
            manifest
                .directory_bytes
                .checked_add(manifest.cell_bytes)
                .and_then(|n| n.checked_add(inputs.root.bytes))
                == Some(manifest.build.output_bytes),
            "diagnostic build output byte binding",
        )
    }

    struct Source {
        pin: Pinned,
        inverse: Vec<usize>,
        row_hashes: Vec<[u8; 32]>,
        width: usize,
        dimensions: usize,
    }
    struct CountedReader<R> {
        inner: R,
        bytes: usize,
    }
    impl<R: Read> Read for CountedReader<R> {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            let count = self.inner.read(buffer)?;
            self.bytes = self
                .bytes
                .checked_add(count)
                .ok_or_else(|| std::io::Error::other("canonical stream read count overflow"))?;
            Ok(count)
        }
    }
    fn vector(body: &[u8], dimensions: usize) -> Result<Vec<f32>> {
        require(
            body.len() == 8 + 4 * dimensions,
            "diagnostic canonical row geometry",
        )?;
        let mut values = reserved(dimensions)?;
        for word in body[8..].chunks_exact(4) {
            let value = f32::from_le_bytes(word.try_into()?);
            require(value.is_finite(), "diagnostic nonfinite source")?;
            values.push(value);
        }
        let normalized = cosine_vector(&values)?;
        require(
            values
                .iter()
                .zip(normalized.iter())
                .all(|(a, b)| (a - b).abs() <= 1e-5),
            "diagnostic original source normalization",
        )?;
        Ok(values)
    }
    impl Source {
        fn authenticate(
            inputs: &DatasetInputs,
            rows: usize,
            dimensions: usize,
            budget: &mut Budget,
        ) -> Result<Self> {
            Self::authenticate_with(inputs, rows, dimensions, budget, |_| Ok(()))
        }
        fn authenticate_with(
            inputs: &DatasetInputs,
            rows: usize,
            dimensions: usize,
            budget: &mut Budget,
            after_open: impl FnOnce(&File) -> Result<()>,
        ) -> Result<Self> {
            let order = Pinned::small(&inputs.order, rows * 8, budget)?;
            let mut inverse = filled(rows, usize::MAX)?;
            for (physical, word) in order.chunks_exact(8).enumerate() {
                let id = usize::try_from(u64::from_le_bytes(word.try_into()?))?;
                require(
                    id < rows && inverse[id] == usize::MAX,
                    "diagnostic source order permutation",
                )?;
                inverse[id] = physical;
            }
            let pin = Pinned::open(&inputs.canonical, inputs.canonical.bytes)?;
            budget.admit(inputs.canonical.bytes)?;
            after_open(&pin.file)?;
            let width = 8 + 4 * dimensions;
            let mut row_hashes = filled(rows, [0_u8; 32])?;
            let mut body = filled(width, 0_u8)?;
            let mut digest = Sha256::new();
            // Limit the underlying read, not just the row loop: buffering and
            // the EOF probe cannot consume growth beyond the admitted length.
            let bounded = CountedReader {
                inner: &pin.file,
                bytes: 0,
            }
            .take(inputs.canonical.bytes as u64);
            let mut reader = std::io::BufReader::with_capacity(65536, bounded);
            let streamed = (|| -> Result<()> {
                for physical in 0..rows {
                    reader.read_exact(&mut body)?;
                    let id = usize::try_from(i64::from_le_bytes(body[..8].try_into()?))?;
                    require(
                        id < rows && inverse[id] == physical,
                        "diagnostic canonical/source order ID binding",
                    )?;
                    vector(&body, dimensions)?;
                    digest.update(&body);
                    row_hashes[id] = Sha256::digest(&body).into();
                    budget.counts.canonical_auth_rows += 1;
                    if physical % 256 == 0 {
                        budget.poll()?;
                    }
                }
                require(
                    reader.read(&mut [0])? == 0
                        && format!("{:x}", digest.finalize()) == inputs.canonical.sha256,
                    "diagnostic original canonical SHA256/EOF",
                )
            })();
            budget.counts.canonical_stream_read_bytes = budget
                .counts
                .canonical_stream_read_bytes
                .checked_add(reader.get_ref().get_ref().bytes)
                .ok_or("canonical stream count overflow")?;
            streamed?;
            budget.verified(inputs.canonical.bytes)?;
            budget.poll()?;
            pin.unchanged()?;
            Ok(Self {
                pin,
                inverse,
                row_hashes,
                width,
                dimensions,
            })
        }
        fn rows(&self, ids: &[usize], budget: &mut Budget) -> Result<Vec<Vec<f32>>> {
            budget.parents(ids.len())?;
            budget.admit(
                ids.len()
                    .checked_mul(self.width)
                    .ok_or("diagnostic canonical replay byte overflow")?,
            )?;
            let mut rows = reserved(ids.len())?;
            for (slot, &id) in ids.iter().enumerate() {
                require(id < self.inverse.len(), "diagnostic source ordinal bounds")?;
                let body = self.pin.at(
                    self.inverse[id]
                        .checked_mul(self.width)
                        .ok_or("diagnostic canonical offset overflow")?,
                    self.width,
                )?;
                let row_hash: [u8; 32] = Sha256::digest(&body).into();
                require(
                    row_hash == self.row_hashes[id]
                        && i64::from_le_bytes(body[..8].try_into()?) == id as i64,
                    "diagnostic replay canonical authentication",
                )?;
                budget.verified(self.width)?;
                rows.push(vector(&body, self.dimensions)?);
                budget.counts.canonical_replay_rows += 1;
                if slot % 64 == 0 {
                    budget.poll()?;
                }
            }
            self.pin.unchanged()?;
            Ok(rows)
        }
    }

    fn rosters(
        prototype: &Prototype,
        inputs: &DatasetInputs,
        budget: &mut Budget,
    ) -> Result<Vec<usize>> {
        budget.admit(inputs.cells.bytes)?;
        let mut leaves = reserved(prototype.manifest.build.cells)?;
        for page in prototype.directories.values() {
            for node in &page.children {
                if let Target::Cell { cell } = &node.target {
                    leaves.push((cell.id, node.rows, cell));
                }
            }
        }
        leaves.sort_unstable_by_key(|item| item.0);
        let mut roster = reserved(prototype.rows())?;
        let mut seen = filled(prototype.rows(), false)?;
        let mut digest = Sha256::new();
        let mut offset = 0;
        for (_, rows, cell) in leaves {
            budget.poll()?;
            require(
                cell.whole.offset == offset && cell.first_row == roster.len(),
                "diagnostic contiguous original cell geometry",
            )?;
            let body = read_checked_range(&prototype.cells, cell.whole.offset, cell.whole.bytes)?;
            require(
                hash(&body) == cell.whole.sha256
                    && hash(&body[..cell.source.bytes]) == cell.source.sha256,
                "diagnostic original whole/source cell SHA256",
            )?;
            digest.update(&body);
            let width = 8 + prototype.codec.record_bytes();
            for (slot, record) in body[..cell.source.bytes].chunks_exact(width).enumerate() {
                let id = usize::try_from(i64::from_le_bytes(record[..8].try_into()?))?;
                let sq8_start = cell.source.bytes + slot * (12 + prototype.dimensions());
                require(
                    id < seen.len()
                        && !seen[id]
                        && i64::from_le_bytes(body[sq8_start..sq8_start + 8].try_into()?)
                            == id as i64,
                    "diagnostic original complete/unique source roster and SQ8 ID binding",
                )?;
                let norm = f32::from_le_bytes(body[sq8_start + 8..sq8_start + 12].try_into()?);
                require(
                    norm.is_finite() && norm > 0.,
                    "diagnostic SQ8 stored norm geometry",
                )?;
                seen[id] = true;
                roster.push(id);
            }
            require(
                cell.source.bytes == rows * width,
                "diagnostic source roster size",
            )?;
            for span in &cell.refinement {
                let start = span.offset - cell.whole.offset;
                require(
                    hash(&body[start..start + span.bytes]) == span.sha256,
                    "diagnostic original refinement SHA256",
                )?;
            }
            offset += body.len();
        }
        require(
            roster.len() == prototype.rows()
                && seen.iter().all(|v| *v)
                && offset == inputs.cells.bytes
                && format!("{:x}", digest.finalize()) == inputs.cells.sha256,
            "diagnostic original cells complete stream SHA256",
        )?;
        budget.verified(offset)?;
        budget.poll()?;
        Ok(roster)
    }
    fn read_checked_range(file: &File, offset: usize, bytes: usize) -> Result<Vec<u8>> {
        let mut body = filled(bytes, 0_u8)?;
        file.read_exact_at(&mut body, offset as u64)?;
        Ok(body)
    }
    fn members(
        node: &Node,
        prototype: &Prototype,
        roster: &[usize],
        depth: usize,
        ids: &mut Vec<usize>,
    ) -> Result<()> {
        require(
            depth <= prototype.manifest.input.max_depth + 1,
            "diagnostic original directory depth",
        )?;
        match &node.target {
            Target::Cell { cell } => {
                ids.extend_from_slice(&roster[cell.first_row..cell.first_row + node.rows])
            }
            Target::Directory { span } => {
                for child in &prototype
                    .directories
                    .get(&span.offset)
                    .ok_or("diagnostic missing directory")?
                    .children
                {
                    members(child, prototype, roster, depth + 1, ids)?;
                }
            }
        }
        Ok(())
    }
    // Frozen hash encodings use the raw 32-byte root digest followed by LE
    // u64 directory offset, then (for panel rows) LE u64 source ordinal.
    fn root_digest(root: &str) -> [u8; 32] {
        // Descriptor validation precedes all calls. Decode without allocating.
        let mut bytes = [0_u8; 32];
        for (index, pair) in root.as_bytes().chunks_exact(2).enumerate() {
            let digit = |value: u8| {
                if value <= b'9' {
                    value - b'0'
                } else {
                    value - b'a' + 10
                }
            };
            bytes[index] = digit(pair[0]) * 16 + digit(pair[1]);
        }
        bytes
    }
    fn node_key(root: &str, offset: usize) -> [u8; 32] {
        let mut digest = Sha256::new();
        digest.update(root_digest(root));
        digest.update((offset as u64).to_le_bytes());
        digest.finalize().into()
    }
    fn row_key(root: &str, offset: usize, ordinal: usize) -> [u8; 32] {
        let mut digest = Sha256::new();
        digest.update(root_digest(root));
        digest.update((offset as u64).to_le_bytes());
        digest.update((ordinal as u64).to_le_bytes());
        digest.finalize().into()
    }
    fn candidate_offsets(prototype: &Prototype, root: &str) -> Result<Vec<usize>> {
        freeze_candidates(
            root,
            prototype.directories.iter().filter_map(|(&offset, page)| {
                (page.children.len() == 2)
                    .then(|| (offset, page.children[0].rows, page.children[1].rows))
            }),
            prototype.directories.len(),
        )
    }
    fn freeze_candidates(
        root: &str,
        pages: impl Iterator<Item = (usize, usize, usize)>,
        capacity: usize,
    ) -> Result<Vec<usize>> {
        let mut offsets = reserved(capacity)?;
        for (offset, a, b) in pages {
            let rows = a
                .checked_add(b)
                .ok_or("diagnostic candidate row overflow")?;
            if (513..=2048).contains(&rows) && a.abs_diff(b) <= 1 {
                require(
                    offsets.len() < capacity,
                    "diagnostic candidate allocation bound",
                )?;
                offsets.push((node_key(root, offset), offset));
            }
        }
        offsets.sort_unstable();
        offsets.truncate(CANDIDATES);
        let mut selected = reserved(offsets.len())?;
        selected.extend(offsets.into_iter().map(|(_, offset)| offset));
        Ok(selected)
    }
    fn id_hash(ids: impl Iterator<Item = usize>) -> String {
        let mut digest = Sha256::new();
        for id in ids {
            digest.update((id as u64).to_le_bytes());
        }
        format!("{:x}", digest.finalize())
    }
    fn float_hash(values: impl Iterator<Item = f32>) -> String {
        let mut digest = Sha256::new();
        for value in values {
            digest.update(value.to_le_bytes());
        }
        format!("{:x}", digest.finalize())
    }

    fn coordinate_split(ids: &[usize], rows: &[Vec<f32>]) -> Result<Vec<bool>> {
        coordinate_partition(ids, rows, ids.len() / 2)
    }
    fn coordinate_partition(
        ids: &[usize],
        rows: &[Vec<f32>],
        left_rows: usize,
    ) -> Result<Vec<bool>> {
        require(
            ids.len() == rows.len()
                && rows.len() >= 2
                && !rows[0].is_empty()
                && left_rows > 0
                && left_rows < ids.len(),
            "diagnostic coordinate geometry",
        )?;
        let dimensions = rows[0].len();
        let mut minimum = filled(dimensions, f32::INFINITY)?;
        let mut maximum = filled(dimensions, f32::NEG_INFINITY)?;
        for row in rows {
            require(
                row.len() == dimensions && row.iter().all(|v| v.is_finite()),
                "diagnostic coordinate nonfinite/geometry",
            )?;
            for coordinate in 0..dimensions {
                minimum[coordinate] = minimum[coordinate].min(row[coordinate]);
                maximum[coordinate] = maximum[coordinate].max(row[coordinate]);
            }
        }
        require(
            minimum
                .iter()
                .zip(&maximum)
                .all(|(a, b)| (b - a).is_finite()),
            "diagnostic coordinate range overflow",
        )?;
        let coordinate = (0..dimensions)
            .max_by(|&a, &b| {
                (maximum[a] - minimum[a])
                    .total_cmp(&(maximum[b] - minimum[b]))
                    .then(b.cmp(&a))
            })
            .ok_or("diagnostic empty coordinate")?;
        let mut projected = reserved(ids.len())?;
        projected.extend(
            rows.iter()
                .enumerate()
                .map(|(slot, row)| (row[coordinate], ids[slot], slot)),
        );
        projected.sort_unstable_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
        let mut left = filled(ids.len(), false)?;
        for item in &projected[..left_rows] {
            left[item.2] = true;
        }
        Ok(left)
    }
    fn constrained(
        ids: &[usize],
        delta: &[f32],
        original: &[bool],
        fallback: &[bool],
        degenerate: bool,
    ) -> Result<Vec<bool>> {
        require(
            ids.len() >= 2
                && ids.len() <= 2048
                && ids.len() == original.len()
                && ids.len() == fallback.len()
                && (ids.len() == delta.len() || (degenerate && delta.is_empty()))
                && delta.iter().all(|v| v.is_finite()),
            "diagnostic constrained geometry/nonfinite",
        )?;
        let count = original.iter().filter(|v| **v).count();
        let lower = ids.len().div_ceil(4);
        let mut left = reserved(ids.len())?;
        if count.min(ids.len() - count) >= lower {
            left.extend_from_slice(original);
        } else if degenerate {
            left.extend_from_slice(fallback);
        } else {
            left.extend_from_slice(original);
            capacity_partition(ids, delta, &mut left)?;
        }
        let count = left.iter().filter(|v| **v).count();
        require(
            count.min(ids.len() - count) >= lower,
            "diagnostic complete balanced constrained partition",
        )?;
        Ok(left)
    }
    struct Replay {
        sample: Vec<usize>,
        centers: Vec<Vec<f32>>,
        delta: Vec<f32>,
        original: Vec<bool>,
        proposed: Vec<bool>,
        fallback: bool,
        identical: bool,
        degenerate: bool,
    }
    fn replay(
        ids: &[usize],
        rows: &[Vec<f32>],
        sample_rows: usize,
        budget: &mut Budget,
    ) -> Result<Replay> {
        require(
            (2..=1024).contains(&sample_rows)
                && ids.len() == rows.len()
                && (2..=2048).contains(&ids.len())
                && ids.windows(2).all(|w| w[0] < w[1]),
            "diagnostic replay geometry/source order",
        )?;
        let sample_count = sample_rows.min(ids.len());
        let mut sample = reserved(sample_count)?;
        let mut training = reserved(sample_count)?;
        for slot in 0..sample_count {
            let index = slot * ids.len() / sample_count;
            sample.push(index);
            let mut row = reserved(rows[index].len())?;
            row.extend_from_slice(&rows[index]);
            training.push(row);
        }
        let identical = training.iter().all(|row| row == &training[0]);
        let centers = if identical {
            Vec::new()
        } else {
            budget.poll()?;
            budget.counts.trainer_calls += 1;
            // Actual trainer may stop early. This deliberately reports an
            // upper bound, not an invented observed iteration count.
            budget.counts.trainer_distance_upper_bound +=
                (2 * sample_count + 2) * 4 + 2 * sample_count;
            train_logical_cell_centroids(&training, VectorMetric::SquaredEuclidean, 2, 4)?
        };
        budget.poll()?;
        let coordinate = coordinate_split(ids, rows)?;
        let mut assigned = filled(ids.len(), false)?;
        let mut delta = reserved(ids.len())?;
        let mut degenerate = identical;
        if !identical {
            require(
                centers.len() == 2
                    && centers.iter().all(|center| {
                        center.len() == rows[0].len() && center.iter().all(|v| v.is_finite())
                    }),
                "diagnostic learned center geometry",
            )?;
            let separation = VectorMetric::SquaredEuclidean.distance(&centers[0], &centers[1])?;
            require(separation.is_finite(), "diagnostic nonfinite separator")?;
            degenerate = separation <= 0.;
            for (slot, row) in rows.iter().enumerate() {
                let a = VectorMetric::SquaredEuclidean.distance(row, &centers[0])?;
                let b = VectorMetric::SquaredEuclidean.distance(row, &centers[1])?;
                let margin = a - b;
                require(
                    a.is_finite() && b.is_finite() && margin.is_finite(),
                    "diagnostic nonfinite learned assignment",
                )?;
                assigned[slot] = a <= b;
                delta.push(margin);
                budget.counts.assignment_distances += 2;
            }
        }
        let count = assigned.iter().filter(|v| **v).count();
        let fallback = identical || count.min(ids.len() - count) < ids.len().div_ceil(4);
        let proposed = constrained(ids, &delta, &assigned, &coordinate, degenerate)?;
        let original = if fallback { coordinate } else { assigned };
        Ok(Replay {
            sample,
            centers,
            delta,
            original,
            proposed,
            fallback,
            identical,
            degenerate,
        })
    }
    fn verify_membership(
        ids: &[usize],
        assigned: &[bool],
        actual_left: &[usize],
        actual_right: &[usize],
    ) -> Result<()> {
        require(
            ids.len() == assigned.len() && actual_left.len() + actual_right.len() == ids.len(),
            "diagnostic original child membership count",
        )?;
        let mut left = reserved(actual_left.len())?;
        let mut right = reserved(actual_right.len())?;
        left.extend_from_slice(actual_left);
        right.extend_from_slice(actual_right);
        left.sort_unstable();
        right.sort_unstable();
        require(
            ids.iter()
                .zip(assigned)
                .filter_map(|(&id, &a)| a.then_some(id))
                .eq(left)
                && ids
                    .iter()
                    .zip(assigned)
                    .filter_map(|(&id, &a)| (!a).then_some(id))
                    .eq(right),
            "diagnostic original child membership mismatch",
        )
    }
    fn panel(ids: &[usize], sample: &[usize], root: &str, offset: usize) -> Result<Vec<usize>> {
        let mut sampled = filled(ids.len(), false)?;
        for &slot in sample {
            require(
                slot < ids.len() && !sampled[slot],
                "diagnostic trainer sample uniqueness",
            )?;
            sampled[slot] = true;
        }
        let mut order = reserved(ids.len())?;
        order.extend(
            ids.iter()
                .enumerate()
                .filter(|(slot, _)| !sampled[*slot])
                .map(|(slot, &id)| (row_key(root, offset, id), id, slot)),
        );
        order.sort_unstable();
        order.truncate(PANEL);
        let mut slots = reserved(order.len())?;
        slots.extend(order.into_iter().map(|(_, _, slot)| slot));
        Ok(slots)
    }
    fn cuts(
        ids: &[usize],
        rows: &[Vec<f32>],
        panel: &[usize],
        old: &[bool],
        matched: &[bool],
        new: &[bool],
        budget: &mut Budget,
    ) -> Result<([usize; 3], String)> {
        require(
            panel.len() == PANEL
                && ids.len() == rows.len()
                && old.len() == ids.len()
                && matched.len() == ids.len()
                && new.len() == ids.len(),
            "diagnostic exact 128-row panel",
        )?;
        let mut crossings = [0_usize; 3];
        let mut edges = Sha256::new();
        let mut neighbors = reserved(ids.len() - 1)?;
        for (ordinal, &slot) in panel.iter().enumerate() {
            require(slot < ids.len(), "diagnostic panel source bounds")?;
            neighbors.clear();
            for (other, row) in rows.iter().enumerate() {
                if slot == other {
                    continue;
                }
                let distance = VectorMetric::Cosine.distance(&rows[slot], row)?;
                require(distance.is_finite(), "diagnostic nonfinite local cosine")?;
                neighbors.push((distance, ids[other], other));
                budget.counts.neighbor_distances += 1;
            }
            neighbors.sort_unstable_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
            for &(distance, id, other) in &neighbors[..NEIGHBORS] {
                for (count, membership) in crossings.iter_mut().zip([old, matched, new]) {
                    *count += usize::from(membership[slot] != membership[other]);
                }
                edges.update((ids[slot] as u64).to_le_bytes());
                edges.update((id as u64).to_le_bytes());
                edges.update(distance.to_le_bytes());
            }
            if ordinal % 4 == 0 {
                budget.poll()?;
            }
        }
        Ok((crossings, format!("{:x}", edges.finalize())))
    }

    #[derive(Default)]
    struct Screening {
        nodes: usize,
        sums: [usize; 3],
        worst: [usize; 3],
    }
    impl Screening {
        fn add(&mut self, crossings: [usize; 3]) {
            self.nodes += 1;
            for index in 0..3 {
                self.sums[index] += crossings[index];
                self.worst[index] = self.worst[index].max(crossings[index]);
            }
        }
        fn guard(&self, baseline: usize) -> bool {
            self.sums[baseline] > 0
                && self.sums[2] * 10 <= self.sums[baseline] * 9
                && self.worst[2] <= self.worst[baseline]
        }
        fn status(&self) -> &'static str {
            if self.nodes < MIN_VERIFIED {
                "INCONCLUSIVE"
            } else if self.guard(0) && self.guard(1) {
                "PASS"
            } else {
                "REJECT"
            }
        }
    }

    fn dataset(
        inputs: &DatasetInputs,
        budget: &mut Budget,
        expected: (usize, usize),
    ) -> Result<Value> {
        let root_body = Pinned::small(&inputs.root, ROOT_CAP, budget)?;
        let manifest: Manifest = serde_json::from_slice(&root_body)?;
        geometry(&manifest, inputs, expected)?;
        let codec = RotatedTwoBitCodec::new(&manifest.mean, manifest.seed)?;
        require(
            codec.record_bytes() == manifest.input.records.bytes / manifest.rows,
            "diagnostic original codec record geometry",
        )?;
        let body = Pinned::small(&inputs.directories, DIRECTORY_CAP, budget)?;
        let cell_pin = Pinned::open(&inputs.cells, inputs.cells.bytes)?;
        let admission = directory_admission(
            manifest.directory_bytes,
            manifest.build.directories,
            manifest.build.cells,
        )?;
        require(
            admission.modeled_preload_peak_bytes <= MEMORY_CAP / 2,
            "diagnostic directory payload admission",
        )?;
        let mut prototype = Prototype {
            admitted_root_sha256: inputs.root.sha256.clone(),
            manifest,
            directories: BTreeMap::new(),
            cells: cell_pin.file.try_clone()?,
            codec,
            startup: ReadStats::default(),
            startup_directory: ReadStats::default(),
            directory_admission: admission,
        };
        prototype.preload(&body)?;
        let mut pending = reserved(prototype.directories.len())?;
        pending.push((prototype.manifest.root_directory.offset, 1));
        let mut deepest = 1;
        while let Some((offset, depth)) = pending.pop() {
            require(
                depth <= prototype.manifest.input.max_depth,
                "diagnostic complete directory depth",
            )?;
            let page = &prototype.directories[&offset];
            require(
                page.children.len() == 2,
                "diagnostic original binary directory fanout",
            )?;
            for child in &page.children {
                deepest = deepest.max(depth + 1);
                match &child.target {
                    Target::Directory { span } => pending.push((span.offset, depth + 1)),
                    Target::Cell { .. } => require(
                        depth + 1 <= prototype.manifest.input.max_depth,
                        "diagnostic complete leaf depth",
                    )?,
                }
            }
        }
        require(
            deepest == prototype.manifest.build.max_depth,
            "diagnostic original maximum depth receipt",
        )?;
        drop(body);
        let selected = candidate_offsets(&prototype, &inputs.root.sha256)?;
        let roster = rosters(&prototype, inputs, budget)?;
        cell_pin.unchanged()?;
        let source =
            Source::authenticate(inputs, prototype.rows(), prototype.dimensions(), budget)?;
        let mut reports = reserved(selected.len())?;
        let mut measured = 0;
        let mut verified_fallbacks = 0;
        let mut nondegenerate_fallbacks = 0;
        let mut changed_fallbacks = 0;
        let mut all_measured = Screening::default();
        let mut acceptance = Screening::default();
        for &offset in &selected {
            budget.poll()?;
            let page = &prototype.directories[&offset];
            let mut left = reserved(page.children[0].rows)?;
            let mut right = reserved(page.children[1].rows)?;
            members(&page.children[0], &prototype, &roster, 1, &mut left)?;
            members(&page.children[1], &prototype, &roster, 1, &mut right)?;
            let mut ids = reserved(left.len() + right.len())?;
            ids.extend_from_slice(&left);
            ids.extend_from_slice(&right);
            ids.sort_unstable();
            require(
                ids.windows(2).all(|w| w[0] < w[1]),
                "diagnostic unique parent membership",
            )?;
            let rows = source.rows(&ids, budget)?;
            let replay = replay(&ids, &rows, prototype.manifest.input.sample_rows, budget)?;
            verify_membership(&ids, &replay.original, &left, &right)?;
            let proposed_left = replay.proposed.iter().filter(|v| **v).count();
            let median = coordinate_split(&ids, &rows)?;
            let matched = coordinate_partition(&ids, &rows, proposed_left)?;
            let changed =
                replay.fallback && !replay.degenerate && replay.proposed != replay.original;
            let slots = panel(&ids, &replay.sample, &inputs.root.sha256, offset)?;
            let mut report = json!({"directory_offset":offset,"selection_hash":node_key(&inputs.root.sha256,offset).iter()
                    .map(|byte| format!("{byte:02x}")).collect::<String>(),
                "rows":ids.len(),"original_membership_verified":true,
                "original_fallback":replay.fallback,"identical_sample":replay.identical,
                "degenerate_separator":replay.degenerate,
                "changed_nondegenerate_fallback":changed,
                "parent_source_ordinals_sha256":id_hash(ids.iter().copied()),
                "original_left_sha256":id_hash(ids.iter().zip(&replay.original).filter_map(|(&id,&a)| a.then_some(id))),
                "original_right_sha256":id_hash(ids.iter().zip(&replay.original).filter_map(|(&id,&a)| (!a).then_some(id))),
                "proposed_left_sha256":id_hash(ids.iter().zip(&replay.proposed).filter_map(|(&id,&a)| a.then_some(id))),
                "proposed_right_sha256":id_hash(ids.iter().zip(&replay.proposed).filter_map(|(&id,&a)| (!a).then_some(id))),
                "coordinate_median_left_sha256":id_hash(ids.iter().zip(&median).filter_map(|(&id,&a)| a.then_some(id))),
                "coordinate_median_right_sha256":id_hash(ids.iter().zip(&median).filter_map(|(&id,&a)| (!a).then_some(id))),
                "matched_coordinate_left_sha256":id_hash(ids.iter().zip(&matched).filter_map(|(&id,&a)| a.then_some(id))),
                "matched_coordinate_right_sha256":id_hash(ids.iter().zip(&matched).filter_map(|(&id,&a)| (!a).then_some(id))),
                "coordinate_median_left_rows":ids.len()/2,"coordinate_median_right_rows":ids.len()-ids.len()/2,
                "matched_coordinate_left_rows":proposed_left,"matched_coordinate_right_rows":ids.len()-proposed_left,
                "proposed_left_rows":proposed_left,"proposed_right_rows":ids.len()-proposed_left,
                "source_fp32_sha256":float_hash(rows.iter().flatten().copied()),
                "trainer_source_ordinals":replay.sample.iter().map(|&slot| ids[slot]).collect::<Vec<_>>(),
                "trainer_max_iterations":4,"trainer_metric":"squared_euclidean",
                "learned_centers":replay.centers,
                "centers_fp32_sha256":float_hash(replay.centers.iter().flatten().copied()),
                "delta_fp32_sha256":float_hash(replay.delta.iter().copied()),
                "source_panel_ordinals":slots.iter().map(|&slot| ids[slot]).collect::<Vec<_>>()});
            if !replay.fallback {
                report["status"] = json!("SKIPPED_SUCCESSFUL_UNCONSTRAINED");
            } else {
                verified_fallbacks += 1;
                nondegenerate_fallbacks += usize::from(!replay.degenerate);
                changed_fallbacks += usize::from(changed);
                if slots.len() < PANEL {
                    report["status"] = json!("INSUFFICIENT_NONTRAINING_ROWS");
                } else {
                    let (crossings, edge_sha) = cuts(
                        &ids,
                        &rows,
                        &slots,
                        &median,
                        &matched,
                        &replay.proposed,
                        budget,
                    )?;
                    measured += 1;
                    all_measured.add(crossings);
                    if changed {
                        acceptance.add(crossings);
                    }
                    report["status"] = json!("MEASURED");
                    report["included_in_acceptance"] = json!(changed);
                    report["local_directed_edges"] = json!(PANEL * NEIGHBORS);
                    report["local_cosine16_edges_sha256"] = json!(edge_sha);
                    for (name, count) in ["coordinate_median", "matched_coordinate", "constrained"]
                        .into_iter()
                        .zip(crossings)
                    {
                        report[format!("{name}_crossing_edges")] = json!(count);
                        report[format!("{name}_cut_fraction")] =
                            json!(count as f64 / (PANEL * NEIGHBORS) as f64);
                    }
                }
            }
            reports.push(report);
        }
        source.pin.unchanged()?;
        cell_pin.unchanged()?;
        let edges = acceptance.nodes * PANEL * NEIGHBORS;
        let fraction = |count| {
            if edges == 0 {
                Value::Null
            } else {
                json!(count as f64 / edges as f64)
            }
        };
        let mut report = json!({"input_pins":inputs,"selected_directory_offsets":selected,"nodes":reports,
            "verified_fallback_nodes":verified_fallbacks,"measured_nodes":measured,
            "nondegenerate_fallback_nodes":nondegenerate_fallbacks,
            "changed_fallback_nodes":changed_fallbacks,"unchanged_fallback_nodes":verified_fallbacks-changed_fallbacks,
            "changed_nondegenerate_measured_nodes":acceptance.nodes,
            "acceptance_scope":"changed nondegenerate measured fallback nodes only",
            "status":acceptance.status(),"acceptance_directed_edges":edges,
            "all_measured_crossing_edges_median_matched_constrained":all_measured.sums,
            "old_median_ten_percent_and_worst_guard":acceptance.guard(0),
            "matched_coordinate_ten_percent_and_worst_guard":acceptance.guard(1)});
        for (index, name) in ["coordinate_median", "matched_coordinate", "constrained"]
            .into_iter()
            .enumerate()
        {
            report[format!("{name}_crossing_edges")] = json!(acceptance.sums[index]);
            report[format!("{name}_aggregate_cut_fraction")] = fraction(acceptance.sums[index]);
            report[format!("{name}_worst_node_cut_fraction")] = if acceptance.nodes == 0 {
                Value::Null
            } else {
                json!(acceptance.worst[index] as f64 / (PANEL * NEIGHBORS) as f64)
            };
        }
        Ok(report)
    }

    /// Check exact originals under the caller's CPU1/512Mi/noSwap cgroup.
    /// Structural/authentication/control/resource failures return an error;
    /// Fewer than four genuinely changed, nondegenerate measured fallback nodes
    /// close INCONCLUSIVE without expanding the first eight.
    /// The returned JSON measures source-neighborhood cuts, never ANN recall.
    pub fn check(config: &Config) -> Result<Value> {
        descriptors(config)?;
        let mut budget = Budget::new(true)?;
        check_with_budget(config, &mut budget, PRODUCTION_GEOMETRY)
    }
    fn check_with_budget(
        config: &Config,
        budget: &mut Budget,
        expected: (usize, usize),
    ) -> Result<Value> {
        descriptors(config)?;
        let mut datasets = serde_json::Map::new();
        for (name, inputs) in &config.datasets {
            datasets.insert(name.clone(), dataset(inputs, budget, expected)?);
        }
        let status = if datasets.values().any(|d| d["status"] == "INCONCLUSIVE") {
            "INCONCLUSIVE"
        } else if datasets.values().all(|d| d["status"] == "PASS") {
            "PASS"
        } else {
            "REJECT"
        };
        budget.poll()?;
        Ok(json!({"schema":REPORT_SCHEMA,"status":status,
            "scope":"source-neighborhood diagnostic; not recall or product quality",
            "requires_matching_supervisor_exit_receipt":true,
            "durability":{"status":"SUPERVISOR_EXIT_RECEIPT_REQUIRED","standalone_authority":false},
            "query_or_truth_used":false,"descendant_rebuilds":0,"retraining_sweeps":0,
            "selection":"first8 SHA256(raw32 root digest || LE u64 directory offset); no expansion",
            "panel":"first128 SHA256(raw32 root digest || LE u64 directory offset || LE u64 source ordinal), excluding trainer sample",
            "datasets":datasets,"resources":budget.receipt()?}))
    }

    /// Execute `CONFIG SHA NEW_OUTPUT` with bounded, fsynced terminal JSON.
    /// Creates the output exclusively before admitting config or originals, so
    /// subsequent failures leave a terminal INVALID or INPUT_UNAVAILABLE report.
    /// Returns the report status; errors are reserved for output creation/sync.
    /// A body alone has no terminal authority; matching original supervisor
    /// exit/resource closure is required even after these sync calls succeed.
    pub fn execute(config_path: &Path, sha: &str, output: &Path) -> Result<String> {
        execute_with_budget(config_path, sha, output, Budget::new(true))
    }
    fn execute_with_budget(
        config_path: &Path,
        sha: &str,
        output: &Path,
        mut budget: Result<Budget>,
    ) -> Result<String> {
        let directory = secure_open(
            output.parent().ok_or("diagnostic output parent")?,
            rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::DIRECTORY,
        )?;
        let mut file = create_output(&directory, output)?;
        let result = (|| -> Result<Value> {
            let budget = budget
                .as_mut()
                .map_err(|error| -> Box<dyn Error + Send + Sync> { error.to_string().into() })?;
            require(valid_sha(sha), "diagnostic config SHA256")?;
            let file = secure_open(config_path, rustix::fs::OFlags::RDONLY)?;
            let pin = Pinned {
                stamp: stamp(&file)?,
                file,
            };
            let bytes = usize::try_from(pin.stamp.2)?;
            require(
                bytes > 0 && bytes <= ROOT_CAP,
                "diagnostic strict config size/type",
            )?;
            budget.admit(bytes)?;
            let body = pin.at(0, bytes)?;
            require(hash(&body) == sha, "diagnostic config authentication")?;
            budget.verified(bytes)?;
            pin.unchanged()?;
            let config: Config = serde_json::from_slice(&body)?;
            let mut report = check_with_budget(&config, budget, PRODUCTION_GEOMETRY)?;
            report["config_sha256"] = json!(sha);
            Ok(report)
        })();
        let mut report = match result {
            Ok(report) => report,
            Err(error) => {
                json!({"schema":REPORT_SCHEMA,"status":if error.is::<InputUnavailable>() {
                "INPUT_UNAVAILABLE" } else { "INVALID" },"config_sha256":sha,
                "scope":"source-neighborhood diagnostic; not recall or product quality",
                "query_or_truth_used":false,"resources":budget.as_ref().ok().and_then(|b| b.receipt().ok()),
                "error":error.to_string().chars().take(1024).collect::<String>()})
            }
        };
        seal_terminal(&mut file, &directory, &mut report)
    }

    fn seal_terminal(file: &mut File, directory: &File, report: &mut Value) -> Result<String> {
        seal_terminal_with(file, directory, report, File::sync_all, File::sync_all)
    }
    fn seal_terminal_with(
        file: &mut File,
        directory: &File,
        report: &mut Value,
        mut sync_file: impl FnMut(&File) -> std::io::Result<()>,
        mut sync_directory: impl FnMut(&File) -> std::io::Result<()>,
    ) -> Result<String> {
        report["requires_matching_supervisor_exit_receipt"] = json!(true);
        report["durability"] =
            json!({"status":"SUPERVISOR_EXIT_RECEIPT_REQUIRED","standalone_authority":false});
        let body = terminal_body(report)?;
        let sealed = (|| -> Result<()> {
            file.write_all(&body)?;
            sync_file(file)?;
            sync_directory(directory)?;
            Ok(())
        })();
        if let Err(error) = sealed {
            report["status"] = json!("INVALID");
            report["error"] = json!(error.to_string().chars().take(1024).collect::<String>());
            report["durability"] = json!({"status":"FAILED_OR_UNCONFIRMED","standalone_authority":false,
                "best_effort_invalid_rewrite":true,"rewrite_durability_confirmed":false});
            // This attempt is diagnostic only. Its success does not repair the
            // failed barrier; the original call always fails and CLI exits 2.
            if let Ok(invalid) = terminal_body(report) {
                let _ = file
                    .seek(SeekFrom::Start(0))
                    .and_then(|_| file.write_all(&invalid))
                    .and_then(|_| file.set_len(invalid.len() as u64))
                    .and_then(|_| sync_file(file));
                let _ = sync_directory(directory);
            }
            return Err(error);
        }
        Ok(report["status"]
            .as_str()
            .ok_or("diagnostic terminal status")?
            .to_owned())
    }

    struct TerminalBuffer(Vec<u8>);
    impl Write for TerminalBuffer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if !self
                .0
                .len()
                .checked_add(bytes.len())
                .is_some_and(|size| size < OUTPUT_CAP)
            {
                return Err(std::io::Error::other("diagnostic terminal output cap"));
            }
            self.0.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    fn terminal_body(report: &mut Value) -> Result<Vec<u8>> {
        let mut buffer = TerminalBuffer(reserved(OUTPUT_CAP)?);
        if serde_json::to_writer(&mut buffer, &*report).is_err() {
            *report = json!({"schema":REPORT_SCHEMA,"status":"INVALID","error":"diagnostic output cap",
                "query_or_truth_used":false,"requires_matching_supervisor_exit_receipt":true,
                "durability":{"status":"SUPERVISOR_EXIT_RECEIPT_REQUIRED","standalone_authority":false}});
            buffer.0.clear();
            serde_json::to_writer(&mut buffer, &*report)?;
        }
        buffer.0.push(b'\n');
        Ok(buffer.0)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn artifact(path: &Path, body: &[u8]) -> Artifact {
            fs::write(path, body).unwrap();
            Artifact {
                path: path.into(),
                bytes: body.len(),
                sha256: hash(body),
            }
        }
        fn inputs(dir: &Path) -> DatasetInputs {
            DatasetInputs {
                root: artifact(&dir.join("root"), b"root"),
                directories: artifact(&dir.join("directories"), b"directory"),
                cells: artifact(&dir.join("cells"), b"cells"),
                canonical: artifact(&dir.join("canonical"), b"canonical"),
                order: artifact(&dir.join("order"), b"order"),
            }
        }
        fn budget() -> Budget {
            Budget::new(false).unwrap()
        }
        fn source_fixture(dir: &Path) -> DatasetInputs {
            let mut inputs = inputs(dir);
            let mut canonical = Vec::new();
            let mut order = Vec::new();
            for (id, row) in [(2_u64, [1_f32, 0_f32]), (0, [0., 1.]), (1, [-1., 0.])] {
                canonical.extend_from_slice(&(id as i64).to_le_bytes());
                for value in row {
                    canonical.extend_from_slice(&value.to_le_bytes());
                }
                order.extend_from_slice(&id.to_le_bytes());
            }
            inputs.canonical = artifact(&dir.join("canonical"), &canonical);
            inputs.order = artifact(&dir.join("order"), &order);
            inputs
        }

        fn pipeline_fixture(dir: &Path) -> DatasetInputs {
            const ROWS: usize = 4096;
            let codec = RotatedTwoBitCodec::new(&[0., 0.], NATIVE_CODEC_SEED).unwrap();
            let mut canonical = Vec::new();
            let mut codes = Vec::new();
            let mut sq8 = Vec::new();
            let mut order = Vec::new();
            for id in (0..ROWS).rev() {
                // Four source clusters give balanced upper nodes. Within each
                // 1024-row cluster, 1/8 of rows occupy a distinct nearby mode;
                // the candidate builder repairs that split to 1/4..3/4 capacity.
                let base = [-20_f64, 20., 160., 200.][id / 1024];
                let angle = if id % 256 < 32 {
                    base + if id / 1024 % 2 == 0 { 5. } else { -5. }
                } else {
                    base
                };
                let (sine, cosine) = angle.to_radians().sin_cos();
                let row = [cosine as f32, sine as f32];
                order.extend_from_slice(&(id as u64).to_le_bytes());
                canonical.extend_from_slice(&(id as i64).to_le_bytes());
                for value in row {
                    canonical.extend_from_slice(&value.to_le_bytes());
                }
                codes.extend(codec.encode(&row).unwrap());
                sq8.extend_from_slice(&(id as i64).to_le_bytes());
                sq8.extend_from_slice(&1_f32.to_le_bytes());
                sq8.extend(row.iter().map(|value| ((value + 1.) * 127.5).round() as u8));
            }
            let canonical = artifact(&dir.join("canonical"), &canonical);
            let order = artifact(&dir.join("order"), &order);
            let records = artifact(&dir.join("records"), &codes);
            let sq8 = artifact(&dir.join("sq8"), &sq8);
            let unused = artifact(
                &dir.join("unused-admission"),
                b"synthetic builder-node fixture only",
            );
            let mean = artifact(&dir.join("mean"), &[0; 8]);
            let config = BuildConfig {
                schema: BUILD_SCHEMA.into(),
                generation: unused.clone(),
                plane: unused,
                canonical: canonical.clone(),
                order: order.clone(),
                records: records.clone(),
                mean,
                sq8: sq8.clone(),
                cell_rows: 512,
                sample_rows: 32,
                max_depth: 24,
                max_build_payload_bytes: 64 * 1024 * 1024,
                max_output_bytes: 16 * 1024 * 1024,
            };
            let directory_path = dir.join("directories");
            let cells_path = dir.join("cells");
            let mut builder = Builder {
                config: &config,
                canonical: secure_open(&canonical.path, rustix::fs::OFlags::RDONLY).unwrap(),
                records: secure_open(&records.path, rustix::fs::OFlags::RDONLY).unwrap(),
                sq8: secure_open(&sq8.path, rustix::fs::OFlags::RDONLY).unwrap(),
                directories: new_file(&directory_path).unwrap(),
                cells: new_file(&cells_path).unwrap(),
                inverse: (0..ROWS).rev().collect(),
                dimensions: 2,
                record_bytes: codec.record_bytes(),
                directory_bytes: 0,
                directory_digest: Sha256::new(),
                cell_bytes: 0,
                next_row: 0,
                receipt: BuildReceipt::default(),
                replay: None,
            };
            // Exercise the real candidate builder/trainer. This is current
            // synthetic geometry, never a replacement for archived original data.
            let node = builder.node((0..ROWS).collect(), 1).unwrap();
            let Target::Directory { span } = node.target else {
                panic!("fixture has no hierarchy")
            };
            assert_eq!(builder.next_row, ROWS);
            assert_eq!(builder.receipt.semantic_repairs, 4);
            assert_eq!(builder.receipt.geometry_fallbacks, 4);
            builder.directories.sync_all().unwrap();
            builder.cells.sync_all().unwrap();
            let mut manifest = Manifest {
                schema: SCHEMA.into(),
                input: config.clone(),
                rows: ROWS,
                dimensions: 2,
                seed: NATIVE_CODEC_SEED,
                mean: vec![0.; 2],
                low: vec![-1.; 2],
                step: vec![2. / 255.; 2],
                root_directory: span,
                directory_bytes: builder.directory_bytes,
                directory_sha256: format!("{:x}", builder.directory_digest.finalize()),
                cell_bytes: builder.cell_bytes,
                build: builder.receipt,
            };
            let mut body = Vec::new();
            for _ in 0..8 {
                body = serde_json::to_vec(&manifest).unwrap();
                let bytes = manifest.directory_bytes + manifest.cell_bytes + body.len();
                if manifest.build.output_bytes == bytes {
                    break;
                }
                manifest.build.output_bytes = bytes;
            }
            assert_eq!(
                manifest.build.output_bytes,
                manifest.directory_bytes + manifest.cell_bytes + body.len()
            );
            DatasetInputs {
                root: artifact(&dir.join("root"), &body),
                directories: artifact(&directory_path, &fs::read(&directory_path).unwrap()),
                cells: artifact(&cells_path, &fs::read(&cells_path).unwrap()),
                canonical,
                order,
            }
        }

        #[test]
        fn constrained_cost_matches_exhaustive_small_partitions() {
            // Fixed-center squared-distance cost differs from sum(left delta)
            // by a constant. Exhaust every feasible small binary partition.
            for rows in 2_usize..=7 {
                let ids = (0..rows).collect::<Vec<_>>();
                let fallback = (0..rows).map(|slot| slot < rows / 2).collect::<Vec<_>>();
                let lower = rows.div_ceil(4);
                for pattern in 0..3_usize.pow(rows as u32) {
                    let mut word = pattern;
                    let delta = (0..rows)
                        .map(|_| {
                            let value = (word % 3) as f32 - 1.;
                            word /= 3;
                            value
                        })
                        .collect::<Vec<_>>();
                    let original = delta.iter().map(|v| *v <= 0.).collect::<Vec<_>>();
                    let actual = constrained(&ids, &delta, &original, &fallback, false).unwrap();
                    let cost = actual
                        .iter()
                        .zip(&delta)
                        .filter_map(|(&left, &d)| left.then_some(d))
                        .sum::<f32>();
                    let best = (0..1_usize << rows)
                        .filter(|mask| {
                            let count = mask.count_ones() as usize;
                            count >= lower && count <= rows - lower
                        })
                        .map(|mask| {
                            delta
                                .iter()
                                .enumerate()
                                .filter_map(|(slot, &d)| ((mask >> slot) & 1 == 1).then_some(d))
                                .sum::<f32>()
                        })
                        .min_by(f32::total_cmp)
                        .unwrap();
                    assert_eq!(cost, best, "rows={rows} pattern={pattern}");
                    let count = actual.iter().filter(|v| **v).count();
                    assert!((lower..=rows - lower).contains(&count));
                }
            }
        }

        #[test]
        fn successful_identical_and_degenerate_splits_are_unchanged() {
            let ids = (0..16).collect::<Vec<_>>();
            let rows = (0..16)
                .map(|slot| {
                    if slot < 8 {
                        vec![1., 0.]
                    } else {
                        vec![-1., 0.]
                    }
                })
                .collect::<Vec<_>>();
            let replayed = replay(&ids, &rows, 8, &mut budget()).unwrap();
            assert!(!replayed.fallback);
            assert_eq!(replayed.original, replayed.proposed);
            assert_eq!(replayed.sample, vec![0, 2, 4, 6, 8, 10, 12, 14]);
            assert_eq!(replayed.centers, vec![vec![1., 0.], vec![-1., 0.]]);
            let same = vec![vec![1., 0.]; 16];
            let replayed = replay(&ids, &same, 8, &mut budget()).unwrap();
            assert!(replayed.fallback && replayed.identical && replayed.degenerate);
            assert!(replayed.centers.is_empty());
            assert_eq!(replayed.original, replayed.proposed);
            assert_eq!(
                replayed.original,
                (0..16).map(|slot| slot < 8).collect::<Vec<_>>()
            );
            let fallback = (0..16).map(|slot| slot < 8).collect::<Vec<_>>();
            assert_eq!(
                constrained(&ids, &[0.; 16], &[true; 16], &fallback, true).unwrap(),
                fallback
            );
        }

        #[test]
        fn learned_unbalanced_replay_preserves_centers_and_moves_minimum_population() {
            let ids = (0..20).collect::<Vec<_>>();
            let mut rows = vec![vec![1., 0.]; 20];
            rows[19] = vec![-1., 0.];
            let proof = replay(&ids, &rows, 20, &mut budget()).unwrap();
            assert!(proof.fallback && !proof.identical && !proof.degenerate);
            assert_eq!(proof.centers, vec![vec![1., 0.], vec![-1., 0.]]);
            assert_eq!(proof.proposed.iter().filter(|v| **v).count(), 15);
            assert_eq!(
                proof.proposed,
                (0..20).map(|slot| slot < 15).collect::<Vec<_>>()
            );
            let unconstrained = proof.delta.iter().map(|v| *v <= 0.).collect::<Vec<_>>();
            assert_eq!(
                unconstrained
                    .iter()
                    .zip(&proof.proposed)
                    .filter(|(a, b)| a != b)
                    .count(),
                4
            );
            assert_eq!(proof.original, coordinate_split(&ids, &rows).unwrap());
        }

        #[test]
        fn first_eight_candidate_hash_order_is_frozen_without_expansion() {
            let root = "a".repeat(64);
            let mut pages = (0..20).map(|offset| (offset, 512, 512)).collect::<Vec<_>>();
            pages.extend([(20, 256, 256), (21, 1025, 1025), (22, 400, 600)]);
            let actual = freeze_candidates(&root, pages.iter().copied(), pages.len()).unwrap();
            let mut expected = (0..20)
                .map(|offset| (node_key(&root, offset), offset))
                .collect::<Vec<_>>();
            expected.sort_unstable();
            assert_eq!(
                actual,
                expected[..8]
                    .iter()
                    .map(|(_, offset)| *offset)
                    .collect::<Vec<_>>()
            );
            pages.reverse();
            assert_eq!(
                actual,
                freeze_candidates(&root, pages.iter().copied(), pages.len()).unwrap()
            );
            assert_eq!(
                freeze_candidates(&root, [(2, 256, 257)].into_iter(), 1).unwrap(),
                vec![2]
            );
            assert!(freeze_candidates(&root, [(0, usize::MAX, 1)].into_iter(), 1).is_err());
            assert!(freeze_candidates(&root, [(0, 512, 512)].into_iter(), 0).is_err());
        }

        #[test]
        fn deterministic_delta_ties_coordinate_ties_and_partial_panel() {
            let ids = vec![60, 10, 40, 30, 20, 70, 50];
            let result = constrained(
                &ids,
                &[1.; 7],
                &[false; 7],
                &[false, true, false, true, false, false, true],
                false,
            )
            .unwrap();
            let mut selected = ids
                .iter()
                .zip(&result)
                .filter_map(|(&id, &a)| a.then_some(id))
                .collect::<Vec<_>>();
            selected.sort_unstable();
            assert_eq!(selected, vec![10, 20]); // ceil(7/4), with ordinal tie breaking.
            let rows = vec![vec![1., 0.]; 7];
            let fallback = coordinate_split(&ids, &rows).unwrap();
            let mut selected = ids
                .iter()
                .zip(&fallback)
                .filter_map(|(&id, &a)| a.then_some(id))
                .collect::<Vec<_>>();
            selected.sort_unstable();
            assert_eq!(selected, vec![10, 20, 30]);
            let ids = (0..200).collect::<Vec<_>>();
            let sample = (0..80).collect::<Vec<_>>();
            let first = panel(&ids, &sample, &"b".repeat(64), 9).unwrap();
            assert_eq!(first.len(), 120); // no expansion/reuse of training rows.
            assert!(first.iter().all(|slot| *slot >= 80));
            assert_eq!(first, panel(&ids, &sample, &"b".repeat(64), 9).unwrap());
            assert!(panel(&ids, &[0, 0], &"b".repeat(64), 9).is_err());
            assert_ne!(node_key(&"a".repeat(64), 1), node_key(&"a".repeat(64), 2));
            assert_ne!(
                row_key(&"a".repeat(64), 1, 2),
                row_key(&"a".repeat(64), 1, 3)
            );
            let raw = [0xaa_u8; 32];
            let mut digest = Sha256::new();
            digest.update(raw);
            digest.update(9_u64.to_le_bytes());
            let expected: [u8; 32] = digest.finalize().into();
            assert_eq!(node_key(&"a".repeat(64), 9), expected);
            let mut digest = Sha256::new();
            digest.update(raw);
            digest.update(9_u64.to_le_bytes());
            digest.update(17_u64.to_le_bytes());
            let expected: [u8; 32] = digest.finalize().into();
            assert_eq!(row_key(&"a".repeat(64), 9, 17), expected);
        }

        #[test]
        fn original_membership_mismatch_is_invalid() {
            let ids = vec![0, 1, 2, 3];
            let membership = vec![true, true, false, false];
            verify_membership(&ids, &membership, &[1, 0], &[3, 2]).unwrap();
            assert!(verify_membership(&ids, &membership, &[0, 2], &[1, 3]).is_err());
            assert!(verify_membership(&ids, &membership, &[0, 0], &[2, 3]).is_err());
            assert!(verify_membership(&ids, &membership, &[0], &[2, 3]).is_err());
        }

        #[test]
        fn local_cosine_edges_reuse_same_rows_and_exclude_self() {
            let ids = (0..160).collect::<Vec<_>>();
            // Equal vectors make distance ties observable: ordinal is the
            // second key and self is excluded from the exact local 16NN.
            let rows = vec![vec![1., 0.]; 160];
            let panel = (0..128).collect::<Vec<_>>();
            let old = (0..160).map(|slot| slot < 80).collect::<Vec<_>>();
            let new = vec![true; 160];
            let ([a, matched, b], sha) =
                cuts(&ids, &rows, &panel, &old, &old, &new, &mut budget()).unwrap();
            assert_eq!(a, 48 * 16);
            assert_eq!(matched, a);
            assert_eq!(b, 0);
            let mut expected = Sha256::new();
            for &source in &panel {
                for neighbor in (0_usize..17).filter(|id| *id != source).take(16) {
                    expected.update((source as u64).to_le_bytes());
                    expected.update((neighbor as u64).to_le_bytes());
                    expected.update(0_f32.to_le_bytes());
                }
            }
            assert_eq!(sha, format!("{:x}", expected.finalize()));
            assert_eq!(
                sha,
                cuts(&ids, &rows, &panel, &old, &old, &new, &mut budget())
                    .unwrap()
                    .1
            );
            let ([a, _, b], _) =
                cuts(&ids, &rows, &panel, &old, &old, &old, &mut budget()).unwrap();
            assert_eq!(a, b);
            let mut changed = rows.clone();
            changed[100][0] = f32::NAN;
            assert!(cuts(&ids, &changed, &panel, &old, &old, &old, &mut budget()).is_err());
            // Identical source vectors have deterministic neighbor ties. A
            // 25/75 coordinate population alone appears to beat the old median
            // by 10% here; the proposed partition equals the matched control.
            let median = coordinate_split(&ids, &rows).unwrap();
            let matched = coordinate_partition(&ids, &rows, 120).unwrap();
            let (counts, _) = cuts(
                &ids,
                &rows,
                &panel,
                &median,
                &matched,
                &matched,
                &mut budget(),
            )
            .unwrap();
            assert!(counts[2] * 10 <= counts[0] * 9);
            assert_eq!(counts[1], counts[2]);
            let mut screening = Screening::default();
            for _ in 0..4 {
                screening.add(counts);
            }
            assert!(screening.guard(0));
            assert!(!screening.guard(1));
            assert_eq!(screening.status(), "REJECT");
            // A worst-node regression must reject an aggregate improvement.
            let mut worse = Screening::default();
            worse.add([200, 200, 160]);
            for _ in 0..3 {
                worse.add([1000, 1000, 200]);
            }
            assert_eq!(worse.status(), "PASS");
            worse.worst[2] = 1001;
            assert_eq!(worse.status(), "REJECT");
            screening.nodes = 3;
            assert_eq!(screening.status(), "INCONCLUSIVE");
        }

        #[test]
        fn canonical_order_nonfinite_and_post_authentication_tamper_fail_closed() {
            let dir = tempfile::tempdir().unwrap();
            let mut inputs = source_fixture(dir.path());
            let mut accounting = budget();
            let source = Source::authenticate(&inputs, 3, 2, &mut accounting).unwrap();
            assert_eq!(accounting.counts.auth_bytes, 72);
            assert_eq!(
                source.rows(&[0, 1, 2], &mut accounting).unwrap(),
                vec![vec![0., 1.], vec![-1., 0.], vec![1., 0.]]
            );
            assert_eq!(accounting.counts.auth_bytes, 120);
            assert_eq!(accounting.counts.admitted_bytes, 120);
            let mut body = fs::read(&inputs.canonical.path).unwrap();
            body[8..12].copy_from_slice(&0.5_f32.to_le_bytes());
            fs::write(&inputs.canonical.path, &body).unwrap();
            assert!(source.rows(&[2], &mut accounting).is_err());
            assert_eq!(accounting.counts.auth_bytes, 120);
            assert_eq!(accounting.counts.admitted_bytes, 136);
            let mut body = fs::read(&inputs.canonical.path).unwrap();
            body[8..12].copy_from_slice(&f32::NAN.to_le_bytes());
            inputs.canonical = artifact(&inputs.canonical.path, &body);
            assert!(Source::authenticate(&inputs, 3, 2, &mut budget()).is_err());
            inputs = source_fixture(dir.path());
            let mut growth = budget();
            assert!(
                Source::authenticate_with(&inputs, 3, 2, &mut growth, |_| {
                    OpenOptions::new()
                        .append(true)
                        .open(&inputs.canonical.path)?
                        .write_all(&[0; 65536])?;
                    Ok(())
                })
                .is_err()
            );
            assert_eq!(
                growth.counts.canonical_stream_read_bytes,
                inputs.canonical.bytes
            );
            assert_eq!(
                growth.counts.admitted_bytes,
                inputs.order.bytes + inputs.canonical.bytes
            );
            inputs = source_fixture(dir.path());
            inputs.order = artifact(&inputs.order.path, &[0_u8; 24]);
            assert!(Source::authenticate(&inputs, 3, 2, &mut budget()).is_err());
            inputs = source_fixture(dir.path());
            let words = [0_u64, 2, 1]
                .into_iter()
                .flat_map(u64::to_le_bytes)
                .collect::<Vec<_>>();
            inputs.order = artifact(&inputs.order.path, &words);
            assert!(Source::authenticate(&inputs, 3, 2, &mut budget()).is_err());
        }

        #[test]
        fn artifact_tamper_geometry_and_all_limits_fail_closed() {
            let dir = tempfile::tempdir().unwrap();
            let a = artifact(&dir.path().join("artifact"), b"abcd");
            assert_eq!(Pinned::small(&a, 4, &mut budget()).unwrap(), b"abcd");
            let mut wrong = a.clone();
            wrong.bytes = usize::MAX;
            assert!(Pinned::small(&wrong, 4, &mut budget()).is_err());
            wrong = a.clone();
            wrong.sha256 = "a".repeat(64);
            assert!(Pinned::small(&wrong, 4, &mut budget()).is_err());
            fs::write(&a.path, b"abce").unwrap();
            assert!(Pinned::small(&a, 4, &mut budget()).is_err());
            assert!(filled::<u64>(usize::MAX, 0).is_err());
            assert!(vector(&[0; 15], 2).is_err());
            assert!(vector(&[0; 16], 2).is_err()); // zero norm.
            assert!(coordinate_split(&[0, 1], &[vec![f32::MAX], vec![-f32::MAX]]).is_err());
            assert!(
                constrained(&[0, 1], &[f32::NAN, 1.], &[false; 2], &[true, false], false).is_err()
            );
            assert!(constrained(&[0, 1], &[0.], &[false; 2], &[true, false], true).is_err());
            assert!(replay(&[0, 0], &[vec![1., 0.], vec![1., 0.]], 2, &mut budget()).is_err());
            let mut b = budget();
            b.counts.admitted_bytes = AUTH_CAP;
            assert!(b.admit(1).is_err());
            assert_eq!(b.counts.auth_bytes, 0);
            assert_eq!(b.counts.admitted_bytes, AUTH_CAP);
            assert!(b.verified(AUTH_CAP + 1).is_err());
            let mut b = budget();
            b.counts.parent_rows = PARENT_ROWS_CAP;
            assert!(b.parents(1).is_err());
            let mut b = budget();
            b.start = Instant::now()
                .checked_sub(std::time::Duration::from_secs(181))
                .unwrap();
            assert!(b.poll().is_err());
            cgroup_controls("100000 100000", "536870912", "0").unwrap();
            for (cpu, memory, swap) in [
                ("max 100000", "536870912", "0"),
                ("200000 100000", "536870912", "0"),
                ("100000 100000", "536870913", "0"),
                ("100000 100000", "536870912", "1"),
                ("0 100000", "536870912", "0"),
            ] {
                assert!(cgroup_controls(cpu, memory, swap).is_err());
            }
            let first = dir.path().join("pipeline-cohere");
            fs::create_dir(&first).unwrap();
            let second = dir.path().join("pipeline-relaion");
            fs::create_dir(&second).unwrap();
            let config = Config {
                schema: CONFIG_SCHEMA.into(),
                datasets: BTreeMap::from([
                    ("cohere".into(), pipeline_fixture(&first)),
                    ("relaion".into(), pipeline_fixture(&second)),
                ]),
            };
            let mut accounting = budget();
            let report = check_with_budget(&config, &mut accounting, (4096, 2)).unwrap();
            // Repairs are no longer old median-fallback nodes. The remaining
            // selected identical nodes cannot establish a new semantic gain.
            assert_eq!(report["status"], "INCONCLUSIVE");
            for dataset in report["datasets"].as_object().unwrap().values() {
                assert_eq!(dataset["measured_nodes"], 4);
                assert_eq!(dataset["changed_nondegenerate_measured_nodes"], 0);
                assert_eq!(dataset["unchanged_fallback_nodes"], 4);
                assert_eq!(
                    dataset["selected_directory_offsets"]
                        .as_array()
                        .unwrap()
                        .len(),
                    6
                );
                for node in dataset["nodes"].as_array().unwrap() {
                    assert_eq!(node["original_membership_verified"], true);
                    assert_eq!(
                        node["matched_coordinate_left_rows"],
                        node["proposed_left_rows"]
                    );
                }
            }
            assert_eq!(accounting.counts.parent_rows, 14336);
            assert!(accounting.counts.admitted_bytes < AUTH_CAP);
            assert_eq!(
                accounting.counts.auth_bytes,
                accounting.counts.admitted_bytes
            );
            // The synthetic geometry seam is private; normal production
            // admission rejects this otherwise complete source-bound fixture.
            assert!(check_with_budget(&config, &mut budget(), PRODUCTION_GEOMETRY).is_err());
        }

        #[test]
        fn fifo_symlink_ancestors_and_missing_originals_fail_closed() {
            let dir = tempfile::tempdir().unwrap();
            let a = artifact(&dir.path().join("real"), b"abcd");
            let link = dir.path().join("link");
            std::os::unix::fs::symlink(&a.path, &link).unwrap();
            let mut redirected = a.clone();
            redirected.path = link;
            assert!(Pinned::small(&redirected, 4, &mut budget()).is_err());
            let alias = dir.path().join("alias");
            std::os::unix::fs::symlink(dir.path(), &alias).unwrap();
            redirected.path = alias.join("real");
            assert!(Pinned::small(&redirected, 4, &mut budget()).is_err());
            let fifo = dir.path().join("fifo");
            rustix::fs::mknodat(
                rustix::fs::CWD,
                &fifo,
                rustix::fs::FileType::Fifo,
                rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
                0,
            )
            .unwrap();
            redirected.path = fifo;
            let start = Instant::now();
            assert!(Pinned::small(&redirected, 4, &mut budget()).is_err());
            assert!(start.elapsed().as_secs_f64() < 1.);
            redirected.path = dir.path().join("missing");
            let error = Pinned::small(&redirected, 4, &mut budget()).unwrap_err();
            assert!(error.is::<InputUnavailable>());
        }

        #[test]
        fn strict_config_and_created_output_terminal_no_overwrite() {
            let dir = tempfile::tempdir().unwrap();
            let first = dir.path().join("cohere");
            fs::create_dir(&first).unwrap();
            let second = dir.path().join("relaion");
            fs::create_dir(&second).unwrap();
            let config = Config {
                schema: CONFIG_SCHEMA.into(),
                datasets: BTreeMap::from([
                    ("cohere".into(), inputs(&first)),
                    ("relaion".into(), inputs(&second)),
                ]),
            };
            descriptors(&config).unwrap();
            let mut obsolete = config.clone();
            obsolete.schema = "borsuk-constrained-split-config-v1".into();
            assert!(descriptors(&obsolete).is_err());
            let value = serde_json::to_value(&config).unwrap();
            for key in [
                "query",
                "queries",
                "truth",
                "gt",
                "requests",
                "sample_rows",
                "download",
            ] {
                let mut bad = value.clone();
                bad[key] = json!([]);
                assert!(serde_json::from_value::<Config>(bad).is_err());
                let mut bad = value.clone();
                bad["datasets"]["cohere"][key] = json!([]);
                assert!(serde_json::from_value::<Config>(bad).is_err());
            }
            let mut bad = config.clone();
            bad.datasets.get_mut("relaion").unwrap().canonical =
                bad.datasets["cohere"].canonical.clone();
            assert!(descriptors(&bad).is_err());
            let config_path = dir.path().join("config");
            fs::write(&config_path, b"{}").unwrap();
            let output = dir.path().join("terminal");
            // Independent of the test caller's cgroup: missing controls or a
            // wrong SHA both fail before originals are opened, with INVALID.
            assert_eq!(
                execute(&config_path, &"a".repeat(64), &output).unwrap(),
                "INVALID"
            );
            let body = fs::read(&output).unwrap();
            let report: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(report["status"], "INVALID");
            assert!(body.len() <= OUTPUT_CAP);
            assert!(execute(&config_path, &"a".repeat(64), &output).is_err());
            assert_eq!(fs::read(&output).unwrap(), body);
            let mut unavailable = config;
            unavailable.datasets.get_mut("cohere").unwrap().root.path =
                dir.path().join("missing-original-root");
            let body = serde_json::to_vec(&unavailable).unwrap();
            fs::write(&config_path, &body).unwrap();
            let output = dir.path().join("unavailable-terminal");
            assert_eq!(
                execute_with_budget(&config_path, &hash(&body), &output, Ok(budget())).unwrap(),
                "INPUT_UNAVAILABLE"
            );
            let report: Value = serde_json::from_slice(&fs::read(&output).unwrap()).unwrap();
            assert_eq!(report["status"], "INPUT_UNAVAILABLE");
            assert_eq!(report["query_or_truth_used"], false);
            let output = dir.path().join("tampered-config-terminal");
            assert_eq!(
                execute_with_budget(&config_path, &"a".repeat(64), &output, Ok(budget())).unwrap(),
                "INVALID"
            );
        }

        #[test]
        fn output_cap_and_fsync_failure_cannot_return_pass() {
            let mut oversized =
                json!({"schema":REPORT_SCHEMA,"status":"PASS","payload":"x".repeat(OUTPUT_CAP)});
            let body = terminal_body(&mut oversized).unwrap();
            assert_eq!(oversized["status"], "INVALID");
            assert!(body.len() < 512 && body.last() == Some(&b'\n'));
            let dir = tempfile::tempdir().unwrap();
            let parent = secure_open(
                dir.path(),
                rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::DIRECTORY,
            )
            .unwrap();
            let output = dir.path().join("sealed");
            let mut file = secure_open(
                &output,
                rustix::fs::OFlags::WRONLY | rustix::fs::OFlags::CREATE | rustix::fs::OFlags::EXCL,
            )
            .unwrap();
            let mut report = json!({"schema":REPORT_SCHEMA,"status":"INCONCLUSIVE"});
            assert_eq!(
                seal_terminal(&mut file, &parent, &mut report).unwrap(),
                "INCONCLUSIVE"
            );
            let bytes = fs::read(&output).unwrap();
            assert_eq!(serde_json::from_slice::<Value>(&bytes).unwrap(), report);
            // Socket writes succeed but fsync is unsupported. An interrupted
            // durability barrier must never be reported as successful sealing.
            let (sender, _receiver) = std::os::unix::net::UnixStream::pair().unwrap();
            let mut socket = File::from(std::os::fd::OwnedFd::from(sender));
            let mut report = json!({"schema":REPORT_SCHEMA,"status":"PASS"});
            assert!(seal_terminal(&mut socket, &parent, &mut report).is_err());
            for failed in ["file", "directory"] {
                let path = dir.path().join(format!("failed-{failed}"));
                let mut file = create_output(&parent, &path).unwrap();
                let mut report =
                    json!({"schema":REPORT_SCHEMA,"status":"PASS","config_sha256":"a".repeat(64)});
                let result = seal_terminal_with(
                    &mut file,
                    &parent,
                    &mut report,
                    |file| {
                        if failed == "file" {
                            Err(std::io::Error::other("injected file sync failure"))
                        } else {
                            file.sync_all()
                        }
                    },
                    |directory| {
                        if failed == "directory" {
                            Err(std::io::Error::other("injected directory sync failure"))
                        } else {
                            directory.sync_all()
                        }
                    },
                );
                assert!(result.is_err());
                let body = fs::read(&path).unwrap();
                let readback: Value = serde_json::from_slice(&body).unwrap();
                assert_eq!(readback["status"], "INVALID");
                assert_eq!(readback["requires_matching_supervisor_exit_receipt"], true);
                assert_eq!(
                    readback["durability"]["rewrite_durability_confirmed"],
                    false
                );
                let supervisor = SupervisorReceipt {
                    run_id: "original-run".into(),
                    config_sha256: "a".repeat(64),
                    report_sha256: hash(&body),
                    process_exit_code: 2,
                    resources_closed: true,
                };
                assert!(admit_pass(&body, "original-run", &supervisor).is_err());
            }
            let path = dir.path().join("pass-looking-body");
            let mut file = create_output(&parent, &path).unwrap();
            let mut report =
                json!({"schema":REPORT_SCHEMA,"status":"PASS","config_sha256":"a".repeat(64)});
            seal_terminal(&mut file, &parent, &mut report).unwrap();
            let body = fs::read(&path).unwrap();
            let mut supervisor = SupervisorReceipt {
                run_id: "original-run".into(),
                config_sha256: "a".repeat(64),
                report_sha256: hash(&body),
                process_exit_code: 2,
                resources_closed: true,
            };
            assert!(admit_pass(&body, "original-run", &supervisor).is_err());
            supervisor.process_exit_code = 0;
            admit_pass(&body, "original-run", &supervisor).unwrap();
            supervisor.resources_closed = false;
            assert!(admit_pass(&body, "original-run", &supervisor).is_err());
            supervisor.resources_closed = true;
            assert!(admit_pass(&body, "different-run", &supervisor).is_err());
            supervisor.report_sha256 = "b".repeat(64);
            assert!(admit_pass(&body, "original-run", &supervisor).is_err());

            let original = dir.path().join("parent");
            fs::create_dir(&original).unwrap();
            let pinned = secure_open(
                &original,
                rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::DIRECTORY,
            )
            .unwrap();
            let identity = (
                pinned.metadata().unwrap().dev(),
                pinned.metadata().unwrap().ino(),
            );
            let moved = dir.path().join("moved-parent");
            fs::rename(&original, &moved).unwrap();
            fs::create_dir(&original).unwrap();
            let intended = original.join("result");
            let mut file = create_output(&pinned, &intended).unwrap();
            let mut report = json!({"schema":REPORT_SCHEMA,"status":"INCONCLUSIVE"});
            seal_terminal_with(
                &mut file,
                &pinned,
                &mut report,
                File::sync_all,
                |directory| {
                    assert_eq!(
                        (directory.metadata()?.dev(), directory.metadata()?.ino()),
                        identity
                    );
                    directory.sync_all()
                },
            )
            .unwrap();
            assert!(!intended.exists());
            assert_eq!(
                serde_json::from_slice::<Value>(&fs::read(moved.join("result")).unwrap()).unwrap(),
                report
            );
            assert!(create_output(&pinned, &intended).is_err());
        }
    }
}
