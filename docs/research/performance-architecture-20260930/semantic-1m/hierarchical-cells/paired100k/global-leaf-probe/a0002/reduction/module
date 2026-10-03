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
pub const BUILD_SCHEMA: &str = "borsuk-hierarchical-cells-build-v1";
const SCHEMA: &str = "borsuk-hierarchical-cells-resident-v3";
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
        require(
            self.bytes > 0 && self.bytes <= cap && valid_sha(&self.sha256),
            "artifact descriptor/cap",
        )?;
        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(rustix::fs::OFlags::NONBLOCK.bits() as i32)
            .open(&self.path)?;
        let metadata = file.metadata()?;
        require(
            metadata.is_file() && metadata.len() == self.bytes as u64,
            "artifact regular file/exact length",
        )?;
        let mut digest = Sha256::new();
        let mut remaining = self.bytes;
        let mut buffer = [0_u8; 65536];
        while remaining > 0 {
            let amount = remaining.min(buffer.len());
            file.read_exact(&mut buffer[..amount])?;
            digest.update(&buffer[..amount]);
            remaining -= amount;
        }
        require(
            file.read(&mut [0])? == 0 && format!("{:x}", digest.finalize()) == self.sha256,
            "artifact digest/length",
        )?;
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
    /// Source-only median splits when sampling could not give a balanced split.
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
        let mut left = Vec::new();
        let mut right = Vec::new();
        if !identical_sample {
            // Trainer failures propagate. No retraining/configuration sweep.
            let centers =
                train_logical_cell_centroids(&sample, VectorMetric::SquaredEuclidean, 2, 4)?;
            for &id in &ids {
                let vector = self.vector(id)?;
                if VectorMetric::SquaredEuclidean.distance(&vector, &centers[0])?
                    <= VectorMetric::SquaredEuclidean.distance(&vector, &centers[1])?
                {
                    left.push(id);
                } else {
                    right.push(id);
                }
            }
        }
        drop(sample);
        // Guarantee logarithmic depth even for highly skewed source geometry.
        // ponytail: balanced widest-coordinate median replaces pathological
        // sampled splits; a better source-only split needs measured evidence.
        if left.len().min(right.len()) < rows.div_ceil(4) {
            self.receipt.geometry_fallbacks += 1;
            let coordinate = (0..self.dimensions)
                .max_by(|&a, &b| {
                    (maximum[a] - minimum[a])
                        .total_cmp(&(maximum[b] - minimum[b]))
                        .then(b.cmp(&a))
                })
                .unwrap();
            let mut projected = Vec::with_capacity(rows);
            for &id in &ids {
                projected.push((self.vector(id)?[coordinate], id));
            }
            projected.sort_unstable_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
            left = projected[..rows / 2].iter().map(|v| v.1).collect();
            right = projected[rows / 2..].iter().map(|v| v.1).collect();
        }
        drop(ids);
        let children = vec![self.node(left, depth + 1)?, self.node(right, depth + 1)?];
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
    // IDs, samples/normalization/trainer clones, recursive prototypes, bounded
    // cell buffers and serializer scratch. Host overhead remains an external charge.
    let modeled = rows * 96
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
    Ok(receipt)
}

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
    /// Admit and authenticate the complete 100k research directory at startup.
    /// The cap is checked before opening/reading/allocating the directory body.
    /// Full-directory residency grows with source size; N>100k is unsupported.
    pub fn open(
        path: &Path,
        root_sha256: &str,
        max_resident_directory_payload_bytes: usize,
    ) -> Result<Self> {
        let bytes = usize::try_from(fs::metadata(path.join("manifest.json"))?.len())?;
        let body = Artifact {
            path: path.join("manifest.json"),
            bytes,
            sha256: root_sha256.into(),
        }
        .read(ROOT_CAP)?;
        let manifest: Manifest = serde_json::from_slice(&body)?;
        require(manifest.seed == NATIVE_CODEC_SEED, "current fixed SQ2 seed")?;
        require(
            manifest.schema == SCHEMA
                && manifest.input.schema == BUILD_SCHEMA
                && (1..=100_000).contains(&manifest.rows)
                && (1..=768).contains(&manifest.dimensions)
                && (1..=512).contains(&manifest.input.cell_rows)
                && (1..=32).contains(&manifest.input.max_depth)
                && manifest.mean.len() == manifest.dimensions
                && manifest.low.len() == manifest.dimensions
                && manifest.step.len() == manifest.dimensions
                && manifest.low.iter().all(|v| v.is_finite())
                && manifest.step.iter().all(|v| v.is_finite() && *v > 0.)
                && in_bounds(&manifest.root_directory, manifest.directory_bytes, PAGE_CAP)
                && valid_sha(&manifest.directory_sha256)
                && (1..=manifest.rows).contains(&manifest.build.cells)
                && (1..=manifest.rows).contains(&manifest.build.directories)
                && manifest.directory_bytes <= manifest.build.directories * PAGE_CAP
                && manifest
                    .directory_bytes
                    .checked_add(manifest.cell_bytes)
                    .and_then(|n| n.checked_add(body.len()))
                    .is_some_and(|n| n <= manifest.input.max_output_bytes),
            "research root schema/geometry/resource binding",
        )?;
        let admission = directory_admission(
            manifest.directory_bytes,
            manifest.build.directories,
            manifest.build.cells,
        )?;
        require(
            max_resident_directory_payload_bytes <= 512 * 1024 * 1024
                && admission.modeled_preload_peak_bytes <= max_resident_directory_payload_bytes,
            "resident directory payload admission",
        )?;
        let codec = RotatedTwoBitCodec::new(&manifest.mean, manifest.seed)?;
        let directories = regular_file(&path.join("directories.bin"), manifest.directory_bytes)?;
        let cells = regular_file(&path.join("cells.bin"), manifest.cell_bytes)?;
        let directory_body = read_at(&directories, 0, manifest.directory_bytes)?;
        require(
            hash(&directory_body) == manifest.directory_sha256,
            "whole directory SHA256",
        )?;
        let mut prototype = Self {
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
        assert_eq!(mean.len(), dimensions);
        let codec = RotatedTwoBitCodec::new(mean, 20260923).unwrap();
        let order = (0..rows).rev().collect::<Vec<_>>();
        let mut canonical = Vec::new();
        let mut codes = Vec::new();
        let mut sq8 = Vec::new();
        for &id in &order {
            let mut vector = vec![0_f32; dimensions];
            vector[if identical { 0 } else { id % 2 }] = 1_f32;
            canonical.extend_from_slice(&(id as i64).to_le_bytes());
            for value in &vector {
                canonical.extend_from_slice(&value.to_le_bytes());
            }
            codes.extend_from_slice(&codec.encode(&vector).unwrap());
            sq8.extend_from_slice(&(id as i64).to_le_bytes());
            sq8.extend_from_slice(&1_f32.to_le_bytes());
            sq8.extend(vector.iter().map(|&v| v as u8));
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
            "low":vec![0_f32;dimensions], "step":vec![1_f32;dimensions]
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
