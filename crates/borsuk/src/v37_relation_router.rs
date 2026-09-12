//! Balanced hyperplane layout and relation-routing qualification for V37.

use std::{
    collections::{BTreeMap, BTreeSet, BinaryHeap},
    fs::{self, File},
    io::{BufReader, Cursor, Read, Seek, SeekFrom, Write},
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
    time::Instant,
};

use arrow_array::{
    Array, FixedSizeListArray, Float32Array, ListArray, RecordBatch, UInt8Array, UInt16Array,
    UInt32Array, UInt64Array,
};
use arrow_buffer::OffsetBuffer;
use arrow_ipc::{
    MetadataVersion,
    reader::FileReader,
    writer::{FileWriter, IpcWriteOptions},
};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::Compression,
    file::properties::WriterProperties,
};
use rayon::{ThreadPool, ThreadPoolBuilder, prelude::*};

use crate::{BorsukError, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const V37_NODE_METADATA_BYTES: u64 = 32;
const V37_RELATION_RECORD_BYTES: u64 = 8;
const V37_MEMORY_LIMIT_BYTES: u64 = 3 * 1_073_741_824;
const V37_MAXIMUM_DIRECT_NODE_VISITS: u64 = 245;
const V37_MAXIMUM_RELATION_NODE_VISITS: u64 = 1_024;
const V37_SELECTED_POSTINGS: u64 = 14;
const V37_GT_NEIGHBORS: u32 = 100;
const V37_MAXIMUM_CEILING_QUERIES: u32 = 1_000;
const V37_AGGREGATE_RECALL_GATE_PPM: u32 = 998_000;
const V37_MINIMUM_RECALL_GATE_PPM: u32 = 800_000;
const V37_MAXIMUM_WORKERS: u64 = 32;
const V37_WORKER_STACK_BYTES: u64 = 2 * 1024 * 1024;
const V37_PREFLIGHT_ROWS: usize = 65_536;
const V37_PREFLIGHT_LEAVES: u64 = 16;
const V37_PREFLIGHT_COORDINATE_SCORES: u64 = 50_331_648;
const V37_PREFLIGHT_COORDINATE_SHA256: &str =
    "62ebfdbb42fa5e6212437932ebe682970389b15fa8cae1d6e2aa6d8610a41608";
static V37_FMA_KERNEL: OnceLock<Option<borsuk_fma::FusedDot8x12>> = OnceLock::new();

/// One strict local phase of the claim-ineligible V37 diagnostic.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum V37LocalRunMode {
    /// Measure the exact production scorer on a deterministic reduced shape.
    PreflightTraining,
    /// Authenticate and build the query-blind ownership tree and table.
    BuildOwnership,
    /// Authenticate the frozen development evidence and compute the exact layout ceiling.
    EvaluateCeiling,
}

impl V37LocalRunMode {
    fn input_roles(self) -> &'static [&'static str] {
        const BUILD: &[&str] = &[
            "v36-authority",
            "v36-execution-authority",
            "v36-receipt",
            "v36-source-registry",
            "v37-authority",
            "source",
        ];
        const CEILING: &[&str] = &[
            "ceiling-authority",
            "development-ground-truth",
            "ownership-tree",
            "ownership",
        ];
        match self {
            Self::PreflightTraining => &["v37-authority"],
            Self::BuildOwnership => BUILD,
            Self::EvaluateCeiling => CEILING,
        }
    }

    fn output_roles(self) -> &'static [&'static str] {
        match self {
            Self::PreflightTraining => &[],
            Self::BuildOwnership => &["ownership-tree", "ownership"],
            Self::EvaluateCeiling => &["ceiling"],
        }
    }
}

/// One authenticated local input; the URI is evidence identity, never a network capability.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V37LocalArtifact {
    role: String,
    path: PathBuf,
    uri: String,
    sha256: String,
    blake3: String,
    encoded_bytes: u64,
}

impl V37LocalArtifact {
    /// Construct one strict local input identity.
    pub fn try_new(
        role: String,
        path: PathBuf,
        uri: String,
        sha256: String,
        blake3: String,
        encoded_bytes: u64,
    ) -> Result<Self> {
        let artifact = Self {
            role,
            path,
            uri,
            sha256,
            blake3,
            encoded_bytes,
        };
        if artifact.role.is_empty()
            || artifact.path.as_os_str().is_empty()
            || artifact.encoded_bytes == 0
            || !valid_s3_object_uri(&artifact.uri)
            || !valid_lower_hex_digest(&artifact.sha256)
            || !valid_lower_hex_digest(&artifact.blake3)
        {
            return Err(invalid("V37 local artifact identity differs"));
        }
        Ok(artifact)
    }

    /// Return the phase-local role.
    pub fn role(&self) -> &str {
        &self.role
    }

    /// Return the local file path without opening it.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// One explicit local output path, with no implicit storage destination.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V37LocalOutput {
    role: String,
    path: PathBuf,
}

impl V37LocalOutput {
    /// Construct an explicit output role and path.
    pub fn try_new(role: String, path: PathBuf) -> Result<Self> {
        if role.is_empty() || path.as_os_str().is_empty() {
            return Err(invalid("V37 local output differs"));
        }
        Ok(Self { role, path })
    }

    /// Return the phase-local output role.
    pub fn role(&self) -> &str {
        &self.role
    }

    /// Return the explicit local output path.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Capability-separated local request for one V37 diagnostic phase.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V37LocalRunRequest {
    mode: V37LocalRunMode,
    inputs: Vec<V37LocalArtifact>,
    outputs: Vec<V37LocalOutput>,
    workers: u32,
}

impl V37LocalRunRequest {
    /// Validate exact input/output roles and reject overlapping local paths.
    pub fn try_new(
        mode: V37LocalRunMode,
        inputs: Vec<V37LocalArtifact>,
        outputs: Vec<V37LocalOutput>,
        workers: u32,
    ) -> Result<Self> {
        let input_roles = inputs
            .iter()
            .map(V37LocalArtifact::role)
            .collect::<Vec<_>>();
        let output_roles = outputs.iter().map(V37LocalOutput::role).collect::<Vec<_>>();
        let mut paths = BTreeSet::new();
        let mut uris = BTreeSet::new();
        if input_roles != mode.input_roles()
            || output_roles != mode.output_roles()
            || !matches!(workers, 1 | 2 | 4 | 8 | 16 | 32)
            || inputs.iter().any(|input| !paths.insert(input.path()))
            || outputs.iter().any(|output| !paths.insert(output.path()))
            || inputs.iter().any(|input| !uris.insert(input.uri.as_str()))
        {
            return Err(invalid("V37 local phase capability differs"));
        }
        Ok(Self {
            mode,
            inputs,
            outputs,
            workers,
        })
    }

    /// Return the exact phase.
    pub const fn mode(&self) -> V37LocalRunMode {
        self.mode
    }

    /// Return input roles in their canonical phase order.
    pub fn input_roles(&self) -> Vec<&str> {
        self.inputs.iter().map(V37LocalArtifact::role).collect()
    }

    /// Return output roles in their canonical phase order.
    pub fn output_roles(&self) -> Vec<&str> {
        self.outputs.iter().map(V37LocalOutput::role).collect()
    }

    /// Return the registered worker count.
    pub const fn workers(&self) -> u32 {
        self.workers
    }
}

#[derive(Debug)]
struct V37AuthenticatedInputStamp {
    file: File,
    device: u64,
    inode: u64,
    length: u64,
    modified_seconds: i64,
    modified_nanoseconds: i64,
    changed_seconds: i64,
    changed_nanoseconds: i64,
}

#[derive(Debug)]
struct V37AuthenticatedLocalInputs(Vec<V37AuthenticatedInputStamp>);

fn authenticate_v37_local_request(
    request: &V37LocalRunRequest,
) -> Result<V37AuthenticatedLocalInputs> {
    const HASH_BUFFER_BYTES: usize = 1_048_576;

    let mut input_paths = BTreeSet::new();
    let mut input_files = BTreeSet::new();
    let mut stamps = Vec::with_capacity(request.inputs.len());
    for input in &request.inputs {
        let path_metadata =
            fs::symlink_metadata(&input.path).map_err(|source| BorsukError::Io {
                path: input.path.clone(),
                source,
            })?;
        if path_metadata.file_type().is_symlink() || !path_metadata.file_type().is_file() {
            return Err(invalid("V37 local input type differs"));
        }
        let file = File::open(&input.path).map_err(|source| BorsukError::Io {
            path: input.path.clone(),
            source,
        })?;
        let metadata = file.metadata().map_err(|source| BorsukError::Io {
            path: input.path.clone(),
            source,
        })?;
        if !metadata.file_type().is_file()
            || metadata.len() != input.encoded_bytes
            || metadata.dev() != path_metadata.dev()
            || metadata.ino() != path_metadata.ino()
            || !input_files.insert((metadata.dev(), metadata.ino()))
        {
            return Err(invalid("V37 local input identity differs"));
        }
        let canonical = fs::canonicalize(&input.path).map_err(|source| BorsukError::Io {
            path: input.path.clone(),
            source,
        })?;
        if !input_paths.insert(canonical) {
            return Err(invalid("V37 local input paths overlap"));
        }

        let mut reader = BufReader::with_capacity(HASH_BUFFER_BYTES, file);
        let mut sha256 = Sha256::new();
        let mut blake3 = blake3::Hasher::new();
        let mut observed_bytes = 0_u64;
        let mut buffer = vec![0_u8; HASH_BUFFER_BYTES];
        loop {
            let read = reader.read(&mut buffer).map_err(|source| BorsukError::Io {
                path: input.path.clone(),
                source,
            })?;
            if read == 0 {
                break;
            }
            observed_bytes = observed_bytes
                .checked_add(read as u64)
                .ok_or_else(|| invalid("V37 local input length overflows"))?;
            sha256.update(&buffer[..read]);
            blake3.update(&buffer[..read]);
        }
        if observed_bytes != input.encoded_bytes
            || format!("{:x}", sha256.finalize()) != input.sha256
            || blake3.finalize().to_hex().as_str() != input.blake3
        {
            return Err(invalid("V37 local input bytes differ"));
        }
        let file = reader.into_inner();
        let after = fs::symlink_metadata(&input.path).map_err(|source| BorsukError::Io {
            path: input.path.clone(),
            source,
        })?;
        if after.file_type().is_symlink()
            || after.dev() != metadata.dev()
            || after.ino() != metadata.ino()
            || after.len() != metadata.len()
            || after.mtime() != metadata.mtime()
            || after.mtime_nsec() != metadata.mtime_nsec()
            || after.ctime() != metadata.ctime()
            || after.ctime_nsec() != metadata.ctime_nsec()
        {
            return Err(invalid("V37 local input changed during authentication"));
        }
        stamps.push(V37AuthenticatedInputStamp {
            file,
            device: after.dev(),
            inode: after.ino(),
            length: after.len(),
            modified_seconds: after.mtime(),
            modified_nanoseconds: after.mtime_nsec(),
            changed_seconds: after.ctime(),
            changed_nanoseconds: after.ctime_nsec(),
        });
    }

    let mut output_paths = BTreeSet::new();
    for output in &request.outputs {
        match fs::symlink_metadata(&output.path) {
            Ok(_) => return Err(invalid("V37 local output already exists")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(BorsukError::Io {
                    path: output.path.clone(),
                    source,
                });
            }
        }
        let parent = output
            .path
            .parent()
            .ok_or_else(|| invalid("V37 local output parent differs"))?;
        let parent_metadata = fs::symlink_metadata(parent).map_err(|source| BorsukError::Io {
            path: parent.to_owned(),
            source,
        })?;
        if parent_metadata.file_type().is_symlink() || !parent_metadata.file_type().is_dir() {
            return Err(invalid("V37 local output parent differs"));
        }
        let canonical_parent = fs::canonicalize(parent).map_err(|source| BorsukError::Io {
            path: parent.to_owned(),
            source,
        })?;
        let file_name = output
            .path
            .file_name()
            .ok_or_else(|| invalid("V37 local output name differs"))?;
        let canonical = canonical_parent.join(file_name);
        if input_paths.contains(&canonical) || !output_paths.insert(canonical) {
            return Err(invalid("V37 local input/output paths overlap"));
        }
    }
    Ok(V37AuthenticatedLocalInputs(stamps))
}

fn authenticated_v37_input_file(
    request: &V37LocalRunRequest,
    authenticated: &V37AuthenticatedLocalInputs,
    role: &str,
) -> Result<File> {
    let index = request
        .inputs
        .iter()
        .position(|input| input.role == role)
        .ok_or_else(|| invalid("V37 authenticated input role is absent"))?;
    let mut file = authenticated
        .0
        .get(index)
        .ok_or_else(|| invalid("V37 authenticated input count differs"))?
        .file
        .try_clone()
        .map_err(|source| BorsukError::Io {
            path: request.inputs[index].path.clone(),
            source,
        })?;
    file.seek(SeekFrom::Start(0))
        .map_err(|source| BorsukError::Io {
            path: request.inputs[index].path.clone(),
            source,
        })?;
    Ok(file)
}

fn read_v37_authenticated_input(
    request: &V37LocalRunRequest,
    authenticated: &V37AuthenticatedLocalInputs,
    role: &str,
    maximum_bytes: u64,
) -> Result<Vec<u8>> {
    let input = local_input(request, role)?;
    if maximum_bytes == 0 || input.encoded_bytes > maximum_bytes {
        return Err(invalid("V37 authenticated input length differs"));
    }
    let mut file = authenticated_v37_input_file(request, authenticated, role)?;
    file.seek(SeekFrom::Start(0))
        .map_err(|source| BorsukError::Io {
            path: input.path.clone(),
            source,
        })?;
    let capacity = usize::try_from(input.encoded_bytes)
        .map_err(|_| invalid("V37 authenticated input exceeds address space"))?;
    let mut bytes = Vec::with_capacity(capacity);
    file.read_to_end(&mut bytes)
        .map_err(|source| BorsukError::Io {
            path: input.path.clone(),
            source,
        })?;
    if bytes.len() as u64 != input.encoded_bytes {
        return Err(invalid("V37 authenticated input length differs"));
    }
    Ok(bytes)
}

fn validate_v37_local_input_stability(
    request: &V37LocalRunRequest,
    authenticated: &V37AuthenticatedLocalInputs,
) -> Result<()> {
    if request.inputs.len() != authenticated.0.len() {
        return Err(invalid("V37 authenticated input count differs"));
    }
    for (input, stamp) in request.inputs.iter().zip(&authenticated.0) {
        let metadata = fs::symlink_metadata(&input.path).map_err(|source| BorsukError::Io {
            path: input.path.clone(),
            source,
        })?;
        if metadata.file_type().is_symlink()
            || !metadata.file_type().is_file()
            || metadata.dev() != stamp.device
            || metadata.ino() != stamp.inode
            || metadata.len() != stamp.length
            || metadata.mtime() != stamp.modified_seconds
            || metadata.mtime_nsec() != stamp.modified_nanoseconds
            || metadata.ctime() != stamp.changed_seconds
            || metadata.ctime_nsec() != stamp.changed_nanoseconds
        {
            return Err(invalid("V37 local input changed after authentication"));
        }
    }
    Ok(())
}

/// Query-independent authority for one balanced hyperplane tree.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V37TreeSpec {
    corpus_rows: u64,
    dimensions: u64,
    seed: u64,
    target_primary_rows: u64,
    training_sample_rows: u64,
    two_means_iterations: u64,
}

/// Query-independent authority for one leaf-to-posting relation plane.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V37RelationSpec {
    leaf_count: u64,
    maximum_leaf_probes: u64,
    prefix_lengths: Vec<u64>,
    q_bits: u32,
    seed: u64,
}

/// Exact quota projection for a single-owner balanced tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V37LayoutProjection {
    pub(crate) leaf_count: u64,
    pub(crate) internal_node_count: u64,
    pub(crate) minimum_leaf_rows: u64,
    pub(crate) maximum_leaf_rows: u64,
    pub(crate) total_rows: u64,
}

/// Resident byte projection for V37's two trees and largest relation prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V37ServingProjection {
    pub(crate) ownership_tree_bytes: u64,
    pub(crate) relation_tree_bytes: u64,
    pub(crate) relation_prefix_bytes: u64,
    pub(crate) total_bytes: u64,
}

/// Exact split sizes for one recursive quota-balanced node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V37ChildQuota {
    pub(crate) left_rows: u64,
    pub(crate) right_rows: u64,
    pub(crate) left_leaves: u64,
    pub(crate) right_leaves: u64,
}

/// Admission result for the resident one-million-row construction path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum V37LayoutDisposition {
    Admissible,
    ResourceRejected,
}

/// Checked resident-construction memory projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V37ConstructionProjection {
    pub(crate) source_decode_working_bytes: u64,
    pub(crate) resident_projected_bytes: u64,
    pub(crate) feature_id_bytes: u64,
    pub(crate) ordinal_index_bytes: u64,
    pub(crate) member_index_bytes: u64,
    pub(crate) score_tuple_bytes: u64,
    pub(crate) assignment_bytes: u64,
    pub(crate) reservoir_bytes: u64,
    pub(crate) tree_bytes: u64,
    pub(crate) ownership_writer_bytes: u64,
    pub(crate) worker_stack_bytes: u64,
    pub(crate) subtotal_bytes: u64,
    pub(crate) allocator_headroom_bytes: u64,
    pub(crate) total_bytes: u64,
    pub(crate) disposition: V37LayoutDisposition,
}

/// Maximum logical query work for the registered V37 ladder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V37WorkProjection {
    pub(crate) maximum_direct_node_visits: u64,
    pub(crate) maximum_relation_node_visits: u64,
    pub(crate) maximum_relation_records: u64,
    pub(crate) selected_postings: u64,
}

/// Reduced-shape tree-training parameters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V37TrainingShape {
    pub(crate) dimensions: usize,
    pub(crate) leaf_count: u64,
    pub(crate) reservoir_rows: usize,
    pub(crate) two_means_iterations: usize,
}

/// One authenticated projected row used by the in-memory preflight trainer.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct V37TrainingRow {
    pub(crate) source_ordinal: u64,
    pub(crate) vector: Vec<f32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V37TrainingEvidence {
    dimensions: u64,
    fma_backend: String,
    leaf_count: u64,
    partition_coordinate_scores: u64,
    partition_coordinate_scores_per_second: u64,
    partition_scoring_elapsed_ns: u64,
    rows: u64,
    training_elapsed_ns: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct V37PreflightEvidence {
    coordinate_generator: String,
    coordinate_sha256: String,
    projected_construction_bytes: u64,
    scalar_fused_comparisons: u64,
    scalar_fused_max_ulp_delta: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V37TrainingProgress {
    completed_internal_nodes: u64,
    partition_coordinate_scores: u64,
    sequence: u64,
    total_internal_nodes: u64,
}

trait V37TrainingDataset: Sync {
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn source_ordinal(&self, index: usize) -> u64;
    fn vector(&self, index: usize) -> &[f32];
}

struct V37OwnedTrainingDataset<'a>(&'a [V37TrainingRow]);

impl V37TrainingDataset for V37OwnedTrainingDataset<'_> {
    fn len(&self) -> usize {
        self.0.len()
    }

    fn source_ordinal(&self, index: usize) -> u64 {
        self.0[index].source_ordinal
    }

    fn vector(&self, index: usize) -> &[f32] {
        &self.0[index].vector
    }
}

struct V37ResidentTrainingDataset<'a> {
    coordinates: &'a [f32],
    dimensions: usize,
}

impl V37TrainingDataset for V37ResidentTrainingDataset<'_> {
    fn len(&self) -> usize {
        self.coordinates.len() / self.dimensions
    }

    fn source_ordinal(&self, index: usize) -> u64 {
        index as u64
    }

    fn vector(&self, index: usize) -> &[f32] {
        let start = index * self.dimensions;
        &self.coordinates[start..start + self.dimensions]
    }
}

/// One deterministic source-to-posting assignment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V37OwnershipAssignment {
    pub(crate) source_ordinal: u64,
    pub(crate) posting_ordinal: u32,
}

/// One format-v2 ownership row bridging compact source ordinals to dataset IDs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V37OwnershipRecord {
    pub(crate) source_ordinal: u64,
    pub(crate) feature_row_id: u64,
    pub(crate) posting_ordinal: u32,
    pub(crate) posting_local_ordinal: u32,
}

/// One preorder node. Leaves carry a posting; internal nodes carry a plane.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct V37BalancedNode {
    pub(crate) normal: Vec<f32>,
    pub(crate) boundary_score_bits: u32,
    pub(crate) boundary_source_ordinal: u64,
    pub(crate) left_node: Option<u32>,
    pub(crate) right_node: Option<u32>,
    pub(crate) posting_ordinal: Option<u32>,
    pub(crate) population: u64,
}

/// Deterministic reduced-shape ownership tree and its single-owner table.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct V37BalancedTree {
    pub(crate) dimensions: usize,
    pub(crate) seed: u64,
    pub(crate) fma_backend: String,
    pub(crate) nodes: Vec<V37BalancedNode>,
    pub(crate) leaf_populations: Vec<u64>,
    pub(crate) assignments: Vec<V37OwnershipAssignment>,
}

/// Authenticated cross-language Arrow IPC bytes for one V37 tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V37EncodedTree {
    pub(crate) bytes: Vec<u8>,
    pub(crate) encoded_bytes: u64,
    pub(crate) sha256: String,
    pub(crate) blake3: String,
}

/// Authenticated cross-language Parquet bytes for source ownership.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V37EncodedOwnership {
    pub(crate) bytes: Vec<u8>,
    pub(crate) encoded_bytes: u64,
    pub(crate) sha256: String,
    pub(crate) blake3: String,
}

/// Query-side equality semantics at one hyperplane boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V37QueryBranch {
    pub(crate) primary_is_left: bool,
    pub(crate) queues_sibling_zero_margin: bool,
}

/// Deterministic GT-free best-bin-first selection over one ownership tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V37DirectSelection {
    pub(crate) selected_postings: Vec<u32>,
    pub(crate) node_visits: u32,
    pub(crate) scored_internal_nodes: u32,
    pub(crate) fma_backend: String,
}

/// A geometry- and backend-validated tree ready for bounded query traversal.
pub(crate) struct V37DirectRouter<'a> {
    tree: &'a V37BalancedTree,
    kernel: borsuk_fma::FusedDot8x12,
    maximum_node_visits: usize,
}

/// A validated independent relation tree ready for bounded leaf probing.
pub(crate) struct V37RelationRouter<'a> {
    tree: &'a V37BalancedTree,
    kernel: borsuk_fma::FusedDot8x12,
    maximum_node_visits: usize,
}

/// GT-free selection evidence associated with one query ordinal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V37DirectSelectionRecord {
    pub(crate) query_ordinal: u32,
    pub(crate) selection: V37DirectSelection,
}

/// Separately recomputed direct-routing quality for one query.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V37DirectSample {
    pub(crate) query_ordinal: u32,
    pub(crate) selected_postings: Vec<u32>,
    pub(crate) node_visits: u32,
    pub(crate) scored_internal_nodes: u32,
    pub(crate) hits: u32,
    pub(crate) recall_ppm: u32,
    pub(crate) ceiling_hits: u32,
    pub(crate) ceiling_gap_hits: u32,
}

/// Claim-ineligible quality result for direct hyperplane-tree routing.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V37DirectRoutingResult {
    schema: String,
    claim_eligible: bool,
    selected_postings_limit: u32,
    aggregate_gate_ppm: u32,
    minimum_gate_ppm: u32,
    fma_backend: String,
    pub(crate) samples: Vec<V37DirectSample>,
    pub(crate) aggregate_recall_ppm: u32,
    pub(crate) minimum_recall_ppm: u32,
    pub(crate) passed: bool,
    pub(crate) disposition: String,
}

/// One full, untruncated relation count with its exact Q24 mass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V37RelationRecord {
    pub(crate) posting_ordinal: u32,
    pub(crate) count: u64,
    pub(crate) mass_q24: u32,
}

/// Full relation counts for one independently trained routing leaf.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V37RelationLeaf {
    pub(crate) population: u64,
    pub(crate) records: Vec<V37RelationRecord>,
}

/// Query-blind leaf-to-ownership relation plane.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V37RelationPlane {
    posting_count: u32,
    prefix_lengths: Vec<u32>,
    pub(crate) leaves: Vec<V37RelationLeaf>,
}

/// Deterministic posting selection from ranked relation leaves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V37RelationSelection {
    pub(crate) selected_postings: Vec<u32>,
    pub(crate) touched_records: u32,
}

/// Relation-tree traversal evidence plus the resulting posting votes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V37RoutedRelationSelection {
    pub(crate) routing: V37DirectSelection,
    pub(crate) votes: V37RelationSelection,
}

/// Content-addressed cross-language relation artifact bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V37EncodedRelationArtifact {
    pub(crate) bytes: Vec<u8>,
    pub(crate) encoded_bytes: u64,
    pub(crate) sha256: String,
    pub(crate) blake3: String,
}

/// Structure-of-arrays serving prefixes for each registered prefix length.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V37RelationPrefixes {
    pub(crate) posting_count: u32,
    pub(crate) prefix_lengths: Vec<u32>,
    pub(crate) leaf_offsets: Vec<Vec<u64>>,
    pub(crate) posting_ordinals: Vec<Vec<u32>>,
    pub(crate) masses_q24: Vec<Vec<u32>>,
}

/// Immutable serving prefixes validated once before any query work.
pub(crate) struct V37ValidatedRelationPrefixes<'a> {
    prefixes: &'a V37RelationPrefixes,
}

/// Exact GT row identities for one separately authorized ceiling query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V37GroundTruth {
    pub(crate) query_ordinal: u32,
    pub(crate) source_ordinals: Vec<u64>,
}

/// Exact dataset IDs from the separately authenticated V36 GT@100 artifact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V37FeatureGroundTruth {
    pub(crate) query_ordinal: u32,
    pub(crate) feature_row_ids: Vec<u64>,
}

pub(crate) fn map_v37_feature_ground_truth(
    ownership: &[V37OwnershipRecord],
    truth: &[V37FeatureGroundTruth],
) -> Result<Vec<V37GroundTruth>> {
    if ownership.is_empty() || truth.is_empty() {
        return Err(invalid("V37 ceiling feature authority is empty"));
    }
    let mut by_feature = BTreeMap::new();
    let mut posting_locals = BTreeMap::<u32, u32>::new();
    for (source_ordinal, row) in ownership.iter().enumerate() {
        let expected_local = posting_locals.entry(row.posting_ordinal).or_default();
        if row.source_ordinal != source_ordinal as u64
            || row.posting_local_ordinal != *expected_local
            || by_feature
                .insert(row.feature_row_id, row.source_ordinal)
                .is_some()
        {
            return Err(invalid("V37 ceiling ownership identity differs"));
        }
        *expected_local = expected_local
            .checked_add(1)
            .ok_or_else(|| invalid("V37 ceiling ownership local ordinal overflows"))?;
    }

    let mut mapped = Vec::with_capacity(truth.len());
    let mut previous_query = None;
    for query in truth {
        if previous_query.is_some_and(|prior| query.query_ordinal <= prior)
            || query.feature_row_ids.len() != V37_GT_NEIGHBORS as usize
        {
            return Err(invalid("V37 ceiling feature truth shape differs"));
        }
        previous_query = Some(query.query_ordinal);
        let mut seen = BTreeSet::new();
        let source_ordinals = query
            .feature_row_ids
            .iter()
            .map(|feature_row_id| {
                if !seen.insert(*feature_row_id) {
                    return Err(invalid("V37 ceiling feature truth row is duplicated"));
                }
                by_feature
                    .get(feature_row_id)
                    .copied()
                    .ok_or_else(|| invalid("V37 ceiling feature truth row is unknown"))
            })
            .collect::<Result<Vec<_>>>()?;
        mapped.push(V37GroundTruth {
            query_ordinal: query.query_ordinal,
            source_ordinals,
        });
    }
    Ok(mapped)
}

fn load_v37_feature_ground_truth_file(
    file: File,
    display_path: &Path,
    expected_queries: u32,
) -> Result<Vec<V37FeatureGroundTruth>> {
    let mut truth = (0..expected_queries)
        .map(|query_ordinal| V37FeatureGroundTruth {
            query_ordinal,
            feature_row_ids: Vec::with_capacity(V37_GT_NEIGHBORS as usize),
        })
        .collect::<Vec<_>>();
    crate::v36_prefix_dataset::scan_v36_prefix_gt100_parquet_file(
        file,
        display_path,
        expected_queries,
        |batch| {
            let queries = column::<UInt32Array>(&batch, 0, "V37 GT query differs")?;
            let ranks = column::<UInt16Array>(&batch, 1, "V37 GT rank differs")?;
            let feature_ids = column::<UInt64Array>(&batch, 2, "V37 GT feature ID differs")?;
            for row in 0..batch.num_rows() {
                let query = usize::try_from(queries.value(row))
                    .map_err(|_| invalid("V37 GT query differs"))?;
                let target = truth
                    .get_mut(query)
                    .ok_or_else(|| invalid("V37 GT query differs"))?;
                if ranks.value(row) as usize != target.feature_row_ids.len() {
                    return Err(invalid("V37 GT rank differs"));
                }
                target.feature_row_ids.push(feature_ids.value(row));
            }
            Ok(())
        },
    )?;
    if truth
        .iter()
        .any(|query| query.feature_row_ids.len() != V37_GT_NEIGHBORS as usize)
    {
        return Err(invalid("V37 GT row count differs"));
    }
    Ok(truth)
}

/// Independently recomputable unique-owner ceiling evidence for one query.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V37CeilingSample {
    pub(crate) query_ordinal: u32,
    pub(crate) selected_postings: Vec<u32>,
    pub(crate) hits: u32,
    pub(crate) recall_ppm: u32,
}

/// Claim-ineligible exact K14 layout-ceiling result.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V37LayoutCeiling {
    schema: String,
    claim_eligible: bool,
    gt_neighbors: u32,
    selected_postings_limit: u32,
    aggregate_gate_ppm: u32,
    minimum_gate_ppm: u32,
    pub(crate) samples: Vec<V37CeilingSample>,
    pub(crate) aggregate_recall_ppm: u32,
    pub(crate) minimum_recall_ppm: u32,
    pub(crate) passed: bool,
    pub(crate) disposition: String,
}

/// Backend-bound numeric authority persisted with every V37 manifest.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V37NumericAuthority {
    fma_backend: String,
    lane_width: u32,
    preflight_coordinate_scores: u64,
    preflight_coordinate_sha256: String,
    worker_count: u32,
}

/// Exact immutable identity for one V37 authority input.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V37ArtifactIdentity {
    blake3: String,
    encoded_bytes: u64,
    role: String,
    sha256: String,
    uri: String,
}

/// Versioned semantic authority for replaying the frozen V36 SRHT projection.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V37ProjectionAuthority {
    algorithm: String,
    projected_corpus_sha256: String,
    routing_dimensions: u64,
    seed: u64,
    source_dimensions: u64,
}

/// Canonical query-independent authority for one V37 construction.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V37AuthorityManifest {
    algorithm: String,
    metric: String,
    numeric: V37NumericAuthority,
    projection: V37ProjectionAuthority,
    relation: V37RelationSpec,
    schema: String,
    source: V37ArtifactIdentity,
    tree: V37TreeSpec,
}

/// Capability-minimal authority for the separate GT-only exact-K14 ceiling.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V37CeilingAuthority {
    construction_authority: V37ArtifactIdentity,
    development_ground_truth: V37ArtifactIdentity,
    gt_neighbors: u32,
    ownership: V37ArtifactIdentity,
    ownership_tree: V37ArtifactIdentity,
    query_count: u32,
    schema: String,
    selected_postings: u32,
}

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

fn valid_lower_hex_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn valid_s3_object_uri(value: &str) -> bool {
    value
        .strip_prefix("s3://")
        .and_then(|object| object.split_once('/'))
        .is_some_and(|(bucket, key)| !bucket.is_empty() && !key.is_empty())
}

fn validate_v37_artifact(identity: &V37ArtifactIdentity, role: &str) -> Result<()> {
    if identity.role != role
        || identity.encoded_bytes == 0
        || !valid_lower_hex_digest(&identity.sha256)
        || !valid_lower_hex_digest(&identity.blake3)
        || !valid_s3_object_uri(&identity.uri)
    {
        return Err(invalid("V37 artifact authority differs"));
    }
    Ok(())
}

fn validate_v37_authority(manifest: &V37AuthorityManifest) -> Result<()> {
    if manifest.schema != "borsuk-v37-relation-authority-v3"
        || manifest.algorithm != "balanced-hyperplane-relation-v1"
        || manifest.metric != "squared-l2"
        || !matches!(
            manifest.numeric.fma_backend.as_str(),
            "aarch64-neon-fma" | "x86-avx-fma"
        )
        || manifest.numeric.lane_width != 8
        || manifest.numeric.preflight_coordinate_scores != V37_PREFLIGHT_COORDINATE_SCORES
        || manifest.numeric.preflight_coordinate_sha256 != V37_PREFLIGHT_COORDINATE_SHA256
        || manifest.numeric.worker_count == 0
        || manifest.projection.algorithm != "v36-srht-f32-v1"
        || manifest.projection.source_dimensions != 768
        || manifest.projection.routing_dimensions != manifest.tree.dimensions
        || manifest.projection.seed != 36
        || !valid_lower_hex_digest(&manifest.projection.projected_corpus_sha256)
    {
        return Err(invalid("V37 manifest authority differs"));
    }
    validate_v37_specs(&manifest.tree, &manifest.relation)?;
    validate_v37_artifact(&manifest.source, "source-corpus")?;
    Ok(())
}

fn canonical_json_value(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(object) => {
            let mut entries: Vec<_> = object.into_iter().collect();
            entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));
            serde_json::Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key, canonical_json_value(value)))
                    .collect(),
            )
        }
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(canonical_json_value).collect())
        }
        value => value,
    }
}

pub(crate) fn canonical_v37_authority_bytes(manifest: &V37AuthorityManifest) -> Result<Vec<u8>> {
    validate_v37_authority(manifest)?;
    let value = serde_json::to_value(manifest)
        .map_err(|error| invalid(&format!("V37 manifest serialization failed: {error}")))?;
    let mut bytes = serde_json::to_vec(&canonical_json_value(value))
        .map_err(|error| invalid(&format!("V37 manifest serialization failed: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

pub(crate) fn parse_v37_authority_bytes(bytes: &[u8]) -> Result<V37AuthorityManifest> {
    let manifest: V37AuthorityManifest = serde_json::from_slice(bytes)
        .map_err(|error| invalid(&format!("V37 manifest parsing failed: {error}")))?;
    if canonical_v37_authority_bytes(&manifest)? != bytes {
        return Err(invalid("V37 manifest bytes are not canonical"));
    }
    Ok(manifest)
}

fn validate_v37_ceiling_authority(authority: &V37CeilingAuthority) -> Result<()> {
    if authority.schema != "borsuk-v37-ceiling-authority-v1"
        || authority.query_count == 0
        || authority.query_count > V37_MAXIMUM_CEILING_QUERIES
        || authority.gt_neighbors != V37_GT_NEIGHBORS
        || authority.selected_postings != V37_SELECTED_POSTINGS as u32
    {
        return Err(invalid("V37 ceiling authority differs"));
    }
    for (identity, role) in [
        (
            &authority.construction_authority,
            "v37-construction-authority",
        ),
        (
            &authority.development_ground_truth,
            "development-ground-truth",
        ),
        (&authority.ownership, "ownership"),
        (&authority.ownership_tree, "ownership-tree"),
    ] {
        validate_v37_artifact(identity, role)?;
    }
    let uris = [
        authority.construction_authority.uri.as_str(),
        authority.development_ground_truth.uri.as_str(),
        authority.ownership.uri.as_str(),
        authority.ownership_tree.uri.as_str(),
    ];
    if uris.into_iter().collect::<BTreeSet<_>>().len() != uris.len() {
        return Err(invalid("V37 ceiling artifact roles overlap"));
    }
    Ok(())
}

pub(crate) fn canonical_v37_ceiling_authority_bytes(
    authority: &V37CeilingAuthority,
) -> Result<Vec<u8>> {
    validate_v37_ceiling_authority(authority)?;
    let value = serde_json::to_value(authority).map_err(|error| {
        invalid(&format!(
            "V37 ceiling authority serialization failed: {error}"
        ))
    })?;
    let mut bytes = serde_json::to_vec(&canonical_json_value(value)).map_err(|error| {
        invalid(&format!(
            "V37 ceiling authority serialization failed: {error}"
        ))
    })?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn parse_v37_ceiling_authority_bytes(bytes: &[u8]) -> Result<V37CeilingAuthority> {
    let authority: V37CeilingAuthority = serde_json::from_slice(bytes)
        .map_err(|error| invalid(&format!("V37 ceiling authority parsing failed: {error}")))?;
    if canonical_v37_ceiling_authority_bytes(&authority)? != bytes {
        return Err(invalid("V37 ceiling authority bytes are not canonical"));
    }
    Ok(authority)
}

pub(crate) fn validate_v37_specs(tree: &V37TreeSpec, relation: &V37RelationSpec) -> Result<()> {
    if tree.corpus_rows == 0
        || tree.dimensions == 0
        || tree.target_primary_rows != 8_192
        || tree.training_sample_rows != 4_096
        || tree.two_means_iterations != 8
        || tree.seed == 0
    {
        return Err(invalid("V37 tree authority differs"));
    }
    if relation.leaf_count != 4_096
        || relation.maximum_leaf_probes != 32
        || relation.prefix_lengths != [16, 32, 64]
        || relation.q_bits != 24
        || relation.seed == 0
        || relation.seed == tree.seed
    {
        return Err(invalid("V37 relation authority differs"));
    }
    Ok(())
}

pub(crate) fn project_v37_child_quota(rows: u64, leaves: u64) -> Result<V37ChildQuota> {
    if leaves <= 1 || rows < leaves {
        return Err(invalid("V37 recursive quota authority differs"));
    }
    let left_leaves = leaves / 2;
    let right_leaves = leaves
        .checked_sub(left_leaves)
        .ok_or_else(|| invalid("V37 recursive quota underflows"))?;
    let left_rows = rows
        .checked_mul(left_leaves)
        .ok_or_else(|| invalid("V37 recursive quota overflows"))?
        / leaves;
    let right_rows = rows
        .checked_sub(left_rows)
        .ok_or_else(|| invalid("V37 recursive quota underflows"))?;
    if left_rows < left_leaves || right_rows < right_leaves {
        return Err(invalid("V37 recursive quota cannot populate leaves"));
    }
    Ok(V37ChildQuota {
        left_rows,
        right_rows,
        left_leaves,
        right_leaves,
    })
}

pub(crate) fn project_v37_layout(tree: &V37TreeSpec) -> Result<V37LayoutProjection> {
    if tree.corpus_rows == 0
        || tree.dimensions == 0
        || tree.seed == 0
        || tree.target_primary_rows != 8_192
        || tree.training_sample_rows != 4_096
        || tree.two_means_iterations != 8
    {
        return Err(invalid("V37 layout authority differs"));
    }
    let leaf_count = tree.corpus_rows.div_ceil(tree.target_primary_rows);
    let internal_node_count = leaf_count
        .checked_sub(1)
        .ok_or_else(|| invalid("V37 layout node count underflows"))?;
    let minimum_leaf_rows = tree.corpus_rows / leaf_count;
    let remainder = tree.corpus_rows % leaf_count;
    let maximum_leaf_rows = minimum_leaf_rows
        .checked_add(u64::from(remainder != 0))
        .ok_or_else(|| invalid("V37 layout row count overflows"))?;
    Ok(V37LayoutProjection {
        leaf_count,
        internal_node_count,
        minimum_leaf_rows,
        maximum_leaf_rows,
        total_rows: tree.corpus_rows,
    })
}

pub(crate) fn project_v37_work(
    tree: &V37TreeSpec,
    relation: &V37RelationSpec,
) -> Result<V37WorkProjection> {
    validate_v37_specs(tree, relation)?;
    let layout = project_v37_layout(tree)?;
    let maximum_direct_node_visits = layout
        .leaf_count
        .checked_mul(2)
        .and_then(|value| value.checked_sub(1))
        .ok_or_else(|| invalid("V37 direct work projection overflows"))?;
    let maximum_relation_records = relation
        .maximum_leaf_probes
        .checked_mul(
            relation
                .prefix_lengths
                .last()
                .copied()
                .ok_or_else(|| invalid("V37 relation prefix is empty"))?,
        )
        .ok_or_else(|| invalid("V37 relation work projection overflows"))?;
    Ok(V37WorkProjection {
        maximum_direct_node_visits,
        maximum_relation_node_visits: V37_MAXIMUM_RELATION_NODE_VISITS,
        maximum_relation_records,
        selected_postings: V37_SELECTED_POSTINGS,
    })
}

fn v37_tree_bytes(internal_nodes: u64, dimensions: u64) -> Result<u64> {
    let normal_bytes = dimensions
        .checked_mul(4)
        .ok_or_else(|| invalid("V37 tree byte projection overflows"))?;
    internal_nodes
        .checked_mul(
            normal_bytes
                .checked_add(V37_NODE_METADATA_BYTES)
                .ok_or_else(|| invalid("V37 tree byte projection overflows"))?,
        )
        .ok_or_else(|| invalid("V37 tree byte projection overflows"))
}

pub(crate) fn project_v37_serving_bytes(
    tree: &V37TreeSpec,
    relation: &V37RelationSpec,
) -> Result<V37ServingProjection> {
    validate_v37_specs(tree, relation)?;
    let layout = project_v37_layout(tree)?;
    let ownership_tree_bytes = v37_tree_bytes(layout.internal_node_count, tree.dimensions)?;
    let relation_internal_nodes = relation
        .leaf_count
        .checked_sub(1)
        .ok_or_else(|| invalid("V37 relation node count underflows"))?;
    let relation_tree_bytes = v37_tree_bytes(relation_internal_nodes, tree.dimensions)?;
    let maximum_prefix = relation
        .prefix_lengths
        .last()
        .copied()
        .ok_or_else(|| invalid("V37 relation prefix is empty"))?;
    let record_bytes = relation
        .leaf_count
        .checked_mul(maximum_prefix)
        .and_then(|value| value.checked_mul(V37_RELATION_RECORD_BYTES))
        .ok_or_else(|| invalid("V37 relation byte projection overflows"))?;
    let offset_bytes = relation
        .leaf_count
        .checked_add(1)
        .and_then(|value| value.checked_mul(8))
        .ok_or_else(|| invalid("V37 relation byte projection overflows"))?;
    let relation_prefix_bytes = record_bytes
        .checked_add(offset_bytes)
        .ok_or_else(|| invalid("V37 relation byte projection overflows"))?;
    let total_bytes = ownership_tree_bytes
        .checked_add(relation_tree_bytes)
        .and_then(|value| value.checked_add(relation_prefix_bytes))
        .ok_or_else(|| invalid("V37 serving byte projection overflows"))?;
    Ok(V37ServingProjection {
        ownership_tree_bytes,
        relation_tree_bytes,
        relation_prefix_bytes,
        total_bytes,
    })
}

pub(crate) fn project_v37_construction_bytes(
    tree: &V37TreeSpec,
    relation: &V37RelationSpec,
) -> Result<V37ConstructionProjection> {
    validate_v37_specs(tree, relation)?;
    let layout = project_v37_layout(tree)?;
    let resident_projected_bytes = tree
        .corpus_rows
        .checked_mul(tree.dimensions)
        .and_then(|value| value.checked_mul(4))
        .ok_or_else(|| invalid("V37 coordinate byte projection overflows"))?;
    let source_decode_working_bytes = 65_536_u64
        .checked_mul(8 + 768 * 4)
        .and_then(|value| value.checked_add(2 * 256 * 1_048_576))
        .ok_or_else(|| invalid("V37 source decoder byte projection overflows"))?;
    let feature_id_bytes = tree
        .corpus_rows
        .checked_mul(8)
        .ok_or_else(|| invalid("V37 feature ID byte projection overflows"))?;
    let ordinal_index_bytes = tree
        .corpus_rows
        .checked_mul(16)
        .ok_or_else(|| invalid("V37 ordinal index byte projection overflows"))?;
    let member_index_bytes = tree
        .corpus_rows
        .checked_mul(8)
        .ok_or_else(|| invalid("V37 member index byte projection overflows"))?;
    let score_tuple_bytes = tree
        .corpus_rows
        .checked_mul(24)
        .ok_or_else(|| invalid("V37 score tuple byte projection overflows"))?;
    let assignment_bytes = tree
        .corpus_rows
        .checked_mul(16)
        .ok_or_else(|| invalid("V37 assignment byte projection overflows"))?;
    let reservoir_bytes = tree
        .corpus_rows
        .checked_mul(16)
        .and_then(|value| {
            tree.training_sample_rows
                .min(tree.corpus_rows)
                .checked_mul(40)
                .and_then(|heap| value.checked_add(heap))
        })
        .ok_or_else(|| invalid("V37 reservoir byte projection overflows"))?;
    let tree_bytes = v37_tree_bytes(layout.internal_node_count, tree.dimensions)?;
    let ownership_writer_bytes = tree
        .corpus_rows
        .checked_mul(48)
        .ok_or_else(|| invalid("V37 ownership writer byte projection overflows"))?;
    let worker_stack_bytes = V37_MAXIMUM_WORKERS
        .checked_mul(V37_WORKER_STACK_BYTES)
        .ok_or_else(|| invalid("V37 worker stack byte projection overflows"))?;
    let subtotal_bytes = [
        source_decode_working_bytes,
        resident_projected_bytes,
        feature_id_bytes,
        ordinal_index_bytes,
        member_index_bytes,
        score_tuple_bytes,
        assignment_bytes,
        reservoir_bytes,
        tree_bytes,
        ownership_writer_bytes,
        worker_stack_bytes,
    ]
    .into_iter()
    .try_fold(0_u64, |total, value| total.checked_add(value))
    .ok_or_else(|| invalid("V37 construction byte projection overflows"))?;
    let allocator_headroom_bytes = subtotal_bytes / 4;
    let total_bytes = subtotal_bytes
        .checked_add(allocator_headroom_bytes)
        .ok_or_else(|| invalid("V37 construction byte projection overflows"))?;
    let disposition = if total_bytes <= V37_MEMORY_LIMIT_BYTES {
        V37LayoutDisposition::Admissible
    } else {
        V37LayoutDisposition::ResourceRejected
    };
    Ok(V37ConstructionProjection {
        source_decode_working_bytes,
        resident_projected_bytes,
        feature_id_bytes,
        ordinal_index_bytes,
        member_index_bytes,
        score_tuple_bytes,
        assignment_bytes,
        reservoir_bytes,
        tree_bytes,
        ownership_writer_bytes,
        worker_stack_bytes,
        subtotal_bytes,
        allocator_headroom_bytes,
        total_bytes,
        disposition,
    })
}

fn validate_score_inputs(row: &[f32], normal: &[f32]) -> Result<()> {
    if row.is_empty()
        || row.len() != normal.len()
        || row.iter().chain(normal).any(|value| !value.is_finite())
    {
        return Err(invalid("V37 hyperplane score input differs"));
    }
    Ok(())
}

fn scalar_dot_8x12(left: &[f32; 96], right: &[f32; 96]) -> f32 {
    let mut lanes = [0.0_f32; 8];
    for (lane, accumulator) in lanes.iter_mut().enumerate() {
        for step in 0..12 {
            let dimension = lane * 12 + step;
            *accumulator = left[dimension].mul_add(right[dimension], *accumulator);
        }
    }
    lanes.into_iter().fold(0.0_f32, |sum, value| sum + value)
}

pub(crate) fn score_v37_hyperplane_scalar(row: &[f32], normal: &[f32]) -> Result<f32> {
    validate_score_inputs(row, normal)?;
    let mut score = 0.0_f32;
    let row_blocks = row.as_chunks::<96>().0;
    let normal_blocks = normal.as_chunks::<96>().0;
    for (row_block, normal_block) in row_blocks.iter().zip(normal_blocks) {
        score += scalar_dot_8x12(row_block, normal_block);
    }
    let consumed = row_blocks.len() * 96;
    for dimension in consumed..row.len() {
        score = row[dimension].mul_add(normal[dimension], score);
    }
    if !score.is_finite() {
        return Err(invalid("V37 hyperplane score is non-finite"));
    }
    Ok(if score == 0.0 { 0.0 } else { score })
}

pub(crate) fn score_v37_hyperplane_fused(
    row: &[f32],
    normal: &[f32],
) -> Result<(f32, &'static str)> {
    let kernel = v37_fused_kernel()?;
    score_v37_hyperplane_with_kernel(row, normal, kernel)
}

fn v37_fused_kernel() -> Result<borsuk_fma::FusedDot8x12> {
    V37_FMA_KERNEL
        .get_or_init(|| borsuk_fma::FusedDot8x12::detect().ok())
        .as_ref()
        .copied()
        .ok_or_else(|| invalid("V37 fused SIMD backend is unavailable"))
}

fn v37_fma_backend_name(kernel: borsuk_fma::FusedDot8x12) -> &'static str {
    match kernel.backend() {
        borsuk_fma::FmaBackend::Aarch64NeonFma => "aarch64-neon-fma",
        borsuk_fma::FmaBackend::X86AvxFma => "x86-avx-fma",
    }
}

fn score_v37_hyperplane_with_kernel(
    row: &[f32],
    normal: &[f32],
    kernel: borsuk_fma::FusedDot8x12,
) -> Result<(f32, &'static str)> {
    validate_score_inputs(row, normal)?;
    let complete = row.len() / 96;
    let mut score = 0.0_f32;
    for block in 0..complete {
        let start = block * 96;
        let row_block: &[f32; 96] = row[start..start + 96]
            .try_into()
            .map_err(|_| invalid("V37 fused score block differs"))?;
        let normal_block: &[f32; 96] = normal[start..start + 96]
            .try_into()
            .map_err(|_| invalid("V37 fused score block differs"))?;
        score += kernel.dot(row_block, normal_block);
    }
    for dimension in complete * 96..row.len() {
        score = row[dimension].mul_add(normal[dimension], score);
    }
    if !score.is_finite() {
        return Err(invalid("V37 hyperplane score is non-finite"));
    }
    Ok((
        if score == 0.0 { 0.0 } else { score },
        v37_fma_backend_name(kernel),
    ))
}

pub(crate) fn select_v37_node_reservoir(
    source_ordinals: &[u64],
    seed: u64,
    node_id: u64,
    maximum_rows: usize,
) -> Result<Vec<u64>> {
    if source_ordinals.is_empty() || maximum_rows == 0 || seed == 0 {
        return Err(invalid("V37 node reservoir authority differs"));
    }
    let mut previous = None;
    let mut unique = source_ordinals.to_vec();
    unique.sort_unstable();
    for ordinal in &unique {
        if previous == Some(*ordinal) {
            return Err(invalid("V37 source ordinals are not unique"));
        }
        previous = Some(*ordinal);
    }
    let limit = maximum_rows.min(unique.len());
    let mut ranked = BinaryHeap::with_capacity(limit);
    for source_ordinal in unique {
        let mut digest = Sha256::new();
        digest.update(b"borsuk-v37-node-reservoir-v1\n");
        digest.update(seed.to_le_bytes());
        digest.update(node_id.to_le_bytes());
        digest.update(source_ordinal.to_le_bytes());
        let candidate = (<[u8; 32]>::from(digest.finalize()), source_ordinal);
        if ranked.len() < limit {
            ranked.push(candidate);
        } else if ranked.peek().is_some_and(|largest| candidate < *largest) {
            ranked.pop();
            ranked.push(candidate);
        }
    }
    let mut ranked = ranked.into_vec();
    ranked.sort_unstable();
    Ok(ranked.into_iter().map(|(_, ordinal)| ordinal).collect())
}

pub(crate) fn route_v37_corpus_member(
    score: f32,
    source_ordinal: u64,
    boundary_score: f32,
    boundary_source_ordinal: u64,
) -> Result<bool> {
    if !score.is_finite() || !boundary_score.is_finite() {
        return Err(invalid("V37 corpus replay score is non-finite"));
    }
    Ok(score
        .total_cmp(&boundary_score)
        .then_with(|| source_ordinal.cmp(&boundary_source_ordinal))
        .is_le())
}

pub(crate) fn route_v37_query(score: f32, boundary_score: f32) -> Result<V37QueryBranch> {
    if !score.is_finite() || !boundary_score.is_finite() {
        return Err(invalid("V37 query score is non-finite"));
    }
    let order = score.total_cmp(&boundary_score);
    Ok(V37QueryBranch {
        primary_is_left: order.is_le(),
        queues_sibling_zero_margin: order.is_eq(),
    })
}

pub(crate) fn route_v37_primary_leaf(
    tree: &V37BalancedTree,
    vector: &[f32],
    source_ordinal: u64,
    expected_backend: &str,
) -> Result<u32> {
    if vector.len() != tree.dimensions
        || vector.iter().any(|value| !value.is_finite())
        || expected_backend != tree.fma_backend
    {
        return Err(invalid("V37 replay authority differs"));
    }
    let kernel = v37_fused_kernel()?;
    if v37_fma_backend_name(kernel) != expected_backend {
        return Err(invalid("V37 replay backend differs"));
    }
    let mut node_index = 0_usize;
    loop {
        let node = tree
            .nodes
            .get(node_index)
            .ok_or_else(|| invalid("V37 replay node differs"))?;
        if let Some(posting) = node.posting_ordinal {
            return Ok(posting);
        }
        let (score, _) = score_v37_hyperplane_with_kernel(vector, &node.normal, kernel)?;
        let is_left = route_v37_corpus_member(
            score,
            source_ordinal,
            f32::from_bits(node.boundary_score_bits),
            node.boundary_source_ordinal,
        )?;
        node_index = if is_left {
            node.left_node
        } else {
            node.right_node
        }
        .ok_or_else(|| invalid("V37 replay child differs"))? as usize;
    }
}

pub(crate) fn prepare_v37_direct_router<'a>(
    tree: &'a V37BalancedTree,
    expected_backend: &str,
    maximum_node_visits: usize,
) -> Result<V37DirectRouter<'a>> {
    validate_v37_tree_geometry(tree)?;
    if expected_backend != tree.fma_backend
        || tree.leaf_populations.len() < V37_SELECTED_POSTINGS as usize
        || maximum_node_visits == 0
        || maximum_node_visits > V37_MAXIMUM_DIRECT_NODE_VISITS as usize
        || maximum_node_visits > tree.nodes.len()
    {
        return Err(invalid("V37 direct routing authority differs"));
    }
    let kernel = v37_fused_kernel()?;
    if v37_fma_backend_name(kernel) != expected_backend {
        return Err(invalid("V37 direct routing backend differs"));
    }
    Ok(V37DirectRouter {
        tree,
        kernel,
        maximum_node_visits,
    })
}

pub(crate) fn select_v37_direct_postings(
    router: &V37DirectRouter<'_>,
    query: &[f32],
) -> Result<V37DirectSelection> {
    select_v37_tree_postings(
        router.tree,
        router.kernel,
        router.maximum_node_visits,
        V37_SELECTED_POSTINGS as usize,
        query,
    )
}

pub(crate) fn prepare_v37_relation_router<'a>(
    tree: &'a V37BalancedTree,
    expected_backend: &str,
    maximum_node_visits: usize,
) -> Result<V37RelationRouter<'a>> {
    validate_v37_tree_geometry(tree)?;
    if expected_backend != tree.fma_backend
        || tree.leaf_populations.len() < 32
        || maximum_node_visits == 0
        || maximum_node_visits > V37_MAXIMUM_RELATION_NODE_VISITS as usize
        || maximum_node_visits > tree.nodes.len()
    {
        return Err(invalid("V37 relation routing authority differs"));
    }
    let kernel = v37_fused_kernel()?;
    if v37_fma_backend_name(kernel) != expected_backend {
        return Err(invalid("V37 relation routing backend differs"));
    }
    Ok(V37RelationRouter {
        tree,
        kernel,
        maximum_node_visits,
    })
}

fn select_v37_tree_postings(
    tree: &V37BalancedTree,
    kernel: borsuk_fma::FusedDot8x12,
    maximum_node_visits: usize,
    posting_limit: usize,
    query: &[f32],
) -> Result<V37DirectSelection> {
    if query.len() != tree.dimensions || query.iter().any(|value| !value.is_finite()) {
        return Err(invalid("V37 direct routing authority differs"));
    }
    let mut queue = vec![(0.0_f32, 0_u32)];
    let mut visited = vec![false; tree.nodes.len()];
    let mut selected_postings = Vec::with_capacity(posting_limit);
    let mut node_visits = 0_usize;
    let mut scored_internal_nodes = 0_usize;
    while selected_postings.len() < posting_limit {
        let next = queue
            .iter()
            .enumerate()
            .min_by(|(_, left), (_, right)| {
                left.0
                    .total_cmp(&right.0)
                    .then_with(|| left.1.cmp(&right.1))
            })
            .map(|(index, _)| index)
            .ok_or_else(|| invalid("V37 direct routing candidate shortage"))?;
        let (path_penalty, node_id) = queue.swap_remove(next);
        node_visits = node_visits
            .checked_add(1)
            .ok_or_else(|| invalid("V37 direct node visits overflow"))?;
        if node_visits > maximum_node_visits {
            return Err(invalid("V37 direct node visit limit exceeded"));
        }
        let node_index = node_id as usize;
        let already_visited = visited
            .get_mut(node_index)
            .ok_or_else(|| invalid("V37 direct node differs"))?;
        if std::mem::replace(already_visited, true) {
            return Err(invalid("V37 direct node was visited twice"));
        }
        let node = &tree.nodes[node_index];
        if let Some(posting) = node.posting_ordinal {
            if selected_postings.contains(&posting) {
                return Err(invalid("V37 direct posting is duplicated"));
            }
            selected_postings.push(posting);
            continue;
        }
        scored_internal_nodes = scored_internal_nodes
            .checked_add(1)
            .ok_or_else(|| invalid("V37 direct score count overflows"))?;
        let (score, _) = score_v37_hyperplane_with_kernel(query, &node.normal, kernel)?;
        let boundary = f32::from_bits(node.boundary_score_bits);
        let branch = route_v37_query(score, boundary)?;
        let margin = score - boundary;
        let squared_margin = margin * margin;
        if !squared_margin.is_finite() || squared_margin < 0.0 {
            return Err(invalid("V37 direct margin is non-finite"));
        }
        let sibling_penalty = if squared_margin.total_cmp(&path_penalty).is_gt() {
            squared_margin
        } else {
            path_penalty
        };
        let left = node
            .left_node
            .ok_or_else(|| invalid("V37 direct left child is absent"))?;
        let right = node
            .right_node
            .ok_or_else(|| invalid("V37 direct right child is absent"))?;
        let (primary, sibling) = if branch.primary_is_left {
            (left, right)
        } else {
            (right, left)
        };
        queue.push((path_penalty, primary));
        queue.push((sibling_penalty, sibling));
    }
    Ok(V37DirectSelection {
        selected_postings,
        node_visits: u32::try_from(node_visits)
            .map_err(|_| invalid("V37 direct node visits exceed receipt width"))?,
        scored_internal_nodes: u32::try_from(scored_internal_nodes)
            .map_err(|_| invalid("V37 direct score count exceeds receipt width"))?,
        fma_backend: tree.fma_backend.clone(),
    })
}

fn squared_distance_with_kernel(
    left: &[f32],
    right: &[f32],
    kernel: borsuk_fma::FusedDot8x12,
) -> Result<(f32, &'static str)> {
    validate_score_inputs(left, right)?;
    let complete = left.len() / 96;
    let mut distance = 0.0_f32;
    for block in 0..complete {
        let start = block * 96;
        let mut difference = [0.0_f32; 96];
        for (output, (left, right)) in difference.iter_mut().zip(
            left[start..start + 96]
                .iter()
                .zip(&right[start..start + 96]),
        ) {
            *output = left - right;
        }
        distance += kernel.dot(&difference, &difference);
    }
    for dimension in complete * 96..left.len() {
        let difference = left[dimension] - right[dimension];
        distance = difference.mul_add(difference, distance);
    }
    if !distance.is_finite() {
        return Err(invalid("V37 squared distance is non-finite"));
    }
    Ok((
        if distance == 0.0 { 0.0 } else { distance },
        v37_fma_backend_name(kernel),
    ))
}

fn mean_vector(
    rows: &impl V37TrainingDataset,
    indices: &[usize],
    dimensions: usize,
) -> Result<Vec<f32>> {
    if indices.is_empty() {
        return Err(invalid("V37 two-means partition is empty"));
    }
    let mut ordered = indices.to_vec();
    ordered.sort_unstable_by_key(|index| rows.source_ordinal(*index));
    let mut sums = vec![0.0_f64; dimensions];
    for index in ordered {
        for (sum, value) in sums.iter_mut().zip(rows.vector(index)) {
            *sum += f64::from(*value);
        }
    }
    let divisor = indices.len() as f64;
    let mean = sums
        .into_iter()
        .map(|sum| (sum / divisor) as f32)
        .collect::<Vec<_>>();
    if mean.iter().any(|value| !value.is_finite()) {
        return Err(invalid("V37 two-means mean is non-finite"));
    }
    Ok(mean)
}

fn repair_v37_empty_partition_with_kernel(
    rows: &impl V37TrainingDataset,
    zero_rows: &mut Vec<usize>,
    one_rows: &mut Vec<usize>,
    kernel: borsuk_fma::FusedDot8x12,
) -> Result<()> {
    let (empty, populated) = if zero_rows.is_empty() && one_rows.len() >= 2 {
        (zero_rows, one_rows)
    } else if one_rows.is_empty() && zero_rows.len() >= 2 {
        (one_rows, zero_rows)
    } else {
        return Err(invalid("V37 empty-label repair authority differs"));
    };
    let mean = mean_vector(rows, populated, rows.vector(populated[0]).len())?;
    let donor_position = populated
        .iter()
        .enumerate()
        .map(|(position, index)| {
            squared_distance_with_kernel(rows.vector(*index), &mean, kernel).map(|(distance, _)| {
                (
                    distance,
                    std::cmp::Reverse(rows.source_ordinal(*index)),
                    position,
                )
            })
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .max_by(|left, right| {
            left.0
                .total_cmp(&right.0)
                .then_with(|| left.1.cmp(&right.1))
        })
        .ok_or_else(|| invalid("V37 empty-label donor is absent"))?
        .2;
    empty.push(populated.remove(donor_position));
    Ok(())
}

fn repair_v37_empty_partition(
    rows: &[V37TrainingRow],
    zero_rows: &mut Vec<usize>,
    one_rows: &mut Vec<usize>,
) -> Result<()> {
    let kernel = v37_fused_kernel()?;
    repair_v37_empty_partition_with_kernel(
        &V37OwnedTrainingDataset(rows),
        zero_rows,
        one_rows,
        kernel,
    )
}

fn deployed_hyperplane(
    members: &[usize],
    rows: &impl V37TrainingDataset,
    ordinal_to_index: &[(u64, usize)],
    shape: &V37TrainingShape,
    seed: u64,
    node_id: u64,
    kernel: borsuk_fma::FusedDot8x12,
) -> Result<Vec<f32>> {
    let ordinals = members
        .iter()
        .map(|index| rows.source_ordinal(*index))
        .collect::<Vec<_>>();
    let selected = select_v37_node_reservoir(&ordinals, seed, node_id, shape.reservoir_rows)?;
    let mut sample = selected
        .iter()
        .map(|ordinal| {
            ordinal_to_index
                .binary_search_by_key(ordinal, |entry| entry.0)
                .ok()
                .map(|index| ordinal_to_index[index].1)
                .ok_or_else(|| invalid("V37 reservoir row is absent"))
        })
        .collect::<Result<Vec<_>>>()?;
    sample.sort_unstable_by_key(|index| rows.source_ordinal(*index));
    if sample.len() < 2 {
        return Err(invalid("V37 node reservoir cannot seed two means"));
    }
    let mut zero = rows.vector(sample[0]).to_vec();
    let mut farthest = None;
    for index in sample.iter().skip(1) {
        let (distance, _) = squared_distance_with_kernel(&zero, rows.vector(*index), kernel)?;
        let candidate = (
            distance,
            rows.source_ordinal(*index),
            rows.vector(*index).to_vec(),
        );
        if farthest
            .as_ref()
            .is_none_or(|current: &(f32, u64, Vec<f32>)| {
                candidate
                    .0
                    .total_cmp(&current.0)
                    .then_with(|| current.1.cmp(&candidate.1))
                    .is_gt()
            })
        {
            farthest = Some(candidate);
        }
    }
    let (distance, _, mut one) = farthest.ok_or_else(|| invalid("V37 seed is absent"))?;
    if distance <= 0.0 {
        return Err(invalid("V37 node geometry is degenerate"));
    }
    for _ in 0..shape.two_means_iterations {
        let mut zero_rows = Vec::new();
        let mut one_rows = Vec::new();
        for index in &sample {
            let zero_distance = squared_distance_with_kernel(rows.vector(*index), &zero, kernel)?.0;
            let one_distance = squared_distance_with_kernel(rows.vector(*index), &one, kernel)?.0;
            if zero_distance.total_cmp(&one_distance).is_gt() {
                one_rows.push(*index);
            } else {
                zero_rows.push(*index);
            }
        }
        if zero_rows.is_empty() || one_rows.is_empty() {
            repair_v37_empty_partition_with_kernel(rows, &mut zero_rows, &mut one_rows, kernel)?;
        }
        zero = mean_vector(rows, &zero_rows, shape.dimensions)?;
        one = mean_vector(rows, &one_rows, shape.dimensions)?;
    }
    let mut normal = one
        .iter()
        .zip(&zero)
        .map(|(one, zero)| one - zero)
        .collect::<Vec<_>>();
    let squared_norm = normal.iter().try_fold(0.0_f64, |sum, value| {
        if !value.is_finite() {
            None
        } else {
            Some(sum + f64::from(*value) * f64::from(*value))
        }
    });
    let squared_norm = squared_norm
        .filter(|value| value.is_finite() && *value > 0.0)
        .ok_or_else(|| invalid("V37 hyperplane normal is degenerate"))?;
    let inverse_norm = squared_norm.sqrt().recip() as f32;
    for value in &mut normal {
        *value *= inverse_norm;
        if *value == 0.0 {
            *value = 0.0;
        }
    }
    Ok(normal)
}

struct V37TreeBuilder<'a, D: V37TrainingDataset> {
    rows: &'a D,
    ordinal_to_index: &'a [(u64, usize)],
    pool: &'a ThreadPool,
    block_rows: usize,
    shape: V37TrainingShape,
    seed: u64,
    nodes: Vec<V37BalancedNode>,
    leaf_populations: Vec<u64>,
    assignments: Vec<V37OwnershipAssignment>,
    backend: &'static str,
    kernel: borsuk_fma::FusedDot8x12,
    partition_coordinate_scores: u64,
    partition_scoring_elapsed_ns: u64,
    completed_internal_nodes: u64,
}

impl<D: V37TrainingDataset> V37TreeBuilder<'_, D> {
    fn score(&self, row: &[f32], normal: &[f32]) -> Result<f32> {
        let (score, backend) = score_v37_hyperplane_with_kernel(row, normal, self.kernel)?;
        if self.backend != backend {
            return Err(invalid("V37 fused backend changed during construction"));
        }
        Ok(score)
    }

    fn build<F>(&mut self, members: Vec<usize>, leaves: u64, progress: &mut F) -> Result<u32>
    where
        F: FnMut(&V37TrainingProgress) -> Result<()>,
    {
        let node_id =
            u32::try_from(self.nodes.len()).map_err(|_| invalid("V37 node ordinal overflows"))?;
        self.nodes.push(V37BalancedNode {
            normal: Vec::new(),
            boundary_score_bits: 0.0_f32.to_bits(),
            boundary_source_ordinal: 0,
            left_node: None,
            right_node: None,
            posting_ordinal: None,
            population: members.len() as u64,
        });
        if leaves == 1 {
            let posting_ordinal = u32::try_from(self.leaf_populations.len())
                .map_err(|_| invalid("V37 posting ordinal overflows"))?;
            self.leaf_populations.push(members.len() as u64);
            for index in members {
                self.assignments.push(V37OwnershipAssignment {
                    source_ordinal: self.rows.source_ordinal(index),
                    posting_ordinal,
                });
            }
            self.nodes[node_id as usize].posting_ordinal = Some(posting_ordinal);
            return Ok(node_id);
        }
        let quota = project_v37_child_quota(members.len() as u64, leaves)?;
        let normal = deployed_hyperplane(
            &members,
            self.rows,
            self.ordinal_to_index,
            &self.shape,
            self.seed,
            u64::from(node_id),
            self.kernel,
        )?;
        let scoring_started = Instant::now();
        let score_blocks = self.pool.install(|| {
            members
                .par_chunks(self.block_rows)
                .map(|block| {
                    block
                        .iter()
                        .map(|index| {
                            self.score(self.rows.vector(*index), &normal)
                                .map(|score| (score, self.rows.source_ordinal(*index), *index))
                        })
                        .collect::<Result<Vec<_>>>()
                })
                .collect::<Result<Vec<_>>>()
        })?;
        let scoring_elapsed_ns = u64::try_from(scoring_started.elapsed().as_nanos())
            .map_err(|_| invalid("V37 partition scoring duration overflows"))?;
        let coordinate_scores = u64::try_from(members.len())
            .ok()
            .zip(u64::try_from(self.shape.dimensions).ok())
            .and_then(|(rows, dimensions)| rows.checked_mul(dimensions))
            .ok_or_else(|| invalid("V37 partition coordinate score count overflows"))?;
        self.partition_coordinate_scores = self
            .partition_coordinate_scores
            .checked_add(coordinate_scores)
            .ok_or_else(|| invalid("V37 partition coordinate score count overflows"))?;
        self.partition_scoring_elapsed_ns = self
            .partition_scoring_elapsed_ns
            .checked_add(scoring_elapsed_ns)
            .ok_or_else(|| invalid("V37 partition scoring duration overflows"))?;
        self.completed_internal_nodes = self
            .completed_internal_nodes
            .checked_add(1)
            .ok_or_else(|| invalid("V37 completed internal node count overflows"))?;
        progress(&V37TrainingProgress {
            completed_internal_nodes: self.completed_internal_nodes,
            partition_coordinate_scores: self.partition_coordinate_scores,
            sequence: self.completed_internal_nodes,
            total_internal_nodes: self
                .shape
                .leaf_count
                .checked_sub(1)
                .ok_or_else(|| invalid("V37 internal node count underflows"))?,
        })?;
        let mut scored = score_blocks.into_iter().flatten().collect::<Vec<_>>();
        scored.sort_unstable_by(|left, right| {
            left.0
                .total_cmp(&right.0)
                .then_with(|| left.1.cmp(&right.1))
        });
        let midpoint = usize::try_from(quota.left_rows)
            .map_err(|_| invalid("V37 recursive quota exceeds address space"))?;
        let boundary = scored[midpoint - 1];
        let right_members = scored[midpoint..]
            .iter()
            .map(|entry| entry.2)
            .collect::<Vec<_>>();
        let left_members = scored[..midpoint]
            .iter()
            .map(|entry| entry.2)
            .collect::<Vec<_>>();
        drop(scored);
        let left_node = self.build(left_members, quota.left_leaves, progress)?;
        let right_node = self.build(right_members, quota.right_leaves, progress)?;
        self.nodes[node_id as usize] = V37BalancedNode {
            normal,
            boundary_score_bits: boundary.0.to_bits(),
            boundary_source_ordinal: boundary.1,
            left_node: Some(left_node),
            right_node: Some(right_node),
            posting_ordinal: None,
            population: quota.left_rows + quota.right_rows,
        };
        Ok(node_id)
    }
}

pub(crate) fn train_v37_ownership_tree(
    rows: &[V37TrainingRow],
    shape: V37TrainingShape,
    seed: u64,
    worker_count: usize,
    block_rows: usize,
) -> Result<V37BalancedTree> {
    train_v37_ownership_tree_dataset(
        &V37OwnedTrainingDataset(rows),
        shape,
        seed,
        worker_count,
        block_rows,
    )
}

pub(crate) fn train_v37_ownership_tree_resident_coordinates(
    coordinates: &[f32],
    shape: V37TrainingShape,
    seed: u64,
    worker_count: usize,
    block_rows: usize,
) -> Result<V37BalancedTree> {
    if shape.dimensions == 0
        || coordinates.is_empty()
        || !coordinates.len().is_multiple_of(shape.dimensions)
    {
        return Err(invalid("V37 resident training coordinates differ"));
    }
    train_v37_ownership_tree_dataset(
        &V37ResidentTrainingDataset {
            coordinates,
            dimensions: shape.dimensions,
        },
        shape,
        seed,
        worker_count,
        block_rows,
    )
}

pub(crate) fn train_v37_ownership_tree_resident_coordinates_with_evidence(
    coordinates: &[f32],
    shape: V37TrainingShape,
    seed: u64,
    worker_count: usize,
    block_rows: usize,
) -> Result<(V37BalancedTree, V37TrainingEvidence)> {
    train_v37_ownership_tree_resident_coordinates_with_progress(
        coordinates,
        shape,
        seed,
        worker_count,
        block_rows,
        |_| Ok(()),
    )
}

pub(crate) fn train_v37_ownership_tree_resident_coordinates_with_progress<F>(
    coordinates: &[f32],
    shape: V37TrainingShape,
    seed: u64,
    worker_count: usize,
    block_rows: usize,
    progress: F,
) -> Result<(V37BalancedTree, V37TrainingEvidence)>
where
    F: FnMut(&V37TrainingProgress) -> Result<()>,
{
    if shape.dimensions == 0
        || coordinates.is_empty()
        || !coordinates.len().is_multiple_of(shape.dimensions)
    {
        return Err(invalid("V37 resident training coordinates differ"));
    }
    train_v37_ownership_tree_dataset_with_progress(
        &V37ResidentTrainingDataset {
            coordinates,
            dimensions: shape.dimensions,
        },
        shape,
        seed,
        worker_count,
        block_rows,
        progress,
    )
}

fn train_v37_ownership_tree_dataset(
    rows: &impl V37TrainingDataset,
    shape: V37TrainingShape,
    seed: u64,
    worker_count: usize,
    block_rows: usize,
) -> Result<V37BalancedTree> {
    train_v37_ownership_tree_dataset_with_evidence(rows, shape, seed, worker_count, block_rows)
        .map(|(tree, _)| tree)
}

fn train_v37_ownership_tree_dataset_with_evidence(
    rows: &impl V37TrainingDataset,
    shape: V37TrainingShape,
    seed: u64,
    worker_count: usize,
    block_rows: usize,
) -> Result<(V37BalancedTree, V37TrainingEvidence)> {
    train_v37_ownership_tree_dataset_with_progress(
        rows,
        shape,
        seed,
        worker_count,
        block_rows,
        |_| Ok(()),
    )
}

fn train_v37_ownership_tree_dataset_with_progress<F>(
    rows: &impl V37TrainingDataset,
    shape: V37TrainingShape,
    seed: u64,
    worker_count: usize,
    block_rows: usize,
    mut progress: F,
) -> Result<(V37BalancedTree, V37TrainingEvidence)>
where
    F: FnMut(&V37TrainingProgress) -> Result<()>,
{
    let training_started = Instant::now();
    if rows.is_empty()
        || shape.dimensions == 0
        || shape.leaf_count < 2
        || shape.reservoir_rows < 2
        || shape.two_means_iterations != 8
        || seed == 0
        || worker_count == 0
        || block_rows == 0
        || rows.len() < shape.leaf_count as usize
    {
        return Err(invalid("V37 training authority differs"));
    }
    let mut ordinal_to_index = Vec::with_capacity(rows.len());
    for index in 0..rows.len() {
        let source_ordinal = rows.source_ordinal(index);
        let vector = rows.vector(index);
        if vector.len() != shape.dimensions || vector.iter().any(|value| !value.is_finite()) {
            return Err(invalid("V37 training row authority differs"));
        }
        ordinal_to_index.push((source_ordinal, index));
    }
    ordinal_to_index.sort_unstable_by_key(|entry| entry.0);
    if ordinal_to_index
        .windows(2)
        .any(|pair| pair[0].0 == pair[1].0)
    {
        return Err(invalid("V37 training row authority differs"));
    }
    let members = ordinal_to_index
        .iter()
        .map(|entry| entry.1)
        .collect::<Vec<_>>();
    let pool = ThreadPoolBuilder::new()
        .num_threads(worker_count)
        .build()
        .map_err(|_| invalid("V37 worker pool construction failed"))?;
    let kernel = v37_fused_kernel()?;
    let backend = v37_fma_backend_name(kernel);
    let mut builder = V37TreeBuilder {
        rows,
        ordinal_to_index: &ordinal_to_index,
        pool: &pool,
        block_rows,
        shape,
        seed,
        nodes: Vec::new(),
        leaf_populations: Vec::new(),
        assignments: Vec::new(),
        backend,
        kernel,
        partition_coordinate_scores: 0,
        partition_scoring_elapsed_ns: 0,
        completed_internal_nodes: 0,
    };
    builder.build(members, shape.leaf_count, &mut progress)?;
    builder
        .assignments
        .sort_unstable_by_key(|assignment| assignment.source_ordinal);
    let tree = V37BalancedTree {
        dimensions: shape.dimensions,
        seed,
        fma_backend: builder.backend.to_owned(),
        nodes: builder.nodes,
        leaf_populations: builder.leaf_populations,
        assignments: builder.assignments,
    };
    let training_elapsed_ns = u64::try_from(training_started.elapsed().as_nanos())
        .map_err(|_| invalid("V37 training duration overflows"))?;
    if builder.partition_scoring_elapsed_ns == 0 {
        return Err(invalid("V37 partition scoring duration differs"));
    }
    let partition_coordinate_scores_per_second = u64::try_from(
        u128::from(builder.partition_coordinate_scores)
            .checked_mul(1_000_000_000)
            .ok_or_else(|| invalid("V37 partition throughput overflows"))?
            / u128::from(builder.partition_scoring_elapsed_ns),
    )
    .map_err(|_| invalid("V37 partition throughput overflows"))?;
    let evidence = V37TrainingEvidence {
        dimensions: u64::try_from(shape.dimensions)
            .map_err(|_| invalid("V37 training dimension count overflows"))?,
        fma_backend: tree.fma_backend.clone(),
        leaf_count: shape.leaf_count,
        partition_coordinate_scores: builder.partition_coordinate_scores,
        partition_coordinate_scores_per_second,
        partition_scoring_elapsed_ns: builder.partition_scoring_elapsed_ns,
        rows: u64::try_from(rows.len()).map_err(|_| invalid("V37 training row count overflows"))?,
        training_elapsed_ns,
    };
    Ok((tree, evidence))
}

pub(crate) fn validate_v37_tree_geometry(tree: &V37BalancedTree) -> Result<()> {
    if tree.dimensions == 0
        || tree.seed == 0
        || !matches!(
            tree.fma_backend.as_str(),
            "aarch64-neon-fma" | "x86-avx-fma"
        )
        || tree.leaf_populations.is_empty()
        || tree.nodes.len()
            != tree
                .leaf_populations
                .len()
                .checked_mul(2)
                .and_then(|value| value.checked_sub(1))
                .ok_or_else(|| invalid("V37 tree node count overflows"))?
    {
        return Err(invalid("V37 tree authority differs"));
    }
    let mut observed_populations = vec![None; tree.leaf_populations.len()];
    fn validate_preorder(
        tree: &V37BalancedTree,
        index: usize,
        expected_leaves: u64,
        next_posting: &mut usize,
        observed_populations: &mut [Option<u64>],
    ) -> Result<usize> {
        if index >= tree.nodes.len() {
            return Err(invalid("V37 tree topology differs"));
        }
        let node = &tree.nodes[index];
        let boundary = f32::from_bits(node.boundary_score_bits);
        if node.population == 0 || !boundary.is_finite() {
            return Err(invalid("V37 tree node authority differs"));
        }
        match (node.left_node, node.right_node, node.posting_ordinal) {
            (Some(left), Some(right), None) => {
                if expected_leaves < 2 {
                    return Err(invalid("V37 tree descendant-leaf quota differs"));
                }
                let quota = project_v37_child_quota(node.population, expected_leaves)?;
                let left = left as usize;
                let right = right as usize;
                if left != index + 1
                    || node.normal.len() != tree.dimensions
                    || node.normal.iter().any(|value| !value.is_finite())
                {
                    return Err(invalid("V37 tree internal node differs"));
                }
                let squared_norm = node.normal.iter().fold(0.0_f64, |sum, value| {
                    sum + f64::from(*value) * f64::from(*value)
                });
                if (squared_norm - 1.0).abs() > 1.0e-5
                    || left >= tree.nodes.len()
                    || right >= tree.nodes.len()
                    || tree.nodes[left].population != quota.left_rows
                    || tree.nodes[right].population != quota.right_rows
                    || tree.nodes[left]
                        .population
                        .checked_add(tree.nodes[right].population)
                        != Some(node.population)
                {
                    return Err(invalid("V37 tree internal node differs"));
                }
                let after_left = validate_preorder(
                    tree,
                    left,
                    quota.left_leaves,
                    next_posting,
                    observed_populations,
                )?;
                if right != after_left {
                    return Err(invalid("V37 tree preorder differs"));
                }
                validate_preorder(
                    tree,
                    right,
                    quota.right_leaves,
                    next_posting,
                    observed_populations,
                )
            }
            (None, None, Some(posting)) => {
                let posting = posting as usize;
                if expected_leaves != 1
                    || posting != *next_posting
                    || posting >= observed_populations.len()
                    || !node.normal.is_empty()
                    || observed_populations[posting]
                        .replace(node.population)
                        .is_some()
                {
                    return Err(invalid("V37 tree leaf differs"));
                }
                *next_posting += 1;
                Ok(index + 1)
            }
            _ => Err(invalid("V37 tree node role differs")),
        }
    }
    let mut next_posting = 0;
    let next_node = validate_preorder(
        tree,
        0,
        tree.leaf_populations.len() as u64,
        &mut next_posting,
        &mut observed_populations,
    )?;
    if next_node != tree.nodes.len()
        || next_posting != tree.leaf_populations.len()
        || observed_populations
            .iter()
            .zip(&tree.leaf_populations)
            .any(|(observed, expected)| *observed != Some(*expected))
    {
        return Err(invalid("V37 tree topology differs"));
    }
    Ok(())
}

fn validate_v37_balanced_tree(tree: &V37BalancedTree) -> Result<()> {
    validate_v37_tree_geometry(tree)?;
    let mut counts = vec![0_u64; tree.leaf_populations.len()];
    let mut previous = None;
    for assignment in &tree.assignments {
        if previous.is_some_and(|ordinal| assignment.source_ordinal <= ordinal)
            || assignment.posting_ordinal as usize >= counts.len()
        {
            return Err(invalid("V37 ownership assignment differs"));
        }
        previous = Some(assignment.source_ordinal);
        counts[assignment.posting_ordinal as usize] = counts[assignment.posting_ordinal as usize]
            .checked_add(1)
            .ok_or_else(|| invalid("V37 ownership population overflows"))?;
    }
    if counts != tree.leaf_populations {
        return Err(invalid("V37 ownership populations differ"));
    }
    Ok(())
}

fn v37_ownership_schema() -> Schema {
    Schema::new(vec![
        Field::new("source_ordinal", DataType::UInt64, false),
        Field::new("feature_row_id", DataType::UInt64, false),
        Field::new("posting_ordinal", DataType::UInt32, false),
        Field::new("posting_local_ordinal", DataType::UInt32, false),
    ])
}

pub(crate) fn encode_v37_ownership_parquet(
    assignments: &[V37OwnershipAssignment],
    feature_row_ids: &[u64],
    leaf_populations: &[u64],
) -> Result<V37EncodedOwnership> {
    if assignments.is_empty()
        || assignments.len() != feature_row_ids.len()
        || leaf_populations.is_empty()
    {
        return Err(invalid("V37 ownership table is empty"));
    }
    let mut local_counts = vec![0_u32; leaf_populations.len()];
    let mut local_ordinals = Vec::with_capacity(assignments.len());
    let mut unique_feature_ids = BTreeSet::new();
    for (source_ordinal, (assignment, feature_row_id)) in
        assignments.iter().zip(feature_row_ids).enumerate()
    {
        if assignment.source_ordinal != source_ordinal as u64
            || !unique_feature_ids.insert(*feature_row_id)
        {
            return Err(invalid("V37 ownership source ordering differs"));
        }
        let posting = assignment.posting_ordinal as usize;
        let local = local_counts
            .get_mut(posting)
            .ok_or_else(|| invalid("V37 ownership posting differs"))?;
        local_ordinals.push(*local);
        *local = local
            .checked_add(1)
            .ok_or_else(|| invalid("V37 ownership local ordinal overflows"))?;
    }
    if local_counts
        .iter()
        .map(|count| u64::from(*count))
        .ne(leaf_populations.iter().copied())
    {
        return Err(invalid("V37 ownership populations differ"));
    }
    let schema = Arc::new(v37_ownership_schema());
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(UInt64Array::from(
                assignments
                    .iter()
                    .map(|assignment| assignment.source_ordinal)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(feature_row_ids.to_vec())),
            Arc::new(UInt32Array::from(
                assignments
                    .iter()
                    .map(|assignment| assignment.posting_ordinal)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt32Array::from(local_ordinals)),
        ],
    )?;
    let properties = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .build();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(properties))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(V37EncodedOwnership {
        encoded_bytes: bytes.len() as u64,
        sha256: format!("{:x}", Sha256::digest(&bytes)),
        blake3: blake3::hash(&bytes).to_hex().to_string(),
        bytes,
    })
}

pub(crate) fn decode_v37_ownership_parquet(
    bytes: &[u8],
    encoded_bytes: u64,
    sha256: &str,
    blake3: &str,
    leaf_populations: &[u64],
) -> Result<Vec<V37OwnershipRecord>> {
    if encoded_bytes != bytes.len() as u64
        || !valid_lower_hex_digest(sha256)
        || !valid_lower_hex_digest(blake3)
        || format!("{:x}", Sha256::digest(bytes)) != sha256
        || blake3::hash(bytes).to_hex().as_str() != blake3
        || leaf_populations.is_empty()
    {
        return Err(invalid("V37 ownership artifact bytes differ"));
    }
    let parquet_bytes = Bytes::copy_from_slice(bytes);
    crate::v36_prefix_dataset::validate_v36_prefix_parquet_footer(&parquet_bytes)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(parquet_bytes.clone())?;
    if builder.schema().as_ref() != &v37_ownership_schema()
        || builder.metadata().num_row_groups() != 1
    {
        return Err(invalid("V37 ownership Parquet schema differs"));
    }
    let expected_rows = leaf_populations
        .iter()
        .try_fold(0_usize, |sum, population| {
            let population = usize::try_from(*population).ok()?;
            sum.checked_add(population)
        });
    let expected_rows =
        expected_rows.ok_or_else(|| invalid("V37 ownership row count overflows"))?;
    let row_groups = builder
        .metadata()
        .row_groups()
        .iter()
        .map(|group| {
            let compressed = group.columns().iter().try_fold(0_i64, |total, column| {
                total.checked_add(column.compressed_size())
            });
            let uncompressed = group.columns().iter().try_fold(0_i64, |total, column| {
                total.checked_add(column.uncompressed_size())
            });
            compressed
                .zip(uncompressed)
                .map(|(compressed, uncompressed)| (group.num_rows(), compressed, uncompressed))
                .ok_or_else(|| invalid("V37 ownership decoder size overflows"))
        })
        .collect::<Result<Vec<_>>>()?;
    crate::v36_prefix_dataset::validate_v36_prefix_source_decoder_bounds(
        &row_groups,
        expected_rows,
        crate::v36_prefix_dataset::SOURCE_DECODER_COMPRESSED_CAP_BYTES,
        crate::v36_prefix_dataset::SOURCE_DECODER_UNCOMPRESSED_CAP_BYTES,
    )?;
    crate::v36_prefix_dataset::validate_v36_parquet_pages(
        &parquet_bytes,
        builder.metadata(),
        &[(1, 8), (1, 8), (1, 4), (1, 4)],
    )?;
    let observed_rows = usize::try_from(builder.metadata().file_metadata().num_rows())
        .map_err(|_| invalid("V37 ownership row count differs"))?;
    if observed_rows != expected_rows {
        return Err(invalid("V37 ownership row count differs"));
    }
    let mut assignments = Vec::new();
    assignments
        .try_reserve_exact(expected_rows)
        .map_err(|_| invalid("V37 ownership allocation exceeds capacity"))?;
    let mut local_counts = vec![0_u32; leaf_populations.len()];
    let mut unique_feature_ids = BTreeSet::new();
    for batch in builder.build()? {
        let batch = batch?;
        if batch.num_columns() != 4
            || batch
                .columns()
                .iter()
                .any(|column| column.null_count() != 0)
        {
            return Err(invalid("V37 ownership Parquet batch differs"));
        }
        let sources = column::<UInt64Array>(&batch, 0, "V37 ownership source differs")?;
        let feature_ids = column::<UInt64Array>(&batch, 1, "V37 ownership feature ID differs")?;
        let postings = column::<UInt32Array>(&batch, 2, "V37 ownership posting differs")?;
        let locals = column::<UInt32Array>(&batch, 3, "V37 ownership local differs")?;
        for row in 0..batch.num_rows() {
            let source_ordinal = sources.value(row);
            let feature_row_id = feature_ids.value(row);
            let posting_ordinal = postings.value(row);
            let posting = posting_ordinal as usize;
            if source_ordinal != assignments.len() as u64
                || !unique_feature_ids.insert(feature_row_id)
                || posting >= local_counts.len()
                || locals.value(row) != local_counts[posting]
            {
                return Err(invalid("V37 ownership row authority differs"));
            }
            local_counts[posting] = local_counts[posting]
                .checked_add(1)
                .ok_or_else(|| invalid("V37 ownership local ordinal overflows"))?;
            assignments.push(V37OwnershipRecord {
                source_ordinal,
                feature_row_id,
                posting_ordinal,
                posting_local_ordinal: locals.value(row),
            });
        }
    }
    if assignments.len() != expected_rows
        || local_counts
            .iter()
            .map(|count| u64::from(*count))
            .ne(leaf_populations.iter().copied())
    {
        return Err(invalid("V37 ownership populations differ"));
    }
    Ok(assignments)
}

fn validate_v37_ceiling(result: &V37LayoutCeiling) -> Result<()> {
    if result.schema != "borsuk-v37-layout-ceiling-v1"
        || result.claim_eligible
        || result.gt_neighbors != V37_GT_NEIGHBORS
        || result.selected_postings_limit != V37_SELECTED_POSTINGS as u32
        || result.aggregate_gate_ppm != V37_AGGREGATE_RECALL_GATE_PPM
        || result.minimum_gate_ppm != V37_MINIMUM_RECALL_GATE_PPM
        || result.samples.is_empty()
    {
        return Err(invalid("V37 ceiling authority differs"));
    }
    let mut total_hits = 0_u64;
    let mut minimum_recall_ppm = u32::MAX;
    let mut previous_query = None;
    for sample in &result.samples {
        let unique = sample
            .selected_postings
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let expected_recall = sample
            .hits
            .checked_mul(1_000_000 / V37_GT_NEIGHBORS)
            .ok_or_else(|| invalid("V37 ceiling recall overflows"))?;
        if previous_query.is_some_and(|query| sample.query_ordinal <= query)
            || sample.selected_postings.is_empty()
            || sample.selected_postings.len() > V37_SELECTED_POSTINGS as usize
            || unique.len() != sample.selected_postings.len()
            || sample.hits > V37_GT_NEIGHBORS
            || sample.recall_ppm != expected_recall
        {
            return Err(invalid("V37 ceiling sample differs"));
        }
        previous_query = Some(sample.query_ordinal);
        total_hits = total_hits
            .checked_add(u64::from(sample.hits))
            .ok_or_else(|| invalid("V37 ceiling hit total overflows"))?;
        minimum_recall_ppm = minimum_recall_ppm.min(sample.recall_ppm);
    }
    let denominator = u64::try_from(result.samples.len())
        .ok()
        .and_then(|queries| queries.checked_mul(u64::from(V37_GT_NEIGHBORS)))
        .ok_or_else(|| invalid("V37 ceiling denominator overflows"))?;
    let aggregate_recall_ppm = u32::try_from(
        total_hits
            .checked_mul(1_000_000)
            .ok_or_else(|| invalid("V37 ceiling aggregate overflows"))?
            / denominator,
    )
    .map_err(|_| invalid("V37 ceiling aggregate exceeds ppm range"))?;
    let passed = aggregate_recall_ppm >= V37_AGGREGATE_RECALL_GATE_PPM
        && minimum_recall_ppm >= V37_MINIMUM_RECALL_GATE_PPM;
    let disposition = if passed {
        "ceiling-passed"
    } else {
        "layout-rejected"
    };
    if result.aggregate_recall_ppm != aggregate_recall_ppm
        || result.minimum_recall_ppm != minimum_recall_ppm
        || result.passed != passed
        || result.disposition != disposition
    {
        return Err(invalid("V37 ceiling aggregate differs"));
    }
    Ok(())
}

pub(crate) fn evaluate_v37_unique_owner_ceiling(
    assignments: &[V37OwnershipAssignment],
    truth: &[V37GroundTruth],
    selected_postings_limit: usize,
) -> Result<V37LayoutCeiling> {
    if assignments.is_empty()
        || truth.is_empty()
        || selected_postings_limit != V37_SELECTED_POSTINGS as usize
    {
        return Err(invalid("V37 ceiling input authority differs"));
    }
    let mut ownership = BTreeMap::new();
    let mut previous_source = None;
    for assignment in assignments {
        if previous_source.is_some_and(|source| assignment.source_ordinal <= source)
            || ownership
                .insert(assignment.source_ordinal, assignment.posting_ordinal)
                .is_some()
        {
            return Err(invalid("V37 ceiling ownership differs"));
        }
        previous_source = Some(assignment.source_ordinal);
    }
    let mut samples = Vec::with_capacity(truth.len());
    let mut previous_query = None;
    for query in truth {
        if previous_query.is_some_and(|ordinal| query.query_ordinal <= ordinal)
            || query.source_ordinals.len() != V37_GT_NEIGHBORS as usize
        {
            return Err(invalid("V37 ceiling truth shape differs"));
        }
        previous_query = Some(query.query_ordinal);
        let mut seen = BTreeSet::new();
        let mut counts = BTreeMap::<u32, u32>::new();
        for source_ordinal in &query.source_ordinals {
            if !seen.insert(*source_ordinal) {
                return Err(invalid("V37 ceiling truth row is duplicated"));
            }
            let posting = ownership
                .get(source_ordinal)
                .ok_or_else(|| invalid("V37 ceiling truth row is unknown"))?;
            let count = counts.entry(*posting).or_default();
            *count = count
                .checked_add(1)
                .ok_or_else(|| invalid("V37 ceiling posting count overflows"))?;
        }
        let mut ranked = counts.into_iter().collect::<Vec<_>>();
        ranked.sort_unstable_by(|left, right| {
            right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0))
        });
        ranked.truncate(selected_postings_limit);
        let hits = ranked
            .iter()
            .try_fold(0_u32, |sum, (_, count)| sum.checked_add(*count));
        let hits = hits.ok_or_else(|| invalid("V37 ceiling hits overflow"))?;
        samples.push(V37CeilingSample {
            query_ordinal: query.query_ordinal,
            selected_postings: ranked.iter().map(|(posting, _)| *posting).collect(),
            hits,
            recall_ppm: hits * (1_000_000 / V37_GT_NEIGHBORS),
        });
    }
    let total_hits = samples
        .iter()
        .try_fold(0_u64, |sum, sample| sum.checked_add(u64::from(sample.hits)));
    let total_hits = total_hits.ok_or_else(|| invalid("V37 ceiling hit total overflows"))?;
    let denominator = u64::try_from(samples.len())
        .ok()
        .and_then(|queries| queries.checked_mul(u64::from(V37_GT_NEIGHBORS)))
        .ok_or_else(|| invalid("V37 ceiling denominator overflows"))?;
    let aggregate_recall_ppm = u32::try_from(
        total_hits
            .checked_mul(1_000_000)
            .ok_or_else(|| invalid("V37 ceiling aggregate overflows"))?
            / denominator,
    )
    .map_err(|_| invalid("V37 ceiling aggregate exceeds ppm range"))?;
    let minimum_recall_ppm = samples
        .iter()
        .map(|sample| sample.recall_ppm)
        .min()
        .ok_or_else(|| invalid("V37 ceiling sample is absent"))?;
    let passed = aggregate_recall_ppm >= V37_AGGREGATE_RECALL_GATE_PPM
        && minimum_recall_ppm >= V37_MINIMUM_RECALL_GATE_PPM;
    let result = V37LayoutCeiling {
        schema: "borsuk-v37-layout-ceiling-v1".to_owned(),
        claim_eligible: false,
        gt_neighbors: V37_GT_NEIGHBORS,
        selected_postings_limit: V37_SELECTED_POSTINGS as u32,
        aggregate_gate_ppm: V37_AGGREGATE_RECALL_GATE_PPM,
        minimum_gate_ppm: V37_MINIMUM_RECALL_GATE_PPM,
        samples,
        aggregate_recall_ppm,
        minimum_recall_ppm,
        passed,
        disposition: if passed {
            "ceiling-passed"
        } else {
            "layout-rejected"
        }
        .to_owned(),
    };
    validate_v37_ceiling(&result)?;
    Ok(result)
}

pub(crate) fn canonical_v37_ceiling_bytes(result: &V37LayoutCeiling) -> Result<Vec<u8>> {
    validate_v37_ceiling(result)?;
    let value = serde_json::to_value(result)
        .map_err(|error| invalid(&format!("V37 ceiling serialization failed: {error}")))?;
    let mut bytes = serde_json::to_vec(&canonical_json_value(value))
        .map_err(|error| invalid(&format!("V37 ceiling serialization failed: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn validate_v37_direct_result(result: &V37DirectRoutingResult) -> Result<()> {
    if result.schema != "borsuk-v37-direct-routing-v1"
        || result.claim_eligible
        || result.selected_postings_limit != V37_SELECTED_POSTINGS as u32
        || result.aggregate_gate_ppm != V37_AGGREGATE_RECALL_GATE_PPM
        || result.minimum_gate_ppm != V37_MINIMUM_RECALL_GATE_PPM
        || !matches!(
            result.fma_backend.as_str(),
            "aarch64-neon-fma" | "x86-avx-fma"
        )
        || result.samples.is_empty()
    {
        return Err(invalid("V37 direct result authority differs"));
    }
    let mut total_hits = 0_u64;
    let mut minimum_recall_ppm = u32::MAX;
    let mut previous_query = None;
    for sample in &result.samples {
        let unique = sample
            .selected_postings
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let expected_recall = sample
            .hits
            .checked_mul(1_000_000 / V37_GT_NEIGHBORS)
            .ok_or_else(|| invalid("V37 direct recall overflows"))?;
        if previous_query.is_some_and(|query| sample.query_ordinal <= query)
            || sample.selected_postings.len() != V37_SELECTED_POSTINGS as usize
            || unique.len() != sample.selected_postings.len()
            || sample.node_visits == 0
            || u64::from(sample.node_visits) > V37_MAXIMUM_DIRECT_NODE_VISITS
            || sample.scored_internal_nodes == 0
            || sample
                .scored_internal_nodes
                .checked_add(V37_SELECTED_POSTINGS as u32)
                != Some(sample.node_visits)
            || sample.scored_internal_nodes > sample.node_visits
            || sample.hits > V37_GT_NEIGHBORS
            || sample.ceiling_hits > V37_GT_NEIGHBORS
            || sample.hits > sample.ceiling_hits
            || sample.ceiling_gap_hits != sample.ceiling_hits - sample.hits
            || sample.recall_ppm != expected_recall
        {
            return Err(invalid("V37 direct sample differs"));
        }
        previous_query = Some(sample.query_ordinal);
        total_hits = total_hits
            .checked_add(u64::from(sample.hits))
            .ok_or_else(|| invalid("V37 direct hit total overflows"))?;
        minimum_recall_ppm = minimum_recall_ppm.min(sample.recall_ppm);
    }
    let denominator = u64::try_from(result.samples.len())
        .ok()
        .and_then(|queries| queries.checked_mul(u64::from(V37_GT_NEIGHBORS)))
        .ok_or_else(|| invalid("V37 direct denominator overflows"))?;
    let aggregate_recall_ppm = u32::try_from(
        total_hits
            .checked_mul(1_000_000)
            .ok_or_else(|| invalid("V37 direct aggregate overflows"))?
            / denominator,
    )
    .map_err(|_| invalid("V37 direct aggregate exceeds ppm range"))?;
    let passed = aggregate_recall_ppm >= V37_AGGREGATE_RECALL_GATE_PPM
        && minimum_recall_ppm >= V37_MINIMUM_RECALL_GATE_PPM;
    let disposition = if passed {
        "direct-passed"
    } else {
        "direct-failed"
    };
    if result.aggregate_recall_ppm != aggregate_recall_ppm
        || result.minimum_recall_ppm != minimum_recall_ppm
        || result.passed != passed
        || result.disposition != disposition
    {
        return Err(invalid("V37 direct aggregate differs"));
    }
    Ok(())
}

pub(crate) fn evaluate_v37_direct_recall(
    assignments: &[V37OwnershipAssignment],
    truth: &[V37GroundTruth],
    ceiling: &V37LayoutCeiling,
    selections: &[V37DirectSelectionRecord],
) -> Result<V37DirectRoutingResult> {
    validate_v37_ceiling(ceiling)?;
    if truth.len() != selections.len() || truth.len() != ceiling.samples.len() {
        return Err(invalid("V37 direct evidence cardinality differs"));
    }
    if evaluate_v37_unique_owner_ceiling(assignments, truth, V37_SELECTED_POSTINGS as usize)?
        != *ceiling
    {
        return Err(invalid("V37 direct ceiling binding differs"));
    }
    let mut ownership = BTreeMap::new();
    let mut previous_source = None;
    for assignment in assignments {
        if previous_source.is_some_and(|source| assignment.source_ordinal <= source)
            || ownership
                .insert(assignment.source_ordinal, assignment.posting_ordinal)
                .is_some()
        {
            return Err(invalid("V37 direct ownership differs"));
        }
        previous_source = Some(assignment.source_ordinal);
    }
    let backend = selections
        .first()
        .map(|record| record.selection.fma_backend.clone())
        .ok_or_else(|| invalid("V37 direct selection is absent"))?;
    let mut samples = Vec::with_capacity(truth.len());
    for ((query, record), ceiling_sample) in truth.iter().zip(selections).zip(&ceiling.samples) {
        if query.query_ordinal != record.query_ordinal
            || query.query_ordinal != ceiling_sample.query_ordinal
            || query.source_ordinals.len() != V37_GT_NEIGHBORS as usize
            || record.selection.fma_backend != backend
        {
            return Err(invalid("V37 direct query binding differs"));
        }
        let selected = record
            .selection
            .selected_postings
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        if selected.len() != V37_SELECTED_POSTINGS as usize {
            return Err(invalid("V37 direct selected postings differ"));
        }
        let mut seen_truth = BTreeSet::new();
        let mut hits = 0_u32;
        for source_ordinal in &query.source_ordinals {
            if !seen_truth.insert(*source_ordinal) {
                return Err(invalid("V37 direct truth row is duplicated"));
            }
            let posting = ownership
                .get(source_ordinal)
                .ok_or_else(|| invalid("V37 direct truth row is unknown"))?;
            if selected.contains(posting) {
                hits = hits
                    .checked_add(1)
                    .ok_or_else(|| invalid("V37 direct hits overflow"))?;
            }
        }
        if hits > ceiling_sample.hits {
            return Err(invalid("V37 direct hits exceed layout ceiling"));
        }
        samples.push(V37DirectSample {
            query_ordinal: query.query_ordinal,
            selected_postings: record.selection.selected_postings.clone(),
            node_visits: record.selection.node_visits,
            scored_internal_nodes: record.selection.scored_internal_nodes,
            hits,
            recall_ppm: hits * (1_000_000 / V37_GT_NEIGHBORS),
            ceiling_hits: ceiling_sample.hits,
            ceiling_gap_hits: ceiling_sample.hits - hits,
        });
    }
    let total_hits = samples
        .iter()
        .try_fold(0_u64, |sum, sample| sum.checked_add(u64::from(sample.hits)));
    let total_hits = total_hits.ok_or_else(|| invalid("V37 direct hit total overflows"))?;
    let denominator = u64::try_from(samples.len())
        .ok()
        .and_then(|queries| queries.checked_mul(u64::from(V37_GT_NEIGHBORS)))
        .ok_or_else(|| invalid("V37 direct denominator overflows"))?;
    let aggregate_recall_ppm = u32::try_from(
        total_hits
            .checked_mul(1_000_000)
            .ok_or_else(|| invalid("V37 direct aggregate overflows"))?
            / denominator,
    )
    .map_err(|_| invalid("V37 direct aggregate exceeds ppm range"))?;
    let minimum_recall_ppm = samples
        .iter()
        .map(|sample| sample.recall_ppm)
        .min()
        .ok_or_else(|| invalid("V37 direct sample is absent"))?;
    let passed = aggregate_recall_ppm >= V37_AGGREGATE_RECALL_GATE_PPM
        && minimum_recall_ppm >= V37_MINIMUM_RECALL_GATE_PPM;
    let result = V37DirectRoutingResult {
        schema: "borsuk-v37-direct-routing-v1".to_owned(),
        claim_eligible: false,
        selected_postings_limit: V37_SELECTED_POSTINGS as u32,
        aggregate_gate_ppm: V37_AGGREGATE_RECALL_GATE_PPM,
        minimum_gate_ppm: V37_MINIMUM_RECALL_GATE_PPM,
        fma_backend: backend,
        samples,
        aggregate_recall_ppm,
        minimum_recall_ppm,
        passed,
        disposition: if passed {
            "direct-passed"
        } else {
            "direct-failed"
        }
        .to_owned(),
    };
    validate_v37_direct_result(&result)?;
    Ok(result)
}

pub(crate) fn canonical_v37_direct_bytes(result: &V37DirectRoutingResult) -> Result<Vec<u8>> {
    validate_v37_direct_result(result)?;
    let value = serde_json::to_value(result)
        .map_err(|error| invalid(&format!("V37 direct serialization failed: {error}")))?;
    let mut bytes = serde_json::to_vec(&canonical_json_value(value))
        .map_err(|error| invalid(&format!("V37 direct serialization failed: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn v37_mass_q24(count: u64, population: u64) -> Result<u32> {
    if count == 0 || count > population {
        return Err(invalid("V37 relation mass authority differs"));
    }
    let numerator = u128::from(count)
        .checked_mul(1_u128 << 24)
        .ok_or_else(|| invalid("V37 relation mass overflows"))?;
    let denominator = u128::from(population);
    let quotient = numerator / denominator;
    let remainder = numerator % denominator;
    let doubled_remainder = remainder
        .checked_mul(2)
        .ok_or_else(|| invalid("V37 relation rounding overflows"))?;
    let rounded = quotient
        .checked_add(u128::from(
            doubled_remainder > denominator
                || (doubled_remainder == denominator && quotient % 2 == 1),
        ))
        .ok_or_else(|| invalid("V37 relation rounding overflows"))?;
    u32::try_from(rounded).map_err(|_| invalid("V37 relation mass exceeds Q24"))
}

pub(crate) fn build_v37_relation_plane(
    relation_memberships: &[V37OwnershipAssignment],
    ownership: &[V37OwnershipAssignment],
    relation_leaf_count: u32,
    posting_count: u32,
    prefix_lengths: &[u32],
) -> Result<V37RelationPlane> {
    if relation_leaf_count == 0
        || posting_count < V37_SELECTED_POSTINGS as u32
        || prefix_lengths != [16, 32, 64]
        || relation_memberships.len() != ownership.len()
        || relation_memberships.is_empty()
    {
        return Err(invalid("V37 relation plane authority differs"));
    }
    let leaf_count = usize::try_from(relation_leaf_count)
        .map_err(|_| invalid("V37 relation leaf count exceeds address space"))?;
    let mut populations = vec![0_u64; leaf_count];
    let mut counts = vec![BTreeMap::<u32, u64>::new(); leaf_count];
    let mut relation_memberships = relation_memberships.to_vec();
    let mut ownership = ownership.to_vec();
    relation_memberships.sort_unstable_by_key(|row| row.source_ordinal);
    ownership.sort_unstable_by_key(|row| row.source_ordinal);
    let mut previous_source = None;
    for (membership, owner) in relation_memberships.iter().zip(&ownership) {
        if membership.source_ordinal != owner.source_ordinal
            || previous_source.is_some_and(|source| membership.source_ordinal <= source)
            || membership.posting_ordinal >= relation_leaf_count
            || owner.posting_ordinal >= posting_count
        {
            return Err(invalid("V37 relation row binding differs"));
        }
        previous_source = Some(membership.source_ordinal);
        let leaf = membership.posting_ordinal as usize;
        populations[leaf] = populations[leaf]
            .checked_add(1)
            .ok_or_else(|| invalid("V37 relation population overflows"))?;
        let count = counts[leaf].entry(owner.posting_ordinal).or_default();
        *count = count
            .checked_add(1)
            .ok_or_else(|| invalid("V37 relation count overflows"))?;
    }
    let leaves = populations
        .into_iter()
        .zip(counts)
        .map(|(population, counts)| {
            if population == 0 {
                return Err(invalid("V37 relation leaf is empty"));
            }
            let mut ranked = counts.into_iter().collect::<Vec<_>>();
            ranked.sort_unstable_by(|left, right| {
                right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0))
            });
            let records = ranked
                .into_iter()
                .map(|(posting_ordinal, count)| {
                    Ok(V37RelationRecord {
                        posting_ordinal,
                        count,
                        mass_q24: v37_mass_q24(count, population)?,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(V37RelationLeaf {
                population,
                records,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(V37RelationPlane {
        posting_count,
        prefix_lengths: prefix_lengths.to_vec(),
        leaves,
    })
}

pub(crate) fn reduce_v37_relation_postings(
    plane: &V37RelationPlane,
    probed_leaves: &[u32],
    prefix_length: u32,
    primary_posting: u32,
) -> Result<V37RelationSelection> {
    if probed_leaves.is_empty()
        || probed_leaves.len() > 32
        || !plane.prefix_lengths.contains(&prefix_length)
        || primary_posting >= plane.posting_count
    {
        return Err(invalid("V37 relation selection authority differs"));
    }
    let mut votes = BTreeMap::<u32, (u64, u32)>::new();
    let mut seen_leaves = BTreeSet::new();
    let mut touched_records = 0_u32;
    for (rank, leaf_ordinal) in probed_leaves.iter().copied().enumerate() {
        if !seen_leaves.insert(leaf_ordinal) {
            return Err(invalid("V37 relation leaf is duplicated"));
        }
        let leaf = plane
            .leaves
            .get(leaf_ordinal as usize)
            .ok_or_else(|| invalid("V37 relation leaf differs"))?;
        let weight = 32_u64
            .checked_sub(u64::try_from(rank).map_err(|_| invalid("V37 rank overflows"))?)
            .ok_or_else(|| invalid("V37 relation rank underflows"))?;
        for record in leaf.records.iter().take(prefix_length as usize) {
            touched_records = touched_records
                .checked_add(1)
                .ok_or_else(|| invalid("V37 touched record count overflows"))?;
            let contribution = weight
                .checked_mul(u64::from(record.mass_q24))
                .ok_or_else(|| invalid("V37 relation vote overflows"))?;
            let candidate = votes
                .entry(record.posting_ordinal)
                .or_insert((0, rank as u32));
            candidate.0 = candidate
                .0
                .checked_add(contribution)
                .ok_or_else(|| invalid("V37 relation vote overflows"))?;
            candidate.1 = candidate.1.min(rank as u32);
        }
    }
    votes.entry(primary_posting).or_insert((0, u32::MAX));
    if votes.len() < V37_SELECTED_POSTINGS as usize {
        return Err(invalid("V37 relation candidate shortage"));
    }
    let mut candidates = votes.into_iter().collect::<Vec<_>>();
    candidates.sort_unstable_by(|left, right| {
        right
            .1
            .0
            .cmp(&left.1.0)
            .then_with(|| left.1.1.cmp(&right.1.1))
            .then_with(|| left.0.cmp(&right.0))
    });
    Ok(V37RelationSelection {
        selected_postings: candidates
            .into_iter()
            .take(V37_SELECTED_POSTINGS as usize)
            .map(|(posting, _)| posting)
            .collect(),
        touched_records,
    })
}

pub(crate) fn select_v37_relation_postings(
    router: &V37RelationRouter<'_>,
    prefixes: &V37ValidatedRelationPrefixes<'_>,
    query: &[f32],
    leaf_probes: u32,
    prefix_length: u32,
    primary_posting: u32,
) -> Result<V37RoutedRelationSelection> {
    let probes = usize::try_from(leaf_probes)
        .map_err(|_| invalid("V37 relation probe count exceeds address space"))?;
    if !matches!(leaf_probes, 8 | 16 | 32)
        || probes > router.tree.leaf_populations.len()
        || prefixes.prefixes.leaf_offsets.first().map(Vec::len)
            != Some(router.tree.leaf_populations.len() + 1)
    {
        return Err(invalid("V37 relation probe authority differs"));
    }
    let routing = select_v37_tree_postings(
        router.tree,
        router.kernel,
        router.maximum_node_visits,
        probes,
        query,
    )?;
    let votes = reduce_v37_relation_prefixes(
        prefixes.prefixes,
        &routing.selected_postings,
        prefix_length,
        primary_posting,
    )?;
    Ok(V37RoutedRelationSelection { routing, votes })
}

pub(crate) fn validate_v37_relation_prefixes(prefixes: &V37RelationPrefixes) -> Result<()> {
    if prefixes.posting_count < V37_SELECTED_POSTINGS as u32
        || prefixes.prefix_lengths != [16, 32, 64]
        || prefixes.leaf_offsets.len() != 3
        || prefixes.posting_ordinals.len() != 3
        || prefixes.masses_q24.len() != 3
    {
        return Err(invalid("V37 relation prefix authority differs"));
    }
    let leaf_count = prefixes.leaf_offsets[0]
        .len()
        .checked_sub(1)
        .ok_or_else(|| invalid("V37 relation prefix leaf count differs"))?;
    if leaf_count == 0 {
        return Err(invalid("V37 relation prefix leaf count differs"));
    }
    for row in 0..3 {
        let offsets = &prefixes.leaf_offsets[row];
        let postings = &prefixes.posting_ordinals[row];
        let masses = &prefixes.masses_q24[row];
        if offsets.len() != leaf_count + 1
            || offsets.first() != Some(&0)
            || offsets.windows(2).any(|pair| pair[0] > pair[1])
            || offsets.last().copied() != Some(postings.len() as u64)
            || postings.len() != masses.len()
        {
            return Err(invalid("V37 relation prefix extent differs"));
        }
        for leaf in 0..leaf_count {
            let start = usize::try_from(offsets[leaf])
                .map_err(|_| invalid("V37 relation prefix offset exceeds address space"))?;
            let end = usize::try_from(offsets[leaf + 1])
                .map_err(|_| invalid("V37 relation prefix offset exceeds address space"))?;
            if end - start > prefixes.prefix_lengths[row] as usize
                || postings[start..end]
                    .iter()
                    .any(|posting| *posting >= prefixes.posting_count)
                || postings[start..end]
                    .iter()
                    .copied()
                    .collect::<BTreeSet<_>>()
                    .len()
                    != end - start
                || masses[start..end].iter().any(|mass| *mass > 1 << 24)
                || masses[start..end].windows(2).any(|pair| pair[0] < pair[1])
            {
                return Err(invalid("V37 relation prefix record differs"));
            }
        }
    }
    for shorter in 0..2 {
        for leaf in 0..leaf_count {
            let short_start = prefixes.leaf_offsets[shorter][leaf];
            let short_end = prefixes.leaf_offsets[shorter][leaf + 1];
            let long_start = prefixes.leaf_offsets[shorter + 1][leaf];
            let long_end = prefixes.leaf_offsets[shorter + 1][leaf + 1];
            let short_start = short_start as usize;
            let short_end = short_end as usize;
            let long_start = long_start as usize;
            let long_end = long_end as usize;
            let short_len = short_end - short_start;
            let long_len = long_end - long_start;
            if short_len > long_len
                || prefixes.posting_ordinals[shorter][short_start..short_end]
                    != prefixes.posting_ordinals[shorter + 1][long_start..long_start + short_len]
                || prefixes.masses_q24[shorter][short_start..short_end]
                    != prefixes.masses_q24[shorter + 1][long_start..long_start + short_len]
                || (short_len < prefixes.prefix_lengths[shorter] as usize && short_len != long_len)
            {
                return Err(invalid("V37 relation prefix nesting differs"));
            }
        }
    }
    Ok(())
}

pub(crate) fn prepare_v37_relation_prefixes(
    prefixes: &V37RelationPrefixes,
) -> Result<V37ValidatedRelationPrefixes<'_>> {
    validate_v37_relation_prefixes(prefixes)?;
    Ok(V37ValidatedRelationPrefixes { prefixes })
}

fn reduce_v37_relation_prefixes(
    prefixes: &V37RelationPrefixes,
    probed_leaves: &[u32],
    prefix_length: u32,
    primary_posting: u32,
) -> Result<V37RelationSelection> {
    let row = prefixes
        .prefix_lengths
        .iter()
        .position(|value| *value == prefix_length)
        .ok_or_else(|| invalid("V37 relation prefix length differs"))?;
    if probed_leaves.is_empty()
        || probed_leaves.len() > 32
        || primary_posting >= prefixes.posting_count
    {
        return Err(invalid("V37 relation selection authority differs"));
    }
    let mut votes = BTreeMap::<u32, (u64, u32)>::new();
    let mut seen_leaves = BTreeSet::new();
    let mut touched_records = 0_u32;
    for (rank, leaf) in probed_leaves.iter().copied().enumerate() {
        if !seen_leaves.insert(leaf) {
            return Err(invalid("V37 relation leaf is duplicated"));
        }
        let leaf = leaf as usize;
        let start = *prefixes.leaf_offsets[row]
            .get(leaf)
            .ok_or_else(|| invalid("V37 relation leaf differs"))? as usize;
        let end = *prefixes.leaf_offsets[row]
            .get(leaf + 1)
            .ok_or_else(|| invalid("V37 relation leaf differs"))? as usize;
        let weight = 32_u64 - rank as u64;
        for index in start..end {
            touched_records = touched_records
                .checked_add(1)
                .ok_or_else(|| invalid("V37 touched record count overflows"))?;
            let posting = prefixes.posting_ordinals[row][index];
            let contribution = weight
                .checked_mul(u64::from(prefixes.masses_q24[row][index]))
                .ok_or_else(|| invalid("V37 relation vote overflows"))?;
            let candidate = votes.entry(posting).or_insert((0, rank as u32));
            candidate.0 = candidate
                .0
                .checked_add(contribution)
                .ok_or_else(|| invalid("V37 relation vote overflows"))?;
            candidate.1 = candidate.1.min(rank as u32);
        }
    }
    votes.entry(primary_posting).or_insert((0, u32::MAX));
    if votes.len() < V37_SELECTED_POSTINGS as usize {
        return Err(invalid("V37 relation candidate shortage"));
    }
    let mut candidates = votes.into_iter().collect::<Vec<_>>();
    candidates.sort_unstable_by(|left, right| {
        right
            .1
            .0
            .cmp(&left.1.0)
            .then_with(|| left.1.1.cmp(&right.1.1))
            .then_with(|| left.0.cmp(&right.0))
    });
    Ok(V37RelationSelection {
        selected_postings: candidates
            .into_iter()
            .take(V37_SELECTED_POSTINGS as usize)
            .map(|(posting, _)| posting)
            .collect(),
        touched_records,
    })
}

fn validate_v37_relation_plane(plane: &V37RelationPlane) -> Result<()> {
    if plane.posting_count < V37_SELECTED_POSTINGS as u32
        || plane.prefix_lengths != [16, 32, 64]
        || plane.leaves.is_empty()
    {
        return Err(invalid("V37 relation plane authority differs"));
    }
    for leaf in &plane.leaves {
        let mut previous = None;
        let mut seen_postings = BTreeSet::new();
        let mut total = 0_u64;
        for record in &leaf.records {
            if record.posting_ordinal >= plane.posting_count
                || !seen_postings.insert(record.posting_ordinal)
                || record.count == 0
                || record.mass_q24 != v37_mass_q24(record.count, leaf.population)?
                || previous.is_some_and(|(count, posting)| {
                    record.count > count
                        || (record.count == count && record.posting_ordinal <= posting)
                })
            {
                return Err(invalid("V37 relation record authority differs"));
            }
            previous = Some((record.count, record.posting_ordinal));
            total = total
                .checked_add(record.count)
                .ok_or_else(|| invalid("V37 relation population overflows"))?;
        }
        if leaf.population == 0 || leaf.records.is_empty() || total != leaf.population {
            return Err(invalid("V37 relation leaf authority differs"));
        }
    }
    Ok(())
}

fn v37_relation_counts_schema() -> Schema {
    Schema::new(vec![
        Field::new("relation_leaf_ordinal", DataType::UInt32, false),
        Field::new("rank", DataType::UInt32, false),
        Field::new("posting_ordinal", DataType::UInt32, false),
        Field::new("count", DataType::UInt64, false),
        Field::new("leaf_population", DataType::UInt64, false),
    ])
}

fn encoded_v37_relation_artifact(bytes: Vec<u8>) -> V37EncodedRelationArtifact {
    V37EncodedRelationArtifact {
        encoded_bytes: bytes.len() as u64,
        sha256: format!("{:x}", Sha256::digest(&bytes)),
        blake3: blake3::hash(&bytes).to_hex().to_string(),
        bytes,
    }
}

pub(crate) fn encode_v37_relation_counts_parquet(
    plane: &V37RelationPlane,
) -> Result<V37EncodedRelationArtifact> {
    validate_v37_relation_plane(plane)?;
    let row_count = plane
        .leaves
        .iter()
        .try_fold(0_usize, |sum, leaf| sum.checked_add(leaf.records.len()))
        .ok_or_else(|| invalid("V37 relation row count overflows"))?;
    let mut leaves = Vec::with_capacity(row_count);
    let mut ranks = Vec::with_capacity(row_count);
    let mut postings = Vec::with_capacity(row_count);
    let mut counts = Vec::with_capacity(row_count);
    let mut populations = Vec::with_capacity(row_count);
    for (leaf_ordinal, leaf) in plane.leaves.iter().enumerate() {
        for (rank, record) in leaf.records.iter().enumerate() {
            leaves.push(
                u32::try_from(leaf_ordinal)
                    .map_err(|_| invalid("V37 relation leaf exceeds artifact width"))?,
            );
            ranks.push(
                u32::try_from(rank)
                    .map_err(|_| invalid("V37 relation rank exceeds artifact width"))?,
            );
            postings.push(record.posting_ordinal);
            counts.push(record.count);
            populations.push(leaf.population);
        }
    }
    let schema = Arc::new(v37_relation_counts_schema());
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(UInt32Array::from(leaves)),
            Arc::new(UInt32Array::from(ranks)),
            Arc::new(UInt32Array::from(postings)),
            Arc::new(UInt64Array::from(counts)),
            Arc::new(UInt64Array::from(populations)),
        ],
    )?;
    let properties = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .build();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(properties))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(encoded_v37_relation_artifact(bytes))
}

pub(crate) fn decode_v37_relation_counts_parquet(
    bytes: &[u8],
    encoded_bytes: u64,
    sha256: &str,
    blake3: &str,
    relation_leaf_count: u32,
    posting_count: u32,
    prefix_lengths: &[u32],
) -> Result<V37RelationPlane> {
    if encoded_bytes != bytes.len() as u64
        || format!("{:x}", Sha256::digest(bytes)) != sha256
        || blake3::hash(bytes).to_hex().as_str() != blake3
        || !valid_lower_hex_digest(sha256)
        || !valid_lower_hex_digest(blake3)
        || relation_leaf_count == 0
    {
        return Err(invalid("V37 relation count artifact differs"));
    }
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))?;
    if builder.schema().as_ref() != &v37_relation_counts_schema()
        || builder.metadata().num_row_groups() != 1
    {
        return Err(invalid("V37 relation count schema differs"));
    }
    let leaf_count = relation_leaf_count as usize;
    let mut leaves = vec![
        V37RelationLeaf {
            population: 0,
            records: Vec::new(),
        };
        leaf_count
    ];
    let mut expected_leaf = 0_usize;
    let mut expected_rank = 0_u32;
    for batch in builder.build()? {
        let batch = batch?;
        if batch.num_columns() != 5
            || batch
                .columns()
                .iter()
                .any(|column| column.null_count() != 0)
        {
            return Err(invalid("V37 relation count batch differs"));
        }
        let leaf_ordinals = column::<UInt32Array>(&batch, 0, "V37 relation leaf differs")?;
        let ranks = column::<UInt32Array>(&batch, 1, "V37 relation rank differs")?;
        let postings = column::<UInt32Array>(&batch, 2, "V37 relation posting differs")?;
        let counts = column::<UInt64Array>(&batch, 3, "V37 relation count differs")?;
        let populations = column::<UInt64Array>(&batch, 4, "V37 relation population differs")?;
        for row in 0..batch.num_rows() {
            let leaf = leaf_ordinals.value(row) as usize;
            if leaf >= leaf_count || leaf < expected_leaf || leaf > expected_leaf + 1 {
                return Err(invalid("V37 relation leaf ordering differs"));
            }
            if leaf != expected_leaf {
                if leaves[expected_leaf].records.is_empty() {
                    return Err(invalid("V37 relation leaf is absent"));
                }
                expected_leaf = leaf;
                expected_rank = 0;
            }
            if ranks.value(row) != expected_rank
                || (leaves[leaf].population != 0
                    && leaves[leaf].population != populations.value(row))
            {
                return Err(invalid("V37 relation count ordering differs"));
            }
            leaves[leaf].population = populations.value(row);
            leaves[leaf].records.push(V37RelationRecord {
                posting_ordinal: postings.value(row),
                count: counts.value(row),
                mass_q24: v37_mass_q24(counts.value(row), populations.value(row))?,
            });
            expected_rank = expected_rank
                .checked_add(1)
                .ok_or_else(|| invalid("V37 relation rank overflows"))?;
        }
    }
    let plane = V37RelationPlane {
        posting_count,
        prefix_lengths: prefix_lengths.to_vec(),
        leaves,
    };
    validate_v37_relation_plane(&plane)?;
    Ok(plane)
}

fn v37_list_field(name: &str, data_type: DataType) -> Field {
    Field::new(
        name,
        DataType::List(Arc::new(Field::new("element", data_type, false))),
        false,
    )
}

fn v37_relation_prefix_schema() -> Schema {
    Schema::new(vec![
        Field::new("prefix_length", DataType::UInt32, false),
        Field::new("posting_count", DataType::UInt32, false),
        Field::new("relation_leaf_count", DataType::UInt32, false),
        v37_list_field("leaf_offsets", DataType::UInt64),
        v37_list_field("posting_ordinals", DataType::UInt32),
        v37_list_field("masses_q24", DataType::UInt32),
    ])
}

fn v37_list_offsets(lengths: &[usize]) -> Result<OffsetBuffer<i32>> {
    let mut offsets = Vec::with_capacity(lengths.len() + 1);
    offsets.push(0_i32);
    let mut total = 0_usize;
    for length in lengths {
        total = total
            .checked_add(*length)
            .ok_or_else(|| invalid("V37 relation list length overflows"))?;
        offsets.push(
            i32::try_from(total)
                .map_err(|_| invalid("V37 relation list exceeds Arrow offset width"))?,
        );
    }
    Ok(OffsetBuffer::new(offsets.into()))
}

pub(crate) fn encode_v37_relation_prefixes_arrow(
    plane: &V37RelationPlane,
) -> Result<V37EncodedRelationArtifact> {
    validate_v37_relation_plane(plane)?;
    let mut leaf_offsets = Vec::new();
    let mut postings = Vec::new();
    let mut masses = Vec::new();
    let mut offset_lengths = Vec::new();
    let mut record_lengths = Vec::new();
    for prefix in &plane.prefix_lengths {
        let mut offsets = Vec::with_capacity(plane.leaves.len() + 1);
        offsets.push(0_u64);
        let start = postings.len();
        for leaf in &plane.leaves {
            for record in leaf.records.iter().take(*prefix as usize) {
                postings.push(record.posting_ordinal);
                masses.push(record.mass_q24);
            }
            offsets.push(
                u64::try_from(postings.len() - start)
                    .map_err(|_| invalid("V37 relation prefix length exceeds u64"))?,
            );
        }
        offset_lengths.push(offsets.len());
        record_lengths.push(postings.len() - start);
        leaf_offsets.extend(offsets);
    }
    let u64_child = Arc::new(Field::new("element", DataType::UInt64, false));
    let u32_child = Arc::new(Field::new("element", DataType::UInt32, false));
    let leaf_offsets = ListArray::new(
        Arc::clone(&u64_child),
        v37_list_offsets(&offset_lengths)?,
        Arc::new(UInt64Array::from(leaf_offsets)),
        None,
    );
    let posting_ordinals = ListArray::new(
        Arc::clone(&u32_child),
        v37_list_offsets(&record_lengths)?,
        Arc::new(UInt32Array::from(postings)),
        None,
    );
    let masses_q24 = ListArray::new(
        u32_child,
        v37_list_offsets(&record_lengths)?,
        Arc::new(UInt32Array::from(masses)),
        None,
    );
    let rows = plane.prefix_lengths.len();
    let schema = Arc::new(v37_relation_prefix_schema());
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(UInt32Array::from(plane.prefix_lengths.clone())),
            Arc::new(UInt32Array::from(vec![plane.posting_count; rows])),
            Arc::new(UInt32Array::from(vec![plane.leaves.len() as u32; rows])),
            Arc::new(leaf_offsets),
            Arc::new(posting_ordinals),
            Arc::new(masses_q24),
        ],
    )?;
    let mut bytes = Vec::new();
    let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5)?;
    let mut writer = FileWriter::try_new_with_options(&mut bytes, schema.as_ref(), options)?;
    writer.write(&batch)?;
    writer.finish()?;
    drop(writer);
    Ok(encoded_v37_relation_artifact(bytes))
}

pub(crate) fn decode_v37_relation_prefixes_arrow(
    bytes: &[u8],
    encoded_bytes: u64,
    sha256: &str,
    blake3: &str,
) -> Result<V37RelationPrefixes> {
    if encoded_bytes != bytes.len() as u64
        || format!("{:x}", Sha256::digest(bytes)) != sha256
        || blake3::hash(bytes).to_hex().as_str() != blake3
        || !valid_lower_hex_digest(sha256)
        || !valid_lower_hex_digest(blake3)
    {
        return Err(invalid("V37 relation prefix artifact differs"));
    }
    let mut reader = FileReader::try_new(Cursor::new(bytes), None)?;
    if reader.schema().as_ref() != &v37_relation_prefix_schema() {
        return Err(invalid("V37 relation prefix schema differs"));
    }
    let batch = reader
        .next()
        .transpose()?
        .ok_or_else(|| invalid("V37 relation prefix batch is absent"))?;
    if reader.next().is_some()
        || batch.num_rows() != 3
        || batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
    {
        return Err(invalid("V37 relation prefix batch differs"));
    }
    let prefixes = column::<UInt32Array>(&batch, 0, "V37 prefix lengths differ")?;
    let posting_counts = column::<UInt32Array>(&batch, 1, "V37 posting count differs")?;
    let leaf_counts = column::<UInt32Array>(&batch, 2, "V37 relation leaf count differs")?;
    let offsets = column::<ListArray>(&batch, 3, "V37 leaf offsets differ")?;
    let postings = column::<ListArray>(&batch, 4, "V37 prefix postings differ")?;
    let masses = column::<ListArray>(&batch, 5, "V37 prefix masses differ")?;
    let prefix_lengths = (0..3).map(|row| prefixes.value(row)).collect::<Vec<_>>();
    if prefix_lengths != [16, 32, 64]
        || (1..3).any(|row| posting_counts.value(row) != posting_counts.value(0))
        || (1..3).any(|row| leaf_counts.value(row) != leaf_counts.value(0))
        || posting_counts.value(0) < V37_SELECTED_POSTINGS as u32
        || leaf_counts.value(0) == 0
    {
        return Err(invalid("V37 relation prefix authority differs"));
    }
    let mut leaf_offsets = Vec::with_capacity(3);
    let mut posting_ordinals = Vec::with_capacity(3);
    let mut masses_q24 = Vec::with_capacity(3);
    for row in 0..3 {
        let offset_array = offsets.value(row);
        let posting_array = postings.value(row);
        let mass_array = masses.value(row);
        if offset_array.null_count() != 0
            || posting_array.null_count() != 0
            || mass_array.null_count() != 0
        {
            return Err(invalid("V37 relation prefix child null differs"));
        }
        let offset_values = offset_array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| invalid("V37 relation offset values differ"))?
            .values()
            .to_vec();
        let posting_values = posting_array
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| invalid("V37 relation posting values differ"))?
            .values()
            .to_vec();
        let mass_values = mass_array
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| invalid("V37 relation mass values differ"))?
            .values()
            .to_vec();
        if offset_values.len() != leaf_counts.value(row) as usize + 1
            || offset_values.first() != Some(&0)
            || offset_values.windows(2).any(|pair| pair[0] > pair[1])
            || offset_values.last().copied() != Some(posting_values.len() as u64)
            || posting_values.len() != mass_values.len()
            || posting_values
                .iter()
                .any(|posting| *posting >= posting_counts.value(row))
            || mass_values.iter().any(|mass| *mass > 1 << 24)
        {
            return Err(invalid("V37 relation prefix values differ"));
        }
        leaf_offsets.push(offset_values);
        posting_ordinals.push(posting_values);
        masses_q24.push(mass_values);
    }
    let prefixes = V37RelationPrefixes {
        posting_count: posting_counts.value(0),
        prefix_lengths,
        leaf_offsets,
        posting_ordinals,
        masses_q24,
    };
    validate_v37_relation_prefixes(&prefixes)?;
    Ok(prefixes)
}

fn v37_tree_schema(dimensions: usize) -> Result<Schema> {
    let dimensions =
        i32::try_from(dimensions).map_err(|_| invalid("V37 tree dimensions exceed Arrow width"))?;
    Ok(Schema::new(vec![
        Field::new("record_kind", DataType::UInt8, false),
        Field::new("node_id", DataType::UInt32, false),
        Field::new("source_ordinal", DataType::UInt64, false),
        Field::new("assignment_posting", DataType::UInt32, false),
        Field::new("population", DataType::UInt64, false),
        Field::new("boundary_score_bits", DataType::UInt32, false),
        Field::new("boundary_source_ordinal", DataType::UInt64, false),
        Field::new("left_node", DataType::UInt32, false),
        Field::new("right_node", DataType::UInt32, false),
        Field::new("node_posting", DataType::UInt32, false),
        Field::new(
            "normal",
            DataType::FixedSizeList(
                Arc::new(Field::new("element", DataType::Float32, false)),
                dimensions,
            ),
            false,
        ),
    ]))
}

pub(crate) fn encode_v37_tree_arrow(tree: &V37BalancedTree) -> Result<V37EncodedTree> {
    validate_v37_balanced_tree(tree)?;
    let schema = Arc::new(v37_tree_schema(tree.dimensions)?);
    let row_count = tree
        .nodes
        .len()
        .checked_add(1)
        .ok_or_else(|| invalid("V37 Arrow row count overflows"))?;
    let mut record_kind = Vec::with_capacity(row_count);
    let mut node_id = Vec::with_capacity(row_count);
    let mut source_ordinal = Vec::with_capacity(row_count);
    let mut assignment_posting = Vec::with_capacity(row_count);
    let mut population = Vec::with_capacity(row_count);
    let mut boundary_score_bits = Vec::with_capacity(row_count);
    let mut boundary_source_ordinal = Vec::with_capacity(row_count);
    let mut left_node = Vec::with_capacity(row_count);
    let mut right_node = Vec::with_capacity(row_count);
    let mut node_posting = Vec::with_capacity(row_count);
    let normal_capacity = row_count
        .checked_mul(tree.dimensions)
        .ok_or_else(|| invalid("V37 Arrow normal count overflows"))?;
    let mut normals = Vec::with_capacity(normal_capacity);
    let backend_code = match tree.fma_backend.as_str() {
        "aarch64-neon-fma" => 1,
        "x86-avx-fma" => 2,
        _ => return Err(invalid("V37 tree backend differs")),
    };
    record_kind.push(2);
    node_id.push(
        u32::try_from(tree.dimensions)
            .map_err(|_| invalid("V37 tree dimensions exceed artifact authority"))?,
    );
    source_ordinal.push(tree.seed);
    assignment_posting.push(backend_code);
    population.push(tree.nodes.len() as u64);
    boundary_score_bits.push(1);
    boundary_source_ordinal.push(tree.leaf_populations.len() as u64);
    left_node.push(u32::MAX);
    right_node.push(u32::MAX);
    node_posting.push(u32::MAX);
    normals.resize(tree.dimensions, 0.0);
    for (index, node) in tree.nodes.iter().enumerate() {
        record_kind.push(0);
        node_id.push(index as u32);
        source_ordinal.push(u64::MAX);
        assignment_posting.push(u32::MAX);
        population.push(node.population);
        boundary_score_bits.push(node.boundary_score_bits);
        boundary_source_ordinal.push(node.boundary_source_ordinal);
        left_node.push(node.left_node.unwrap_or(u32::MAX));
        right_node.push(node.right_node.unwrap_or(u32::MAX));
        node_posting.push(node.posting_ordinal.unwrap_or(u32::MAX));
        if node.normal.is_empty() {
            normals.resize(normals.len() + tree.dimensions, 0.0);
        } else {
            normals.extend_from_slice(&node.normal);
        }
    }
    let normal = FixedSizeListArray::try_new(
        Arc::new(Field::new("element", DataType::Float32, false)),
        i32::try_from(tree.dimensions)
            .map_err(|_| invalid("V37 tree dimensions exceed Arrow width"))?,
        Arc::new(Float32Array::from(normals)),
        None,
    )?;
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt8Array::from(record_kind)),
            Arc::new(UInt32Array::from(node_id)),
            Arc::new(UInt64Array::from(source_ordinal)),
            Arc::new(UInt32Array::from(assignment_posting)),
            Arc::new(UInt64Array::from(population)),
            Arc::new(UInt32Array::from(boundary_score_bits)),
            Arc::new(UInt64Array::from(boundary_source_ordinal)),
            Arc::new(UInt32Array::from(left_node)),
            Arc::new(UInt32Array::from(right_node)),
            Arc::new(UInt32Array::from(node_posting)),
            Arc::new(normal),
        ],
    )?;
    let mut bytes = Vec::new();
    let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5)?;
    let mut writer = FileWriter::try_new_with_options(&mut bytes, schema.as_ref(), options)?;
    writer.write(&batch)?;
    writer.finish()?;
    drop(writer);
    Ok(V37EncodedTree {
        encoded_bytes: bytes.len() as u64,
        sha256: format!("{:x}", Sha256::digest(&bytes)),
        blake3: blake3::hash(&bytes).to_hex().to_string(),
        bytes,
    })
}

fn column<'a, T: 'static>(batch: &'a RecordBatch, index: usize, message: &str) -> Result<&'a T> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| invalid(message))
}

pub(crate) fn decode_v37_tree_arrow(
    bytes: &[u8],
    encoded_bytes: u64,
    sha256: &str,
    blake3: &str,
) -> Result<V37BalancedTree> {
    if encoded_bytes != bytes.len() as u64
        || !valid_lower_hex_digest(sha256)
        || !valid_lower_hex_digest(blake3)
        || format!("{:x}", Sha256::digest(bytes)) != sha256
        || blake3::hash(bytes).to_hex().as_str() != blake3
    {
        return Err(invalid("V37 tree artifact bytes differ"));
    }
    let mut reader = FileReader::try_new(Cursor::new(bytes), None)?;
    let observed_schema = reader.schema();
    let normal_field = observed_schema
        .fields()
        .get(10)
        .ok_or_else(|| invalid("V37 tree Arrow schema differs"))?;
    let dimensions = match normal_field.data_type() {
        DataType::FixedSizeList(child, dimensions)
            if child.name() == "element"
                && child.data_type() == &DataType::Float32
                && !child.is_nullable()
                && *dimensions > 0 =>
        {
            *dimensions as usize
        }
        _ => return Err(invalid("V37 tree Arrow normal schema differs")),
    };
    let expected_schema = v37_tree_schema(dimensions)?;
    if observed_schema.as_ref() != &expected_schema {
        return Err(invalid("V37 tree Arrow schema differs"));
    }
    let batch = reader
        .next()
        .transpose()?
        .ok_or_else(|| invalid("V37 tree Arrow batch is absent"))?;
    if reader.next().is_some()
        || batch.num_rows() < 2
        || batch.columns().iter().any(|array| array.null_count() != 0)
    {
        return Err(invalid("V37 tree Arrow batch differs"));
    }
    let record_kind = column::<UInt8Array>(&batch, 0, "V37 record kind differs")?;
    let node_ids = column::<UInt32Array>(&batch, 1, "V37 node ordinal differs")?;
    let source_ordinals = column::<UInt64Array>(&batch, 2, "V37 source ordinal differs")?;
    let assignment_postings = column::<UInt32Array>(&batch, 3, "V37 assignment differs")?;
    let populations = column::<UInt64Array>(&batch, 4, "V37 population differs")?;
    let boundaries = column::<UInt32Array>(&batch, 5, "V37 boundary differs")?;
    let boundary_ordinals = column::<UInt64Array>(&batch, 6, "V37 boundary ordinal differs")?;
    let left_nodes = column::<UInt32Array>(&batch, 7, "V37 left child differs")?;
    let right_nodes = column::<UInt32Array>(&batch, 8, "V37 right child differs")?;
    let node_postings = column::<UInt32Array>(&batch, 9, "V37 node posting differs")?;
    let normals = column::<FixedSizeListArray>(&batch, 10, "V37 normal differs")?;
    let authority_normal = normals.value(0);
    let authority_normal = authority_normal
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| invalid("V37 authority normal differs"))?;
    if record_kind.value(0) != 2
        || node_ids.value(0) as usize != dimensions
        || assignment_postings.value(0) > 2
        || assignment_postings.value(0) == 0
        || boundaries.value(0) != 1
        || left_nodes.value(0) != u32::MAX
        || right_nodes.value(0) != u32::MAX
        || node_postings.value(0) != u32::MAX
        || authority_normal.values().iter().any(|value| *value != 0.0)
    {
        return Err(invalid("V37 tree authority record differs"));
    }
    let seed = source_ordinals.value(0);
    let backend = match assignment_postings.value(0) {
        1 => "aarch64-neon-fma".to_owned(),
        2 => "x86-avx-fma".to_owned(),
        _ => return Err(invalid("V37 tree backend differs")),
    };
    let node_count = usize::try_from(populations.value(0))
        .map_err(|_| invalid("V37 tree node count exceeds address space"))?;
    let leaf_count = usize::try_from(boundary_ordinals.value(0))
        .map_err(|_| invalid("V37 tree leaf count exceeds address space"))?;
    let expected_node_count = leaf_count
        .checked_mul(2)
        .and_then(|value| value.checked_sub(1))
        .ok_or_else(|| invalid("V37 tree authority count overflows"))?;
    let expected_rows = node_count
        .checked_add(1)
        .ok_or_else(|| invalid("V37 tree Arrow row count overflows"))?;
    if leaf_count == 0 || node_count != expected_node_count || expected_rows != batch.num_rows() {
        return Err(invalid("V37 tree Arrow row count differs"));
    }
    let mut nodes = Vec::with_capacity(node_count);
    for (index, row) in (1..=node_count).enumerate() {
        if record_kind.value(row) != 0
            || node_ids.value(row) != index as u32
            || source_ordinals.value(row) != u64::MAX
            || assignment_postings.value(row) != u32::MAX
        {
            return Err(invalid("V37 node record differs"));
        }
        let values = normals.value(row);
        let values = values
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| invalid("V37 normal values differ"))?;
        if values.null_count() != 0 || values.len() != dimensions {
            return Err(invalid("V37 normal width differs"));
        }
        let posting = (node_postings.value(row) != u32::MAX).then(|| node_postings.value(row));
        nodes.push(V37BalancedNode {
            normal: if posting.is_some() {
                if values.values().iter().any(|value| *value != 0.0) {
                    return Err(invalid("V37 leaf normal differs"));
                }
                Vec::new()
            } else {
                values.values().to_vec()
            },
            boundary_score_bits: boundaries.value(row),
            boundary_source_ordinal: boundary_ordinals.value(row),
            left_node: (left_nodes.value(row) != u32::MAX).then(|| left_nodes.value(row)),
            right_node: (right_nodes.value(row) != u32::MAX).then(|| right_nodes.value(row)),
            posting_ordinal: posting,
            population: populations.value(row),
        });
    }
    let mut leaf_populations = vec![0_u64; leaf_count];
    for node in &nodes {
        if let Some(posting) = node.posting_ordinal {
            if posting as usize >= leaf_populations.len() {
                return Err(invalid("V37 leaf posting differs"));
            }
            leaf_populations[posting as usize] = node.population;
        }
    }
    let tree = V37BalancedTree {
        dimensions,
        seed,
        fma_backend: backend,
        nodes,
        leaf_populations,
        assignments: Vec::new(),
    };
    validate_v37_tree_geometry(&tree)?;
    Ok(tree)
}

fn local_input<'a>(request: &'a V37LocalRunRequest, role: &str) -> Result<&'a V37LocalArtifact> {
    request
        .inputs
        .iter()
        .find(|input| input.role == role)
        .ok_or_else(|| invalid("V37 local input role is absent"))
}

fn local_output<'a>(request: &'a V37LocalRunRequest, role: &str) -> Result<&'a V37LocalOutput> {
    request
        .outputs
        .iter()
        .find(|output| output.role == role)
        .ok_or_else(|| invalid("V37 local output role is absent"))
}

fn local_matches_identity(input: &V37LocalArtifact, identity: &V37ArtifactIdentity) -> bool {
    input.uri == identity.uri
        && input.sha256 == identity.sha256
        && input.blake3 == identity.blake3
        && input.encoded_bytes == identity.encoded_bytes
}

fn v36_matches_identity(
    observed: &crate::V36ArtifactIdentity,
    expected: &V37ArtifactIdentity,
) -> bool {
    observed.uri == expected.uri
        && observed.sha256 == expected.sha256
        && observed.blake3 == expected.blake3
        && observed.encoded_bytes == expected.encoded_bytes
}

fn stage_v37_output(path: &Path, bytes: &[u8]) -> Result<tempfile::NamedTempFile> {
    let parent = path
        .parent()
        .ok_or_else(|| invalid("V37 local output parent differs"))?;
    let mut temporary =
        tempfile::NamedTempFile::new_in(parent).map_err(|source| BorsukError::Io {
            path: parent.to_owned(),
            source,
        })?;
    temporary
        .write_all(bytes)
        .and_then(|()| temporary.as_file().sync_all())
        .map_err(|source| BorsukError::Io {
            path: path.to_owned(),
            source,
        })?;
    Ok(temporary)
}

fn publish_v37_output(path: &Path, bytes: &[u8]) -> Result<()> {
    let temporary = stage_v37_output(path, bytes)?;
    temporary
        .persist_noclobber(path)
        .map_err(|error| BorsukError::Io {
            path: path.to_owned(),
            source: error.error,
        })?;
    Ok(())
}

fn publish_v37_output_pair(
    first_path: &Path,
    first_bytes: &[u8],
    second_path: &Path,
    second_bytes: &[u8],
) -> Result<()> {
    let first = stage_v37_output(first_path, first_bytes)?;
    let second = stage_v37_output(second_path, second_bytes)?;
    first
        .persist_noclobber(first_path)
        .map_err(|error| BorsukError::Io {
            path: first_path.to_owned(),
            source: error.error,
        })?;
    if let Err(error) = second.persist_noclobber(second_path) {
        return Err(BorsukError::Io {
            path: second_path.to_owned(),
            source: error.error,
        });
    }
    Ok(())
}

fn encoded_identity(role: &str, uri: String, bytes: &[u8]) -> V37ArtifactIdentity {
    V37ArtifactIdentity {
        blake3: blake3::hash(bytes).to_hex().to_string(),
        encoded_bytes: bytes.len() as u64,
        role: role.to_owned(),
        sha256: format!("{:x}", Sha256::digest(bytes)),
        uri,
    }
}

fn local_artifact_identity(input: &V37LocalArtifact) -> V37ArtifactIdentity {
    V37ArtifactIdentity {
        blake3: input.blake3.clone(),
        encoded_bytes: input.encoded_bytes,
        role: input.role.clone(),
        sha256: input.sha256.clone(),
        uri: input.uri.clone(),
    }
}

fn canonical_v37_local_receipt(
    mode: &str,
    inputs: &[V37LocalArtifact],
    artifacts: &[V37ArtifactIdentity],
    training_evidence: &V37TrainingEvidence,
    preflight_evidence: Option<&V37PreflightEvidence>,
) -> Result<Vec<u8>> {
    if training_evidence.rows == 0
        || training_evidence.dimensions == 0
        || training_evidence.leaf_count < 2
        || training_evidence.partition_coordinate_scores == 0
        || training_evidence.partition_scoring_elapsed_ns == 0
        || training_evidence.training_elapsed_ns < training_evidence.partition_scoring_elapsed_ns
        || !matches!(
            training_evidence.fma_backend.as_str(),
            "aarch64-neon-fma" | "x86-avx-fma"
        )
    {
        return Err(invalid("V37 training evidence differs"));
    }
    let expected_scores_per_second = u64::try_from(
        u128::from(training_evidence.partition_coordinate_scores)
            .checked_mul(1_000_000_000)
            .ok_or_else(|| invalid("V37 partition throughput overflows"))?
            / u128::from(training_evidence.partition_scoring_elapsed_ns),
    )
    .map_err(|_| invalid("V37 partition throughput overflows"))?;
    if training_evidence.partition_coordinate_scores_per_second != expected_scores_per_second {
        return Err(invalid("V37 partition throughput differs"));
    }
    let inputs = inputs
        .iter()
        .map(local_artifact_identity)
        .collect::<Vec<_>>();
    let mut value = serde_json::json!({
        "artifacts": artifacts,
        "claim_eligible": false,
        "inputs": inputs,
        "mode": mode,
        "schema": "borsuk-v37-local-result-v3",
        "training_evidence": training_evidence,
    });
    if let Some(evidence) = preflight_evidence {
        value
            .as_object_mut()
            .ok_or_else(|| invalid("V37 local result shape differs"))?
            .insert(
                "preflight_evidence".to_owned(),
                serde_json::to_value(evidence).map_err(|error| {
                    invalid(&format!("V37 preflight serialization failed: {error}"))
                })?,
            );
    }
    let mut bytes = serde_json::to_vec(&canonical_json_value(value))
        .map_err(|error| invalid(&format!("V37 local result serialization failed: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn canonical_v37_training_progress_bytes(progress: &V37TrainingProgress) -> Result<Vec<u8>> {
    if progress.sequence == 0
        || progress.sequence != progress.completed_internal_nodes
        || progress.completed_internal_nodes > progress.total_internal_nodes
        || progress.partition_coordinate_scores == 0
    {
        return Err(invalid("V37 training progress differs"));
    }
    let value = serde_json::json!({
        "completed_internal_nodes": progress.completed_internal_nodes,
        "partition_coordinate_scores": progress.partition_coordinate_scores,
        "schema": "borsuk-v37-training-progress-v1",
        "sequence": progress.sequence,
        "total_internal_nodes": progress.total_internal_nodes,
    });
    let mut bytes = serde_json::to_vec(&canonical_json_value(value)).map_err(|error| {
        invalid(&format!(
            "V37 training progress serialization failed: {error}"
        ))
    })?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn canonical_v37_bound_ceiling_bytes(
    authority_input: &V37LocalArtifact,
    authority: &V37CeilingAuthority,
    ceiling: &V37LayoutCeiling,
) -> Result<Vec<u8>> {
    if authority_input.role != "ceiling-authority" {
        return Err(invalid("V37 ceiling authority identity differs"));
    }
    validate_v37_ceiling_authority(authority)?;
    validate_v37_ceiling(ceiling)?;
    let value = serde_json::json!({
        "ceiling": ceiling,
        "ceiling_authority": local_artifact_identity(authority_input),
        "claim_eligible": false,
        "inputs": authority,
        "schema": "borsuk-v37-bound-ceiling-v1",
    });
    let mut bytes = serde_json::to_vec(&canonical_json_value(value))
        .map_err(|error| invalid(&format!("V37 bound ceiling serialization failed: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn validate_v37_local_build_authority(
    request: &V37LocalRunRequest,
    manifest: &V37AuthorityManifest,
) -> Result<()> {
    let source_input = local_input(request, "source")?;
    let backend = v37_fma_backend_name(v37_fused_kernel()?);
    if request.mode != V37LocalRunMode::BuildOwnership
        || manifest.numeric.worker_count != request.workers
        || manifest.numeric.fma_backend != backend
        || !local_matches_identity(source_input, &manifest.source)
        || project_v37_construction_bytes(&manifest.tree, &manifest.relation)?.disposition
            != V37LayoutDisposition::Admissible
    {
        return Err(invalid("V37 construction input authority differs"));
    }
    Ok(())
}

fn v37_preflight_coordinates(dimensions: usize, seed: u64) -> Result<Vec<f32>> {
    let values = V37_PREFLIGHT_ROWS
        .checked_mul(dimensions)
        .ok_or_else(|| invalid("V37 preflight shape overflows"))?;
    let mut coordinates = Vec::new();
    coordinates
        .try_reserve_exact(values)
        .map_err(|_| invalid("V37 preflight coordinates exceed capacity"))?;
    for row in 0..V37_PREFLIGHT_ROWS as u64 {
        for dimension in 0..dimensions as u64 {
            let mut value = seed
                ^ row.wrapping_mul(0x9e37_79b9_7f4a_7c15)
                ^ dimension.wrapping_mul(0xbf58_476d_1ce4_e5b9);
            value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            value ^= value >> 31;
            let mantissa = u32::try_from(value >> 41)
                .map_err(|_| invalid("V37 preflight mantissa overflows"))?;
            coordinates.push(f32::from_bits(0x3f80_0000 | mantissa) - 1.5);
        }
    }
    Ok(coordinates)
}

fn v37_preflight_evidence(
    coordinates: &[f32],
    dimensions: usize,
    tree: &V37BalancedTree,
    projected_construction_bytes: u64,
) -> Result<V37PreflightEvidence> {
    let mut coordinate_sha256 = Sha256::new();
    for coordinate in coordinates {
        coordinate_sha256.update(coordinate.to_bits().to_le_bytes());
    }
    let comparison_rows = (coordinates.len() / dimensions).min(1_024);
    let mut comparisons = 0_u64;
    for node in tree
        .nodes
        .iter()
        .filter(|node| node.posting_ordinal.is_none())
    {
        for row in coordinates.chunks_exact(dimensions).take(comparison_rows) {
            let scalar = score_v37_hyperplane_scalar(row, &node.normal)?;
            let (fused, backend) = score_v37_hyperplane_fused(row, &node.normal)?;
            if backend != tree.fma_backend || scalar.to_bits() != fused.to_bits() {
                return Err(invalid("V37 preflight scalar/fused evidence differs"));
            }
            comparisons = comparisons
                .checked_add(1)
                .ok_or_else(|| invalid("V37 preflight comparison count overflows"))?;
        }
    }
    Ok(V37PreflightEvidence {
        coordinate_generator: "splitmix23-f32-v1".to_owned(),
        coordinate_sha256: format!("{:x}", coordinate_sha256.finalize()),
        projected_construction_bytes,
        scalar_fused_comparisons: comparisons,
        scalar_fused_max_ulp_delta: 0,
    })
}

/// Run one capability-separated local V37 fail-fast phase.
#[doc(hidden)]
pub fn run_v37_local_request(request: V37LocalRunRequest) -> Result<Vec<u8>> {
    run_v37_local_request_with_progress(request, |_| Ok(()))
}

/// Run one local V37 phase and report canonical native training progress.
#[doc(hidden)]
pub fn run_v37_local_request_with_progress<F>(
    request: V37LocalRunRequest,
    progress: F,
) -> Result<Vec<u8>>
where
    F: FnMut(&[u8]) -> std::io::Result<()>,
{
    let authenticated = authenticate_v37_local_request(&request)?;
    run_v37_authenticated_local_request(&request, &authenticated, progress)
}

fn run_v37_authenticated_local_request<F>(
    request: &V37LocalRunRequest,
    authenticated: &V37AuthenticatedLocalInputs,
    mut progress: F,
) -> Result<Vec<u8>>
where
    F: FnMut(&[u8]) -> std::io::Result<()>,
{
    match request.mode {
        V37LocalRunMode::PreflightTraining => {
            let manifest = parse_v37_authority_bytes(&read_v37_authenticated_input(
                request,
                authenticated,
                "v37-authority",
                16 * 1_048_576,
            )?)?;
            let dimensions = usize::try_from(manifest.tree.dimensions)
                .map_err(|_| invalid("V37 preflight dimensions exceed address space"))?;
            let backend = v37_fma_backend_name(v37_fused_kernel()?);
            let construction = project_v37_construction_bytes(&manifest.tree, &manifest.relation)?;
            if manifest.numeric.worker_count != request.workers
                || manifest.numeric.fma_backend != backend
                || dimensions != 192
                || construction.disposition != V37LayoutDisposition::Admissible
            {
                return Err(invalid("V37 preflight authority differs"));
            }
            let coordinates = v37_preflight_coordinates(dimensions, manifest.tree.seed)?;
            let (tree, evidence) = train_v37_ownership_tree_resident_coordinates_with_progress(
                &coordinates,
                V37TrainingShape {
                    dimensions,
                    leaf_count: V37_PREFLIGHT_LEAVES,
                    reservoir_rows: usize::try_from(manifest.tree.training_sample_rows)
                        .map_err(|_| invalid("V37 preflight reservoir exceeds address space"))?
                        .min(V37_PREFLIGHT_ROWS),
                    two_means_iterations: usize::try_from(manifest.tree.two_means_iterations)
                        .map_err(|_| invalid("V37 preflight iterations exceed address space"))?,
                },
                manifest.tree.seed,
                request.workers as usize,
                65_536,
                |snapshot| {
                    let bytes = canonical_v37_training_progress_bytes(snapshot)?;
                    progress(&bytes).map_err(|source| BorsukError::Io {
                        path: PathBuf::from("<v37-training-progress>"),
                        source,
                    })
                },
            )?;
            let preflight =
                v37_preflight_evidence(&coordinates, dimensions, &tree, construction.total_bytes)?;
            if preflight.coordinate_sha256 != manifest.numeric.preflight_coordinate_sha256
                || evidence.partition_coordinate_scores
                    != manifest.numeric.preflight_coordinate_scores
            {
                return Err(invalid("V37 preflight registered evidence differs"));
            }
            validate_v37_local_input_stability(request, authenticated)?;
            canonical_v37_local_receipt(
                "preflight-training",
                &request.inputs,
                &[],
                &evidence,
                Some(&preflight),
            )
        }
        V37LocalRunMode::BuildOwnership => {
            let manifest = parse_v37_authority_bytes(&read_v37_authenticated_input(
                request,
                authenticated,
                "v37-authority",
                16 * 1_048_576,
            )?)?;
            validate_v37_local_build_authority(request, &manifest)?;
            let source_input = local_input(request, "source")?;
            let v36_authority = read_v37_authenticated_input(
                request,
                authenticated,
                "v36-authority",
                16 * 1_048_576,
            )?;
            let v36_execution = read_v37_authenticated_input(
                request,
                authenticated,
                "v36-execution-authority",
                16 * 1_048_576,
            )?;
            let v36_receipt = read_v37_authenticated_input(
                request,
                authenticated,
                "v36-receipt",
                16 * 1_048_576,
            )?;
            let v36_registry = read_v37_authenticated_input(
                request,
                authenticated,
                "v36-source-registry",
                16 * 1_048_576,
            )?;
            let inputs = crate::v36_prefix_dataset::load_v36_prefix_geometry_authority_bytes(
                &v36_authority,
                &v36_execution,
                &v36_receipt,
                &v36_registry,
            )?;
            let construction = inputs.construction();
            if construction.corpus_rows() != manifest.tree.corpus_rows
                || !v36_matches_identity(construction.source(), &manifest.source)
            {
                return Err(invalid("V37 construction V36 binding differs"));
            }
            let feature_ids = crate::v36_prefix_dataset::load_v36_prefix_source_feature_ids_file(
                authenticated_v37_input_file(request, authenticated, "source")?,
                &source_input.path,
                construction.corpus_rows(),
            )?;
            let projected = crate::v36_prefix_dataset::project_v36_prefix_source_resident_file(
                authenticated_v37_input_file(request, authenticated, "source")?,
                &source_input.path,
                &feature_ids,
                65_536,
            )?;
            if projected.projected_corpus_sha256() != manifest.projection.projected_corpus_sha256 {
                return Err(invalid("V37 projected corpus replay differs"));
            }
            let layout = project_v37_layout(&manifest.tree)?;
            let (tree, training_evidence) =
                train_v37_ownership_tree_resident_coordinates_with_progress(
                    projected.projected_coordinates(),
                    V37TrainingShape {
                        dimensions: usize::try_from(manifest.tree.dimensions)
                            .map_err(|_| invalid("V37 dimensions exceed address space"))?,
                        leaf_count: layout.leaf_count,
                        reservoir_rows: usize::try_from(manifest.tree.training_sample_rows)
                            .map_err(|_| invalid("V37 reservoir exceeds address space"))?,
                        two_means_iterations: usize::try_from(manifest.tree.two_means_iterations)
                            .map_err(|_| {
                            invalid("V37 iterations exceed address space")
                        })?,
                    },
                    manifest.tree.seed,
                    request.workers as usize,
                    65_536,
                    |snapshot| {
                        let bytes = canonical_v37_training_progress_bytes(snapshot)?;
                        progress(&bytes).map_err(|source| BorsukError::Io {
                            path: PathBuf::from("<v37-training-progress>"),
                            source,
                        })
                    },
                )?;
            let tree_bytes = encode_v37_tree_arrow(&tree)?;
            let ownership_bytes = encode_v37_ownership_parquet(
                &tree.assignments,
                projected.feature_ids(),
                &tree.leaf_populations,
            )?;
            let tree_output = local_output(request, "ownership-tree")?;
            let ownership_output = local_output(request, "ownership")?;
            validate_v37_local_input_stability(request, authenticated)?;
            publish_v37_output_pair(
                &tree_output.path,
                &tree_bytes.bytes,
                &ownership_output.path,
                &ownership_bytes.bytes,
            )?;
            canonical_v37_local_receipt(
                "build-ownership",
                &request.inputs,
                &[
                    encoded_identity(
                        "ownership-tree",
                        format!("file://{}", tree_output.path.display()),
                        &tree_bytes.bytes,
                    ),
                    encoded_identity(
                        "ownership",
                        format!("file://{}", ownership_output.path.display()),
                        &ownership_bytes.bytes,
                    ),
                ],
                &training_evidence,
                None,
            )
        }
        V37LocalRunMode::EvaluateCeiling => {
            let authority_input = local_input(request, "ceiling-authority")?;
            let authority = parse_v37_ceiling_authority_bytes(&read_v37_authenticated_input(
                request,
                authenticated,
                "ceiling-authority",
                16 * 1_048_576,
            )?)?;
            let tree_input = local_input(request, "ownership-tree")?;
            let ownership_input = local_input(request, "ownership")?;
            let truth_input = local_input(request, "development-ground-truth")?;
            if !local_matches_identity(tree_input, &authority.ownership_tree)
                || !local_matches_identity(ownership_input, &authority.ownership)
                || !local_matches_identity(truth_input, &authority.development_ground_truth)
            {
                return Err(invalid("V37 ceiling input binding differs"));
            }
            let tree_file = read_v37_authenticated_input(
                request,
                authenticated,
                "ownership-tree",
                tree_input.encoded_bytes,
            )?;
            let tree = decode_v37_tree_arrow(
                &tree_file,
                tree_input.encoded_bytes,
                &tree_input.sha256,
                &tree_input.blake3,
            )?;
            let ownership_file = read_v37_authenticated_input(
                request,
                authenticated,
                "ownership",
                ownership_input.encoded_bytes,
            )?;
            let ownership = decode_v37_ownership_parquet(
                &ownership_file,
                ownership_input.encoded_bytes,
                &ownership_input.sha256,
                &ownership_input.blake3,
                &tree.leaf_populations,
            )?;
            let feature_truth = load_v37_feature_ground_truth_file(
                authenticated_v37_input_file(request, authenticated, "development-ground-truth")?,
                &truth_input.path,
                authority.query_count,
            )?;
            let truth = map_v37_feature_ground_truth(&ownership, &feature_truth)?;
            let assignments = ownership
                .iter()
                .map(|row| V37OwnershipAssignment {
                    source_ordinal: row.source_ordinal,
                    posting_ordinal: row.posting_ordinal,
                })
                .collect::<Vec<_>>();
            let ceiling = evaluate_v37_unique_owner_ceiling(
                &assignments,
                &truth,
                authority.selected_postings as usize,
            )?;
            let bytes = canonical_v37_bound_ceiling_bytes(authority_input, &authority, &ceiling)?;
            validate_v37_local_input_stability(request, authenticated)?;
            publish_v37_output(&local_output(request, "ceiling")?.path, &bytes)?;
            Ok(bytes)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, fs, path::Path};

    use sha2::{Digest, Sha256};

    use super::{
        V37_PREFLIGHT_COORDINATE_SCORES, V37_PREFLIGHT_COORDINATE_SHA256, V37ArtifactIdentity,
        V37AuthorityManifest, V37CeilingAuthority, V37DirectSelection, V37DirectSelectionRecord,
        V37FeatureGroundTruth, V37GroundTruth, V37LayoutDisposition, V37LocalArtifact,
        V37LocalOutput, V37LocalRunMode, V37LocalRunRequest, V37NumericAuthority,
        V37OwnershipRecord, V37ProjectionAuthority, V37RelationSpec, V37TrainingRow,
        V37TrainingShape, V37TreeSpec, authenticate_v37_local_request, build_v37_relation_plane,
        canonical_v37_authority_bytes, canonical_v37_bound_ceiling_bytes,
        canonical_v37_ceiling_authority_bytes, canonical_v37_ceiling_bytes,
        canonical_v37_direct_bytes, canonical_v37_local_receipt, decode_v37_ownership_parquet,
        decode_v37_relation_counts_parquet, decode_v37_relation_prefixes_arrow,
        decode_v37_tree_arrow, encode_v37_ownership_parquet, encode_v37_relation_counts_parquet,
        encode_v37_relation_prefixes_arrow, encode_v37_tree_arrow, evaluate_v37_direct_recall,
        evaluate_v37_unique_owner_ceiling, map_v37_feature_ground_truth, parse_v37_authority_bytes,
        prepare_v37_direct_router, prepare_v37_relation_prefixes, prepare_v37_relation_router,
        project_v37_child_quota, project_v37_construction_bytes, project_v37_layout,
        project_v37_serving_bytes, project_v37_work, publish_v37_output_pair,
        read_v37_authenticated_input, reduce_v37_relation_postings, repair_v37_empty_partition,
        route_v37_corpus_member, route_v37_primary_leaf, route_v37_query, run_v37_local_request,
        score_v37_hyperplane_fused, score_v37_hyperplane_scalar, select_v37_direct_postings,
        select_v37_node_reservoir, select_v37_relation_postings, train_v37_ownership_tree,
        train_v37_ownership_tree_resident_coordinates,
        train_v37_ownership_tree_resident_coordinates_with_evidence,
        train_v37_ownership_tree_resident_coordinates_with_progress, v37_mass_q24,
        validate_v37_local_build_authority, validate_v37_local_input_stability,
        validate_v37_relation_prefixes, validate_v37_specs,
    };

    fn local_artifact(root: &Path, role: &str) -> V37LocalArtifact {
        let path = root.join(role);
        let bytes = format!("registered-{role}\n").into_bytes();
        fs::write(&path, &bytes).unwrap();
        V37LocalArtifact::try_new(
            role.to_owned(),
            path,
            format!("s3://frozen-v37/{role}"),
            format!("{:x}", Sha256::digest(&bytes)),
            blake3::hash(&bytes).to_hex().to_string(),
            bytes.len() as u64,
        )
        .unwrap()
    }

    fn local_artifact_bytes(root: &Path, role: &str, bytes: &[u8]) -> V37LocalArtifact {
        let path = root.join(role);
        fs::write(&path, bytes).unwrap();
        V37LocalArtifact::try_new(
            role.to_owned(),
            path,
            format!("s3://frozen-v37/{role}"),
            format!("{:x}", Sha256::digest(bytes)),
            blake3::hash(bytes).to_hex().to_string(),
            bytes.len() as u64,
        )
        .unwrap()
    }

    fn local_build_request(root: &Path) -> V37LocalRunRequest {
        let roles = [
            "v36-authority",
            "v36-execution-authority",
            "v36-receipt",
            "v36-source-registry",
            "v37-authority",
            "source",
        ];
        let inputs = roles
            .iter()
            .map(|role| local_artifact(root, role))
            .collect();
        let outputs = ["ownership-tree", "ownership"]
            .into_iter()
            .map(|role| {
                V37LocalOutput::try_new(role.to_owned(), root.join(format!("out-{role}"))).unwrap()
            })
            .collect();
        V37LocalRunRequest::try_new(V37LocalRunMode::BuildOwnership, inputs, outputs, 4).unwrap()
    }

    #[test]
    fn v37_relation_local_authenticates_bytes_and_rejects_filesystem_aliases_before_execution() {
        let root = tempfile::tempdir().unwrap();
        let request = local_build_request(root.path());
        authenticate_v37_local_request(&request).unwrap();

        fs::write(root.path().join("source"), b"changed\n").unwrap();
        assert!(authenticate_v37_local_request(&request).is_err());

        let alias_root = tempfile::tempdir().unwrap();
        let mut alias = local_build_request(alias_root.path());
        fs::hard_link(
            alias_root.path().join("source"),
            alias_root.path().join("out-ownership-tree"),
        )
        .unwrap();
        assert!(authenticate_v37_local_request(&alias).is_err());

        fs::create_dir(alias_root.path().join("nested")).unwrap();
        alias.outputs[0].path = alias_root.path().join("nested/../source");
        assert!(authenticate_v37_local_request(&alias).is_err());
    }

    #[test]
    fn v37_relation_local_detects_input_replacement_after_authentication() {
        let root = tempfile::tempdir().unwrap();
        let request = local_build_request(root.path());
        let authenticated = authenticate_v37_local_request(&request).unwrap();
        fs::write(root.path().join("source"), b"subverted-source!\n").unwrap();
        assert!(validate_v37_local_input_stability(&request, &authenticated).is_err());
    }

    #[test]
    fn v37_relation_local_keeps_authenticated_file_capability_across_parent_swap() {
        let parent = tempfile::tempdir().unwrap();
        let bundle = parent.path().join("bundle");
        fs::create_dir(&bundle).unwrap();
        let request = local_build_request(&bundle);
        let authenticated = authenticate_v37_local_request(&request).unwrap();
        let original = parent.path().join("authenticated-bundle");
        fs::rename(&bundle, &original).unwrap();
        fs::create_dir(&bundle).unwrap();
        fs::write(bundle.join("source"), b"substitute-source\n").unwrap();

        assert_eq!(
            read_v37_authenticated_input(&request, &authenticated, "source", 1_048_576).unwrap(),
            b"registered-source\n"
        );
    }

    #[test]
    fn v37_relation_local_receipt_binds_every_input_and_output_identity() {
        let root = tempfile::tempdir().unwrap();
        let request = local_build_request(root.path());
        let outputs = [artifact("ownership-tree", '7'), artifact("ownership", '8')];
        let evidence = super::V37TrainingEvidence {
            dimensions: 192,
            fma_backend: "aarch64-neon-fma".to_owned(),
            leaf_count: 16,
            partition_coordinate_scores: 76_800,
            partition_coordinate_scores_per_second: 20_000_000,
            partition_scoring_elapsed_ns: 3_840_000,
            rows: 100,
            training_elapsed_ns: 4_000_000,
        };
        let bytes = canonical_v37_local_receipt(
            "build-ownership",
            &request.inputs,
            &outputs,
            &evidence,
            None,
        )
        .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["schema"], "borsuk-v37-local-result-v3");
        assert_eq!(value["inputs"].as_array().unwrap().len(), 6);
        assert_eq!(value["artifacts"].as_array().unwrap().len(), 2);
        assert_eq!(value["inputs"][0]["role"], "v36-authority");
        assert_eq!(value["inputs"][5]["role"], "source");
        assert_eq!(
            value["training_evidence"]["partition_coordinate_scores"],
            76_800
        );
        assert_eq!(bytes.last(), Some(&b'\n'));
    }

    #[test]
    fn v37_relation_output_pair_failure_preserves_owned_scratch_for_explicit_cleanup() {
        let root = tempfile::tempdir().unwrap();
        let tree = root.path().join("tree.arrow");
        let ownership = root.path().join("ownership.parquet");
        fs::write(&ownership, b"existing").unwrap();

        assert!(publish_v37_output_pair(&tree, b"tree", &ownership, b"ownership").is_err());
        assert_eq!(fs::read(tree).unwrap(), b"tree");
        assert_eq!(fs::read(ownership).unwrap(), b"existing");
    }

    #[test]
    fn v37_relation_local_build_rejects_worker_and_backend_authority_drift() {
        let root = tempfile::tempdir().unwrap();
        let request = local_build_request(root.path());
        let source = request.inputs.last().unwrap();
        let mut manifest = authority();
        manifest.numeric.worker_count = 4;
        manifest.numeric.fma_backend =
            super::v37_fma_backend_name(super::v37_fused_kernel().unwrap()).to_owned();
        manifest.source = V37ArtifactIdentity {
            blake3: source.blake3.clone(),
            encoded_bytes: source.encoded_bytes,
            role: "source-corpus".to_owned(),
            sha256: source.sha256.clone(),
            uri: source.uri.clone(),
        };
        validate_v37_local_build_authority(&request, &manifest).unwrap();

        let mut wrong_workers = manifest.clone();
        wrong_workers.numeric.worker_count = 8;
        assert!(validate_v37_local_build_authority(&request, &wrong_workers).is_err());

        let mut wrong_backend = manifest;
        wrong_backend.numeric.fma_backend = "x86-avx-fma".to_owned();
        assert!(validate_v37_local_build_authority(&request, &wrong_backend).is_err());
    }

    fn ownership_spec(rows: u64) -> V37TreeSpec {
        V37TreeSpec {
            corpus_rows: rows,
            dimensions: 192,
            seed: 37,
            target_primary_rows: 8_192,
            training_sample_rows: 4_096,
            two_means_iterations: 8,
        }
    }

    fn relation_spec() -> V37RelationSpec {
        V37RelationSpec {
            leaf_count: 4_096,
            maximum_leaf_probes: 32,
            prefix_lengths: vec![16, 32, 64],
            q_bits: 24,
            seed: 38,
        }
    }

    fn artifact(role: &str, digit: char) -> V37ArtifactIdentity {
        V37ArtifactIdentity {
            blake3: digit.to_string().repeat(64),
            encoded_bytes: 1,
            role: role.to_owned(),
            sha256: digit.to_string().repeat(64),
            uri: format!("s3://borsuk-v37-test/{role}"),
        }
    }

    fn authority() -> V37AuthorityManifest {
        V37AuthorityManifest {
            algorithm: "balanced-hyperplane-relation-v1".to_owned(),
            metric: "squared-l2".to_owned(),
            numeric: V37NumericAuthority {
                fma_backend: "aarch64-neon-fma".to_owned(),
                lane_width: 8,
                preflight_coordinate_scores: V37_PREFLIGHT_COORDINATE_SCORES,
                preflight_coordinate_sha256: V37_PREFLIGHT_COORDINATE_SHA256.to_owned(),
                worker_count: 16,
            },
            projection: V37ProjectionAuthority {
                algorithm: "v36-srht-f32-v1".to_owned(),
                projected_corpus_sha256: "2".repeat(64),
                routing_dimensions: 192,
                seed: 36,
                source_dimensions: 768,
            },
            relation: relation_spec(),
            schema: "borsuk-v37-relation-authority-v3".to_owned(),
            source: artifact("source-corpus", '1'),
            tree: ownership_spec(1_000_000),
        }
    }

    fn ceiling_authority() -> V37CeilingAuthority {
        V37CeilingAuthority {
            construction_authority: artifact("v37-construction-authority", '3'),
            development_ground_truth: artifact("development-ground-truth", '4'),
            gt_neighbors: 100,
            ownership: artifact("ownership", '5'),
            ownership_tree: artifact("ownership-tree", '6'),
            query_count: 1_000,
            schema: "borsuk-v37-ceiling-authority-v1".to_owned(),
            selected_postings: 14,
        }
    }

    fn training_rows(count: u64, dimensions: usize) -> Vec<V37TrainingRow> {
        (0..count)
            .map(|source_ordinal| {
                let mut vector = vec![0.0_f32; dimensions];
                vector[0] = source_ordinal as f32;
                vector[1] = (source_ordinal % 3) as f32 * 0.125;
                vector[dimensions - 1] = (source_ordinal % 2) as f32 * f32::EPSILON;
                V37TrainingRow {
                    source_ordinal,
                    vector,
                }
            })
            .collect()
    }

    #[test]
    fn v37_relation_authority_projects_recursive_quotas_and_checked_work() {
        let root = project_v37_child_quota(1_000_000, 123).unwrap();
        assert_eq!(root.left_leaves, 61);
        assert_eq!(root.right_leaves, 62);
        assert_eq!(root.left_rows, 495_934);
        assert_eq!(root.right_rows, 504_066);

        let left = project_v37_child_quota(root.left_rows, root.left_leaves).unwrap();
        assert_eq!(left.left_leaves, 30);
        assert_eq!(left.right_leaves, 31);
        assert_eq!(left.left_rows, 243_901);
        assert_eq!(left.right_rows, 252_033);

        let work = project_v37_work(&ownership_spec(1_000_000), &relation_spec()).unwrap();
        assert_eq!(work.maximum_direct_node_visits, 245);
        assert_eq!(work.maximum_relation_node_visits, 1_024);
        assert_eq!(work.maximum_relation_records, 2_048);
        assert_eq!(work.selected_postings, 14);

        assert!(project_v37_child_quota(u64::MAX, 4).is_err());
        assert!(project_v37_child_quota(1, 1).is_err());
    }

    #[test]
    fn v37_relation_authority_projects_exact_construction_memory_and_disposition() {
        let projection =
            project_v37_construction_bytes(&ownership_spec(1_000_000), &relation_spec()).unwrap();
        assert_eq!(projection.source_decode_working_bytes, 738_721_792);
        assert_eq!(projection.resident_projected_bytes, 768_000_000);
        assert_eq!(projection.feature_id_bytes, 8_000_000);
        assert_eq!(projection.ordinal_index_bytes, 16_000_000);
        assert_eq!(projection.member_index_bytes, 8_000_000);
        assert_eq!(projection.score_tuple_bytes, 24_000_000);
        assert_eq!(projection.assignment_bytes, 16_000_000);
        assert_eq!(projection.reservoir_bytes, 16_163_840);
        assert_eq!(projection.tree_bytes, 97_600);
        assert_eq!(projection.ownership_writer_bytes, 48_000_000);
        assert_eq!(projection.worker_stack_bytes, 67_108_864);
        assert_eq!(projection.subtotal_bytes, 1_710_092_096);
        assert_eq!(projection.allocator_headroom_bytes, 427_523_024);
        assert_eq!(projection.total_bytes, 2_137_615_120);
        assert_eq!(projection.disposition, V37LayoutDisposition::Admissible);

        let too_large =
            project_v37_construction_bytes(&ownership_spec(100_000_000), &relation_spec()).unwrap();
        assert_eq!(
            too_large.disposition,
            V37LayoutDisposition::ResourceRejected
        );

        let mut overflow = ownership_spec(1_000_000);
        overflow.dimensions = u64::MAX;
        assert!(project_v37_construction_bytes(&overflow, &relation_spec()).is_err());
    }

    #[test]
    fn v37_relation_authority_projects_exact_one_million_quota_tree() {
        let tree = ownership_spec(1_000_000);
        let relation = relation_spec();
        validate_v37_specs(&tree, &relation).unwrap();

        let projection = project_v37_layout(&tree).unwrap();
        assert_eq!(projection.leaf_count, 123);
        assert_eq!(projection.internal_node_count, 122);
        assert_eq!(projection.minimum_leaf_rows, 8_130);
        assert_eq!(projection.maximum_leaf_rows, 8_131);
        assert_eq!(
            projection.maximum_leaf_rows - projection.minimum_leaf_rows,
            1
        );
        assert_eq!(projection.total_rows, 1_000_000);
    }

    #[test]
    fn v37_relation_authority_projects_checked_hundred_million_ram() {
        let tree = ownership_spec(100_000_000);
        let relation = relation_spec();
        validate_v37_specs(&tree, &relation).unwrap();

        let layout = project_v37_layout(&tree).unwrap();
        assert_eq!(layout.leaf_count, 12_208);
        assert_eq!(layout.internal_node_count, 12_207);
        assert_eq!(layout.minimum_leaf_rows, 8_191);
        assert_eq!(layout.maximum_leaf_rows, 8_192);

        let serving = project_v37_serving_bytes(&tree, &relation).unwrap();
        assert_eq!(serving.ownership_tree_bytes, 9_765_600);
        assert_eq!(serving.relation_tree_bytes, 3_276_000);
        assert_eq!(serving.relation_prefix_bytes, 2_129_928);
        assert_eq!(serving.total_bytes, 15_171_528);
        assert!(serving.total_bytes < 3 * 1_073_741_824);
    }

    #[test]
    fn v37_relation_authority_rejects_numeric_and_policy_drift() {
        let tree = ownership_spec(1_000_000);
        let relation = relation_spec();

        for invalid_tree in [
            V37TreeSpec {
                dimensions: 0,
                ..tree.clone()
            },
            V37TreeSpec {
                corpus_rows: 0,
                ..tree.clone()
            },
            V37TreeSpec {
                target_primary_rows: 0,
                ..tree.clone()
            },
            V37TreeSpec {
                training_sample_rows: 4_095,
                ..tree.clone()
            },
            V37TreeSpec {
                two_means_iterations: 7,
                ..tree.clone()
            },
        ] {
            assert!(validate_v37_specs(&invalid_tree, &relation).is_err());
        }

        for invalid_relation in [
            V37RelationSpec {
                seed: tree.seed,
                ..relation.clone()
            },
            V37RelationSpec {
                leaf_count: 0,
                ..relation.clone()
            },
            V37RelationSpec {
                maximum_leaf_probes: 31,
                ..relation.clone()
            },
            V37RelationSpec {
                prefix_lengths: vec![16, 64],
                ..relation.clone()
            },
            V37RelationSpec {
                q_bits: 23,
                ..relation.clone()
            },
        ] {
            assert!(validate_v37_specs(&tree, &invalid_relation).is_err());
        }
    }

    #[test]
    fn v37_relation_authority_canonical_manifest_rejects_identity_drift() {
        let manifest = authority();
        let bytes = canonical_v37_authority_bytes(&manifest).unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));
        assert!(!bytes[..bytes.len() - 1].contains(&b'\n'));
        assert_eq!(parse_v37_authority_bytes(&bytes).unwrap(), manifest);

        for invalid in [
            V37AuthorityManifest {
                schema: "borsuk-v37-relation-authority-v1".to_owned(),
                ..manifest.clone()
            },
            V37AuthorityManifest {
                algorithm: "centroid".to_owned(),
                ..manifest.clone()
            },
            V37AuthorityManifest {
                metric: "cosine".to_owned(),
                ..manifest.clone()
            },
            V37AuthorityManifest {
                numeric: V37NumericAuthority {
                    fma_backend: "scalar-control".to_owned(),
                    ..manifest.numeric.clone()
                },
                ..manifest.clone()
            },
            V37AuthorityManifest {
                numeric: V37NumericAuthority {
                    worker_count: 0,
                    ..manifest.numeric.clone()
                },
                ..manifest.clone()
            },
            V37AuthorityManifest {
                numeric: V37NumericAuthority {
                    preflight_coordinate_scores: V37_PREFLIGHT_COORDINATE_SCORES - 1,
                    ..manifest.numeric.clone()
                },
                ..manifest.clone()
            },
            V37AuthorityManifest {
                numeric: V37NumericAuthority {
                    preflight_coordinate_sha256: "0".repeat(64),
                    ..manifest.numeric.clone()
                },
                ..manifest.clone()
            },
            V37AuthorityManifest {
                source: V37ArtifactIdentity {
                    encoded_bytes: 0,
                    ..manifest.source.clone()
                },
                ..manifest.clone()
            },
            V37AuthorityManifest {
                projection: V37ProjectionAuthority {
                    projected_corpus_sha256: "A".repeat(64),
                    ..manifest.projection.clone()
                },
                ..manifest.clone()
            },
            V37AuthorityManifest {
                projection: V37ProjectionAuthority {
                    algorithm: "projected-corpus-object".to_owned(),
                    ..manifest.projection.clone()
                },
                ..manifest.clone()
            },
            V37AuthorityManifest {
                projection: V37ProjectionAuthority {
                    source_dimensions: 192,
                    ..manifest.projection.clone()
                },
                ..manifest.clone()
            },
            V37AuthorityManifest {
                source: V37ArtifactIdentity {
                    uri: "s3://".to_owned(),
                    ..manifest.source.clone()
                },
                ..manifest.clone()
            },
            V37AuthorityManifest {
                source: V37ArtifactIdentity {
                    uri: "s3://bucket-only".to_owned(),
                    ..manifest.source.clone()
                },
                ..manifest.clone()
            },
        ] {
            assert!(canonical_v37_authority_bytes(&invalid).is_err());
        }

        let mut unknown = bytes.clone();
        unknown.splice(1..1, b"\"extra\":0,".iter().copied());
        assert!(parse_v37_authority_bytes(&unknown).is_err());

        let mut noncanonical = bytes.clone();
        noncanonical.insert(0, b' ');
        assert!(parse_v37_authority_bytes(&noncanonical).is_err());
    }

    #[test]
    fn v37_relation_authority_separates_ceiling_truth_capability_and_bindings() {
        let authority = ceiling_authority();
        let bytes = canonical_v37_ceiling_authority_bytes(&authority).unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));
        assert!(!bytes.windows(6).any(|window| window == b"source"));

        for invalid in [
            V37CeilingAuthority {
                query_count: 0,
                ..authority.clone()
            },
            V37CeilingAuthority {
                query_count: 1_001,
                ..authority.clone()
            },
            V37CeilingAuthority {
                gt_neighbors: 99,
                ..authority.clone()
            },
            V37CeilingAuthority {
                selected_postings: 15,
                ..authority.clone()
            },
            V37CeilingAuthority {
                ownership: V37ArtifactIdentity {
                    uri: authority.ownership_tree.uri.clone(),
                    ..authority.ownership.clone()
                },
                ..authority.clone()
            },
        ] {
            assert!(canonical_v37_ceiling_authority_bytes(&invalid).is_err());
        }
    }

    #[test]
    fn v37_relation_ceiling_result_binds_exact_authority_and_prerequisites() {
        let root = tempfile::tempdir().unwrap();
        let authority_input = local_artifact(root.path(), "ceiling-authority");
        let authority = ceiling_authority();
        let assignments = (0_u64..100)
            .map(|source_ordinal| super::V37OwnershipAssignment {
                source_ordinal,
                posting_ordinal: 0,
            })
            .collect::<Vec<_>>();
        let truth = vec![V37GroundTruth {
            query_ordinal: 0,
            source_ordinals: (0_u64..100).collect(),
        }];
        let ceiling = evaluate_v37_unique_owner_ceiling(&assignments, &truth, 14).unwrap();
        let bytes =
            canonical_v37_bound_ceiling_bytes(&authority_input, &authority, &ceiling).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["schema"], "borsuk-v37-bound-ceiling-v1");
        assert_eq!(value["ceiling_authority"]["role"], "ceiling-authority");
        assert_eq!(
            value["inputs"]["development_ground_truth"]["role"],
            "development-ground-truth"
        );
        assert_eq!(value["ceiling"]["aggregate_recall_ppm"], 1_000_000);
        assert_eq!(bytes.last(), Some(&b'\n'));
    }

    #[test]
    fn v37_relation_local_ceiling_runs_real_codecs_and_stops_layout_rejected() {
        let root = tempfile::tempdir().unwrap();
        let rows = training_rows(100, 192);
        let tree = train_v37_ownership_tree(
            &rows,
            V37TrainingShape {
                dimensions: 192,
                leaf_count: 16,
                reservoir_rows: 32,
                two_means_iterations: 8,
            },
            37,
            2,
            32,
        )
        .unwrap();
        let tree_bytes = encode_v37_tree_arrow(&tree).unwrap();
        let feature_ids = (0_u64..100)
            .map(|ordinal| 50_000 + ordinal * 17)
            .collect::<Vec<_>>();
        let ownership_bytes =
            encode_v37_ownership_parquet(&tree.assignments, &feature_ids, &tree.leaf_populations)
                .unwrap();

        let truth_path = root.path().join("development-ground-truth");
        let truth_batch = arrow_array::RecordBatch::try_new(
            std::sync::Arc::new(crate::v36_prefix_gt100_schema()),
            vec![
                std::sync::Arc::new(arrow_array::UInt32Array::from(vec![0; 100])),
                std::sync::Arc::new(arrow_array::UInt16Array::from_iter_values(0..100)),
                std::sync::Arc::new(arrow_array::UInt64Array::from(feature_ids)),
                std::sync::Arc::new(arrow_array::Float64Array::from_iter_values(
                    (0..100).map(f64::from),
                )),
            ],
        )
        .unwrap();
        crate::write_v36_prefix_gt100_parquet(&truth_path, [truth_batch]).unwrap();
        let truth_bytes = fs::read(&truth_path).unwrap();

        let tree_input = local_artifact_bytes(root.path(), "ownership-tree", &tree_bytes.bytes);
        let ownership_input =
            local_artifact_bytes(root.path(), "ownership", &ownership_bytes.bytes);
        let truth_input =
            local_artifact_bytes(root.path(), "development-ground-truth", &truth_bytes);
        let authority = V37CeilingAuthority {
            construction_authority: artifact("v37-construction-authority", '3'),
            development_ground_truth: super::local_artifact_identity(&truth_input),
            gt_neighbors: 100,
            ownership: super::local_artifact_identity(&ownership_input),
            ownership_tree: super::local_artifact_identity(&tree_input),
            query_count: 1,
            schema: "borsuk-v37-ceiling-authority-v1".to_owned(),
            selected_postings: 14,
        };
        let authority_bytes = canonical_v37_ceiling_authority_bytes(&authority).unwrap();
        let authority_input =
            local_artifact_bytes(root.path(), "ceiling-authority", &authority_bytes);
        let output_path = root.path().join("ceiling.json");
        let request = V37LocalRunRequest::try_new(
            V37LocalRunMode::EvaluateCeiling,
            vec![authority_input, truth_input, tree_input, ownership_input],
            vec![V37LocalOutput::try_new("ceiling".to_owned(), output_path.clone()).unwrap()],
            1,
        )
        .unwrap();

        let bytes = run_v37_local_request(request).unwrap();
        assert_eq!(fs::read(output_path).unwrap(), bytes);
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["schema"], "borsuk-v37-bound-ceiling-v1");
        assert_eq!(value["ceiling"]["aggregate_recall_ppm"], 880_000);
        assert_eq!(value["ceiling"]["minimum_recall_ppm"], 880_000);
        assert_eq!(value["ceiling"]["disposition"], "layout-rejected");
        assert!(!root.path().join("direct.json").exists());
        assert!(!root.path().join("relations.arrow").exists());
    }

    #[test]
    fn v37_relation_tree_selects_exact_framed_node_reservoir() {
        let ordinals = (0..8).collect::<Vec<_>>();
        assert_eq!(
            select_v37_node_reservoir(&ordinals, 37, 0, 4).unwrap(),
            vec![3, 7, 2, 1]
        );
        assert!(select_v37_node_reservoir(&ordinals, 37, 0, 0).is_err());
        assert!(select_v37_node_reservoir(&[1, 1], 37, 0, 1).is_err());
    }

    #[test]
    fn v37_relation_tree_trains_exact_uneven_quota_and_single_ownership() {
        let rows = training_rows(12, 192);
        let shape = V37TrainingShape {
            dimensions: 192,
            leaf_count: 3,
            reservoir_rows: 8,
            two_means_iterations: 8,
        };
        let first = train_v37_ownership_tree(&rows, shape, 37, 1, 3).unwrap();
        assert_eq!(first.nodes.len(), 5);
        assert_eq!(first.leaf_populations, vec![4, 4, 4]);
        assert_eq!(first.assignments.len(), 12);
        assert_eq!(
            first
                .assignments
                .iter()
                .map(|assignment| assignment.source_ordinal)
                .collect::<Vec<_>>(),
            (0..12).collect::<Vec<_>>()
        );
        assert!(
            first
                .assignments
                .iter()
                .all(|assignment| assignment.posting_ordinal < 3)
        );

        let mut reversed = rows.clone();
        reversed.reverse();
        let reordered = train_v37_ownership_tree(&reversed, shape, 37, 4, 5).unwrap();
        assert_eq!(first, reordered);

        let flat = rows
            .iter()
            .flat_map(|row| row.vector.iter().copied())
            .collect::<Vec<_>>();
        let borrowed =
            train_v37_ownership_tree_resident_coordinates(&flat, shape, 37, 4, 5).unwrap();
        assert_eq!(first, borrowed);
    }

    #[test]
    fn v37_relation_preflight_measures_the_production_partition_scorer() {
        let rows = training_rows(100, 192);
        let coordinates = rows
            .iter()
            .flat_map(|row| row.vector.iter().copied())
            .collect::<Vec<_>>();
        let (tree, evidence) = train_v37_ownership_tree_resident_coordinates_with_evidence(
            &coordinates,
            V37TrainingShape {
                dimensions: 192,
                leaf_count: 16,
                reservoir_rows: 32,
                two_means_iterations: 8,
            },
            37,
            2,
            32,
        )
        .unwrap();

        assert_eq!(evidence.rows, 100);
        assert_eq!(evidence.dimensions, 192);
        assert_eq!(evidence.leaf_count, 16);
        assert_eq!(evidence.partition_coordinate_scores, 76_800);
        assert!(evidence.partition_scoring_elapsed_ns > 0);
        assert!(evidence.training_elapsed_ns >= evidence.partition_scoring_elapsed_ns);
        assert_eq!(evidence.fma_backend, tree.fma_backend);
        assert_eq!(
            evidence.partition_coordinate_scores_per_second,
            u64::try_from(
                u128::from(evidence.partition_coordinate_scores) * 1_000_000_000
                    / u128::from(evidence.partition_scoring_elapsed_ns)
            )
            .unwrap()
        );
    }

    #[test]
    fn v37_relation_preflight_progress_is_native_monotone_and_exact() {
        let rows = training_rows(100, 192);
        let coordinates = rows
            .iter()
            .flat_map(|row| row.vector.iter().copied())
            .collect::<Vec<_>>();
        let mut progress = Vec::new();
        let (_, evidence) = train_v37_ownership_tree_resident_coordinates_with_progress(
            &coordinates,
            V37TrainingShape {
                dimensions: 192,
                leaf_count: 16,
                reservoir_rows: 32,
                two_means_iterations: 8,
            },
            37,
            2,
            32,
            |snapshot| {
                progress.push(snapshot.clone());
                Ok(())
            },
        )
        .unwrap();

        assert_eq!(progress.len(), 15);
        assert!(progress.windows(2).all(|pair| {
            pair[0].sequence + 1 == pair[1].sequence
                && pair[0].completed_internal_nodes + 1 == pair[1].completed_internal_nodes
                && pair[0].partition_coordinate_scores < pair[1].partition_coordinate_scores
        }));
        let final_progress = progress.last().unwrap();
        assert_eq!(final_progress.sequence, 15);
        assert_eq!(final_progress.completed_internal_nodes, 15);
        assert_eq!(
            final_progress.partition_coordinate_scores,
            evidence.partition_coordinate_scores
        );
        assert_eq!(final_progress.total_internal_nodes, 15);
    }

    #[test]
    fn v37_relation_preflight_progress_is_canonical_and_runner_visible() {
        let progress = super::V37TrainingProgress {
            completed_internal_nodes: 7,
            partition_coordinate_scores: 12_345_600,
            sequence: 7,
            total_internal_nodes: 15,
        };
        let bytes = super::canonical_v37_training_progress_bytes(&progress).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["schema"], "borsuk-v37-training-progress-v1");
        assert_eq!(value["sequence"], 7);
        assert_eq!(value["completed_internal_nodes"], 7);
        assert_eq!(value["total_internal_nodes"], 15);
        assert_eq!(value["partition_coordinate_scores"], 12_345_600);
        assert_eq!(bytes.last(), Some(&b'\n'));

        let _runner =
            super::run_v37_local_request_with_progress::<fn(&[u8]) -> std::io::Result<()>>;
    }

    #[test]
    fn v37_relation_preflight_local_run_is_exactly_bounded_and_source_free() {
        let root = tempfile::tempdir().unwrap();
        let mut manifest = authority();
        manifest.numeric.worker_count = 2;
        manifest.numeric.fma_backend =
            super::v37_fma_backend_name(super::v37_fused_kernel().unwrap()).to_owned();
        let manifest_bytes = canonical_v37_authority_bytes(&manifest).unwrap();
        let request = V37LocalRunRequest::try_new(
            V37LocalRunMode::PreflightTraining,
            vec![local_artifact_bytes(
                root.path(),
                "v37-authority",
                &manifest_bytes,
            )],
            Vec::new(),
            2,
        )
        .unwrap();
        let mut progress = Vec::new();
        let bytes = super::run_v37_local_request_with_progress(request, |snapshot| {
            progress.push(snapshot.to_vec());
            Ok(())
        })
        .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["schema"], "borsuk-v37-local-result-v3");
        assert_eq!(value["mode"], "preflight-training");
        assert_eq!(value["inputs"].as_array().unwrap().len(), 1);
        assert!(value["artifacts"].as_array().unwrap().is_empty());
        assert_eq!(value["training_evidence"]["rows"], 65_536);
        assert_eq!(value["training_evidence"]["dimensions"], 192);
        assert_eq!(value["training_evidence"]["leaf_count"], 16);
        assert_eq!(
            value["preflight_evidence"]["coordinate_generator"],
            "splitmix23-f32-v1"
        );
        assert_eq!(
            value["preflight_evidence"]["projected_construction_bytes"],
            2_137_615_120_u64
        );
        assert_eq!(
            value["preflight_evidence"]["scalar_fused_comparisons"],
            15_360
        );
        assert_eq!(value["preflight_evidence"]["scalar_fused_max_ulp_delta"], 0);
        assert_eq!(
            value["preflight_evidence"]["coordinate_sha256"]
                .as_str()
                .unwrap()
                .len(),
            64
        );
        assert_eq!(progress.len(), 15);
    }

    #[test]
    fn v37_relation_tree_fused_scores_match_registered_scalar_order() {
        for dimensions in [192, 197] {
            let mut row = Vec::with_capacity(dimensions);
            let mut normal = Vec::with_capacity(dimensions);
            for index in 0..dimensions {
                row.push(if index % 7 == 0 {
                    f32::from_bits(1)
                } else {
                    (index as f32 - 50.0) * 0.03125
                });
                normal.push(if index % 5 == 0 {
                    -0.0
                } else {
                    (91.0 - index as f32) * 0.015625
                });
            }
            let scalar = score_v37_hyperplane_scalar(&row, &normal).unwrap();
            let (fused, backend) = score_v37_hyperplane_fused(&row, &normal).unwrap();
            assert_eq!(fused.to_bits(), scalar.to_bits());
            assert_ne!(backend, "scalar-control");

            row.reverse();
            normal.reverse();
            assert_eq!(
                score_v37_hyperplane_fused(&row, &normal)
                    .unwrap()
                    .0
                    .to_bits(),
                score_v37_hyperplane_scalar(&row, &normal)
                    .unwrap()
                    .to_bits()
            );
        }
        assert!(score_v37_hyperplane_scalar(&[f32::NAN], &[1.0]).is_err());
        assert!(score_v37_hyperplane_scalar(&[1.0], &[1.0, 2.0]).is_err());
    }

    #[test]
    fn v37_relation_tree_locks_boundary_replay_and_query_equality() {
        assert!(route_v37_corpus_member(1.0, 7, 1.0, 7).unwrap());
        assert!(route_v37_corpus_member(1.0, 6, 1.0, 7).unwrap());
        assert!(!route_v37_corpus_member(1.0, 8, 1.0, 7).unwrap());
        assert!(!route_v37_corpus_member(1.5, 0, 1.0, 7).unwrap());

        let equal = route_v37_query(1.0, 1.0).unwrap();
        assert!(equal.primary_is_left);
        assert!(equal.queues_sibling_zero_margin);
        assert!(route_v37_query(f32::NAN, 1.0).is_err());

        let mut degenerate = training_rows(8, 192);
        for row in &mut degenerate {
            row.vector.fill(1.0);
        }
        assert!(
            train_v37_ownership_tree(
                &degenerate,
                V37TrainingShape {
                    dimensions: 192,
                    leaf_count: 2,
                    reservoir_rows: 8,
                    two_means_iterations: 8,
                },
                37,
                1,
                8,
            )
            .is_err()
        );
    }

    #[test]
    fn v37_relation_tree_arrow_round_trip_binds_exact_bytes_and_topology() {
        let rows = training_rows(12, 192);
        let shape = V37TrainingShape {
            dimensions: 192,
            leaf_count: 3,
            reservoir_rows: 8,
            two_means_iterations: 8,
        };
        let tree = train_v37_ownership_tree(&rows, shape, 37, 2, 4).unwrap();
        let artifact = encode_v37_tree_arrow(&tree).unwrap();
        assert!(artifact.bytes.starts_with(b"ARROW1"));
        assert_eq!(artifact.encoded_bytes, artifact.bytes.len() as u64);
        assert_eq!(artifact.sha256.len(), 64);
        assert_eq!(artifact.blake3.len(), 64);
        assert_eq!(encode_v37_tree_arrow(&tree).unwrap().bytes, artifact.bytes);
        let decoded = decode_v37_tree_arrow(
            &artifact.bytes,
            artifact.encoded_bytes,
            &artifact.sha256,
            &artifact.blake3,
        )
        .unwrap();
        assert_eq!(decoded.dimensions, tree.dimensions);
        assert_eq!(decoded.seed, tree.seed);
        assert_eq!(decoded.fma_backend, tree.fma_backend);
        assert_eq!(decoded.nodes, tree.nodes);
        assert_eq!(decoded.leaf_populations, tree.leaf_populations);
        assert!(decoded.assignments.is_empty());

        let mut corrupted = artifact.bytes.clone();
        let middle = corrupted.len() / 2;
        corrupted[middle] ^= 1;
        assert!(
            decode_v37_tree_arrow(
                &corrupted,
                artifact.encoded_bytes,
                &artifact.sha256,
                &artifact.blake3,
            )
            .is_err()
        );
        assert!(
            decode_v37_tree_arrow(
                &artifact.bytes,
                artifact.encoded_bytes + 1,
                &artifact.sha256,
                &artifact.blake3,
            )
            .is_err()
        );
    }

    #[test]
    fn v37_relation_tree_separates_geometry_arrow_from_ownership_parquet() {
        use arrow_ipc::reader::FileReader;
        use std::io::Cursor;

        let rows = training_rows(12, 192);
        let tree = train_v37_ownership_tree(
            &rows,
            V37TrainingShape {
                dimensions: 192,
                leaf_count: 3,
                reservoir_rows: 8,
                two_means_iterations: 8,
            },
            37,
            1,
            12,
        )
        .unwrap();
        let tree_artifact = encode_v37_tree_arrow(&tree).unwrap();
        let mut reader = FileReader::try_new(Cursor::new(&tree_artifact.bytes), None).unwrap();
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), tree.nodes.len() + 1);

        let feature_ids = (0..rows.len())
            .map(|ordinal| 10_000 + ordinal as u64 * 7)
            .collect::<Vec<_>>();
        let ownership =
            encode_v37_ownership_parquet(&tree.assignments, &feature_ids, &tree.leaf_populations)
                .unwrap();
        assert!(ownership.bytes.starts_with(b"PAR1"));
        assert_eq!(ownership.encoded_bytes, ownership.bytes.len() as u64);
        assert_eq!(
            decode_v37_ownership_parquet(
                &ownership.bytes,
                ownership.encoded_bytes,
                &ownership.sha256,
                &ownership.blake3,
                &tree.leaf_populations,
            )
            .unwrap(),
            tree.assignments
                .iter()
                .zip(&feature_ids)
                .enumerate()
                .map(
                    |(source_ordinal, (assignment, feature_row_id))| V37OwnershipRecord {
                        source_ordinal: source_ordinal as u64,
                        feature_row_id: *feature_row_id,
                        posting_ordinal: assignment.posting_ordinal,
                        posting_local_ordinal: tree.assignments[..source_ordinal]
                            .iter()
                            .filter(|prior| prior.posting_ordinal == assignment.posting_ordinal)
                            .count() as u32,
                    }
                )
                .collect::<Vec<_>>()
        );

        let mut invalid_posting = tree.assignments.clone();
        invalid_posting[0].posting_ordinal = 1_000;
        assert!(
            encode_v37_ownership_parquet(&invalid_posting, &feature_ids, &tree.leaf_populations)
                .is_err()
        );

        let mut duplicate_feature_ids = feature_ids;
        duplicate_feature_ids[1] = duplicate_feature_ids[0];
        assert!(
            encode_v37_ownership_parquet(
                &tree.assignments,
                &duplicate_feature_ids,
                &tree.leaf_populations
            )
            .is_err()
        );
    }

    #[test]
    fn v37_relation_tree_arrow_rejects_graph_and_numeric_drift() {
        let rows = training_rows(12, 192);
        let shape = V37TrainingShape {
            dimensions: 192,
            leaf_count: 3,
            reservoir_rows: 8,
            two_means_iterations: 8,
        };
        let tree = train_v37_ownership_tree(&rows, shape, 37, 1, 12).unwrap();

        let mut invalid_child = tree.clone();
        invalid_child.nodes[0].left_node = Some(999);
        assert!(encode_v37_tree_arrow(&invalid_child).is_err());

        let mut non_preorder = tree.clone();
        let left = non_preorder.nodes[0].left_node;
        non_preorder.nodes[0].left_node = non_preorder.nodes[0].right_node;
        non_preorder.nodes[0].right_node = left;
        assert!(encode_v37_tree_arrow(&non_preorder).is_err());

        let mut invalid_normal = tree.clone();
        invalid_normal.nodes[0].normal[0] = f32::NAN;
        assert!(encode_v37_tree_arrow(&invalid_normal).is_err());

        let mut duplicate_posting = tree.clone();
        let first_posting = duplicate_posting
            .nodes
            .iter()
            .find_map(|node| node.posting_ordinal)
            .unwrap();
        let second_leaf = duplicate_posting
            .nodes
            .iter()
            .enumerate()
            .filter(|(_, node)| node.posting_ordinal.is_some())
            .nth(1)
            .map(|(index, _)| index)
            .unwrap();
        duplicate_posting.nodes[second_leaf].posting_ordinal = Some(first_posting);
        assert!(encode_v37_tree_arrow(&duplicate_posting).is_err());

        let mut reordered_postings = tree.clone();
        let leaf_indices = reordered_postings
            .nodes
            .iter()
            .enumerate()
            .filter(|(_, node)| node.posting_ordinal.is_some())
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        let first = reordered_postings.nodes[leaf_indices[0]].posting_ordinal;
        reordered_postings.nodes[leaf_indices[0]].posting_ordinal =
            reordered_postings.nodes[leaf_indices[1]].posting_ordinal;
        reordered_postings.nodes[leaf_indices[1]].posting_ordinal = first;
        assert!(encode_v37_tree_arrow(&reordered_postings).is_err());

        let mut missing_assignment = tree.clone();
        missing_assignment.assignments.pop();
        assert!(encode_v37_tree_arrow(&missing_assignment).is_err());
    }

    #[test]
    fn v37_relation_tree_rejects_quota_preserving_population_drift() {
        let rows = training_rows(12, 192);
        let shape = V37TrainingShape {
            dimensions: 192,
            leaf_count: 3,
            reservoir_rows: 8,
            two_means_iterations: 8,
        };
        let mut tree = train_v37_ownership_tree(&rows, shape, 37, 1, 12).unwrap();
        let leaf_nodes = tree
            .nodes
            .iter()
            .enumerate()
            .filter_map(|(index, node)| node.posting_ordinal.map(|posting| (posting, index)))
            .collect::<Vec<_>>();
        for ((_, node), population) in leaf_nodes.iter().zip([3_u64, 5, 4]) {
            tree.nodes[*node].population = population;
        }
        tree.leaf_populations = vec![3, 5, 4];
        for (index, assignment) in tree.assignments.iter_mut().enumerate() {
            assignment.posting_ordinal = if index < 3 {
                0
            } else if index < 8 {
                1
            } else {
                2
            };
        }
        for index in (0..tree.nodes.len()).rev() {
            if let (Some(left), Some(right)) =
                (tree.nodes[index].left_node, tree.nodes[index].right_node)
            {
                tree.nodes[index].population =
                    tree.nodes[left as usize].population + tree.nodes[right as usize].population;
            }
        }
        assert!(encode_v37_tree_arrow(&tree).is_err());
    }

    #[test]
    fn v37_relation_tree_replays_persisted_planes_and_backend() {
        let rows = training_rows(12, 192);
        let tree = train_v37_ownership_tree(
            &rows,
            V37TrainingShape {
                dimensions: 192,
                leaf_count: 3,
                reservoir_rows: 8,
                two_means_iterations: 8,
            },
            37,
            2,
            4,
        )
        .unwrap();
        let artifact = encode_v37_tree_arrow(&tree).unwrap();
        let persisted_tree = decode_v37_tree_arrow(
            &artifact.bytes,
            artifact.encoded_bytes,
            &artifact.sha256,
            &artifact.blake3,
        )
        .unwrap();
        for row in &rows {
            let expected = tree
                .assignments
                .iter()
                .find(|assignment| assignment.source_ordinal == row.source_ordinal)
                .unwrap()
                .posting_ordinal;
            assert_eq!(
                route_v37_primary_leaf(
                    &persisted_tree,
                    &row.vector,
                    row.source_ordinal,
                    &tree.fma_backend,
                )
                .unwrap(),
                expected
            );
        }
        assert!(
            route_v37_primary_leaf(
                &persisted_tree,
                &rows[0].vector,
                rows[0].source_ordinal,
                "drift",
            )
            .is_err()
        );
    }

    #[test]
    fn v37_relation_tree_arrow_rejects_malformed_authority_without_panicking() {
        use arrow_array::{RecordBatch, UInt8Array};
        use arrow_ipc::{
            MetadataVersion,
            writer::{FileWriter, IpcWriteOptions},
        };
        use arrow_schema::{DataType, Field, Schema};
        use sha2::{Digest, Sha256};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "record_kind",
            DataType::UInt8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(UInt8Array::from(vec![2]))],
        )
        .unwrap();
        let mut bytes = Vec::new();
        let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5).unwrap();
        let mut writer =
            FileWriter::try_new_with_options(&mut bytes, schema.as_ref(), options).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        drop(writer);
        let sha256 = format!("{:x}", Sha256::digest(&bytes));
        let blake3 = blake3::hash(&bytes).to_hex().to_string();
        let outcome = std::panic::catch_unwind(|| {
            decode_v37_tree_arrow(&bytes, bytes.len() as u64, &sha256, &blake3)
        });
        assert!(outcome.is_ok());
        assert!(outcome.unwrap().is_err());
    }

    #[test]
    fn v37_relation_tree_repairs_empty_label_with_farthest_donor() {
        let rows = vec![
            V37TrainingRow {
                source_ordinal: 0,
                vector: vec![0.0; 192],
            },
            V37TrainingRow {
                source_ordinal: 1,
                vector: vec![2.0; 192],
            },
            V37TrainingRow {
                source_ordinal: 2,
                vector: vec![5.0; 192],
            },
        ];
        let mut zero = Vec::new();
        let mut one = vec![0, 1, 2];
        repair_v37_empty_partition(&rows, &mut zero, &mut one).unwrap();
        assert_eq!(zero, vec![2]);
        assert_eq!(one, vec![0, 1]);

        let tied = vec![
            V37TrainingRow {
                source_ordinal: 7,
                vector: vec![-1.0; 192],
            },
            V37TrainingRow {
                source_ordinal: 3,
                vector: vec![1.0; 192],
            },
        ];
        let mut zero = Vec::new();
        let mut one = vec![0, 1];
        repair_v37_empty_partition(&tied, &mut zero, &mut one).unwrap();
        assert_eq!(tied[zero[0]].source_ordinal, 3);
    }

    #[test]
    fn v37_relation_ceiling_sums_exact_largest_fourteen_owner_counts() {
        let assignments = (0_u64..200)
            .map(|source_ordinal| super::V37OwnershipAssignment {
                source_ordinal,
                posting_ordinal: if source_ordinal < 100 {
                    (source_ordinal % 20) as u32
                } else {
                    0
                },
            })
            .collect::<Vec<_>>();
        let truth = vec![
            V37GroundTruth {
                query_ordinal: 0,
                source_ordinals: (0..100).collect(),
            },
            V37GroundTruth {
                query_ordinal: 1,
                source_ordinals: (100..200).collect(),
            },
        ];
        let result = evaluate_v37_unique_owner_ceiling(&assignments, &truth, 14).unwrap();
        assert_eq!(
            result.samples[0].selected_postings,
            (0..14).collect::<Vec<_>>()
        );
        assert_eq!(result.samples[0].hits, 70);
        assert_eq!(result.samples[0].recall_ppm, 700_000);
        assert_eq!(result.samples[1].selected_postings, vec![0]);
        assert_eq!(result.samples[1].hits, 100);
        assert_eq!(result.aggregate_recall_ppm, 850_000);
        assert_eq!(result.minimum_recall_ppm, 700_000);
        assert!(!result.passed);
        assert_eq!(result.disposition, "layout-rejected");
    }

    #[test]
    fn v37_relation_ceiling_rejects_truth_and_result_drift() {
        let assignments = (0_u64..100)
            .map(|source_ordinal| super::V37OwnershipAssignment {
                source_ordinal,
                posting_ordinal: 0,
            })
            .collect::<Vec<_>>();
        let truth = vec![V37GroundTruth {
            query_ordinal: 0,
            source_ordinals: (0..100).collect(),
        }];
        let result = evaluate_v37_unique_owner_ceiling(&assignments, &truth, 14).unwrap();
        assert!(result.passed);
        assert_eq!(result.disposition, "ceiling-passed");
        let bytes = canonical_v37_ceiling_bytes(&result).unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));

        let mut drift = result.clone();
        drift.samples[0].hits = 99;
        assert!(canonical_v37_ceiling_bytes(&drift).is_err());

        let mut short = truth.clone();
        short[0].source_ordinals.pop();
        assert!(evaluate_v37_unique_owner_ceiling(&assignments, &short, 14).is_err());
        let mut duplicate = truth.clone();
        duplicate[0].source_ordinals[99] = 0;
        assert!(evaluate_v37_unique_owner_ceiling(&assignments, &duplicate, 14).is_err());
        let mut unknown = truth;
        unknown[0].source_ordinals[99] = 999;
        assert!(evaluate_v37_unique_owner_ceiling(&assignments, &unknown, 14).is_err());
    }

    #[test]
    fn v37_relation_ceiling_maps_nonordinal_feature_ids_without_source_or_query_capability() {
        let ownership = (0_u64..100)
            .map(|source_ordinal| V37OwnershipRecord {
                source_ordinal,
                feature_row_id: 50_000 + source_ordinal * 17,
                posting_ordinal: (source_ordinal % 10) as u32,
                posting_local_ordinal: (source_ordinal / 10) as u32,
            })
            .collect::<Vec<_>>();
        let feature_truth = vec![V37FeatureGroundTruth {
            query_ordinal: 0,
            feature_row_ids: ownership.iter().map(|row| row.feature_row_id).collect(),
        }];
        let mapped = map_v37_feature_ground_truth(&ownership, &feature_truth).unwrap();
        assert_eq!(mapped[0].source_ordinals, (0_u64..100).collect::<Vec<_>>());
        let ceiling = evaluate_v37_unique_owner_ceiling(
            &ownership
                .iter()
                .map(|row| super::V37OwnershipAssignment {
                    source_ordinal: row.source_ordinal,
                    posting_ordinal: row.posting_ordinal,
                })
                .collect::<Vec<_>>(),
            &mapped,
            14,
        )
        .unwrap();
        assert!(ceiling.passed);

        let mut unknown = feature_truth.clone();
        unknown[0].feature_row_ids[99] = 999;
        assert!(map_v37_feature_ground_truth(&ownership, &unknown).is_err());
        let mut duplicate_ownership = ownership;
        duplicate_ownership[99].feature_row_id = duplicate_ownership[0].feature_row_id;
        assert!(map_v37_feature_ground_truth(&duplicate_ownership, &feature_truth).is_err());
    }

    #[test]
    fn v37_relation_direct_selects_exact_fourteen_without_truth() {
        let rows = training_rows(128, 192);
        let tree = train_v37_ownership_tree(
            &rows,
            V37TrainingShape {
                dimensions: 192,
                leaf_count: 16,
                reservoir_rows: 32,
                two_means_iterations: 8,
            },
            37,
            4,
            17,
        )
        .unwrap();
        let router = prepare_v37_direct_router(&tree, &tree.fma_backend, 31).unwrap();
        let selected = select_v37_direct_postings(&router, &rows[17].vector).unwrap();
        assert_eq!(selected.selected_postings.len(), 14);
        assert_eq!(
            selected
                .selected_postings
                .iter()
                .copied()
                .collect::<BTreeSet<_>>()
                .len(),
            14
        );
        assert!(selected.node_visits <= 31);
        assert!(selected.scored_internal_nodes <= 15);
        assert_eq!(selected.fma_backend, tree.fma_backend);

        let artifact = encode_v37_tree_arrow(&tree).unwrap();
        let persisted = decode_v37_tree_arrow(
            &artifact.bytes,
            artifact.encoded_bytes,
            &artifact.sha256,
            &artifact.blake3,
        )
        .unwrap();
        let persisted_router =
            prepare_v37_direct_router(&persisted, &tree.fma_backend, 31).unwrap();
        assert_eq!(
            select_v37_direct_postings(&persisted_router, &rows[17].vector).unwrap(),
            selected
        );
    }

    #[test]
    fn v37_relation_direct_locks_equality_backend_and_visit_bounds() {
        let rows = training_rows(128, 192);
        let mut tree = train_v37_ownership_tree(
            &rows,
            V37TrainingShape {
                dimensions: 192,
                leaf_count: 16,
                reservoir_rows: 32,
                two_means_iterations: 8,
            },
            37,
            1,
            128,
        )
        .unwrap();
        for node in &mut tree.nodes {
            if node.posting_ordinal.is_none() {
                node.normal.fill(0.0);
                node.normal[0] = 1.0;
                node.boundary_score_bits = 0.0_f32.to_bits();
            }
        }
        let query = vec![0.0; 192];
        let router = prepare_v37_direct_router(&tree, &tree.fma_backend, 31).unwrap();
        let equal = select_v37_direct_postings(&router, &query).unwrap();
        assert_eq!(equal.selected_postings, (0..14).collect::<Vec<_>>());
        assert!(prepare_v37_direct_router(&tree, "scalar-control", 31).is_err());
        let short = prepare_v37_direct_router(&tree, &tree.fma_backend, 13).unwrap();
        assert!(select_v37_direct_postings(&short, &query).is_err());
        let mut nonfinite = query;
        nonfinite[0] = f32::NAN;
        assert!(select_v37_direct_postings(&router, &nonfinite).is_err());
    }

    #[test]
    fn v37_relation_direct_recomputes_recall_and_ceiling_gap_separately() {
        let assignments = (0_u64..200)
            .map(|source_ordinal| super::V37OwnershipAssignment {
                source_ordinal,
                posting_ordinal: if source_ordinal < 100 {
                    (source_ordinal % 20) as u32
                } else {
                    0
                },
            })
            .collect::<Vec<_>>();
        let truth = vec![
            V37GroundTruth {
                query_ordinal: 0,
                source_ordinals: (0..100).collect(),
            },
            V37GroundTruth {
                query_ordinal: 1,
                source_ordinals: (100..200).collect(),
            },
        ];
        let ceiling = evaluate_v37_unique_owner_ceiling(&assignments, &truth, 14).unwrap();
        let selections = vec![
            V37DirectSelectionRecord {
                query_ordinal: 0,
                selection: V37DirectSelection {
                    selected_postings: (0..14).collect(),
                    node_visits: 20,
                    scored_internal_nodes: 6,
                    fma_backend: "aarch64-neon-fma".to_owned(),
                },
            },
            V37DirectSelectionRecord {
                query_ordinal: 1,
                selection: V37DirectSelection {
                    selected_postings: (1..15).collect(),
                    node_visits: 22,
                    scored_internal_nodes: 8,
                    fma_backend: "aarch64-neon-fma".to_owned(),
                },
            },
        ];
        let result =
            evaluate_v37_direct_recall(&assignments, &truth, &ceiling, &selections).unwrap();
        assert_eq!(result.samples[0].hits, 70);
        assert_eq!(result.samples[0].ceiling_gap_hits, 0);
        assert_eq!(result.samples[1].hits, 0);
        assert_eq!(result.samples[1].ceiling_gap_hits, 100);
        assert_eq!(result.aggregate_recall_ppm, 350_000);
        assert_eq!(result.minimum_recall_ppm, 0);
        assert!(!result.passed);
        assert_eq!(result.disposition, "direct-failed");
        let bytes = canonical_v37_direct_bytes(&result).unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));

        let mut drift = result;
        drift.samples[0].ceiling_gap_hits = 1;
        assert!(canonical_v37_direct_bytes(&drift).is_err());

        let mut impossible_work =
            evaluate_v37_direct_recall(&assignments, &truth, &ceiling, &selections).unwrap();
        impossible_work.samples[0].node_visits -= 1;
        assert!(canonical_v37_direct_bytes(&impossible_work).is_err());

        let mut excessive_work =
            evaluate_v37_direct_recall(&assignments, &truth, &ceiling, &selections).unwrap();
        excessive_work.samples[0].node_visits = 246;
        assert!(canonical_v37_direct_bytes(&excessive_work).is_err());

        let foreign_assignments = (0_u64..200)
            .map(|source_ordinal| super::V37OwnershipAssignment {
                source_ordinal,
                posting_ordinal: 0,
            })
            .collect::<Vec<_>>();
        let foreign_ceiling =
            evaluate_v37_unique_owner_ceiling(&foreign_assignments, &truth, 14).unwrap();
        assert!(
            evaluate_v37_direct_recall(&assignments, &truth, &foreign_ceiling, &selections)
                .is_err()
        );
    }

    #[test]
    fn v37_relation_plane_builds_full_counts_before_exact_q24_prefixes() {
        let relation_membership = (0_u64..64)
            .map(|source_ordinal| super::V37OwnershipAssignment {
                source_ordinal,
                posting_ordinal: (source_ordinal % 2) as u32,
            })
            .collect::<Vec<_>>();
        let ownership = (0_u64..64)
            .map(|source_ordinal| super::V37OwnershipAssignment {
                source_ordinal,
                posting_ordinal: ((source_ordinal / 2) % 16) as u32,
            })
            .collect::<Vec<_>>();
        let plane =
            build_v37_relation_plane(&relation_membership, &ownership, 2, 16, &[16, 32, 64])
                .unwrap();
        assert_eq!(plane.leaves.len(), 2);
        assert_eq!(plane.leaves[0].population, 32);
        assert_eq!(plane.leaves[0].records.len(), 16);
        assert_eq!(plane.leaves[0].records[0].posting_ordinal, 0);
        assert_eq!(plane.leaves[0].records[0].count, 2);
        assert_eq!(plane.leaves[0].records[0].mass_q24, 1_048_576);
        assert_eq!(plane.leaves[1].records[15].posting_ordinal, 15);

        let mut reordered_membership = relation_membership.clone();
        let mut reordered_ownership = ownership.clone();
        reordered_membership.reverse();
        reordered_ownership.reverse();
        assert_eq!(
            build_v37_relation_plane(
                &reordered_membership,
                &reordered_ownership,
                2,
                16,
                &[16, 32, 64],
            )
            .unwrap(),
            plane
        );

        let selected = reduce_v37_relation_postings(&plane, &[0], 16, 15).unwrap();
        assert_eq!(selected.selected_postings, (0..14).collect::<Vec<_>>());
        assert_eq!(selected.touched_records, 16);

        let mut duplicate = relation_membership;
        duplicate[1].source_ordinal = 0;
        assert!(build_v37_relation_plane(&duplicate, &ownership, 2, 16, &[16, 32, 64]).is_err());
    }

    #[test]
    fn v37_relation_plane_votes_by_leaf_rank_and_rejects_candidate_shortage() {
        let relation_membership = (0_u64..46)
            .map(|source_ordinal| super::V37OwnershipAssignment {
                source_ordinal,
                posting_ordinal: (source_ordinal / 23) as u32,
            })
            .collect::<Vec<_>>();
        let ownership = (0_u64..46)
            .map(|source_ordinal| {
                let local = source_ordinal % 23;
                let leaf = source_ordinal / 23;
                let posting_ordinal = if local < 9 {
                    leaf as u32
                } else if leaf == 0 {
                    (local - 8) as u32
                } else if local == 9 {
                    0
                } else {
                    (local - 8) as u32
                };
                super::V37OwnershipAssignment {
                    source_ordinal,
                    posting_ordinal,
                }
            })
            .collect::<Vec<_>>();
        let plane =
            build_v37_relation_plane(&relation_membership, &ownership, 2, 15, &[16, 32, 64])
                .unwrap();
        let forward = reduce_v37_relation_postings(&plane, &[0, 1], 16, 14).unwrap();
        let reversed = reduce_v37_relation_postings(&plane, &[1, 0], 16, 14).unwrap();
        assert_eq!(forward.selected_postings[0], 0);
        assert_eq!(reversed.selected_postings[0], 1);

        let shortage_membership = (0_u64..4)
            .map(|source_ordinal| super::V37OwnershipAssignment {
                source_ordinal,
                posting_ordinal: 0,
            })
            .collect::<Vec<_>>();
        let shortage_ownership = (0_u64..4)
            .map(|source_ordinal| super::V37OwnershipAssignment {
                source_ordinal,
                posting_ordinal: (source_ordinal % 2) as u32,
            })
            .collect::<Vec<_>>();
        let shortage = build_v37_relation_plane(
            &shortage_membership,
            &shortage_ownership,
            1,
            14,
            &[16, 32, 64],
        )
        .unwrap();
        assert!(reduce_v37_relation_postings(&shortage, &[0], 16, 0).is_err());
    }

    #[test]
    fn v37_relation_plane_routes_bounded_independent_leaves_before_voting() {
        let rows = training_rows(512, 192);
        let tree = train_v37_ownership_tree(
            &rows,
            V37TrainingShape {
                dimensions: 192,
                leaf_count: 32,
                reservoir_rows: 64,
                two_means_iterations: 8,
            },
            73,
            4,
            64,
        )
        .unwrap();
        let relation_membership = tree.assignments.clone();
        let ownership = (0_u64..512)
            .map(|source_ordinal| super::V37OwnershipAssignment {
                source_ordinal,
                posting_ordinal: (source_ordinal % 16) as u32,
            })
            .collect::<Vec<_>>();
        let plane =
            build_v37_relation_plane(&relation_membership, &ownership, 32, 16, &[16, 32, 64])
                .unwrap();
        let encoded = encode_v37_relation_prefixes_arrow(&plane).unwrap();
        let prefixes = decode_v37_relation_prefixes_arrow(
            &encoded.bytes,
            encoded.encoded_bytes,
            &encoded.sha256,
            &encoded.blake3,
        )
        .unwrap();
        let prefixes = prepare_v37_relation_prefixes(&prefixes).unwrap();
        let router = prepare_v37_relation_router(&tree, &tree.fma_backend, 63).unwrap();
        for probes in [8, 16, 32] {
            let selected =
                select_v37_relation_postings(&router, &prefixes, &rows[0].vector, probes, 16, 15)
                    .unwrap();
            assert_eq!(selected.votes.selected_postings.len(), 14);
            assert_eq!(selected.routing.selected_postings.len(), probes as usize);
            assert!(selected.routing.node_visits <= 63);
            assert_eq!(selected.routing.fma_backend, tree.fma_backend);
        }
        assert!(prepare_v37_relation_router(&tree, &tree.fma_backend, 1_025).is_err());
        assert!(
            select_v37_relation_postings(&router, &prefixes, &rows[0].vector, 7, 16, 15).is_err()
        );
    }

    #[test]
    fn v37_relation_plane_codecs_bind_cross_language_structure_and_ties_even() {
        assert_eq!(v37_mass_q24(1, 1 << 25).unwrap(), 0);
        assert_eq!(v37_mass_q24(3, 1 << 25).unwrap(), 2);
        assert_eq!(v37_mass_q24(7, 7).unwrap(), 1 << 24);

        let relation_membership = (0_u64..320)
            .map(|source_ordinal| super::V37OwnershipAssignment {
                source_ordinal,
                posting_ordinal: (source_ordinal % 2) as u32,
            })
            .collect::<Vec<_>>();
        let ownership = (0_u64..320)
            .map(|source_ordinal| super::V37OwnershipAssignment {
                source_ordinal,
                posting_ordinal: ((source_ordinal / 2) % 80) as u32,
            })
            .collect::<Vec<_>>();
        let plane =
            build_v37_relation_plane(&relation_membership, &ownership, 2, 80, &[16, 32, 64])
                .unwrap();
        let mut duplicate_posting = plane.clone();
        duplicate_posting.leaves[0].records[1].posting_ordinal =
            duplicate_posting.leaves[0].records[0].posting_ordinal;
        assert!(encode_v37_relation_counts_parquet(&duplicate_posting).is_err());
        let counts = encode_v37_relation_counts_parquet(&plane).unwrap();
        assert_eq!(
            decode_v37_relation_counts_parquet(
                &counts.bytes,
                counts.encoded_bytes,
                &counts.sha256,
                &counts.blake3,
                2,
                80,
                &[16, 32, 64],
            )
            .unwrap(),
            plane
        );
        assert!(
            decode_v37_relation_counts_parquet(
                &counts.bytes,
                counts.encoded_bytes,
                &"0".repeat(64),
                &counts.blake3,
                2,
                80,
                &[16, 32, 64],
            )
            .is_err()
        );

        let prefixes = encode_v37_relation_prefixes_arrow(&plane).unwrap();
        let decoded = decode_v37_relation_prefixes_arrow(
            &prefixes.bytes,
            prefixes.encoded_bytes,
            &prefixes.sha256,
            &prefixes.blake3,
        )
        .unwrap();
        assert_eq!(decoded.prefix_lengths, vec![16, 32, 64]);
        assert_eq!(decoded.leaf_offsets.len(), 3);
        assert_eq!(decoded.leaf_offsets[0], vec![0, 16, 32]);
        assert_eq!(decoded.leaf_offsets[1], vec![0, 32, 64]);
        assert_eq!(decoded.leaf_offsets[2], vec![0, 64, 128]);
        assert_eq!(decoded.posting_ordinals[2].len(), 128);
        assert_eq!(decoded.masses_q24[2].len(), 128);
        let mut drift = decoded.clone();
        drift.leaf_offsets[0][1] += 1;
        assert!(validate_v37_relation_prefixes(&drift).is_err());

        let mut increasing_mass = decoded;
        for masses in &mut increasing_mass.masses_q24 {
            masses[0] = 0;
            masses[1] = 1;
        }
        assert!(validate_v37_relation_prefixes(&increasing_mass).is_err());
    }

    #[test]
    fn v37_relation_plane_recovers_a_posting_missed_by_direct_routing() {
        let ownership = (0_u64..128)
            .map(|source_ordinal| super::V37OwnershipAssignment {
                source_ordinal,
                posting_ordinal: if source_ordinal < 100 {
                    0
                } else {
                    1 + ((source_ordinal - 100) % 14) as u32
                },
            })
            .collect::<Vec<_>>();
        let relation_membership = (0_u64..128)
            .map(|source_ordinal| super::V37OwnershipAssignment {
                source_ordinal,
                posting_ordinal: (source_ordinal % 8) as u32,
            })
            .collect::<Vec<_>>();
        let plane =
            build_v37_relation_plane(&relation_membership, &ownership, 8, 15, &[16, 32, 64])
                .unwrap();
        let relation =
            reduce_v37_relation_postings(&plane, &(0..8).collect::<Vec<_>>(), 16, 14).unwrap();
        assert!(relation.selected_postings.contains(&0));

        let truth = vec![V37GroundTruth {
            query_ordinal: 0,
            source_ordinals: (0..100).collect(),
        }];
        let ceiling = evaluate_v37_unique_owner_ceiling(&ownership, &truth, 14).unwrap();
        let direct = evaluate_v37_direct_recall(
            &ownership,
            &truth,
            &ceiling,
            &[V37DirectSelectionRecord {
                query_ordinal: 0,
                selection: V37DirectSelection {
                    selected_postings: (1..=14).collect(),
                    node_visits: 20,
                    scored_internal_nodes: 6,
                    fma_backend: "aarch64-neon-fma".to_owned(),
                },
            }],
        )
        .unwrap();
        assert_eq!(direct.aggregate_recall_ppm, 0);

        let recovered = evaluate_v37_direct_recall(
            &ownership,
            &truth,
            &ceiling,
            &[V37DirectSelectionRecord {
                query_ordinal: 0,
                selection: V37DirectSelection {
                    selected_postings: relation.selected_postings,
                    node_visits: 22,
                    scored_internal_nodes: 8,
                    fma_backend: "aarch64-neon-fma".to_owned(),
                },
            }],
        )
        .unwrap();
        assert_eq!(recovered.aggregate_recall_ppm, 1_000_000);
        assert!(recovered.passed);
    }
}
