//! Query-blind hyperplane-boundary spill qualification for V38.

use std::{
    collections::BTreeSet,
    fs::{self, File},
    io::{BufReader, Read, Seek, SeekFrom, Write},
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use arrow_array::{Array, Float32Array, RecordBatch, UInt8Array, UInt32Array, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::Compression,
    file::properties::WriterProperties,
};
use rayon::{ThreadPoolBuilder, prelude::*};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{BorsukError, Result};

const V38_CORPUS_ROWS: u64 = 1_000_000;
const V38_POSTING_COUNT: u32 = 123;
const V38_MAXIMUM_ALTERNATE_ASSIGNMENTS: u64 = 250_000;
const V38_MAXIMUM_OWNERS_PER_ROW: u8 = 2;
const V38_MAXIMUM_ROWS_PER_POSTING: u32 = 10_240;
const V38_PROJECTED_RECORD_BYTES: u32 = 48;
const V38_PROJECTED_FRAMING_ALLOWANCE_BYTES: u32 = 32_768;
const V38_SELECTED_POSTINGS: u32 = 14;
const V38_AGGREGATE_GATE_PPM: u32 = 998_000;
const V38_MINIMUM_GATE_PPM: u32 = 800_000;
const V38_QUERY_COUNT: u32 = 1_000;
const V38_GT_NEIGHBORS: u32 = 100;
const V38_MAXIMUM_QUERY_SOLVER_NODES: u64 = 250_000;
const V38_MAXIMUM_SOLVER_NODES: u64 = 25_000_000;
const V38_MAXIMUM_CERTIFICATE_BYTES: u32 = 256;
const V38_MAXIMUM_WORKERS: u32 = 32;
const V38_V37_MEASURED_PEAK_BYTES: u64 = 2_716_549_120;
const V38_AUXILIARY_MEMORY_LIMIT_BYTES: u64 = 64 * 1_048_576;
const V38_PROJECTED_PEAK_BYTES: u64 =
    V38_V37_MEASURED_PEAK_BYTES + V38_AUXILIARY_MEMORY_LIMIT_BYTES;
const V38_PREDECESSOR_REPLAY_ALLOWANCE_NS: u64 = 120_000_000_000;

/// One strict local phase of the claim-ineligible V38 diagnostic.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum V38LocalRunMode {
    /// Measure construction primitives on a deterministic source-free shape.
    PreflightSpill,
    /// Authenticate V37 construction evidence and build the query-blind spill relation.
    BuildSpill,
    /// Authenticate only the sealed spill artifacts and development ground truth.
    EvaluateCeiling,
}

impl V38LocalRunMode {
    fn input_roles(self) -> &'static [&'static str] {
        match self {
            Self::PreflightSpill => &["v38-authority"],
            Self::BuildSpill => &[
                "v38-authority",
                "v37-authority",
                "v37-build-manifest",
                "v37-local-result",
                "v37-terminal",
                "v37-tree",
                "v37-ownership",
                "source",
            ],
            Self::EvaluateCeiling => &[
                "v38-ceiling-authority",
                "v38-construction-result",
                "spill-relation",
                "spill-postings",
                "gt100",
            ],
        }
    }

    fn output_roles(self) -> &'static [&'static str] {
        match self {
            Self::PreflightSpill => &[],
            Self::BuildSpill => &["spill-relation", "spill-postings"],
            Self::EvaluateCeiling => &["ceiling"],
        }
    }
}

/// One authenticated local input; its URI is evidence identity, not a network capability.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V38LocalArtifact {
    role: String,
    path: PathBuf,
    uri: String,
    sha256: String,
    blake3: String,
    encoded_bytes: u64,
}

impl V38LocalArtifact {
    /// Construct one exact local artifact identity.
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
            return Err(invalid("V38 local artifact identity differs"));
        }
        Ok(artifact)
    }

    /// Return the phase-local role.
    pub fn role(&self) -> &str {
        &self.role
    }

    /// Return the authenticated local path without opening it.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// One explicit local output path with no storage capability.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V38LocalOutput {
    role: String,
    path: PathBuf,
}

impl V38LocalOutput {
    /// Construct an exact output role and local path.
    pub fn try_new(role: String, path: PathBuf) -> Result<Self> {
        if role.is_empty() || path.as_os_str().is_empty() {
            return Err(invalid("V38 local output differs"));
        }
        Ok(Self { role, path })
    }

    /// Return the phase-local role.
    pub fn role(&self) -> &str {
        &self.role
    }

    /// Return the explicit local output path.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Capability-separated request for exactly one V38 phase.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V38LocalRunRequest {
    mode: V38LocalRunMode,
    inputs: Vec<V38LocalArtifact>,
    outputs: Vec<V38LocalOutput>,
    workers: u32,
}

impl V38LocalRunRequest {
    /// Validate exact ordered roles and reject overlapping paths or evidence URIs.
    pub fn try_new(
        mode: V38LocalRunMode,
        inputs: Vec<V38LocalArtifact>,
        outputs: Vec<V38LocalOutput>,
        workers: u32,
    ) -> Result<Self> {
        let input_roles = inputs
            .iter()
            .map(V38LocalArtifact::role)
            .collect::<Vec<_>>();
        let output_roles = outputs.iter().map(V38LocalOutput::role).collect::<Vec<_>>();
        let mut paths = BTreeSet::new();
        let mut uris = BTreeSet::new();
        if input_roles != mode.input_roles()
            || output_roles != mode.output_roles()
            || !matches!(workers, 1 | 2 | 4 | 8 | 16 | 32)
            || inputs.iter().any(|input| !paths.insert(input.path()))
            || outputs.iter().any(|output| !paths.insert(output.path()))
            || inputs.iter().any(|input| !uris.insert(input.uri.as_str()))
        {
            return Err(invalid("V38 local phase capability differs"));
        }
        Ok(Self {
            mode,
            inputs,
            outputs,
            workers,
        })
    }

    /// Return the exact phase.
    pub const fn mode(&self) -> V38LocalRunMode {
        self.mode
    }

    /// Return input roles in canonical phase order.
    pub fn input_roles(&self) -> Vec<&str> {
        self.inputs.iter().map(V38LocalArtifact::role).collect()
    }

    /// Return output roles in canonical phase order.
    pub fn output_roles(&self) -> Vec<&str> {
        self.outputs.iter().map(V38LocalOutput::role).collect()
    }

    /// Return the fixed worker count.
    pub const fn workers(&self) -> u32 {
        self.workers
    }
}

#[derive(Debug)]
struct V38AuthenticatedInputStamp {
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
struct V38AuthenticatedLocalInputs(Vec<V38AuthenticatedInputStamp>);

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct V38PreflightEvidence {
    coordinate_generator: String,
    dimensions: u32,
    leaf_count: u32,
    predecessor_replay_allowance_ns: u64,
    projected_construction_elapsed_ns: u64,
    projected_peak_bytes: u64,
    projected_scoring_elapsed_ns: u64,
    projected_spill_post_scoring_elapsed_ns: u64,
    proposal_coordinate_scores: u64,
    proposal_coordinates_per_second: u64,
    relation_rows: u64,
    rows: u64,
    scalar_fused_comparisons: u64,
    scalar_fused_max_ulp_delta: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V38SpillSpec {
    corpus_rows: u64,
    posting_count: u32,
    maximum_alternate_assignments: u64,
    maximum_owners_per_row: u8,
    maximum_rows_per_posting: u32,
    projected_record_bytes: u32,
    projected_framing_allowance_bytes: u32,
    selected_postings: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V38SpillCapacity {
    total_posting_capacity: u64,
    maximum_total_assignments: u64,
    maximum_posting_payload_bytes: u64,
    maximum_complete_object_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V38ArtifactIdentity {
    blake3: String,
    encoded_bytes: u64,
    role: String,
    sha256: String,
    uri: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V38OutputTarget {
    role: String,
    uri: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V38ConstructionAuthority {
    algorithm: String,
    inputs: Vec<V38ArtifactIdentity>,
    metric: String,
    numeric_backend: String,
    outputs: Vec<V38OutputTarget>,
    schema: String,
    source_archive: V38ArtifactIdentity,
    source_commit: String,
    spec: V38SpillSpec,
    workers: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V38CeilingAuthority {
    aggregate_gate_ppm: u32,
    construction_result: V38ArtifactIdentity,
    development_ground_truth: V38ArtifactIdentity,
    gt_neighbors: u32,
    maximum_certificate_bytes: u32,
    maximum_query_solver_nodes: u64,
    maximum_solver_nodes: u64,
    minimum_gate_ppm: u32,
    posting_summary: V38ArtifactIdentity,
    query_count: u32,
    relation: V38ArtifactIdentity,
    schema: String,
    selected_postings: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct V37TerminalArtifact {
    blake3: String,
    encoded_bytes: u64,
    role: String,
    sha256: String,
    uri: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct V37TerminalObject {
    encoded_bytes: u64,
    role: String,
    sha256: String,
    uri: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct V37TerminalMonitor {
    elapsed_milliseconds: u64,
    peak_psi_full_ppm: u64,
    peak_rss_bytes: u64,
    swap_end_bytes: u64,
    swap_start_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct V37ConstructionTerminal {
    artifacts: Vec<V37TerminalArtifact>,
    binary_bytes: u64,
    binary_sha256: String,
    binary_uri: String,
    claim_eligible: bool,
    instance_id: String,
    manifest_bytes: u64,
    manifest_sha256: String,
    manifest_uri: String,
    monitor: V37TerminalMonitor,
    phase: String,
    progress: V37TerminalObject,
    result: V37TerminalObject,
    run_id: String,
    schema: String,
    source_archive_bytes: u64,
    source_archive_sha256: String,
    source_archive_uri: String,
    source_commit: String,
    status: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct V37BuildManifestOutput {
    role: String,
    uri: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct V37BuildManifest {
    claim_eligible: bool,
    inputs: Vec<V38ArtifactIdentity>,
    outputs: Vec<V37BuildManifestOutput>,
    phase: String,
    run_id: String,
    schema: String,
    source_commit: String,
    workers: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct V37LocalTrainingEvidence {
    dimensions: u64,
    fma_backend: String,
    leaf_count: u32,
    partition_coordinate_scores: u64,
    partition_coordinate_scores_per_second: u64,
    partition_scoring_elapsed_ns: u64,
    rows: u64,
    training_elapsed_ns: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct V37LocalResult {
    artifacts: Vec<V38ArtifactIdentity>,
    claim_eligible: bool,
    inputs: Vec<V38ArtifactIdentity>,
    mode: String,
    schema: String,
    training_evidence: V37LocalTrainingEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct V38ConstructionEvidence {
    accepted_alternates: u64,
    capacity_rejected: u64,
    exhausted: bool,
    maximum_posting_population: u64,
    owners_one: u64,
    owners_two: u64,
    proposed_alternates: u64,
    total_assignments: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct V38ConstructionResult {
    artifacts: Vec<V38ArtifactIdentity>,
    claim_eligible: bool,
    construction_evidence: V38ConstructionEvidence,
    inputs: Vec<V38ArtifactIdentity>,
    mode: String,
    schema: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct V38AdmissionResult {
    capacity_rejected: u64,
    exhausted: bool,
    records: Vec<V38SpillRecord>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct V38AdmissionEvidence {
    capacity_rejected: u64,
    exhausted: bool,
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

fn validate_artifact(identity: &V38ArtifactIdentity, role: &str) -> Result<()> {
    if identity.role != role
        || identity.encoded_bytes == 0
        || !valid_lower_hex_digest(&identity.sha256)
        || !valid_lower_hex_digest(&identity.blake3)
        || !valid_s3_object_uri(&identity.uri)
    {
        return Err(invalid("V38 artifact authority differs"));
    }
    Ok(())
}

fn same_terminal_artifact(
    observed: &V37TerminalArtifact,
    registered: &V38ArtifactIdentity,
    observed_role: &str,
) -> bool {
    observed.role == observed_role
        && observed.blake3 == registered.blake3
        && observed.encoded_bytes == registered.encoded_bytes
        && observed.sha256 == registered.sha256
        && observed.uri == registered.uri
}

fn validate_v37_terminal_object(object: &V37TerminalObject, role: &str) -> bool {
    object.role == role
        && object.encoded_bytes > 0
        && valid_lower_hex_digest(&object.sha256)
        && valid_s3_object_uri(&object.uri)
}

fn local_input<'a>(request: &'a V38LocalRunRequest, role: &str) -> Result<&'a V38LocalArtifact> {
    request
        .inputs
        .iter()
        .find(|input| input.role == role)
        .ok_or_else(|| invalid("V38 local input role is absent"))
}

fn local_output<'a>(request: &'a V38LocalRunRequest, role: &str) -> Result<&'a V38LocalOutput> {
    request
        .outputs
        .iter()
        .find(|output| output.role == role)
        .ok_or_else(|| invalid("V38 local output role is absent"))
}

fn local_artifact_identity(input: &V38LocalArtifact) -> V38ArtifactIdentity {
    V38ArtifactIdentity {
        blake3: input.blake3.clone(),
        encoded_bytes: input.encoded_bytes,
        role: input.role.clone(),
        sha256: input.sha256.clone(),
        uri: input.uri.clone(),
    }
}

fn local_matches_identity(input: &V38LocalArtifact, expected: &V38ArtifactIdentity) -> bool {
    input.role == expected.role
        && input.uri == expected.uri
        && input.sha256 == expected.sha256
        && input.blake3 == expected.blake3
        && input.encoded_bytes == expected.encoded_bytes
}

fn authenticated_v38_input_file(
    request: &V38LocalRunRequest,
    authenticated: &V38AuthenticatedLocalInputs,
    role: &str,
) -> Result<File> {
    let index = request
        .inputs
        .iter()
        .position(|input| input.role == role)
        .ok_or_else(|| invalid("V38 authenticated input role is absent"))?;
    authenticated
        .0
        .get(index)
        .ok_or_else(|| invalid("V38 authenticated input count differs"))?
        .file
        .try_clone()
        .map_err(|source| BorsukError::Io {
            path: request.inputs[index].path.clone(),
            source,
        })
}

fn bytes_match_identity(bytes: &[u8], identity: &V38ArtifactIdentity) -> bool {
    bytes.len() as u64 == identity.encoded_bytes
        && format!("{:x}", Sha256::digest(bytes)) == identity.sha256
        && blake3::hash(bytes).to_hex().as_str() == identity.blake3
}

fn valid_git_sha(value: &str) -> bool {
    value.len() == 40
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn valid_v37_token(value: &str) -> bool {
    let bytes = value.as_bytes();
    (1..=128).contains(&bytes.len())
        && bytes[0].is_ascii_alphanumeric()
        && bytes[1..]
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(*byte, b'.' | b'_' | b'-'))
}

fn v37_manifest_uris_are_disjoint(manifest: &V37BuildManifest) -> bool {
    let mut uris = BTreeSet::new();
    manifest
        .inputs
        .iter()
        .map(|input| input.uri.as_str())
        .chain(manifest.outputs.iter().map(|output| output.uri.as_str()))
        .all(|uri| uris.insert(uri))
}

fn same_terminal_object(
    observed: &V37TerminalObject,
    expected: &V38ArtifactIdentity,
    role: &str,
) -> bool {
    observed.role == role
        && observed.uri == expected.uri
        && observed.sha256 == expected.sha256
        && observed.encoded_bytes == expected.encoded_bytes
}

fn same_local_output(
    observed: &V38ArtifactIdentity,
    published: &V38ArtifactIdentity,
    role: &str,
) -> bool {
    let Some(path) = observed.uri.strip_prefix("file://") else {
        return false;
    };
    Path::new(path).is_absolute()
        && Path::new(path).file_name().and_then(|name| name.to_str()) == Some(role)
        && observed.role == role
        && observed.sha256 == published.sha256
        && observed.blake3 == published.blake3
        && observed.encoded_bytes == published.encoded_bytes
}

fn projected_v37_partition_coordinate_scores(
    rows: u64,
    leaves: u32,
    dimensions: u64,
) -> Result<u64> {
    if rows == 0 || leaves < 2 || dimensions == 0 {
        return Err(invalid("V37 partition work authority differs"));
    }
    let mut pending = vec![(rows, u64::from(leaves))];
    let mut scored_rows = 0_u64;
    while let Some((node_rows, node_leaves)) = pending.pop() {
        if node_leaves == 1 {
            continue;
        }
        let quota = crate::v37_relation_router::project_v37_child_quota(node_rows, node_leaves)?;
        scored_rows = scored_rows
            .checked_add(node_rows)
            .ok_or_else(|| invalid("V37 partition work overflows"))?;
        pending.push((quota.right_rows, quota.right_leaves));
        pending.push((quota.left_rows, quota.left_leaves));
    }
    scored_rows
        .checked_mul(dimensions)
        .ok_or_else(|| invalid("V37 partition work overflows"))
}

fn validate_v38_v37_construction_evidence(
    authority: &V38ConstructionAuthority,
    v37_authority_bytes: &[u8],
    manifest_bytes: &[u8],
    result_bytes: &[u8],
    terminal_bytes: &[u8],
) -> Result<crate::v37_relation_router::V37V38ConstructionBinding> {
    validate_construction_authority(authority)?;
    let v37_authority_identity = authority
        .inputs
        .first()
        .ok_or_else(|| invalid("V38 V37 authority is absent"))?;
    let manifest_identity = authority
        .inputs
        .get(1)
        .ok_or_else(|| invalid("V38 V37 build manifest is absent"))?;
    let result_identity = authority
        .inputs
        .get(2)
        .ok_or_else(|| invalid("V38 V37 local result is absent"))?;
    let terminal_identity = authority
        .inputs
        .get(3)
        .ok_or_else(|| invalid("V38 V37 terminal is absent"))?;
    if !bytes_match_identity(v37_authority_bytes, v37_authority_identity)
        || !bytes_match_identity(manifest_bytes, manifest_identity)
        || !bytes_match_identity(result_bytes, result_identity)
        || !bytes_match_identity(terminal_bytes, terminal_identity)
    {
        return Err(invalid("V37 predecessor object bytes differ"));
    }
    let binding = crate::v37_relation_router::v37_v38_construction_binding(v37_authority_bytes)?;
    let manifest: V37BuildManifest = serde_json::from_slice(manifest_bytes)
        .map_err(|error| invalid(&format!("V37 build manifest parsing failed: {error}")))?;
    if canonical_bytes(&manifest, "V37 build manifest")? != manifest_bytes {
        return Err(invalid("V37 build manifest bytes are not canonical"));
    }
    let result: V37LocalResult = serde_json::from_slice(result_bytes)
        .map_err(|error| invalid(&format!("V37 local result parsing failed: {error}")))?;
    if canonical_bytes(&result, "V37 local result")? != result_bytes {
        return Err(invalid("V37 local result bytes are not canonical"));
    }
    let terminal: V37ConstructionTerminal =
        serde_json::from_slice(terminal_bytes).map_err(|error| {
            invalid(&format!(
                "V37 construction terminal parsing failed: {error}"
            ))
        })?;
    if canonical_bytes(&terminal, "V37 construction terminal")? != terminal_bytes {
        return Err(invalid("V37 construction terminal bytes are not canonical"));
    }
    let tree = authority
        .inputs
        .get(4)
        .ok_or_else(|| invalid("V38 V37 tree authority is absent"))?;
    let ownership = authority
        .inputs
        .get(5)
        .ok_or_else(|| invalid("V38 V37 ownership authority is absent"))?;
    let source = authority
        .inputs
        .get(6)
        .ok_or_else(|| invalid("V38 source authority is absent"))?;
    let expected_partition_scores = projected_v37_partition_coordinate_scores(
        result.training_evidence.rows,
        result.training_evidence.leaf_count,
        result.training_evidence.dimensions,
    )?;
    let expected_partition_throughput =
        if result.training_evidence.partition_scoring_elapsed_ns == 0 {
            0
        } else {
            u64::try_from(
                u128::from(result.training_evidence.partition_coordinate_scores) * 1_000_000_000
                    / u128::from(result.training_evidence.partition_scoring_elapsed_ns),
            )
            .map_err(|_| invalid("V37 partition throughput overflows"))?
        };
    let manifest_roles = [
        "v36-authority",
        "v36-execution-authority",
        "v36-receipt",
        "v36-source-registry",
        "v37-authority",
        "source",
    ];
    if manifest.schema != "borsuk-v37-spot-phase-manifest-v1"
        || manifest.claim_eligible
        || manifest.phase != "build-ownership"
        || !valid_v37_token(&manifest.run_id)
        || !valid_git_sha(&manifest.source_commit)
        || manifest.workers != authority.workers
        || manifest.inputs.len() != manifest_roles.len()
        || manifest
            .inputs
            .iter()
            .zip(manifest_roles)
            .any(|(identity, role)| validate_artifact(identity, role).is_err())
        || manifest.inputs[4] != *v37_authority_identity
        || manifest.inputs[5] != *source
        || !v37_manifest_uris_are_disjoint(&manifest)
        || manifest.outputs.len() != 2
        || manifest.outputs[0].role != "ownership-tree"
        || manifest.outputs[0].uri != tree.uri
        || manifest.outputs[1].role != "ownership"
        || manifest.outputs[1].uri != ownership.uri
        || result.schema != "borsuk-v37-local-result-v3"
        || result.claim_eligible
        || result.mode != "build-ownership"
        || result.inputs != manifest.inputs
        || result.artifacts.len() != 2
        || !same_local_output(&result.artifacts[0], tree, "ownership-tree")
        || !same_local_output(&result.artifacts[1], ownership, "ownership")
        || result.training_evidence.rows != authority.spec.corpus_rows
        || result.training_evidence.dimensions != binding.dimensions
        || result.training_evidence.leaf_count != authority.spec.posting_count
        || result.training_evidence.fma_backend != binding.fma_backend
        || result.training_evidence.partition_coordinate_scores != expected_partition_scores
        || result.training_evidence.partition_scoring_elapsed_ns == 0
        || result.training_evidence.training_elapsed_ns
            < result.training_evidence.partition_scoring_elapsed_ns
        || result
            .training_evidence
            .partition_coordinate_scores_per_second
            != expected_partition_throughput
        || terminal.schema != "borsuk-v37-spot-terminal-v2"
        || terminal.claim_eligible
        || terminal.phase != "build-ownership"
        || terminal.status != "complete"
        || terminal.run_id != manifest.run_id
        || terminal.source_commit != manifest.source_commit
        || terminal.artifacts.len() != 2
        || !same_terminal_artifact(&terminal.artifacts[0], tree, "ownership-tree")
        || !same_terminal_artifact(&terminal.artifacts[1], ownership, "ownership")
        || !validate_v37_terminal_object(&terminal.progress, "progress")
        || !same_terminal_object(&terminal.result, result_identity, "local-result")
        || terminal.binary_bytes == 0
        || !valid_lower_hex_digest(&terminal.binary_sha256)
        || !valid_s3_object_uri(&terminal.binary_uri)
        || terminal.manifest_bytes != manifest_identity.encoded_bytes
        || terminal.manifest_sha256 != manifest_identity.sha256
        || terminal.manifest_uri != manifest_identity.uri
        || terminal.source_archive_bytes == 0
        || !valid_lower_hex_digest(&terminal.source_archive_sha256)
        || !valid_s3_object_uri(&terminal.source_archive_uri)
        || !valid_git_sha(&terminal.source_commit)
        || terminal.instance_id.is_empty()
        || terminal.monitor.elapsed_milliseconds == 0
        || terminal.monitor.peak_rss_bytes > 3 * 1_073_741_824
        || terminal.monitor.peak_psi_full_ppm > 750_000
        || terminal.monitor.swap_start_bytes != terminal.monitor.swap_end_bytes
        || binding.corpus_rows != authority.spec.corpus_rows
        || binding.dimensions != 192
        || binding.workers != authority.workers
        || binding.fma_backend != authority.numeric_backend
        || binding.source_uri != source.uri
        || binding.source_sha256 != source.sha256
        || binding.source_blake3 != source.blake3
        || binding.source_bytes != source.encoded_bytes
    {
        return Err(invalid("V37 construction evidence differs"));
    }
    Ok(binding)
}

fn authenticate_v38_local_request(
    request: &V38LocalRunRequest,
) -> Result<V38AuthenticatedLocalInputs> {
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
            return Err(invalid("V38 local input type differs"));
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
            return Err(invalid("V38 local input identity differs"));
        }
        let canonical = fs::canonicalize(&input.path).map_err(|source| BorsukError::Io {
            path: input.path.clone(),
            source,
        })?;
        if !input_paths.insert(canonical) {
            return Err(invalid("V38 local input paths overlap"));
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
                .ok_or_else(|| invalid("V38 local input length overflows"))?;
            sha256.update(&buffer[..read]);
            blake3.update(&buffer[..read]);
        }
        if observed_bytes != input.encoded_bytes
            || format!("{:x}", sha256.finalize()) != input.sha256
            || blake3.finalize().to_hex().as_str() != input.blake3
        {
            return Err(invalid("V38 local input bytes differ"));
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
            return Err(invalid("V38 local input changed during authentication"));
        }
        stamps.push(V38AuthenticatedInputStamp {
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
            Ok(_) => return Err(invalid("V38 local output already exists")),
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
            .ok_or_else(|| invalid("V38 local output parent differs"))?;
        let parent_metadata = fs::symlink_metadata(parent).map_err(|source| BorsukError::Io {
            path: parent.to_owned(),
            source,
        })?;
        if parent_metadata.file_type().is_symlink() || !parent_metadata.file_type().is_dir() {
            return Err(invalid("V38 local output parent differs"));
        }
        let canonical_parent = fs::canonicalize(parent).map_err(|source| BorsukError::Io {
            path: parent.to_owned(),
            source,
        })?;
        let file_name = output
            .path
            .file_name()
            .ok_or_else(|| invalid("V38 local output name differs"))?;
        let canonical = canonical_parent.join(file_name);
        if input_paths.contains(&canonical) || !output_paths.insert(canonical) {
            return Err(invalid("V38 local input/output paths overlap"));
        }
    }
    Ok(V38AuthenticatedLocalInputs(stamps))
}

fn validate_v38_local_input_stability(
    request: &V38LocalRunRequest,
    authenticated: &V38AuthenticatedLocalInputs,
) -> Result<()> {
    if request.inputs.len() != authenticated.0.len() {
        return Err(invalid("V38 authenticated input count differs"));
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
            || stamp
                .file
                .metadata()
                .map_err(|source| BorsukError::Io {
                    path: input.path.clone(),
                    source,
                })?
                .ino()
                != stamp.inode
        {
            return Err(invalid("V38 local input changed after authentication"));
        }
    }
    Ok(())
}

fn read_v38_authenticated_input(
    request: &V38LocalRunRequest,
    authenticated: &V38AuthenticatedLocalInputs,
    role: &str,
    maximum_bytes: u64,
) -> Result<Vec<u8>> {
    let index = request
        .inputs
        .iter()
        .position(|input| input.role == role)
        .ok_or_else(|| invalid("V38 authenticated input role is absent"))?;
    let input = &request.inputs[index];
    if maximum_bytes == 0 || input.encoded_bytes > maximum_bytes {
        return Err(invalid("V38 authenticated input length differs"));
    }
    let mut file = authenticated
        .0
        .get(index)
        .ok_or_else(|| invalid("V38 authenticated input count differs"))?
        .file
        .try_clone()
        .map_err(|source| BorsukError::Io {
            path: input.path.clone(),
            source,
        })?;
    file.seek(SeekFrom::Start(0))
        .map_err(|source| BorsukError::Io {
            path: input.path.clone(),
            source,
        })?;
    let capacity = usize::try_from(input.encoded_bytes)
        .map_err(|_| invalid("V38 authenticated input exceeds address space"))?;
    let mut bytes = Vec::with_capacity(capacity);
    file.read_to_end(&mut bytes)
        .map_err(|source| BorsukError::Io {
            path: input.path.clone(),
            source,
        })?;
    if bytes.len() as u64 != input.encoded_bytes {
        return Err(invalid("V38 authenticated input length differs"));
    }
    Ok(bytes)
}

fn stage_v38_output(path: &Path, bytes: &[u8]) -> Result<tempfile::NamedTempFile> {
    let parent = path
        .parent()
        .ok_or_else(|| invalid("V38 local output parent differs"))?;
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

fn publish_v38_output(path: &Path, bytes: &[u8]) -> Result<()> {
    stage_v38_output(path, bytes)?
        .persist_noclobber(path)
        .map_err(|error| BorsukError::Io {
            path: path.to_owned(),
            source: error.error,
        })?;
    Ok(())
}

fn publish_v38_output_pair(
    first_path: &Path,
    first_bytes: &[u8],
    second_path: &Path,
    second_bytes: &[u8],
) -> Result<()> {
    let first = stage_v38_output(first_path, first_bytes)?;
    let second = stage_v38_output(second_path, second_bytes)?;
    first
        .persist_noclobber(first_path)
        .map_err(|error| BorsukError::Io {
            path: first_path.to_owned(),
            source: error.error,
        })?;
    if let Err(error) = second.persist_noclobber(second_path) {
        fs::remove_file(first_path).map_err(|source| BorsukError::Io {
            path: first_path.to_owned(),
            source,
        })?;
        return Err(BorsukError::Io {
            path: second_path.to_owned(),
            source: error.error,
        });
    }
    Ok(())
}

fn parse_v38_construction_authority(bytes: &[u8]) -> Result<V38ConstructionAuthority> {
    let authority: V38ConstructionAuthority = serde_json::from_slice(bytes).map_err(|error| {
        invalid(&format!(
            "V38 construction authority parsing failed: {error}"
        ))
    })?;
    if canonical_v38_construction_authority_bytes(&authority)? != bytes {
        return Err(invalid(
            "V38 construction authority bytes are not canonical",
        ));
    }
    Ok(authority)
}

fn v38_preflight_coordinates(dimensions: usize) -> Result<Vec<f32>> {
    const ROWS: usize = 65_536;
    let values = ROWS
        .checked_mul(dimensions)
        .ok_or_else(|| invalid("V38 preflight shape overflows"))?;
    let mut coordinates = Vec::new();
    coordinates
        .try_reserve_exact(values)
        .map_err(|_| invalid("V38 preflight coordinates exceed capacity"))?;
    for row in 0..ROWS as u64 {
        for dimension in 0..dimensions as u64 {
            let mut value = 38_u64
                ^ row.wrapping_mul(0x9e37_79b9_7f4a_7c15)
                ^ dimension.wrapping_mul(0xbf58_476d_1ce4_e5b9);
            value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            value ^= value >> 31;
            let mantissa = u32::try_from(value >> 41)
                .map_err(|_| invalid("V38 preflight mantissa overflows"))?;
            coordinates.push(f32::from_bits(0x3f80_0000 | mantissa) - 1.5);
        }
    }
    Ok(coordinates)
}

fn v38_preflight_primary(
    tree: &crate::v37_relation_router::V37BalancedTree,
) -> Result<Vec<crate::v37_relation_router::V37OwnershipRecord>> {
    let mut local_ordinals = vec![0_u32; tree.leaf_populations.len()];
    let mut primary = Vec::with_capacity(tree.assignments.len());
    for (index, assignment) in tree.assignments.iter().enumerate() {
        let source_ordinal =
            u64::try_from(index).map_err(|_| invalid("V38 preflight source ordinal overflows"))?;
        let posting = usize::try_from(assignment.posting_ordinal)
            .map_err(|_| invalid("V38 preflight posting exceeds address space"))?;
        if assignment.source_ordinal != source_ordinal || posting >= local_ordinals.len() {
            return Err(invalid("V38 preflight primary assignment differs"));
        }
        let posting_local_ordinal = local_ordinals[posting];
        local_ordinals[posting] = posting_local_ordinal
            .checked_add(1)
            .ok_or_else(|| invalid("V38 preflight local ordinal overflows"))?;
        primary.push(crate::v37_relation_router::V37OwnershipRecord {
            source_ordinal,
            feature_row_id: 10_000_000 + source_ordinal,
            posting_ordinal: assignment.posting_ordinal,
            posting_local_ordinal,
        });
    }
    Ok(primary)
}

fn canonical_v38_progress_bytes(completed_rows: u64, total_rows: u64) -> Result<Vec<u8>> {
    if completed_rows > total_rows || total_rows == 0 {
        return Err(invalid("V38 preflight progress differs"));
    }
    canonical_bytes(
        &serde_json::json!({
            "completed_rows": completed_rows,
            "schema": "borsuk-v38-preflight-progress-v1",
            "total_rows": total_rows,
        }),
        "preflight progress",
    )
}

fn report_v38_build_scoring_progress<F>(
    completed_rows: u64,
    total_rows: u64,
    progress: &mut F,
) -> Result<()>
where
    F: FnMut(&[u8]) -> std::io::Result<()>,
{
    if completed_rows > total_rows || total_rows == 0 {
        return Err(invalid("V38 build progress differs"));
    }
    if completed_rows == total_rows {
        return Ok(());
    }
    progress(&canonical_v38_progress_bytes(completed_rows, total_rows)?).map_err(|source| {
        BorsukError::Io {
            path: PathBuf::from("<v38-build-progress>"),
            source,
        }
    })
}

fn canonical_v38_ceiling_progress_bytes(completed_queries: u32) -> Result<Vec<u8>> {
    if completed_queries > V38_QUERY_COUNT {
        return Err(invalid("V38 ceiling progress differs"));
    }
    canonical_bytes(
        &serde_json::json!({
            "completed_queries": completed_queries,
            "schema": "borsuk-v38-ceiling-progress-v1",
            "total_queries": V38_QUERY_COUNT,
        }),
        "ceiling progress",
    )
}

fn run_v38_preflight(
    request: &V38LocalRunRequest,
    authenticated: &V38AuthenticatedLocalInputs,
    progress: &mut impl FnMut(&[u8]) -> std::io::Result<()>,
) -> Result<Vec<u8>> {
    const ROWS: usize = 65_536;
    const DIMENSIONS: usize = 192;
    const LEAVES: usize = 16;
    const FULL_COORDINATE_SCORES: u64 = 23_424_000_000;

    let authority_bytes =
        read_v38_authenticated_input(request, authenticated, "v38-authority", 16 * 1_048_576)?;
    let authority = parse_v38_construction_authority(&authority_bytes)?;
    let (_, backend) = crate::v37_relation_router::score_v37_hyperplane_fused(
        &[0.0; DIMENSIONS],
        &[0.0; DIMENSIONS],
    )?;
    if authority.workers != request.workers
        || authority.numeric_backend != backend
        || authority.spec.corpus_rows != V38_CORPUS_ROWS
    {
        return Err(invalid("V38 preflight authority differs"));
    }
    progress(&canonical_v38_progress_bytes(0, ROWS as u64)?).map_err(|source| BorsukError::Io {
        path: PathBuf::from("<v38-preflight-progress>"),
        source,
    })?;
    let coordinates = v38_preflight_coordinates(DIMENSIONS)?;
    let tree = crate::v37_relation_router::train_v37_ownership_tree_resident_coordinates(
        &coordinates,
        crate::v37_relation_router::V37TrainingShape {
            dimensions: DIMENSIONS,
            leaf_count: LEAVES as u64,
            reservoir_rows: 4_096,
            two_means_iterations: 8,
        },
        38,
        request.workers as usize,
        65_536,
    )?;
    if tree.fma_backend != backend || tree.nodes.len() != 2 * LEAVES - 1 {
        return Err(invalid("V38 preflight tree differs"));
    }
    let (coordinate_rows, coordinate_remainder) = coordinates.as_chunks::<DIMENSIONS>();
    if !coordinate_remainder.is_empty() {
        return Err(invalid("V38 preflight coordinate shape differs"));
    }
    let mut scalar_fused_comparisons = 0_u64;
    for node in tree
        .nodes
        .iter()
        .filter(|node| node.posting_ordinal.is_none())
    {
        for row in coordinate_rows.iter().take(1_024) {
            let scalar =
                crate::v37_relation_router::score_v37_hyperplane_scalar(row, &node.normal)?;
            let (fused, observed_backend) =
                crate::v37_relation_router::score_v37_hyperplane_fused(row, &node.normal)?;
            if scalar.to_bits() != fused.to_bits() || observed_backend != backend {
                return Err(invalid("V38 preflight scalar/fused evidence differs"));
            }
            scalar_fused_comparisons = scalar_fused_comparisons
                .checked_add(1)
                .ok_or_else(|| invalid("V38 preflight comparison count overflows"))?;
        }
    }
    let primary = v38_preflight_primary(&tree)?;
    let rows = coordinate_rows
        .iter()
        .map(|row| row.to_vec())
        .collect::<Vec<_>>();
    let started = Instant::now();
    let relation =
        build_v38_spill_relation(&tree, &rows, &primary, 16_384, 10_240, request.workers)?;
    let scoring_elapsed_ns = u64::try_from(started.elapsed().as_nanos())
        .map_err(|_| invalid("V38 preflight elapsed time overflows"))?
        .max(1);
    let relation_bytes = encode_v38_spill_relation_parquet(&relation.records)?;
    let decoded_relation =
        decode_v38_spill_relation_parquet(&relation_bytes, &primary, LEAVES as u32, 10_240)?;
    let posting_bytes = encode_v38_posting_summary_parquet(&relation.postings)?;
    let decoded_postings = decode_v38_posting_summary_parquet(
        &posting_bytes,
        &decoded_relation,
        LEAVES as u32,
        10_240,
    )?;
    if decoded_relation != relation.records || decoded_postings != relation.postings {
        return Err(invalid("V38 preflight codec replay differs"));
    }
    let proposal_coordinate_scores = (ROWS as u64)
        .checked_mul((LEAVES - 1) as u64)
        .and_then(|scores| scores.checked_mul(DIMENSIONS as u64))
        .ok_or_else(|| invalid("V38 preflight work overflows"))?;
    let proposal_coordinates_per_second = u64::try_from(
        u128::from(proposal_coordinate_scores) * 1_000_000_000_u128
            / u128::from(scoring_elapsed_ns),
    )
    .map_err(|_| invalid("V38 preflight throughput overflows"))?;
    let projected_scoring_elapsed_ns = u64::try_from(
        u128::from(scoring_elapsed_ns) * u128::from(FULL_COORDINATE_SCORES)
            / u128::from(proposal_coordinate_scores),
    )
    .map_err(|_| invalid("V38 preflight scoring projection overflows"))?;
    let total_elapsed_ns = u64::try_from(started.elapsed().as_nanos())
        .map_err(|_| invalid("V38 preflight total elapsed time overflows"))?;
    let post_scoring_elapsed_ns = total_elapsed_ns
        .checked_sub(scoring_elapsed_ns)
        .ok_or_else(|| invalid("V38 preflight elapsed phases differ"))?;
    let projected_post_scoring_elapsed_ns = u64::try_from(
        u128::from(post_scoring_elapsed_ns) * u128::from(V38_CORPUS_ROWS) / ROWS as u128,
    )
    .map_err(|_| invalid("V38 preflight post-scoring projection overflows"))?;
    let projected_construction_elapsed_ns = projected_scoring_elapsed_ns
        .checked_add(projected_post_scoring_elapsed_ns)
        .and_then(|elapsed| elapsed.checked_add(V38_PREDECESSOR_REPLAY_ALLOWANCE_NS))
        .ok_or_else(|| invalid("V38 preflight construction projection overflows"))?;
    if proposal_coordinates_per_second < 78_080_000
        || projected_scoring_elapsed_ns > 300_000_000_000
        || projected_construction_elapsed_ns > 450_000_000_000
        || V38_PROJECTED_PEAK_BYTES > 3 * 1_073_741_824
    {
        return Err(invalid(&format!(
            "V38 preflight admission failed: coordinates_per_second={proposal_coordinates_per_second}, projected_scoring_elapsed_ns={projected_scoring_elapsed_ns}, projected_construction_elapsed_ns={projected_construction_elapsed_ns}, projected_peak_bytes={V38_PROJECTED_PEAK_BYTES}"
        )));
    }
    progress(&canonical_v38_progress_bytes(ROWS as u64, ROWS as u64)?).map_err(|source| {
        BorsukError::Io {
            path: PathBuf::from("<v38-preflight-progress>"),
            source,
        }
    })?;
    let evidence = V38PreflightEvidence {
        coordinate_generator: "splitmix23-f32-v1".to_owned(),
        dimensions: DIMENSIONS as u32,
        leaf_count: LEAVES as u32,
        predecessor_replay_allowance_ns: V38_PREDECESSOR_REPLAY_ALLOWANCE_NS,
        projected_construction_elapsed_ns,
        projected_peak_bytes: V38_PROJECTED_PEAK_BYTES,
        projected_scoring_elapsed_ns,
        projected_spill_post_scoring_elapsed_ns: projected_post_scoring_elapsed_ns,
        proposal_coordinate_scores,
        proposal_coordinates_per_second,
        relation_rows: relation.records.len() as u64,
        rows: ROWS as u64,
        scalar_fused_comparisons,
        scalar_fused_max_ulp_delta: 0,
    };
    let inputs = request
        .inputs
        .iter()
        .map(|input| V38ArtifactIdentity {
            blake3: input.blake3.clone(),
            encoded_bytes: input.encoded_bytes,
            role: input.role.clone(),
            sha256: input.sha256.clone(),
            uri: input.uri.clone(),
        })
        .collect::<Vec<_>>();
    canonical_bytes(
        &serde_json::json!({
            "claim_eligible": false,
            "inputs": inputs,
            "mode": "preflight-spill",
            "preflight_evidence": evidence,
            "schema": "borsuk-v38-local-result-v1",
        }),
        "local preflight result",
    )
}

fn run_v38_build(
    request: &V38LocalRunRequest,
    authenticated: &V38AuthenticatedLocalInputs,
    progress: &mut impl FnMut(&[u8]) -> std::io::Result<()>,
) -> Result<Vec<u8>> {
    let authority_bytes =
        read_v38_authenticated_input(request, authenticated, "v38-authority", 16 * 1_048_576)?;
    let authority = parse_v38_construction_authority(&authority_bytes)?;
    if request.workers != authority.workers {
        return Err(invalid("V38 construction worker binding differs"));
    }
    if request.inputs[1..]
        .iter()
        .zip(&authority.inputs)
        .any(|(input, expected)| !local_matches_identity(input, expected))
    {
        return Err(invalid("V38 construction input binding differs"));
    }
    let v37_authority_bytes =
        read_v38_authenticated_input(request, authenticated, "v37-authority", 16 * 1_048_576)?;
    let v37_manifest = local_input(request, "v37-build-manifest")?;
    let v37_manifest_bytes = read_v38_authenticated_input(
        request,
        authenticated,
        "v37-build-manifest",
        v37_manifest.encoded_bytes,
    )?;
    let v37_result = local_input(request, "v37-local-result")?;
    let v37_result_bytes = read_v38_authenticated_input(
        request,
        authenticated,
        "v37-local-result",
        v37_result.encoded_bytes,
    )?;
    let v37_terminal = local_input(request, "v37-terminal")?;
    let v37_terminal_bytes = read_v38_authenticated_input(
        request,
        authenticated,
        "v37-terminal",
        v37_terminal.encoded_bytes,
    )?;
    let binding = validate_v38_v37_construction_evidence(
        &authority,
        &v37_authority_bytes,
        &v37_manifest_bytes,
        &v37_result_bytes,
        &v37_terminal_bytes,
    )?;
    let tree_input = local_input(request, "v37-tree")?;
    let tree_bytes =
        read_v38_authenticated_input(request, authenticated, "v37-tree", tree_input.encoded_bytes)?;
    let tree = crate::v37_relation_router::decode_v37_tree_arrow(
        &tree_bytes,
        tree_input.encoded_bytes,
        &tree_input.sha256,
        &tree_input.blake3,
    )?;
    if tree.dimensions as u64 != binding.dimensions
        || tree.seed != binding.tree_seed
        || tree.fma_backend != binding.fma_backend
        || tree.leaf_populations.len() != authority.spec.posting_count as usize
        || tree.leaf_populations.iter().sum::<u64>() != authority.spec.corpus_rows
    {
        return Err(invalid("V38 V37 tree binding differs"));
    }
    let ownership_input = local_input(request, "v37-ownership")?;
    let ownership_bytes = read_v38_authenticated_input(
        request,
        authenticated,
        "v37-ownership",
        ownership_input.encoded_bytes,
    )?;
    let ownership = crate::v37_relation_router::decode_v37_ownership_parquet(
        &ownership_bytes,
        ownership_input.encoded_bytes,
        &ownership_input.sha256,
        &ownership_input.blake3,
        &tree.leaf_populations,
    )?;
    let source = local_input(request, "source")?;
    let feature_ids = ownership
        .iter()
        .map(|record| record.feature_row_id)
        .collect::<Vec<_>>();
    let projected = crate::v36_prefix_dataset::project_v36_prefix_source_resident_file(
        authenticated_v38_input_file(request, authenticated, "source")?,
        &source.path,
        &feature_ids,
        65_536,
    )?;
    if projected.projected_corpus_sha256() != binding.projected_corpus_sha256
        || projected.row_count() as u64 != authority.spec.corpus_rows
        || projected.dimensions() as u64 != binding.dimensions
    {
        return Err(invalid("V38 projected source replay differs"));
    }
    progress(&canonical_v38_progress_bytes(
        0,
        authority.spec.corpus_rows,
    )?)
    .map_err(|source| BorsukError::Io {
        path: PathBuf::from("<v38-build-progress>"),
        source,
    })?;
    let (relation, admission) = build_v38_spill_relation_flat_with_evidence_and_progress(
        &tree,
        projected.projected_coordinates(),
        projected.dimensions(),
        &ownership,
        V38SpillBuildLimits {
            maximum_alternates: authority.spec.maximum_alternate_assignments,
            maximum_rows_per_posting: authority.spec.maximum_rows_per_posting,
            workers: request.workers,
        },
        |completed_rows| {
            report_v38_build_scoring_progress(completed_rows, authority.spec.corpus_rows, progress)
        },
    )?;
    let relation_bytes = encode_v38_spill_relation_parquet(&relation.records)?;
    let postings_bytes = encode_v38_posting_summary_parquet(&relation.postings)?;
    let relation_output = local_output(request, "spill-relation")?;
    let postings_output = local_output(request, "spill-postings")?;
    let artifacts = vec![
        encoded_v38_artifact("spill-relation", &authority.outputs[0].uri, &relation_bytes),
        encoded_v38_artifact("spill-postings", &authority.outputs[1].uri, &postings_bytes),
    ];
    let accepted_alternates = relation.records.len() as u64 - authority.spec.corpus_rows;
    let result = V38ConstructionResult {
        artifacts,
        claim_eligible: false,
        construction_evidence: V38ConstructionEvidence {
            accepted_alternates,
            capacity_rejected: admission.capacity_rejected,
            exhausted: admission.exhausted,
            maximum_posting_population: relation
                .postings
                .iter()
                .map(|posting| posting.total_population)
                .max()
                .ok_or_else(|| invalid("V38 posting evidence is absent"))?,
            owners_one: authority.spec.corpus_rows - accepted_alternates,
            owners_two: accepted_alternates,
            proposed_alternates: authority.spec.corpus_rows,
            total_assignments: relation.records.len() as u64,
        },
        inputs: request.inputs.iter().map(local_artifact_identity).collect(),
        mode: "build-spill".to_owned(),
        schema: "borsuk-v38-local-result-v1".to_owned(),
    };
    let result_bytes = canonical_v38_construction_result_bytes(&result)?;
    progress(&canonical_v38_progress_bytes(
        authority.spec.corpus_rows,
        authority.spec.corpus_rows,
    )?)
    .map_err(|source| BorsukError::Io {
        path: PathBuf::from("<v38-build-progress>"),
        source,
    })?;
    validate_v38_local_input_stability(request, authenticated)?;
    publish_v38_output_pair(
        &relation_output.path,
        &relation_bytes,
        &postings_output.path,
        &postings_bytes,
    )?;
    Ok(result_bytes)
}

fn parse_v38_ceiling_authority(bytes: &[u8]) -> Result<V38CeilingAuthority> {
    let authority: V38CeilingAuthority = serde_json::from_slice(bytes)
        .map_err(|error| invalid(&format!("V38 ceiling authority parsing failed: {error}")))?;
    if canonical_v38_ceiling_authority_bytes(&authority)? != bytes {
        return Err(invalid("V38 ceiling authority bytes are not canonical"));
    }
    Ok(authority)
}

fn run_v38_ceiling(
    request: &V38LocalRunRequest,
    authenticated: &V38AuthenticatedLocalInputs,
    progress: &mut impl FnMut(&[u8]) -> std::io::Result<()>,
) -> Result<Vec<u8>> {
    let authority_bytes = read_v38_authenticated_input(
        request,
        authenticated,
        "v38-ceiling-authority",
        16 * 1_048_576,
    )?;
    let authority = parse_v38_ceiling_authority(&authority_bytes)?;
    for (role, expected) in [
        ("v38-construction-result", &authority.construction_result),
        ("spill-relation", &authority.relation),
        ("spill-postings", &authority.posting_summary),
        ("gt100", &authority.development_ground_truth),
    ] {
        if !local_matches_identity(local_input(request, role)?, expected) {
            return Err(invalid("V38 ceiling input binding differs"));
        }
    }
    let construction_input = local_input(request, "v38-construction-result")?;
    let construction_bytes = read_v38_authenticated_input(
        request,
        authenticated,
        "v38-construction-result",
        construction_input.encoded_bytes,
    )?;
    let construction = parse_v38_construction_result_bytes(&construction_bytes)?;
    if construction.artifacts
        != [
            authority.relation.clone(),
            authority.posting_summary.clone(),
        ]
    {
        return Err(invalid("V38 construction output binding differs"));
    }
    let relation_input = local_input(request, "spill-relation")?;
    let relation_bytes = read_v38_authenticated_input(
        request,
        authenticated,
        "spill-relation",
        relation_input.encoded_bytes,
    )?;
    let records = decode_v38_spill_relation_parquet_unbound(
        &relation_bytes,
        V38_CORPUS_ROWS as usize,
        V38_POSTING_COUNT,
        V38_MAXIMUM_ROWS_PER_POSTING,
    )?;
    let postings_input = local_input(request, "spill-postings")?;
    let postings_bytes = read_v38_authenticated_input(
        request,
        authenticated,
        "spill-postings",
        postings_input.encoded_bytes,
    )?;
    let postings = decode_v38_posting_summary_parquet(
        &postings_bytes,
        &records,
        V38_POSTING_COUNT,
        V38_MAXIMUM_ROWS_PER_POSTING,
    )?;
    let gt_input = local_input(request, "gt100")?;
    let truth = crate::v37_relation_router::load_v37_feature_ground_truth_file(
        authenticated_v38_input_file(request, authenticated, "gt100")?,
        &gt_input.path,
        V38_QUERY_COUNT,
    )?;
    let result = evaluate_v38_multi_owner_ceiling_with_progress(
        &authority,
        &V38SpillRelation { records, postings },
        &truth,
        |completed_queries| {
            progress(&canonical_v38_ceiling_progress_bytes(completed_queries)?).map_err(|source| {
                BorsukError::Io {
                    path: PathBuf::from("<v38-ceiling-progress>"),
                    source,
                }
            })
        },
    )?;
    let bytes = canonical_v38_ceiling_bytes(&result)?;
    validate_v38_local_input_stability(request, authenticated)?;
    publish_v38_output(&local_output(request, "ceiling")?.path, &bytes)?;
    Ok(bytes)
}

/// Run one capability-separated V38 local phase.
#[doc(hidden)]
pub fn run_v38_local_request(request: V38LocalRunRequest) -> Result<Vec<u8>> {
    run_v38_local_request_with_progress(request, |_| Ok(()))
}

/// Run one V38 phase while emitting canonical progress records.
#[doc(hidden)]
pub fn run_v38_local_request_with_progress<F>(
    request: V38LocalRunRequest,
    mut progress: F,
) -> Result<Vec<u8>>
where
    F: FnMut(&[u8]) -> std::io::Result<()>,
{
    let authenticated = authenticate_v38_local_request(&request)?;
    let result = match request.mode {
        V38LocalRunMode::PreflightSpill => {
            run_v38_preflight(&request, &authenticated, &mut progress)
        }
        V38LocalRunMode::BuildSpill => run_v38_build(&request, &authenticated, &mut progress),
        V38LocalRunMode::EvaluateCeiling => {
            run_v38_ceiling(&request, &authenticated, &mut progress)
        }
    }?;
    validate_v38_local_input_stability(&request, &authenticated)?;
    Ok(result)
}

pub(crate) fn project_v38_spill_capacity(spec: &V38SpillSpec) -> Result<V38SpillCapacity> {
    let total_posting_capacity = u64::from(spec.posting_count)
        .checked_mul(u64::from(spec.maximum_rows_per_posting))
        .ok_or_else(|| invalid("V38 posting capacity overflows"))?;
    let maximum_total_assignments = spec
        .corpus_rows
        .checked_add(spec.maximum_alternate_assignments)
        .ok_or_else(|| invalid("V38 assignment capacity overflows"))?;
    let maximum_posting_payload_bytes = u64::from(spec.maximum_rows_per_posting)
        .checked_mul(u64::from(spec.projected_record_bytes))
        .ok_or_else(|| invalid("V38 posting payload overflows"))?;
    let maximum_complete_object_bytes = maximum_posting_payload_bytes
        .checked_add(u64::from(spec.projected_framing_allowance_bytes))
        .ok_or_else(|| invalid("V38 complete object projection overflows"))?;
    Ok(V38SpillCapacity {
        total_posting_capacity,
        maximum_total_assignments,
        maximum_posting_payload_bytes,
        maximum_complete_object_bytes,
    })
}

pub(crate) fn validate_v38_spill_spec(spec: &V38SpillSpec) -> Result<()> {
    if spec.corpus_rows != V38_CORPUS_ROWS
        || spec.posting_count != V38_POSTING_COUNT
        || spec.maximum_alternate_assignments != V38_MAXIMUM_ALTERNATE_ASSIGNMENTS
        || spec.maximum_owners_per_row != V38_MAXIMUM_OWNERS_PER_ROW
        || spec.maximum_rows_per_posting != V38_MAXIMUM_ROWS_PER_POSTING
        || spec.projected_record_bytes != V38_PROJECTED_RECORD_BYTES
        || spec.projected_framing_allowance_bytes != V38_PROJECTED_FRAMING_ALLOWANCE_BYTES
        || spec.selected_postings != V38_SELECTED_POSTINGS
    {
        return Err(invalid("V38 spill specification differs"));
    }
    let projection = project_v38_spill_capacity(spec)?;
    if projection.total_posting_capacity < projection.maximum_total_assignments
        || projection.maximum_posting_payload_bytes != 491_520
        || projection.maximum_complete_object_bytes != 524_288
    {
        return Err(invalid("V38 spill capacity differs"));
    }
    Ok(())
}

fn validate_construction_authority(authority: &V38ConstructionAuthority) -> Result<()> {
    if authority.schema != "borsuk-v38-boundary-spill-authority-v1"
        || authority.algorithm != "query-blind-hyperplane-boundary-spill-v1"
        || authority.metric != "squared-l2"
        || !matches!(
            authority.numeric_backend.as_str(),
            "aarch64-neon-fma" | "x86-avx-fma"
        )
        || authority.workers == 0
        || authority.workers > V38_MAXIMUM_WORKERS
        || !valid_git_sha(&authority.source_commit)
    {
        return Err(invalid("V38 construction authority differs"));
    }
    validate_v38_spill_spec(&authority.spec)?;
    validate_artifact(&authority.source_archive, "source-archive")?;
    let roles = [
        "v37-authority",
        "v37-build-manifest",
        "v37-local-result",
        "v37-terminal",
        "v37-tree",
        "v37-ownership",
        "source",
    ];
    if authority.inputs.len() != roles.len() {
        return Err(invalid("V38 construction input authority differs"));
    }
    for (identity, role) in authority.inputs.iter().zip(roles) {
        validate_artifact(identity, role)?;
    }
    let output_roles = ["spill-relation", "spill-postings"];
    if authority.outputs.len() != output_roles.len() {
        return Err(invalid("V38 construction output authority differs"));
    }
    for (output, role) in authority.outputs.iter().zip(output_roles) {
        if output.role != role || !valid_s3_object_uri(&output.uri) {
            return Err(invalid("V38 construction output authority differs"));
        }
    }
    let mut roles = BTreeSet::new();
    let mut uris = BTreeSet::new();
    for (role, uri) in authority
        .inputs
        .iter()
        .map(|item| (item.role.as_str(), item.uri.as_str()))
        .chain(std::iter::once((
            authority.source_archive.role.as_str(),
            authority.source_archive.uri.as_str(),
        )))
        .chain(
            authority
                .outputs
                .iter()
                .map(|item| (item.role.as_str(), item.uri.as_str())),
        )
    {
        if !roles.insert(role) || !uris.insert(uri) {
            return Err(invalid("V38 construction roles overlap"));
        }
    }
    Ok(())
}

fn validate_ceiling_authority(authority: &V38CeilingAuthority) -> Result<()> {
    if authority.schema != "borsuk-v38-boundary-spill-ceiling-authority-v1"
        || authority.aggregate_gate_ppm != V38_AGGREGATE_GATE_PPM
        || authority.minimum_gate_ppm != V38_MINIMUM_GATE_PPM
        || authority.query_count != V38_QUERY_COUNT
        || authority.gt_neighbors != V38_GT_NEIGHBORS
        || authority.selected_postings != V38_SELECTED_POSTINGS
        || authority.maximum_query_solver_nodes != V38_MAXIMUM_QUERY_SOLVER_NODES
        || authority.maximum_solver_nodes != V38_MAXIMUM_SOLVER_NODES
        || authority.maximum_certificate_bytes != V38_MAXIMUM_CERTIFICATE_BYTES
    {
        return Err(invalid("V38 ceiling authority differs"));
    }
    for (identity, role) in [
        (&authority.construction_result, "v38-construction-result"),
        (&authority.development_ground_truth, "gt100"),
        (&authority.posting_summary, "spill-postings"),
        (&authority.relation, "spill-relation"),
    ] {
        validate_artifact(identity, role)?;
    }
    let uris = [
        authority.construction_result.uri.as_str(),
        authority.development_ground_truth.uri.as_str(),
        authority.posting_summary.uri.as_str(),
        authority.relation.uri.as_str(),
    ];
    if uris.into_iter().collect::<BTreeSet<_>>().len() != uris.len() {
        return Err(invalid("V38 ceiling artifact roles overlap"));
    }
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

fn canonical_bytes<T: Serialize>(value: &T, context: &str) -> Result<Vec<u8>> {
    let value = serde_json::to_value(value)
        .map_err(|error| invalid(&format!("V38 {context} serialization failed: {error}")))?;
    let mut bytes = serde_json::to_vec(&canonical_json_value(value))
        .map_err(|error| invalid(&format!("V38 {context} serialization failed: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

pub(crate) fn canonical_v38_construction_authority_bytes(
    authority: &V38ConstructionAuthority,
) -> Result<Vec<u8>> {
    validate_construction_authority(authority)?;
    canonical_bytes(authority, "construction authority")
}

pub(crate) fn canonical_v38_ceiling_authority_bytes(
    authority: &V38CeilingAuthority,
) -> Result<Vec<u8>> {
    validate_ceiling_authority(authority)?;
    canonical_bytes(authority, "ceiling authority")
}

/// Validate and return the canonical bytes of a V38 construction authority.
#[doc(hidden)]
pub fn validate_v38_construction_authority_bytes(bytes: &[u8]) -> Result<Vec<u8>> {
    let authority: V38ConstructionAuthority = serde_json::from_slice(bytes).map_err(|error| {
        invalid(&format!(
            "V38 construction authority parsing failed: {error}"
        ))
    })?;
    let canonical = canonical_v38_construction_authority_bytes(&authority)?;
    if canonical != bytes {
        return Err(invalid(
            "V38 construction authority bytes are not canonical",
        ));
    }
    Ok(canonical)
}

/// Validate and return the canonical bytes of a V38 ceiling authority.
#[doc(hidden)]
pub fn validate_v38_ceiling_authority_bytes(bytes: &[u8]) -> Result<Vec<u8>> {
    let authority: V38CeilingAuthority = serde_json::from_slice(bytes)
        .map_err(|error| invalid(&format!("V38 ceiling authority parsing failed: {error}")))?;
    let canonical = canonical_v38_ceiling_authority_bytes(&authority)?;
    if canonical != bytes {
        return Err(invalid("V38 ceiling authority bytes are not canonical"));
    }
    Ok(canonical)
}

fn encoded_v38_artifact(role: &str, uri: &str, bytes: &[u8]) -> V38ArtifactIdentity {
    V38ArtifactIdentity {
        blake3: blake3::hash(bytes).to_hex().to_string(),
        encoded_bytes: bytes.len() as u64,
        role: role.to_owned(),
        sha256: format!("{:x}", Sha256::digest(bytes)),
        uri: uri.to_owned(),
    }
}

fn canonical_v38_construction_result_bytes(result: &V38ConstructionResult) -> Result<Vec<u8>> {
    if result.schema != "borsuk-v38-local-result-v1"
        || result.claim_eligible
        || result.mode != "build-spill"
        || result.inputs.len() != V38LocalRunMode::BuildSpill.input_roles().len()
        || result.artifacts.len() != 2
        || result.artifacts[0].role != "spill-relation"
        || result.artifacts[1].role != "spill-postings"
        || result.construction_evidence.proposed_alternates != V38_CORPUS_ROWS
        || result.construction_evidence.accepted_alternates > V38_MAXIMUM_ALTERNATE_ASSIGNMENTS
        || result.construction_evidence.capacity_rejected
            > result.construction_evidence.proposed_alternates
        || result.construction_evidence.accepted_alternates
            + result.construction_evidence.capacity_rejected
            > result.construction_evidence.proposed_alternates
        || result.construction_evidence.owners_one + result.construction_evidence.owners_two
            != V38_CORPUS_ROWS
        || result.construction_evidence.owners_two
            != result.construction_evidence.accepted_alternates
        || result.construction_evidence.total_assignments
            != V38_CORPUS_ROWS + result.construction_evidence.accepted_alternates
        || result.construction_evidence.maximum_posting_population == 0
        || result.construction_evidence.maximum_posting_population
            > u64::from(V38_MAXIMUM_ROWS_PER_POSTING)
        || result.construction_evidence.exhausted
            == (result.construction_evidence.accepted_alternates
                == V38_MAXIMUM_ALTERNATE_ASSIGNMENTS)
    {
        return Err(invalid("V38 construction result authority differs"));
    }
    for (identity, role) in result
        .inputs
        .iter()
        .zip(V38LocalRunMode::BuildSpill.input_roles())
    {
        validate_artifact(identity, role)?;
    }
    validate_artifact(&result.artifacts[0], "spill-relation")?;
    validate_artifact(&result.artifacts[1], "spill-postings")?;
    let uris = result
        .inputs
        .iter()
        .chain(&result.artifacts)
        .map(|identity| identity.uri.as_str())
        .collect::<BTreeSet<_>>();
    if uris.len() != result.inputs.len() + result.artifacts.len() {
        return Err(invalid("V38 construction result artifact roles overlap"));
    }
    canonical_bytes(result, "construction result")
}

fn parse_v38_construction_result_bytes(bytes: &[u8]) -> Result<V38ConstructionResult> {
    let result: V38ConstructionResult = serde_json::from_slice(bytes)
        .map_err(|error| invalid(&format!("V38 construction result parsing failed: {error}")))?;
    if canonical_v38_construction_result_bytes(&result)? != bytes {
        return Err(invalid("V38 construction result bytes are not canonical"));
    }
    Ok(result)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V38SpillProposal {
    alternate_posting: u32,
    alternate_violation_bits: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V38SpillRecord {
    source_ordinal: u64,
    feature_row_id: u64,
    posting_ordinal: u32,
    owner_role: u8,
    posting_local_ordinal: u32,
    alternate_violation_bits: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V38PostingSummary {
    posting_ordinal: u32,
    primary_population: u64,
    alternate_population: u64,
    total_population: u64,
    projected_payload_bytes: u64,
    projected_framing_allowance_bytes: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V38SpillRelation {
    records: Vec<V38SpillRecord>,
    postings: Vec<V38PostingSummary>,
}

fn v38_spill_relation_schema() -> Schema {
    Schema::new(vec![
        Field::new("source_ordinal", DataType::UInt64, false),
        Field::new("feature_row_id", DataType::UInt64, false),
        Field::new("posting_ordinal", DataType::UInt32, false),
        Field::new("owner_role", DataType::UInt8, false),
        Field::new("posting_local_ordinal", DataType::UInt32, false),
        Field::new("alternate_violation", DataType::Float32, true),
    ])
}

fn v38_posting_summary_schema() -> Schema {
    Schema::new(vec![
        Field::new("posting_ordinal", DataType::UInt32, false),
        Field::new("primary_population", DataType::UInt64, false),
        Field::new("alternate_population", DataType::UInt64, false),
        Field::new("total_population", DataType::UInt64, false),
        Field::new("projected_payload_bytes", DataType::UInt64, false),
        Field::new("projected_framing_allowance_bytes", DataType::UInt32, false),
    ])
}

fn parquet_properties(maximum_row_group_rows: usize) -> Result<WriterProperties> {
    if maximum_row_group_rows == 0 {
        return Err(invalid("V38 Parquet row group size differs"));
    }
    Ok(WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_max_row_group_row_count(Some(maximum_row_group_rows))
        .build())
}

fn column<'a, T: 'static>(batch: &'a RecordBatch, index: usize, message: &str) -> Result<&'a T> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| invalid(message))
}

fn validate_v38_spill_relation_shape(records: &[V38SpillRecord]) -> Result<u32> {
    if records.is_empty() {
        return Err(invalid("V38 spill relation is empty"));
    }
    let mut previous = None;
    let mut current_source = None;
    let mut primary_seen = false;
    let mut primary_feature_id = None;
    let mut primary_posting = None;
    let mut unique_feature_ids = BTreeSet::new();
    let mut maximum_posting = None;
    for record in records {
        let key = (record.source_ordinal, record.owner_role);
        if previous.is_some_and(|value| key <= value)
            || record.owner_role > 1
            || record.owner_role == 0 && record.alternate_violation_bits.is_some()
            || record.owner_role == 1 && record.alternate_violation_bits.is_none()
        {
            return Err(invalid("V38 spill relation ordering or role differs"));
        }
        if current_source != Some(record.source_ordinal) {
            if current_source.is_some() && !primary_seen {
                return Err(invalid("V38 spill primary owner is absent"));
            }
            current_source = Some(record.source_ordinal);
            primary_seen = false;
            primary_feature_id = None;
            primary_posting = None;
        }
        if record.owner_role == 0 {
            if primary_seen || !unique_feature_ids.insert(record.feature_row_id) {
                return Err(invalid("V38 spill primary owner is duplicated"));
            }
            primary_seen = true;
            primary_feature_id = Some(record.feature_row_id);
            primary_posting = Some(record.posting_ordinal);
        } else if !primary_seen {
            return Err(invalid("V38 spill alternate precedes primary"));
        } else if primary_feature_id != Some(record.feature_row_id)
            || primary_posting == Some(record.posting_ordinal)
        {
            return Err(invalid("V38 spill alternate binding differs"));
        }
        if let Some(bits) = record.alternate_violation_bits {
            let value = f32::from_bits(bits);
            if !value.is_finite() || value.total_cmp(&0.0).is_lt() {
                return Err(invalid("V38 spill alternate violation differs"));
            }
        }
        maximum_posting = Some(
            maximum_posting.map_or(record.posting_ordinal, |maximum: u32| {
                maximum.max(record.posting_ordinal)
            }),
        );
        previous = Some(key);
    }
    if !primary_seen {
        return Err(invalid("V38 spill primary owner is absent"));
    }
    let posting_count = maximum_posting
        .and_then(|maximum| maximum.checked_add(1))
        .ok_or_else(|| invalid("V38 spill posting count overflows"))?;
    if usize::try_from(posting_count)
        .ok()
        .is_none_or(|count| count > records.len())
    {
        return Err(invalid("V38 spill posting count exceeds relation rows"));
    }
    let mut next_primary = vec![0_u32; posting_count as usize];
    for record in records.iter().filter(|record| record.owner_role == 0) {
        let posting = record.posting_ordinal as usize;
        if record.posting_local_ordinal != next_primary[posting] {
            return Err(invalid("V38 spill primary local order differs"));
        }
        next_primary[posting] = next_primary[posting]
            .checked_add(1)
            .ok_or_else(|| invalid("V38 spill primary population overflows"))?;
    }
    let mut next_alternate = next_primary;
    for record in records.iter().filter(|record| record.owner_role == 1) {
        let posting = record.posting_ordinal as usize;
        if record.posting_local_ordinal != next_alternate[posting] {
            return Err(invalid("V38 spill alternate local order differs"));
        }
        next_alternate[posting] = next_alternate[posting]
            .checked_add(1)
            .ok_or_else(|| invalid("V38 spill total population overflows"))?;
    }
    Ok(posting_count)
}

fn validate_v38_spill_relation_against_primary(
    records: &[V38SpillRecord],
    primary: &[crate::v37_relation_router::V37OwnershipRecord],
    posting_count: u32,
    maximum_rows_per_posting: u32,
) -> Result<()> {
    if validate_v38_spill_relation_shape(records)? > posting_count
        || primary.is_empty()
        || maximum_rows_per_posting == 0
    {
        return Err(invalid("V38 spill relation authority differs"));
    }
    let observed_primary: Vec<_> = records
        .iter()
        .filter(|record| record.owner_role == 0)
        .map(|record| {
            (
                record.source_ordinal,
                record.feature_row_id,
                record.posting_ordinal,
                record.posting_local_ordinal,
            )
        })
        .collect();
    let expected_primary: Vec<_> = primary
        .iter()
        .map(|record| {
            (
                record.source_ordinal,
                record.feature_row_id,
                record.posting_ordinal,
                record.posting_local_ordinal,
            )
        })
        .collect();
    if observed_primary != expected_primary {
        return Err(invalid("V38 spill primary ownership differs"));
    }
    let mut next_alternate = vec![0_u32; posting_count as usize];
    for record in primary {
        let posting = record.posting_ordinal as usize;
        if posting >= next_alternate.len() {
            return Err(invalid("V38 spill primary posting differs"));
        }
        next_alternate[posting] = next_alternate[posting]
            .checked_add(1)
            .ok_or_else(|| invalid("V38 spill primary population overflows"))?;
    }
    for record in records {
        let posting = record.posting_ordinal as usize;
        if posting >= next_alternate.len() {
            return Err(invalid("V38 spill posting local ordinal differs"));
        }
        if record.owner_role == 1 {
            if record.posting_local_ordinal != next_alternate[posting] {
                return Err(invalid("V38 spill alternate local order differs"));
            }
            next_alternate[posting] = next_alternate[posting]
                .checked_add(1)
                .ok_or_else(|| invalid("V38 spill total population overflows"))?;
        }
    }
    if next_alternate
        .iter()
        .any(|population| *population == 0 || *population > maximum_rows_per_posting)
    {
        return Err(invalid("V38 spill posting local range differs"));
    }
    Ok(())
}

pub(crate) fn encode_v38_spill_relation_parquet(records: &[V38SpillRecord]) -> Result<Vec<u8>> {
    encode_v38_spill_relation_parquet_with_row_group_size(records, 1_048_576)
}

pub(crate) fn encode_v38_spill_relation_parquet_with_row_group_size(
    records: &[V38SpillRecord],
    maximum_row_group_rows: usize,
) -> Result<Vec<u8>> {
    validate_v38_spill_relation_shape(records)?;
    let schema = Arc::new(v38_spill_relation_schema());
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(UInt64Array::from_iter_values(
                records.iter().map(|record| record.source_ordinal),
            )),
            Arc::new(UInt64Array::from_iter_values(
                records.iter().map(|record| record.feature_row_id),
            )),
            Arc::new(UInt32Array::from_iter_values(
                records.iter().map(|record| record.posting_ordinal),
            )),
            Arc::new(UInt8Array::from_iter_values(
                records.iter().map(|record| record.owner_role),
            )),
            Arc::new(UInt32Array::from_iter_values(
                records.iter().map(|record| record.posting_local_ordinal),
            )),
            Arc::new(Float32Array::from(
                records
                    .iter()
                    .map(|record| record.alternate_violation_bits.map(f32::from_bits))
                    .collect::<Vec<_>>(),
            )),
        ],
    )?;
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(
        &mut bytes,
        schema,
        Some(parquet_properties(maximum_row_group_rows)?),
    )?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(bytes)
}

pub(crate) fn decode_v38_spill_relation_parquet(
    bytes: &[u8],
    primary: &[crate::v37_relation_router::V37OwnershipRecord],
    posting_count: u32,
    maximum_rows_per_posting: u32,
) -> Result<Vec<V38SpillRecord>> {
    decode_v38_spill_relation_parquet_inner(
        bytes,
        Some(primary),
        primary.len(),
        posting_count,
        maximum_rows_per_posting,
    )
}

fn decode_v38_spill_relation_parquet_unbound(
    bytes: &[u8],
    source_rows: usize,
    posting_count: u32,
    maximum_rows_per_posting: u32,
) -> Result<Vec<V38SpillRecord>> {
    let records = decode_v38_spill_relation_parquet_inner(
        bytes,
        None,
        source_rows,
        posting_count,
        maximum_rows_per_posting,
    )?;
    if records
        .iter()
        .filter(|record| record.owner_role == 0)
        .count()
        != source_rows
    {
        return Err(invalid("V38 spill relation source population differs"));
    }
    Ok(records)
}

fn decode_v38_spill_relation_parquet_inner(
    bytes: &[u8],
    primary: Option<&[crate::v37_relation_router::V37OwnershipRecord]>,
    source_rows: usize,
    posting_count: u32,
    maximum_rows_per_posting: u32,
) -> Result<Vec<V38SpillRecord>> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))?;
    let maximum_rows = source_rows
        .checked_mul(2)
        .ok_or_else(|| invalid("V38 spill relation row bound overflows"))?;
    let observed_rows = usize::try_from(builder.metadata().file_metadata().num_rows())
        .map_err(|_| invalid("V38 spill relation row count differs"))?;
    if builder.schema().as_ref() != &v38_spill_relation_schema()
        || builder.metadata().num_row_groups() == 0
        || observed_rows < source_rows
        || observed_rows > maximum_rows
    {
        return Err(invalid("V38 spill relation Parquet schema differs"));
    }
    let mut records = Vec::new();
    for batch in builder.build()? {
        let batch = batch?;
        if batch.num_columns() != 6
            || batch.columns()[..5]
                .iter()
                .any(|column| column.null_count() != 0)
        {
            return Err(invalid("V38 spill relation Parquet batch differs"));
        }
        let source = column::<UInt64Array>(&batch, 0, "V38 source ordinal differs")?;
        let feature = column::<UInt64Array>(&batch, 1, "V38 feature row differs")?;
        let posting = column::<UInt32Array>(&batch, 2, "V38 posting ordinal differs")?;
        let role = column::<UInt8Array>(&batch, 3, "V38 owner role differs")?;
        let local = column::<UInt32Array>(&batch, 4, "V38 posting local ordinal differs")?;
        let violation = column::<Float32Array>(&batch, 5, "V38 alternate violation differs")?;
        for row in 0..batch.num_rows() {
            records.push(V38SpillRecord {
                source_ordinal: source.value(row),
                feature_row_id: feature.value(row),
                posting_ordinal: posting.value(row),
                owner_role: role.value(row),
                posting_local_ordinal: local.value(row),
                alternate_violation_bits: (!violation.is_null(row))
                    .then(|| violation.value(row).to_bits()),
            });
        }
    }
    if let Some(primary) = primary {
        validate_v38_spill_relation_against_primary(
            &records,
            primary,
            posting_count,
            maximum_rows_per_posting,
        )?;
    } else {
        if validate_v38_spill_relation_shape(&records)? > posting_count {
            return Err(invalid("V38 spill relation authority differs"));
        }
        let summaries =
            summarize_v38_spill_relation(&records, posting_count, maximum_rows_per_posting)?;
        if summaries.len() != posting_count as usize {
            return Err(invalid("V38 spill relation posting count differs"));
        }
    }
    Ok(records)
}

pub(crate) fn summarize_v38_spill_relation(
    records: &[V38SpillRecord],
    posting_count: u32,
    maximum_rows_per_posting: u32,
) -> Result<Vec<V38PostingSummary>> {
    if validate_v38_spill_relation_shape(records)? > posting_count || posting_count == 0 {
        return Err(invalid("V38 spill posting summary authority differs"));
    }
    let mut populations = vec![(0_u64, 0_u64); posting_count as usize];
    let mut next_primary = vec![0_u32; posting_count as usize];
    for record in records {
        let posting = record.posting_ordinal as usize;
        if posting >= populations.len() {
            return Err(invalid("V38 spill posting summary ordinal differs"));
        }
        if record.owner_role == 0 {
            if record.posting_local_ordinal != next_primary[posting] {
                return Err(invalid("V38 spill primary local order differs"));
            }
            next_primary[posting] = next_primary[posting]
                .checked_add(1)
                .ok_or_else(|| invalid("V38 spill primary population overflows"))?;
            populations[posting].0 += 1;
        } else {
            populations[posting].1 += 1;
        }
    }
    let mut next_alternate = next_primary.clone();
    for record in records.iter().filter(|record| record.owner_role == 1) {
        let posting = record.posting_ordinal as usize;
        if record.posting_local_ordinal != next_alternate[posting] {
            return Err(invalid("V38 spill alternate local order differs"));
        }
        next_alternate[posting] = next_alternate[posting]
            .checked_add(1)
            .ok_or_else(|| invalid("V38 spill total population overflows"))?;
    }
    let mut summaries = Vec::with_capacity(posting_count as usize);
    for posting in 0..posting_count as usize {
        let (primary_population, alternate_population) = populations[posting];
        let total_population = primary_population
            .checked_add(alternate_population)
            .ok_or_else(|| invalid("V38 spill posting population overflows"))?;
        if primary_population == 0
            || total_population > u64::from(maximum_rows_per_posting)
            || u64::from(next_alternate[posting]) != total_population
        {
            return Err(invalid("V38 spill posting population differs"));
        }
        summaries.push(V38PostingSummary {
            posting_ordinal: posting as u32,
            primary_population,
            alternate_population,
            total_population,
            projected_payload_bytes: total_population
                .checked_mul(u64::from(V38_PROJECTED_RECORD_BYTES))
                .ok_or_else(|| invalid("V38 spill payload projection overflows"))?,
            projected_framing_allowance_bytes: V38_PROJECTED_FRAMING_ALLOWANCE_BYTES,
        });
    }
    Ok(summaries)
}

fn validate_v38_posting_summaries(summaries: &[V38PostingSummary]) -> Result<()> {
    if summaries.is_empty() {
        return Err(invalid("V38 spill posting summary is empty"));
    }
    for (index, summary) in summaries.iter().enumerate() {
        if summary.posting_ordinal != index as u32
            || summary.primary_population == 0
            || summary.total_population
                != summary
                    .primary_population
                    .checked_add(summary.alternate_population)
                    .ok_or_else(|| invalid("V38 spill posting population overflows"))?
            || summary.projected_payload_bytes
                != summary
                    .total_population
                    .checked_mul(u64::from(V38_PROJECTED_RECORD_BYTES))
                    .ok_or_else(|| invalid("V38 spill payload projection overflows"))?
            || summary.projected_framing_allowance_bytes != V38_PROJECTED_FRAMING_ALLOWANCE_BYTES
            || summary.projected_payload_bytes > 491_520
        {
            return Err(invalid("V38 spill posting summary differs"));
        }
    }
    Ok(())
}

pub(crate) fn encode_v38_posting_summary_parquet(
    summaries: &[V38PostingSummary],
) -> Result<Vec<u8>> {
    validate_v38_posting_summaries(summaries)?;
    let schema = Arc::new(v38_posting_summary_schema());
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(UInt32Array::from_iter_values(
                summaries.iter().map(|value| value.posting_ordinal),
            )),
            Arc::new(UInt64Array::from_iter_values(
                summaries.iter().map(|value| value.primary_population),
            )),
            Arc::new(UInt64Array::from_iter_values(
                summaries.iter().map(|value| value.alternate_population),
            )),
            Arc::new(UInt64Array::from_iter_values(
                summaries.iter().map(|value| value.total_population),
            )),
            Arc::new(UInt64Array::from_iter_values(
                summaries.iter().map(|value| value.projected_payload_bytes),
            )),
            Arc::new(UInt32Array::from_iter_values(
                summaries
                    .iter()
                    .map(|value| value.projected_framing_allowance_bytes),
            )),
        ],
    )?;
    let mut bytes = Vec::new();
    let mut writer =
        ArrowWriter::try_new(&mut bytes, schema, Some(parquet_properties(1_048_576)?))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(bytes)
}

pub(crate) fn decode_v38_posting_summary_parquet(
    bytes: &[u8],
    relation: &[V38SpillRecord],
    posting_count: u32,
    maximum_rows_per_posting: u32,
) -> Result<Vec<V38PostingSummary>> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))?;
    let observed_rows = usize::try_from(builder.metadata().file_metadata().num_rows())
        .map_err(|_| invalid("V38 spill posting row count differs"))?;
    if builder.schema().as_ref() != &v38_posting_summary_schema()
        || builder.metadata().num_row_groups() == 0
        || observed_rows != posting_count as usize
    {
        return Err(invalid("V38 spill posting Parquet schema differs"));
    }
    let mut summaries = Vec::new();
    for batch in builder.build()? {
        let batch = batch?;
        if batch.num_columns() != 6
            || batch
                .columns()
                .iter()
                .any(|column| column.null_count() != 0)
        {
            return Err(invalid("V38 spill posting Parquet batch differs"));
        }
        let posting = column::<UInt32Array>(&batch, 0, "V38 posting ordinal differs")?;
        let primary = column::<UInt64Array>(&batch, 1, "V38 primary population differs")?;
        let alternate = column::<UInt64Array>(&batch, 2, "V38 alternate population differs")?;
        let total = column::<UInt64Array>(&batch, 3, "V38 total population differs")?;
        let payload = column::<UInt64Array>(&batch, 4, "V38 projected payload differs")?;
        let framing = column::<UInt32Array>(&batch, 5, "V38 framing allowance differs")?;
        for row in 0..batch.num_rows() {
            summaries.push(V38PostingSummary {
                posting_ordinal: posting.value(row),
                primary_population: primary.value(row),
                alternate_population: alternate.value(row),
                total_population: total.value(row),
                projected_payload_bytes: payload.value(row),
                projected_framing_allowance_bytes: framing.value(row),
            });
        }
    }
    validate_v38_posting_summaries(&summaries)?;
    if summaries != summarize_v38_spill_relation(relation, posting_count, maximum_rows_per_posting)?
    {
        return Err(invalid(
            "V38 spill posting summary does not reproduce relation",
        ));
    }
    Ok(summaries)
}

pub(crate) fn build_v38_spill_relation(
    tree: &crate::v37_relation_router::V37BalancedTree,
    projected_rows: &[Vec<f32>],
    primary: &[crate::v37_relation_router::V37OwnershipRecord],
    maximum_alternates: u64,
    maximum_rows_per_posting: u32,
    workers: u32,
) -> Result<V38SpillRelation> {
    let rows = projected_rows.iter().map(Vec::as_slice).collect::<Vec<_>>();
    Ok(build_v38_spill_relation_slices_with_evidence(
        tree,
        &rows,
        primary,
        maximum_alternates,
        maximum_rows_per_posting,
        workers,
    )?
    .0)
}

fn build_v38_spill_relation_slices_with_evidence(
    tree: &crate::v37_relation_router::V37BalancedTree,
    projected_rows: &[&[f32]],
    primary: &[crate::v37_relation_router::V37OwnershipRecord],
    maximum_alternates: u64,
    maximum_rows_per_posting: u32,
    workers: u32,
) -> Result<(V38SpillRelation, V38AdmissionEvidence)> {
    build_v38_spill_relation_slices_with_evidence_and_progress(
        tree,
        projected_rows,
        primary,
        V38SpillBuildLimits {
            maximum_alternates,
            maximum_rows_per_posting,
            workers,
        },
        |_| Ok(()),
    )
}

fn build_v38_spill_relation_slices_with_evidence_and_progress<F>(
    tree: &crate::v37_relation_router::V37BalancedTree,
    projected_rows: &[&[f32]],
    primary: &[crate::v37_relation_router::V37OwnershipRecord],
    limits: V38SpillBuildLimits,
    report_completed_rows: F,
) -> Result<(V38SpillRelation, V38AdmissionEvidence)>
where
    F: FnMut(u64) -> Result<()>,
{
    build_v38_spill_relation_rows_by_index_with_evidence_and_progress(
        tree,
        projected_rows.len(),
        |index| projected_rows[index],
        primary,
        limits,
        report_completed_rows,
    )
}

fn build_v38_spill_relation_flat_with_evidence_and_progress<F>(
    tree: &crate::v37_relation_router::V37BalancedTree,
    projected_coordinates: &[f32],
    dimensions: usize,
    primary: &[crate::v37_relation_router::V37OwnershipRecord],
    limits: V38SpillBuildLimits,
    report_completed_rows: F,
) -> Result<(V38SpillRelation, V38AdmissionEvidence)>
where
    F: FnMut(u64) -> Result<()>,
{
    if dimensions == 0
        || dimensions != tree.dimensions
        || !projected_coordinates.len().is_multiple_of(dimensions)
    {
        return Err(invalid("V38 flat projected coordinates differ"));
    }
    let row_count = projected_coordinates.len() / dimensions;
    build_v38_spill_relation_rows_by_index_with_evidence_and_progress(
        tree,
        row_count,
        |index| {
            let start = index * dimensions;
            &projected_coordinates[start..start + dimensions]
        },
        primary,
        limits,
        report_completed_rows,
    )
}

fn build_v38_spill_relation_rows_by_index_with_evidence_and_progress<'a, R, F>(
    tree: &crate::v37_relation_router::V37BalancedTree,
    row_count: usize,
    row_at: R,
    primary: &[crate::v37_relation_router::V37OwnershipRecord],
    limits: V38SpillBuildLimits,
    mut report_completed_rows: F,
) -> Result<(V38SpillRelation, V38AdmissionEvidence)>
where
    R: Fn(usize) -> &'a [f32] + Sync,
    F: FnMut(u64) -> Result<()>,
{
    const RESIDENT_PROPOSAL_LIMIT_BYTES: u64 = 64 * 1_048_576;

    crate::v37_relation_router::validate_v37_tree_geometry(tree)?;
    let posting_count = u32::try_from(tree.leaf_populations.len())
        .map_err(|_| invalid("V38 spill posting count exceeds authority width"))?;
    let projected_row_count = u64::try_from(row_count)
        .map_err(|_| invalid("V38 spill row count exceeds authority width"))?;
    let auxiliary_bytes = project_v38_admission_auxiliary_bytes(
        projected_row_count,
        limits.maximum_alternates,
        posting_count,
    )?;
    if row_count == 0
        || row_count != primary.len()
        || auxiliary_bytes > RESIDENT_PROPOSAL_LIMIT_BYTES
        || limits.workers == 0
        || limits.workers > V38_MAXIMUM_WORKERS
    {
        return Err(invalid("V38 spill construction authority differs"));
    }
    let pool = ThreadPoolBuilder::new()
        .num_threads(limits.workers as usize)
        .build()
        .map_err(|_| invalid("V38 spill worker pool differs"))?;
    let mut proposals = Vec::with_capacity(row_count);
    for block_start in (0..row_count).step_by(65_536) {
        let block_end = block_start.saturating_add(65_536).min(row_count);
        let mut block_proposals = pool.install(|| {
            (block_start..block_end)
                .into_par_iter()
                .map(|index| {
                    let row = row_at(index);
                    let record = &primary[index];
                    let source_ordinal = u64::try_from(index)
                        .map_err(|_| invalid("V38 spill source ordinal overflows"))?;
                    if record.source_ordinal != source_ordinal
                        || row.len() != tree.dimensions
                        || row.iter().any(|value| !value.is_finite())
                    {
                        return Err(invalid("V38 spill primary replay differs"));
                    }
                    let scores = score_v38_internal_nodes(tree, row)?;
                    if replay_v38_primary_from_scores(tree, &scores, source_ordinal)?
                        != record.posting_ordinal
                    {
                        return Err(invalid("V38 spill primary replay differs"));
                    }
                    propose_v38_alternate_owner_from_scores(tree, &scores, record.posting_ordinal)
                        .map(|proposal| (source_ordinal, proposal))
                })
                .collect::<Result<Vec<_>>>()
        })?;
        proposals.append(&mut block_proposals);
        report_completed_rows(
            u64::try_from(block_end)
                .map_err(|_| invalid("V38 spill completed row count overflows"))?,
        )?;
    }
    let admission = admit_v38_spill_proposals_with_evidence(
        primary,
        &proposals,
        posting_count,
        limits.maximum_alternates,
        limits.maximum_rows_per_posting,
    )?;
    let postings = summarize_v38_spill_relation(
        &admission.records,
        posting_count,
        limits.maximum_rows_per_posting,
    )?;
    let evidence = V38AdmissionEvidence {
        capacity_rejected: admission.capacity_rejected,
        exhausted: admission.exhausted,
    };
    Ok((
        V38SpillRelation {
            records: admission.records,
            postings,
        },
        evidence,
    ))
}

fn squared_positive_margin(value: f32) -> Result<f32> {
    if !value.is_finite() {
        return Err(invalid("V38 spill margin is non-finite"));
    }
    let positive = if value.total_cmp(&0.0).is_gt() {
        value
    } else {
        0.0
    };
    let squared = positive * positive;
    if !squared.is_finite() {
        return Err(invalid("V38 spill squared margin is non-finite"));
    }
    Ok(if squared == 0.0 { 0.0 } else { squared })
}

fn larger_penalty(left: f32, right: f32) -> f32 {
    if left.total_cmp(&right).is_gt() {
        left
    } else {
        right
    }
}

#[cfg(test)]
pub(crate) fn propose_v38_alternate_owner(
    tree: &crate::v37_relation_router::V37BalancedTree,
    projected_row: &[f32],
    primary_posting: u32,
) -> Result<V38SpillProposal> {
    crate::v37_relation_router::validate_v37_tree_geometry(tree)?;
    if projected_row.len() != tree.dimensions
        || projected_row.iter().any(|value| !value.is_finite())
        || primary_posting as usize >= tree.leaf_populations.len()
    {
        return Err(invalid("V38 spill row authority differs"));
    }
    let scores = score_v38_internal_nodes(tree, projected_row)?;
    propose_v38_alternate_owner_from_scores(tree, &scores, primary_posting)
}

fn score_v38_internal_nodes(
    tree: &crate::v37_relation_router::V37BalancedTree,
    projected_row: &[f32],
) -> Result<Vec<Option<f32>>> {
    let mut scores = vec![None; tree.nodes.len()];
    for (index, node) in tree.nodes.iter().enumerate() {
        if node.posting_ordinal.is_none() {
            let (score, backend) = crate::v37_relation_router::score_v37_hyperplane_fused(
                projected_row,
                &node.normal,
            )?;
            if backend != tree.fma_backend {
                return Err(invalid("V38 spill numeric backend differs"));
            }
            scores[index] = Some(score);
        }
    }
    Ok(scores)
}

fn replay_v38_primary_from_scores(
    tree: &crate::v37_relation_router::V37BalancedTree,
    scores: &[Option<f32>],
    source_ordinal: u64,
) -> Result<u32> {
    let mut node_index = 0_usize;
    loop {
        let node = tree
            .nodes
            .get(node_index)
            .ok_or_else(|| invalid("V38 spill primary replay node differs"))?;
        if let Some(posting) = node.posting_ordinal {
            return Ok(posting);
        }
        let score = scores
            .get(node_index)
            .and_then(|score| *score)
            .ok_or_else(|| invalid("V38 spill primary replay score is absent"))?;
        let is_left = crate::v37_relation_router::route_v37_corpus_member(
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
        .ok_or_else(|| invalid("V38 spill primary replay child is absent"))?
            as usize;
    }
}

fn propose_v38_alternate_owner_from_scores(
    tree: &crate::v37_relation_router::V37BalancedTree,
    scores: &[Option<f32>],
    primary_posting: u32,
) -> Result<V38SpillProposal> {
    fn visit(
        tree: &crate::v37_relation_router::V37BalancedTree,
        scores: &[Option<f32>],
        node_index: usize,
        path_penalty: f32,
        primary_posting: u32,
        best: &mut Option<V38SpillProposal>,
    ) -> Result<()> {
        let node = tree
            .nodes
            .get(node_index)
            .ok_or_else(|| invalid("V38 spill tree node differs"))?;
        if let Some(posting) = node.posting_ordinal {
            if posting != primary_posting {
                let proposal = V38SpillProposal {
                    alternate_posting: posting,
                    alternate_violation_bits: path_penalty.to_bits(),
                };
                let replace = best.as_ref().is_none_or(|current| {
                    path_penalty
                        .total_cmp(&f32::from_bits(current.alternate_violation_bits))
                        .then_with(|| posting.cmp(&current.alternate_posting))
                        .is_lt()
                });
                if replace {
                    *best = Some(proposal);
                }
            }
            return Ok(());
        }
        let score = scores
            .get(node_index)
            .and_then(|score| *score)
            .ok_or_else(|| invalid("V38 spill node score is absent"))?;
        let boundary = f32::from_bits(node.boundary_score_bits);
        let left_penalty = larger_penalty(path_penalty, squared_positive_margin(score - boundary)?);
        let right_penalty =
            larger_penalty(path_penalty, squared_positive_margin(boundary - score)?);
        visit(
            tree,
            scores,
            node.left_node
                .ok_or_else(|| invalid("V38 spill left child is absent"))? as usize,
            left_penalty,
            primary_posting,
            best,
        )?;
        visit(
            tree,
            scores,
            node.right_node
                .ok_or_else(|| invalid("V38 spill right child is absent"))? as usize,
            right_penalty,
            primary_posting,
            best,
        )
    }

    let mut best = None;
    visit(tree, scores, 0, 0.0, primary_posting, &mut best)?;
    best.ok_or_else(|| invalid("V38 spill alternate owner is absent"))
}

#[derive(Debug, Clone, Copy)]
struct RankedProposal {
    source_ordinal: u64,
    primary_posting: u32,
    primary_population: u32,
    local_rank: u32,
    proposal: V38SpillProposal,
}

#[derive(Debug, Clone, Copy)]
struct V38SpillBuildLimits {
    maximum_alternates: u64,
    maximum_rows_per_posting: u32,
    workers: u32,
}

pub(crate) fn project_v38_admission_auxiliary_bytes(
    row_count: u64,
    maximum_alternates: u64,
    posting_count: u32,
) -> Result<u64> {
    use std::mem::size_of;

    let proposal_input = row_count
        .checked_mul(size_of::<(u64, V38SpillProposal)>() as u64)
        .ok_or_else(|| invalid("V38 proposal input projection overflows"))?;
    let feature_scratch = row_count
        .checked_mul(size_of::<u64>() as u64)
        .and_then(|bytes| bytes.checked_add(proposal_input))
        .ok_or_else(|| invalid("V38 feature scratch projection overflows"))?;
    let ranked = row_count
        .checked_mul(size_of::<RankedProposal>() as u64)
        .and_then(|bytes| bytes.checked_add(proposal_input))
        .and_then(|bytes| {
            maximum_alternates
                .checked_mul(size_of::<(u64, V38SpillProposal)>() as u64)
                .and_then(|accepted| bytes.checked_add(accepted))
        })
        .ok_or_else(|| invalid("V38 ranked proposal projection overflows"))?;
    let posting_scratch = u64::from(posting_count)
        .checked_mul((2 * size_of::<u32>() + size_of::<Vec<(u64, V38SpillProposal)>>()) as u64)
        .ok_or_else(|| invalid("V38 posting scratch projection overflows"))?;
    feature_scratch
        .max(ranked)
        .checked_add(posting_scratch)
        .ok_or_else(|| invalid("V38 admission projection overflows"))
}

#[cfg(test)]
pub(crate) fn admit_v38_spill_proposals(
    primary: &[crate::v37_relation_router::V37OwnershipRecord],
    proposals: &[(u64, V38SpillProposal)],
    posting_count: u32,
    maximum_alternates: u64,
    maximum_rows_per_posting: u32,
) -> Result<Vec<V38SpillRecord>> {
    Ok(admit_v38_spill_proposals_with_evidence(
        primary,
        proposals,
        posting_count,
        maximum_alternates,
        maximum_rows_per_posting,
    )?
    .records)
}

fn admit_v38_spill_proposals_with_evidence(
    primary: &[crate::v37_relation_router::V37OwnershipRecord],
    proposals: &[(u64, V38SpillProposal)],
    posting_count: u32,
    maximum_alternates: u64,
    maximum_rows_per_posting: u32,
) -> Result<V38AdmissionResult> {
    if primary.is_empty()
        || primary.len() != proposals.len()
        || posting_count < 2
        || maximum_alternates == 0
        || maximum_rows_per_posting == 0
    {
        return Err(invalid("V38 spill admission authority differs"));
    }
    let posting_count_usize = posting_count as usize;
    let mut populations = vec![0_u32; posting_count_usize];
    let mut feature_ids = Vec::with_capacity(primary.len());
    for (index, record) in primary.iter().enumerate() {
        let posting = record.posting_ordinal as usize;
        if record.source_ordinal != index as u64
            || posting >= posting_count_usize
            || record.posting_local_ordinal != populations[posting]
        {
            return Err(invalid("V38 primary ownership differs"));
        }
        feature_ids.push(record.feature_row_id);
        populations[posting] = populations[posting]
            .checked_add(1)
            .ok_or_else(|| invalid("V38 primary population overflows"))?;
    }
    feature_ids.sort_unstable();
    if feature_ids.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(invalid("V38 primary feature IDs differ"));
    }
    drop(feature_ids);
    if populations
        .iter()
        .any(|population| *population == 0 || *population > maximum_rows_per_posting)
    {
        return Err(invalid("V38 primary local ordinals differ"));
    }
    let primary_populations = populations.clone();

    let mut ranked = Vec::with_capacity(proposals.len());
    for &(source_ordinal, proposal) in proposals {
        let index = primary
            .binary_search_by_key(&source_ordinal, |record| record.source_ordinal)
            .map_err(|_| invalid("V38 spill proposal source differs"))?;
        let primary_posting = primary[index].posting_ordinal;
        let violation = f32::from_bits(proposal.alternate_violation_bits);
        if proposal.alternate_posting >= posting_count
            || proposal.alternate_posting == primary_posting
            || !violation.is_finite()
            || violation.total_cmp(&0.0).is_lt()
        {
            return Err(invalid("V38 spill proposal differs"));
        }
        ranked.push(RankedProposal {
            source_ordinal,
            primary_posting,
            primary_population: 0,
            local_rank: 0,
            proposal,
        });
    }
    ranked.sort_unstable_by_key(|proposal| proposal.source_ordinal);
    if ranked
        .iter()
        .zip(primary)
        .any(|(proposal, record)| proposal.source_ordinal != record.source_ordinal)
    {
        return Err(invalid("V38 spill proposal coverage differs"));
    }

    ranked.sort_unstable_by(|left, right| {
        left.primary_posting
            .cmp(&right.primary_posting)
            .then_with(|| {
                f32::from_bits(left.proposal.alternate_violation_bits)
                    .total_cmp(&f32::from_bits(right.proposal.alternate_violation_bits))
            })
            .then_with(|| left.source_ordinal.cmp(&right.source_ordinal))
    });
    let mut local_counts = vec![0_u32; posting_count_usize];
    for proposal in &mut ranked {
        let posting = proposal.primary_posting as usize;
        local_counts[posting] = local_counts[posting]
            .checked_add(1)
            .ok_or_else(|| invalid("V38 spill local rank overflows"))?;
        proposal.local_rank = local_counts[posting];
        proposal.primary_population = populations[posting];
    }
    if local_counts != populations {
        return Err(invalid("V38 spill primary proposal population differs"));
    }
    ranked.sort_unstable_by(|left, right| {
        (u128::from(left.local_rank) * u128::from(right.primary_population))
            .cmp(&(u128::from(right.local_rank) * u128::from(left.primary_population)))
            .then_with(|| {
                f32::from_bits(left.proposal.alternate_violation_bits)
                    .total_cmp(&f32::from_bits(right.proposal.alternate_violation_bits))
            })
            .then_with(|| left.source_ordinal.cmp(&right.source_ordinal))
            .then_with(|| {
                left.proposal
                    .alternate_posting
                    .cmp(&right.proposal.alternate_posting)
            })
    });

    let maximum_alternates = usize::try_from(maximum_alternates)
        .map_err(|_| invalid("V38 spill alternate limit exceeds address space"))?;
    let mut accepted = Vec::with_capacity(maximum_alternates.min(primary.len()));
    let mut capacity_rejected = 0_u64;
    let mut exhausted = true;
    for ranked in ranked {
        if accepted.len() == maximum_alternates {
            exhausted = false;
            break;
        }
        let target = ranked.proposal.alternate_posting as usize;
        if populations[target] >= maximum_rows_per_posting {
            capacity_rejected = capacity_rejected
                .checked_add(1)
                .ok_or_else(|| invalid("V38 spill capacity rejection count overflows"))?;
            continue;
        }
        populations[target] += 1;
        accepted.push((ranked.source_ordinal, ranked.proposal));
    }

    let mut accepted_by_posting = vec![Vec::<(u64, V38SpillProposal)>::new(); posting_count_usize];
    for accepted in accepted {
        accepted_by_posting[accepted.1.alternate_posting as usize].push(accepted);
    }
    let mut alternate_records = Vec::new();
    for (posting, accepted) in accepted_by_posting.iter_mut().enumerate() {
        accepted.sort_unstable_by_key(|accepted| accepted.0);
        let primary_population = primary_populations[posting] as usize;
        for (index, &(source_ordinal, proposal)) in accepted.iter().enumerate() {
            let primary_record = &primary[primary
                .binary_search_by_key(&source_ordinal, |record| record.source_ordinal)
                .map_err(|_| invalid("V38 accepted source differs"))?];
            alternate_records.push(V38SpillRecord {
                source_ordinal,
                feature_row_id: primary_record.feature_row_id,
                posting_ordinal: posting as u32,
                owner_role: 1,
                posting_local_ordinal: u32::try_from(primary_population + index)
                    .map_err(|_| invalid("V38 alternate local ordinal overflows"))?,
                alternate_violation_bits: Some(proposal.alternate_violation_bits),
            });
        }
    }
    let mut relation = Vec::with_capacity(primary.len() + alternate_records.len());
    relation.extend(primary.iter().map(|record| V38SpillRecord {
        source_ordinal: record.source_ordinal,
        feature_row_id: record.feature_row_id,
        posting_ordinal: record.posting_ordinal,
        owner_role: 0,
        posting_local_ordinal: record.posting_local_ordinal,
        alternate_violation_bits: None,
    }));
    relation.extend(alternate_records);
    relation.sort_unstable_by(|left, right| {
        left.source_ordinal
            .cmp(&right.source_ordinal)
            .then_with(|| left.owner_role.cmp(&right.owner_role))
    });
    Ok(V38AdmissionResult {
        capacity_rejected,
        exhausted,
        records: relation,
    })
}

type V38CoverageMask = [u64; 2];

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V38CoverageCertificate {
    query_ordinal: u32,
    selected_postings: Vec<u32>,
    feasible_hits: u32,
    certified_upper_hits: u32,
    exact: bool,
    solver_visits: u64,
}

#[derive(Debug, Clone, Copy)]
struct V38CoverageFrame {
    candidates: V38CoverageMask,
    selected: V38CoverageMask,
    covered: V38CoverageMask,
    selected_count: u32,
}

fn v38_mask_contains(mask: V38CoverageMask, ordinal: u32) -> bool {
    mask[(ordinal / 64) as usize] & (1_u64 << (ordinal % 64)) != 0
}

fn v38_mask_insert(mask: &mut V38CoverageMask, ordinal: u32) {
    mask[(ordinal / 64) as usize] |= 1_u64 << (ordinal % 64);
}

fn v38_mask_remove(mask: &mut V38CoverageMask, ordinal: u32) {
    mask[(ordinal / 64) as usize] &= !(1_u64 << (ordinal % 64));
}

fn v38_coverage_hits(mask: V38CoverageMask) -> u32 {
    mask[0].count_ones() + mask[1].count_ones()
}

fn v38_uncovered_gain(posting: V38CoverageMask, covered: V38CoverageMask) -> u32 {
    (posting[0] & !covered[0]).count_ones() + (posting[1] & !covered[1]).count_ones()
}

fn v38_padded_selection(
    selected: V38CoverageMask,
    posting_count: u32,
    selected_postings: u32,
) -> Vec<u32> {
    let mut postings = (0..posting_count)
        .filter(|posting| v38_mask_contains(selected, *posting))
        .collect::<Vec<_>>();
    for posting in 0..posting_count {
        if postings.len() == selected_postings as usize {
            break;
        }
        if !v38_mask_contains(selected, posting) {
            postings.push(posting);
        }
    }
    postings.sort_unstable();
    postings
}

fn v38_selection_hits(selection: &[u32], posting_masks: &[V38CoverageMask]) -> u32 {
    let covered = selection.iter().fold([0_u64; 2], |mut covered, posting| {
        let posting = posting_masks[*posting as usize];
        covered[0] |= posting[0];
        covered[1] |= posting[1];
        covered
    });
    v38_coverage_hits(covered)
}

fn v38_frame_upper(
    frame: V38CoverageFrame,
    posting_masks: &[V38CoverageMask],
    posting_count: u32,
    selected_postings: u32,
    truth_rows: u32,
) -> u32 {
    let covered = v38_coverage_hits(frame.covered);
    let slots = (selected_postings - frame.selected_count) as usize;
    let mut largest = [0_u32; V38_SELECTED_POSTINGS as usize];
    for posting in 0..posting_count {
        if !v38_mask_contains(frame.candidates, posting) {
            continue;
        }
        let gain = v38_uncovered_gain(posting_masks[posting as usize], frame.covered);
        for index in 0..slots {
            if gain > largest[index] {
                largest[index..slots].rotate_right(1);
                largest[index] = gain;
                break;
            }
        }
    }
    covered
        .saturating_add(largest[..slots].iter().sum())
        .min(truth_rows)
}

fn v38_update_coverage_incumbent(
    frame: V38CoverageFrame,
    posting_masks: &[V38CoverageMask],
    posting_count: u32,
    selected_postings: u32,
    incumbent_hits: &mut u32,
    incumbent_selection: &mut Vec<u32>,
) {
    let selection = v38_padded_selection(frame.selected, posting_count, selected_postings);
    let hits = v38_selection_hits(&selection, posting_masks);
    if hits > *incumbent_hits || hits == *incumbent_hits && selection < *incumbent_selection {
        *incumbent_hits = hits;
        *incumbent_selection = selection;
    }
}

pub(crate) fn solve_v38_query_coverage(
    owners: &[(u32, Option<u32>)],
    posting_count: u32,
    selected_postings: u32,
    maximum_solver_nodes: u64,
) -> Result<V38CoverageCertificate> {
    if owners.is_empty()
        || owners.len() > V38_GT_NEIGHBORS as usize
        || posting_count == 0
        || posting_count > V38_POSTING_COUNT
        || selected_postings == 0
        || selected_postings > posting_count
        || selected_postings > V38_SELECTED_POSTINGS
        || maximum_solver_nodes == 0
    {
        return Err(invalid("V38 query coverage authority differs"));
    }

    let mut posting_masks = vec![[0_u64; 2]; posting_count as usize];
    for (row, (primary, alternate)) in owners.iter().copied().enumerate() {
        if primary >= posting_count
            || alternate.is_some_and(|posting| posting >= posting_count || posting == primary)
        {
            return Err(invalid("V38 query coverage owner differs"));
        }
        let word = row / 64;
        let bit = 1_u64 << (row % 64);
        posting_masks[primary as usize][word] |= bit;
        if let Some(posting) = alternate {
            posting_masks[posting as usize][word] |= bit;
        }
    }

    let mut greedy_selected = [0_u64; 2];
    let mut greedy_covered = [0_u64; 2];
    for _ in 0..selected_postings {
        let mut best = None;
        for posting in 0..posting_count {
            if v38_mask_contains(greedy_selected, posting) {
                continue;
            }
            let gain = v38_uncovered_gain(posting_masks[posting as usize], greedy_covered);
            if best.is_none_or(|(_, best_gain)| gain > best_gain) {
                best = Some((posting, gain));
            }
        }
        let Some((posting, gain)) = best else {
            break;
        };
        if gain == 0 {
            break;
        }
        v38_mask_insert(&mut greedy_selected, posting);
        greedy_covered[0] |= posting_masks[posting as usize][0];
        greedy_covered[1] |= posting_masks[posting as usize][1];
    }
    let mut incumbent_selection =
        v38_padded_selection(greedy_selected, posting_count, selected_postings);
    let mut incumbent_hits = v38_selection_hits(&incumbent_selection, &posting_masks);

    let mut candidates = [0_u64; 2];
    for posting in 0..posting_count {
        v38_mask_insert(&mut candidates, posting);
    }
    let root = V38CoverageFrame {
        candidates,
        selected: [0_u64; 2],
        covered: [0_u64; 2],
        selected_count: 0,
    };
    let truth_rows = owners.len() as u32;
    let mut stack = vec![root];
    let mut solver_visits = 0_u64;
    let mut interrupted_upper = None;

    while let Some(frame) = stack.pop() {
        solver_visits += 1;
        let upper = v38_frame_upper(
            frame,
            &posting_masks,
            posting_count,
            selected_postings,
            truth_rows,
        );
        v38_update_coverage_incumbent(
            frame,
            &posting_masks,
            posting_count,
            selected_postings,
            &mut incumbent_hits,
            &mut incumbent_selection,
        );
        if solver_visits == maximum_solver_nodes {
            interrupted_upper = Some(upper);
            break;
        }
        if upper < incumbent_hits || frame.selected_count == selected_postings {
            continue;
        }

        let mut branch = None;
        for posting in 0..posting_count {
            if !v38_mask_contains(frame.candidates, posting) {
                continue;
            }
            let gain = v38_uncovered_gain(posting_masks[posting as usize], frame.covered);
            if branch.is_none_or(|(_, best_gain)| gain > best_gain) {
                branch = Some((posting, gain));
            }
        }
        let Some((posting, gain)) = branch else {
            continue;
        };
        if gain == 0 {
            continue;
        }

        let mut remaining = frame.candidates;
        v38_mask_remove(&mut remaining, posting);
        stack.push(V38CoverageFrame {
            candidates: remaining,
            ..frame
        });
        let mut selected = frame.selected;
        v38_mask_insert(&mut selected, posting);
        stack.push(V38CoverageFrame {
            candidates: remaining,
            selected,
            covered: [
                frame.covered[0] | posting_masks[posting as usize][0],
                frame.covered[1] | posting_masks[posting as usize][1],
            ],
            selected_count: frame.selected_count + 1,
        });
    }

    let exact = interrupted_upper.is_none();
    let certified_upper_hits = if exact {
        incumbent_hits
    } else {
        stack
            .iter()
            .copied()
            .map(|frame| {
                v38_frame_upper(
                    frame,
                    &posting_masks,
                    posting_count,
                    selected_postings,
                    truth_rows,
                )
            })
            .chain(interrupted_upper)
            .fold(incumbent_hits, u32::max)
    };
    Ok(V38CoverageCertificate {
        query_ordinal: 0,
        selected_postings: incumbent_selection,
        feasible_hits: incumbent_hits,
        certified_upper_hits,
        exact,
        solver_visits,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum V38TerminalDisposition {
    LayoutFeasible,
    LayoutRejected,
    Indeterminate,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V38LayoutCeiling {
    schema: String,
    claim_eligible: bool,
    authority: V38CeilingAuthority,
    gt_neighbors: u32,
    selected_postings: u32,
    aggregate_gate_ppm: u32,
    minimum_gate_ppm: u32,
    certificates: Vec<V38CoverageCertificate>,
    aggregate_feasible_ppm: u32,
    aggregate_certified_upper_ppm: u32,
    minimum_feasible_ppm: u32,
    minimum_certified_upper_ppm: u32,
    exact_query_count: u32,
    total_solver_visits: u64,
    passed: bool,
    disposition: V38TerminalDisposition,
}

fn v38_zero_visit_bounds(
    query_ordinal: u32,
    owners: &[(u32, Option<u32>)],
    posting_count: u32,
    selected_postings: u32,
) -> Result<V38CoverageCertificate> {
    if owners.is_empty()
        || owners.len() > V38_GT_NEIGHBORS as usize
        || posting_count == 0
        || posting_count > V38_POSTING_COUNT
        || selected_postings == 0
        || selected_postings > posting_count
        || selected_postings > V38_SELECTED_POSTINGS
    {
        return Err(invalid("V38 query coverage authority differs"));
    }
    let mut posting_masks = vec![[0_u64; 2]; posting_count as usize];
    for (row, (primary, alternate)) in owners.iter().copied().enumerate() {
        if primary >= posting_count
            || alternate.is_some_and(|posting| posting >= posting_count || posting == primary)
        {
            return Err(invalid("V38 query coverage owner differs"));
        }
        let word = row / 64;
        let bit = 1_u64 << (row % 64);
        posting_masks[primary as usize][word] |= bit;
        if let Some(posting) = alternate {
            posting_masks[posting as usize][word] |= bit;
        }
    }

    let mut selected = [0_u64; 2];
    let mut covered = [0_u64; 2];
    for _ in 0..selected_postings {
        let mut best = None;
        for posting in 0..posting_count {
            if v38_mask_contains(selected, posting) {
                continue;
            }
            let gain = v38_uncovered_gain(posting_masks[posting as usize], covered);
            if best.is_none_or(|(_, best_gain)| gain > best_gain) {
                best = Some((posting, gain));
            }
        }
        let Some((posting, gain)) = best else {
            break;
        };
        if gain == 0 {
            break;
        }
        v38_mask_insert(&mut selected, posting);
        covered[0] |= posting_masks[posting as usize][0];
        covered[1] |= posting_masks[posting as usize][1];
    }
    let selected = v38_padded_selection(selected, posting_count, selected_postings);
    let feasible_hits = v38_selection_hits(&selected, &posting_masks);
    let mut candidates = [0_u64; 2];
    for posting in 0..posting_count {
        v38_mask_insert(&mut candidates, posting);
    }
    let certified_upper_hits = v38_frame_upper(
        V38CoverageFrame {
            candidates,
            selected: [0_u64; 2],
            covered: [0_u64; 2],
            selected_count: 0,
        },
        &posting_masks,
        posting_count,
        selected_postings,
        owners.len() as u32,
    );
    Ok(V38CoverageCertificate {
        query_ordinal,
        selected_postings: selected,
        feasible_hits,
        certified_upper_hits,
        exact: feasible_hits == certified_upper_hits,
        solver_visits: 0,
    })
}

#[cfg(test)]
fn solve_v38_coverage_batch(
    query_owners: &[Vec<(u32, Option<u32>)>],
    posting_count: u32,
    selected_postings: u32,
    maximum_query_solver_nodes: u64,
    maximum_solver_nodes: u64,
) -> Result<Vec<V38CoverageCertificate>> {
    solve_v38_coverage_batch_with_progress(
        query_owners,
        posting_count,
        selected_postings,
        maximum_query_solver_nodes,
        maximum_solver_nodes,
        |_| Ok(()),
    )
}

fn solve_v38_coverage_batch_with_progress<F>(
    query_owners: &[Vec<(u32, Option<u32>)>],
    posting_count: u32,
    selected_postings: u32,
    maximum_query_solver_nodes: u64,
    maximum_solver_nodes: u64,
    mut report_completed_queries: F,
) -> Result<Vec<V38CoverageCertificate>>
where
    F: FnMut(u32) -> Result<()>,
{
    if query_owners.is_empty()
        || maximum_query_solver_nodes == 0
        || maximum_query_solver_nodes > V38_MAXIMUM_QUERY_SOLVER_NODES
        || maximum_solver_nodes == 0
        || maximum_solver_nodes > V38_MAXIMUM_SOLVER_NODES
    {
        return Err(invalid("V38 coverage batch authority differs"));
    }
    let mut certificates = query_owners
        .iter()
        .enumerate()
        .map(|(query_ordinal, owners)| {
            v38_zero_visit_bounds(
                query_ordinal as u32,
                owners,
                posting_count,
                selected_postings,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let mut total_solver_visits = 0_u64;
    for (query_ordinal, (certificate, owners)) in
        certificates.iter_mut().zip(query_owners).enumerate()
    {
        if certificate.exact {
            let completed = u32::try_from(query_ordinal + 1)
                .map_err(|_| invalid("V38 completed query count overflows"))?;
            if completed % 32 == 0 || query_ordinal + 1 == query_owners.len() {
                report_completed_queries(completed)?;
            }
            continue;
        }
        let remaining = maximum_solver_nodes.saturating_sub(total_solver_visits);
        if remaining == 0 {
            break;
        }
        let mut solved = solve_v38_query_coverage(
            owners,
            posting_count,
            selected_postings,
            remaining.min(maximum_query_solver_nodes),
        )?;
        solved.query_ordinal = query_ordinal as u32;
        total_solver_visits = total_solver_visits
            .checked_add(solved.solver_visits)
            .ok_or_else(|| invalid("V38 solver visits overflow"))?;
        *certificate = solved;
        let completed = u32::try_from(query_ordinal + 1)
            .map_err(|_| invalid("V38 completed query count overflows"))?;
        if completed % 32 == 0 || query_ordinal + 1 == query_owners.len() {
            report_completed_queries(completed)?;
        }
    }
    Ok(certificates)
}

fn v38_recall_ppm(hits: u64, queries: usize) -> Result<u32> {
    let denominator = u64::try_from(queries)
        .ok()
        .and_then(|queries| queries.checked_mul(u64::from(V38_GT_NEIGHBORS)))
        .ok_or_else(|| invalid("V38 ceiling denominator overflows"))?;
    u32::try_from(
        hits.checked_mul(1_000_000)
            .ok_or_else(|| invalid("V38 ceiling recall overflows"))?
            / denominator,
    )
    .map_err(|_| invalid("V38 ceiling recall exceeds ppm range"))
}

fn v38_finish_layout_ceiling(
    authority: &V38CeilingAuthority,
    certificates: Vec<V38CoverageCertificate>,
) -> Result<V38LayoutCeiling> {
    validate_ceiling_authority(authority)?;
    let feasible_hits = certificates.iter().try_fold(0_u64, |sum, certificate| {
        sum.checked_add(u64::from(certificate.feasible_hits))
    });
    let feasible_hits = feasible_hits.ok_or_else(|| invalid("V38 feasible hits overflow"))?;
    let upper_hits = certificates.iter().try_fold(0_u64, |sum, certificate| {
        sum.checked_add(u64::from(certificate.certified_upper_hits))
    });
    let upper_hits = upper_hits.ok_or_else(|| invalid("V38 upper hits overflow"))?;
    let aggregate_feasible_ppm = v38_recall_ppm(feasible_hits, certificates.len())?;
    let aggregate_certified_upper_ppm = v38_recall_ppm(upper_hits, certificates.len())?;
    let minimum_feasible_ppm = certificates
        .iter()
        .map(|certificate| certificate.feasible_hits * 10_000)
        .min()
        .ok_or_else(|| invalid("V38 ceiling certificate is absent"))?;
    let minimum_certified_upper_ppm = certificates
        .iter()
        .map(|certificate| certificate.certified_upper_hits * 10_000)
        .min()
        .ok_or_else(|| invalid("V38 ceiling certificate is absent"))?;
    let total_solver_visits = certificates.iter().try_fold(0_u64, |sum, certificate| {
        sum.checked_add(certificate.solver_visits)
    });
    let total_solver_visits =
        total_solver_visits.ok_or_else(|| invalid("V38 solver visits overflow"))?;
    let feasible = aggregate_feasible_ppm >= V38_AGGREGATE_GATE_PPM
        && minimum_feasible_ppm >= V38_MINIMUM_GATE_PPM;
    let rejected = aggregate_certified_upper_ppm < V38_AGGREGATE_GATE_PPM
        || minimum_certified_upper_ppm < V38_MINIMUM_GATE_PPM;
    let disposition = if feasible {
        V38TerminalDisposition::LayoutFeasible
    } else if rejected {
        V38TerminalDisposition::LayoutRejected
    } else {
        V38TerminalDisposition::Indeterminate
    };
    Ok(V38LayoutCeiling {
        schema: "borsuk-v38-boundary-spill-ceiling-v1".to_owned(),
        claim_eligible: false,
        authority: authority.clone(),
        gt_neighbors: V38_GT_NEIGHBORS,
        selected_postings: V38_SELECTED_POSTINGS,
        aggregate_gate_ppm: V38_AGGREGATE_GATE_PPM,
        minimum_gate_ppm: V38_MINIMUM_GATE_PPM,
        exact_query_count: certificates
            .iter()
            .filter(|certificate| certificate.exact)
            .count() as u32,
        certificates,
        aggregate_feasible_ppm,
        aggregate_certified_upper_ppm,
        minimum_feasible_ppm,
        minimum_certified_upper_ppm,
        total_solver_visits,
        passed: disposition == V38TerminalDisposition::LayoutFeasible,
        disposition,
    })
}

#[cfg(test)]
pub(crate) fn evaluate_v38_multi_owner_ceiling(
    authority: &V38CeilingAuthority,
    relation: &V38SpillRelation,
    truth: &[crate::v37_relation_router::V37FeatureGroundTruth],
) -> Result<V38LayoutCeiling> {
    evaluate_v38_multi_owner_ceiling_with_progress(authority, relation, truth, |_| Ok(()))
}

fn evaluate_v38_multi_owner_ceiling_with_progress<F>(
    authority: &V38CeilingAuthority,
    relation: &V38SpillRelation,
    truth: &[crate::v37_relation_router::V37FeatureGroundTruth],
    mut report_completed_queries: F,
) -> Result<V38LayoutCeiling>
where
    F: FnMut(u32) -> Result<()>,
{
    validate_ceiling_authority(authority)?;
    if relation.postings.len() != V38_POSTING_COUNT as usize
        || truth.len() != V38_QUERY_COUNT as usize
    {
        return Err(invalid("V38 ceiling input population differs"));
    }
    validate_v38_spill_relation_shape(&relation.records)?;
    validate_v38_posting_summaries(&relation.postings)?;
    if summarize_v38_spill_relation(
        &relation.records,
        V38_POSTING_COUNT,
        V38_MAXIMUM_ROWS_PER_POSTING,
    )? != relation.postings
    {
        return Err(invalid("V38 ceiling posting summary differs"));
    }

    let mut owners_by_feature = std::collections::BTreeMap::new();
    for record in &relation.records {
        if record.owner_role == 0 {
            if owners_by_feature
                .insert(record.feature_row_id, (record.posting_ordinal, None))
                .is_some()
            {
                return Err(invalid("V38 ceiling feature owner is duplicated"));
            }
        } else {
            let owners = owners_by_feature
                .get_mut(&record.feature_row_id)
                .ok_or_else(|| invalid("V38 ceiling primary feature owner is absent"))?;
            if owners.1.replace(record.posting_ordinal).is_some() {
                return Err(invalid("V38 ceiling alternate feature owner is duplicated"));
            }
        }
    }

    let mut query_owners = Vec::with_capacity(truth.len());
    let mut certificates = Vec::with_capacity(truth.len());
    for (query_ordinal, query) in truth.iter().enumerate() {
        if query.query_ordinal != query_ordinal as u32
            || query.feature_row_ids.len() != V38_GT_NEIGHBORS as usize
            || query
                .feature_row_ids
                .iter()
                .copied()
                .collect::<BTreeSet<_>>()
                .len()
                != V38_GT_NEIGHBORS as usize
        {
            return Err(invalid("V38 ceiling truth shape differs"));
        }
        let owners = query
            .feature_row_ids
            .iter()
            .map(|feature| {
                owners_by_feature
                    .get(feature)
                    .copied()
                    .ok_or_else(|| invalid("V38 ceiling truth feature is unknown"))
            })
            .collect::<Result<Vec<_>>>()?;
        certificates.push(v38_zero_visit_bounds(
            query.query_ordinal,
            &owners,
            V38_POSTING_COUNT,
            V38_SELECTED_POSTINGS,
        )?);
        query_owners.push(owners);
    }

    let bounds = v38_finish_layout_ceiling(authority, certificates)?;
    if bounds.disposition != V38TerminalDisposition::Indeterminate {
        let mut completed = 32;
        while completed < V38_QUERY_COUNT {
            report_completed_queries(completed)?;
            completed += 32;
        }
        report_completed_queries(V38_QUERY_COUNT)?;
        return Ok(bounds);
    }

    let certificates = solve_v38_coverage_batch_with_progress(
        &query_owners,
        V38_POSTING_COUNT,
        V38_SELECTED_POSTINGS,
        V38_MAXIMUM_QUERY_SOLVER_NODES,
        V38_MAXIMUM_SOLVER_NODES,
        report_completed_queries,
    )?;
    v38_finish_layout_ceiling(authority, certificates)
}

fn validate_v38_layout_ceiling(result: &V38LayoutCeiling) -> Result<()> {
    if result.schema != "borsuk-v38-boundary-spill-ceiling-v1"
        || result.claim_eligible
        || result.gt_neighbors != V38_GT_NEIGHBORS
        || result.selected_postings != V38_SELECTED_POSTINGS
        || result.aggregate_gate_ppm != V38_AGGREGATE_GATE_PPM
        || result.minimum_gate_ppm != V38_MINIMUM_GATE_PPM
        || result.certificates.len() != V38_QUERY_COUNT as usize
    {
        return Err(invalid("V38 ceiling result authority differs"));
    }
    for (query_ordinal, certificate) in result.certificates.iter().enumerate() {
        let unique = certificate
            .selected_postings
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        if certificate.query_ordinal != query_ordinal as u32
            || certificate.selected_postings.len() != V38_SELECTED_POSTINGS as usize
            || unique.len() != certificate.selected_postings.len()
            || !certificate
                .selected_postings
                .windows(2)
                .all(|pair| pair[0] < pair[1])
            || certificate
                .selected_postings
                .last()
                .is_none_or(|posting| *posting >= V38_POSTING_COUNT)
            || certificate.feasible_hits > certificate.certified_upper_hits
            || certificate.certified_upper_hits > V38_GT_NEIGHBORS
            || certificate.exact && certificate.feasible_hits != certificate.certified_upper_hits
            || certificate.solver_visits > V38_MAXIMUM_QUERY_SOLVER_NODES
            || canonical_bytes(certificate, "coverage certificate")?.len()
                > V38_MAXIMUM_CERTIFICATE_BYTES as usize
        {
            return Err(invalid("V38 ceiling certificate differs"));
        }
    }
    let expected = v38_finish_layout_ceiling(&result.authority, result.certificates.clone())?;
    if &expected != result || result.total_solver_visits > V38_MAXIMUM_SOLVER_NODES {
        return Err(invalid("V38 ceiling aggregate differs"));
    }
    Ok(())
}

pub(crate) fn canonical_v38_ceiling_bytes(result: &V38LayoutCeiling) -> Result<Vec<u8>> {
    validate_v38_layout_ceiling(result)?;
    canonical_bytes(result, "ceiling result")
}

#[cfg(test)]
pub(crate) fn validate_v38_ceiling_bytes(
    authority: &V38CeilingAuthority,
    relation: &V38SpillRelation,
    truth: &[crate::v37_relation_router::V37FeatureGroundTruth],
    claimed: &[u8],
) -> Result<Vec<u8>> {
    let result: V38LayoutCeiling = serde_json::from_slice(claimed)
        .map_err(|error| invalid(&format!("V38 ceiling result parsing failed: {error}")))?;
    if &result.authority != authority {
        return Err(invalid("V38 ceiling result authority binding differs"));
    }
    let canonical = canonical_v38_ceiling_bytes(&result)?;
    if canonical != claimed {
        return Err(invalid("V38 ceiling result bytes are not canonical"));
    }
    let replay = evaluate_v38_multi_owner_ceiling(authority, relation, truth)?;
    let expected = canonical_v38_ceiling_bytes(&replay)?;
    if expected != canonical {
        return Err(invalid("V38 ceiling replay differs"));
    }
    Ok(canonical)
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use sha2::{Digest, Sha256};

    use super::{
        V38ArtifactIdentity, V38CeilingAuthority, V38ConstructionAuthority,
        V38ConstructionEvidence, V38ConstructionResult, V38CoverageCertificate, V38LayoutCeiling,
        V38LocalArtifact, V38LocalOutput, V38LocalRunMode, V38LocalRunRequest, V38OutputTarget,
        V38PostingSummary, V38SpillBuildLimits, V38SpillProposal, V38SpillRecord, V38SpillRelation,
        V38SpillSpec, V38TerminalDisposition, admit_v38_spill_proposals, build_v38_spill_relation,
        build_v38_spill_relation_flat_with_evidence_and_progress,
        build_v38_spill_relation_slices_with_evidence_and_progress, canonical_bytes,
        canonical_v38_ceiling_authority_bytes, canonical_v38_ceiling_bytes,
        canonical_v38_construction_authority_bytes, canonical_v38_construction_result_bytes,
        decode_v38_posting_summary_parquet, decode_v38_spill_relation_parquet,
        decode_v38_spill_relation_parquet_unbound, encode_v38_posting_summary_parquet,
        encode_v38_spill_relation_parquet, encode_v38_spill_relation_parquet_with_row_group_size,
        evaluate_v38_multi_owner_ceiling, evaluate_v38_multi_owner_ceiling_with_progress,
        project_v38_admission_auxiliary_bytes, project_v38_spill_capacity,
        propose_v38_alternate_owner, report_v38_build_scoring_progress, run_v38_local_request,
        run_v38_local_request_with_progress, solve_v38_coverage_batch, solve_v38_query_coverage,
        summarize_v38_spill_relation, validate_v38_ceiling_authority_bytes,
        validate_v38_ceiling_bytes, validate_v38_construction_authority_bytes,
        validate_v38_spill_spec, validate_v38_v37_construction_evidence,
    };
    use crate::v37_relation_router::{
        V37BalancedNode, V37BalancedTree, V37FeatureGroundTruth, V37OwnershipRecord,
    };

    fn digest(byte: u8) -> String {
        format!("{byte:02x}").repeat(32)
    }

    fn artifact(role: &str, byte: u8) -> V38ArtifactIdentity {
        V38ArtifactIdentity {
            blake3: digest(byte.wrapping_add(1)),
            encoded_bytes: 100 + u64::from(byte),
            role: role.to_owned(),
            sha256: digest(byte),
            uri: format!("s3://borsuk-test/v38/{role}-{byte}"),
        }
    }

    fn local_artifact(root: &Path, role: &str, byte: u8) -> V38LocalArtifact {
        let bytes = format!("registered-v38-{role}-{byte}\n").into_bytes();
        local_artifact_bytes(root, role, &bytes)
    }

    fn local_artifact_bytes(root: &Path, role: &str, bytes: &[u8]) -> V38LocalArtifact {
        let path = root.join(role);
        fs::write(&path, bytes).unwrap();
        V38LocalArtifact::try_new(
            role.to_owned(),
            path,
            format!("s3://borsuk-test/v38-local/{role}"),
            format!("{:x}", Sha256::digest(bytes)),
            blake3::hash(bytes).to_hex().to_string(),
            bytes.len() as u64,
        )
        .unwrap()
    }

    fn local_output(root: &Path, role: &str) -> V38LocalOutput {
        V38LocalOutput::try_new(role.to_owned(), root.join(format!("out-{role}"))).unwrap()
    }

    fn local_request(root: &Path, mode: V38LocalRunMode) -> V38LocalRunRequest {
        let (roles, outputs): (&[&str], &[&str]) = match mode {
            V38LocalRunMode::PreflightSpill => (&["v38-authority"], &[]),
            V38LocalRunMode::BuildSpill => (
                &[
                    "v38-authority",
                    "v37-authority",
                    "v37-build-manifest",
                    "v37-local-result",
                    "v37-terminal",
                    "v37-tree",
                    "v37-ownership",
                    "source",
                ],
                &["spill-relation", "spill-postings"],
            ),
            V38LocalRunMode::EvaluateCeiling => (
                &[
                    "v38-ceiling-authority",
                    "v38-construction-result",
                    "spill-relation",
                    "spill-postings",
                    "gt100",
                ],
                &["ceiling"],
            ),
        };
        let inputs = roles
            .iter()
            .enumerate()
            .map(|(index, role)| local_artifact(root, role, index as u8 + 1))
            .collect();
        let outputs = outputs
            .iter()
            .map(|role| local_output(root, role))
            .collect();
        V38LocalRunRequest::try_new(mode, inputs, outputs, 4).unwrap()
    }

    #[test]
    fn v38_boundary_local_exposes_only_three_exact_phase_capability_sets() {
        let root = tempfile::tempdir().unwrap();
        let preflight = local_request(root.path(), V38LocalRunMode::PreflightSpill);
        assert_eq!(preflight.input_roles(), ["v38-authority"]);
        assert!(preflight.output_roles().is_empty());

        let root = tempfile::tempdir().unwrap();
        let build = local_request(root.path(), V38LocalRunMode::BuildSpill);
        assert_eq!(
            build.input_roles(),
            [
                "v38-authority",
                "v37-authority",
                "v37-build-manifest",
                "v37-local-result",
                "v37-terminal",
                "v37-tree",
                "v37-ownership",
                "source",
            ]
        );
        assert_eq!(build.output_roles(), ["spill-relation", "spill-postings"]);

        let root = tempfile::tempdir().unwrap();
        let ceiling = local_request(root.path(), V38LocalRunMode::EvaluateCeiling);
        assert_eq!(
            ceiling.input_roles(),
            [
                "v38-ceiling-authority",
                "v38-construction-result",
                "spill-relation",
                "spill-postings",
                "gt100",
            ]
        );
        assert_eq!(ceiling.output_roles(), ["ceiling"]);
        assert_eq!(ceiling.workers(), 4);
    }

    #[test]
    fn v38_boundary_local_rejects_role_uri_and_path_capability_overlap() {
        let root = tempfile::tempdir().unwrap();
        let baseline = local_request(root.path(), V38LocalRunMode::BuildSpill);

        let mut wrong_role = baseline.clone();
        wrong_role.inputs.swap(0, 1);
        let mode = wrong_role.mode();
        let workers = wrong_role.workers();
        assert!(
            V38LocalRunRequest::try_new(mode, wrong_role.inputs, wrong_role.outputs, workers,)
                .is_err()
        );

        let mut duplicate_uri = baseline.clone();
        duplicate_uri.inputs[1].uri = duplicate_uri.inputs[0].uri.clone();
        let mode = duplicate_uri.mode();
        let workers = duplicate_uri.workers();
        assert!(
            V38LocalRunRequest::try_new(
                mode,
                duplicate_uri.inputs,
                duplicate_uri.outputs,
                workers,
            )
            .is_err()
        );

        let mut duplicate_path = baseline.clone();
        duplicate_path.inputs[1].path = duplicate_path.inputs[0].path.clone();
        let mode = duplicate_path.mode();
        let workers = duplicate_path.workers();
        assert!(
            V38LocalRunRequest::try_new(
                mode,
                duplicate_path.inputs,
                duplicate_path.outputs,
                workers,
            )
            .is_err()
        );
    }

    #[test]
    fn v38_boundary_local_authenticates_real_files_and_rejects_output_inode_aliases() {
        let root = tempfile::tempdir().unwrap();
        let request = local_request(root.path(), V38LocalRunMode::BuildSpill);
        fs::hard_link(request.inputs[5].path(), request.outputs[0].path()).unwrap();

        assert!(run_v38_local_request(request).is_err());
        let _progress_runner =
            run_v38_local_request_with_progress::<fn(&[u8]) -> std::io::Result<()>>;
    }

    #[test]
    fn v38_boundary_local_build_rejects_runtime_worker_drift_before_science() {
        let root = tempfile::tempdir().unwrap();
        let mut request = local_request(root.path(), V38LocalRunMode::BuildSpill);
        let authority =
            canonical_v38_construction_authority_bytes(&construction_authority()).unwrap();
        request.inputs[0] = local_artifact_bytes(root.path(), "v38-authority", &authority);
        let error = run_v38_local_request(request).unwrap_err().to_string();
        assert!(error.contains("construction worker binding differs"));
    }

    #[test]
    #[cfg_attr(
        debug_assertions,
        ignore = "wall-clock admission requires the optimized diagnostic binary"
    )]
    fn v38_boundary_local_preflight_runs_real_bounded_source_free_primitives() {
        let root = tempfile::tempdir().unwrap();
        let mut authority = construction_authority();
        authority.workers = 4;
        authority.numeric_backend =
            crate::v37_relation_router::score_v37_hyperplane_fused(&[0.0; 192], &[0.0; 192])
                .unwrap()
                .1
                .to_owned();
        let authority_bytes = canonical_v38_construction_authority_bytes(&authority).unwrap();
        let request = V38LocalRunRequest::try_new(
            V38LocalRunMode::PreflightSpill,
            vec![local_artifact_bytes(
                root.path(),
                "v38-authority",
                &authority_bytes,
            )],
            Vec::new(),
            4,
        )
        .unwrap();
        let mut progress = Vec::new();
        let bytes = run_v38_local_request_with_progress(request, |snapshot| {
            progress.push(snapshot.to_vec());
            Ok(())
        })
        .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

        assert_eq!(value["schema"], "borsuk-v38-local-result-v1");
        assert_eq!(value["mode"], "preflight-spill");
        assert_eq!(value["claim_eligible"], false);
        assert_eq!(value["inputs"].as_array().unwrap().len(), 1);
        assert_eq!(value["preflight_evidence"]["rows"], 65_536);
        assert_eq!(value["preflight_evidence"]["dimensions"], 192);
        assert_eq!(value["preflight_evidence"]["leaf_count"], 16);
        assert_eq!(
            value["preflight_evidence"]["proposal_coordinate_scores"],
            188_743_680_u64
        );
        assert_eq!(
            value["preflight_evidence"]["scalar_fused_comparisons"],
            15_360
        );
        assert_eq!(value["preflight_evidence"]["scalar_fused_max_ulp_delta"], 0);
        assert!(
            value["preflight_evidence"]["proposal_coordinates_per_second"]
                .as_u64()
                .unwrap()
                >= 78_080_000
        );
        assert!(
            value["preflight_evidence"]["projected_scoring_elapsed_ns"]
                .as_u64()
                .unwrap()
                <= 300_000_000_000
        );
        assert!(
            value["preflight_evidence"]["projected_construction_elapsed_ns"]
                .as_u64()
                .unwrap()
                <= 450_000_000_000
        );
        assert_eq!(
            value["preflight_evidence"]["predecessor_replay_allowance_ns"],
            120_000_000_000_u64
        );
        assert_eq!(
            value["preflight_evidence"]["projected_construction_elapsed_ns"]
                .as_u64()
                .unwrap(),
            value["preflight_evidence"]["projected_scoring_elapsed_ns"]
                .as_u64()
                .unwrap()
                + value["preflight_evidence"]["projected_spill_post_scoring_elapsed_ns"]
                    .as_u64()
                    .unwrap()
                + 120_000_000_000
        );
        assert!(
            value["preflight_evidence"]["projected_peak_bytes"]
                .as_u64()
                .unwrap()
                <= 3 * 1_073_741_824
        );
        assert!(
            value["preflight_evidence"]["relation_rows"]
                .as_u64()
                .is_some_and(|rows| (65_536..=81_920).contains(&rows))
        );
        assert_eq!(bytes.last(), Some(&b'\n'));
        assert!(!progress.is_empty());
        assert!(
            progress
                .iter()
                .all(|snapshot| snapshot.last() == Some(&b'\n'))
        );
        assert!(!root.path().join("source").exists());
        assert!(!root.path().join("gt100").exists());
    }

    fn registered_spec() -> V38SpillSpec {
        V38SpillSpec {
            corpus_rows: 1_000_000,
            posting_count: 123,
            maximum_alternate_assignments: 250_000,
            maximum_owners_per_row: 2,
            maximum_rows_per_posting: 10_240,
            projected_record_bytes: 48,
            projected_framing_allowance_bytes: 32_768,
            selected_postings: 14,
        }
    }

    fn construction_authority() -> V38ConstructionAuthority {
        V38ConstructionAuthority {
            algorithm: "query-blind-hyperplane-boundary-spill-v1".to_owned(),
            inputs: vec![
                artifact("v37-authority", 1),
                artifact("v37-build-manifest", 2),
                artifact("v37-local-result", 3),
                artifact("v37-terminal", 4),
                artifact("v37-tree", 5),
                artifact("v37-ownership", 7),
                artifact("source", 9),
            ],
            metric: "squared-l2".to_owned(),
            numeric_backend: "aarch64-neon-fma".to_owned(),
            outputs: vec![
                V38OutputTarget {
                    role: "spill-relation".to_owned(),
                    uri: "s3://borsuk-test/v38/spill-relation.parquet".to_owned(),
                },
                V38OutputTarget {
                    role: "spill-postings".to_owned(),
                    uri: "s3://borsuk-test/v38/spill-postings.parquet".to_owned(),
                },
            ],
            schema: "borsuk-v38-boundary-spill-authority-v1".to_owned(),
            source_archive: artifact("source-archive", 11),
            source_commit: "53342ce869248c02eac00b968721ec739b1cc539".to_owned(),
            spec: registered_spec(),
            workers: 32,
        }
    }

    fn v37_authority_bytes(source: &V38ArtifactIdentity, workers: u32, backend: &str) -> Vec<u8> {
        canonical_bytes(
            &serde_json::json!({
                "algorithm": "balanced-hyperplane-relation-v1",
                "metric": "squared-l2",
                "numeric": {
                    "fma_backend": backend,
                    "lane_width": 8,
                    "preflight_coordinate_scores": 50_331_648_u64,
                    "preflight_coordinate_sha256": "62ebfdbb42fa5e6212437932ebe682970389b15fa8cae1d6e2aa6d8610a41608",
                    "worker_count": workers,
                },
                "projection": {
                    "algorithm": "v36-srht-f32-v1",
                    "projected_corpus_sha256": digest(81),
                    "routing_dimensions": 192,
                    "seed": 36,
                    "source_dimensions": 768,
                },
                "relation": {
                    "leaf_count": 4_096,
                    "maximum_leaf_probes": 32,
                    "prefix_lengths": [16, 32, 64],
                    "q_bits": 24,
                    "seed": 73,
                },
                "schema": "borsuk-v37-relation-authority-v3",
                "source": {
                    "blake3": source.blake3,
                    "encoded_bytes": source.encoded_bytes,
                    "role": "source-corpus",
                    "sha256": source.sha256,
                    "uri": source.uri,
                },
                "tree": {
                    "corpus_rows": 1_000_000,
                    "dimensions": 192,
                    "seed": 37,
                    "target_primary_rows": 8_192,
                    "training_sample_rows": 4_096,
                    "two_means_iterations": 8,
                },
            }),
            "V37 authority fixture",
        )
        .unwrap()
    }

    fn v37_construction_terminal_bytes(
        manifest: &V38ArtifactIdentity,
        result: &V38ArtifactIdentity,
        tree: &V38ArtifactIdentity,
        ownership: &V38ArtifactIdentity,
    ) -> Vec<u8> {
        canonical_bytes(
            &serde_json::json!({
                "artifacts": [
                    {
                        "blake3": tree.blake3,
                        "encoded_bytes": tree.encoded_bytes,
                        "role": "ownership-tree",
                        "sha256": tree.sha256,
                        "uri": tree.uri,
                    },
                    {
                        "blake3": ownership.blake3,
                        "encoded_bytes": ownership.encoded_bytes,
                        "role": "ownership",
                        "sha256": ownership.sha256,
                        "uri": ownership.uri,
                    }
                ],
                "binary_bytes": 9_001,
                "binary_sha256": digest(83),
                "binary_uri": "s3://borsuk-test/v37/binary",
                "claim_eligible": false,
                "instance_id": "i-0123456789abcdef0",
                "manifest_bytes": manifest.encoded_bytes,
                "manifest_sha256": manifest.sha256,
                "manifest_uri": manifest.uri,
                "monitor": {
                    "elapsed_milliseconds": 103_126,
                    "peak_psi_full_ppm": 0,
                    "peak_rss_bytes": 2_716_549_120_u64,
                    "swap_end_bytes": 0,
                    "swap_start_bytes": 0,
                },
                "phase": "build-ownership",
                "progress": {
                    "encoded_bytes": 300,
                    "role": "progress",
                    "sha256": digest(85),
                    "uri": "s3://borsuk-test/v37/progress.json",
                },
                "result": {
                    "encoded_bytes": result.encoded_bytes,
                    "role": "local-result",
                    "sha256": result.sha256,
                    "uri": result.uri,
                },
                "run_id": "v37-build-fixture",
                "schema": "borsuk-v37-spot-terminal-v2",
                "source_archive_bytes": 1_000_003,
                "source_archive_sha256": digest(86),
                "source_archive_uri": "s3://borsuk-test/v37/source.tar.gz",
                "source_commit": "53342ce869248c02eac00b968721ec739b1cc539",
                "status": "complete",
            }),
            "V37 construction terminal fixture",
        )
        .unwrap()
    }

    fn v37_build_manifest_bytes(
        authority: &V38ArtifactIdentity,
        source: &V38ArtifactIdentity,
        tree: &V38ArtifactIdentity,
        ownership: &V38ArtifactIdentity,
        workers: u32,
    ) -> Vec<u8> {
        let mut inputs = vec![
            artifact("v36-authority", 61),
            artifact("v36-execution-authority", 62),
            artifact("v36-receipt", 63),
            artifact("v36-source-registry", 64),
            authority.clone(),
            source.clone(),
        ];
        inputs[4].role = "v37-authority".to_owned();
        inputs[5].role = "source".to_owned();
        canonical_bytes(
            &serde_json::json!({
                "claim_eligible": false,
                "inputs": inputs,
                "outputs": [
                    {"role": "ownership-tree", "uri": tree.uri},
                    {"role": "ownership", "uri": ownership.uri},
                ],
                "phase": "build-ownership",
                "run_id": "v37-build-fixture",
                "schema": "borsuk-v37-spot-phase-manifest-v1",
                "source_commit": "53342ce869248c02eac00b968721ec739b1cc539",
                "workers": workers,
            }),
            "V37 build manifest fixture",
        )
        .unwrap()
    }

    fn v37_local_result_bytes(
        manifest_bytes: &[u8],
        tree: &V38ArtifactIdentity,
        ownership: &V38ArtifactIdentity,
        backend: &str,
    ) -> Vec<u8> {
        let manifest: serde_json::Value = serde_json::from_slice(manifest_bytes).unwrap();
        canonical_bytes(
            &serde_json::json!({
                "artifacts": [
                    {
                        "blake3": tree.blake3,
                        "encoded_bytes": tree.encoded_bytes,
                        "role": "ownership-tree",
                        "sha256": tree.sha256,
                        "uri": "file:///var/tmp/v37/ownership-tree",
                    },
                    {
                        "blake3": ownership.blake3,
                        "encoded_bytes": ownership.encoded_bytes,
                        "role": "ownership",
                        "sha256": ownership.sha256,
                        "uri": "file:///var/tmp/v37/ownership",
                    },
                ],
                "claim_eligible": false,
                "inputs": manifest["inputs"],
                "mode": "build-ownership",
                "schema": "borsuk-v37-local-result-v3",
                "training_evidence": {
                    "dimensions": 192,
                    "fma_backend": backend,
                    "leaf_count": 123,
                    "partition_coordinate_scores": 1_336_195_200_u64,
                    "partition_coordinate_scores_per_second": 1_336_195_200_u64,
                    "partition_scoring_elapsed_ns": 1_000_000_000_u64,
                    "rows": 1_000_000,
                    "training_elapsed_ns": 2_000_000_000_u64,
                },
            }),
            "V37 local result fixture",
        )
        .unwrap()
    }

    fn replace_artifact_bytes(identity: &mut V38ArtifactIdentity, bytes: &[u8]) {
        identity.encoded_bytes = bytes.len() as u64;
        identity.sha256 = format!("{:x}", Sha256::digest(bytes));
        identity.blake3 = blake3::hash(bytes).to_hex().to_string();
    }

    #[test]
    fn v38_boundary_local_build_binds_exact_v37_construction_evidence() {
        let mut authority = construction_authority();
        let source = authority.inputs[6].clone();
        let v37_authority =
            v37_authority_bytes(&source, authority.workers, &authority.numeric_backend);
        replace_artifact_bytes(&mut authority.inputs[0], &v37_authority);
        let manifest = v37_build_manifest_bytes(
            &authority.inputs[0],
            &authority.inputs[6],
            &authority.inputs[4],
            &authority.inputs[5],
            authority.workers,
        );
        replace_artifact_bytes(&mut authority.inputs[1], &manifest);
        let result = v37_local_result_bytes(
            &manifest,
            &authority.inputs[4],
            &authority.inputs[5],
            &authority.numeric_backend,
        );
        replace_artifact_bytes(&mut authority.inputs[2], &result);
        let terminal = v37_construction_terminal_bytes(
            &authority.inputs[1],
            &authority.inputs[2],
            &authority.inputs[4],
            &authority.inputs[5],
        );
        replace_artifact_bytes(&mut authority.inputs[3], &terminal);

        validate_v38_v37_construction_evidence(
            &authority,
            &v37_authority,
            &manifest,
            &result,
            &terminal,
        )
        .unwrap();

        let mut wrong_partition_authority = authority.clone();
        let mut wrong_partition_result: serde_json::Value =
            serde_json::from_slice(&result).unwrap();
        wrong_partition_result["training_evidence"]["partition_coordinate_scores"] =
            serde_json::json!(23_424_000_000_u64);
        wrong_partition_result["training_evidence"]["partition_coordinate_scores_per_second"] =
            serde_json::json!(23_424_000_000_u64);
        let wrong_partition_result =
            canonical_bytes(&wrong_partition_result, "wrong V37 partition work").unwrap();
        replace_artifact_bytes(
            &mut wrong_partition_authority.inputs[2],
            &wrong_partition_result,
        );
        let mut wrong_partition_terminal: serde_json::Value =
            serde_json::from_slice(&terminal).unwrap();
        wrong_partition_terminal["result"]["encoded_bytes"] =
            serde_json::json!(wrong_partition_authority.inputs[2].encoded_bytes);
        wrong_partition_terminal["result"]["sha256"] =
            serde_json::json!(wrong_partition_authority.inputs[2].sha256);
        let wrong_partition_terminal =
            canonical_bytes(&wrong_partition_terminal, "wrong V37 partition terminal").unwrap();
        replace_artifact_bytes(
            &mut wrong_partition_authority.inputs[3],
            &wrong_partition_terminal,
        );
        assert!(
            validate_v38_v37_construction_evidence(
                &wrong_partition_authority,
                &v37_authority,
                &manifest,
                &wrong_partition_result,
                &wrong_partition_terminal,
            )
            .is_err()
        );

        let mut changed: serde_json::Value = serde_json::from_slice(&terminal).unwrap();
        changed["artifacts"][0]["sha256"] = serde_json::json!(digest(99));
        let changed = canonical_bytes(&changed, "changed V37 terminal").unwrap();
        assert!(
            validate_v38_v37_construction_evidence(
                &authority,
                &v37_authority,
                &manifest,
                &result,
                &changed,
            )
            .is_err()
        );

        let mut changed: serde_json::Value = serde_json::from_slice(&terminal).unwrap();
        changed["status"] = serde_json::json!("failed");
        let changed = canonical_bytes(&changed, "changed V37 terminal").unwrap();
        assert!(
            validate_v38_v37_construction_evidence(
                &authority,
                &v37_authority,
                &manifest,
                &result,
                &changed,
            )
            .is_err()
        );

        let wrong_workers = v37_authority_bytes(&source, 16, &authority.numeric_backend);
        assert!(
            validate_v38_v37_construction_evidence(
                &authority,
                &wrong_workers,
                &manifest,
                &result,
                &terminal,
            )
            .is_err()
        );

        let mut changed: serde_json::Value = serde_json::from_slice(&result).unwrap();
        changed["training_evidence"]["fma_backend"] = serde_json::json!("scalar-control");
        let changed = canonical_bytes(&changed, "changed V37 result").unwrap();
        assert!(
            validate_v38_v37_construction_evidence(
                &authority,
                &v37_authority,
                &manifest,
                &changed,
                &terminal,
            )
            .is_err()
        );

        let mut changed: serde_json::Value = serde_json::from_slice(&manifest).unwrap();
        changed["outputs"][0]["uri"] = serde_json::json!("s3://borsuk-test/v37/wrong-tree");
        let changed = canonical_bytes(&changed, "changed V37 manifest").unwrap();
        assert!(
            validate_v38_v37_construction_evidence(
                &authority,
                &v37_authority,
                &changed,
                &result,
                &terminal,
            )
            .is_err()
        );

        // A manifest run identifier is a filesystem/systemd token in the frozen
        // V37 writer. Re-root every dependent byte identity so this mutation
        // reaches that semantic gate rather than the outer digest gate.
        let mut invalid_token_authority = authority.clone();
        let mut invalid_token_manifest: serde_json::Value =
            serde_json::from_slice(&manifest).unwrap();
        invalid_token_manifest["run_id"] = serde_json::json!("../not-a-token");
        let invalid_token_manifest =
            canonical_bytes(&invalid_token_manifest, "invalid-token V37 manifest").unwrap();
        replace_artifact_bytes(
            &mut invalid_token_authority.inputs[1],
            &invalid_token_manifest,
        );
        let mut invalid_token_terminal: serde_json::Value =
            serde_json::from_slice(&terminal).unwrap();
        invalid_token_terminal["run_id"] = serde_json::json!("../not-a-token");
        invalid_token_terminal["manifest_bytes"] =
            serde_json::json!(invalid_token_authority.inputs[1].encoded_bytes);
        invalid_token_terminal["manifest_sha256"] =
            serde_json::json!(invalid_token_authority.inputs[1].sha256);
        let invalid_token_terminal =
            canonical_bytes(&invalid_token_terminal, "invalid-token V37 terminal").unwrap();
        replace_artifact_bytes(
            &mut invalid_token_authority.inputs[3],
            &invalid_token_terminal,
        );
        assert!(
            validate_v38_v37_construction_evidence(
                &invalid_token_authority,
                &v37_authority,
                &invalid_token_manifest,
                &result,
                &invalid_token_terminal,
            )
            .is_err()
        );

        // V37 rejects duplicate input URIs even when every object is otherwise
        // concrete and the complete dependent result/terminal chain is re-rooted.
        let mut overlap_authority = authority.clone();
        let mut overlap_manifest: serde_json::Value = serde_json::from_slice(&manifest).unwrap();
        overlap_manifest["inputs"][1]["uri"] = overlap_manifest["inputs"][0]["uri"].clone();
        let overlap_manifest =
            canonical_bytes(&overlap_manifest, "overlapping V37 manifest").unwrap();
        replace_artifact_bytes(&mut overlap_authority.inputs[1], &overlap_manifest);
        let overlap_manifest_value: serde_json::Value =
            serde_json::from_slice(&overlap_manifest).unwrap();
        let mut overlap_result: serde_json::Value = serde_json::from_slice(&result).unwrap();
        overlap_result["inputs"] = overlap_manifest_value["inputs"].clone();
        let overlap_result = canonical_bytes(&overlap_result, "overlapping V37 result").unwrap();
        replace_artifact_bytes(&mut overlap_authority.inputs[2], &overlap_result);
        let mut overlap_terminal: serde_json::Value = serde_json::from_slice(&terminal).unwrap();
        overlap_terminal["manifest_bytes"] =
            serde_json::json!(overlap_authority.inputs[1].encoded_bytes);
        overlap_terminal["manifest_sha256"] = serde_json::json!(overlap_authority.inputs[1].sha256);
        overlap_terminal["result"]["encoded_bytes"] =
            serde_json::json!(overlap_authority.inputs[2].encoded_bytes);
        overlap_terminal["result"]["sha256"] =
            serde_json::json!(overlap_authority.inputs[2].sha256);
        let overlap_terminal =
            canonical_bytes(&overlap_terminal, "overlapping V37 terminal").unwrap();
        replace_artifact_bytes(&mut overlap_authority.inputs[3], &overlap_terminal);
        assert!(
            validate_v38_v37_construction_evidence(
                &overlap_authority,
                &v37_authority,
                &overlap_manifest,
                &overlap_result,
                &overlap_terminal,
            )
            .is_err()
        );
    }

    #[test]
    fn v38_boundary_local_construction_result_binds_outputs_and_admission_evidence() {
        let authority = construction_authority();
        let result = V38ConstructionResult {
            artifacts: vec![
                artifact("spill-relation", 41),
                artifact("spill-postings", 43),
            ],
            claim_eligible: false,
            construction_evidence: V38ConstructionEvidence {
                accepted_alternates: 250_000,
                capacity_rejected: 17,
                exhausted: false,
                maximum_posting_population: 10_240,
                owners_one: 750_000,
                owners_two: 250_000,
                proposed_alternates: 1_000_000,
                total_assignments: 1_250_000,
            },
            inputs: std::iter::once(artifact("v38-authority", 39))
                .chain(authority.inputs)
                .collect(),
            mode: "build-spill".to_owned(),
            schema: "borsuk-v38-local-result-v1".to_owned(),
        };
        let canonical = canonical_v38_construction_result_bytes(&result).unwrap();
        assert_eq!(canonical.last(), Some(&b'\n'));

        let mut changed = result.clone();
        changed.construction_evidence.owners_one -= 1;
        assert!(canonical_v38_construction_result_bytes(&changed).is_err());
        let mut changed = result.clone();
        changed.artifacts.swap(0, 1);
        assert!(canonical_v38_construction_result_bytes(&changed).is_err());
        let mut changed = result;
        changed.construction_evidence.exhausted = true;
        assert!(canonical_v38_construction_result_bytes(&changed).is_err());
    }

    #[test]
    fn v38_boundary_local_ceiling_decodes_sealed_relation_without_source() {
        let relation = complete_population_relation(true);
        let encoded = encode_v38_spill_relation_parquet(&relation.records).unwrap();
        let decoded =
            decode_v38_spill_relation_parquet_unbound(&encoded, 123, 123, 10_240).unwrap();
        assert_eq!(decoded, relation.records);
    }

    fn ceiling_authority() -> V38CeilingAuthority {
        V38CeilingAuthority {
            aggregate_gate_ppm: 998_000,
            construction_result: artifact("v38-construction-result", 21),
            development_ground_truth: artifact("gt100", 23),
            gt_neighbors: 100,
            maximum_certificate_bytes: 256,
            maximum_query_solver_nodes: 250_000,
            maximum_solver_nodes: 25_000_000,
            minimum_gate_ppm: 800_000,
            posting_summary: artifact("spill-postings", 25),
            query_count: 1_000,
            relation: artifact("spill-relation", 27),
            schema: "borsuk-v38-boundary-spill-ceiling-authority-v1".to_owned(),
            selected_postings: 14,
        }
    }

    fn three_leaf_tree(root_normal: [f32; 2]) -> V37BalancedTree {
        let backend =
            crate::v37_relation_router::score_v37_hyperplane_fused(&[0.0, 0.0], &[1.0, 0.0])
                .unwrap()
                .1
                .to_owned();
        V37BalancedTree {
            dimensions: 2,
            seed: 37,
            fma_backend: backend,
            nodes: vec![
                V37BalancedNode {
                    normal: root_normal.to_vec(),
                    boundary_score_bits: 0.0_f32.to_bits(),
                    boundary_source_ordinal: 0,
                    left_node: Some(1),
                    right_node: Some(2),
                    posting_ordinal: None,
                    population: 6,
                },
                V37BalancedNode {
                    normal: vec![],
                    boundary_score_bits: 0.0_f32.to_bits(),
                    boundary_source_ordinal: 1,
                    left_node: None,
                    right_node: None,
                    posting_ordinal: Some(0),
                    population: 2,
                },
                V37BalancedNode {
                    normal: vec![0.0, 1.0],
                    boundary_score_bits: 0.0_f32.to_bits(),
                    boundary_source_ordinal: 2,
                    left_node: Some(3),
                    right_node: Some(4),
                    posting_ordinal: None,
                    population: 4,
                },
                V37BalancedNode {
                    normal: vec![],
                    boundary_score_bits: 0.0_f32.to_bits(),
                    boundary_source_ordinal: 3,
                    left_node: None,
                    right_node: None,
                    posting_ordinal: Some(1),
                    population: 2,
                },
                V37BalancedNode {
                    normal: vec![],
                    boundary_score_bits: 0.0_f32.to_bits(),
                    boundary_source_ordinal: 4,
                    left_node: None,
                    right_node: None,
                    posting_ordinal: Some(2),
                    population: 2,
                },
            ],
            leaf_populations: vec![2, 2, 2],
            assignments: vec![],
        }
    }

    #[test]
    fn v38_boundary_authority_projects_exact_registered_capacity() {
        let projection = project_v38_spill_capacity(&registered_spec()).unwrap();
        assert_eq!(projection.total_posting_capacity, 1_259_520);
        assert_eq!(projection.maximum_total_assignments, 1_250_000);
        assert_eq!(projection.maximum_posting_payload_bytes, 491_520);
        assert_eq!(projection.maximum_complete_object_bytes, 524_288);
        validate_v38_spill_spec(&registered_spec()).unwrap();
    }

    #[test]
    fn v38_boundary_authority_requires_one_full_git_commit_identity() {
        canonical_v38_construction_authority_bytes(&construction_authority()).unwrap();
        for invalid_commit in [
            "53342ce869248c02eac00b968721ec739b1cc5390",
            "53342ce869248c02eac00b968721ec739b1cc53900",
            "53342CE869248C02EAC00B968721EC739B1CC539",
        ] {
            let mut changed = construction_authority();
            changed.source_commit = invalid_commit.to_owned();
            assert!(canonical_v38_construction_authority_bytes(&changed).is_err());
        }
        let mut changed = construction_authority();
        changed.source_commit = digest(13);
        assert!(canonical_v38_construction_authority_bytes(&changed).is_err());
    }

    #[test]
    fn v38_boundary_authority_rejects_nonregistered_and_overflowing_specs() {
        for mutate in [
            |spec: &mut V38SpillSpec| spec.maximum_alternate_assignments = 250_001,
            |spec: &mut V38SpillSpec| spec.maximum_owners_per_row = 3,
            |spec: &mut V38SpillSpec| spec.maximum_rows_per_posting = 10_241,
            |spec: &mut V38SpillSpec| spec.projected_record_bytes = 49,
            |spec: &mut V38SpillSpec| spec.selected_postings = 15,
        ] {
            let mut changed = registered_spec();
            mutate(&mut changed);
            assert!(validate_v38_spill_spec(&changed).is_err());
        }
        let mut overflow = registered_spec();
        overflow.corpus_rows = u64::MAX;
        overflow.posting_count = u32::MAX;
        assert!(project_v38_spill_capacity(&overflow).is_err());
    }

    #[test]
    fn v38_boundary_authority_canonicalizes_and_binds_construction_roles() {
        let authority = construction_authority();
        let bytes = canonical_v38_construction_authority_bytes(&authority).unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));
        assert_eq!(
            validate_v38_construction_authority_bytes(&bytes).unwrap(),
            bytes
        );

        let mut changed = authority.clone();
        changed.inputs[0].role = "construction-manifest".to_owned();
        assert!(canonical_v38_construction_authority_bytes(&changed).is_err());
        let mut changed = authority.clone();
        changed.outputs[0].uri = changed.inputs[0].uri.clone();
        assert!(canonical_v38_construction_authority_bytes(&changed).is_err());
        let mut changed = authority;
        changed.numeric_backend = "scalar-control".to_owned();
        assert!(canonical_v38_construction_authority_bytes(&changed).is_err());
    }

    #[test]
    fn v38_boundary_authority_rejects_schema_type_and_digest_drift() {
        let bytes = canonical_v38_construction_authority_bytes(&construction_authority()).unwrap();
        let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("extra".to_owned(), true.into());
        assert!(
            validate_v38_construction_authority_bytes(&serde_json::to_vec(&value).unwrap())
                .is_err()
        );
        let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        value["workers"] = "32".into();
        assert!(
            validate_v38_construction_authority_bytes(&serde_json::to_vec(&value).unwrap())
                .is_err()
        );
        let mut changed = construction_authority();
        changed.inputs[2].sha256 = digest(0xff).to_uppercase();
        assert!(canonical_v38_construction_authority_bytes(&changed).is_err());
    }

    #[test]
    fn v38_boundary_authority_ceiling_binds_exact_solver_and_artifact_roles() {
        let authority = ceiling_authority();
        let bytes = canonical_v38_ceiling_authority_bytes(&authority).unwrap();
        assert_eq!(validate_v38_ceiling_authority_bytes(&bytes).unwrap(), bytes);
        for mutate in [
            |value: &mut V38CeilingAuthority| value.selected_postings = 13,
            |value: &mut V38CeilingAuthority| value.maximum_solver_nodes += 1,
            |value: &mut V38CeilingAuthority| value.aggregate_gate_ppm = 997_999,
            |value: &mut V38CeilingAuthority| value.minimum_gate_ppm = 799_999,
        ] {
            let mut changed = authority.clone();
            mutate(&mut changed);
            assert!(canonical_v38_ceiling_authority_bytes(&changed).is_err());
        }
        let mut changed = authority;
        changed.relation.role = "spill-postings".to_owned();
        assert!(canonical_v38_ceiling_authority_bytes(&changed).is_err());
    }

    #[test]
    fn v38_boundary_geometry_scores_wrong_side_path_without_renormalizing() {
        let tree = three_leaf_tree([1.000_001, 0.0]);
        let proposal = propose_v38_alternate_owner(&tree, &[-2.0, 1.0], 0).unwrap();
        assert_eq!(proposal.alternate_posting, 1);
        let left_score = (-2.0_f32).mul_add(1.000_001, 0.0);
        assert_eq!(
            proposal.alternate_violation_bits,
            (left_score * left_score).to_bits()
        );

        let proposal = propose_v38_alternate_owner(&tree, &[2.0, -1.0], 1).unwrap();
        assert_eq!(proposal.alternate_posting, 2);
        assert_eq!(proposal.alternate_violation_bits, 1.0_f32.to_bits());
    }

    #[test]
    fn v38_boundary_geometry_excludes_primary_and_breaks_zero_margin_ties_by_posting() {
        let tree = three_leaf_tree([1.0, 0.0]);
        let proposal = propose_v38_alternate_owner(&tree, &[0.0, 0.0], 0).unwrap();
        assert_eq!(proposal.alternate_posting, 1);
        assert_eq!(proposal.alternate_violation_bits, 0.0_f32.to_bits());
        assert!(propose_v38_alternate_owner(&tree, &[f32::NAN, 0.0], 0).is_err());
        assert!(propose_v38_alternate_owner(&tree, &[0.0], 0).is_err());
        assert!(propose_v38_alternate_owner(&tree, &[0.0, 0.0], 9).is_err());
    }

    #[test]
    fn v38_boundary_geometry_exercises_registered_192d_fused_blocks() {
        let mut tree = three_leaf_tree([1.0, 0.0]);
        tree.dimensions = 192;
        tree.nodes[0].normal = vec![0.0; 192];
        tree.nodes[0].normal[0] = 1.0;
        tree.nodes[2].normal = vec![0.0; 192];
        tree.nodes[2].normal[1] = 1.0;
        let mut row = vec![0.0; 192];
        row[0] = -2.0;
        row[1] = 1.0;
        for node in [&tree.nodes[0], &tree.nodes[2]] {
            let fused = crate::v37_relation_router::score_v37_hyperplane_fused(&row, &node.normal)
                .unwrap()
                .0;
            let scalar =
                crate::v37_relation_router::score_v37_hyperplane_scalar(&row, &node.normal)
                    .unwrap();
            assert_eq!(fused.to_bits(), scalar.to_bits());
        }
        let proposal = propose_v38_alternate_owner(&tree, &row, 0).unwrap();
        assert_eq!(proposal.alternate_posting, 1);
        assert_eq!(proposal.alternate_violation_bits, 4.0_f32.to_bits());
    }

    fn primary_record(
        source_ordinal: u64,
        feature_row_id: u64,
        posting_ordinal: u32,
        posting_local_ordinal: u32,
    ) -> V37OwnershipRecord {
        V37OwnershipRecord {
            source_ordinal,
            feature_row_id,
            posting_ordinal,
            posting_local_ordinal,
        }
    }

    fn proposal(alternate_posting: u32, violation: f32) -> V38SpillProposal {
        V38SpillProposal {
            alternate_posting,
            alternate_violation_bits: violation.to_bits(),
        }
    }

    #[test]
    fn v38_boundary_admission_uses_exact_fractional_fairness_and_capacity() {
        let primary = vec![
            primary_record(0, 100, 1, 0),
            primary_record(1, 101, 1, 1),
            primary_record(2, 102, 2, 0),
            primary_record(3, 103, 2, 1),
            primary_record(4, 104, 0, 0),
            primary_record(5, 105, 0, 1),
            primary_record(6, 106, 0, 2),
            primary_record(7, 107, 0, 3),
        ];
        let proposals = vec![
            (0, proposal(2, 0.5)),
            (1, proposal(0, 8.0)),
            (2, proposal(0, 5.0)),
            (3, proposal(0, 6.0)),
            (4, proposal(1, 4.0)),
            (5, proposal(1, 3.0)),
            (6, proposal(1, 2.0)),
            (7, proposal(2, 1.0)),
        ];
        let relation = admit_v38_spill_proposals(&primary, &proposals, 3, 2, 5).unwrap();
        let alternates: Vec<_> = relation
            .iter()
            .filter(|record| record.owner_role == 1)
            .map(|record| {
                (
                    record.source_ordinal,
                    record.posting_ordinal,
                    record.posting_local_ordinal,
                )
            })
            .collect();
        assert_eq!(alternates, vec![(0, 2, 2), (7, 2, 3)]);
    }

    #[test]
    fn v38_boundary_admission_never_falls_back_and_appends_by_source_ordinal() {
        let primary = vec![
            primary_record(0, 100, 0, 0),
            primary_record(1, 101, 0, 1),
            primary_record(2, 102, 1, 0),
            primary_record(3, 103, 1, 1),
            primary_record(4, 104, 2, 0),
            primary_record(5, 105, 2, 1),
        ];
        let proposals = vec![
            (3, proposal(2, 1.0)),
            (2, proposal(2, 0.5)),
            (1, proposal(2, 2.0)),
            (0, proposal(2, 3.0)),
            (4, proposal(0, 4.0)),
            (5, proposal(0, 5.0)),
        ];
        let relation = admit_v38_spill_proposals(&primary, &proposals, 3, 4, 3).unwrap();
        let alternates: Vec<_> = relation
            .iter()
            .filter(|record| record.owner_role == 1)
            .map(|record| {
                (
                    record.source_ordinal,
                    record.posting_ordinal,
                    record.posting_local_ordinal,
                )
            })
            .collect();
        assert_eq!(alternates, vec![(2, 2, 2), (4, 0, 2)]);
    }

    fn codec_fixture() -> (Vec<V37OwnershipRecord>, Vec<super::V38SpillRecord>) {
        let primary = vec![
            primary_record(0, 100, 0, 0),
            primary_record(1, 101, 0, 1),
            primary_record(2, 102, 1, 0),
            primary_record(3, 103, 1, 1),
        ];
        let proposals = vec![
            (0, proposal(1, 0.25)),
            (1, proposal(1, 0.5)),
            (2, proposal(0, 0.75)),
            (3, proposal(0, 1.0)),
        ];
        let relation = admit_v38_spill_proposals(&primary, &proposals, 2, 2, 3).unwrap();
        (primary, relation)
    }

    #[test]
    fn v38_boundary_codec_round_trips_exact_relation_and_posting_summary() {
        let (primary, relation) = codec_fixture();
        let relation_bytes = encode_v38_spill_relation_parquet(&relation).unwrap();
        assert_eq!(
            decode_v38_spill_relation_parquet(&relation_bytes, &primary, 2, 3).unwrap(),
            relation
        );

        let summaries = summarize_v38_spill_relation(&relation, 2, 3).unwrap();
        assert_eq!(
            summaries,
            vec![
                V38PostingSummary {
                    posting_ordinal: 0,
                    primary_population: 2,
                    alternate_population: 1,
                    total_population: 3,
                    projected_payload_bytes: 144,
                    projected_framing_allowance_bytes: 32_768,
                },
                V38PostingSummary {
                    posting_ordinal: 1,
                    primary_population: 2,
                    alternate_population: 1,
                    total_population: 3,
                    projected_payload_bytes: 144,
                    projected_framing_allowance_bytes: 32_768,
                },
            ]
        );
        let summary_bytes = encode_v38_posting_summary_parquet(&summaries).unwrap();
        assert_eq!(
            decode_v38_posting_summary_parquet(&summary_bytes, &relation, 2, 3).unwrap(),
            summaries
        );
    }

    #[test]
    fn v38_boundary_codec_rejects_order_owner_population_and_violation_drift() {
        let (primary, relation) = codec_fixture();
        let valid = encode_v38_spill_relation_parquet(&relation).unwrap();
        assert!(
            decode_v38_spill_relation_parquet(&valid[..valid.len() - 1], &primary, 2, 3).is_err()
        );

        let mut changed = relation.clone();
        changed.swap(0, 1);
        assert!(encode_v38_spill_relation_parquet(&changed).is_err());
        let mut changed = relation.clone();
        changed[0].owner_role = 1;
        assert!(encode_v38_spill_relation_parquet(&changed).is_err());
        let mut changed = relation.clone();
        changed[1].alternate_violation_bits = Some(f32::NAN.to_bits());
        assert!(encode_v38_spill_relation_parquet(&changed).is_err());
        let mut changed = relation.clone();
        changed[1].feature_row_id += 1;
        assert!(encode_v38_spill_relation_parquet(&changed).is_err());
        let mut changed = relation.clone();
        changed[1].posting_ordinal = changed[0].posting_ordinal;
        assert!(encode_v38_spill_relation_parquet(&changed).is_err());
        let mut changed = relation.clone();
        changed[0].posting_ordinal = u32::MAX;
        assert!(encode_v38_spill_relation_parquet(&changed).is_err());

        let summaries = summarize_v38_spill_relation(&relation, 2, 3).unwrap();
        let mut changed = summaries.clone();
        changed[0].projected_payload_bytes += 1;
        assert!(encode_v38_posting_summary_parquet(&changed).is_err());
        let mut changed = summaries;
        changed[1].posting_ordinal = 0;
        assert!(encode_v38_posting_summary_parquet(&changed).is_err());
    }

    #[test]
    fn v38_boundary_codec_accepts_multigroup_and_rejects_alternate_ordinal_swap() {
        let primary = vec![
            primary_record(0, 100, 0, 0),
            primary_record(1, 101, 0, 1),
            primary_record(2, 102, 1, 0),
            primary_record(3, 103, 1, 1),
        ];
        let relation = vec![
            super::V38SpillRecord {
                source_ordinal: 0,
                feature_row_id: 100,
                posting_ordinal: 0,
                owner_role: 0,
                posting_local_ordinal: 0,
                alternate_violation_bits: None,
            },
            super::V38SpillRecord {
                source_ordinal: 0,
                feature_row_id: 100,
                posting_ordinal: 1,
                owner_role: 1,
                posting_local_ordinal: 2,
                alternate_violation_bits: Some(0.25_f32.to_bits()),
            },
            super::V38SpillRecord {
                source_ordinal: 1,
                feature_row_id: 101,
                posting_ordinal: 0,
                owner_role: 0,
                posting_local_ordinal: 1,
                alternate_violation_bits: None,
            },
            super::V38SpillRecord {
                source_ordinal: 1,
                feature_row_id: 101,
                posting_ordinal: 1,
                owner_role: 1,
                posting_local_ordinal: 3,
                alternate_violation_bits: Some(0.5_f32.to_bits()),
            },
            super::V38SpillRecord {
                source_ordinal: 2,
                feature_row_id: 102,
                posting_ordinal: 1,
                owner_role: 0,
                posting_local_ordinal: 0,
                alternate_violation_bits: None,
            },
            super::V38SpillRecord {
                source_ordinal: 3,
                feature_row_id: 103,
                posting_ordinal: 1,
                owner_role: 0,
                posting_local_ordinal: 1,
                alternate_violation_bits: None,
            },
        ];
        let encoded = encode_v38_spill_relation_parquet_with_row_group_size(&relation, 2).unwrap();
        assert_eq!(
            decode_v38_spill_relation_parquet(&encoded, &primary, 2, 4).unwrap(),
            relation
        );
        let mut changed = relation;
        changed[1].posting_local_ordinal = 3;
        changed[3].posting_local_ordinal = 2;
        assert!(encode_v38_spill_relation_parquet(&changed).is_err());
    }

    #[test]
    fn v38_boundary_builder_projects_registered_auxiliary_memory_below_64_mib() {
        let bytes = project_v38_admission_auxiliary_bytes(1_000_000, 250_000, 123).unwrap();
        assert_eq!(bytes, 52_003_936);
        assert!(bytes <= 64 * 1_048_576);
        assert!(project_v38_admission_auxiliary_bytes(u64::MAX, 250_000, 123).is_err());
    }

    #[test]
    fn v38_boundary_build_progress_reserves_the_terminal_marker_for_publication() {
        let mut snapshots = Vec::new();
        report_v38_build_scoring_progress(983_040, 1_000_000, &mut |snapshot| {
            snapshots.push(snapshot.to_vec());
            Ok(())
        })
        .unwrap();
        report_v38_build_scoring_progress(1_000_000, 1_000_000, &mut |snapshot| {
            snapshots.push(snapshot.to_vec());
            Ok(())
        })
        .unwrap();
        assert_eq!(snapshots.len(), 1);
        let snapshot: serde_json::Value = serde_json::from_slice(&snapshots[0]).unwrap();
        assert_eq!(snapshot["completed_rows"], 983_040);
        assert_eq!(snapshot["total_rows"], 1_000_000);
        assert!(report_v38_build_scoring_progress(1_000_001, 1_000_000, &mut |_| Ok(())).is_err());
    }

    #[test]
    fn v38_boundary_builder_replays_primary_and_is_worker_deterministic() {
        let tree = three_leaf_tree([1.0, 0.0]);
        let rows = vec![
            vec![-1.0, 0.0],
            vec![-2.0, 0.0],
            vec![1.0, -1.0],
            vec![2.0, -2.0],
            vec![1.0, 1.0],
            vec![2.0, 2.0],
        ];
        let primary = vec![
            primary_record(0, 100, 0, 0),
            primary_record(1, 101, 0, 1),
            primary_record(2, 102, 1, 0),
            primary_record(3, 103, 1, 1),
            primary_record(4, 104, 2, 0),
            primary_record(5, 105, 2, 1),
        ];
        let one: V38SpillRelation =
            build_v38_spill_relation(&tree, &rows, &primary, 3, 3, 1).unwrap();
        let four = build_v38_spill_relation(&tree, &rows, &primary, 3, 3, 4).unwrap();
        assert_eq!(one, four);
        let row_slices = rows.iter().map(Vec::as_slice).collect::<Vec<_>>();
        let mut progress = Vec::new();
        let (progressive, _) = build_v38_spill_relation_slices_with_evidence_and_progress(
            &tree,
            &row_slices,
            &primary,
            V38SpillBuildLimits {
                maximum_alternates: 3,
                maximum_rows_per_posting: 3,
                workers: 4,
            },
            |completed| {
                progress.push(completed);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(progressive, one);
        assert_eq!(progress, [6]);
        assert_eq!(one.records.len(), 8);
        assert_eq!(
            one.postings
                .iter()
                .map(|value| value.total_population)
                .sum::<u64>(),
            8
        );

        let mut changed = primary.clone();
        changed[0].posting_ordinal = 1;
        assert!(build_v38_spill_relation(&tree, &rows, &changed, 3, 3, 1).is_err());
        let mut changed = rows;
        changed[0][0] = f32::NAN;
        assert!(build_v38_spill_relation(&tree, &changed, &primary, 3, 3, 1).is_err());
    }

    #[test]
    fn v38_boundary_builder_flat_scientific_path_matches_reference_without_row_refs() {
        let tree = three_leaf_tree([1.0, 0.0]);
        let rows = vec![
            vec![-1.0, 0.0],
            vec![-2.0, 0.0],
            vec![1.0, -1.0],
            vec![2.0, -2.0],
            vec![1.0, 1.0],
            vec![2.0, 2.0],
        ];
        let primary = vec![
            primary_record(0, 100, 0, 0),
            primary_record(1, 101, 0, 1),
            primary_record(2, 102, 1, 0),
            primary_record(3, 103, 1, 1),
            primary_record(4, 104, 2, 0),
            primary_record(5, 105, 2, 1),
        ];
        let expected = build_v38_spill_relation(&tree, &rows, &primary, 3, 3, 2).unwrap();
        let flat = rows.iter().flatten().copied().collect::<Vec<_>>();
        let mut progress = Vec::new();
        let (actual, _) = build_v38_spill_relation_flat_with_evidence_and_progress(
            &tree,
            &flat,
            2,
            &primary,
            V38SpillBuildLimits {
                maximum_alternates: 3,
                maximum_rows_per_posting: 3,
                workers: 2,
            },
            |completed| {
                progress.push(completed);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(actual, expected);
        assert_eq!(progress, [6]);
    }

    fn brute_force_cover(
        owners: &[(u32, Option<u32>)],
        posting_count: u32,
        selected_postings: u32,
    ) -> (u32, Vec<u32>) {
        let mut best = (0_u32, Vec::new());
        for mask in 0_u64..(1_u64 << posting_count) {
            if mask.count_ones() > selected_postings {
                continue;
            }
            let hits = owners
                .iter()
                .filter(|(primary, alternate)| {
                    mask & (1_u64 << primary) != 0
                        || alternate.is_some_and(|posting| mask & (1_u64 << posting) != 0)
                })
                .count() as u32;
            let mut selection = (0..posting_count)
                .filter(|posting| mask & (1_u64 << posting) != 0)
                .collect::<Vec<_>>();
            for posting in 0..posting_count {
                if selection.len() == selected_postings as usize {
                    break;
                }
                if !selection.contains(&posting) {
                    selection.push(posting);
                }
            }
            selection.sort_unstable();
            if hits > best.0 || hits == best.0 && (best.1.is_empty() || selection < best.1) {
                best = (hits, selection);
            }
        }
        best
    }

    fn greedy_trap() -> Vec<(u32, Option<u32>)> {
        vec![
            (0, Some(1)),
            (0, Some(1)),
            (0, Some(1)),
            (0, Some(2)),
            (0, Some(2)),
            (0, Some(2)),
            (1, None),
            (1, None),
            (2, None),
            (2, None),
        ]
    }

    #[test]
    fn v38_boundary_cover_exact_solver_beats_greedy_and_pads_selection() {
        let certificate: V38CoverageCertificate =
            solve_v38_query_coverage(&greedy_trap(), 4, 2, 250_000).unwrap();
        assert_eq!(certificate.feasible_hits, 10);
        assert_eq!(certificate.certified_upper_hits, 10);
        assert!(certificate.exact);
        assert_eq!(certificate.selected_postings, vec![1, 2]);

        let loops = vec![(2, None), (2, None), (4, None)];
        let padded = solve_v38_query_coverage(&loops, 6, 4, 250_000).unwrap();
        assert_eq!(padded.feasible_hits, 3);
        assert_eq!(padded.selected_postings.len(), 4);
        assert!(
            padded
                .selected_postings
                .windows(2)
                .all(|pair| pair[0] < pair[1])
        );
    }

    #[test]
    fn v38_boundary_cover_matches_exhaustive_fixed_seed_graphs() {
        let mut state = 0x0003_8d15_ca11_u64;
        for _ in 0..128 {
            let posting_count = 6;
            let mut owners = Vec::new();
            for _ in 0..12 {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1);
                let primary = ((state >> 32) % u64::from(posting_count)) as u32;
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1);
                let candidate = ((state >> 32) % u64::from(posting_count)) as u32;
                let alternate = (candidate != primary && state & 1 == 1).then_some(candidate);
                owners.push((primary, alternate));
            }
            let expected = brute_force_cover(&owners, posting_count, 3);
            let actual = solve_v38_query_coverage(&owners, posting_count, 3, 250_000).unwrap();
            assert_eq!((actual.feasible_hits, actual.selected_postings), expected);
        }
    }

    #[test]
    fn v38_boundary_cover_interruption_preserves_incumbent_and_frontier_upper_bound() {
        let stopped = solve_v38_query_coverage(&greedy_trap(), 4, 2, 1).unwrap();
        assert!(!stopped.exact);
        assert_eq!(stopped.feasible_hits, 8);
        assert!(stopped.certified_upper_hits >= stopped.feasible_hits);
        assert!(stopped.certified_upper_hits >= 10);
        assert_eq!(stopped.solver_visits, 1);

        let pending_sibling = solve_v38_query_coverage(&greedy_trap(), 4, 2, 3).unwrap();
        assert!(!pending_sibling.exact);
        assert_eq!(pending_sibling.feasible_hits, 8);
        assert_eq!(pending_sibling.certified_upper_hits, 10);
        assert_eq!(pending_sibling.solver_visits, 3);

        let exact = solve_v38_query_coverage(&greedy_trap(), 4, 2, 250_000).unwrap();
        assert!(exact.exact);
        assert_eq!(exact.certified_upper_hits, exact.feasible_hits);
    }

    #[test]
    fn v38_boundary_cover_total_budget_preserves_unsearched_query_bounds() {
        let queries = vec![greedy_trap(), greedy_trap()];
        let certificates = solve_v38_coverage_batch(&queries, 4, 2, 3, 3).unwrap();
        assert_eq!(certificates.len(), 2);
        assert_eq!(certificates[0].solver_visits, 3);
        assert_eq!(certificates[0].feasible_hits, 8);
        assert_eq!(certificates[0].certified_upper_hits, 10);
        assert_eq!(certificates[1].solver_visits, 0);
        assert_eq!(certificates[1].feasible_hits, 8);
        assert_eq!(certificates[1].certified_upper_hits, 10);
        assert!(!certificates[1].exact);
    }

    fn complete_population_relation(with_alternates: bool) -> V38SpillRelation {
        let mut records = Vec::new();
        for source_ordinal in 0_u64..123 {
            records.push(V38SpillRecord {
                source_ordinal,
                feature_row_id: 10_000 + source_ordinal,
                posting_ordinal: source_ordinal as u32,
                owner_role: 0,
                posting_local_ordinal: 0,
                alternate_violation_bits: None,
            });
            if with_alternates && source_ordinal < 100 {
                records.push(V38SpillRecord {
                    source_ordinal,
                    feature_row_id: 10_000 + source_ordinal,
                    posting_ordinal: 100 + source_ordinal as u32 % 14,
                    owner_role: 1,
                    posting_local_ordinal: 1 + source_ordinal as u32 / 14,
                    alternate_violation_bits: Some((source_ordinal as f32 / 100.0).to_bits()),
                });
            }
        }
        let postings = summarize_v38_spill_relation(&records, 123, 10_240).unwrap();
        V38SpillRelation { records, postings }
    }

    fn complete_population_truth() -> Vec<V37FeatureGroundTruth> {
        (0_u32..1_000)
            .map(|query_ordinal| V37FeatureGroundTruth {
                query_ordinal,
                feature_row_ids: (10_000_u64..10_100).collect(),
            })
            .collect()
    }

    fn indeterminate_first_query_relation() -> (V38SpillRelation, Vec<V37FeatureGroundTruth>) {
        let mut owners = (0_u32..123)
            .map(|posting| {
                (
                    10_000 + u64::from(posting),
                    posting,
                    (posting < 100).then_some(100 + posting % 14),
                )
            })
            .collect::<Vec<_>>();
        let mut trap = Vec::new();
        for group in 0_u32..10 {
            let base = group * 3;
            trap.extend(greedy_trap().into_iter().map(|(primary, alternate)| {
                (base + primary, alternate.map(|posting| base + posting))
            }));
        }
        owners.extend(
            trap.iter()
                .enumerate()
                .map(|(ordinal, (primary, alternate))| {
                    (20_000 + ordinal as u64, *primary, *alternate)
                }),
        );
        let mut primary_populations = [0_u32; 123];
        for (_, primary, _) in &owners {
            primary_populations[*primary as usize] += 1;
        }
        let mut next_primary = [0_u32; 123];
        let mut next_alternate = primary_populations;
        let mut records = Vec::new();
        for (source_ordinal, (feature_row_id, primary, alternate)) in owners.into_iter().enumerate()
        {
            records.push(V38SpillRecord {
                source_ordinal: source_ordinal as u64,
                feature_row_id,
                posting_ordinal: primary,
                owner_role: 0,
                posting_local_ordinal: next_primary[primary as usize],
                alternate_violation_bits: None,
            });
            next_primary[primary as usize] += 1;
            if let Some(alternate) = alternate {
                records.push(V38SpillRecord {
                    source_ordinal: source_ordinal as u64,
                    feature_row_id,
                    posting_ordinal: alternate,
                    owner_role: 1,
                    posting_local_ordinal: next_alternate[alternate as usize],
                    alternate_violation_bits: Some(0.0_f32.to_bits()),
                });
                next_alternate[alternate as usize] += 1;
            }
        }
        let postings = summarize_v38_spill_relation(&records, 123, 10_240).unwrap();
        let relation = V38SpillRelation { records, postings };
        let mut truth = complete_population_truth();
        truth[0].feature_row_ids = (20_000_u64..20_100).collect();
        (relation, truth)
    }

    #[test]
    fn v38_boundary_cover_complete_bounds_short_circuit_feasible_without_dfs() {
        let result: V38LayoutCeiling = evaluate_v38_multi_owner_ceiling(
            &ceiling_authority(),
            &complete_population_relation(true),
            &complete_population_truth(),
        )
        .unwrap();
        assert_eq!(result.disposition, V38TerminalDisposition::LayoutFeasible);
        assert_eq!(result.aggregate_feasible_ppm, 1_000_000);
        assert_eq!(result.minimum_feasible_ppm, 1_000_000);
        assert_eq!(result.total_solver_visits, 0);
        assert_eq!(result.certificates.len(), 1_000);
    }

    #[test]
    fn v38_boundary_cover_emits_bounded_deterministic_progress_and_propagates_failure() {
        let authority = ceiling_authority();
        let relation = complete_population_relation(true);
        let truth = complete_population_truth();
        let mut progress = Vec::new();
        let result = evaluate_v38_multi_owner_ceiling_with_progress(
            &authority,
            &relation,
            &truth,
            |completed| {
                progress.push(completed);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(result.disposition, V38TerminalDisposition::LayoutFeasible);
        assert_eq!(progress.first(), Some(&32));
        assert_eq!(progress.last(), Some(&1_000));
        assert_eq!(progress.len(), 32);

        let error =
            evaluate_v38_multi_owner_ceiling_with_progress(&authority, &relation, &truth, |_| {
                Err(super::invalid("registered progress failure"))
            })
            .unwrap_err()
            .to_string();
        assert!(error.contains("registered progress failure"));
    }

    #[test]
    fn v38_boundary_cover_progress_never_restarts_after_zero_visit_bounds() {
        let (relation, truth) = indeterminate_first_query_relation();
        let mut progress = Vec::new();
        let result = evaluate_v38_multi_owner_ceiling_with_progress(
            &ceiling_authority(),
            &relation,
            &truth,
            |completed| {
                progress.push(completed);
                Ok(())
            },
        )
        .unwrap();
        assert!(result.total_solver_visits > 0);
        assert_eq!(progress.last(), Some(&1_000));
        assert!(progress.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[test]
    fn v38_boundary_cover_complete_bounds_short_circuit_rejected_without_dfs() {
        let result: V38LayoutCeiling = evaluate_v38_multi_owner_ceiling(
            &ceiling_authority(),
            &complete_population_relation(false),
            &complete_population_truth(),
        )
        .unwrap();
        assert_eq!(result.disposition, V38TerminalDisposition::LayoutRejected);
        assert_eq!(result.aggregate_certified_upper_ppm, 140_000);
        assert_eq!(result.minimum_certified_upper_ppm, 140_000);
        assert_eq!(result.total_solver_visits, 0);
        assert_eq!(result.certificates.len(), 1_000);
    }

    #[test]
    fn v38_boundary_cover_canonical_replay_rejects_claimed_evidence_drift() {
        let authority = ceiling_authority();
        let relation = complete_population_relation(true);
        let truth = complete_population_truth();
        let result = evaluate_v38_multi_owner_ceiling(&authority, &relation, &truth).unwrap();
        let bytes = canonical_v38_ceiling_bytes(&result).unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));
        assert_eq!(
            validate_v38_ceiling_bytes(&authority, &relation, &truth, &bytes).unwrap(),
            bytes
        );
        let mut changed_authority = authority.clone();
        changed_authority.relation.sha256 = digest(99);
        assert!(validate_v38_ceiling_bytes(&changed_authority, &relation, &truth, &bytes).is_err());

        let mut forged = result.clone();
        forged.certificates[0].selected_postings = (0..14).collect();
        forged.certificates[0].feasible_hits = 14;
        forged.certificates[0].certified_upper_hits = 100;
        forged.certificates[0].exact = false;
        forged.aggregate_feasible_ppm = 999_140;
        forged.aggregate_certified_upper_ppm = 1_000_000;
        forged.minimum_feasible_ppm = 140_000;
        forged.minimum_certified_upper_ppm = 1_000_000;
        forged.exact_query_count = 999;
        forged.total_solver_visits = 0;
        forged.passed = false;
        forged.disposition = V38TerminalDisposition::Indeterminate;
        let forged_bytes = canonical_v38_ceiling_bytes(&forged).unwrap();
        assert!(validate_v38_ceiling_bytes(&authority, &relation, &truth, &forged_bytes).is_err());

        let mutations: [fn(&mut V38LayoutCeiling); 20] = [
            |value| value.schema.push_str("-changed"),
            |value| value.claim_eligible = true,
            |value| value.gt_neighbors -= 1,
            |value| value.selected_postings -= 1,
            |value| value.aggregate_gate_ppm -= 1,
            |value| value.minimum_gate_ppm -= 1,
            |value| value.certificates[0].query_ordinal = 1,
            |value| value.certificates[0].selected_postings.swap(0, 1),
            |value| value.certificates[0].feasible_hits -= 1,
            |value| value.certificates[0].certified_upper_hits -= 1,
            |value| value.certificates[0].exact = false,
            |value| value.certificates[0].solver_visits = 1,
            |value| value.aggregate_feasible_ppm -= 1,
            |value| value.aggregate_certified_upper_ppm -= 1,
            |value| value.minimum_feasible_ppm -= 1,
            |value| value.minimum_certified_upper_ppm -= 1,
            |value| value.exact_query_count -= 1,
            |value| value.total_solver_visits = 1,
            |value| value.passed = false,
            |value| value.disposition = V38TerminalDisposition::Indeterminate,
        ];
        for mutation in mutations {
            let mut changed = result.clone();
            mutation(&mut changed);
            let claimed = canonical_bytes(&changed, "mutated ceiling").unwrap();
            assert!(validate_v38_ceiling_bytes(&authority, &relation, &truth, &claimed).is_err());
        }
        assert!(
            validate_v38_ceiling_bytes(&authority, &relation, &truth, &bytes[..bytes.len() - 1])
                .is_err()
        );
    }
}
