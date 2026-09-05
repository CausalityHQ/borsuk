use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{BorsukError, Result};

const FORMAT: &str = "borsuk-v35-generation-v1";
const HARD_LIMIT_BYTES: u64 = 3_221_225_472;
const TREE_CAP_BYTES: u64 = 32 * 1_048_576;
const LIVENESS_PLANE_BYTES: u64 = 25_000_000;
const DIRECTORY_BYTES: u64 = 16_777_216;
const OBJECT_DATA_CACHE_BYTES: u64 = 159_549_376;
const SHARED_CACHE_BYTES: u64 = 201_326_592;
const DELTA_BYTES: u64 = 64 * 1_048_576;
const DELTA_MUTATION_DIRECTORY_BYTES: u64 = 32_000_000;
const DELTA_POSTING_REFERENCE_BYTES: u64 = 16_000_000;
const DELTA_LEAF_BYTES: u64 = 6_890_624;
const DELTA_TREE_BYTES: u64 = 223_680;
const DELTA_RUN_OVERHEAD_BYTES: u64 = 4_194_304;
const DELTA_RESERVED_BYTES: u64 = 7_800_256;
const RUNTIME_BYTES: u64 = 256 * 1_048_576;
const QUERY_WORKSPACE_BYTES: u64 = 512 * 1_048_576;
const HEADROOM_BYTES: u64 = 256 * 1_048_576;
const PROJECTED_ROWS: u64 = 100_000_000;

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Authenticated source and routing dimensions for one V35 generation.
pub struct V35Dimensions {
    /// Resident routing dimension.
    pub routing: u16,
    /// Original vector dimension retained in remote S3 objects.
    pub source: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
/// Query-independent projection arm.
pub enum V35ProjectionArm {
    /// Learned deterministic corpus-only PCA basis.
    Pca,
    /// Query-independent structured random projection control.
    Srht,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Complete immutable identity for one V35 object.
pub struct V35ArtifactIdentity {
    /// Lower-case hexadecimal complete-object digest.
    pub digest: String,
    /// Digest algorithm; V35 generation inputs use SHA-256.
    pub digest_algorithm: String,
    /// Complete object length.
    pub length: u64,
    /// Unique semantic object role.
    pub role: String,
    /// Immutable object URI.
    pub uri: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Remote residual scalar-quantization rate.
pub struct V35RemoteCodeRate {
    /// Bits stored per source dimension: four or eight.
    pub bits_per_dimension: u8,
    /// Packed code bytes per row, excluding object envelopes.
    pub bytes_per_row: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Strict V35 generation manifest authenticated before allocation.
pub struct V35GenerationManifest {
    /// Complete role-separated immutable inputs.
    pub artifacts: Vec<V35ArtifactIdentity>,
    /// Source and resident routing dimensions.
    pub dimensions: V35Dimensions,
    /// Breaking persistent format marker.
    pub format: String,
    /// Number of resident routing leaves per generation.
    pub leaf_count: u64,
    /// Exact source-space metric.
    pub metric: String,
    /// Exact source-vector normalization policy.
    pub normalization: String,
    /// One or two patches per leaf.
    pub patches_per_leaf: u8,
    /// Corpus-only projection arm.
    pub projection: V35ProjectionArm,
    /// Remote scalar-quantization rate.
    pub remote_code: V35RemoteCodeRate,
    /// SHA-256 of the immutable source archive.
    pub source_archive_sha256: String,
    /// Immutable source identity.
    pub source_id: String,
    /// Resident routing nodes per generation.
    pub tree_node_count: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Checked resident-memory admission and remote-size projection.
pub struct V35ServingProjection {
    /// Numeric bytes stored for one leaf.
    pub bytes_per_leaf: u64,
    /// Leaf bytes for active and retiring generations.
    pub active_and_retiring_leaf_bytes: u64,
    /// Fixed tree admission for active and retiring generations.
    pub active_and_retiring_tree_bytes: u64,
    /// Projection-basis bytes for active and retiring generations.
    pub active_and_retiring_basis_bytes: u64,
    /// Snapshot liveness allocation.
    pub liveness_plane_bytes: u64,
    /// Immutable directory allocation.
    pub directory_bytes: u64,
    /// Selective code/page object-data cache allocation.
    pub object_data_cache_bytes: u64,
    /// Sum of liveness, directory, and object-data cache allocations.
    pub shared_cache_bytes: u64,
    /// Bounded online-delta allocation.
    pub delta_bytes: u64,
    /// Complete latest-sequence mutation directory.
    pub delta_mutation_directory_bytes: u64,
    /// Delta posting and page-reference state.
    pub delta_posting_reference_bytes: u64,
    /// Resident one-patch delta leaves.
    pub delta_leaf_bytes: u64,
    /// Resident delta routing tree.
    pub delta_tree_bytes: u64,
    /// Four immutable-run descriptors and construction overhead.
    pub delta_run_overhead_bytes: u64,
    /// Unborrowable delta safety reservation.
    pub delta_reserved_bytes: u64,
    /// Runtime, allocator, and thread-stack allocation.
    pub runtime_bytes: u64,
    /// Sixteen bounded query workspaces.
    pub query_workspace_bytes: u64,
    /// Deliberately unallocated process headroom.
    pub unallocated_headroom_bytes: u64,
    /// Complete checked resident admission.
    pub admission_budget_bytes: u64,
    /// Strict three-GiB process limit.
    pub hard_limit_bytes: u64,
    /// SQ4 payload for 100M rows; remote S3 bytes, never resident admission.
    pub remote_sq4_payload_bytes: u64,
}

fn is_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn expected_remote_bytes_per_row(dimensions: u32, bits: u8) -> Option<u32> {
    match bits {
        4 => dimensions.checked_add(1).map(|value| value / 2),
        8 => Some(dimensions),
        _ => None,
    }
}

fn validate_identity(identity: &V35ArtifactIdentity) -> Result<()> {
    if identity.digest_algorithm != "sha256"
        || !is_digest(&identity.digest)
        || identity.length == 0
        || identity.role.is_empty()
        || identity.uri.is_empty()
    {
        return Err(invalid("V35 artifact identity differs"));
    }
    Ok(())
}

fn validate_manifest_fields(manifest: &V35GenerationManifest) -> Result<()> {
    if manifest.format != FORMAT
        || manifest.source_id.is_empty()
        || !is_digest(&manifest.source_archive_sha256)
        || manifest.metric != "squared-l2"
        || manifest.normalization != "none"
        || manifest.dimensions.source == 0
        || !matches!(manifest.dimensions.routing, 64 | 128 | 192)
        || u32::from(manifest.dimensions.routing) > manifest.dimensions.source
        || !matches!(manifest.patches_per_leaf, 1 | 2)
        || manifest.leaf_count == 0
        || manifest.tree_node_count == 0
        || expected_remote_bytes_per_row(
            manifest.dimensions.source,
            manifest.remote_code.bits_per_dimension,
        ) != Some(manifest.remote_code.bytes_per_row)
    {
        return Err(invalid("V35 generation manifest authority differs"));
    }

    let required_roles = BTreeSet::from([
        "active-liveness",
        "active-projection-basis",
        "active-routing",
        "code-directory",
        "page-directory",
        "retiring-liveness",
        "retiring-projection-basis",
        "retiring-routing",
    ]);
    let mut roles = BTreeSet::new();
    let mut uris = BTreeSet::new();
    for identity in &manifest.artifacts {
        validate_identity(identity)?;
        if !roles.insert(identity.role.as_str()) || !uris.insert(identity.uri.as_str()) {
            return Err(invalid("V35 artifact roles and URIs must be unique"));
        }
    }
    if roles != required_roles {
        return Err(invalid("V35 artifact role set differs"));
    }
    Ok(())
}

fn canonical_json_value(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(canonical_json_value).collect())
        }
        serde_json::Value::Object(values) => serde_json::Value::Object(
            values
                .into_iter()
                .map(|(key, value)| (key, canonical_json_value(value)))
                .collect::<BTreeMap<_, _>>()
                .into_iter()
                .collect(),
        ),
        scalar => scalar,
    }
}

/// Authenticate and decode one canonical newline V35 generation manifest.
pub fn validate_v35_manifest(
    bytes: &[u8],
    registered: &V35ArtifactIdentity,
) -> Result<V35GenerationManifest> {
    validate_identity(registered)?;
    if registered.role != "generation-manifest"
        || registered.length != u64::try_from(bytes.len()).unwrap_or(u64::MAX)
        || format!("{:x}", Sha256::digest(bytes)) != registered.digest
        || !bytes.ends_with(b"\n")
        || bytes.ends_with(b"\n\n")
    {
        return Err(invalid("V35 registered manifest bytes differ"));
    }
    let manifest: V35GenerationManifest = serde_json::from_slice(bytes)
        .map_err(|_| invalid("V35 generation manifest JSON differs"))?;
    let value = serde_json::to_value(&manifest)
        .map_err(|_| invalid("V35 generation manifest cannot be canonicalized"))?;
    let mut canonical = serde_json::to_vec(&canonical_json_value(value))
        .map_err(|_| invalid("V35 generation manifest cannot be serialized"))?;
    canonical.push(b'\n');
    if canonical != bytes {
        return Err(invalid("V35 generation manifest is not canonical"));
    }
    validate_manifest_fields(&manifest)?;
    Ok(manifest)
}

/// Project resident serving memory separately from the remote 100M-row payload.
pub fn project_v35_serving_memory(
    manifest: &V35GenerationManifest,
) -> Result<V35ServingProjection> {
    validate_manifest_fields(manifest)?;
    let checked_delta_bytes = [
        DELTA_MUTATION_DIRECTORY_BYTES,
        DELTA_POSTING_REFERENCE_BYTES,
        DELTA_LEAF_BYTES,
        DELTA_TREE_BYTES,
        DELTA_RUN_OVERHEAD_BYTES,
        DELTA_RESERVED_BYTES,
    ]
    .into_iter()
    .try_fold(0_u64, |sum, value| sum.checked_add(value))
    .ok_or_else(|| invalid("V35 delta projection overflows"))?;
    if checked_delta_bytes != DELTA_BYTES {
        return Err(invalid("V35 delta projection authority differs"));
    }
    let routing = u64::from(manifest.dimensions.routing);
    let patches = u64::from(manifest.patches_per_leaf);
    let bytes_per_leaf = routing
        .checked_mul(8)
        .and_then(|value| value.checked_add(128))
        .and_then(|value| value.checked_mul(patches))
        .ok_or_else(|| invalid("V35 leaf projection overflows"))?;
    let active_and_retiring_leaf_bytes = manifest
        .leaf_count
        .checked_mul(bytes_per_leaf)
        .and_then(|value| value.checked_mul(2))
        .ok_or_else(|| invalid("V35 generation leaf projection overflows"))?;
    let node_bytes = routing
        .checked_add(128)
        .and_then(|width| manifest.tree_node_count.checked_mul(width))
        .ok_or_else(|| invalid("V35 tree projection overflows"))?;
    if node_bytes > TREE_CAP_BYTES {
        return Err(invalid("V35 tree projection exceeds per-generation cap"));
    }
    let active_and_retiring_tree_bytes = TREE_CAP_BYTES
        .checked_mul(2)
        .ok_or_else(|| invalid("V35 tree admission overflows"))?;
    let active_and_retiring_basis_bytes = u64::from(manifest.dimensions.source)
        .checked_mul(routing)
        .and_then(|value| value.checked_mul(4))
        .and_then(|value| value.checked_mul(2))
        .ok_or_else(|| invalid("V35 projection-basis admission overflows"))?;
    let admission_budget_bytes = [
        active_and_retiring_leaf_bytes,
        active_and_retiring_tree_bytes,
        active_and_retiring_basis_bytes,
        SHARED_CACHE_BYTES,
        DELTA_BYTES,
        RUNTIME_BYTES,
        QUERY_WORKSPACE_BYTES,
        HEADROOM_BYTES,
    ]
    .into_iter()
    .try_fold(0_u64, |sum, value| sum.checked_add(value))
    .ok_or_else(|| invalid("V35 serving admission overflows"))?;
    if admission_budget_bytes >= HARD_LIMIT_BYTES {
        return Err(invalid("V35 serving admission reaches three GiB"));
    }
    let remote_sq4_payload_bytes = PROJECTED_ROWS
        .checked_mul(u64::from(manifest.dimensions.source).div_ceil(2))
        .ok_or_else(|| invalid("V35 remote payload projection overflows"))?;

    Ok(V35ServingProjection {
        bytes_per_leaf,
        active_and_retiring_leaf_bytes,
        active_and_retiring_tree_bytes,
        active_and_retiring_basis_bytes,
        liveness_plane_bytes: LIVENESS_PLANE_BYTES,
        directory_bytes: DIRECTORY_BYTES,
        object_data_cache_bytes: OBJECT_DATA_CACHE_BYTES,
        shared_cache_bytes: SHARED_CACHE_BYTES,
        delta_bytes: DELTA_BYTES,
        delta_mutation_directory_bytes: DELTA_MUTATION_DIRECTORY_BYTES,
        delta_posting_reference_bytes: DELTA_POSTING_REFERENCE_BYTES,
        delta_leaf_bytes: DELTA_LEAF_BYTES,
        delta_tree_bytes: DELTA_TREE_BYTES,
        delta_run_overhead_bytes: DELTA_RUN_OVERHEAD_BYTES,
        delta_reserved_bytes: DELTA_RESERVED_BYTES,
        runtime_bytes: RUNTIME_BYTES,
        query_workspace_bytes: QUERY_WORKSPACE_BYTES,
        unallocated_headroom_bytes: HEADROOM_BYTES,
        admission_budget_bytes,
        hard_limit_bytes: HARD_LIMIT_BYTES,
        remote_sq4_payload_bytes,
    })
}
