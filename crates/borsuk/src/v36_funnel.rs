use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{BorsukError, Result};

const FORMAT: &str = "borsuk-v36-funnel-manifest-v1";
const PROJECTION_DIMENSIONS: u16 = 192;
const PROJECTION_SEED: u64 = 36;
const COARSE_FRAGMENT_LIMIT_BYTES: u64 = 512 * 1_024;
const NORMAL_GET_LIMIT: u16 = 14;
const NORMAL_BYTE_LIMIT: u64 = 7 * 1_048_576;
const HARD_GET_LIMIT: u16 = 16;
const HARD_BYTE_LIMIT: u64 = 8 * 1_048_576;
const MIB: u64 = 1_048_576;
const POSTING_SUMMARY_SLOT_BYTES: u64 = 4_736;
const OBJECT_DIRECTORY_ENTRY_BYTES: u64 = 80;
const POSTING_DIRECTORY_ENTRY_BYTES: u64 = 16;
const FINE_INTERVAL_ENTRY_BYTES: u64 = 16;
const QUERY_WORKSPACE_BYTES: u64 = 256 * MIB;
const RUNTIME_BYTES: u64 = 512 * MIB;

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        && value.bytes().any(|byte| byte != b'0')
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
/// Primary-row target of one V36 geometry arm.
pub enum V36PrimaryRows {
    /// Historical 256-primary-row control.
    Control256,
    /// Candidate 4,096-primary-row posting.
    Posting4096,
    /// Candidate 8,192-primary-row posting.
    Posting8192,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
/// Query-independent V36 row replication policy.
pub enum V36Replication {
    /// One owner.
    Single,
    /// Historical fixed double-assignment control.
    DoubleControl,
    /// Closure replication with epsilon 0.05.
    ClosureEpsilon05,
    /// Closure replication with epsilon 0.15.
    ClosureEpsilon15,
    /// Closure replication with epsilon 0.30.
    ClosureEpsilon30,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// One closed V36 posting geometry arm.
pub struct V36GeometryArm {
    /// Target primary rows.
    pub primary_rows: V36PrimaryRows,
    /// Replication policy.
    pub replication: V36Replication,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
/// V36 posting-ranking score.
pub enum V36ShapeScore {
    /// Centroid squared-L2 control.
    Centroid,
    /// Diagonal Gaussian lower-tail heuristic.
    Diagonal,
    /// Rank-two Gaussian lower-tail heuristic.
    Rank2,
    /// Rank-four Gaussian lower-tail heuristic.
    Rank4,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
/// Coarse-row scoring representation.
pub enum V36CoarseCode {
    /// Offline projected-f32 causal control.
    ProjectedF32Diagnostic,
    /// Twenty-four sign bytes plus norm and two u64 identities.
    Sign24,
    /// Thirty-two PQ4 code bytes plus two u64 identities.
    ResidualPq4Code32,
    /// Forty-eight PQ4 code bytes plus two u64 identities.
    ResidualPq4Code48,
}

impl V36CoarseCode {
    /// Raw bytes per immutable base assignment, excluding Arrow envelopes.
    pub const fn record_bytes(self) -> u64 {
        match self {
            Self::ProjectedF32Diagnostic => 192 * 4 + 16,
            Self::Sign24 => 24 + 4 + 16,
            Self::ResidualPq4Code32 => 32 + 16,
            Self::ResidualPq4Code48 => 48 + 16,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
/// Fine-row representation.
pub enum V36FineCodec {
    /// Per-dimension scalar int8.
    Sq8,
    /// IEEE binary16.
    F16,
    /// Offline source-f32 causal control.
    SourceF32Control,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
/// Maximum complete encoded fine Arrow object size.
pub enum V36ChunkCeiling {
    /// 64 KiB.
    Kib64,
    /// 256 KiB.
    Kib256,
    /// 512 KiB.
    Kib512,
}

impl V36ChunkCeiling {
    const fn encoded_bytes(self) -> u64 {
        match self {
            Self::Kib64 => 64 * 1_024,
            Self::Kib256 => 256 * 1_024,
            Self::Kib512 => 512 * 1_024,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Complete immutable V36 object identity.
pub struct V36ArtifactIdentity {
    /// Complete-object BLAKE3.
    pub blake3: String,
    /// Complete encoded length.
    pub encoded_bytes: u64,
    /// Unique semantic role.
    pub role: String,
    /// Complete-object SHA-256.
    pub sha256: String,
    /// Immutable URI.
    pub uri: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Registered complete-byte identity of a V36 manifest.
pub struct V36RegisteredManifest {
    /// Complete BLAKE3 of an independently fetched manifest.
    pub blake3: String,
    /// Complete encoded length.
    pub encoded_bytes: u64,
    /// Complete SHA-256.
    pub sha256: String,
    /// Immutable URI.
    pub uri: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Strict, breaking V36 qualification manifest.
pub struct V36FunnelManifest {
    /// Complete role-separated artifacts.
    pub artifacts: Vec<V36ArtifactIdentity>,
    /// Fine object ceiling.
    pub chunk_ceiling: V36ChunkCeiling,
    /// Qualification manifests never make publication claims.
    pub claim_eligible: bool,
    /// Coarse representation.
    pub coarse_code: V36CoarseCode,
    /// Fine representation.
    pub fine_codec: V36FineCodec,
    /// Exact format marker.
    pub format: String,
    /// Posting geometry.
    pub geometry: V36GeometryArm,
    /// Exact metric.
    pub metric: String,
    /// Resident projection dimensions.
    pub projection_dimensions: u16,
    /// Structured projection seed.
    pub projection_seed: u64,
    /// Posting score.
    pub shape_score: V36ShapeScore,
    /// Source dimensions.
    pub source_dimensions: u16,
    /// Bounded unique source-ID heap size.
    pub unique_k: u16,
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

fn valid_geometry(manifest: &V36FunnelManifest) -> bool {
    match (
        manifest.geometry.primary_rows,
        manifest.geometry.replication,
    ) {
        (V36PrimaryRows::Control256, V36Replication::Single | V36Replication::DoubleControl) => {
            manifest.shape_score == V36ShapeScore::Centroid
        }
        (
            V36PrimaryRows::Posting4096 | V36PrimaryRows::Posting8192,
            V36Replication::Single
            | V36Replication::ClosureEpsilon05
            | V36Replication::ClosureEpsilon15
            | V36Replication::ClosureEpsilon30,
        ) => true,
        _ => false,
    }
}

fn validate_manifest_fields(manifest: &V36FunnelManifest) -> Result<()> {
    if manifest.format != FORMAT
        || manifest.claim_eligible
        || manifest.metric != "squared-l2"
        || manifest.projection_dimensions != PROJECTION_DIMENSIONS
        || manifest.projection_seed != PROJECTION_SEED
        || !matches!(manifest.source_dimensions, 384 | 768 | 1_536 | 3_072)
        || !matches!(manifest.unique_k, 512 | 1_024 | 1_536 | 2_048)
        || !valid_geometry(manifest)
    {
        return Err(invalid("V36 manifest authority differs"));
    }

    let mut required_roles = BTreeSet::from([
        "coarse-directory",
        "dataset-authority",
        "fine-directory",
        "posting-directory",
        "posting-summaries",
        "projection",
        "source-parquet",
        "super-centroids",
        "visibility-directory",
    ]);
    if matches!(
        manifest.coarse_code,
        V36CoarseCode::ResidualPq4Code32 | V36CoarseCode::ResidualPq4Code48
    ) {
        required_roles.insert("coarse-codebook");
    }
    let mut roles = BTreeSet::new();
    let mut uris = BTreeSet::new();
    for artifact in &manifest.artifacts {
        if !valid_digest(&artifact.sha256)
            || !valid_digest(&artifact.blake3)
            || artifact.encoded_bytes == 0
            || artifact.role.is_empty()
            || artifact.uri.is_empty()
            || !roles.insert(artifact.role.as_str())
            || !uris.insert(artifact.uri.as_str())
        {
            return Err(invalid("V36 artifact identity differs"));
        }
    }
    if roles != required_roles {
        return Err(invalid("V36 artifact role set differs"));
    }
    Ok(())
}

/// Authenticate, strictly decode, and validate one canonical V36 manifest.
pub fn validate_v36_manifest(
    bytes: &[u8],
    registered: &V36RegisteredManifest,
) -> Result<V36FunnelManifest> {
    if registered.uri.is_empty()
        || !valid_digest(&registered.sha256)
        || !valid_digest(&registered.blake3)
        || registered.encoded_bytes != u64::try_from(bytes.len()).unwrap_or(u64::MAX)
        || format!("{:x}", Sha256::digest(bytes)) != registered.sha256
        || blake3::hash(bytes).to_hex().as_str() != registered.blake3
        || !bytes.ends_with(b"\n")
        || bytes.ends_with(b"\n\n")
    {
        return Err(invalid("V36 registered manifest bytes differ"));
    }
    let manifest: V36FunnelManifest =
        serde_json::from_slice(bytes).map_err(|_| invalid("V36 manifest JSON differs"))?;
    let mut canonical = serde_json::to_vec(&canonical_json_value(
        serde_json::to_value(&manifest)
            .map_err(|_| invalid("V36 manifest cannot be canonicalized"))?,
    ))
    .map_err(|_| invalid("V36 manifest cannot be serialized"))?;
    canonical.push(b'\n');
    if canonical != bytes {
        return Err(invalid("V36 manifest is not canonical"));
    }
    validate_manifest_fields(&manifest)?;
    Ok(manifest)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Normal and retry-inclusive hard transport limits for one wave.
pub struct V36TransportLimits {
    /// Normal complete-object GET cap.
    pub normal_gets: u16,
    /// Normal returned-byte cap.
    pub normal_bytes: u64,
    /// Retry-inclusive GET cap.
    pub hard_gets: u16,
    /// Retry-inclusive returned-byte cap.
    pub hard_bytes: u64,
}

impl V36TransportLimits {
    /// Frozen V36 qualification limits.
    pub const fn qualification() -> Self {
        Self {
            normal_gets: NORMAL_GET_LIMIT,
            normal_bytes: NORMAL_BYTE_LIMIT,
            hard_gets: HARD_GET_LIMIT,
            hard_bytes: HARD_BYTE_LIMIT,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// One complete authenticated coarse Arrow fragment.
pub struct V36TransportFragment {
    /// Complete-object BLAKE3.
    pub blake3: String,
    /// Peak decoded allocation.
    pub decoded_capacity_bytes: u64,
    /// Complete encoded response body.
    pub encoded_bytes: u64,
    /// First dense assignment ordinal in this fragment.
    pub first_dense_ordinal: u64,
    /// Dense fragment ordinal within its posting.
    pub fragment_ordinal: u32,
    /// Response metadata returned on every attempt.
    pub response_metadata_bytes_per_attempt: u64,
    /// Complete-body retry count observed for this object.
    pub retries: u8,
    /// Dense assignment rows in this fragment.
    pub row_count: u32,
    /// Complete-object SHA-256.
    pub sha256: String,
    /// Immutable object URI.
    pub uri: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Complete fragment list for one posting.
pub struct V36TransportPosting {
    /// Increasing complete fragments.
    pub fragments: Vec<V36TransportFragment>,
    /// Dense posting ordinal.
    pub posting_ordinal: u32,
    /// Complete stored-assignment rows across all fragments.
    pub stored_assignment_rows: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Retry-inclusive classification of one observed V36 wave.
pub enum V36TransportDisposition {
    /// The observed wave remained within the hard reserve.
    Determinate,
    /// The observed wave exceeded the hard reserve and cannot be scored.
    Indeterminate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Deterministic atomic-posting transport plan.
pub struct V36TransportPlan {
    /// Posting ordinals admitted atomically.
    pub admitted_postings: Vec<u32>,
    /// Peak decoded allocation for admitted normal objects.
    pub decoded_capacity_bytes: u64,
    /// Whether the observed retry-inclusive wave is scoreable.
    pub disposition: V36TransportDisposition,
    /// Posting ordinals excluded by normal limits.
    pub excluded_postings: Vec<u32>,
    /// Retry-inclusive response-body bytes.
    pub hard_returned_bytes_with_retries: u64,
    /// Retry-inclusive GET count.
    pub hard_gets_with_retries: u16,
    /// Normal response-body bytes.
    pub normal_encoded_bytes: u64,
    /// Normal GET count.
    pub normal_gets: u16,
}

/// Plan one V36 object wave without partially admitting a posting.
pub fn plan_v36_transport(
    ranked_postings: &[u32],
    directory: &[V36TransportPosting],
    limits: V36TransportLimits,
) -> Result<V36TransportPlan> {
    if limits != V36TransportLimits::qualification() {
        return Err(invalid("V36 transport limits differ"));
    }

    let mut postings = BTreeMap::new();
    let mut object_uris = BTreeSet::new();
    for posting in directory {
        if posting.fragments.is_empty()
            || posting.stored_assignment_rows == 0
            || postings.insert(posting.posting_ordinal, posting).is_some()
        {
            return Err(invalid("V36 posting directory differs"));
        }
        let mut next_dense_ordinal = posting.fragments[0].first_dense_ordinal;
        let mut stored_assignment_rows = 0_u64;
        for (expected, fragment) in posting.fragments.iter().enumerate() {
            if fragment.fragment_ordinal != u32::try_from(expected).unwrap_or(u32::MAX)
                || fragment.first_dense_ordinal != next_dense_ordinal
                || fragment.encoded_bytes == 0
                || fragment.encoded_bytes > COARSE_FRAGMENT_LIMIT_BYTES
                || fragment.decoded_capacity_bytes == 0
                || fragment.row_count == 0
                || !valid_digest(&fragment.sha256)
                || !valid_digest(&fragment.blake3)
                || fragment.uri.is_empty()
                || !object_uris.insert(fragment.uri.as_str())
            {
                return Err(invalid("V36 transport fragment differs"));
            }
            next_dense_ordinal = next_dense_ordinal
                .checked_add(u64::from(fragment.row_count))
                .ok_or_else(|| invalid("V36 fragment row range overflows"))?;
            stored_assignment_rows = stored_assignment_rows
                .checked_add(u64::from(fragment.row_count))
                .ok_or_else(|| invalid("V36 posting row count overflows"))?;
        }
        if stored_assignment_rows != posting.stored_assignment_rows {
            return Err(invalid("V36 posting completeness differs"));
        }
    }

    let mut ranked_unique = BTreeSet::new();
    if ranked_postings
        .iter()
        .any(|ordinal| !ranked_unique.insert(*ordinal) || !postings.contains_key(ordinal))
    {
        return Err(invalid("V36 ranked posting authority differs"));
    }

    let mut plan = V36TransportPlan {
        admitted_postings: Vec::new(),
        decoded_capacity_bytes: 0,
        disposition: V36TransportDisposition::Determinate,
        excluded_postings: Vec::new(),
        hard_returned_bytes_with_retries: 0,
        hard_gets_with_retries: 0,
        normal_encoded_bytes: 0,
        normal_gets: 0,
    };
    for posting_ordinal in ranked_postings {
        let posting = postings
            .get(posting_ordinal)
            .ok_or_else(|| invalid("V36 ranked posting authority differs"))?;
        let posting_gets = u16::try_from(posting.fragments.len())
            .map_err(|_| invalid("V36 normal GET count overflows"))?;
        let posting_bytes = posting.fragments.iter().try_fold(0_u64, |sum, fragment| {
            sum.checked_add(fragment.encoded_bytes)
        });
        let posting_bytes = posting_bytes.ok_or_else(|| invalid("V36 normal bytes overflow"))?;
        let next_gets = plan
            .normal_gets
            .checked_add(posting_gets)
            .ok_or_else(|| invalid("V36 normal GET count overflows"))?;
        let next_bytes = plan
            .normal_encoded_bytes
            .checked_add(posting_bytes)
            .ok_or_else(|| invalid("V36 normal bytes overflow"))?;
        if next_gets > limits.normal_gets || next_bytes > limits.normal_bytes {
            plan.excluded_postings.push(*posting_ordinal);
            continue;
        }

        let mut hard_gets = plan.hard_gets_with_retries;
        let mut hard_bytes = plan.hard_returned_bytes_with_retries;
        let mut decoded = plan.decoded_capacity_bytes;
        for fragment in &posting.fragments {
            let attempts = u16::from(fragment.retries)
                .checked_add(1)
                .ok_or_else(|| invalid("V36 retry count overflows"))?;
            hard_gets = hard_gets
                .checked_add(attempts)
                .ok_or_else(|| invalid("V36 hard GET count overflows"))?;
            let returned_per_attempt = fragment
                .encoded_bytes
                .checked_add(fragment.response_metadata_bytes_per_attempt)
                .ok_or_else(|| invalid("V36 hard bytes overflow"))?;
            hard_bytes = hard_bytes
                .checked_add(
                    returned_per_attempt
                        .checked_mul(u64::from(attempts))
                        .ok_or_else(|| invalid("V36 hard bytes overflow"))?,
                )
                .ok_or_else(|| invalid("V36 hard bytes overflow"))?;
            decoded = decoded
                .checked_add(fragment.decoded_capacity_bytes)
                .ok_or_else(|| invalid("V36 decoder capacity overflows"))?;
        }
        if hard_gets > limits.hard_gets || hard_bytes > limits.hard_bytes {
            plan.admitted_postings.push(*posting_ordinal);
            plan.normal_gets = next_gets;
            plan.normal_encoded_bytes = next_bytes;
            plan.hard_gets_with_retries = hard_gets;
            plan.hard_returned_bytes_with_retries = hard_bytes;
            plan.decoded_capacity_bytes = decoded;
            plan.disposition = V36TransportDisposition::Indeterminate;
            return Ok(plan);
        }
        plan.admitted_postings.push(*posting_ordinal);
        plan.normal_gets = next_gets;
        plan.normal_encoded_bytes = next_bytes;
        plan.hard_gets_with_retries = hard_gets;
        plan.hard_returned_bytes_with_retries = hard_bytes;
        plan.decoded_capacity_bytes = decoded;
    }
    Ok(plan)
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Explicit resident allocations and remote-row scale for one V36 arm.
pub struct V36ResourceRequest {
    /// Immutable active-generation allocations not itemized elsewhere.
    pub active_generation_bytes: u64,
    /// New arena retained during compaction.
    pub compaction_new_arena_bytes: u64,
    /// Old arena retained during compaction.
    pub compaction_old_arena_bytes: u64,
    /// Shared decoded-object cache capacity.
    pub decoded_cache_bytes: u64,
    /// Recent encoded coarse rows.
    pub delta_coarse_bytes: u64,
    /// Recent coarse posting CSR.
    pub delta_csr_bytes: u64,
    /// Simultaneously live encoded and decoded object buffers.
    pub encoded_decoded_overlap_bytes: u64,
    /// Fine ordinal-interval directory.
    pub fine_interval_directory_bytes: u64,
    /// Optional resident posting-summary HNSW.
    pub hnsw_bytes: u64,
    /// Latest-wins liveness state.
    pub liveness_bytes: u64,
    /// Stored coarse assignments per million primary rows.
    pub mean_replication_ppm: u32,
    /// Posting and immutable-object directories.
    pub posting_object_directory_bytes: u64,
    /// Recent fine-row arena.
    pub recent_fine_bytes: u64,
    /// Immutable retiring-generation allocations not itemized elsewhere.
    pub retiring_generation_bytes: u64,
    /// Complete-body retry buffers.
    pub retry_buffer_bytes: u64,
    /// Exact nested qualification row count.
    pub rows: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Checked V36 resident admission and remote payload projection.
pub struct V36ResourceLedger {
    /// Whether observed mean replication passes the construction gate.
    pub construction_replication_pass: bool,
    /// Active-generation allocation.
    pub active_generation_bytes: u64,
    /// New compaction arena.
    pub compaction_new_arena_bytes: u64,
    /// Old compaction arena.
    pub compaction_old_arena_bytes: u64,
    /// Decoded-object cache.
    pub decoded_cache_bytes: u64,
    /// Combined recent coarse-code and CSR allocation.
    pub delta_coarse_and_csr_bytes: u64,
    /// Encoded-plus-decoded overlap.
    pub encoded_decoded_overlap_bytes: u64,
    /// Combined posting/object and fine-interval directories.
    pub directory_bytes: u64,
    /// Optional posting-summary HNSW.
    pub hnsw_bytes: u64,
    /// Latest-wins liveness state.
    pub liveness_bytes: u64,
    /// Lower-bound coarse fragments before Arrow envelopes.
    pub minimum_coarse_fragment_count: u64,
    /// Lower-bound fine chunks before Arrow envelopes.
    pub minimum_fine_chunk_count: u64,
    /// Lower-bound resident fine interval directory.
    pub minimum_fine_interval_directory_bytes: u64,
    /// Lower-bound resident posting and object directory.
    pub minimum_posting_object_directory_bytes: u64,
    /// Exact primary posting count.
    pub posting_count: u64,
    /// Equal-slot resident posting summaries.
    pub posting_summary_bytes: u64,
    /// Source-to-M192 projection matrix.
    pub projection_bytes: u64,
    /// Sixteen fixed 16-MiB query workspaces.
    pub query_workspace_bytes: u64,
    /// Recent fine-row arena.
    pub recent_fine_bytes: u64,
    /// Complete checked process admission.
    pub resident_admission_bytes: u64,
    /// Scale-specific strict RSS limit.
    pub resident_limit_bytes: u64,
    /// Retiring-generation allocation.
    pub retiring_generation_bytes: u64,
    /// Raw remote coarse payload before Arrow envelopes.
    pub remote_coarse_payload_bytes: u64,
    /// Raw cap-eight coarse payload before Arrow envelopes.
    pub remote_coarse_cap_eight_bytes: u64,
    /// Raw shared remote fine payload before Arrow envelopes.
    pub remote_fine_payload_bytes: u64,
    /// Complete-body retry buffers.
    pub retry_buffer_bytes: u64,
    /// Runtime, allocator, stack, and unallocated headroom.
    pub runtime_bytes: u64,
    /// Exact construction super-cell count.
    pub super_cell_count: u64,
    /// Registered construction super centroids.
    pub super_centroid_bytes: u64,
}

fn checked_sum(values: &[u64], message: &str) -> Result<u64> {
    values.iter().try_fold(0_u64, |sum, value| {
        sum.checked_add(*value).ok_or_else(|| invalid(message))
    })
}

fn scale_authority(rows: u64) -> Result<(u64, u64)> {
    match rows {
        1_000_000 => Ok((4, 2_048 * MIB)),
        10_000_000 => Ok((64, 2_048 * MIB)),
        100_000_000 => Ok((512, 3_072 * MIB)),
        _ => Err(invalid("V36 resource scale differs")),
    }
}

/// Project every resident allocation separately from immutable S3 payloads.
pub fn project_v36_resources(
    manifest: &V36FunnelManifest,
    request: &V36ResourceRequest,
) -> Result<V36ResourceLedger> {
    validate_manifest_fields(manifest)?;
    let (super_cell_count, resident_limit_bytes) = scale_authority(request.rows)?;
    if !(1_000_000..=8_000_000).contains(&request.mean_replication_ppm) {
        return Err(invalid("V36 replication projection differs"));
    }
    let construction_replication_pass = request.mean_replication_ppm <= 3_000_000;

    let primary_rows = match manifest.geometry.primary_rows {
        V36PrimaryRows::Control256 => 256,
        V36PrimaryRows::Posting4096 => 4_096,
        V36PrimaryRows::Posting8192 => 8_192,
    };
    let posting_count = request.rows.div_ceil(primary_rows);
    let projection_bytes = u64::from(manifest.source_dimensions)
        .checked_mul(u64::from(PROJECTION_DIMENSIONS))
        .and_then(|value| value.checked_mul(4))
        .ok_or_else(|| invalid("V36 projection bytes overflow"))?;
    let super_centroid_bytes = super_cell_count
        .checked_mul(u64::from(PROJECTION_DIMENSIONS))
        .and_then(|value| value.checked_mul(4))
        .ok_or_else(|| invalid("V36 super-centroid bytes overflow"))?;
    let posting_summary_bytes = posting_count
        .checked_mul(POSTING_SUMMARY_SLOT_BYTES)
        .ok_or_else(|| invalid("V36 posting-summary bytes overflow"))?;
    let delta_coarse_and_csr_bytes = checked_sum(
        &[request.delta_coarse_bytes, request.delta_csr_bytes],
        "V36 delta bytes overflow",
    )?;
    if projection_bytes > 3 * MIB
        || super_centroid_bytes > 4 * MIB
        || posting_summary_bytes > 128 * MIB
        || request.liveness_bytes > 64 * MIB
        || request.decoded_cache_bytes > 256 * MIB
        || delta_coarse_and_csr_bytes > 512 * MIB
        || request.recent_fine_bytes > 128 * MIB
    {
        return Err(invalid("V36 resident allocation exceeds component cap"));
    }

    let remote_coarse_payload_bytes = request
        .rows
        .checked_mul(manifest.coarse_code.record_bytes())
        .and_then(|value| value.checked_mul(u64::from(request.mean_replication_ppm)))
        .map(|value| value.div_ceil(1_000_000))
        .ok_or_else(|| invalid("V36 remote coarse payload overflows"))?;
    let remote_coarse_cap_eight_bytes = request
        .rows
        .checked_mul(manifest.coarse_code.record_bytes())
        .and_then(|value| value.checked_mul(8))
        .ok_or_else(|| invalid("V36 cap-eight coarse payload overflows"))?;
    let fine_vector_bytes_per_row = match manifest.fine_codec {
        V36FineCodec::Sq8 => u64::from(manifest.source_dimensions),
        V36FineCodec::F16 => u64::from(manifest.source_dimensions) * 2,
        V36FineCodec::SourceF32Control => u64::from(manifest.source_dimensions) * 4,
    };
    let fine_bytes_per_row = fine_vector_bytes_per_row
        .checked_add(16)
        .ok_or_else(|| invalid("V36 remote fine row bytes overflow"))?;
    let remote_fine_payload_bytes = request
        .rows
        .checked_mul(fine_bytes_per_row)
        .ok_or_else(|| invalid("V36 remote fine payload overflows"))?;
    let minimum_coarse_fragment_count =
        remote_coarse_payload_bytes.div_ceil(COARSE_FRAGMENT_LIMIT_BYTES);
    let minimum_fine_chunk_count =
        remote_fine_payload_bytes.div_ceil(manifest.chunk_ceiling.encoded_bytes());
    let minimum_posting_object_directory_bytes = minimum_coarse_fragment_count
        .checked_add(minimum_fine_chunk_count)
        .and_then(|objects| objects.checked_mul(OBJECT_DIRECTORY_ENTRY_BYTES))
        .and_then(|bytes| {
            posting_count
                .checked_mul(POSTING_DIRECTORY_ENTRY_BYTES)
                .and_then(|posting_bytes| bytes.checked_add(posting_bytes))
        })
        .ok_or_else(|| invalid("V36 minimum object directory overflows"))?;
    let minimum_fine_interval_directory_bytes = minimum_fine_chunk_count
        .checked_mul(FINE_INTERVAL_ENTRY_BYTES)
        .ok_or_else(|| invalid("V36 minimum fine directory overflows"))?;
    if request.posting_object_directory_bytes < minimum_posting_object_directory_bytes
        || request.fine_interval_directory_bytes < minimum_fine_interval_directory_bytes
    {
        return Err(invalid(
            "V36 directory projection understates format minimum",
        ));
    }
    let directory_bytes = checked_sum(
        &[
            request.posting_object_directory_bytes,
            request.fine_interval_directory_bytes,
        ],
        "V36 directory bytes overflow",
    )?;
    if directory_bytes > 128 * MIB {
        return Err(invalid("V36 resident allocation exceeds component cap"));
    }
    let resident_admission_bytes = checked_sum(
        &[
            projection_bytes,
            super_centroid_bytes,
            posting_summary_bytes,
            request.hnsw_bytes,
            directory_bytes,
            request.liveness_bytes,
            request.active_generation_bytes,
            request.retiring_generation_bytes,
            request.decoded_cache_bytes,
            request.encoded_decoded_overlap_bytes,
            delta_coarse_and_csr_bytes,
            request.recent_fine_bytes,
            request.compaction_old_arena_bytes,
            request.compaction_new_arena_bytes,
            request.retry_buffer_bytes,
            QUERY_WORKSPACE_BYTES,
            RUNTIME_BYTES,
        ],
        "V36 resident admission overflows",
    )?;
    if resident_admission_bytes >= resident_limit_bytes {
        return Err(invalid("V36 resident admission reaches scale limit"));
    }

    Ok(V36ResourceLedger {
        active_generation_bytes: request.active_generation_bytes,
        construction_replication_pass,
        compaction_new_arena_bytes: request.compaction_new_arena_bytes,
        compaction_old_arena_bytes: request.compaction_old_arena_bytes,
        decoded_cache_bytes: request.decoded_cache_bytes,
        delta_coarse_and_csr_bytes,
        encoded_decoded_overlap_bytes: request.encoded_decoded_overlap_bytes,
        directory_bytes,
        hnsw_bytes: request.hnsw_bytes,
        liveness_bytes: request.liveness_bytes,
        minimum_coarse_fragment_count,
        minimum_fine_chunk_count,
        minimum_fine_interval_directory_bytes,
        minimum_posting_object_directory_bytes,
        posting_count,
        posting_summary_bytes,
        projection_bytes,
        query_workspace_bytes: QUERY_WORKSPACE_BYTES,
        recent_fine_bytes: request.recent_fine_bytes,
        resident_admission_bytes,
        resident_limit_bytes,
        retiring_generation_bytes: request.retiring_generation_bytes,
        remote_coarse_payload_bytes,
        remote_coarse_cap_eight_bytes,
        remote_fine_payload_bytes,
        retry_buffer_bytes: request.retry_buffer_bytes,
        runtime_bytes: RUNTIME_BYTES,
        super_cell_count,
        super_centroid_bytes,
    })
}
