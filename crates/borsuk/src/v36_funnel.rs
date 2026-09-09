use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{BorsukError, Result};

const FORMAT: &str = "borsuk-v36-funnel-manifest-v3";
const PROJECTION_DIMENSIONS: u16 = 192;
const PROJECTION_SEED: u64 = 36;
const COARSE_FRAGMENT_LIMIT_BYTES: u64 = 512 * 1_024;
const NORMAL_GET_LIMIT: u16 = 14;
const NORMAL_BYTE_LIMIT: u64 = 7 * 1_048_576;
const HARD_GET_LIMIT: u16 = 16;
const HARD_BYTE_LIMIT: u64 = 8 * 1_048_576;
const MIB: u64 = 1_048_576;
pub(crate) const POSTING_SUMMARY_SLOT_BYTES: u64 = 4_736;
const OBJECT_DIRECTORY_ENTRY_BYTES: u64 = 80;
const POSTING_DIRECTORY_ENTRY_BYTES: u64 = 16;
const FINE_INTERVAL_ENTRY_BYTES: u64 = 16;
const QUERY_WORKSPACE_BYTES: u64 = 16 * 32 * MIB;
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
    /// Six total projected-space prototypes, including the posting centroid.
    #[serde(rename = "prototype-six")]
    Prototype6,
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
#[serde(rename_all = "kebab-case", tag = "arm")]
/// Frozen projection authority for the bounded V36 screen.
pub enum V36ProjectionArm {
    /// Deterministic SRHT control.
    Srht192 {
        /// Structured projection seed.
        seed: u64,
    },
    /// Deterministic centered covariance eigenspace.
    CenteredSubspace192 {
        /// Complete authenticated Arrow IPC projection object.
        artifact: Box<V36ArtifactIdentity>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// One role selected from the bounded screen population.
pub struct V36PrefixRoleAuthority {
    /// Role name.
    pub role: String,
    /// Exact row count.
    pub rows: u64,
    /// Population-specific role-selection seed label.
    pub seed_label: String,
    /// Role-selection seed digest.
    pub seed_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// One complete immutable source object consumed by the bounded screen.
pub struct V36PrefixSourceObject {
    /// Independently authenticated BLAKE3.
    pub blake3: String,
    /// Complete encoded length.
    pub encoded_bytes: u64,
    /// Registered source-manifest path.
    pub path: String,
    /// Query-independent object-sampling digest.
    pub sample_sha256: String,
    /// Registered complete-object SHA-256.
    pub sha256: String,
    /// Immutable source URI.
    pub uri: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// One object from an authenticated ordered V36 source registry.
pub struct V36PrefixRegisteredSourceObject {
    /// Complete encoded length.
    pub encoded_bytes: u64,
    /// Registered source-manifest path.
    pub path: String,
    /// Registered complete-object SHA-256.
    pub sha256: String,
    /// Immutable source URI.
    pub uri: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Immutable inputs and limits for one V36 prefix population freeze.
pub struct V36PrefixFreezeAuthority {
    /// Diagnostic authority can never support release claims.
    pub claim_eligible: bool,
    /// Zero-based independently registered screen cohort.
    pub cohort_ordinal: u8,
    /// Capability available while constructing the population.
    pub construction_capability: String,
    /// Population-specific corpus-selection seed label.
    pub corpus_seed_label: String,
    /// SHA-256 of the corpus-selection seed label.
    pub corpus_seed_sha256: String,
    /// Exact corpus rows after query removal.
    pub corpus_rows: u64,
    /// Exact SHA-256 of the upstream dataset authority bytes.
    pub dataset_authority_sha256: String,
    /// Exact distinct candidates retained before role hashing.
    pub distinct_candidates: u64,
    /// Physical duplicate resolution rule.
    pub duplicate_rule: String,
    /// Capability available to later evaluation.
    pub evaluation_capability: String,
    /// Prior cohort selected-ID artifact; absent only for cohort zero.
    pub excluded_population_identity: Option<V36ArtifactIdentity>,
    /// Screen query roles excluded from every later full-source role.
    pub future_full_source_exclusion_roles: Vec<String>,
    /// Complete-source disposition for any invalid gated row.
    pub invalid_row_policy: String,
    /// Maximum complete source objects.
    pub object_cap: u16,
    /// Query-independent object ranking algorithm.
    pub object_sampling_algorithm: String,
    /// Digest of the complete ordered source registry.
    pub ordered_source_manifest_sha256: String,
    /// Query-independent population-row sampling algorithm.
    pub population_sampling_algorithm: String,
    /// Population-row sampling seed label.
    pub population_seed_label: String,
    /// SHA-256 of the population-row sampling seed label.
    pub population_seed_sha256: String,
    /// Sum of encoded lengths across the complete registered source set.
    pub registry_encoded_bytes: u64,
    /// Count of objects in the complete registered source set.
    pub registry_objects: u32,
    /// Ordered query-role authorities.
    pub roles: Vec<V36PrefixRoleAuthority>,
    /// Exact authority schema marker.
    pub schema: String,
    /// Number of complete objects in this cohort's registered window.
    pub selected_object_count: u16,
    /// Sum of encoded lengths in the registered object window.
    pub selected_object_encoded_bytes: u64,
    /// Zero-based start of this cohort's registered object window.
    pub selected_object_start: u16,
    /// Maximum complete encoded source bytes.
    pub source_byte_cap: u64,
    /// Frozen upstream source revision.
    pub source_revision: String,
    /// Bytes available to each streaming workspace.
    pub workspace_bytes: u64,
    /// Count of streaming workspaces.
    pub workspace_count: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Immutable executable and lifecycle authority for one bounded freeze attempt.
pub struct V36PrefixFreezeExecutionAuthority {
    /// Maximum active execution wall time.
    pub active_wall_seconds: u64,
    /// Unique attempt identity.
    pub attempt_id: String,
    /// Maximum active seconds between durable checkpoint publications.
    pub checkpoint_seconds: u64,
    /// Diagnostic execution can never support a release claim.
    pub claim_eligible: bool,
    /// Exact binary, freeze-authority, source-archive, and registry identities.
    pub inputs: Vec<V36ArtifactIdentity>,
    /// Attempt-scoped S3 output prefix.
    pub output_prefix: String,
    /// Exact newest run-scoped population head, absent only for a fresh start.
    pub resume: Option<V36PrefixResumeBinding>,
    /// Exact authority schema marker.
    pub schema: String,
    /// Exact source commit used to build the executable.
    pub source_commit: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Immutable checkpoint head bound into one replacement attempt.
pub struct V36PrefixResumeBinding {
    /// Exact newest generation observed before launch.
    pub generation: u32,
    /// Immutable manifest named by the pointer.
    pub manifest: V36ArtifactIdentity,
    /// Exact canonical pointer length.
    pub pointer_encoded_bytes: u64,
    /// SHA-256 of the exact canonical pointer bytes.
    pub pointer_sha256: String,
    /// Sole run-scoped newest-pointer URI.
    pub pointer_uri: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Authority for the bounded hash-object-sampled V36 population.
pub struct V36PrefixPopulationAuthority {
    /// Diagnostic populations can never make release claims.
    pub claim_eligible: bool,
    /// Zero-based independently registered screen cohort.
    pub cohort_ordinal: u8,
    /// Capability available to construction.
    pub construction_capability: String,
    /// Complete consumed-object identities in sample order.
    pub consumed_objects: Vec<V36PrefixSourceObject>,
    /// Exact corpus row count after role removal.
    pub corpus_rows: u64,
    /// Population-specific corpus-selection seed label.
    pub corpus_seed_label: String,
    /// SHA-256 of the corpus-selection seed label.
    pub corpus_seed_sha256: String,
    /// Exact SHA-256 of the upstream dataset authority bytes.
    pub dataset_authority_sha256: String,
    /// Exact distinct-row target before role removal.
    pub distinct_candidates: u64,
    /// Duplicate resolution rule.
    pub duplicate_rule: String,
    /// Capability available to evaluation.
    pub evaluation_capability: String,
    /// Prior cohort selected-ID artifact; absent only for cohort zero.
    pub excluded_population_identity: Option<V36ArtifactIdentity>,
    /// Exact format marker.
    pub format: String,
    /// Screen query roles excluded from every later full-source role.
    pub future_full_source_exclusion_roles: Vec<String>,
    /// Maximum complete source objects.
    pub object_cap: u16,
    /// Query-independent object sampling algorithm.
    pub object_sampling_algorithm: String,
    /// Ordered complete-source-manifest digest.
    pub ordered_source_manifest_sha256: String,
    /// Query-independent population-row sampling algorithm.
    pub population_sampling_algorithm: String,
    /// Population-row sampling seed label.
    pub population_seed_label: String,
    /// SHA-256 of the population-row sampling seed label.
    pub population_seed_sha256: String,
    /// Identity distinct from full-source qualification.
    pub population_id: String,
    /// Ordered role authorities.
    pub roles: Vec<V36PrefixRoleAuthority>,
    /// Number of complete objects in the registered cohort window.
    pub selected_object_count: u16,
    /// Sum of encoded lengths in the registered cohort window.
    pub selected_object_encoded_bytes: u64,
    /// Zero-based start of the registered cohort window.
    pub selected_object_start: u16,
    /// Maximum complete encoded source bytes.
    pub source_byte_cap: u64,
    /// Frozen source revision.
    pub source_revision: String,
    /// Bytes per streaming query workspace.
    pub workspace_bytes: u64,
    /// Streaming query workspace count.
    pub workspace_count: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Canonical transaction receipt for one completed prefix population freeze.
pub struct V36PrefixFreezeReceipt {
    /// Diagnostic evidence can never make a release claim.
    pub claim_eligible: bool,
    /// Registered source-object ordinal containing the distinct-row cutoff.
    pub cutoff_object_ordinal: u16,
    /// Physical row offset of the cutoff within that complete object.
    pub cutoff_row_offset: u64,
    /// Distinct IDs observed through the complete cutoff object.
    pub distinct_rows_observed: u64,
    /// Physical rows repeating an earlier feature ID.
    pub duplicate_rows: u64,
    /// SHA-256 of the exact execution authority bytes.
    pub execution_authority_sha256: String,
    /// SHA-256 of the exact pre-freeze authority bytes.
    pub freeze_authority_sha256: String,
    /// Ordered complete output artifact identities.
    pub outputs: Vec<V36ArtifactIdentity>,
    /// Physical rows observed through the complete cutoff object.
    pub physical_rows: u64,
    /// Complete post-freeze semantic population authority.
    pub population: V36PrefixPopulationAuthority,
    /// Exact externally selected population consumed by materialization.
    pub selection: V36PrefixPopulationSelection,
    /// Exact receipt schema marker.
    pub schema: String,
    /// SHA-256 of the exact source archive evidence.
    pub source_archive_sha256: String,
    /// SHA-256 of the exact complete source registry bytes.
    pub source_registry_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
/// Phase-specific durable state for one V36 prefix-freeze checkpoint.
pub enum V36PrefixCheckpointPhase {
    /// A consecutive prefix of complete registered source objects.
    Population,
    /// Complete population selection ready for vector materialization.
    Selected {
        /// Immutable population-score cutoff and selected-row identities.
        selection: Box<V36PrefixPopulationSelection>,
    },
    /// Complete role-separated Parquet outputs ready for reuse.
    Materialized {
        /// Exact population selection consumed by materialization.
        selection: Box<V36PrefixPopulationSelection>,
        /// Named immutable population authority, source, and query artifacts.
        artifacts: Box<V36PrefixMaterializedArtifacts>,
    },
    /// All quality-query heaps after one complete source-row prefix.
    GroundTruth {
        /// Exact population selection consumed by materialization.
        selection: Box<V36PrefixPopulationSelection>,
        /// Exact materialized lineage consumed by this GT generation.
        materialized: Box<V36PrefixMaterializedArtifacts>,
        /// Canonical Arrow IPC top-100 heap snapshot.
        heaps: Box<V36ArtifactIdentity>,
        /// First source row not incorporated into every query heap.
        next_source_ordinal: u64,
    },
    /// Exact GT@100 Parquets and their authenticated terminal heap.
    Complete {
        /// Exact population selection consumed by materialization.
        selection: Box<V36PrefixPopulationSelection>,
        /// Exact materialized lineage consumed by exact GT.
        materialized: Box<V36PrefixMaterializedArtifacts>,
        /// Named immutable exact-GT artifacts.
        ground_truth: Box<V36PrefixGroundTruthArtifacts>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Immutable population selection produced after the complete object window.
pub struct V36PrefixPopulationSelection {
    /// Feature-row ID at the inclusive population-score cutoff.
    pub cutoff_feature_row_id: u64,
    /// SHA-256 population score at the inclusive cutoff.
    pub cutoff_score_sha256: String,
    /// Distinct rows eligible after any authenticated prior-cohort exclusion.
    pub eligible_rows: u64,
    /// Distinct rows removed by authenticated prior-cohort exclusion.
    pub excluded_rows: u64,
    /// Exact prior-cohort selected-ID artifact consumed during exclusion.
    pub excluded_population_identity: Option<V36ArtifactIdentity>,
    /// Complete Arrow IPC selected-identity artifact.
    pub selected_ids: V36ArtifactIdentity,
    /// Exact number of retained population identities.
    pub selected_rows: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Complete population accounting retained across every checkpoint phase.
pub struct V36PrefixPopulationCheckpoint {
    /// Number of complete registered source objects incorporated.
    pub completed_objects: u16,
    /// Complete source objects committed in ranked order.
    pub consumed_objects: Vec<V36PrefixSourceObject>,
    /// Distinct feature IDs observed through the complete prefix.
    pub distinct_rows: u64,
    /// Duplicate physical rows observed through the complete prefix.
    pub duplicate_rows: u64,
    /// One immutable Arrow IPC first-occurrence run per source object.
    pub identity_runs: Vec<V36ArtifactIdentity>,
    /// Physical rows observed through the complete prefix.
    pub physical_rows: u64,
    /// Number of objects in the registered cohort window.
    pub selected_object_count: u16,
    /// Global ranked ordinal at which the cohort window starts.
    pub selected_object_start: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Named immutable outputs at the completed materialization boundary.
pub struct V36PrefixMaterializedArtifacts {
    /// Canonical population authority JSON.
    pub population_authority: V36ArtifactIdentity,
    /// Query-excluded source Parquet.
    pub source: V36ArtifactIdentity,
    /// Development query Parquet.
    pub development_query: V36ArtifactIdentity,
    /// Validation query Parquet.
    pub validation_query: V36ArtifactIdentity,
    /// Sealed-holdout query Parquet.
    pub sealed_holdout_query: V36ArtifactIdentity,
    /// Performance-only query Parquet.
    pub performance_query: V36ArtifactIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Immutable exact-ground-truth outputs at the completed checkpoint boundary.
pub struct V36PrefixGroundTruthArtifacts {
    /// Final canonical all-query heap snapshot.
    pub heaps: V36ArtifactIdentity,
    /// Development GT@100 Parquet.
    pub development: V36ArtifactIdentity,
    /// Validation GT@100 Parquet.
    pub validation: V36ArtifactIdentity,
    /// Sealed-holdout GT@100 Parquet.
    pub sealed_holdout: V36ArtifactIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Trusted campaign authority used to authenticate checkpoint claims.
pub struct V36PrefixCheckpointContext {
    /// Zero-based independently registered screen cohort.
    pub cohort_ordinal: u8,
    /// Exact materialized source row count.
    pub corpus_rows: u64,
    /// Registered distinct-row cutoff.
    pub distinct_candidates: u64,
    /// Prior-cohort selected-ID artifact required for nonzero cohorts.
    pub excluded_population_identity: Option<V36ArtifactIdentity>,
    /// SHA-256 of the scientific freeze authority.
    pub freeze_authority_sha256: String,
    /// Durable GT source-block row count.
    pub gt_block_rows: u64,
    /// Exact campaign-scoped immutable object prefix.
    pub object_prefix: String,
    /// One run-scoped compare-and-swap pointer URI.
    pub pointer_uri: String,
    /// Source registry entries in query-independent registered rank order.
    pub ranked_objects: Vec<V36PrefixRegisteredSourceObject>,
    /// Stable campaign run identity.
    pub run_id: String,
    /// SHA-256 of the frozen source archive.
    pub source_archive_sha256: String,
    /// Number of complete objects in the registered cohort window.
    pub selected_object_count: u16,
    /// Global ranked ordinal at which the cohort window starts.
    pub selected_object_start: u16,
    /// Maximum complete encoded source bytes.
    pub source_byte_cap: u64,
    /// Exact source commit.
    pub source_commit: String,
    /// SHA-256 of the complete registered source list.
    pub source_registry_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Canonical manifest for one immutable V36 prefix-freeze checkpoint generation.
pub struct V36PrefixCheckpointManifest {
    /// Diagnostic checkpoints can never make release claims.
    pub claim_eligible: bool,
    /// SHA-256 of the producing attempt's execution authority.
    pub execution_authority_sha256: String,
    /// SHA-256 of the scientific freeze authority.
    pub freeze_authority_sha256: String,
    /// Monotonic checkpoint generation.
    pub generation: u32,
    /// Phase-specific durable state.
    pub phase: V36PrefixCheckpointPhase,
    /// Complete population accounting retained by every phase.
    pub population: V36PrefixPopulationCheckpoint,
    /// Prior immutable checkpoint manifest, absent only for generation zero.
    pub previous_checkpoint: Option<V36ArtifactIdentity>,
    /// Attempt that produced this generation.
    pub producer_attempt_id: String,
    /// Zero-based producer attempt ordinal used to fence zombie writers.
    pub producer_attempt_ordinal: u8,
    /// EC2 instance that produced this generation.
    pub producer_instance_id: String,
    /// Exact checkpoint schema marker.
    pub schema: String,
    /// Campaign run identity shared across replacement attempts.
    pub run_id: String,
    /// SHA-256 of the frozen source archive.
    pub source_archive_sha256: String,
    /// Exact source commit.
    pub source_commit: String,
    /// SHA-256 of the complete registered source list.
    pub source_registry_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Run-scoped compare-and-swap pointer to one immutable checkpoint manifest.
pub struct V36PrefixCheckpointPointer {
    /// Diagnostic checkpoint pointers can never make release claims.
    pub claim_eligible: bool,
    /// Manifest generation referenced by this pointer.
    pub generation: u32,
    /// Exact immutable checkpoint manifest object.
    pub manifest: V36ArtifactIdentity,
    /// Attempt that produced the referenced manifest.
    pub producer_attempt_id: String,
    /// Zero-based producer ordinal used to fence zombie writers.
    pub producer_attempt_ordinal: u8,
    /// Stable campaign run identity.
    pub run_id: String,
    /// Exact pointer schema marker.
    pub schema: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Conditional write required to advance one run-scoped checkpoint pointer.
pub enum V36PrefixCheckpointPointerCondition {
    /// Create generation zero only when no pointer exists.
    Create,
    /// Replace the authenticated current pointer only at its exact ETag.
    Replace {
        /// Current pointer ETag supplied to the conditional write.
        etag: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Dependency-first immutable publication followed by one pointer CAS.
pub struct V36PrefixCheckpointPublication {
    /// Immutable phase dependencies uploaded before the manifest.
    pub dependencies: Vec<V36ArtifactIdentity>,
    /// Exact conditional pointer operation.
    pub condition: V36PrefixCheckpointPointerCondition,
    /// Content-addressed immutable manifest identity.
    pub manifest: V36ArtifactIdentity,
    /// Exact canonical manifest bytes.
    pub manifest_bytes: Vec<u8>,
    /// Exact canonical pointer bytes written last.
    pub pointer_bytes: Vec<u8>,
    /// One trusted run-scoped conditional pointer destination.
    pub pointer_uri: String,
    /// SHA-256 of the exact predecessor pointer bytes, absent at genesis.
    pub previous_pointer_sha256: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Closed manifest for one bounded V36 screen arm.
pub struct V36PrefixScreenManifest {
    /// Complete role-separated artifacts.
    pub artifacts: Vec<V36ArtifactIdentity>,
    /// Screen results can never make release claims.
    pub claim_eligible: bool,
    /// Fine object ceiling.
    pub chunk_ceiling: V36ChunkCeiling,
    /// Coarse representation.
    pub coarse_code: V36CoarseCode,
    /// Fine representation.
    pub fine_codec: V36FineCodec,
    /// Exact format marker.
    pub format: String,
    /// Posting geometry.
    pub geometry: V36GeometryArm,
    /// Bound population authority.
    pub population: V36PrefixPopulationAuthority,
    /// Reserved metadata bytes in the equal posting-summary slot.
    pub posting_summary_metadata_bytes: u64,
    /// Complete equal posting-summary slot bytes.
    pub posting_summary_slot_bytes: u64,
    /// Reserved projected-vector bytes in the equal posting-summary slot.
    pub posting_summary_vector_bytes: u64,
    /// Frozen projection authority.
    pub projection: V36ProjectionArm,
    /// Frozen posting-ranking score.
    pub shape_score: V36ShapeScore,
    /// Bounded unique source-ID heap size.
    pub unique_k: u16,
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
    /// Frozen projection authority.
    pub projection: V36ProjectionArm,
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

fn source_sample_sha256(path: &str, encoded_bytes: u64) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"borsuk-v36-screen-object-v1");
    hasher.update(path.as_bytes());
    hasher.update(encoded_bytes.to_le_bytes());
    format!("{:x}", hasher.finalize())
}

fn validate_prefix_source_registry(
    population: &V36PrefixPopulationAuthority,
    source_registry: &[V36PrefixRegisteredSourceObject],
) -> Result<()> {
    let window_start = usize::from(population.selected_object_start);
    let window_end = window_start
        .checked_add(population.consumed_objects.len())
        .ok_or_else(|| invalid("V36 prefix source registry window overflows"))?;
    if source_registry.len() < window_end {
        return Err(invalid("V36 prefix source registry is incomplete"));
    }
    validate_prefix_source_registry_identity(
        &population.source_revision,
        &population.ordered_source_manifest_sha256,
        source_registry,
    )?;

    let mut ranked = source_registry
        .iter()
        .map(|object| {
            (
                source_sample_sha256(&object.path, object.encoded_bytes),
                object.path.as_str(),
                object,
            )
        })
        .collect::<Vec<_>>();
    ranked.sort_by(|left, right| (left.0.as_str(), left.1).cmp(&(right.0.as_str(), right.1)));
    for (observed, (sample_sha256, _, registered)) in population
        .consumed_objects
        .iter()
        .zip(ranked.into_iter().skip(window_start))
    {
        if observed.sample_sha256 != sample_sha256
            || observed.path != registered.path
            || observed.uri != registered.uri
            || observed.sha256 != registered.sha256
            || observed.encoded_bytes != registered.encoded_bytes
        {
            return Err(invalid(
                "V36 prefix consumed objects are not the ranked prefix",
            ));
        }
    }
    Ok(())
}

fn validate_prefix_source_registry_identity(
    source_revision: &str,
    ordered_source_manifest_sha256: &str,
    source_registry: &[V36PrefixRegisteredSourceObject],
) -> Result<()> {
    let mut paths = BTreeSet::new();
    let mut uris = BTreeSet::new();
    let mut ordered = source_registry.iter().collect::<Vec<_>>();
    ordered.sort_by(|left, right| left.path.as_bytes().cmp(right.path.as_bytes()));
    let mut manifest_hasher = Sha256::new();
    for object in ordered {
        let expected_uri = format!(
            "https://huggingface.co/datasets/andropar/relaion2b-natural-embeddings/resolve/{}/{path}",
            source_revision,
            path = object.path,
        );
        if object.path.is_empty()
            || object.uri != expected_uri
            || object.encoded_bytes == 0
            || !valid_digest(&object.sha256)
            || !paths.insert(object.path.as_str())
            || !uris.insert(object.uri.as_str())
        {
            return Err(invalid("V36 prefix registered source object differs"));
        }
        manifest_hasher.update(object.path.as_bytes());
        manifest_hasher.update(b"\t");
        manifest_hasher.update(object.sha256.as_bytes());
        manifest_hasher.update(b"\t");
        manifest_hasher.update(object.encoded_bytes.to_string().as_bytes());
        manifest_hasher.update(b"\n");
    }
    if format!("{:x}", manifest_hasher.finalize()) != ordered_source_manifest_sha256 {
        return Err(invalid("V36 prefix ordered source manifest differs"));
    }
    Ok(())
}

fn validate_prefix_population_shape(
    population: &V36PrefixPopulationAuthority,
    source_registry: &[V36PrefixRegisteredSourceObject],
) -> Result<()> {
    if population.format != "borsuk-v36-prefix-population-authority-v2"
        || population.claim_eligible
        || population.population_id != "borsuk-v36-prefix-screen-population-v2"
        || population.object_sampling_algorithm
            != "sha256-borsuk-v36-screen-object-v1-path-utf8-length-le-u64-then-path"
        || population.population_sampling_algorithm
            != "sha256-seed-sha256-manifest-sha256-feature-row-id-le-u64-v2"
        || population.population_seed_label != "borsuk-v36-prefix-screen-population-row-v2"
        || population.population_seed_sha256
            != format!(
                "{:x}",
                Sha256::digest(population.population_seed_label.as_bytes())
            )
        || population.corpus_seed_label != "borsuk-v36-prefix-screen-corpus-v2"
        || population.corpus_seed_sha256
            != format!(
                "{:x}",
                Sha256::digest(population.corpus_seed_label.as_bytes())
            )
        || population.dataset_authority_sha256
            != "0d2e8cef3cf27860131a6a8c33d08b858f8837263212cb03515ae53c76acd5c1"
        || population.duplicate_rule != "first-selected-object-ordinal-then-row-offset"
        || population.distinct_candidates == 0
        || population.corpus_rows == 0
        || population.object_cap != 16
        || population.cohort_ordinal > 1
        || population.selected_object_count == 0
        || population.selected_object_count > population.object_cap
        || population.selected_object_start
            != u16::from(population.cohort_ordinal)
                .checked_mul(population.selected_object_count)
                .ok_or_else(|| invalid("V36 prefix selected object window overflows"))?
        || population.source_byte_cap == 0
        || population.workspace_count != 16
        || population.workspace_bytes != 32 * MIB
        || population.construction_capability != "named-query-excluded-corpus-only-no-query-truth"
        || population.evaluation_capability != "named-artifacts-only-no-source-list-discovery"
        || population.source_revision != "bfc7465dcf1245bd605d35dcaf5d2177bbc2025a"
        || !valid_digest(&population.ordered_source_manifest_sha256)
    {
        return Err(invalid("V36 prefix population authority differs"));
    }
    let expected_exclusion_roles = ["development", "validation", "sealed-holdout", "performance"];
    if population.future_full_source_exclusion_roles != expected_exclusion_roles.map(str::to_owned)
        || match population.cohort_ordinal {
            0 => population.excluded_population_identity.is_some(),
            1 => !population
                .excluded_population_identity
                .as_ref()
                .is_some_and(|artifact| {
                    valid_checkpoint_artifact(artifact, "population-selected-identities")
                }),
            _ => true,
        }
    {
        return Err(invalid("V36 prefix cohort authority differs"));
    }

    let expected_roles = [
        (
            "development",
            "borsuk-v36-prefix-screen-development-query-v2",
        ),
        ("validation", "borsuk-v36-prefix-screen-validation-query-v2"),
        (
            "sealed-holdout",
            "borsuk-v36-prefix-screen-sealed-holdout-query-v2",
        ),
        (
            "performance",
            "borsuk-v36-prefix-screen-performance-query-v2",
        ),
    ];
    if population.roles.len() != expected_roles.len()
        || population
            .roles
            .iter()
            .zip(expected_roles)
            .any(|(role, (name, seed_label))| {
                role.role != name
                    || role.rows == 0
                    || role.seed_label != seed_label
                    || role.seed_sha256 != format!("{:x}", Sha256::digest(seed_label.as_bytes()))
            })
    {
        return Err(invalid("V36 prefix role authority differs"));
    }
    let role_seeds = population
        .roles
        .iter()
        .map(|role| role.seed_sha256.as_str())
        .collect::<BTreeSet<_>>();
    if role_seeds.len() != expected_roles.len() {
        return Err(invalid("V36 prefix role seeds overlap"));
    }
    let query_rows = population.roles.iter().try_fold(0_u64, |sum, role| {
        sum.checked_add(role.rows)
            .ok_or_else(|| invalid("V36 prefix role row count overflows"))
    })?;
    if query_rows
        .checked_add(population.corpus_rows)
        .is_none_or(|assigned| assigned > population.distinct_candidates)
    {
        return Err(invalid("V36 prefix population row count differs"));
    }

    if population.consumed_objects.len() != usize::from(population.selected_object_count) {
        return Err(invalid("V36 prefix consumed-object count differs"));
    }
    let mut paths = BTreeSet::new();
    let mut uris = BTreeSet::new();
    let mut previous: Option<(&str, &str)> = None;
    let mut total_bytes = 0_u64;
    for object in &population.consumed_objects {
        let expected_sample_sha256 = source_sample_sha256(&object.path, object.encoded_bytes);
        if object.path.is_empty()
            || object.uri.is_empty()
            || object.encoded_bytes == 0
            || !valid_digest(&object.sha256)
            || !valid_digest(&object.blake3)
            || !valid_digest(&object.sample_sha256)
            || object.sample_sha256 != expected_sample_sha256
            || !paths.insert(object.path.as_str())
            || !uris.insert(object.uri.as_str())
        {
            return Err(invalid("V36 prefix source-object identity differs"));
        }
        let current = (object.sample_sha256.as_str(), object.path.as_str());
        if previous.is_some_and(|prior| prior >= current) {
            return Err(invalid("V36 prefix source-object order differs"));
        }
        previous = Some(current);
        total_bytes = total_bytes
            .checked_add(object.encoded_bytes)
            .ok_or_else(|| invalid("V36 prefix source bytes overflow"))?;
    }
    if total_bytes != population.selected_object_encoded_bytes
        || total_bytes > population.source_byte_cap
    {
        return Err(invalid("V36 prefix source bytes differ"));
    }
    validate_prefix_source_registry(population, source_registry)?;
    Ok(())
}

fn validate_prefix_population(
    population: &V36PrefixPopulationAuthority,
    source_registry: &[V36PrefixRegisteredSourceObject],
) -> Result<()> {
    validate_prefix_population_shape(population, source_registry)?;
    let expected_role_rows = [1_000_u64, 1_000, 1_000, 10_000];
    if population.distinct_candidates != 1_100_000
        || population.corpus_rows != 1_000_000
        || population.source_byte_cap != 6 * 1_024 * MIB
        || population
            .roles
            .iter()
            .zip(expected_role_rows)
            .any(|(role, expected_rows)| role.rows != expected_rows)
    {
        return Err(invalid("V36 prefix population authority differs"));
    }
    Ok(())
}

/// Validate the complete bounded population authority against its source registry.
pub fn validate_v36_prefix_population_authority(
    population: &V36PrefixPopulationAuthority,
    source_registry: &[V36PrefixRegisteredSourceObject],
) -> Result<()> {
    validate_prefix_population(population, source_registry)
}

/// Validate immutable freeze inputs without requiring future consumed-object evidence.
pub fn validate_v36_prefix_freeze_authority(
    authority: &V36PrefixFreezeAuthority,
    source_registry: &[V36PrefixRegisteredSourceObject],
) -> Result<()> {
    if authority.schema != "borsuk-v36-prefix-freeze-authority-v2"
        || authority.claim_eligible
        || authority.construction_capability != "named-query-excluded-corpus-only-no-query-truth"
        || authority.evaluation_capability != "named-artifacts-only-no-source-list-discovery"
        || authority.invalid_row_policy != "reject-complete-source-revision"
        || authority.object_sampling_algorithm
            != "sha256-borsuk-v36-screen-object-v1-path-utf8-length-le-u64-then-path"
        || authority.population_sampling_algorithm
            != "sha256-seed-sha256-manifest-sha256-feature-row-id-le-u64-v2"
        || authority.population_seed_label != "borsuk-v36-prefix-screen-population-row-v2"
        || authority.population_seed_sha256
            != format!(
                "{:x}",
                Sha256::digest(authority.population_seed_label.as_bytes())
            )
        || authority.corpus_seed_label != "borsuk-v36-prefix-screen-corpus-v2"
        || authority.corpus_seed_sha256
            != format!(
                "{:x}",
                Sha256::digest(authority.corpus_seed_label.as_bytes())
            )
        || authority.dataset_authority_sha256
            != "0d2e8cef3cf27860131a6a8c33d08b858f8837263212cb03515ae53c76acd5c1"
        || authority.duplicate_rule != "first-selected-object-ordinal-then-row-offset"
        || authority.distinct_candidates != 1_100_000
        || authority.corpus_rows != 1_000_000
        || authority.object_cap != 16
        || authority.cohort_ordinal > 1
        || authority.selected_object_count == 0
        || authority.selected_object_count > authority.object_cap
        || authority.selected_object_start
            != u16::from(authority.cohort_ordinal)
                .checked_mul(authority.selected_object_count)
                .ok_or_else(|| invalid("V36 prefix selected object window overflows"))?
        || authority.source_byte_cap != 6 * 1_024 * MIB
        || authority.registry_objects != u32::try_from(source_registry.len()).unwrap_or(u32::MAX)
        || authority.workspace_count != 16
        || authority.workspace_bytes != 32 * MIB
        || authority.source_revision != "bfc7465dcf1245bd605d35dcaf5d2177bbc2025a"
    {
        return Err(invalid("V36 prefix freeze authority differs"));
    }
    let expected_exclusion_roles = ["development", "validation", "sealed-holdout", "performance"];
    if authority.future_full_source_exclusion_roles != expected_exclusion_roles.map(str::to_owned)
        || match authority.cohort_ordinal {
            0 => authority.excluded_population_identity.is_some(),
            1 => !authority
                .excluded_population_identity
                .as_ref()
                .is_some_and(|artifact| {
                    valid_checkpoint_artifact(artifact, "population-selected-identities")
                }),
            _ => true,
        }
    {
        return Err(invalid("V36 prefix cohort authority differs"));
    }
    let registry_encoded_bytes = source_registry.iter().try_fold(0_u64, |total, object| {
        total
            .checked_add(object.encoded_bytes)
            .ok_or_else(|| invalid("V36 prefix registry bytes overflow"))
    })?;
    if authority.registry_encoded_bytes != registry_encoded_bytes {
        return Err(invalid("V36 prefix registry byte total differs"));
    }
    let mut ranked = source_registry.iter().collect::<Vec<_>>();
    ranked.sort_by(|left, right| {
        (
            source_sample_sha256(&left.path, left.encoded_bytes),
            left.path.as_bytes(),
        )
            .cmp(&(
                source_sample_sha256(&right.path, right.encoded_bytes),
                right.path.as_bytes(),
            ))
    });
    let window_start = usize::from(authority.selected_object_start);
    let window_end = window_start
        .checked_add(usize::from(authority.selected_object_count))
        .ok_or_else(|| invalid("V36 prefix selected object window overflows"))?;
    let selected_bytes = ranked
        .get(window_start..window_end)
        .ok_or_else(|| invalid("V36 prefix selected object window differs"))?
        .iter()
        .try_fold(0_u64, |total, object| {
            total
                .checked_add(object.encoded_bytes)
                .ok_or_else(|| invalid("V36 prefix selected object bytes overflow"))
        })?;
    if selected_bytes != authority.selected_object_encoded_bytes
        || selected_bytes > authority.source_byte_cap
    {
        return Err(invalid("V36 prefix selected object bytes differ"));
    }
    let expected_roles = [
        (
            "development",
            1_000_u64,
            "borsuk-v36-prefix-screen-development-query-v2",
        ),
        (
            "validation",
            1_000,
            "borsuk-v36-prefix-screen-validation-query-v2",
        ),
        (
            "sealed-holdout",
            1_000,
            "borsuk-v36-prefix-screen-sealed-holdout-query-v2",
        ),
        (
            "performance",
            10_000,
            "borsuk-v36-prefix-screen-performance-query-v2",
        ),
    ];
    if authority.roles.len() != expected_roles.len()
        || authority
            .roles
            .iter()
            .zip(expected_roles)
            .any(|(role, expected)| {
                role.role != expected.0
                    || role.rows != expected.1
                    || role.seed_label != expected.2
                    || role.seed_sha256 != format!("{:x}", Sha256::digest(expected.2.as_bytes()))
            })
    {
        return Err(invalid("V36 prefix freeze role authority differs"));
    }
    validate_prefix_source_registry_identity(
        &authority.source_revision,
        &authority.ordered_source_manifest_sha256,
        source_registry,
    )
}

/// Validate the exact registered cohort-A screen authority used by the campaign runner.
///
/// The generic freeze validator deliberately supports reduced synthetic registries. The
/// executable campaign boundary must additionally pin the immutable production registry and
/// reject future cohorts until their exclusion evidence is authenticated and consumed.
pub fn validate_v36_prefix_registered_screen_authority(
    authority: &V36PrefixFreezeAuthority,
    source_registry: &[V36PrefixRegisteredSourceObject],
) -> Result<()> {
    validate_v36_prefix_freeze_authority(authority, source_registry)?;
    if authority.cohort_ordinal != 0
        || authority.selected_object_start != 0
        || authority.selected_object_count != 16
        || authority.selected_object_encoded_bytes != 5_485_265_954
        || authority.registry_objects != 2_298
        || authority.registry_encoded_bytes != 787_439_811_692
        || authority.ordered_source_manifest_sha256
            != "76ac61cf2821a331419ad40d5eb94d2cdafccccf39af17af1d328b7a2f0bc6c7"
        || authority.excluded_population_identity.is_some()
    {
        return Err(invalid("V36 prefix registered screen authority differs"));
    }
    Ok(())
}

/// Canonical newline JSON for one validated pre-freeze authority.
pub fn canonical_v36_prefix_freeze_authority_bytes(
    authority: &V36PrefixFreezeAuthority,
    source_registry: &[V36PrefixRegisteredSourceObject],
) -> Result<Vec<u8>> {
    validate_v36_prefix_freeze_authority(authority, source_registry)?;
    canonical_value_bytes(authority)
}

/// Canonical newline JSON for the complete registry bound by a freeze authority.
pub fn canonical_v36_prefix_source_registry_bytes(
    authority: &V36PrefixFreezeAuthority,
    source_registry: &[V36PrefixRegisteredSourceObject],
) -> Result<Vec<u8>> {
    validate_v36_prefix_freeze_authority(authority, source_registry)?;
    canonical_value_bytes(&source_registry)
}

fn valid_s3_object_uri(uri: &str, prefix: bool) -> bool {
    let Ok(parsed) = url::Url::parse(uri) else {
        return false;
    };
    let path = parsed.path();
    parsed.scheme() == "s3"
        && parsed.host_str().is_some_and(|host| !host.is_empty())
        && path.len() > 1
        && !path.split('/').any(|part| part == "..")
        && parsed.query().is_none()
        && parsed.fragment().is_none()
        && path.ends_with('/') == prefix
}

/// Validate the immutable executable and lifecycle authority for one attempt.
pub fn validate_v36_prefix_freeze_execution_authority(
    authority: &V36PrefixFreezeExecutionAuthority,
) -> Result<()> {
    if authority.schema != "borsuk-v36-prefix-freeze-execution-authority-v2"
        || authority.claim_eligible
        || authority.active_wall_seconds == 0
        || authority.active_wall_seconds > 43_200
        || authority.checkpoint_seconds != 300
        || !authority.attempt_id.starts_with("v36-prefix-screen-")
        || authority.attempt_id.len() > 128
        || !authority
            .attempt_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        || authority.source_commit.len() != 40
        || !authority
            .source_commit
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
        || authority
            .source_commit
            .bytes()
            .any(|byte| byte.is_ascii_uppercase())
        || !valid_s3_object_uri(&authority.output_prefix, true)
    {
        return Err(invalid("V36 prefix freeze execution authority differs"));
    }
    let expected_roles = [
        "binary",
        "freeze-authority",
        "source-archive",
        "source-registry",
    ];
    let mut uris = BTreeSet::new();
    if authority.inputs.len() != expected_roles.len()
        || authority
            .inputs
            .iter()
            .zip(expected_roles)
            .any(|(input, role)| {
                input.role != role
                    || input.encoded_bytes == 0
                    || !valid_digest(&input.sha256)
                    || !valid_digest(&input.blake3)
                    || !valid_s3_object_uri(&input.uri, false)
                    || !uris.insert(input.uri.as_str())
            })
    {
        return Err(invalid("V36 prefix freeze execution inputs differ"));
    }
    if authority.resume.as_ref().is_some_and(|resume| {
        !valid_checkpoint_artifact(&resume.manifest, "checkpoint-manifest")
            || resume.pointer_encoded_bytes == 0
            || !valid_digest(&resume.pointer_sha256)
            || !valid_s3_object_uri(&resume.pointer_uri, false)
            || resume.pointer_uri == resume.manifest.uri
    }) {
        return Err(invalid("V36 prefix freeze resume binding differs"));
    }
    Ok(())
}

/// Canonical newline JSON for one validated execution authority.
pub fn canonical_v36_prefix_freeze_execution_authority_bytes(
    authority: &V36PrefixFreezeExecutionAuthority,
) -> Result<Vec<u8>> {
    validate_v36_prefix_freeze_execution_authority(authority)?;
    canonical_value_bytes(authority)
}

fn valid_checkpoint_artifact(artifact: &V36ArtifactIdentity, role: &str) -> bool {
    artifact.role == role
        && artifact.encoded_bytes > 0
        && valid_digest(&artifact.sha256)
        && valid_digest(&artifact.blake3)
        && valid_s3_object_uri(&artifact.uri, false)
        && url::Url::parse(&artifact.uri).ok().is_some_and(|uri| {
            uri.path()
                .rsplit('/')
                .next()
                .is_some_and(|name| name.starts_with(&format!("{}-", artifact.sha256)))
        })
}

fn materialized_artifacts(
    artifacts: &V36PrefixMaterializedArtifacts,
) -> [(&V36ArtifactIdentity, &'static str); 6] {
    [
        (&artifacts.population_authority, "population-authority"),
        (&artifacts.source, "source"),
        (&artifacts.development_query, "development-query"),
        (&artifacts.validation_query, "validation-query"),
        (&artifacts.sealed_holdout_query, "sealed-holdout-query"),
        (&artifacts.performance_query, "performance-query"),
    ]
}

fn ground_truth_artifacts(
    artifacts: &V36PrefixGroundTruthArtifacts,
) -> [(&V36ArtifactIdentity, &'static str); 4] {
    [
        (&artifacts.heaps, "gt-heaps"),
        (&artifacts.development, "development-gt100"),
        (&artifacts.validation, "validation-gt100"),
        (&artifacts.sealed_holdout, "sealed-holdout-gt100"),
    ]
}

#[doc(hidden)]
/// Return the exact dependency-first closure for one checkpoint phase.
pub fn plan_v36_prefix_checkpoint_dependency_closure(
    manifest: &V36PrefixCheckpointManifest,
) -> Result<Vec<V36ArtifactIdentity>> {
    let mut dependencies = manifest.population.identity_runs.clone();
    match &manifest.phase {
        V36PrefixCheckpointPhase::Population => {}
        V36PrefixCheckpointPhase::Selected { selection } => {
            dependencies.push(selection.selected_ids.clone());
        }
        V36PrefixCheckpointPhase::Materialized {
            artifacts,
            selection,
        } => {
            dependencies.push(selection.selected_ids.clone());
            dependencies.extend(
                materialized_artifacts(artifacts)
                    .into_iter()
                    .map(|(artifact, _)| artifact.clone()),
            );
        }
        V36PrefixCheckpointPhase::GroundTruth {
            heaps,
            materialized,
            selection,
            ..
        } => {
            dependencies.push(selection.selected_ids.clone());
            dependencies.extend(
                materialized_artifacts(materialized)
                    .into_iter()
                    .map(|(artifact, _)| artifact.clone()),
            );
            dependencies.push(heaps.as_ref().clone());
        }
        V36PrefixCheckpointPhase::Complete {
            ground_truth,
            materialized,
            selection,
        } => {
            dependencies.push(selection.selected_ids.clone());
            dependencies.extend(
                materialized_artifacts(materialized)
                    .into_iter()
                    .map(|(artifact, _)| artifact.clone()),
            );
            dependencies.extend(
                ground_truth_artifacts(ground_truth)
                    .into_iter()
                    .map(|(artifact, _)| artifact.clone()),
            );
        }
    }
    let mut uris = BTreeSet::new();
    if dependencies
        .iter()
        .any(|artifact| !uris.insert(artifact.uri.as_str()))
    {
        return Err(invalid("V36 prefix checkpoint dependencies overlap"));
    }
    Ok(dependencies)
}

fn valid_materialized_artifacts(artifacts: &V36PrefixMaterializedArtifacts) -> bool {
    let mut uris = BTreeSet::new();
    materialized_artifacts(artifacts)
        .into_iter()
        .all(|(artifact, role)| {
            valid_checkpoint_artifact(artifact, role) && uris.insert(artifact.uri.as_str())
        })
}

fn checkpoint_source_matches_registry(
    consumed: &V36PrefixSourceObject,
    registered: &V36PrefixRegisteredSourceObject,
) -> bool {
    let mut sample = Sha256::new();
    sample.update(b"borsuk-v36-screen-object-v1");
    sample.update(registered.path.as_bytes());
    sample.update(registered.encoded_bytes.to_le_bytes());
    consumed.encoded_bytes == registered.encoded_bytes
        && consumed.path == registered.path
        && consumed.sha256 == registered.sha256
        && consumed.uri == registered.uri
        && consumed.sample_sha256 == format!("{:x}", sample.finalize())
}

fn valid_population_selection(
    selection: &V36PrefixPopulationSelection,
    population: &V36PrefixPopulationCheckpoint,
) -> bool {
    selection.selected_rows > 0
        && selection.eligible_rows >= selection.selected_rows
        && selection.eligible_rows.checked_add(selection.excluded_rows)
            == Some(population.distinct_rows)
        && valid_digest(&selection.cutoff_score_sha256)
        && selection
            .excluded_population_identity
            .as_ref()
            .is_none_or(|identity| {
                valid_checkpoint_artifact(identity, "population-selected-identities")
            })
        && valid_checkpoint_artifact(&selection.selected_ids, "population-selected-identities")
}

fn population_selection_matches_context(
    context: &V36PrefixCheckpointContext,
    selection: &V36PrefixPopulationSelection,
) -> bool {
    selection.selected_rows == context.distinct_candidates
        && selection
            .selected_ids
            .uri
            .starts_with(&context.object_prefix)
        && match context.cohort_ordinal {
            0 => {
                context.excluded_population_identity.is_none()
                    && selection.excluded_population_identity.is_none()
                    && selection.excluded_rows == 0
            }
            1 => {
                context.excluded_population_identity.is_some()
                    && selection.excluded_population_identity
                        == context.excluded_population_identity
            }
            _ => false,
        }
}

/// Validate one immutable V36 prefix-freeze checkpoint manifest.
pub fn validate_v36_prefix_checkpoint_manifest(
    manifest: &V36PrefixCheckpointManifest,
) -> Result<()> {
    if manifest.schema != "borsuk-v36-prefix-freeze-checkpoint-v2"
        || manifest.claim_eligible
        || !valid_digest(&manifest.execution_authority_sha256)
        || !valid_digest(&manifest.freeze_authority_sha256)
        || !valid_digest(&manifest.source_archive_sha256)
        || !valid_digest(&manifest.source_registry_sha256)
        || manifest.source_commit.len() != 40
        || !manifest
            .source_commit
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || manifest.producer_attempt_id
            != format!(
                "{}-attempt-{:04}",
                manifest.run_id, manifest.producer_attempt_ordinal
            )
        || manifest.producer_attempt_ordinal >= 3
        || !manifest.run_id.starts_with("v36-prefix-screen-")
        || manifest.producer_instance_id.is_empty()
        || (manifest.generation == 0) != manifest.previous_checkpoint.is_none()
        || manifest
            .previous_checkpoint
            .as_ref()
            .is_some_and(|previous| !valid_checkpoint_artifact(previous, "checkpoint-manifest"))
    {
        return Err(invalid("V36 prefix checkpoint authority differs"));
    }
    let population = &manifest.population;
    if population.consumed_objects.is_empty()
        || population.selected_object_count == 0
        || population.selected_object_count > 16
        || population
            .selected_object_start
            .checked_add(population.selected_object_count)
            .is_none()
        || usize::from(population.completed_objects) != population.consumed_objects.len()
        || population.completed_objects > population.selected_object_count
        || population.identity_runs.len() != population.consumed_objects.len()
        || population.distinct_rows == 0
        || population
            .distinct_rows
            .checked_add(population.duplicate_rows)
            != Some(population.physical_rows)
        || population.consumed_objects.iter().any(|object| {
            object.path.is_empty()
                || object.encoded_bytes == 0
                || !valid_digest(&object.sha256)
                || !valid_digest(&object.blake3)
                || !valid_digest(&object.sample_sha256)
                || object.uri.is_empty()
        })
        || population
            .identity_runs
            .iter()
            .enumerate()
            .any(|(ordinal, run)| {
                let Some(global_ordinal) = population
                    .selected_object_start
                    .checked_add(u16::try_from(ordinal).unwrap_or(u16::MAX))
                else {
                    return true;
                };
                !valid_checkpoint_artifact(
                    run,
                    &format!("population-identity-run-{global_ordinal:04}"),
                )
            })
    {
        return Err(invalid("V36 prefix population checkpoint differs"));
    }
    match &manifest.phase {
        V36PrefixCheckpointPhase::Population => {}
        V36PrefixCheckpointPhase::Selected { selection } => {
            if population.completed_objects != population.selected_object_count
                || !valid_population_selection(selection, population)
            {
                return Err(invalid("V36 prefix selected checkpoint differs"));
            }
        }
        V36PrefixCheckpointPhase::GroundTruth {
            heaps,
            materialized,
            next_source_ordinal,
            selection,
        } => {
            if population.completed_objects != population.selected_object_count
                || !valid_population_selection(selection, population)
                || *next_source_ordinal == 0
                || !valid_checkpoint_artifact(heaps, "gt-heaps")
                || !valid_materialized_artifacts(materialized)
            {
                return Err(invalid("V36 prefix GT checkpoint differs"));
            }
        }
        V36PrefixCheckpointPhase::Materialized {
            artifacts,
            selection,
        } => {
            if population.completed_objects != population.selected_object_count
                || !valid_population_selection(selection, population)
                || !valid_materialized_artifacts(artifacts)
            {
                return Err(invalid("V36 prefix materialized checkpoint differs"));
            }
        }
        V36PrefixCheckpointPhase::Complete {
            ground_truth,
            materialized,
            selection,
        } => {
            if population.completed_objects != population.selected_object_count
                || !valid_population_selection(selection, population)
                || !valid_materialized_artifacts(materialized)
                || ground_truth_artifacts(ground_truth)
                    .into_iter()
                    .any(|(artifact, role)| !valid_checkpoint_artifact(artifact, role))
            {
                return Err(invalid("V36 prefix complete checkpoint differs"));
            }
        }
    }
    Ok(())
}

/// Validate one checkpoint against its frozen campaign and ranked source authority.
pub fn validate_v36_prefix_checkpoint_manifest_with_context(
    context: &V36PrefixCheckpointContext,
    manifest: &V36PrefixCheckpointManifest,
) -> Result<()> {
    validate_v36_prefix_checkpoint_manifest(manifest)?;
    let population = &manifest.population;
    let consumed_bytes = population
        .consumed_objects
        .iter()
        .try_fold(0_u64, |total, object| {
            total.checked_add(object.encoded_bytes)
        })
        .ok_or_else(|| invalid("V36 prefix checkpoint source bytes overflow"))?;
    let structural_artifacts_in_scope = population
        .identity_runs
        .iter()
        .chain(manifest.previous_checkpoint.iter())
        .all(|artifact| artifact.uri.starts_with(&context.object_prefix));
    let phase_in_scope = match &manifest.phase {
        V36PrefixCheckpointPhase::Population => true,
        V36PrefixCheckpointPhase::Selected { selection } => {
            population_selection_matches_context(context, selection)
        }
        V36PrefixCheckpointPhase::Materialized {
            artifacts,
            selection,
        } => {
            population_selection_matches_context(context, selection)
                && materialized_artifacts(artifacts)
                    .into_iter()
                    .all(|(artifact, _)| artifact.uri.starts_with(&context.object_prefix))
        }
        V36PrefixCheckpointPhase::GroundTruth {
            heaps,
            materialized,
            next_source_ordinal,
            selection,
        } => {
            let aligned = *next_source_ordinal == context.corpus_rows
                || (context.gt_block_rows > 0
                    && next_source_ordinal.is_multiple_of(context.gt_block_rows));
            aligned
                && *next_source_ordinal <= context.corpus_rows
                && population_selection_matches_context(context, selection)
                && heaps.uri.starts_with(&context.object_prefix)
                && materialized_artifacts(materialized)
                    .into_iter()
                    .all(|(artifact, _)| artifact.uri.starts_with(&context.object_prefix))
        }
        V36PrefixCheckpointPhase::Complete {
            ground_truth,
            materialized,
            selection,
        } => {
            population_selection_matches_context(context, selection)
                && materialized_artifacts(materialized)
                    .into_iter()
                    .chain(ground_truth_artifacts(ground_truth))
                    .all(|(artifact, _)| artifact.uri.starts_with(&context.object_prefix))
        }
    };
    let expected_pointer_uri = context
        .object_prefix
        .strip_suffix("objects/")
        .map(|prefix| format!("{prefix}runs/{}/latest.json", context.run_id));
    if context.run_id != manifest.run_id
        || context.freeze_authority_sha256 != manifest.freeze_authority_sha256
        || context.source_archive_sha256 != manifest.source_archive_sha256
        || context.source_commit != manifest.source_commit
        || context.source_registry_sha256 != manifest.source_registry_sha256
        || context.selected_object_count == 0
        || context.selected_object_count > 16
        || context.selected_object_start
            != u16::from(context.cohort_ordinal).saturating_mul(context.selected_object_count)
        || match context.cohort_ordinal {
            0 => context.excluded_population_identity.is_some(),
            1 => context
                .excluded_population_identity
                .as_ref()
                .is_none_or(|identity| {
                    !valid_checkpoint_artifact(identity, "population-selected-identities")
                }),
            _ => true,
        }
        || context
            .selected_object_start
            .checked_add(context.selected_object_count)
            .is_none()
        || context.ranked_objects.len() != usize::from(context.selected_object_count)
        || population.selected_object_start != context.selected_object_start
        || population.selected_object_count != context.selected_object_count
        || population.consumed_objects.len() > context.ranked_objects.len()
        || population
            .consumed_objects
            .iter()
            .zip(&context.ranked_objects)
            .any(|(consumed, registered)| !checkpoint_source_matches_registry(consumed, registered))
        || consumed_bytes > context.source_byte_cap
        || !context.object_prefix.starts_with("s3://")
        || !context.object_prefix.ends_with("/objects/")
        || expected_pointer_uri.as_deref() != Some(context.pointer_uri.as_str())
        || !structural_artifacts_in_scope
        || !phase_in_scope
    {
        return Err(invalid("V36 prefix checkpoint context differs"));
    }
    Ok(())
}

fn checkpoint_phase_rank(phase: &V36PrefixCheckpointPhase) -> u8 {
    match phase {
        V36PrefixCheckpointPhase::Population => 0,
        V36PrefixCheckpointPhase::Selected { .. } => 1,
        V36PrefixCheckpointPhase::Materialized { .. } => 2,
        V36PrefixCheckpointPhase::GroundTruth { .. } => 3,
        V36PrefixCheckpointPhase::Complete { .. } => 4,
    }
}

/// Validate one authenticated monotonic checkpoint transition, including cross-attempt resume.
pub fn validate_v36_prefix_checkpoint_transition(
    context: &V36PrefixCheckpointContext,
    previous: &V36PrefixCheckpointManifest,
    next: &V36PrefixCheckpointManifest,
) -> Result<()> {
    validate_v36_prefix_checkpoint_manifest_with_context(context, previous)?;
    validate_v36_prefix_checkpoint_manifest_with_context(context, next)?;
    let previous_bytes = canonical_v36_prefix_checkpoint_manifest_bytes(previous)?;
    let previous_sha256 = format!("{:x}", Sha256::digest(&previous_bytes));
    let previous_blake3 = blake3::hash(&previous_bytes).to_hex().to_string();
    let linked = next.previous_checkpoint.as_ref().is_some_and(|identity| {
        identity.role == "checkpoint-manifest"
            && identity.encoded_bytes == previous_bytes.len() as u64
            && identity.sha256 == previous_sha256
            && identity.blake3 == previous_blake3
            && identity.uri.starts_with(&context.object_prefix)
    });
    let population_prefix = next
        .population
        .consumed_objects
        .starts_with(&previous.population.consumed_objects)
        && next
            .population
            .identity_runs
            .starts_with(&previous.population.identity_runs)
        && next.population.distinct_rows >= previous.population.distinct_rows
        && next.population.duplicate_rows >= previous.population.duplicate_rows
        && next.population.physical_rows >= previous.population.physical_rows;
    let population_frozen = (matches!(previous.phase, V36PrefixCheckpointPhase::Population)
        && previous.population.completed_objects < previous.population.selected_object_count)
        || next.population == previous.population;
    let phase_lineage = match (&previous.phase, &next.phase) {
        (V36PrefixCheckpointPhase::Population, V36PrefixCheckpointPhase::Population) => {
            previous.population.completed_objects < previous.population.selected_object_count
                && next.population.completed_objects
                    == previous.population.completed_objects.saturating_add(1)
        }
        (V36PrefixCheckpointPhase::Population, V36PrefixCheckpointPhase::Selected { .. }) => {
            previous.population.completed_objects == previous.population.selected_object_count
        }
        (
            V36PrefixCheckpointPhase::Selected {
                selection: previous,
            },
            V36PrefixCheckpointPhase::Materialized {
                selection: next, ..
            },
        ) => previous == next,
        (
            V36PrefixCheckpointPhase::Materialized {
                artifacts: previous,
                selection: previous_selection,
            },
            V36PrefixCheckpointPhase::GroundTruth {
                materialized: next,
                selection: next_selection,
                ..
            },
        ) => previous == next && previous_selection == next_selection,
        (
            V36PrefixCheckpointPhase::GroundTruth {
                materialized: previous_materialized,
                next_source_ordinal: previous_ordinal,
                selection: previous_selection,
                ..
            },
            V36PrefixCheckpointPhase::GroundTruth {
                materialized: next_materialized,
                next_source_ordinal: next_ordinal,
                selection: next_selection,
                ..
            },
        ) => {
            previous_materialized == next_materialized
                && previous_selection == next_selection
                && next_ordinal > previous_ordinal
        }
        (
            V36PrefixCheckpointPhase::GroundTruth {
                heaps: previous_heaps,
                materialized: previous_materialized,
                next_source_ordinal,
                selection: previous_selection,
            },
            V36PrefixCheckpointPhase::Complete {
                ground_truth,
                materialized: next_materialized,
                selection: next_selection,
            },
        ) => {
            *next_source_ordinal == context.corpus_rows
                && ground_truth.heaps == **previous_heaps
                && previous_materialized == next_materialized
                && previous_selection == next_selection
        }
        _ => false,
    };
    let same_attempt_valid = next.producer_attempt_ordinal != previous.producer_attempt_ordinal
        || (next.producer_attempt_id == previous.producer_attempt_id
            && next.execution_authority_sha256 == previous.execution_authority_sha256
            && next.producer_instance_id == previous.producer_instance_id);
    if next.generation != previous.generation.saturating_add(1)
        || !linked
        || next.run_id != previous.run_id
        || next.freeze_authority_sha256 != previous.freeze_authority_sha256
        || next.source_archive_sha256 != previous.source_archive_sha256
        || next.source_commit != previous.source_commit
        || next.source_registry_sha256 != previous.source_registry_sha256
        || next.producer_attempt_ordinal < previous.producer_attempt_ordinal
        || !same_attempt_valid
        || !population_prefix
        || !population_frozen
        || checkpoint_phase_rank(&next.phase) < checkpoint_phase_rank(&previous.phase)
        || !phase_lineage
    {
        return Err(invalid("V36 prefix checkpoint transition differs"));
    }
    Ok(())
}

/// Canonical newline JSON for one validated V36 prefix-freeze checkpoint.
pub fn canonical_v36_prefix_checkpoint_manifest_bytes(
    manifest: &V36PrefixCheckpointManifest,
) -> Result<Vec<u8>> {
    validate_v36_prefix_checkpoint_manifest(manifest)?;
    canonical_value_bytes(manifest)
}

/// Canonical newline JSON for one validated run-scoped checkpoint pointer.
pub fn canonical_v36_prefix_checkpoint_pointer_bytes(
    context: &V36PrefixCheckpointContext,
    pointer: &V36PrefixCheckpointPointer,
) -> Result<Vec<u8>> {
    if pointer.schema != "borsuk-v36-prefix-checkpoint-pointer-v2"
        || pointer.claim_eligible
        || pointer.run_id != context.run_id
        || pointer.producer_attempt_ordinal >= 3
        || pointer.producer_attempt_id
            != format!(
                "{}-attempt-{:04}",
                pointer.run_id, pointer.producer_attempt_ordinal
            )
        || pointer.manifest.role != "checkpoint-manifest"
        || !valid_checkpoint_artifact(&pointer.manifest, "checkpoint-manifest")
        || !pointer.manifest.uri.starts_with(&context.object_prefix)
    {
        return Err(invalid("V36 prefix checkpoint pointer differs"));
    }
    canonical_value_bytes(pointer)
}

fn checkpoint_manifest_identity(
    context: &V36PrefixCheckpointContext,
    manifest: &V36PrefixCheckpointManifest,
    bytes: &[u8],
) -> V36ArtifactIdentity {
    let sha256 = format!("{:x}", Sha256::digest(bytes));
    V36ArtifactIdentity {
        blake3: blake3::hash(bytes).to_hex().to_string(),
        encoded_bytes: bytes.len() as u64,
        role: "checkpoint-manifest".to_owned(),
        sha256: sha256.clone(),
        uri: format!(
            "{}{sha256}-checkpoint-{:08}.json",
            context.object_prefix, manifest.generation
        ),
    }
}

/// Build an immutable dependency-first publication and final run-pointer CAS.
pub fn plan_v36_prefix_checkpoint_publication(
    context: &V36PrefixCheckpointContext,
    manifest: &V36PrefixCheckpointManifest,
    previous_manifest: Option<&V36PrefixCheckpointManifest>,
    current_pointer: Option<(&[u8], &str)>,
) -> Result<V36PrefixCheckpointPublication> {
    validate_v36_prefix_checkpoint_manifest_with_context(context, manifest)?;
    let condition = match (manifest.generation, previous_manifest, current_pointer) {
        (0, None, None) if manifest.previous_checkpoint.is_none() => {
            V36PrefixCheckpointPointerCondition::Create
        }
        (0, _, _) => return Err(invalid("V36 prefix checkpoint pointer genesis differs")),
        (_, Some(previous), Some((bytes, etag))) if !etag.is_empty() => {
            let current: V36PrefixCheckpointPointer = serde_json::from_slice(bytes)
                .map_err(|_| invalid("V36 prefix current checkpoint pointer JSON differs"))?;
            let previous_bytes = canonical_v36_prefix_checkpoint_manifest_bytes(previous)?;
            let previous_identity =
                checkpoint_manifest_identity(context, previous, &previous_bytes);
            if canonical_v36_prefix_checkpoint_pointer_bytes(context, &current)? != bytes
                || current.generation.checked_add(1) != Some(manifest.generation)
                || manifest.previous_checkpoint.as_ref() != Some(&current.manifest)
                || current.manifest != previous_identity
                || validate_v36_prefix_checkpoint_transition(context, previous, manifest).is_err()
            {
                return Err(invalid("V36 prefix current checkpoint pointer differs"));
            }
            V36PrefixCheckpointPointerCondition::Replace {
                etag: etag.to_owned(),
            }
        }
        _ => return Err(invalid("V36 prefix checkpoint predecessor is missing")),
    };

    let manifest_bytes = canonical_v36_prefix_checkpoint_manifest_bytes(manifest)?;
    let manifest_identity = checkpoint_manifest_identity(context, manifest, &manifest_bytes);
    let pointer = V36PrefixCheckpointPointer {
        claim_eligible: false,
        generation: manifest.generation,
        manifest: manifest_identity.clone(),
        producer_attempt_id: manifest.producer_attempt_id.clone(),
        producer_attempt_ordinal: manifest.producer_attempt_ordinal,
        run_id: manifest.run_id.clone(),
        schema: "borsuk-v36-prefix-checkpoint-pointer-v2".to_owned(),
    };
    let pointer_bytes = canonical_v36_prefix_checkpoint_pointer_bytes(context, &pointer)?;
    let dependencies = plan_v36_prefix_checkpoint_dependency_closure(manifest)?;
    Ok(V36PrefixCheckpointPublication {
        dependencies,
        condition,
        manifest: manifest_identity,
        manifest_bytes,
        pointer_bytes,
        pointer_uri: context.pointer_uri.clone(),
        previous_pointer_sha256: current_pointer
            .map(|(bytes, _)| format!("{:x}", Sha256::digest(bytes))),
    })
}

/// Resolve an ambiguous pointer write only when the observed bytes equal the intention exactly.
pub fn validate_v36_prefix_checkpoint_pointer_observation(
    intended: &[u8],
    observed: &[u8],
) -> Result<()> {
    if intended.is_empty() || intended != observed {
        return Err(invalid("V36 prefix checkpoint pointer observation differs"));
    }
    Ok(())
}

/// Validate a completed freeze receipt against every immutable input authority.
pub fn validate_v36_prefix_freeze_receipt(
    receipt: &V36PrefixFreezeReceipt,
    authority: &V36PrefixFreezeAuthority,
    execution: &V36PrefixFreezeExecutionAuthority,
    source_registry: &[V36PrefixRegisteredSourceObject],
) -> Result<()> {
    validate_v36_prefix_freeze_authority(authority, source_registry)?;
    validate_v36_prefix_freeze_execution_authority(execution)?;
    let expected_population = bind_v36_prefix_population_authority(
        authority,
        receipt.population.consumed_objects.clone(),
        source_registry,
    )?;
    let freeze_sha256 = format!(
        "{:x}",
        Sha256::digest(canonical_v36_prefix_freeze_authority_bytes(
            authority,
            source_registry,
        )?)
    );
    let execution_sha256 = format!(
        "{:x}",
        Sha256::digest(canonical_v36_prefix_freeze_execution_authority_bytes(
            execution,
        )?)
    );
    let registry_sha256 = format!(
        "{:x}",
        Sha256::digest(canonical_v36_prefix_source_registry_bytes(
            authority,
            source_registry,
        )?)
    );
    let source_archive_sha256 = execution
        .inputs
        .iter()
        .find(|input| input.role == "source-archive")
        .map(|input| input.sha256.as_str())
        .ok_or_else(|| invalid("V36 prefix freeze receipt archive differs"))?;
    if receipt.schema != "borsuk-v36-prefix-freeze-receipt-v2"
        || receipt.claim_eligible
        || receipt.population != expected_population
        || receipt.selection.selected_rows != authority.distinct_candidates
        || receipt.selection.selected_rows > receipt.selection.eligible_rows
        || receipt.selection.excluded_population_identity != authority.excluded_population_identity
        || (authority.cohort_ordinal == 0 && receipt.selection.excluded_rows != 0)
        || receipt
            .selection
            .eligible_rows
            .checked_add(receipt.selection.excluded_rows)
            != Some(receipt.distinct_rows_observed)
        || !valid_checkpoint_artifact(
            &receipt.selection.selected_ids,
            "population-selected-identities",
        )
        || !valid_digest(&receipt.selection.cutoff_score_sha256)
        || receipt.freeze_authority_sha256 != freeze_sha256
        || receipt.execution_authority_sha256 != execution_sha256
        || receipt.source_registry_sha256 != registry_sha256
        || receipt.source_archive_sha256 != source_archive_sha256
        || receipt.physical_rows < receipt.distinct_rows_observed
        || receipt.distinct_rows_observed < authority.distinct_candidates
        || receipt
            .physical_rows
            .checked_sub(receipt.distinct_rows_observed)
            != Some(receipt.duplicate_rows)
        || receipt.cutoff_object_ordinal < receipt.population.selected_object_start
        || receipt.cutoff_object_ordinal
            >= receipt
                .population
                .selected_object_start
                .checked_add(receipt.population.selected_object_count)
                .ok_or_else(|| invalid("V36 prefix freeze receipt object window overflows"))?
        || receipt.cutoff_row_offset >= receipt.physical_rows
    {
        return Err(invalid("V36 prefix freeze receipt differs"));
    }
    let expected_outputs = [
        ("population-authority", "population-authority.json"),
        ("source", "source.parquet"),
        ("development-query", "development-query.parquet"),
        ("development-gt100", "development-gt100.parquet"),
        ("validation-query", "validation-query.parquet"),
        ("validation-gt100", "validation-gt100.parquet"),
        ("sealed-holdout-query", "sealed-holdout-query.parquet"),
        ("sealed-holdout-gt100", "sealed-holdout-gt100.parquet"),
        ("performance-query", "performance-query.parquet"),
    ];
    let mut uris = BTreeSet::new();
    if receipt.outputs.len() != expected_outputs.len()
        || receipt
            .outputs
            .iter()
            .zip(expected_outputs)
            .any(|(output, (role, filename))| {
                output.role != role
                    || output.encoded_bytes == 0
                    || !valid_digest(&output.sha256)
                    || !valid_digest(&output.blake3)
                    || !valid_s3_object_uri(&output.uri, false)
                    || output.uri != format!("{}{filename}", execution.output_prefix)
                    || !uris.insert(output.uri.as_str())
            })
    {
        return Err(invalid("V36 prefix freeze receipt outputs differ"));
    }
    Ok(())
}

/// Canonical newline JSON for one validated completed freeze receipt.
pub fn canonical_v36_prefix_freeze_receipt_bytes(
    receipt: &V36PrefixFreezeReceipt,
    authority: &V36PrefixFreezeAuthority,
    execution: &V36PrefixFreezeExecutionAuthority,
    source_registry: &[V36PrefixRegisteredSourceObject],
) -> Result<Vec<u8>> {
    validate_v36_prefix_freeze_receipt(receipt, authority, execution, source_registry)?;
    canonical_value_bytes(receipt)
}

/// Bind observed complete consumed objects into the post-freeze population receipt.
pub fn bind_v36_prefix_population_authority(
    authority: &V36PrefixFreezeAuthority,
    consumed_objects: Vec<V36PrefixSourceObject>,
    source_registry: &[V36PrefixRegisteredSourceObject],
) -> Result<V36PrefixPopulationAuthority> {
    validate_v36_prefix_freeze_authority(authority, source_registry)?;
    let population = V36PrefixPopulationAuthority {
        claim_eligible: authority.claim_eligible,
        cohort_ordinal: authority.cohort_ordinal,
        construction_capability: authority.construction_capability.clone(),
        consumed_objects,
        corpus_rows: authority.corpus_rows,
        corpus_seed_label: authority.corpus_seed_label.clone(),
        corpus_seed_sha256: authority.corpus_seed_sha256.clone(),
        dataset_authority_sha256: authority.dataset_authority_sha256.clone(),
        distinct_candidates: authority.distinct_candidates,
        duplicate_rule: authority.duplicate_rule.clone(),
        evaluation_capability: authority.evaluation_capability.clone(),
        excluded_population_identity: authority.excluded_population_identity.clone(),
        format: "borsuk-v36-prefix-population-authority-v2".into(),
        future_full_source_exclusion_roles: authority.future_full_source_exclusion_roles.clone(),
        object_cap: authority.object_cap,
        object_sampling_algorithm: authority.object_sampling_algorithm.clone(),
        ordered_source_manifest_sha256: authority.ordered_source_manifest_sha256.clone(),
        population_id: "borsuk-v36-prefix-screen-population-v2".into(),
        population_sampling_algorithm: authority.population_sampling_algorithm.clone(),
        population_seed_label: authority.population_seed_label.clone(),
        population_seed_sha256: authority.population_seed_sha256.clone(),
        roles: authority.roles.clone(),
        selected_object_count: authority.selected_object_count,
        selected_object_encoded_bytes: authority.selected_object_encoded_bytes,
        selected_object_start: authority.selected_object_start,
        source_byte_cap: authority.source_byte_cap,
        source_revision: authority.source_revision.clone(),
        workspace_bytes: authority.workspace_bytes,
        workspace_count: authority.workspace_count,
    };
    validate_prefix_population(&population, source_registry)?;
    Ok(population)
}

/// Canonical newline JSON for one validated post-freeze population authority.
pub fn canonical_v36_prefix_population_authority_bytes(
    population: &V36PrefixPopulationAuthority,
    source_registry: &[V36PrefixRegisteredSourceObject],
) -> Result<Vec<u8>> {
    validate_prefix_population(population, source_registry)?;
    canonical_value_bytes(population)
}

pub(crate) fn canonical_v36_prefix_checkpoint_population_authority_bytes(
    population: &V36PrefixPopulationAuthority,
    source_registry: &[V36PrefixRegisteredSourceObject],
) -> Result<Vec<u8>> {
    validate_prefix_population_shape(population, source_registry)?;
    canonical_value_bytes(population)
}

pub(crate) fn canonical_v36_prefix_checkpoint_source_registry_bytes(
    population: &V36PrefixPopulationAuthority,
    source_registry: &[V36PrefixRegisteredSourceObject],
) -> Result<Vec<u8>> {
    validate_prefix_population_shape(population, source_registry)?;
    canonical_value_bytes(&source_registry)
}

fn validate_prefix_projection(projection: &V36ProjectionArm) -> Result<()> {
    match projection {
        V36ProjectionArm::Srht192 { seed } if *seed == PROJECTION_SEED => Ok(()),
        V36ProjectionArm::CenteredSubspace192 { artifact }
            if artifact.role == "centered-projection-basis"
                && !artifact.uri.is_empty()
                && artifact.encoded_bytes > 0
                && valid_digest(&artifact.sha256)
                && valid_digest(&artifact.blake3) =>
        {
            Ok(())
        }
        _ => Err(invalid("V36 prefix projection authority differs")),
    }
}

fn canonical_value_bytes(value: &impl Serialize) -> Result<Vec<u8>> {
    let value = serde_json::to_value(value)
        .map_err(|_| invalid("V36 prefix authority cannot be canonicalized"))?;
    let mut bytes = serde_json::to_vec(&canonical_json_value(value))
        .map_err(|_| invalid("V36 prefix authority cannot be serialized"))?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn artifact_binds_value(artifact: &V36ArtifactIdentity, value: &impl Serialize) -> Result<bool> {
    let bytes = canonical_value_bytes(value)?;
    Ok(
        artifact.encoded_bytes == u64::try_from(bytes.len()).unwrap_or(u64::MAX)
            && artifact.sha256 == format!("{:x}", Sha256::digest(&bytes))
            && artifact.blake3 == blake3::hash(&bytes).to_hex().as_str(),
    )
}

fn validate_prefix_manifest_fields(
    manifest: &V36PrefixScreenManifest,
    source_registry: &[V36PrefixRegisteredSourceObject],
) -> Result<()> {
    if manifest.format != "borsuk-v36-prefix-screen-manifest-v2"
        || manifest.claim_eligible
        || manifest.posting_summary_vector_bytes != 4_608
        || manifest.posting_summary_metadata_bytes != 128
        || manifest.posting_summary_slot_bytes != POSTING_SUMMARY_SLOT_BYTES
        || !matches!(
            manifest.geometry.primary_rows,
            V36PrimaryRows::Posting4096 | V36PrimaryRows::Posting8192
        )
        || !valid_geometry_parts(
            manifest.geometry.primary_rows,
            manifest.geometry.replication,
            manifest.shape_score,
        )
        || !matches!(manifest.unique_k, 512 | 1_024 | 1_536 | 2_048)
        || manifest
            .posting_summary_vector_bytes
            .checked_add(manifest.posting_summary_metadata_bytes)
            != Some(manifest.posting_summary_slot_bytes)
    {
        return Err(invalid("V36 prefix screen manifest differs"));
    }
    validate_prefix_population(&manifest.population, source_registry)?;
    validate_prefix_projection(&manifest.projection)?;

    let mut required_roles = BTreeSet::from([
        "population-authority",
        "posting-summaries",
        "query-excluded-corpus-parquet",
    ]);
    required_roles.insert(match &manifest.projection {
        V36ProjectionArm::Srht192 { .. } => "projection",
        V36ProjectionArm::CenteredSubspace192 { .. } => "centered-projection-basis",
    });
    let mut roles = BTreeSet::new();
    let mut uris = BTreeSet::new();
    for artifact in &manifest.artifacts {
        if !valid_digest(&artifact.sha256)
            || !valid_digest(&artifact.blake3)
            || artifact.encoded_bytes == 0
            || artifact.uri.is_empty()
            || !roles.insert(artifact.role.as_str())
            || !uris.insert(artifact.uri.as_str())
        {
            return Err(invalid("V36 prefix artifact identity differs"));
        }
    }
    if roles != required_roles {
        return Err(invalid("V36 prefix artifact role set differs"));
    }
    let population_artifact = manifest
        .artifacts
        .iter()
        .find(|artifact| artifact.role == "population-authority")
        .ok_or_else(|| invalid("V36 prefix population artifact is missing"))?;
    if !artifact_binds_value(population_artifact, &manifest.population)? {
        return Err(invalid("V36 prefix embedded authority binding differs"));
    }
    match &manifest.projection {
        V36ProjectionArm::Srht192 { .. } => {
            let projection_artifact = manifest
                .artifacts
                .iter()
                .find(|artifact| artifact.role == "projection")
                .ok_or_else(|| invalid("V36 prefix projection artifact is missing"))?;
            if !artifact_binds_value(projection_artifact, &manifest.projection)? {
                return Err(invalid("V36 prefix embedded authority binding differs"));
            }
        }
        V36ProjectionArm::CenteredSubspace192 { artifact, .. } => {
            let projection_artifact = manifest
                .artifacts
                .iter()
                .find(|candidate| candidate.role == "centered-projection-basis")
                .ok_or_else(|| invalid("V36 prefix projection artifact is missing"))?;
            if projection_artifact != artifact.as_ref() {
                return Err(invalid("V36 prefix embedded authority binding differs"));
            }
        }
    }
    Ok(())
}

/// Serialize one validated V36 prefix-screen manifest as canonical newline JSON.
pub fn canonical_v36_prefix_screen_manifest_bytes(
    manifest: &V36PrefixScreenManifest,
) -> Result<Vec<u8>> {
    canonical_value_bytes(manifest)
}

/// Authenticate, strictly decode, and validate one canonical V36 prefix manifest.
pub fn validate_v36_prefix_screen_manifest(
    bytes: &[u8],
    registered: &V36RegisteredManifest,
    source_registry: &[V36PrefixRegisteredSourceObject],
) -> Result<V36PrefixScreenManifest> {
    if registered.uri.is_empty()
        || !valid_digest(&registered.sha256)
        || !valid_digest(&registered.blake3)
        || registered.encoded_bytes != u64::try_from(bytes.len()).unwrap_or(u64::MAX)
        || format!("{:x}", Sha256::digest(bytes)) != registered.sha256
        || blake3::hash(bytes).to_hex().as_str() != registered.blake3
        || !bytes.ends_with(b"\n")
        || bytes.ends_with(b"\n\n")
    {
        return Err(invalid("V36 registered prefix manifest bytes differ"));
    }
    let manifest: V36PrefixScreenManifest =
        serde_json::from_slice(bytes).map_err(|_| invalid("V36 prefix manifest JSON differs"))?;
    if canonical_v36_prefix_screen_manifest_bytes(&manifest)? != bytes {
        return Err(invalid("V36 prefix manifest is not canonical"));
    }
    validate_prefix_manifest_fields(&manifest, source_registry)?;
    Ok(manifest)
}

fn valid_geometry_parts(
    primary_rows: V36PrimaryRows,
    replication: V36Replication,
    shape_score: V36ShapeScore,
) -> bool {
    match (primary_rows, replication) {
        (V36PrimaryRows::Control256, V36Replication::Single | V36Replication::DoubleControl) => {
            shape_score == V36ShapeScore::Centroid
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

fn valid_geometry(manifest: &V36FunnelManifest) -> bool {
    valid_geometry_parts(
        manifest.geometry.primary_rows,
        manifest.geometry.replication,
        manifest.shape_score,
    )
}

fn validate_manifest_fields(manifest: &V36FunnelManifest) -> Result<()> {
    if manifest.format != FORMAT
        || manifest.claim_eligible
        || manifest.metric != "squared-l2"
        || manifest.projection_dimensions != PROJECTION_DIMENSIONS
        || validate_prefix_projection(&manifest.projection).is_err()
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
        "source-parquet",
        "super-centroids",
        "visibility-directory",
    ]);
    required_roles.insert(match &manifest.projection {
        V36ProjectionArm::Srht192 { .. } => "projection",
        V36ProjectionArm::CenteredSubspace192 { .. } => "centered-projection-basis",
    });
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
    if let V36ProjectionArm::CenteredSubspace192 { artifact, .. } = &manifest.projection {
        let projection_artifact = manifest
            .artifacts
            .iter()
            .find(|candidate| candidate.role == "centered-projection-basis")
            .ok_or_else(|| invalid("V36 centered projection artifact is missing"))?;
        if projection_artifact != artifact.as_ref() {
            return Err(invalid("V36 centered projection artifact binding differs"));
        }
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
    /// Last dense assignment ordinal in this sparse fragment.
    pub last_dense_ordinal: u64,
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
    /// First posting excluded by normal limits; its ranked suffix is implicit.
    pub first_excluded_posting: Option<u32>,
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
        let mut previous_last_dense_ordinal = None;
        let mut stored_assignment_rows = 0_u64;
        for (expected, fragment) in posting.fragments.iter().enumerate() {
            let dense_span = fragment
                .last_dense_ordinal
                .checked_sub(fragment.first_dense_ordinal)
                .and_then(|span| span.checked_add(1));
            if fragment.fragment_ordinal != u32::try_from(expected).unwrap_or(u32::MAX)
                || dense_span.is_none_or(|span| span < u64::from(fragment.row_count))
                || previous_last_dense_ordinal
                    .is_some_and(|previous| fragment.first_dense_ordinal <= previous)
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
            previous_last_dense_ordinal = Some(fragment.last_dense_ordinal);
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
        first_excluded_posting: None,
        hard_returned_bytes_with_retries: 0,
        hard_gets_with_retries: 0,
        normal_encoded_bytes: 0,
        normal_gets: 0,
    };
    for (rank_index, posting_ordinal) in ranked_postings.iter().enumerate() {
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
            plan.first_excluded_posting = Some(ranked_postings[rank_index]);
            break;
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
    /// Sixteen fixed 32-MiB streaming query workspaces.
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
